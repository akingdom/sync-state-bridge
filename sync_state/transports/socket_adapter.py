import asyncio
import json
import struct
import logging

logger = logging.getLogger("sync_state_bridge.socket")
LENGTH_PREFIX = "!I"


class StateSyncSocketServer:
    def __init__(self, state_sync, max_queue_size: int = 100):
        self.state_sync = state_sync
        self.max_queue_size = max_queue_size
        self.metrics = {
            "queue_length": 0,
            "deltas_dropped": 0,
            "full_snapshots_sent": 0,
            "client_congested_count": 0
        }

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        outbound_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=self.max_queue_size)
        priority_snapshot_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=10)
        is_congested = False

        async def _write_worker():
            nonlocal is_congested
            while True:
                if not priority_snapshot_queue.empty():
                    packet = await priority_snapshot_queue.get()
                    priority_snapshot_queue.task_done()
                else:
                    packet = await outbound_queue.get()
                    outbound_queue.task_done()

                self.metrics["queue_length"] = outbound_queue.qsize()
                payload = json.dumps(packet).encode("utf-8")
                header = struct.pack(LENGTH_PREFIX, len(payload))
                
                try:
                    writer.write(header)
                    writer.write(payload)
                    await writer.drain()
                except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
                    break

        write_task = asyncio.create_task(_write_worker())
        packet_gen = None

        try:
            header_bytes = await reader.readexactly(4)
            (length,) = struct.unpack(LENGTH_PREFIX, header_bytes)
            client_versions = json.loads((await reader.readexactly(length)).decode("utf-8"))

            packet_gen = self.state_sync.iter_packets(client_versions)
            async for packet in packet_gen:
                event_type = packet.get("event")
                data = packet.get("data", {})

                if event_type == "delta" and data.get("full"):
                    self.metrics["full_snapshots_sent"] += 1

                try:
                    outbound_queue.put_nowait(packet)
                except asyncio.QueueFull:
                    self.metrics["deltas_dropped"] += 1
                    if not is_congested:
                        is_congested = True
                        self.metrics["client_congested_count"] += 1
                        logger.warning("Client transport congested. Dropping delta frame.")
                    
                    while not outbound_queue.empty():
                        try:
                            outbound_queue.get_nowait()
                            outbound_queue.task_done()
                        except asyncio.QueueEmpty:
                            break

                    recovery_packet = {
                        "event": "delta",
                        "data": self.state_sync.get_delta(data.get("type", ""), client_version=-1)
                    }
                    try:
                        priority_snapshot_queue.put_nowait(recovery_packet)
                    except asyncio.QueueFull:
                        pass

        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            if packet_gen is not None:
                await packet_gen.aclose()
            write_task.cancel()
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def start_tcp(self, host: str = "127.0.0.1", port: int = 8765):
        return await asyncio.start_server(self.handle_connection, host, port)
