import asyncio
import json
import struct
import logging
import threading
from typing import Optional

from ..qos_queue import PriorityQoSQueue

logger = logging.getLogger("sync_state_bridge.socket")
LENGTH_PREFIX = "!I"


class StateSyncSocketServer:
    def __init__(
        self,
        state_sync,
        max_queue_size: int = 100,
        use_priority_queue: bool = False,
    ):
        self.state_sync = state_sync
        self.max_queue_size = max_queue_size
        self.use_priority_queue = use_priority_queue
        self.metrics = {
            "queue_length": 0,
            "deltas_dropped": 0,
            "full_snapshots_sent": 0,
            "client_congested_count": 0,
        }

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        loop = asyncio.get_running_loop()

        # Bridge queue: async side consumes from here
        outbound_queue: asyncio.Queue = asyncio.Queue()

        if self.use_priority_queue:
            # Priority queue sits on the producer side
            qos_queue = PriorityQoSQueue(capacity=self.max_queue_size)
            stopped = False
            cv = threading.Condition()

            def _bridge_loop():
                nonlocal stopped
                while not stopped:
                    frame_bytes = qos_queue.pop()
                    if frame_bytes is None:
                        with cv:
                            if qos_queue.qsize() == 0 and not stopped:
                                cv.wait(timeout=0.05)
                        continue
                    # Decode bytes back to dict for the existing async worker
                    try:
                        packet = json.loads(frame_bytes.decode("utf-8"))
                    except Exception:
                        continue
                    # Feed into the async queue from the thread
                    loop.call_soon_threadsafe(outbound_queue.put_nowait, packet)

            bridge_thread = threading.Thread(target=_bridge_loop, daemon=True)
            bridge_thread.start()
        else:
            # Legacy: plain asyncio.Queue
            qos_queue = None
            bridge_thread = None
            cv = None
            stopped = False

        async def _write_worker():
            nonlocal stopped
            while True:
                if self.use_priority_queue and qos_queue is not None:
                    # We need to await a packet from the bridge queue
                    packet = await outbound_queue.get()
                    outbound_queue.task_done()
                else:
                    # Legacy path
                    packet = await outbound_queue.get()
                    outbound_queue.task_done()

                # Update metrics
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
            client_versions = json.loads(
                (await reader.readexactly(length)).decode("utf-8")
            )

            packet_gen = self.state_sync.iter_packets(client_versions)
            async for packet in packet_gen:
                event_type = packet.get("event")
                data = packet.get("data", {})

                if event_type == "delta" and data.get("full"):
                    self.metrics["full_snapshots_sent"] += 1

                if self.use_priority_queue and qos_queue is not None:
                    # Use the priority queue – map event to QoS level
                    qos_level = 3 if event_type == "manifest" or data.get("full") else 2
                    frame_bytes = (json.dumps(packet) + "\n").encode("utf-8")
                    accepted = qos_queue.put(frame_bytes, qos_level=qos_level)
                    if not accepted:
                        self.metrics["deltas_dropped"] += 1
                        # Trigger congestion recovery (optional)
                        if not self.metrics.get("client_congested_count"):
                            self.metrics["client_congested_count"] = 1
                            recovery_packet = {
                                "event": "delta",
                                "data": self.state_sync.get_delta(
                                    data.get("type", ""), client_version=-1
                                ),
                            }
                            recovery_bytes = (json.dumps(recovery_packet) + "\n").encode(
                                "utf-8"
                            )
                            qos_queue.put(recovery_bytes, qos_level=3)
                else:
                    # Legacy path
                    try:
                        outbound_queue.put_nowait(packet)
                    except asyncio.QueueFull:
                        self.metrics["deltas_dropped"] += 1
                        if not self.metrics.get("client_congested_count"):
                            self.metrics["client_congested_count"] = 1
                            # Legacy recovery logic
                            while not outbound_queue.empty():
                                try:
                                    outbound_queue.get_nowait()
                                    outbound_queue.task_done()
                                except asyncio.QueueEmpty:
                                    break
                            recovery_packet = {
                                "event": "delta",
                                "data": self.state_sync.get_delta(
                                    data.get("type", ""), client_version=-1
                                ),
                            }
                            try:
                                outbound_queue.put_nowait(recovery_packet)
                            except asyncio.QueueFull:
                                pass

        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            if packet_gen is not None:
                await packet_gen.aclose()
            write_task.cancel()
            if self.use_priority_queue and bridge_thread is not None:
                stopped = True
                with cv:
                    cv.notify_all()
                bridge_thread.join(timeout=1.0)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def start_tcp(self, host: str = "127.0.0.1", port: int = 8765):
        return await asyncio.start_server(self.handle_connection, host, port)