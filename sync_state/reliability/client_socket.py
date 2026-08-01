import asyncio
import json
import struct
import logging
from typing import Dict, Any, Callable, Optional

logger = logging.getLogger("sync_state_bridge.client")
LENGTH_PREFIX = "!I"


class StateSyncSocketClient:
    """Production socket client with automatic exponential backoff reconnection."""

    def __init__(
        self, 
        host: str = "127.0.0.1", 
        port: int = 8765, 
        on_delta_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        max_reconnect_delay: float = 32.0
    ):
        self.host = host
        self.port = port
        self.on_delta_callback = on_delta_callback
        self.max_reconnect_delay = max_reconnect_delay
        self.local_versions: Dict[str, int] = {}
        self._running = False

    async def connect_and_listen(self):
        """Persistent event-loop wrapper for network auto-healing."""
        self._running = True
        reconnect_delay = 1.0

        while self._running:
            try:
                logger.info(f"Connecting to state sync server at {self.host}:{self.port}...")
                reader, writer = await asyncio.open_connection(self.host, self.port)
                reconnect_delay = 1.0  # Reset backoff interval on successful handshake

                handshake_payload = json.dumps(self.local_versions).encode("utf-8")
                header = struct.pack(LENGTH_PREFIX, len(handshake_payload))
                writer.write(header)
                writer.write(handshake_payload)
                await writer.drain()

                while self._running:
                    header_bytes = await reader.readexactly(4)
                    (length,) = struct.unpack(LENGTH_PREFIX, header_bytes)
                    payload_bytes = await reader.readexactly(length)
                    packet = json.loads(payload_bytes.decode("utf-8"))

                    event = packet.get("event")
                    data = packet.get("data", {})

                    if event == "manifest":
                        logger.info(f"Received server manifest. Schema version: {data.get('schema_version')}")

                    elif event == "delta":
                        type_name = data.get("type")
                        version = data.get("version", 0)
                        if type_name:
                            self.local_versions[type_name] = max(self.local_versions.get(type_name, 0), version)

                        if self.on_delta_callback:
                            if asyncio.iscoroutinefunction(self.on_delta_callback):
                                await self.on_delta_callback(data)
                            else:
                                self.on_delta_callback(data)

            except (ConnectionRefusedError, asyncio.IncompleteReadError, BrokenPipeError, OSError) as e:
                logger.warning(f"Connection error ({e}). Reconnecting in {reconnect_delay:.1f}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2.0, self.max_reconnect_delay)

    def stop(self):
        self._running = False
