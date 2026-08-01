"""
sync_state/core/ipc_transport.py

Framed TCP/Unix socket transport for inter-process communication.
"""

import asyncio
import struct
import logging
import json
from typing import Optional, Callable, Dict, Any, List
from .transport_adapter import TransportAdapter

logger = logging.getLogger("sync_state.ipc")

MAGIC = b"SSB1"
VERSION = 0x01
FRAME_HEADER_SIZE = 12
FRAME_DELTA = 1
FRAME_COMMAND = 4

class IPCError(Exception):
    pass


def pack_header(payload_len: int, flags: int = 0) -> bytes:
    return struct.pack(">4sBBHI", MAGIC, VERSION, 0x00, flags, payload_len)


def unpack_header(data: bytes) -> tuple:
    magic, version, frame_type, flags, length = struct.unpack(">4sBBHI", data)
    if magic != MAGIC:
        raise ValueError("Invalid magic")
    if version != VERSION:
        raise ValueError(f"Unsupported version: {version}")
    return frame_type, flags, length


async def read_frame(reader: asyncio.StreamReader) -> Optional[bytes]:
    """
    Read a complete framed message (header + payload) from the reader.
    Returns the full frame bytes or None if connection closed.
    """
    try:
        header = await reader.readexactly(FRAME_HEADER_SIZE)
    except asyncio.IncompleteReadError:
        return None
    _, _, length = unpack_header(header)
    if length > 16 * 1024 * 1024:
        raise ValueError("Frame too large")
    payload = await reader.readexactly(length)
    return header + payload


async def read_payload(reader: asyncio.StreamReader) -> tuple:
    """
    Read a framed message and return (frame_type, payload_bytes).
    Returns (None, None) on connection close.
    """
    frame = await read_frame(reader)
    if frame is None:
        return None, None
    frame_type, _, _ = unpack_header(frame[:FRAME_HEADER_SIZE])
    return frame_type, frame[FRAME_HEADER_SIZE:]


class IPCTransport(TransportAdapter):
    """
    Framed transport over TCP or Unix sockets.
    Supports both client (connect) and server (listen) modes.
    """

    def __init__(self, connect_to: Optional[str] = None, listen_on: Optional[str] = None):
        if connect_to and listen_on:
            raise ValueError("Specify either connect_to or listen_on, not both")
        if not connect_to and not listen_on:
            raise ValueError("Must specify connect_to or listen_on")
        self.connect_to = connect_to
        self.listen_on = listen_on

        self._closed = False
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._server: Optional[asyncio.Server] = None
        self._callbacks: List[Callable[[Dict], None]] = []
        self._stop_event: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def start(self):
        """Start the transport (connect or listen)."""
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        if self.connect_to:
            await self._connect()
        elif self.listen_on:
            await self._listen()
        asyncio.create_task(self._read_loop())

    async def _connect(self):
        host, port = self._parse_addr(self.connect_to)
        self._reader, self._writer = await asyncio.open_connection(host, port)
        logger.info(f"IPC connected to {self.connect_to}")

    async def _listen(self):
        host, port = self._parse_addr(self.listen_on)
        self._server = await asyncio.start_server(
            self._handle_client, host, port
        )
        logger.info(f"IPC listening on {self.listen_on}")

    async def _handle_client(self, reader, writer):
        if self._reader is None:
            self._reader = reader
            self._writer = writer
            logger.info("IPC client connected")
        else:
            writer.close()
            await writer.wait_closed()

    def _parse_addr(self, addr: str):
        if ":" in addr:
            host, port = addr.split(":", 1)
            return host, int(port)
        else:
            return addr, 0  # Unix socket (path)

    async def _read_loop(self):
        while not self._stop_event.is_set():
            try:
                if self._reader is None:
                    await asyncio.sleep(0.01)
                    continue
                header = await self._reader.readexactly(FRAME_HEADER_SIZE)
            except asyncio.IncompleteReadError:
                break
            except Exception as e:
                logger.error(f"IPC read error: {e}")
                break
            try:
                _, _, length = unpack_header(header)
                if length > 16 * 1024 * 1024:
                    raise ValueError("Frame too large")
                payload = await self._reader.readexactly(length)
                frame = json.loads(payload.decode())
                for cb in self._callbacks:
                    try:
                        cb(frame)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
            except Exception as e:
                logger.error(f"IPC frame handling error: {e}")

    def emit(self, frame: Dict[str, Any]) -> bool:
        """Send a frame synchronously (must be called from event loop)."""
        if self._closed or self._writer is None:
            return False
        if self._writer is None:
            logger.warning("IPC not connected")
            return False
        try:
            payload = json.dumps(frame).encode()
            header = pack_header(len(payload))
            self._writer.write(header + payload)
            return True
        except Exception as e:
            logger.error(f"IPC emit error: {e}")
            return False

    def on_frame(self, callback: Callable[[Dict], None]) -> None:
        self._callbacks.append(callback)

    async def close(self) -> None:
        self._closed = True
        self._stop_event.set()
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    def stop(self) -> None:
        """Synchronous stop – schedules a close and waits briefly."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.close())
        except RuntimeError:
            asyncio.run(self.close())