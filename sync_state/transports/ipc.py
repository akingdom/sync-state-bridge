"""
sync_state/transports/ipc.py

Length‑prefixed framed transport for reliable IPC.
Extended with DuplexIPCTransport and new frame types.
"""

import asyncio
import struct
import threading
import logging
import time
import socket
import os
import json
from typing import Optional, BinaryIO, Union, Tuple

from .base import TransportAdapter, TransportMetrics
from ..qos_queue import PriorityQoSQueue

MAGIC = b"SSB1"
VERSION = 0x01
FRAME_HEADER_SIZE = 4 + 1 + 1 + 2 + 4  # 12 bytes

FRAME_DELTA = 1
FRAME_COMMAND = 2
FRAME_SNAPSHOT_CHUNK = 3
FRAME_COMMAND_ACK = 4          # new

logger = logging.getLogger("sync_state.transports.ipc")


def pack_header(frame_type: int, payload_len: int, flags: int = 0) -> bytes:
    return struct.pack(">4sBBHI", MAGIC, VERSION, frame_type, flags, payload_len)


def unpack_header(data: bytes) -> Tuple[int, int, int]:
    magic, version, frame_type, flags, length = struct.unpack(">4sBBHI", data)
    if magic != MAGIC:
        raise ValueError(f"Invalid magic: {magic}")
    if version != VERSION:
        raise ValueError(f"Unsupported version: {version}")
    return frame_type, flags, length


class FramedIPCTransport(TransportAdapter):
    """
    Thread‑safe transport that writes length‑prefixed frames.
    Accepts either a synchronous file-like object or an asyncio StreamWriter.
    """

    def __init__(self, writer: Union[BinaryIO, asyncio.StreamWriter], capacity: int = 1000):
        if hasattr(writer, 'drain') and hasattr(writer, 'write'):
            sock = writer.transport.get_extra_info('socket')
            new_fd = os.dup(sock.fileno())
            sync_sock = socket.socket(fileno=new_fd)
            sync_sock.setblocking(True)
            self._sync_writer = sync_sock.makefile('wb')
        else:
            self._sync_writer = writer

        self.capacity = capacity
        self._queue = PriorityQoSQueue(capacity=capacity)
        self._stopped = False
        self._emitted = 0
        self._dropped = 0
        self._backpressure = 0
        self._cv = threading.Condition()

        self._worker = threading.Thread(target=self._flush_loop, daemon=True)
        self._worker.start()

    def emit(self, frame_bytes: bytes, qos_level: int = 1,
             entity_id: Optional[str] = None) -> bool:
        if self._stopped:
            return False
        accepted = self._queue.put(frame_bytes, qos_level, entity_id)
        if accepted:
            self._emitted += 1
            with self._cv:
                self._cv.notify()
        else:
            self._dropped += 1
            self._backpressure += 1
        return accepted

    def send_payload(self, payload: bytes, frame_type: int = FRAME_DELTA,
                     flags: int = 0, qos_level: int = 1,
                     entity_id: Optional[str] = None) -> bool:
        header = pack_header(frame_type, len(payload), flags)
        frame = header + payload
        return self.emit(frame, qos_level, entity_id)

    def get_metrics(self) -> TransportMetrics:
        return TransportMetrics(
            queue_depth=self._queue.qsize(),
            queue_capacity=self.capacity,
            frames_emitted=self._emitted,
            frames_dropped=self._dropped,
            backpressure_events=self._backpressure,
        )

    def _flush_loop(self):
        while not self._stopped:
            frame = self._queue.pop()
            if frame is None:
                with self._cv:
                    if self._queue.qsize() == 0 and not self._stopped:
                        self._cv.wait(timeout=0.05)
                continue
            try:
                self._sync_writer.write(frame)
                self._sync_writer.flush()
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                logger.error("IPC write error: %s", e)
                self._stopped = True
                break
            except Exception as e:
                logger.error("Unexpected IPC write error: %s", e)

    def flush(self, timeout: float = 2.0) -> bool:
        start = time.time()
        while self._queue.qsize() > 0:
            if time.time() - start > timeout:
                return False
            time.sleep(0.005)
        return True

    def close(self):
        self._stopped = True
        with self._cv:
            self._cv.notify_all()
        if self._worker.is_alive():
            self._worker.join(timeout=1.0)
        if self._sync_writer:
            try:
                self._sync_writer.close()
            except Exception:
                pass


async def read_frame(reader: asyncio.StreamReader) -> Optional[bytes]:
    try:
        header = await reader.readexactly(FRAME_HEADER_SIZE)
    except asyncio.IncompleteReadError:
        return None
    frame_type, flags, length = unpack_header(header)
    if length > 16 * 1024 * 1024:  # 16 MB limit
        raise ValueError(f"Frame length {length} exceeds 16 MB limit")
    payload = await reader.readexactly(length)
    return header + payload


async def read_payload(reader: asyncio.StreamReader) -> Tuple[int, bytes]:
    frame = await read_frame(reader)
    if frame is None:
        return None, None
    frame_type, _, _ = unpack_header(frame[:FRAME_HEADER_SIZE])
    return frame_type, frame[FRAME_HEADER_SIZE:]


# ---------- NEW DuplexIPCTransport ----------
class DuplexIPCTransport:
    """
    Asynchronous, full‑duplex framing layer over a (reader, writer) pair.
    Supports sending/receiving all frame types.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self._closed = False

    async def send_frame(self, frame_type: int, payload: bytes, flags: int = 0) -> None:
        if self._closed:
            raise RuntimeError("Transport closed")
        header = pack_header(frame_type, len(payload), flags)
        self.writer.write(header + payload)
        await self.writer.drain()

    async def send_command(self, command_id: str, action: str, params: dict, client_id: str) -> None:
        payload = json.dumps({
            "command_id": command_id,
            "action": action,
            "params": params,
            "client_id": client_id
        }).encode()
        await self.send_frame(FRAME_COMMAND, payload)

    async def send_ack(self, command_id: str, status: str, result: dict, tick_id: int) -> None:
        payload = json.dumps({
            "command_id": command_id,
            "status": status,
            "result": result,
            "tick_id": tick_id
        }).encode()
        await self.send_frame(FRAME_COMMAND_ACK, payload)

    async def send_delta(self, delta: dict) -> None:
        payload = (json.dumps(delta) + "\n").encode()
        await self.send_frame(FRAME_DELTA, payload)

    async def read_frame(self) -> Tuple[Optional[int], Optional[bytes]]:
        if self._closed:
            return None, None
        frame = await read_frame(self.reader)
        if frame is None:
            self._closed = True
            return None, None
        frame_type, _, _ = unpack_header(frame[:FRAME_HEADER_SIZE])
        payload = frame[FRAME_HEADER_SIZE:]
        return frame_type, payload

    async def close(self):
        self._closed = True
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass