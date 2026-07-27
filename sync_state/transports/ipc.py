"""
sync_state/transports/ipc.py

AsyncIPCTransport: writes to a binary stream from a background thread.
Accepts either a synchronous file-like writer or an asyncio StreamWriter.
"""

import threading
import logging
import time
import socket
import os
from typing import Optional, BinaryIO, Union

from .base import TransportAdapter, TransportMetrics
from ..qos_queue import PriorityQoSQueue

logger = logging.getLogger("sync_state.transports.ipc")

class AsyncIPCTransport(TransportAdapter):
    """
    Thread-based transport that writes frames to a binary writer.

    If a StreamWriter is provided, it will extract the socket and create
    a synchronous writer for thread-safe writes.
    """

    def __init__(self, writer: Union[BinaryIO, 'asyncio.StreamWriter'], capacity: int = 1000):
        self._stream_writer = None
        self._sync_writer = None

        # Detect if we got an asyncio StreamWriter (has .drain)
        if hasattr(writer, 'drain') and hasattr(writer, 'write'):
            self._stream_writer = writer
            sock = writer.transport.get_extra_info('socket')
            # Duplicate the socket for independent blocking I/O
            new_fd = os.dup(sock.fileno())
            sync_sock = socket.socket(fileno=new_fd)
            sync_sock.setblocking(True)
            self._sync_writer = sync_sock.makefile('wb')
            logger.debug("Converted StreamWriter to synchronous writer")
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

    def get_metrics(self) -> TransportMetrics:
        return TransportMetrics(
            queue_depth=self._queue.qsize(),
            queue_capacity=self.capacity,
            frames_emitted=self._emitted,
            frames_dropped=self._dropped,
            backpressure_events=self._backpressure,
        )

    def _flush_loop(self) -> None:
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
                logger.error("IPC write error (connection lost): %s", e)
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

    def close(self) -> None:
        self._stopped = True
        with self._cv:
            self._cv.notify_all()
        if self._worker.is_alive():
            self._worker.join(timeout=1.0)
        # Close the synchronous writer if we created it
        if self._sync_writer:
            try:
                self._sync_writer.close()
            except Exception:
                pass
        # Close the underlying asyncio writer if we have one
        if self._stream_writer:
            try:
                self._stream_writer.close()
            except Exception:
                pass