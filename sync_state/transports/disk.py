"""
sync_state/transports/disk.py

DiskPersistenceAdapter: persistent append-only file writer (WAL) with persistent handle.
"""

import os
import time
import threading
import logging
from typing import Optional
from .base import TransportAdapter, TransportMetrics
from ..qos_queue import PriorityQoSQueue

logger = logging.getLogger("sync_state.transports.disk")

class DiskPersistenceAdapter(TransportAdapter):
    """
    Writes frames to a disk file in append-only mode using a persistent file handle.
    """

    def __init__(self, file_path: str, zero_loss_mode: bool = False,
                 capacity: int = 1000):
        self.file_path = os.path.abspath(file_path)
        self.zero_loss_mode = zero_loss_mode
        self.capacity = capacity
        self._queue = PriorityQoSQueue(capacity=capacity)
        self._stopped = False
        self._emitted = 0
        self._dropped = 0
        self._backpressure = 0
        self._cv = threading.Condition()

        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

        self._worker = threading.Thread(target=self._flush_loop, daemon=True)
        self._worker.start()

    def emit(self, frame_bytes: bytes, qos_level: int = 1,
             entity_id: Optional[str] = None) -> bool:
        if self._stopped:
            return False
            
        effective_qos = 3 if self.zero_loss_mode else qos_level
        accepted = self._queue.put(frame_bytes, effective_qos, entity_id)
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
        try:
            with open(self.file_path, "ab", buffering=0) as f:
                while not self._stopped:
                    frame = self._queue.pop()
                    if frame is None:
                        with self._cv:
                            if self._queue.qsize() == 0 and not self._stopped:
                                self._cv.wait(timeout=0.05)
                        continue
                    
                    f.write(frame)
        except IOError as e:
            logger.critical("Fatal disk persistence error on file %s: %s", self.file_path, e)
            self._stopped = True

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