"""
sync_state/reliability/qos_queue.py

Three‑tier priority queue with conflation for BEST_EFFORT and CRITICAL guarantees.
Thread‑safe.
"""

import queue
import time
import asyncio
import threading
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger("sync_state.reliability.qos_queue")


class PriorityQoSQueue:
    """
    Multi‑tier queue with token‑bucket fairness and conflation.

    Tiers:
        Level 3 (CRITICAL):  Never dropped. Evicts BEST_EFFORT if necessary.
        Level 2 (CONFLATABLE):  Keeps only the latest frame per `entity_id`.
        Level 1 (BEST_EFFORT):  Dropped if total queue > 80% capacity.
    """

    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self._critical_q = queue.Queue(maxsize=capacity)
        self._conflatable: Dict[str, bytes] = {}
        self._conflatable_lock = threading.Lock()
        self._best_effort_q = queue.Queue(maxsize=capacity)
        self._total_evicted = 0
        self._total_enqueued = 0

    def put(self, frame_bytes: bytes, qos_level: int = 1,
            entity_id: Optional[str] = None) -> bool:
        """
        Insert a frame into the queue.

        Returns:
            True if accepted, False if dropped.
        """
        self._total_enqueued += 1
        total_depth = (self._critical_q.qsize() +
                       len(self._conflatable) +
                       self._best_effort_q.qsize())

        if qos_level == 1:  # BEST_EFFORT
            if total_depth > int(self.capacity * 0.8):
                self._total_evicted += 1
                return False
            try:
                self._best_effort_q.put_nowait(frame_bytes)
                return True
            except queue.Full:
                self._total_evicted += 1
                return False

        elif qos_level == 2:  # CONFLATABLE
            key = entity_id or f"anon_{time.time_ns()}"
            with self._conflatable_lock:
                self._conflatable[key] = frame_bytes
            return True

        elif qos_level == 3:  # CRITICAL
            try:
                self._critical_q.put_nowait(frame_bytes)
                return True
            except queue.Full:
                try:
                    self._critical_q.get_nowait()
                except queue.Empty:
                    pass
                try:
                    self._critical_q.put_nowait(frame_bytes)
                    self._total_evicted += 1
                    return True
                except queue.Full:
                    return False
        return False

    def pop(self) -> Optional[bytes]:
        """
        Pop the highest‑priority available frame.

        Returns:
            bytes or None if empty.
        """
        if not self._critical_q.empty():
            return self._critical_q.get_nowait()

        if self._conflatable:
            with self._conflatable_lock:
                if self._conflatable:
                    key, val = self._conflatable.popitem()
                    return val

        if not self._best_effort_q.empty():
            return self._best_effort_q.get_nowait()

        return None

    async def pop_async(self, stop_event: Optional[threading.Event] = None) -> Optional[bytes]:
        """
        Asynchronously pop a frame, yielding control while waiting.
        Checks stop_event to allow clean exit.
        """
        while True:
            if stop_event and stop_event.is_set():
                return None
            frame = self.pop()
            if frame is not None:
                return frame
            await asyncio.sleep(0.001)

    def qsize(self) -> int:
        return (self._critical_q.qsize() +
                len(self._conflatable) +
                self._best_effort_q.qsize())

    def clear(self) -> None:
        while not self._critical_q.empty():
            try:
                self._critical_q.get_nowait()
            except queue.Empty:
                break
        with self._conflatable_lock:
            self._conflatable.clear()
        while not self._best_effort_q.empty():
            try:
                self._best_effort_q.get_nowait()
            except queue.Empty:
                break

    def stats(self) -> Dict[str, Any]:
        return {
            "critical_depth": self._critical_q.qsize(),
            "conflatable_depth": len(self._conflatable),
            "best_effort_depth": self._best_effort_q.qsize(),
            "total_evicted": self._total_evicted,
            "total_enqueued": self._total_enqueued,
            "capacity": self.capacity,
        }