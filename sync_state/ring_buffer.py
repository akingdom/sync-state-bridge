"""
sync_state/ring_buffer.py

Fixed‑capacity ring buffer for caching deltas by tick ID.
"""

from collections import deque
from typing import Dict, Optional, List

class DeltaRingBuffer:
    """
    Stores recent tick payloads for fast SSE reconnection.

    Capacity: number of ticks to retain.
    Max Bytes: maximum total buffer size available
    """

    def __init__(self, capacity: int = 500, max_bytes: int = 100 * 1024 * 1024):
        self.capacity = capacity
        self.max_bytes = max_bytes
        self._total_bytes = 0
        self._buffer = deque(maxlen=capacity)  # list of (tick_id, payload)
        self._lookup: Dict[int, str] = {}

    def append(self, tick_id: int, payload: str):
        payload_size = len(payload.encode('utf-8'))
        if len(self._buffer) == self.capacity:
            oldest_tick, oldest_payload = self._buffer[0]
            self._total_bytes -= len(oldest_payload.encode('utf-8'))
            self._lookup.pop(oldest_tick, None)
        # If adding this payload would exceed max_bytes, drop the oldest until it fits
        while self._total_bytes + payload_size > self.max_bytes and self._buffer:
            oldest_tick, oldest_payload = self._buffer.popleft()
            self._total_bytes -= len(oldest_payload.encode('utf-8'))
            self._lookup.pop(oldest_tick, None)
        self._buffer.append((tick_id, payload))
        self._lookup[tick_id] = payload
        self._total_bytes += payload_size

    def get_missed_deltas(self, last_tick_id: int) -> Optional[List[str]]:
        """
        Retrieve all deltas after `last_tick_id` if it exists in buffer.

        Returns:
            List of payload strings, or None if `last_tick_id` not found.
        """
        if last_tick_id not in self._lookup:
            return None
        missed = []
        capture = False
        for tid, pl in self._buffer:
            if capture:
                missed.append(pl)
            elif tid == last_tick_id:
                capture = True
        return missed

    def latest_tick_id(self) -> Optional[int]:
        """Return the tick ID of the most recent stored delta."""
        if self._buffer:
            return self._buffer[-1][0]
        return None

    def clear(self) -> None:
        """Empty the buffer."""
        self._buffer.clear()
        self._lookup.clear()