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
    """

    def __init__(self, capacity: int = 500):
        self.capacity = capacity
        self._buffer = deque(maxlen=capacity)  # list of (tick_id, payload)
        self._lookup: Dict[int, str] = {}

    def append(self, tick_id: int, payload: str) -> None:
        """Store a tick payload."""
        if len(self._buffer) == self.capacity:
            oldest_tick, _ = self._buffer[0]
            self._lookup.pop(oldest_tick, None)
        self._buffer.append((tick_id, payload))
        self._lookup[tick_id] = payload

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