"""
sync_state/transports/base.py

Transport adapter protocol and metrics.
"""

from typing import Protocol, Optional, runtime_checkable
from dataclasses import dataclass

@dataclass(frozen=True)
class TransportMetrics:
    queue_depth: int
    queue_capacity: int
    frames_emitted: int
    frames_dropped: int
    backpressure_events: int

@runtime_checkable
class TransportAdapter(Protocol):
    """
    Contract for all egress transports.
    Must be thread‑safe.
    """

    def emit(self, frame_bytes: bytes, qos_level: int = 1,
             entity_id: Optional[str] = None) -> bool:
        """
        Ingest a frame.

        Returns:
            True if accepted, False if dropped.
        """
        ...

    def get_metrics(self) -> TransportMetrics:
        """Return snapshot telemetry."""
        ...

    def flush(self, timeout: float = 2.0) -> bool:
        """
        Block until all queued frames are written or timeout expires.
        Returns True if all flushed, False if timeout.
        """
        ...

    def close(self) -> None:
        """Gracefully halt and release resources."""
        ...