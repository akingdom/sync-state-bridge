import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class DropPolicy(Enum):
    """Defines how deltas are handled when transport buffers overflow."""
    CRITICAL = "never_drop"          # Guarded: Queued until delivered, never discarded
    CONFLATABLE = "keep_latest"      # Conflated: Drops intermediate deltas in favor of newest
    BEST_EFFORT = "drop_on_overflow" # Discardable: Dropped immediately under queue pressure


@dataclass
class QoS:
    """Quality of Service metadata for entity streams."""
    drop_policy: DropPolicy = DropPolicy.CONFLATABLE
    ideal_cadence_ms: int = 50       # Target refresh interval in ms
    ttl_ms: Optional[int] = 1000     # Time-To-Live in queue before delta expires

    def is_expired(self, created_at_timestamp: float) -> bool:
        """Check if a delta has exceeded its TTL in transport queue."""
        if self.ttl_ms is None or self.drop_policy == DropPolicy.CRITICAL:
            return False
        return (time.time() - created_at_timestamp) * 1000.0 > self.ttl_ms


@dataclass
class TypeMetadata:
    """Registration metadata for snapshot providers."""
    type_name: str
    qos: QoS = QoS()
    max_frame_bytes: int = 1_048_576  # 1MB default chunking threshold
