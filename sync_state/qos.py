"""
sync_state/qos.py

Quality of Service definitions for entity streams.
Extended with SyncDirection and new TypeMetadata fields.
"""

import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Callable


class DropPolicy(Enum):
    CRITICAL = "never_drop"
    CONFLATABLE = "keep_latest"
    BEST_EFFORT = "drop_on_overflow"


@dataclass
class QoS:
    drop_policy: DropPolicy = DropPolicy.CONFLATABLE
    ideal_cadence_ms: int = 50
    ttl_ms: Optional[int] = 1000

    def is_expired(self, created_at_timestamp: float) -> bool:
        if self.ttl_ms is None or self.drop_policy == DropPolicy.CRITICAL:
            return False
        return (time.time() - created_at_timestamp) * 1000.0 > self.ttl_ms


# ---------- NEW ----------
class SyncDirection(Enum):
    UNIDIRECTIONAL = "ro"
    BIDIRECTIONAL = "rw"


@dataclass
class TypeMetadata:
    type_name: str
    qos: QoS = QoS()
    max_frame_bytes: int = 1_048_576
    direction: SyncDirection = SyncDirection.UNIDIRECTIONAL
    ack_timeout_ms: Optional[int] = None          # None = no ack; 0 = immediate fault
    fault_handler: Optional[Callable[[str, dict], None]] = None  # (client_id, context)