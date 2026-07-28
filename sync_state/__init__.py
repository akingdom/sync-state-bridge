"""
sync_state – deterministic, race‑safe state synchronisation bridge.
"""

from .sync_core import StateSync, StateSyncError, ProviderValidationError
from .qos import QoS, DropPolicy, TypeMetadata
from .presets import Presets
from .client_socket import StateSyncSocketClient
from .js import get_client_js_content

# New modules (additive)
from .bridge import SyncStateBridge
from .qos_queue import PriorityQoSQueue
from .ring_buffer import DeltaRingBuffer
from .chunking import chunk_snapshot, SnapshotReassembler
from .transports.base import TransportAdapter, TransportMetrics
from .transports.ipc import FramedIPCTransport, FRAME_DELTA, FRAME_COMMAND, FRAME_SNAPSHOT_CHUNK
from .transports.disk import DiskPersistenceAdapter

# Optional
from .supervisor import Supervisor

__all__ = [
    # Existing
    "StateSync",
    "StateSyncError",
    "ProviderValidationError",
    "QoS",
    "DropPolicy",
    "TypeMetadata",
    "Presets",
    "StateSyncSocketClient",
    "get_client_js_content",
    # Bridge
    "SyncStateBridge",
    "PriorityQoSQueue",
    "DeltaRingBuffer",
    "chunk_snapshot",
    "SnapshotReassembler",
    "TransportAdapter",
    "TransportMetrics",
    "FramedIPCTransport",
    "FRAME_DELTA",
    "FRAME_COMMAND",
    "FRAME_SNAPSHOT_CHUNK",
    "DiskPersistenceAdapter",
]