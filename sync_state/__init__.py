"""
sync_state – deterministic, race‑safe state synchronisation bridge.
"""

from .sync_core import StateSync, StateSyncError, ProviderValidationError
from .qos import QoS, DropPolicy, TypeMetadata, SyncDirection
from .presets import Presets
from .client_socket import StateSyncSocketClient
from .js import get_client_js_content
from .bridge import SyncStateBridge
from .qos_queue import PriorityQoSQueue
from .ring_buffer import DeltaRingBuffer
from .chunking import chunk_snapshot, SnapshotReassembler
from .transports.base import TransportAdapter, TransportMetrics
from .transports.ipc import FramedIPCTransport, DuplexIPCTransport, FRAME_DELTA, FRAME_COMMAND, FRAME_SNAPSHOT_CHUNK, FRAME_COMMAND_ACK
from .transports.disk import DiskPersistenceAdapter
from .router import Router, RouterEntry
from .transports.http_link import HTTPLink
from .supervisor import Supervisor

__all__ = [
    "StateSync",
    "StateSyncError",
    "ProviderValidationError",
    "QoS",
    "DropPolicy",
    "TypeMetadata",
    "SyncDirection",
    "Presets",
    "StateSyncSocketClient",
    "get_client_js_content",
    "SyncStateBridge",
    "PriorityQoSQueue",
    "DeltaRingBuffer",
    "chunk_snapshot",
    "SnapshotReassembler",
    "TransportAdapter",
    "TransportMetrics",
    "FramedIPCTransport",
    "DuplexIPCTransport",
    "FRAME_DELTA",
    "FRAME_COMMAND",
    "FRAME_SNAPSHOT_CHUNK",
    "FRAME_COMMAND_ACK",
    "DiskPersistenceAdapter",
    "Router",
    "RouterEntry",
    "HTTPLink",
    "Supervisor",
]