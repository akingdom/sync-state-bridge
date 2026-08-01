"""
sync-state-bridge – Versioned delta-state synchronization engine.
"""

# Core
from .core.router import Router, RouterConfigError, RouterUnauthorizedError
from .core.ipc_transport import IPCTransport, IPCError, read_payload, pack_header
from .core.state_sync import StateSync, ProviderValidationError, StateSyncError
from .core.transport_adapter import TransportAdapter
from .core.synchronisation_kernel import SynchronisationKernel, KernelScheduler
from .core.sync_bridge import SyncStateBridge, BridgeState, LifecycleError, RegistrationLockedError, KernelNotStartedError
from .core.governor import Governor

# Web helpers
from .web.client_js import get_client_js_content
from .web.http_sse_transport import HTTPSSETransport

# Reliability (optional)
from .reliability.qos import QoS, DropPolicy
from .reliability.qos_queue import PriorityQoSQueue
from .reliability.presets import Presets
from .reliability.ring_buffer import DeltaRingBuffer
from .reliability.chunking import chunk_snapshot, SnapshotReassembler
from .reliability.client_socket import StateSyncSocketClient

__all__ = [
    # Core
    "Router",
    "RouterConfigError",
    "RouterUnauthorizedError",
    "IPCTransport",
    "IPCError",
    "read_payload",
    "pack_header",
    "StateSync",
    "ProviderValidationError",
    "StateSyncError",
    "TransportAdapter",
    "SynchronisationKernel",
    "KernelScheduler",
    "SyncStateBridge",
    "BridgeState",
    "LifecycleError",
    "RegistrationLockedError",
    "KernelNotStartedError",
    "Governor",
    # Web
    "get_client_js_content",
    "HTTPSSETransport",
    # Reliability
    "QoS",
    "DropPolicy",
    "PriorityQoSQueue",
    "Presets",
    "DeltaRingBuffer",
    "chunk_snapshot",
    "SnapshotReassembler",
    "StateSyncSocketClient",
]