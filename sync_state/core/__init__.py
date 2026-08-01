"""
sync_state.core – Core components for the synchronization fabric.
"""

from .router import Router, RouterConfigError, RouterUnauthorizedError
from .ipc_transport import IPCTransport, IPCError, read_payload, pack_header
from .state_sync import StateSync, ProviderValidationError, StateSyncError
from .transport_adapter import TransportAdapter
from .synchronisation_kernel import SynchronisationKernel, KernelScheduler, SyncJob
from .sync_bridge import SyncStateBridge, BridgeState, LifecycleError, RegistrationLockedError, KernelNotStartedError
from .governor import Governor

# Constants from IPC (if needed)
try:
    from .ipc_transport import FRAME_DELTA, FRAME_COMMAND
except ImportError:
    FRAME_DELTA = None
    FRAME_COMMAND = None

__all__ = [
    "Router",
    "RouterConfigError",
    "RouterUnauthorizedError",
    "IPCTransport",
    "IPCError",
    "read_payload",
    "pack_header",
    "FRAME_DELTA",
    "FRAME_COMMAND",
    "StateSync",
    "ProviderValidationError",
    "StateSyncError",
    "TransportAdapter",
    "SynchronisationKernel",
    "KernelScheduler",
    "SyncJob",
    "SyncStateBridge",
    "BridgeState",
    "LifecycleError",
    "RegistrationLockedError",
    "KernelNotStartedError",
    "Governor",
]