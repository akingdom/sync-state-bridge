"""
sync_state/core/sync_bridge.py

Lifecycle-aware SyncStateBridge enforcing static registration,
registration locking, and operational state gating.
"""

import time
import threading
import logging
from enum import Enum, auto
from typing import List, Dict, Any, Optional, Callable, Set

from .router import Router
from .synchronisation_kernel import SynchronisationKernel
from .governor import Governor

logger = logging.getLogger("sync_state.bridge")


class BridgeState(Enum):
    UNINITIALIZED = auto()
    REGISTERING = auto()
    BOOTSTRAPPING = auto()
    OPERATIONAL = auto()
    SHUTDOWN = auto()


class LifecycleError(Exception):
    pass


class RegistrationLockedError(LifecycleError):
    pass


class KernelNotStartedError(LifecycleError):
    pass


class SyncStateBridge:
    """
    Orchestrator enforcing strict static registration, system boot sequencing,
    and active execution gating.
    """

    def __init__(self, kernel_capacity: int = 2000):
        self.state = BridgeState.UNINITIALIZED
        self.router = Router()
        self.kernel = SynchronisationKernel(capacity=kernel_capacity)   # <-- added
        self.governor = Governor()
        self.governor.attach(self.kernel)                              # <-- now works
        self._transports: Dict[object, Dict[str, Any]] = {}
        self._on_ready_callbacks: List[Callable[[], None]] = []
        self._lock = threading.Lock()

        self.state = BridgeState.REGISTERING
        logger.info("SyncStateBridge initialized. State: REGISTERING")

    # ------------------------------------------------------------------------
    # Registration Phase
    # ------------------------------------------------------------------------

    def register_transport(
        self,
        transport: object,
        type_list: List[str],
        direction: str,
        qos_map: Optional[Dict[str, int]] = None,
    ) -> None:
        """
        Register a transport with the bridge.

        Args:
            transport: Transport adapter (must have `emit`, `on_frame`, `start` methods).
            type_list: List of type names this transport handles.
            direction: "in", "out", or "both".
            qos_map: Mapping of type names to QoS levels (1,2,3) for outgoing frames.
        """
        with self._lock:
            if self.state not in (BridgeState.REGISTERING, BridgeState.BOOTSTRAPPING):
                raise RegistrationLockedError(
                    f"Cannot register transport in state {self.state.name}."
                )

            # Register with Router for routing decisions
            self.router.register_transport(transport, type_list, direction)

            # Register with Kernel if outgoing
            if "out" in direction:
                self.kernel.register_transport(transport, qos_map or {})

            self._transports[transport] = {
                "types": type_list,
                "direction": direction,
                "qos_map": qos_map or {},
            }
            logger.info(f"Registered transport {transport} for types {type_list} direction {direction}")

    def on_ready(self, callback: Callable[[], None]) -> None:
        """Register a callback to be called when the bridge transitions to OPERATIONAL."""
        with self._lock:
            if self.state == BridgeState.OPERATIONAL:
                callback()
            else:
                self._on_ready_callbacks.append(callback)

    # ------------------------------------------------------------------------
    # Boot Sequence
    # ------------------------------------------------------------------------

    def start(self) -> None:
        """
        Transition system through BOOTSTRAPPING to OPERATIONAL.
        Locks registration and enables data processing.
        """
        with self._lock:
            if self.state == BridgeState.OPERATIONAL:
                logger.warning("start() called on already operational bridge.")
                return

            self.state = BridgeState.BOOTSTRAPPING
            logger.info("State: BOOTSTRAPPING. Verifying %d transports...", len(self._transports))

            # Start the kernel
            self.kernel.start()

            # Transition to OPERATIONAL
            self.state = BridgeState.OPERATIONAL
            logger.info("State: OPERATIONAL. Registration locked. Processing enabled.")

            # Fire callbacks
            for cb in self._on_ready_callbacks:
                try:
                    cb()
                except Exception as e:
                    logger.error("Error in on_ready callback: %s", e)

    # ------------------------------------------------------------------------
    # Runtime Frame Submission
    # ------------------------------------------------------------------------

    def submit(self, frame: Dict[str, Any], source_transport: object) -> None:
        """
        Submit a frame to the system.

        Args:
            frame: The frame to deliver (must contain `type` and optionally `id`).
            source_transport: The transport that originated the frame.
        """
        if self.state != BridgeState.OPERATIONAL:
            raise KernelNotStartedError(
                f"Cannot submit frame. Bridge in state: {self.state.name}"
            )

        # Determine target transports using the Router
        target_transports = self.router.route(frame, source_transport)
        if not target_transports:
            return

        # Pass to Kernel for scheduling and delivery
        self.kernel.submit(frame, target_transports)

    # ------------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------------

    def close(self) -> None:
        """Shut down the bridge and all components."""
        self.kernel.stop()
        with self._lock:
            self.state = BridgeState.SHUTDOWN
            logger.info("SyncStateBridge shut down.")