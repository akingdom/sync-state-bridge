"""
sync_state/bridge.py

SyncStateBridge: orchestrator wrapping StateSync, handling commands,
and pumping deltas into transports via PriorityQoSQueue.
Extended with command handling and fault flag.
"""

import time
import json
import queue
import logging
from typing import List, Dict, Any, Callable, Optional

from .sync_core import StateSync
from .qos_queue import PriorityQoSQueue
from .transports.base import TransportAdapter
from .chunking import chunk_snapshot

logger = logging.getLogger("sync_state.bridge")


class SyncStateBridge:
    """
    Orchestrator that wraps StateSync and manages transport emission.
    It also handles command ingestion (from UI or gateway) and can emit
    full snapshots using chunking.
    """

    def __init__(self, state_sync: Optional[StateSync] = None,
                 command_queue_maxsize: int = 1000):
        self.state_sync = state_sync or StateSync()
        self.current_tick = 0
        self._transports: List[TransportAdapter] = []
        self._pending_deltas: List[Dict[str, Any]] = []
        self._command_queue = queue.Queue(maxsize=command_queue_maxsize)
        self._faulted = False
        self._command_handler: Optional[Callable[[str, dict], dict]] = None
        self._fault_handlers: List[Callable[[str], None]] = []

    def register_transport(self, transport: TransportAdapter) -> None:
        """Add an egress transport."""
        self._transports.append(transport)

    def register_command_handler(self, handler: Callable[[str, dict], dict]) -> None:
        """Set the function that processes commands: receives (action, params) -> returns result dict."""
        self._command_handler = handler

    def register_fault_handler(self, handler: Callable[[str], None]) -> None:
        """Register a callback for local fault detection."""
        self._fault_handlers.append(handler)

    def set_fault(self, message: str) -> None:
        """Trigger a fault, preventing further commits."""
        self._faulted = True
        logger.critical("Fault triggered: %s", message)
        for handler in self._fault_handlers:
            try:
                handler(message)
            except Exception as e:
                logger.error("Fault handler failed: %s", e)

    def handle_command(self, action: str, params: dict) -> dict:
        """Synchronous command dispatcher. Raises if no handler or faulted."""
        if self._faulted:
            raise RuntimeError("Bridge is faulted; commands not accepted")
        if self._command_handler is None:
            raise RuntimeError("No command handler registered")
        try:
            result = self._command_handler(action, params)
        except Exception as e:
            logger.exception("Command handler error")
            return {"error": str(e)}
        return result

    def track_change(self, entity_id: str, op: str, changes: Dict[str, Any]) -> None:
        """
        Record a mutation for the next commit.

        op: "add", "update", or "delete"
        """
        self._pending_deltas.append({
            "id": entity_id,
            "op": op,
            "changes": changes,
        })

    def push_command(self, action: str, params: Optional[Dict] = None,
                     cmd_seq: int = 0) -> bool:
        """
        Push an external command (e.g., from HTTP API) into the command queue.

        Returns True if accepted, False if queue is full.
        """
        try:
            self._command_queue.put_nowait({
                "cmd_seq": cmd_seq,
                "timestamp": time.time(),
                "action": action,
                "params": params or {}
            })
            return True
        except queue.Full:
            logger.warning("Command queue saturated; dropped command: %s", action)
            return False

    def process_pending_commands(self, handler: Callable[[str, Dict], None]) -> None:
        """
        Drain the command queue and invoke handler for each command.
        Should be called before each simulation tick.
        """
        while not self._command_queue.empty():
            try:
                cmd = self._command_queue.get_nowait()
                handler(cmd["action"], cmd["params"])
                self._command_queue.task_done()
            except queue.Empty:
                break

    def emit_full_snapshot(self, snapshot_id: str, state_data: Dict[str, Any]) -> None:
        """
        Chunk and transmit a full snapshot to all registered transports.
        """
        chunks = chunk_snapshot(state_data, snapshot_id)
        for chunk in chunks:
            encoded = (json.dumps(chunk) + "\n").encode('utf-8')
            for transport in self._transports:
                transport.emit(encoded, qos_level=3, entity_id=snapshot_id)

    def commit_tick(self, tick_id: int) -> None:
        """
        Commit the current tick: flush pending deltas to the transports.
        Raises RuntimeError if faulted.
        """
        if self._faulted:
            raise RuntimeError("Bridge is faulted; cannot commit")
        self.current_tick = tick_id

        if not self._pending_deltas:
            return

        payload = {
            "type": "DELTA",
            "tick": self.current_tick,
            "timestamp": time.time(),
            "deltas": list(self._pending_deltas),
        }
        self._pending_deltas.clear()

        try:
            encoded_bytes = (json.dumps(payload) + "\n").encode('utf-8')
        except Exception as e:
            logger.error("Failed to serialize tick payload: %s", e)
            return

        for transport in self._transports:
            try:
                transport.emit(encoded_bytes, qos_level=2)  # conflatable
            except Exception as e:
                logger.error("Transport emit error in %s: %s",
                             transport.__class__.__name__, e)

    def close(self) -> None:
        """Shut down all transports."""
        for transport in self._transports:
            try:
                transport.close()
            except Exception as e:
                logger.error("Error closing transport: %s", e)