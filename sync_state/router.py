"""
sync_state/router.py

Static message switch: routes frames to workers and clients,
with timeout monitoring and access control.
Zero‑copy forwarding of bytes objects.
"""

import asyncio
import json
import time
import uuid
import logging
from typing import Dict, Optional, Callable

from .qos import TypeMetadata, SyncDirection
from .transports.ipc import (
    FRAME_COMMAND, FRAME_COMMAND_ACK, FRAME_DELTA, FRAME_SNAPSHOT_CHUNK,
    DuplexIPCTransport
)

logger = logging.getLogger("sync_state.router")


class RouterEntry:
    """Static configuration for a type."""
    __slots__ = ("worker_queue", "metadata")
    def __init__(self, worker_queue: asyncio.Queue, metadata: TypeMetadata):
        self.worker_queue = worker_queue
        self.metadata = metadata


class Router:
    """
    Static router: holds a fixed routing table (type → worker queue)
    and a type metadata table. Forwards frames to workers or broadcasts to clients.
    Monitors pending commands and triggers faults on timeout.
    """

    def __init__(self, type_config: Dict[str, RouterEntry]):
        """
        type_config: mapping from type_name to RouterEntry.
        """
        self.type_config = type_config
        self.client_queues: Dict[str, asyncio.Queue] = {}        # client_id -> queue
        self.pending_futures: Dict[str, asyncio.Future] = {}     # command_id -> Future
        self.pending_client: Dict[str, str] = {}                 # command_id -> client_id
        self.pending_action: Dict[str, str] = {}                 # command_id -> action
        self._loop = asyncio.get_event_loop()

    def get_client_queue(self, client_id: str) -> asyncio.Queue:
        """Retrieve or create a client queue."""
        if client_id not in self.client_queues:
            self.client_queues[client_id] = asyncio.Queue()
        return self.client_queues[client_id]

    async def handle_frame_from_http(self, client_id: str, frame_bytes: bytes) -> dict:
        """
        Called by http_link when a POST /command arrives.
        Returns the immediate receipt dict.
        """
        # Parse the frame to extract metadata
        payload = json.loads(frame_bytes.decode())  # assumes JSON
        action = payload.get("action")
        if not action:
            raise ValueError("Missing 'action'")

        type_name = action.split(":", 1)[0] if ":" in action else action
        entry = self.type_config.get(type_name)
        if not entry:
            raise ValueError(f"Unknown type: {type_name}")
        metadata = entry.metadata

        if metadata.direction == SyncDirection.UNIDIRECTIONAL:
            raise PermissionError(f"Type {type_name} is read‑only")

        command_id = payload.get("command_id") or str(uuid.uuid4())

        # Set up pending future if ack required
        if metadata.ack_timeout_ms is not None:
            future = asyncio.Future()
            self.pending_futures[command_id] = future
            self.pending_client[command_id] = client_id
            self.pending_action[command_id] = action
            # Start timeout monitor
            self._loop.create_task(self._monitor_ack(
                command_id,
                metadata.ack_timeout_ms,
                client_id,
                action,
                metadata.fault_handler
            ))
        else:
            # No ack needed: no future
            self.pending_futures.pop(command_id, None)

        # Forward the original frame bytes to the worker queue
        await entry.worker_queue.put(frame_bytes)

        return {"status": "received", "command_id": command_id}

    async def _monitor_ack(self, command_id: str, timeout_ms: int,
                           client_id: str, action: str,
                           fault_handler: Optional[Callable]):
        await asyncio.sleep(timeout_ms / 1000.0)
        future = self.pending_futures.get(command_id)
        if future is not None and not future.done():
            # Timeout triggered
            self.pending_futures.pop(command_id, None)
            self.pending_client.pop(command_id, None)
            self.pending_action.pop(command_id, None)
            logger.error("ACK timeout for command %s (client %s)", command_id, client_id)
            # Invoke fault handler if provided
            if fault_handler:
                try:
                    fault_handler(client_id, {"command_id": command_id, "action": action})
                except Exception as e:
                    logger.exception("Fault handler error")
            # Close client connection (send fault SSE)
            queue = self.client_queues.get(client_id)
            if queue:
                await queue.put({
                    "event": "error",
                    "data": json.dumps({"fault": True, "command_id": command_id})
                })
            # Optionally, we could close the queue (but we keep it open for future resync)

    async def resolve_ack(self, ack_payload: bytes):
        """
        Called when a FRAME_COMMAND_ACK arrives from the worker.
        Resolves the pending future and forwards the ack to the client queue.
        """
        data = json.loads(ack_payload)
        cid = data.get("command_id")
        if not cid:
            return
        future = self.pending_futures.pop(cid, None)
        if future is not None and not future.done():
            future.set_result(data)
        client_id = self.pending_client.pop(cid, None)
        if client_id:
            queue = self.client_queues.get(client_id)
            if queue:
                # Forward as SSE command_ack
                await queue.put({
                    "event": "command_ack",
                    "data": json.dumps(data)
                })
        # Clean up action mapping
        self.pending_action.pop(cid, None)

    async def broadcast_delta(self, delta_payload: bytes):
        """Broadcast a FRAME_DELTA to all connected clients."""
        for client_id, queue in self.client_queues.items():
            await queue.put({
                "event": "delta",
                "data": delta_payload.decode()
            })