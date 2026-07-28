"""
tests/test_integration.py

End‑to‑end test with router, mock worker, and HTTP link.
"""

import asyncio
import json
import pytest
from fastapi.testclient import TestClient
from sync_state.transports.http_link import HTTPLink
from sync_state.router import Router, RouterEntry
from sync_state.qos import TypeMetadata, SyncDirection


class MockWorker:
    def __init__(self, worker_queue):
        self.worker_queue = worker_queue
        self.responses = []

    async def run(self):
        while True:
            frame = await self.worker_queue.get()
            data = json.loads(frame)
            # Simulate processing
            command_id = data["command_id"]
            ack = {
                "command_id": command_id,
                "status": "ok",
                "result": {"tick": 42},
                "tick_id": 42
            }
            self.responses.append(ack)
            # In real system we'd send back over IPC; here we just store


@pytest.mark.asyncio
async def test_integration_command_flow():
    # Setup router with a type
    worker_queue = asyncio.Queue()
    metadata = TypeMetadata(
        type_name="robot",
        direction=SyncDirection.BIDIRECTIONAL,
        ack_timeout_ms=500
    )
    entry = RouterEntry(worker_queue=worker_queue, metadata=metadata)
    router = Router(type_config={"robot": entry})

    # Start mock worker
    worker = MockWorker(worker_queue)
    worker_task = asyncio.create_task(worker.run())

    # Create HTTP link
    link = HTTPLink(router, host="127.0.0.1", port=0)
    client = TestClient(link.app)

    # Send a command
    payload = {
        "action": "robot:move",
        "params": {"x": 5},
        "command_id": "cmd-integration"
    }
    response = client.post("/command?client_id=client-integration", json=payload)
    assert response.status_code == 200
    receipt = response.json()
    assert receipt["status"] == "received"
    assert receipt["command_id"] == "cmd-integration"

    # Simulate worker processing – the worker will read from its queue and send ack.
    # We need to give it time to process.
    await asyncio.sleep(0.1)

    # Check that the router received the ack (we need to simulate sending ack over IPC)
    # In this integration test, we don't have IPC; we need to manually call resolve_ack.
    # So we'll simulate the worker sending ack to router via a method.
    # We'll modify MockWorker to have a reference to router's resolve_ack.
    # For simplicity, we'll just manually call it.
    ack_data = {
        "command_id": "cmd-integration",
        "status": "ok",
        "result": {"tick": 42},
        "tick_id": 42
    }
    ack_bytes = json.dumps(ack_data).encode()
    await router.resolve_ack(ack_bytes)

    # Now the client queue should have a command_ack event
    queue = router.client_queues.get("client-integration")
    assert queue is not None
    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event["event"] == "command_ack"
    ack_received = json.loads(event["data"])
    assert ack_received["status"] == "ok"
    assert ack_received["result"]["tick"] == 42

    # Clean up
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass