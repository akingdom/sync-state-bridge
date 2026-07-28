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

    async def run(self, router):
        while True:
            frame = await self.worker_queue.get()
            data = json.loads(frame)
            command_id = data["command_id"]
            ack = {
                "command_id": command_id,
                "status": "ok",
                "result": {"tick": 42},
                "tick_id": 42
            }
            self.responses.append(ack)
            # Send ack back via router
            await router.resolve_ack(json.dumps(ack).encode())


@pytest.mark.asyncio
async def test_integration_command_flow():
    worker_queue = asyncio.Queue()
    metadata = TypeMetadata(
        type_name="robot",
        direction=SyncDirection.BIDIRECTIONAL,
        ack_timeout_ms=500
    )
    entry = RouterEntry(worker_queue=worker_queue, metadata=metadata)
    router = Router(type_config={"robot": entry})

    worker = MockWorker(worker_queue)
    worker_task = asyncio.create_task(worker.run(router))

    link = HTTPLink(router, host="127.0.0.1", port=0)
    client = TestClient(link.app)

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

    # Wait for worker to process and send ack
    await asyncio.sleep(0.1)

    # Check that ack was forwarded to client queue
    queue = router.client_queues.get("client-integration")
    assert queue is not None
    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event["event"] == "command_ack"
    ack_received = json.loads(event["data"])
    assert ack_received["status"] == "ok"
    assert ack_received["result"]["tick"] == 42

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass