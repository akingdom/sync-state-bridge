import asyncio
import json
import pytest
from sync_state.router import Router, RouterEntry
from sync_state.qos import TypeMetadata, SyncDirection, QoS


class MockWorkerQueue:
    def __init__(self):
        self.queue = asyncio.Queue()

    async def put(self, item):
        await self.queue.put(item)

    async def get(self):
        return await self.queue.get()


@pytest.fixture
def router_and_worker():
    """Return a router with a single type 'robot', and the worker queue."""
    worker_q = MockWorkerQueue()
    metadata = TypeMetadata(
        type_name="robot",
        direction=SyncDirection.BIDIRECTIONAL,
        ack_timeout_ms=100,
        fault_handler=None
    )
    entry = RouterEntry(worker_queue=worker_q, metadata=metadata)
    router = Router(type_config={"robot": entry})
    return router, worker_q


@pytest.fixture
def router_readonly():
    """Router with a read‑only type."""
    worker_q = MockWorkerQueue()
    metadata = TypeMetadata(
        type_name="sensor",
        direction=SyncDirection.UNIDIRECTIONAL,
        ack_timeout_ms=None
    )
    entry = RouterEntry(worker_queue=worker_q, metadata=metadata)
    router = Router(type_config={"sensor": entry})
    return router, worker_q


@pytest.mark.asyncio
async def test_router_routes_to_correct_worker(router_and_worker):
    router, worker_q = router_and_worker
    client_id = "client-123"
    frame = json.dumps({
        "action": "robot:move",
        "params": {"x": 5},
        "command_id": "cmd-1"
    }).encode() + b"\n"

    receipt = await router.handle_frame_from_http(client_id, frame)
    assert receipt["status"] == "received"
    assert receipt["command_id"] == "cmd-1"

    # Worker should receive the frame with injected client_id
    received = await worker_q.get()
    received_data = json.loads(received)
    assert received_data["action"] == "robot:move"
    assert received_data["params"]["x"] == 5
    assert received_data["command_id"] == "cmd-1"
    assert received_data["client_id"] == "client-123"


@pytest.mark.asyncio
async def test_router_rejects_readonly(router_readonly):
    router, _ = router_readonly
    client_id = "client-123"
    frame = json.dumps({
        "action": "sensor:read",
        "params": {},
        "command_id": "cmd-2"
    }).encode() + b"\n"

    with pytest.raises(PermissionError, match="read‑only"):
        await router.handle_frame_from_http(client_id, frame)


@pytest.mark.asyncio
async def test_router_timeout_triggers_fault(router_and_worker):
    router, _ = router_and_worker
    fault_triggered = False

    def fault_handler(client_id, context):
        nonlocal fault_triggered
        fault_triggered = True
        assert client_id == "client-456"
        assert context["command_id"] == "cmd-3"

    entry = router.type_config["robot"]
    entry.metadata.fault_handler = fault_handler
    entry.metadata.ack_timeout_ms = 50

    client_id = "client-456"
    frame = json.dumps({
        "action": "robot:move",
        "params": {"x": 1},
        "command_id": "cmd-3"
    }).encode() + b"\n"

    # Ensure client queue exists
    router.get_client_queue(client_id)

    receipt = await router.handle_frame_from_http(client_id, frame)
    assert receipt["status"] == "received"

    # Wait for timeout (50ms + margin)
    await asyncio.sleep(0.15)

    assert fault_triggered is True

    # Check that error event was put into client queue
    queue = router.client_queues[client_id]
    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event["event"] == "error"
    error_data = json.loads(event["data"])
    assert error_data["fault"] is True
    assert error_data["command_id"] == "cmd-3"


@pytest.mark.asyncio
async def test_router_resolves_ack_and_forwards(router_and_worker):
    router, worker_q = router_and_worker
    client_id = "client-789"
    router.get_client_queue(client_id)

    frame = json.dumps({
        "action": "robot:move",
        "params": {"x": 2},
        "command_id": "cmd-4"
    }).encode() + b"\n"

    receipt = await router.handle_frame_from_http(client_id, frame)
    assert receipt["command_id"] == "cmd-4"

    # Simulate worker sending ack
    ack_payload = json.dumps({
        "command_id": "cmd-4",
        "status": "ok",
        "result": {"tick": 123},
        "tick_id": 123
    }).encode()

    await router.resolve_ack(ack_payload)

    queue = router.client_queues[client_id]
    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event["event"] == "command_ack"
    data = json.loads(event["data"])
    assert data["command_id"] == "cmd-4"
    assert data["status"] == "ok"
    assert data["result"]["tick"] == 123

    assert "cmd-4" not in router.pending_futures
    assert "cmd-4" not in router.pending_client


@pytest.mark.asyncio
async def test_router_idempotency(router_and_worker):
    # Router is stateless; idempotency is on worker side.
    # We test that router forwards duplicates without interference.
    router, worker_q = router_and_worker
    client_id = "client-dup"
    frame = json.dumps({
        "action": "robot:move",
        "params": {"x": 3},
        "command_id": "cmd-dup"
    }).encode() + b"\n"

    receipt1 = await router.handle_frame_from_http(client_id, frame)
    receipt2 = await router.handle_frame_from_http(client_id, frame)
    assert receipt1["command_id"] == "cmd-dup"
    assert receipt2["command_id"] == "cmd-dup"

    item1 = await worker_q.get()
    item2 = await worker_q.get()
    data1 = json.loads(item1)
    data2 = json.loads(item2)
    assert data1["command_id"] == "cmd-dup"
    assert data2["command_id"] == "cmd-dup"