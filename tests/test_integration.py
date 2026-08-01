# filename: tests/test_integration.py

import asyncio
import threading
import time
import pytest
from fastapi.testclient import TestClient
from sync_state.core.sync_bridge import SyncStateBridge
from sync_state.core.ipc_transport import IPCTransport
from sync_state.web.http_sse_transport import HTTPSSETransport


@pytest.fixture
async def full_system():
    bridge = SyncStateBridge(kernel_capacity=100)
    ipc = IPCTransport(listen_on="127.0.0.1:8767")
    http = HTTPSSETransport(host="127.0.0.1", port=0, static_dir=None)

    qos_map = {"player": 3, "particle": 1, "controls": 2}

    bridge.register_transport(ipc, ["player", "particle"], direction="in")
    bridge.register_transport(ipc, ["controls"], direction="out", qos_map=qos_map)
    bridge.register_transport(http, ["controls"], direction="in")
    bridge.register_transport(http, ["player", "particle"], direction="out", qos_map=qos_map)

    bridge.start()
    await ipc.start()

    async def stats_endpoint():
        return {"governor": bridge.governor.evaluate(current_queue_depth=0)}

    http.add_route("/stats", stats_endpoint)
    http.finalize()

    http.start()

    timeout = 5.0
    start_time = time.time()
    while not http._running and time.time() - start_time < timeout:
        await asyncio.sleep(0.01)

    http.on_frame(lambda frame: bridge.submit(frame, http))
    ipc.on_frame(lambda frame: bridge.submit(frame, ipc))

    yield bridge, ipc, http

    # Cleanup sequence
    http.stop()
    await ipc.close()
    bridge.close()


@pytest.mark.asyncio
async def test_full_pipeline_frame_flow(full_system):
    bridge, ipc, http = full_system

    client_queue = asyncio.Queue()
    http._out_queues.add(client_queue)

    with TestClient(http._app) as test_client:
        payload = {"type": "controls", "id": "player_0", "data": {"up": True}}
        response = test_client.post("/update", json=payload)
        assert response.status_code == 200

    worker_frame = {"type": "player", "id": "player_0", "pos": [100, 200]}
    bridge.submit(worker_frame, source_transport=None)

    delivered = await asyncio.wait_for(client_queue.get(), timeout=1.0)
    assert delivered["type"] == "player"
    assert delivered["pos"] == [100, 200]


@pytest.mark.asyncio
async def test_governor_stats_endpoint(full_system):
    _, _, http = full_system

    with TestClient(http._app) as client:
        response = client.get("/stats")
        assert response.status_code == 200
        data = response.json()
        assert "governor" in data
        assert "health" in data["governor"]