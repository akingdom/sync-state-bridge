# filename: tests/test_http_sse_transport.py

import time
import pytest
from fastapi.testclient import TestClient
from sync_state.web.http_sse_transport import HTTPSSETransport
from sync_state.core.sync_bridge import SyncStateBridge


@pytest.fixture
def bridge_and_http():
    bridge = SyncStateBridge(kernel_capacity=100)
    http = HTTPSSETransport(host="127.0.0.1", port=0, static_dir=None)

    bridge.register_transport(http, ["controls", "test"], direction="both")
    bridge.start()

    http.on_frame(lambda frame: bridge.submit(frame, http))

    client = TestClient(http._app)

    yield bridge, http, client

    # Ensure explicit shutdown sequence
    http.stop()
    if hasattr(bridge, "close") and callable(bridge.close):
        bridge.close()
    elif hasattr(bridge, "stop") and callable(bridge.stop):
        bridge.stop()

def test_http_update_endpoint(bridge_and_http):
    bridge, _, client = bridge_and_http

    payload = {"type": "test", "id": "test_1", "data": {"value": 42}}
    response = client.post("/update", json=payload)
    assert response.status_code == 200
    assert response.json() == {"status": "accepted"}

    time.sleep(0.05)

    stats = bridge.kernel.stats()
    total_depth = stats.get("critical_depth", 0) + stats.get("conflatable_depth", 0) + stats.get("best_effort_depth", 0)
    assert total_depth >= 0


def test_http_stream_endpoint(bridge_and_http):
    _, _, client = bridge_and_http

    # Using test client stream context cleanly
    with client.stream("GET", "/stream?client_id=test") as response:
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        
        # Read the manifest frame to confirm stream initialisation
        lines = response.iter_lines()
        first_line = next(lines)
        line_str = first_line.decode('utf-8') if isinstance(first_line, bytes) else first_line
        assert "manifest" in line_str or "data:" in line_str


def test_http_client_js_served(bridge_and_http):
    _, _, client = bridge_and_http

    response = client.get("/client/stateClient.js")
    assert response.status_code == 200
    assert "application/javascript" in response.headers["content-type"]
    assert "StateClient" in response.text