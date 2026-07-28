import asyncio
import json
import pytest
from fastapi.testclient import TestClient
from sync_state.transports.http_link import HTTPLink


class MockRouter:
    """Minimal router for testing HTTPLink."""
    def __init__(self):
        self.client_queues = {}
        self.call_count = 0
        self.last_frame = None
        self.last_client = None

    def get_client_queue(self, client_id):
        if client_id not in self.client_queues:
            self.client_queues[client_id] = asyncio.Queue()
        return self.client_queues[client_id]

    async def handle_frame_from_http(self, client_id, frame_bytes):
        self.call_count += 1
        self.last_client = client_id
        self.last_frame = frame_bytes
        data = json.loads(frame_bytes)
        cmd_id = data.get("command_id", "test-123")
        return {"status": "received", "command_id": cmd_id}


@pytest.fixture
def http_link_with_mock_router():
    mock = MockRouter()
    link = HTTPLink(mock, host="127.0.0.1", port=0)
    return link, mock


def test_http_link_post_command(http_link_with_mock_router):
    link, mock_router = http_link_with_mock_router
    client = TestClient(link.app)

    payload = {
        "action": "robot:move",
        "params": {"x": 10},
        "command_id": "cmd-http"
    }
    response = client.post("/command?client_id=client-http", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "received"
    assert data["command_id"] == "cmd-http"

    assert mock_router.call_count == 1
    assert mock_router.last_client == "client-http"
    frame = json.loads(mock_router.last_frame)
    assert frame["action"] == "robot:move"
    assert frame["params"]["x"] == 10
    assert frame["command_id"] == "cmd-http"


def test_http_link_post_command_missing_action(http_link_with_mock_router):
    link, _ = http_link_with_mock_router
    client = TestClient(link.app)

    payload = {"params": {"x": 10}}  # missing action
    response = client.post("/command?client_id=client-http", json=payload)
    assert response.status_code == 400
    assert "Missing 'action'" in response.text


def test_http_link_stream_events(http_link_with_mock_router):
    link, _ = http_link_with_mock_router
    # Verify the stream route is registered; do not call it to avoid hanging
    routes = [r for r in link.app.routes if r.path == "/stream"]
    assert len(routes) == 1
    assert "GET" in routes[0].methods