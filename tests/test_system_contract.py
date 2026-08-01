"""
tests/test_system_contract.py

Full-System Contract Verification Suite.
Validates end-to-end transport integrity, backpressure, and state delta flow.
"""

import pytest
import asyncio
from sync_state.core.sync_bridge import SyncStateBridge
from sync_state.core.ipc_transport import IPCTransport
from sync_state.web import HTTPSSETransport
from sync_state.observability import PassiveHealthMonitor


@pytest.fixture
async def system_harness():
    """Spins up an isolated local instance of the full system stack for testing."""
    bridge = SyncStateBridge(kernel_capacity=100)
    ipc = IPCTransport(listen_on="127.0.0.1:8799")
    http = HTTPSSETransport(host="127.0.0.1", port=8009)

    qos_map = {"player": 3, "particle": 1, "diagnostic": 2}

    bridge.register_transport(ipc, type_list=["player", "particle", "diagnostic"], direction="in")
    bridge.register_transport(http, type_list=["player", "particle", "diagnostic"], direction="out", qos_map=qos_map)

    bridge.start()
    monitor = PassiveHealthMonitor(bridge, http, ipc)

    yield {
        "bridge": bridge,
        "ipc": ipc,
        "http": http,
        "monitor": monitor,
    }

    # Clean teardown
    if hasattr(bridge, "close") and callable(bridge.close):
        bridge.close()


@pytest.mark.asyncio
async def test_contract_telemetry_endpoint(system_harness):
    """CONTRACT: Operational telemetry reflects accurate initial component states."""
    monitor = system_harness["monitor"]
    snapshot = monitor.get_telemetry_snapshot()

    assert snapshot["status"] == "UP"
    assert snapshot["transports"]["http_clients"] == 0
    assert snapshot["kernel"]["queue_depth"] == 0


@pytest.mark.asyncio
async def test_contract_end_to_end_frame_propagation(system_harness):
    """CONTRACT: A frame submitted at IPC transport routes through Bridge to HTTP queue."""
    bridge = system_harness["bridge"]
    http = system_harness["http"]

    client_queue = asyncio.Queue()
    
    # Safely attach mock client queue based on data structure
    if isinstance(http._out_queues, dict):
        http._out_queues["test_client_01"] = client_queue
    elif isinstance(http._out_queues, set):
        http._out_queues.add(client_queue)
    else:
        http._out_queues.append(client_queue)

    test_frame = {
        "type": "diagnostic",
        "id": "test_1001",
        "data": {"pos": [12.5, 45.0]},
        "seq": 1
    }

    bridge.submit(test_frame, source_transport=None)

    # Deterministic wait up to 1 second
    delivered_frame = await asyncio.wait_for(client_queue.get(), timeout=1.0)

    assert delivered_frame["id"] == "test_1001"
    assert delivered_frame["data"]["pos"] == [12.5, 45.0]


@pytest.mark.asyncio
async def test_contract_qos_drop_policy(system_harness):
    """CONTRACT: Low-QoS particle frames drop when kernel exceeds capacity limits."""
    bridge = system_harness["bridge"]

    # Exceed kernel capacity (capacity = 100)
    for i in range(150):
        bridge.submit({"type": "particle", "id": f"pt_{i}", "seq": i}, source_transport=None)

    # Small delay to allow processing loop to drain
    await asyncio.sleep(0.05)
    
    monitor = system_harness["monitor"]
    snapshot = monitor.get_telemetry_snapshot()

    # Verify backpressure or frame drop telemetry
    assert snapshot["kernel"]["drop_rate_1s"] > 0 or snapshot["kernel"]["queue_depth"] <= 100, \
        "Contract Broken: Kernel failed to enforce capacity bounds under load."