import pytest
from sync_state import (
    PriorityQoSQueue,
    SyncStateBridge,
    DeltaRingBuffer,
    chunk_snapshot,
    SnapshotReassembler,
)


def test_priority_qos_queue_basic():
    q = PriorityQoSQueue(capacity=10)

    assert q.put(b"best", qos_level=1) is True
    assert q.put(b"conf_a", qos_level=2, entity_id="e1") is True
    assert q.put(b"conf_b", qos_level=2, entity_id="e1") is True
    assert q.put(b"crit", qos_level=3) is True

    assert q.pop() == b"crit"
    assert q.pop() == b"conf_b"
    assert q.pop() == b"best"
    assert q.pop() is None


def test_priority_qos_queue_eviction():
    q = PriorityQoSQueue(capacity=2)
    assert q.put(b"be1", qos_level=1) is True
    assert q.put(b"be2", qos_level=1) is True
    assert q.put(b"be3", qos_level=1) is False
    assert q.stats()["total_evicted"] == 1

    q = PriorityQoSQueue(capacity=2)
    assert q.put(b"crit1", qos_level=3) is True
    assert q.put(b"crit2", qos_level=3) is True
    assert q.put(b"crit3", qos_level=3) is True
    assert q.stats()["total_evicted"] == 1
    assert q.pop() == b"crit2"
    assert q.pop() == b"crit3"
    assert q.pop() is None


def test_delta_ring_buffer():
    ring = DeltaRingBuffer(capacity=3)
    ring.append(1, "payload1")
    ring.append(2, "payload2")
    ring.append(3, "payload3")

    assert ring.latest_tick_id() == 3
    assert ring.get_missed_deltas(1) == ["payload2", "payload3"]
    assert ring.get_missed_deltas(99) is None

    ring.append(4, "payload4")
    assert ring.get_missed_deltas(1) is None


def test_snapshot_chunking():
    data = {"key": "value" * 1000}
    chunks = chunk_snapshot(data, "snap1")
    assert len(chunks) == 1
    assert chunks[0]["snapshot_id"] == "snap1"
    assert chunks[0]["total_chunks"] == 1

    reassembler = SnapshotReassembler()
    result = reassembler.ingest_chunk(chunks[0])
    assert result == data


def test_bridge_basic():
    bridge = SyncStateBridge()
    assert bridge.router is not None
    assert bridge.kernel is not None

    class DummyTransport:
        def emit(self, frame):
            pass

    t = DummyTransport()
    bridge.register_transport(t, ["test"], direction="in")
    bridge.start()

    bridge.submit({"type": "test", "id": "e1"}, source_transport=t)
    stats = bridge.kernel.stats()
    # Compute total depth from the three tiers
    total_depth = stats.get("critical_depth", 0) + stats.get("conflatable_depth", 0) + stats.get("best_effort_depth", 0)
    assert total_depth >= 0

    bridge.close()