import pytest
import json
import time
from sync_state import (
    PriorityQoSQueue,
    SyncStateBridge,
    DeltaRingBuffer,
    chunk_snapshot,
    SnapshotReassembler,
    StateSync,
)


def test_priority_qos_queue_basic():
    q = PriorityQoSQueue(capacity=10)

    # BEST_EFFORT
    assert q.put(b"best", qos_level=1) is True
    # CONFLATABLE
    assert q.put(b"conf_a", qos_level=2, entity_id="e1") is True
    assert q.put(b"conf_b", qos_level=2, entity_id="e1") is True  # overwrites
    # CRITICAL
    assert q.put(b"crit", qos_level=3) is True

    # Pop order: CRITICAL first
    assert q.pop() == b"crit"
    # Then CONFLATABLE (latest)
    assert q.pop() == b"conf_b"
    # Then BEST_EFFORT
    assert q.pop() == b"best"
    assert q.pop() is None


def test_priority_qos_queue_eviction():
    # 1. BEST_EFFORT drop when total depth exceeds 80% capacity
    q = PriorityQoSQueue(capacity=2)
    assert q.put(b"be1", qos_level=1) is True
    assert q.put(b"be2", qos_level=1) is True   # total_depth=1 -> 1 <= 1 (80% of 2 = 1) -> accepted
    assert q.put(b"be3", qos_level=1) is False  # total_depth=2 -> 2 > 1 -> dropped
    assert q.stats()["total_evicted"] == 1

    # 2. CRITICAL evicts oldest CRITICAL when critical queue is full
    q = PriorityQoSQueue(capacity=2)  # capacity only limits best_effort queue? Actually it limits all queues? But critical queue has its own maxsize = capacity.
    # Fill critical queue (maxsize=2)
    assert q.put(b"crit1", qos_level=3) is True
    assert q.put(b"crit2", qos_level=3) is True
    # Now put a third CRITICAL: should evict crit1
    assert q.put(b"crit3", qos_level=3) is True
    assert q.stats()["total_evicted"] == 1
    # Verify that crit1 is gone and crit3 is in the queue
    # We can pop to check: order is FIFO, so first pop should return crit2 (since crit1 was evicted)
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

    ring.append(4, "payload4")  # evicts tick 1
    assert ring.get_missed_deltas(1) is None


def test_snapshot_chunking():
    data = {"key": "value" * 1000}  # small enough for 1 chunk
    chunks = chunk_snapshot(data, "snap1")
    assert len(chunks) == 1
    assert chunks[0]["snapshot_id"] == "snap1"
    assert chunks[0]["total_chunks"] == 1

    reassembler = SnapshotReassembler()
    result = reassembler.ingest_chunk(chunks[0])
    assert result == data


def test_bridge_basic():
    sync = StateSync()
    bridge = SyncStateBridge(sync)
    assert bridge.state_sync is sync

    bridge.track_change("e1", "update", {"x": 1})
    bridge.track_change("e2", "delete", {})
    assert len(bridge._pending_deltas) == 2

    # Commit without transport - should not error
    bridge.commit_tick(1)
    assert bridge.current_tick == 1
    assert len(bridge._pending_deltas) == 0