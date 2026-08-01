import pytest
import asyncio
from sync_state.core.state_sync import StateSync, canonical_hash
from sync_state.reliability.qos import QoS, DropPolicy
from sync_state.reliability.presets import Presets


def test_canonical_hash_deterministic():
    obj_a = {"b": 2, "a": 1, "c": [3, 4]}
    obj_b = {"a": 1, "c": [3, 4], "b": 2}
    assert canonical_hash(obj_a) == canonical_hash(obj_b)


@pytest.mark.asyncio
async def test_commit_and_delta_generation():
    sync = StateSync()
    data = [{"id": "e1", "val": 100}]

    sync.register_snapshot_provider("sensors", lambda: data, qos=Presets.conservative())
    sync.mark_dirty("sensors")
    await sync.commit()

    delta_v0 = sync.get_delta("sensors", client_version=0)
    assert delta_v0["full"] is True
    assert delta_v0["version"] == 1
    assert len(delta_v0["added"]) == 1
    assert delta_v0["added"][0]["id"] == "e1"


@pytest.mark.asyncio
async def test_version_gap_triggers_full_snapshot():
    sync = StateSync()
    data = [{"id": "e1", "val": 100}]

    sync.register_snapshot_provider("sensors", lambda: data)
    sync.mark_dirty("sensors")
    await sync.commit()

    delta = sync.get_delta("sensors", client_version=99)
    assert delta["full"] is True
    assert len(delta["added"]) == 1


@pytest.mark.asyncio
async def test_get_last_delta():
    sync = StateSync()
    data = [{"id": "e1", "val": 100}]
    sync.register_snapshot_provider("sensors", lambda: data)
    sync.mark_dirty("sensors")
    await sync.commit()

    delta = sync.get_last_delta("sensors")
    assert delta is not None
    assert delta["type"] == "sensors"
    assert delta["version"] == 1
    assert len(delta["added"]) == 1
    assert delta["added"][0]["id"] == "e1"