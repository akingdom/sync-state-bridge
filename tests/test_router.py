import pytest
from sync_state.core.router import Router, RouterConfigError, RouterUnauthorizedError


class DummyTransport:
    def __init__(self):
        self.emitted = []

    def emit(self, frame):
        self.emitted.append(frame)

def dummy_conflict_policy(type_name, frames_by_source):
    return next(iter(frames_by_source.values()))

def test_router_single_authority():
    router = Router()
    t1 = DummyTransport()
    t2 = DummyTransport()

    router.register_transport(t1, ["player"], "in")
    router.register_transport(t2, ["player"], "out")

    frame = {"type": "player", "id": "p1"}
    targets = router.route(frame, t1)
    assert targets == [t2]


def test_router_denies_unauthorized_source():
    router = Router()
    t1 = DummyTransport()
    t2 = DummyTransport()

    router.register_transport(t1, ["player"], "out")
    router.register_transport(t2, ["player"], "in")

    frame = {"type": "player", "id": "p1"}
    with pytest.raises(RouterUnauthorizedError):
        router.route(frame, t1)


def test_router_requires_type():
    router = Router()
    t1 = DummyTransport()
    frame = {"id": "p1"}
    with pytest.raises(ValueError, match="missing 'type'"):
        router.route(frame, t1)


def test_router_multiple_sources_allowed():
    router = Router()
    t1 = DummyTransport()
    t2 = DummyTransport()

    # Pass dummy conflict_policy if multi-source requires it
    router.register_transport(t1, ["player"], "in", allow_multiple_sources=True, conflict_policy=lambda x: x)
    router.register_transport(t2, ["player"], "in", allow_multiple_sources=True, conflict_policy=lambda x: x)


def test_router_multiple_sources_requires_conflict_policy():
    router = Router()
    t1 = DummyTransport()
    t2 = DummyTransport()

    router.register_transport(t1, ["player"], "in", allow_multiple_sources=True)
    
    # Registering a second inbound source without providing a conflict_policy should raise
    with pytest.raises(RouterConfigError, match="conflict_policy"):
        router.register_transport(t2, ["player"], "in", allow_multiple_sources=True)


def test_router_returns_empty_for_no_routes():
    router = Router()
    t1 = DummyTransport()
    frame = {"type": "unknown", "id": "x"}
    targets = router.route(frame, t1)
    assert targets == []


def test_router_excludes_source_transport():
    router = Router()
    t1 = DummyTransport()
    t2 = DummyTransport()
    router.register_transport(t1, ["player"], "both")
    router.register_transport(t2, ["player"], "out")
    frame = {"type": "player", "id": "p1"}
    targets = router.route(frame, t1)
    assert t1 not in targets
    assert t2 in targets