import json
import hashlib
import asyncio
import copy
import time
from collections import defaultdict, deque
from typing import Dict, List, Any, Callable, Optional, Set, AsyncIterator
from .qos import QoS, DropPolicy, TypeMetadata   # if you have these


def canonical_hash(entity: Dict[str, Any]) -> str:
    """Deterministic, canonical SHA-256 hash calculation for dictionaries."""
    if "_v" in entity:
        return str(entity["_v"])

    def _default_encoder(o: Any) -> Any:
        if isinstance(o, (set, tuple)):
            return list(o)
        if hasattr(o, "to_dict") and callable(o.to_dict):
            return o.to_dict()
        return sorted(list(o.__dict__.items())) if hasattr(o, "__dict__") else str(o)

    try:
        import orjson
        encoded = orjson.dumps(entity, option=orjson.OPT_SORT_KEYS, default=_default_encoder)
    except ImportError:
        encoded = json.dumps(entity, sort_keys=True, default=_default_encoder).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()

class StateSyncError(Exception):
    """Base exception for StateSync operations."""
    pass


class ProviderValidationError(StateSyncError):
    """Raised when a snapshot provider returns invalid or unsafe data."""
    pass


class StateSync:
    def __init__(self, id_key: str = "id", max_history: int = 1000, schema_version: int = 1):
        self.id_key = id_key
        self.max_history = max_history
        self.schema_version = schema_version

        self._lock = asyncio.Lock()
        self._dirty_types: Set[str] = set()
        self._snapshot_providers: Dict[str, Callable[[], List[Dict]]] = {}

        self._versions: Dict[str, int] = defaultdict(int)
        self._change_logs: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
        self._current_snapshots: Dict[str, Dict[Any, Dict]] = {}
        self._type_metadata: Dict[str, TypeMetadata] = {}   # <-- add this

        self._stream_events: List[asyncio.Event] = []

    def register_snapshot_provider(
        self,
        type_name: str,
        provider: Callable[[], List[Dict]],
        qos: Optional[QoS] = None,
        max_frame_bytes: int = 1_048_576
    ):
        from .qos import QoS, TypeMetadata  # ensure import is available
        qos_obj = qos or QoS()
        self._snapshot_providers[type_name] = provider
        self._type_metadata[type_name] = TypeMetadata(
            type_name=type_name,
            qos=qos_obj,
            max_frame_bytes=max_frame_bytes
        )

    def mark_dirty(self, type_name: str):
        self._dirty_types.add(type_name)

    def _canonical_hash(self, entity: Dict) -> str:
        if "_v" in entity:
            return str(entity["_v"])

        def _default_encoder(o):
            if isinstance(o, (set, tuple)):
                return list(o)
            return f"__repr__:{type(o).__name__}:{repr(o)}"

        try:
            import orjson
            encoded = orjson.dumps(entity, option=orjson.OPT_SORT_KEYS, default=_default_encoder)
        except ImportError:
            encoded = json.dumps(entity, sort_keys=True, default=_default_encoder).encode('utf-8')
        return hashlib.sha256(encoded).hexdigest()

    def _validate_and_sanitize_snapshot(self, raw_entities: Any, type_name: str) -> Dict[Any, Dict]:
        if not isinstance(raw_entities, (list, tuple)):
            raise ProviderValidationError(f"Provider for '{type_name}' must return a list or tuple.")

        sanitized = {}
        for idx, entity in enumerate(raw_entities):
            if not isinstance(entity, dict):
                raise ProviderValidationError(f"Entity at index {idx} in '{type_name}' is not a dict.")
            if self.id_key not in entity:
                raise ProviderValidationError(f"Entity at index {idx} in '{type_name}' missing key '{self.id_key}'.")
            eid = entity[self.id_key]
            if eid is None or not isinstance(eid, (str, int)):
                raise ProviderValidationError(f"Entity ID at index {idx} in '{type_name}' must be str or int.")
            if eid in sanitized:
                raise ProviderValidationError(f"Duplicate entity ID '{eid}' detected in '{type_name}'.")
            sanitized[eid] = copy.deepcopy(entity)
        return sanitized

    async def commit(self):
        dirty_types_to_process = []
        async with self._lock:
            if not self._dirty_types:
                return
            dirty_types_to_process = list(self._dirty_types)
            self._dirty_types.clear()

        fetched_snapshots: Dict[str, Dict[Any, Dict]] = {}
        for type_name in dirty_types_to_process:
            if type_name in self._snapshot_providers:
                try:
                    raw_data = self._snapshot_providers[type_name]()
                    fetched_snapshots[type_name] = self._validate_and_sanitize_snapshot(raw_data, type_name)
                except Exception:
                    async with self._lock:
                        self._dirty_types.add(type_name)
                    raise

        async with self._lock:
            for type_name, new_snapshot in fetched_snapshots.items():
                old_snapshot = self._current_snapshots.get(type_name, {})
                old_ids = set(old_snapshot.keys())
                new_ids = set(new_snapshot.keys())

                self._versions[type_name] += 1
                current_version = self._versions[type_name]

                added = new_ids - old_ids
                deleted = old_ids - new_ids
                common = new_ids & old_ids

                updated = [
                    eid for eid in common
                    if self._canonical_hash(old_snapshot[eid]) != self._canonical_hash(new_snapshot[eid])
                ]

                self._change_logs[type_name].append({
                    "version": current_version,
                    "added": list(added),
                    "updated": updated,
                    "deleted": list(deleted)
                })
                self._current_snapshots[type_name] = new_snapshot

            # Notify all active streams
            for evt in self._stream_events:
                evt.set()

    def get_delta(self, type_name: str, client_version: int) -> Dict[str, Any]:
        snapshot = self._current_snapshots.get(type_name, {})
        history = self._change_logs.get(type_name)
        current_version = self._versions.get(type_name, 0)

        if not history or client_version < history[0]["version"] or client_version > current_version:
            return {
                "type": type_name,
                "full": True,
                "version": current_version,
                "added": list(snapshot.values()),
                "updated": [],
                "deleted": []
            }

        entity_state: Dict[Any, str] = {}
        for entry in history:
            if entry["version"] <= client_version:
                continue
            for eid in entry["added"]:
                entity_state[eid] = "added"
            for eid in entry["updated"]:
                if eid not in entity_state:
                    entity_state[eid] = "updated"
                elif entity_state[eid] == "added":
                    continue
                elif entity_state[eid] == "deleted":
                    continue
            for eid in entry["deleted"]:
                if entity_state.get(eid) == "added":
                    del entity_state[eid]
                else:
                    entity_state[eid] = "deleted"

        added_ids = [eid for eid, state in entity_state.items() if state == "added"]
        updated_ids = [eid for eid, state in entity_state.items() if state == "updated"]
        deleted_ids = [eid for eid, state in entity_state.items() if state == "deleted"]

        return {
            "type": type_name,
            "full": False,
            "version": current_version,
            "added": [snapshot[eid] for eid in added_ids if eid in snapshot],
            "updated": [snapshot[eid] for eid in updated_ids if eid in snapshot],
            "deleted": deleted_ids
        }

    async def stream_deltas(self, client_versions: Dict[str, int], keepalive_interval: int = 15):
        # Create a per‑stream event on the current loop
        stream_event = asyncio.Event()
        self._stream_events.append(stream_event)

        try:
            local_client_versions = dict(client_versions)

            # Initial sync
            deltas_to_send = []
            full_manifest_versions = {}

            async with self._lock:
                for type_name in self._snapshot_providers:
                    c_ver = local_client_versions.get(type_name, 0)
                    delta = self.get_delta(type_name, c_ver)
                    full_manifest_versions[type_name] = self._versions.get(type_name, 0)
                    if delta["full"] or delta["added"] or delta["updated"] or delta["deleted"]:
                        deltas_to_send.append(delta)
                        local_client_versions[type_name] = delta["version"]

            manifest = {
                "schema_version": self.schema_version,
                "versions": full_manifest_versions,
                "types": list(self._snapshot_providers.keys())
            }

            yield f"event: manifest\ndata: {json.dumps(manifest)}\n\n"

            for delta in deltas_to_send:
                yield f"event: delta\ndata: {json.dumps(delta)}\n\n"

            # Reactive loop
            while True:
                try:
                    await asyncio.wait_for(stream_event.wait(), timeout=keepalive_interval)
                except asyncio.TimeoutError:
                    # Keepalive
                    yield f"event: keepalive\ndata: {{}}\n\n"
                    continue

                # Clear the event so it can be used again
                stream_event.clear()

                # A commit happened – check for new deltas
                deltas_to_send = []
                async with self._lock:
                    for type_name in self._snapshot_providers:
                        c_ver = local_client_versions.get(type_name, 0)
                        delta = self.get_delta(type_name, c_ver)
                        if delta["full"] or delta["added"] or delta["updated"] or delta["deleted"]:
                            deltas_to_send.append(delta)
                            local_client_versions[type_name] = delta["version"]

                for delta in deltas_to_send:
                    yield f"event: delta\ndata: {json.dumps(delta)}\n\n"

        finally:
            # Remove this stream's event from the list
            if stream_event in self._stream_events:
                self._stream_events.remove(stream_event)

    def get_versions(self) -> Dict[str, int]: # 1.0.1
        """Return a copy of the current version map for all tracked types."""
        return dict(self._versions)