# state-sync-bridge

A deterministic, race‑safe versioned delta-state synchronization engine for real-time applications.

- **Versioned, per‑type sync** – each entity type has its own version history.
- **Segmented deltas** – send only what changed, with a manifest first.
- **FSM lifecycle** – correct handling of add→update→delete sequences.
- **Resilient reconnection** – out‑of‑order drop, monotonicity guards, schema mismatch handling.
- **Pure Python + JS** – no binary dependencies.

## Quick Start

Install the server package:

```bash
pip install state-sync-bridge
```

Use it in your FastAPI app:

```python
from state_sync import StateSync

sync = StateSync()
sync.register_snapshot_provider("vehicles", get_vehicles)

# After changes:
sync.mark_dirty("vehicles")
await sync.commit()

# Stream to client:
@app.get("/stream")
async def stream(versions: str = "{}"):
    return StreamingResponse(sync.stream_deltas(json.loads(versions)))
```

Client:

```javascript
import { StateClient } from 'state-sync-bridge';

const client = new StateClient('/stream');
client.connect();
client.setCallbacks({
    onDelta: (delta) => console.log('Sync:', delta)
});
```

## Demos

- **Chat** – real‑time message broadcast with shared history.
- **Game** – "Find the Clusters" demonstrating turn‑based sync.

Run the demos:

```bash
cd examples/chat && python server.py
cd examples/game && python server.py
```

## License

MIT
```

### `PROTOCOL.md`

```markdown
# SyncStateBridge Protocol (v4)

## Messages

All messages are Server‑Sent Events (SSE).

### Manifest (event: manifest)

Sent immediately on connection.

```json
{
  "schema_version": 1,
  "versions": { "vehicles": 42, "buildings": 12 },
  "types": ["vehicles", "buildings"]
}
```

### Delta (event: delta)

Sent for each dirty type.

```json
{
  "type": "vehicles",
  "full": false,
  "version": 42,
  "added": [ { "id": "v-101", "x": 12.5 } ],
  "updated": [ { "id": "v-88", "x": 99.1 } ],
  "deleted": ["v-12"]
}
```

If `full: true`, client must replace its local state for that type entirely.

### Keepalive (event: keepalive)

Sent every 15 seconds to keep the connection alive.

## Versioning

- Each type has its own independent version counter.
- Client sends `versions` query parameter with its current versions for each type.
- If client version is outside server history, server sends full snapshot.

## FSM Rules (for delta generation)

- **Added** → entity appears in delta.
- **Updated** → entity appears in `updated`.
- **Deleted** → entity appears in `deleted`.
- If entity is added and deleted within the same delta window, it is omitted (net zero).
- If entity is added and then updated within the same window, it remains `added` (latest payload).
```

---

## Final Steps

- Place all files into the repository structure.
- Run demos:

```bash
pip install fastapi uvicorn
cd examples/chat
uvicorn server:app --reload
```

- Open `http://localhost:8000` and start chatting.

The game demo works similarly. Both demos use the real `StateSync` and `StateClient`, demonstrating the bridge’s capabilities.

