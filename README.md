![State Sync Diagram](https://raw.githubusercontent.com/akingdom/sync-state-bridge/refs/heads/main/diagram.svg)

# sync-state-bridge

A deterministic, race‑safe state synchronisation bridge for real‑time applications, with built‑in QoS, backpressure handling, and network resilience.

- **Versioned, per‑type sync** – each entity type has its own version history.
- **Segmented deltas** – send only what changed, with a manifest first.
- **FSM lifecycle** – correct handling of add→update→delete sequences.
- **Resilient reconnection** – out‑of‑order drop, monotonicity guards, schema mismatch handling.
- **Quality of Service (QoS)** – define drop policies (CRITICAL, CONFLATABLE, BEST_EFFORT) per entity type.
- **Backpressure‑aware transports** – bounded queues, priority full‑snapshot injection, congestion metrics.
- **Reconnecting client** – exponential backoff, version tracking, automatic recovery.
- **Pure Python + JS** – no binary dependencies.

## Quick Start

Install the server package:

```bash
pip install sync-state-bridge
```

For optional features (faster JSON, serial transport):

```bash
pip install sync-state-bridge[fast,serial]
```

### Basic Usage

```python
from sync_state import StateSync, Presets

sync = StateSync()

# Register a snapshot provider with a QoS profile
sync.register_snapshot_provider(
    "vehicles",
    get_vehicles,
    qos=Presets.low_bandwidth()   # optimised for slow links
)

# After changes:
sync.mark_dirty("vehicles")
await sync.commit()

# Stream deltas over HTTP (SSE)
from fastapi import FastAPI, StreamingResponse
app = FastAPI()

@app.get("/stream")
async def stream(versions: str = "{}"):
    return StreamingResponse(
        sync.stream_deltas(json.loads(versions)),
        media_type="text/event-stream"
    )
```

### Socket Server (for low‑level IPC)

```python
from sync_state.transports import StateSyncSocketServer

server = StateSyncSocketServer(sync)
await server.start_tcp(host="0.0.0.0", port=8765)
```

### Reconnecting Client

```python
from sync_state import StateSyncSocketClient

def handle_delta(delta):
    print(f"Update: {delta}")

client = StateSyncSocketClient(
    host="127.0.0.1",
    port=8765,
    on_delta_callback=handle_delta
)
await client.connect_and_listen()
```

## Quality of Service (QoS)

Each entity type can have a `QoS` profile:

| Policy        | Behaviour |
|---------------|-----------|
| `CRITICAL`    | Never dropped; queued until delivered. |
| `CONFLATABLE` | Intermediate deltas dropped; only the latest is sent (ideal for high‑frequency telemetry). |
| `BEST_EFFORT` | Discarded immediately under queue pressure. |

Pre‑configured profiles (`Presets.conservative()`, `Presets.low_bandwidth()`, `Presets.high_throughput()`) are provided.

## Testing

Run the unit tests:

```bash
pytest tests/
```

The test suite covers:
- Deterministic hashing (`canonical_hash`)
- Commit & delta generation
- Version‑gap full‑snapshot recovery

## Demos

- [**Chat**](https://github.com/akingdom/sync-state-bridge/blob/main/example/chat/) – real‑time message broadcast with shared history.
- [**Game**](https://github.com/akingdom/sync-state-bridge/blob/main/example/game/) – "Find the Clusters" demonstrating turn‑based sync.

Run the demos:

```bash
cd examples/chat && python server.py
```
```bash
cd examples/game && python server.py
```

## Protocol

See [`PROTOCOL.md`](https://github.com/akingdom/sync-state-bridge/blob/main/PROTOCOL.md) for the full SSE‑based delta protocol.

## License

MIT
