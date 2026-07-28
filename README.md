![State Sync Diagram](https://raw.githubusercontent.com/akingdom/sync-state-bridge/refs/heads/main/diagram.svg)

# [sync-state-bridge](https://github.com/akingdom/sync-state-bridge/)

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

## Advanced Features

### QoS‑Aware Priority Queue

The `PriorityQoSQueue` provides three tiers:
- **CRITICAL** (Level 3) – never dropped.
- **CONFLATABLE** (Level 2) – only the latest frame per entity is kept.
- **BEST_EFFORT** (Level 1) – dropped under queue pressure.

### Orchestrator (`SyncStateBridge`)

Wraps `StateSync` to handle commands and pump deltas into transports.

```python
from sync_state import SyncStateBridge, AsyncIPCTransport

bridge = SyncStateBridge()
transport = AsyncIPCTransport(writer)  # e.g., a socket writer
bridge.register_transport(transport)

# In simulation loop:
bridge.process_pending_commands(handler)
bridge.track_change("player_1", "update", {"x": 10})
bridge.commit_tick(tick_id)
```

### Snapshot Chunking

Large snapshots are split into 64 KB chunks for reliable transmission.

```python
from sync_state import chunk_snapshot, SnapshotReassembler

chunks = chunk_snapshot(state, "snap_123")
reassembler = SnapshotReassembler()
for chunk in chunks:
    reassembler.ingest_chunk(chunk)
```

### Disk Persistence (WAL)

Use `DiskPersistenceAdapter` to write an append‑only log for crash recovery.

```python
from sync_state import DiskPersistenceAdapter

persistence = DiskPersistenceAdapter("logs/wal.bin", zero_loss_mode=True)
bridge.register_transport(persistence)
```

## Bidirectional Control & Receipts

### Entity Configuration: Read‑Only vs Read‑Write
When registering a snapshot provider, you can now set its access direction and receipt timeout:

```python
from sync_state import StateSync, TypeMetadata, SyncDirection, Presets

sync = StateSync()

# Read‑write actuator (requires ack within 500ms)
sync.register_snapshot_provider(
    "robots",
    get_robots,
    metadata=TypeMetadata(
        type_name="robots",
        direction=SyncDirection.BIDIRECTIONAL,
        ack_timeout_ms=500,
        fault_handler=lambda client_id, ctx: set_gpio(SAFETY_PIN, LOW)
    )
)

# Read‑only telemetry (no commands allowed)
sync.register_snapshot_provider(
    "sensors",
    get_sensors,
    metadata=TypeMetadata(
        type_name="sensors",
        direction=SyncDirection.UNIDIRECTIONAL
    )
)
```

### Sending Commands from JavaScript Client
The client now supports sending commands with automatic command IDs:

```javascript
import { StateClient } from './stateClient.js';

const client = new StateClient("http://localhost:8000");
client.setCallbacks({
    onCommandAck: (ack) => {
        console.log(`Command ${ack.command_id} completed: ${ack.status}`);
        if (ack.status === 'ok') {
            console.log(`Result tick: ${ack.result.tick}`);
        }
    },
    onError: (err) => {
        if (err.fault) {
            console.error("Fault detected! Hardware emergency stop triggered.");
        }
    }
});
client.connect();

// Send a command
const receipt = await client.sendCommand('robots:move', { x: 10, y: 20 });
console.log('Command received by gateway:', receipt.command_id);
```

### Handling Receipts and Faults
- **Immediate Receipt:** The `sendCommand()` promise resolves with `{status:"received", command_id}` as soon as the HTTP POST returns.
- **Action Receipt:** The `onCommandAck` callback is invoked when the Worker finishes processing.
- **Fault:** If the Worker does not ack within `ack_timeout_ms`, the `onError` callback is invoked with `fault: true`. The connection is closed automatically.

### Worker‑Side Command Handler
In the Worker process, define how commands translate to state mutations:

```python
# worker.py
def command_handler(action: str, params: dict) -> dict:
    type_name, operation = action.split(":", 1)
    if operation == "move":
        bridge.track_change(params["id"], "update", {"x": params["x"], "y": params["y"]})
        bridge.commit_tick(bridge.current_tick + 1)
        return {"status": "ok", "tick": bridge.current_tick}
    else:
        raise ValueError(f"Unknown operation: {operation}")

bridge.register_command_handler(command_handler)
```

### Zero‑Tolerance (Immediate Fault) Mode
For safety‑critical actuators, set `ack_timeout_ms=0`. The Gateway will trigger a fault **if the Worker does not acknowledge synchronously**.


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

