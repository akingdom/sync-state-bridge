# sync-state-bridge/reliability – Production‑Ready Quality of Service

This folder contains the modules that turn `sync-state-bridge` from a simple data pipeline into a **production‑grade, resilient, and predictable** synchronisation system.

All modules here are **optional** – you can run the core without them – but they are **critical for any real‑world deployment** where network conditions, load, or reliability matter.

---

## FAQ

### 1. How do I control which frames are critical vs. fire‑and‑forget?

→ Use `qos.py` and `qos_queue.py`.

```python
from sync_state.reliability import QoS, DropPolicy, PriorityQoSQueue

# Per‑type QoS
qos = QoS(drop_policy=DropPolicy.CRITICAL, ttl_ms=None)
```

| Policy | Behaviour |
|--------|-----------|
| `CRITICAL` | Never dropped – queued until delivered |
| `CONFLATABLE` | Only the latest frame per entity is kept |
| `BEST_EFFORT` | Dropped immediately under queue pressure |

The `PriorityQoSQueue` provides three tiers with automatic eviction.

---

### 2. How do I handle backpressure when clients are slow?

→ The `PriorityQoSQueue` is backpressure‑aware. It drops `BEST_EFFORT` frames first and conflates `CONFLATABLE` frames. Use the metrics to monitor queue depth.

```python
stats = qos_queue.stats()
if stats["critical_depth"] > 50:
    # alert or throttle
```

---

### 3. Are there pre‑configured QoS profiles for common scenarios?

→ Yes, in `presets.py`.

```python
from sync_state.reliability import Presets

qos = Presets.low_bandwidth()   # ideal for serial, BLE, satellite
qos = Presets.high_throughput() # high‑frequency telemetry
qos = Presets.conservative()    # safe for general IP networks
```

---

### 4. How do clients recover after a disconnect without re‑downloading the full state?

→ Use `ring_buffer.py`.

```python
from sync_state.reliability import DeltaRingBuffer

ring = DeltaRingBuffer(capacity=500)
ring.append(tick, payload)

# On reconnection:
missed = ring.get_missed_deltas(client_last_tick)
if missed is not None:
    send(missed)
else:
    send_full_snapshot()
```

This keeps a short history of deltas, allowing fast catch‑up.

---

### 5. My snapshots are huge – how do I send them reliably over lossy transports?

→ Use `chunking.py`.

```python
from sync_state.reliability import chunk_snapshot, SnapshotReassembler

chunks = chunk_snapshot(state, "snap_123")
for chunk in chunks:
    send(chunk)

# On the receiving side:
reassembler = SnapshotReassembler()
for chunk in chunks:
    reassembler.ingest_chunk(chunk)  # returns full dict when complete
```

Chunks are 64KB, with MD5 hash verification and a 5‑second reassembly timeout.

---

### 6. How do I make a client automatically reconnect with exponential backoff?

→ Use `client_socket.py`.

```python
from sync_state.reliability import StateSyncSocketClient

client = StateSyncSocketClient(
    host="127.0.0.1",
    port=8765,
    on_delta_callback=handle_delta,
    max_reconnect_delay=32.0
)
await client.connect_and_listen()
```

It handles reconnection, version tracking, and automatically resumes from where it left off.

---

## When to Use Each Module

| Module | Use When... |
|--------|-------------|
| `qos.py` | You need to define per‑type delivery semantics. |
| `qos_queue.py` | You have multiple priority levels and need to prevent queue exhaustion. |
| `presets.py` | You want a quick, proven configuration without tuning each parameter. |
| `ring_buffer.py` | Clients disconnect and reconnect frequently; you want to avoid full resyncs. |
| `chunking.py` | Your state snapshots exceed 64KB and you need to send them over unreliable or MTU‑limited transports. |
| `client_socket.py` | You are building a native (non‑browser) client that needs automatic reconnection. |

---

## Relationship to the Core

- **Core (`core/`)** – provides the Router, IPC, and StateSync. It handles forwarding and versioning.
- **Reliability (`reliability/`)** – adds QoS, queues, buffering, fragmentation, and client resilience.
- **Quickstart (`quickstart/`)** – provides ready‑to‑run server/worker classes that optionally integrate reliability modules.

You can use the core alone for simple setups, and gradually add reliability modules as your system grows. Each module is independent – pick what you need.

---

## Additional Tips

- **Always monitor queue depth** – `PriorityQoSQueue.stats()` gives you `critical_depth`, `conflatable_depth`, and `best_effort_depth`. Set alerts.
- **Choose presets wisely** – `low_bandwidth` is good for expensive links; `high_throughput` is good for fast, noisy telemetry.
- **Chunking is loss‑aware** – if a chunk fails hash verification, the entire snapshot is discarded and must be resent. Ensure your transport can request a fresh snapshot.
- **Client socket handles backoff** – it will not flood the server on reconnection; use `max_reconnect_delay` to cap it.

---

For more details, see the main [README](../README.md) and the [Protocol Specification](../PROTOCOL.md).
