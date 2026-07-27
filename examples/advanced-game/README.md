# Advanced Demo: Swarm vs Asteroids

This demo showcases the full power of `sync-state-bridge` under a realistic, high‑frequency game simulation.

## Features

- **3,000 swarm particles** – each updated 60 times/sec with conflation.
- **8 asteroids** – particles avoid them.
- **Up to 3 players** – each with unique controls.
- **Dynamic swarm aggression** – shooting particles makes the swarm chase players.
- **QoS tiers** – player movement/fire = CRITICAL, particle positions = CONFLATABLE.
- **Process separation** – worker and gateway run in separate processes.
- **Ring buffer** – fast reconnection for clients.
- **Full snapshot** – new clients receive the complete state.
- **Interactive UI** – click to attract, join with keyboard.

## Controls

| Player | Movement | Fire |
|--------|----------|------|
| P1     | WASD     | Z    |
| P2     | Arrows   | .    |
| P3     | IJKL     | ,    |

Click **Join** to take control of a ship. Keys are shown on‑screen.

## Running

```bash
# In the advanced example directory
python supervisor_runner.py
```

Then open `http://localhost:8000`.

## Architecture

- `worker.py` – runs the simulation, emits deltas via `SyncStateBridge` over TCP to the gateway.
- `gateway.py` – FastAPI server, maintains the ring buffer, broadcasts SSE, forwards commands to worker.
- `supervisor_runner.py` – launches both with automatic restart.

## Stress Test

With 3,000 particles and up to 3 players firing, the system handles ~180,000 entity updates per second. The QoS queue drops BEST_EFFORT telemetry if overloaded, but keeps CRITICAL commands and CONFLATABLE particle positions flowing.
