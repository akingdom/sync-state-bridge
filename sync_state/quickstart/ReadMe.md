# sync-state-bridge/quickstart – Ready‑to‑Run Convenience Classes

This folder contains **training wheels** for the sync-state-bridge library. They are optional convenience classes that get you up and running in minutes, without needing to understand the underlying composition.

---

## What's Inside

- `server.py` – `SyncServer`: full gateway with both IPC (worker) and HTTP (web) transports.
- `httpserver.py` – `HTTPServer`: web‑only gateway (no IPC). Subclass of `SyncServer`.
- `worker.py` – `SyncWorker`: engine process that connects to the gateway via IPC.

---

## When to Use This

| You want... | Use... |
|-------------|--------|
| A complete server with web UI + worker in one line of code. | `SyncServer` |
| A web‑only server (no worker) for UI testing or client‑only apps. | `HTTPServer` |
| A worker engine that runs the game/physics logic and communicates via IPC. | `SyncWorker` (subclass and override `on_frame` and `on_tick`) |
| Full customisation of the server (e.g., adding authentication, custom routes). | Drop down to `web.httphelper` and `core.router` directly. |

---

## Basic Usage

### Full Gateway (Web + Worker)

```python
from sync_state.quickstart import SyncServer

server = SyncServer(
    worker_ipc_port=8766,        # port for worker IPC
    host="127.0.0.1",
    port=8000,
    static_dir="static",
    client_sendable_types=["controls"],
    server_broadcast_types=["asteroid", "player"],
)
server.run()
```

### Web‑Only Gateway (No Worker)

```python
from sync_state.quickstart import HTTPServer

server = HTTPServer(
    host="127.0.0.1",
    port=8000,
    static_dir="static",
    client_sendable_types=["controls"],
    server_broadcast_types=["asteroid", "player"],
)
server.run()
```

### Worker Engine

```python
from sync_state.quickstart import SyncWorker

class GameWorker(SyncWorker):
    async def on_start(self):
        self.game = GameState()

    async def on_frame(self, frame):
        if frame["type"] == "controls":
            self.game.update_controls(frame)

    async def on_tick(self):
        self.game.update()
        for delta in self.game.get_deltas():
            self.send(delta)

worker = GameWorker(gateway_ipc_port=8766)
asyncio.run(worker.run())
```

---

## Customisation

Override `on_startup()` in `SyncServer` or `HTTPServer` to add custom logic during server startup.

```python
class MyServer(SyncServer):
    async def on_startup(self):
        print("Server starting...")
        # e.g., connect to database, initialise services
```

---

## How They Work Under the Hood

- **`SyncServer`** creates a `Router`, an IPC transport (listening on `worker_ipc_port`), and HTTP routes (using `web.httphelper`). It then runs uvicorn.
- **`HTTPServer`** does the same but disables the IPC transport.
- **`SyncWorker`** connects to the gateway's IPC port and enters a loop: reading frames, calling `on_frame`, and calling `on_tick` at a fixed interval.

---

## Relationship to `core/` and `web/`

- **`core/`** provides the engine (Router, IPC, StateSync).
- **`web/`** provides the HTTP/SSE helper (`create_http_routes`).
- **`quickstart/`** composes these into ready‑to‑run classes.

If you outgrow the convenience classes, you can always drop down to the low‑level components and build your own server composition.

---

## Where to Go Next

- For custom transport logic, see `core/router.py` and `core/ipc_transport.py`.
- For customising the HTTP/SSE layer, see `web/httphelper.py`.
- For adding QoS, backpressure, or reconnection resilience, see `reliability/`.
- For full details of the sync protocol, see [PROTOCOL.md](../PROTOCOL.md).
