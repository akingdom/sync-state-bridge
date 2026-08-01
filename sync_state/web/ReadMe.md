# sync-state-bridge/web – HTTP/SSE Helpers for Browser Clients

This folder provides the **low‑level HTTP/SSE transport helper** for the sync-state-bridge library. It is designed for applications that need to expose the sync protocol to web browsers or other HTTP‑capable clients.

---

## What's Inside

- `httphelper.py` – provides `create_http_routes()`, a FastAPI router builder.
- `client_js.py` – provides `get_client_js_content()`, returning the JavaScript client library as a string.
- `static/stateClient.js` – the actual JavaScript client source (served via the application).

---

## When to Use This

| Use Case | Approach |
|----------|----------|
| **You have an existing FastAPI app** and want to add sync routes. | Use `create_http_routes()` and mount them. |
| **You want full control** over the server lifecycle (host, port, middleware). | Use `create_http_routes()` directly. |
| **You are building a native (non‑browser) client** that speaks HTTP/SSE. | Use the same routes; they are transport‑agnostic. |
| **You want a quick, ready‑to‑run server** with no customisation. | Use the `quickstart.SyncServer` or `quickstart.HTTPServer` instead. |

---

## Basic Usage (Advanced)

```python
from fastapi import FastAPI
from sync_state.web import create_http_routes
from sync_state.core import Router

router = Router()
# ... register transports ...

http_routes = create_http_routes(
    router,
    client_sendable_types=["controls"],
    server_broadcast_types=["asteroid", "player"],
    path_prefix="/sync"
)

app = FastAPI()
app.include_router(http_routes)
# ... mount other routes, static files, etc.
```

---

## Broadcast to Clients

The returned `APIRouter` has a `broadcast()` function attached. You can use it to push frames to all connected SSE clients.

```python
http_router = create_http_routes(...)
http_router.broadcast('{"type": "player", "id": "p1", ...}')
```

This is useful when you receive a frame from the worker (via IPC) and need to forward it to all browsers.

---

## Serving the JavaScript Client

```python
from sync_state.web import get_client_js_content

@app.get("/client/stateClient.js")
def serve_client_js():
    return HTMLResponse(
        content=get_client_js_content(),
        media_type="application/javascript"
    )
```

---

## Relationship to `quickstart/`

- **`web/`** provides the **low‑level building blocks**.
- **`quickstart/`** provides **ready‑to‑run convenience classes** (`SyncServer`, `HTTPServer`) that use these blocks internally.

Start with `quickstart/` if you want a working server in 5 minutes. Use `web/` directly if you need to customise the server integration.

---

## Where to Go Next

- For customising the transport (e.g., adding authentication, CORS), refer to FastAPI documentation.
- For integrating the sync protocol into a non‑FastAPI web framework, you can adapt the logic from `httphelper.py`.
- For full details of the sync protocol, see [PROTOCOL.md](../PROTOCOL.md).
