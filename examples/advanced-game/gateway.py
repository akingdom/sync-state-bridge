#!/usr/bin/env python3
"""
Gateway: FastAPI server that:
- Accepts worker connection on TCP port 8766
- Maintains DeltaRingBuffer
- Broadcasts new frames to all SSE clients
- Forwards player commands to worker
"""

import asyncio
import json
import logging
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from sync_state import DeltaRingBuffer
from sync_state.js import get_client_js_content

app = FastAPI()
ring = DeltaRingBuffer(capacity=500)
current_state = {}  # type -> dict(id -> entity)

# Broadcaster: set of asyncio.Queues for SSE clients
client_queues = set()

# Worker connection
worker_writer = None

async def forward_to_worker(action: str, params: dict = None):
    """Send a command to the worker."""
    global worker_writer
    if not worker_writer:
        return False
    cmd = {"action": action, "params": params or {}}
    try:
        worker_writer.write((json.dumps(cmd) + "\n").encode())
        await worker_writer.drain()
        return True
    except Exception:
        return False

# ----------------------------------------------------------------------
# IPC server: accept worker connection
# ----------------------------------------------------------------------
async def ipc_server():
    global worker_writer, current_state
    server = await asyncio.start_server(
        lambda r, w: handle_worker(r, w),
        '127.0.0.1', 8766
    )
    print("[Gateway] IPC server listening on 8766")
    async with server:
        await server.serve_forever()

async def handle_worker(reader, writer):
    global worker_writer, current_state
    worker_writer = writer
    print("[Gateway] Worker connected")
    try:
        while True:
            try:
                line = await reader.readline()
                if not line:
                    break
                frame = json.loads(line.decode().strip())
                frame_type = frame.get("type")
                if frame_type == "DELTA":
                    tick = frame.get("tick")
                    if tick is not None:
                        ring.append(tick, line.decode().strip())
                    # Update current_state from deltas
                    for d in frame.get("deltas", []):
                        eid = d["id"]
                        op = d["op"]
                        # Determine type from id prefix
                        if eid.startswith("p"):
                            t = "particle"
                        elif eid.startswith("ast"):
                            t = "asteroid"
                        elif eid.startswith("player"):
                            t = "player"
                        elif eid.startswith("bullet"):
                            t = "bullet"
                        else:
                            t = "unknown"
                        if t not in current_state:
                            current_state[t] = {}
                        if op == "delete":
                            current_state[t].pop(eid, None)
                        else:
                            entity = {"id": eid, **d.get("changes", {})}
                            current_state[t][eid] = entity

                    # Broadcast to all SSE clients
                    payload = line.decode().strip()
                    for q in list(client_queues):
                        try:
                            q.put_nowait(payload)
                        except asyncio.QueueFull:
                            client_queues.remove(q)
                # Ignore other frames
            except ValueError as e:
                # line too long or malformed
                print(f"[Gateway] Read line too large or malformed: {e}")
                break
            except json.JSONDecodeError as e:
                print(f"[Gateway] Invalid JSON from worker: {e}")
                continue
            except Exception as e:
                print(f"[Gateway] Worker error: {e}")
                break
    finally:
        worker_writer = None
        writer.close()
        await writer.wait_closed()
        print("[Gateway] Worker disconnected")

# ----------------------------------------------------------------------
# SSE endpoint (unchanged)
# ----------------------------------------------------------------------
@app.get("/stream")
async def stream(request: Request):
    last_id = request.headers.get("Last-Event-ID")
    last_tick = int(last_id) if last_id and last_id.isdigit() else None

    q = asyncio.Queue(maxsize=100)
    client_queues.add(q)

    async def event_generator():
        try:
            yield f"event: manifest\ndata: {json.dumps({'schema_version':1})}\n\n"

            if last_tick is None or ring.get_missed_deltas(last_tick) is None:
                all_entities = []
                for t, d in current_state.items():
                    all_entities.extend(d.values())
                snap = {"type": "SNAPSHOT", "tick": 0, "entities": all_entities}
                yield f"event: delta\ndata: {json.dumps(snap)}\n\n"
            else:
                missed = ring.get_missed_deltas(last_tick)
                if missed:
                    for m in missed:
                        yield f"event: delta\ndata: {m}\n\n"

            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"event: delta\ndata: {payload}\n\n"
                except asyncio.TimeoutError:
                    yield "event: keepalive\ndata: {}\n\n"
        finally:
            client_queues.discard(q)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

# ----------------------------------------------------------------------
# Command endpoints (unchanged)
# ----------------------------------------------------------------------
@app.post("/join")
async def join(request: Request):
    data = await request.json()
    pid = data.get("pid", 0)
    await forward_to_worker("join", {"pid": pid})
    return {"ok": True}

@app.post("/move")
async def move(request: Request):
    data = await request.json()
    await forward_to_worker("move", data)
    return {"ok": True}

@app.post("/fire")
async def fire(request: Request):
    data = await request.json()
    await forward_to_worker("fire", data)
    return {"ok": True}

@app.post("/reset")
async def reset():
    await forward_to_worker("reset")
    return {"ok": True}

@app.post("/spawn")
async def spawn(request: Request):
    data = await request.json()
    await forward_to_worker("spawn", data)
    return {"ok": True}

@app.post("/clear")
async def clear():
    await forward_to_worker("clear")
    return {"ok": True}

# ----------------------------------------------------------------------
# Static & client JS (unchanged)
# ----------------------------------------------------------------------
@app.get("/client/stateClient.js")
def serve_client_js():
    return HTMLResponse(content=get_client_js_content(), media_type="application/javascript")

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    with open("static/index.html") as f:
        return HTMLResponse(f.read())

# ----------------------------------------------------------------------
# Startup using lifespan instead of on_event (to avoid deprecation)
# ----------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    asyncio.create_task(ipc_server())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)