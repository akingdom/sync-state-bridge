#!/usr/bin/env python3
"""
Gateway: FastAPI server using framed IPC for commands and deltas.
"""
import asyncio
import json
import sys
import struct
import socket
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from sync_state import DeltaRingBuffer
from sync_state.js import get_client_js_content
from sync_state.transports.ipc import read_payload, FRAME_DELTA

# We'll use the same framing constants as the worker
FRAME_COMMAND = 4
FRAME_HEADER_SIZE = 12
MAGIC = b"SSB1"
VERSION = 0x01

def pack_header(frame_type: int, payload_len: int, flags: int = 0) -> bytes:
    return struct.pack(">4sBBHI", MAGIC, VERSION, frame_type, flags, payload_len)

print("[Gateway] Starting up...", file=sys.stderr)

app = FastAPI()
ring = DeltaRingBuffer(capacity=500)
current_state = {}
client_queues = set()
worker_writer = None

async def forward_to_worker(action: str, params: dict = None):
    global worker_writer
    if not worker_writer:
        print("[Gateway] forward_to_worker: worker_writer is None", file=sys.stderr)
        return False
    cmd = {"action": action, "params": params or {}}
    try:
        json_bytes = json.dumps(cmd).encode('utf-8')
        header = pack_header(FRAME_COMMAND, len(json_bytes), 0)
        frame = header + json_bytes
        print(f"[Gateway] Sending command frame: {cmd}")
        worker_writer.write(frame)
        await worker_writer.drain()
        return True
    except Exception as e:
        print(f"[Gateway] Error writing command: {e}", file=sys.stderr)
        return False

async def ping_worker():
    global worker_writer
    while True:
        await asyncio.sleep(2.0)
        if worker_writer:
            try:
                cmd = {"action": "ping", "params": {}}
                json_bytes = json.dumps(cmd).encode('utf-8')
                header = pack_header(FRAME_COMMAND, len(json_bytes), 0)
                frame = header + json_bytes
                worker_writer.write(frame)
                await worker_writer.drain()
                print("[Gateway] Ping sent")
            except Exception as e:
                print(f"[Gateway] Ping error: {e}")

async def ipc_server():
    server = await asyncio.start_server(
        handle_worker,
        '127.0.0.1', 8766
    )
    print("[Gateway] IPC server listening on 8766")
    asyncio.create_task(ping_worker())
    async with server:
        await server.serve_forever()

async def handle_worker(reader, writer):
    global worker_writer
    worker_writer = writer
    sock = writer.transport.get_extra_info('socket')
    if sock:
        print(f"[Gateway] Worker connected, fd: {sock.fileno()}")
        # Disable Nagle for low latency
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        while True:
            try:
                frame_type, payload = await read_payload(reader)
                if frame_type is None:
                    break
                if frame_type == FRAME_DELTA:
                    line = payload.decode('utf-8').strip()
                    if not line:
                        continue
                    frame = json.loads(line)
                    tick = frame.get("tick")
                    if tick is not None:
                        ring.append(tick, line)
                    for d in frame.get("deltas", []):
                        eid = d["id"]
                        op = d["op"]
                        t = d.get("type", "unknown")
                        if t not in current_state:
                            current_state[t] = {}
                        if op == "delete":
                            current_state[t].pop(eid, None)
                        else:
                            entity = {"id": eid, **d.get("changes", {})}
                            current_state[t][eid] = entity
                    for q in list(client_queues):
                        try:
                            q.put_nowait(line)
                        except asyncio.QueueFull:
                            client_queues.remove(q)
            except asyncio.IncompleteReadError:
                break
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
                state_copy = current_state.copy()
                all_entities = []
                for t, d in state_copy.items():
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
# Command endpoints
# ----------------------------------------------------------------------
@app.post("/join")
async def join(request: Request):
    data = await request.json()
    pid = data.get("pid", 0)
    if not await forward_to_worker("join", {"pid": pid}):
        raise HTTPException(status_code=503, detail="Worker not connected")
    return {"ok": True}

@app.post("/move")
async def move(request: Request):
    data = await request.json()
    if not await forward_to_worker("move", data):
        raise HTTPException(status_code=503, detail="Worker not connected")
    return {"ok": True}

@app.post("/fire")
async def fire(request: Request):
    data = await request.json()
    if not await forward_to_worker("fire", data):
        raise HTTPException(status_code=503, detail="Worker not connected")
    return {"ok": True}

@app.post("/reset")
async def reset():
    if not await forward_to_worker("reset"):
        raise HTTPException(status_code=503, detail="Worker not connected")
    return {"ok": True}

@app.post("/spawn")
async def spawn(request: Request):
    data = await request.json()
    if not await forward_to_worker("spawn", data):
        raise HTTPException(status_code=503, detail="Worker not connected")
    return {"ok": True}

@app.post("/clear")
async def clear():
    if not await forward_to_worker("clear"):
        raise HTTPException(status_code=503, detail="Worker not connected")
    return {"ok": True}

# ----------------------------------------------------------------------
# Static & client JS
# ----------------------------------------------------------------------
@app.get("/client/stateClient.js")
def serve_client_js():
    return HTMLResponse(content=get_client_js_content(), media_type="application/javascript")

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    with open("static/index.html") as f:
        return HTMLResponse(f.read())

@app.on_event("startup")
async def startup():
    asyncio.create_task(ipc_server())

if __name__ == "__main__":
    import uvicorn
    print("[Gateway] Launching uvicorn...", file=sys.stderr)
    uvicorn.run(app, host="0.0.0.0", port=8000)
    print("[Gateway] Shut down.", file=sys.stderr)