#!/usr/bin/env python3
"""
Gateway – robust SSE stream, resilient command forwarding.
"""
import asyncio
import json
import sys
import struct
import socket
import logging
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from sync_state import DeltaRingBuffer
from sync_state.js import get_client_js_content

# Framing constants
MAGIC = b"SSB1"
VERSION = 0x01
FRAME_HEADER_SIZE = 12
FRAME_COMMAND = 4
FRAME_DELTA = 2

def pack_header(frame_type: int, payload_len: int, flags: int = 0) -> bytes:
    return struct.pack(">4sBBHI", MAGIC, VERSION, frame_type, flags, payload_len)

def unpack_header(data: bytes):
    magic, version, frame_type, flags, length = struct.unpack(">4sBBHI", data)
    if magic != MAGIC:
        raise ValueError("Invalid magic")
    if version != VERSION:
        raise ValueError(f"Unsupported version: {version}")
    return frame_type, flags, length

async def read_frame(reader: asyncio.StreamReader):
    try:
        header = await reader.readexactly(FRAME_HEADER_SIZE)
    except asyncio.IncompleteReadError:
        return None
    frame_type, flags, length = unpack_header(header)
    if length > 16 * 1024 * 1024:
        raise ValueError("Frame too large")
    payload = await reader.readexactly(length)
    return frame_type, payload

print("[Gateway] Starting up...", file=sys.stderr)

app = FastAPI()
ring = DeltaRingBuffer(capacity=500)
current_state = {}
client_queues = set()
worker_writer = None
write_lock = None

# Track client versions
client_last_versions = {}
client_queue_map = {}

async def forward_to_worker(action: str, params: dict = None):
    global worker_writer, write_lock
    if write_lock is None:
        raise RuntimeError("Lock not initialised")
    if worker_writer is None:
        print("[Gateway] Worker not connected", file=sys.stderr)
        return False
    cmd = {"action": action, "params": params or {}}
    json_bytes = json.dumps(cmd).encode('utf-8')
    header = pack_header(FRAME_COMMAND, len(json_bytes), 0)
    frame = header + json_bytes
    async with write_lock:
        try:
            worker_writer.write(frame)
            await worker_writer.drain()
            print(f"[Gateway] Sent command: {action} {params}")
            return True
        except Exception as e:
            print(f"[Gateway] Error forwarding command: {e}", file=sys.stderr)
            return False

async def ipc_server():
    server = await asyncio.start_server(handle_worker, '127.0.0.1', 8766)
    print("[Gateway] IPC server listening on 8766")
    async with server:
        await server.serve_forever()

async def handle_worker(reader, writer):
    global worker_writer, current_state
    worker_writer = writer
    sock = writer.transport.get_extra_info('socket')
    if sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        print(f"[Gateway] Worker connected, fd: {sock.fileno()}")
    try:
        while True:
            frame = await read_frame(reader)
            if frame is None:
                break
            frame_type, payload = frame
            if frame_type == FRAME_DELTA:
                line = payload.decode('utf-8').strip()
                if not line:
                    continue
                try:
                    frame_data = json.loads(line)
                except json.JSONDecodeError:
                    print("[Gateway] Invalid delta JSON", file=sys.stderr)
                    continue
                tick = frame_data.get("tick")
                if tick is not None:
                    ring.append(tick, line)
                for d in frame_data.get("deltas", []):
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
                # Broadcast to all clients
                for q in list(client_queues):
                    try:
                        q.put_nowait(line)
                    except asyncio.QueueFull:
                        # Remove dead queue
                        for cid, qq in list(client_queue_map.items()):
                            if qq == q:
                                del client_queue_map[cid]
                                break
            elif frame_type == FRAME_COMMAND:
                cmd = json.loads(payload.decode())
                action = cmd.get("action")
                if action == "get_min_versions":
                    min_versions = {}
                    if client_last_versions:
                        all_types = set()
                        for v in client_last_versions.values():
                            all_types.update(v.keys())
                        for t in all_types:
                            min_ver = min((v.get(t, 0) for v in client_last_versions.values()), default=0)
                            min_versions[t] = min_ver
                    response = {"action": "min_versions_response", "versions": min_versions}
                    json_bytes = json.dumps(response).encode('utf-8')
                    header = pack_header(FRAME_COMMAND, len(json_bytes), 0)
                    writer.write(header + json_bytes)
                    await writer.drain()
    except Exception as e:
        print(f"[Gateway] Worker error: {e}")
    finally:
        worker_writer = None
        writer.close()
        await writer.wait_closed()
        print("[Gateway] Worker disconnected")

@app.get("/stream")
async def stream(request: Request, client_id: str = Query(...)):
    last_id = request.headers.get("Last-Event-ID")
    last_tick = int(last_id) if last_id and last_id.isdigit() else None

    q = asyncio.Queue(maxsize=100)
    client_queue_map[client_id] = q
    client_queues.add(q)
    client_last_versions[client_id] = {}

    async def event_generator():
        try:
            # Send manifest
            yield f"event: manifest\ndata: {json.dumps({'schema_version':1})}\n\n"

            # Send snapshot if needed
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
                        try:
                            data = json.loads(m)
                            versions = data.get("versions", {})
                            for t, v in versions.items():
                                client_last_versions[client_id][t] = v
                        except:
                            pass

            # Main loop – robust exception handling
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"event: delta\ndata: {payload}\n\n"
                    try:
                        data = json.loads(payload)
                        versions = data.get("versions", {})
                        for t, v in versions.items():
                            client_last_versions[client_id][t] = v
                    except:
                        pass
                except asyncio.TimeoutError:
                    # Send keepalive
                    yield "event: keepalive\ndata: {}\n\n"
                except Exception as e:
                    # Log but don't break – keep the stream alive
                    print(f"[Gateway] Stream error (continuing): {e}", file=sys.stderr)
                    yield "event: error\ndata: {}\n\n"
        except Exception as e:
            print(f"[Gateway] Fatal stream error: {e}", file=sys.stderr)
        finally:
            client_queues.discard(q)
            client_queue_map.pop(client_id, None)
            client_last_versions.pop(client_id, None)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        }
    )

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

@app.get("/client/stateClient.js")
def serve_client_js():
    return HTMLResponse(content=get_client_js_content(), media_type="application/javascript")

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    with open("static/index.html") as f:
        return HTMLResponse(f.read())

write_lock = None

@app.on_event("startup")
async def startup():
    global write_lock
    write_lock = asyncio.Lock()
    asyncio.create_task(ipc_server())

if __name__ == "__main__":
    import uvicorn
    print("[Gateway] Launching uvicorn...", file=sys.stderr)
    uvicorn.run(app, host="0.0.0.0", port=8000)