#!/usr/bin/env python3
"""
Gateway – uses Router for command routing and client queues.
- /command: generic endpoint (no game-specific endpoints)
- /stream: SSE with snapshot on connect
- IPC with worker (framed)
"""
import asyncio
import json
import sys
import struct
import socket
from typing import Optional
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from sync_state import DeltaRingBuffer
from sync_state.js import get_client_js_content
from sync_state.router import Router, RouterEntry
from sync_state.qos import TypeMetadata, SyncDirection

# Framing constants (same as worker)
MAGIC = b"SSB1"
VERSION = 0x01
FRAME_HEADER_SIZE = 12
FRAME_COMMAND = 4
FRAME_DELTA = 2
FRAME_COMMAND_ACK = 6

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

# ----------------------------------------------------------------------
# Global state – set in startup
# ----------------------------------------------------------------------
router: Optional[Router] = None
worker_queue: Optional[asyncio.Queue] = None
write_lock: Optional[asyncio.Lock] = None
worker_writer: Optional[asyncio.StreamWriter] = None
current_state = {}
ring = DeltaRingBuffer(capacity=500)

app = FastAPI()

# ----------------------------------------------------------------------
# IPC Server – handles worker connection
# ----------------------------------------------------------------------
async def ipc_server():
    server = await asyncio.start_server(handle_worker, '127.0.0.1', 8766)
    print("[Gateway] IPC server listening on 8766")
    async with server:
        await server.serve_forever()

async def forward_to_worker(frame_bytes: bytes) -> bool:
    """Send a frame to the worker over the IPC connection."""
    global worker_writer
    if worker_writer is None:
        return False
    # write_lock is a global set in startup
    async with write_lock:
        try:
            worker_writer.write(frame_bytes)
            await worker_writer.drain()
            return True
        except Exception:
            return False

async def handle_worker(reader, writer):
    global worker_writer, current_state
    worker_writer = writer
    sock = writer.transport.get_extra_info('socket')
    if sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        print(f"[Gateway] Worker connected, fd: {sock.fileno()}")

    # Task that reads from worker_queue and forwards commands to worker
    async def send_commands_to_worker():
        while True:
            try:
                frame = await worker_queue.get()
                if frame is None:
                    break
                if not await forward_to_worker(frame):
                    break
            except Exception as e:
                print(f"[Gateway] Error forwarding command: {e}", file=sys.stderr)
                break

    send_task = asyncio.create_task(send_commands_to_worker())

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
                # Broadcast to all clients via router
                await router.broadcast_delta(payload)
            elif frame_type == FRAME_COMMAND_ACK:
                await router.resolve_ack(payload)
            else:
                # ignore other frames
                pass
    except Exception as e:
        print(f"[Gateway] Worker error: {e}", file=sys.stderr)
    finally:
        worker_writer = None
        send_task.cancel()
        writer.close()
        await writer.wait_closed()
        print("[Gateway] Worker disconnected")

# ----------------------------------------------------------------------
# HTTP endpoints
# ----------------------------------------------------------------------
@app.post("/command")
async def command_endpoint(client_id: str = Query(...), payload: dict = None):
    """Generic command endpoint – forwards to router."""
    if payload is None:
        raise HTTPException(400, "Missing payload")
    try:
        receipt = await router.handle_frame_from_http(client_id, json.dumps(payload).encode())
        return receipt
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/stream")
async def stream_endpoint(client_id: str = Query(...)):
    """SSE stream – sends snapshot then deltas from router's client queue."""
    queue = router.get_client_queue(client_id)

    # Build snapshot from current_state
    all_entities = []
    for t, d in current_state.items():
        all_entities.extend(d.values())
    snapshot = {"type": "SNAPSHOT", "tick": 0, "entities": all_entities}
    snapshot_line = json.dumps(snapshot)

    async def event_generator():
        yield f"event: manifest\ndata: {json.dumps({'schema_version':1})}\n\n"
        yield f"event: delta\ndata: {snapshot_line}\n\n"

        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=15)
                event_type = item.get("event", "delta")
                data = item.get("data", "")
                yield f"event: {event_type}\ndata: {data}\n\n"
            except asyncio.TimeoutError:
                yield "event: keepalive\ndata: {}\n\n"
            except Exception as e:
                print(f"[Gateway] Stream error: {e}", file=sys.stderr)
                # keep going

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )

# ----------------------------------------------------------------------
# Static files and index
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
# Startup – create asyncio primitives inside the loop
# ----------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    global router, worker_queue, write_lock
    # Create primitives bound to the current loop
    worker_queue = asyncio.Queue()
    write_lock = asyncio.Lock()
    # Router needs the worker queue
    type_config = {
        "game": RouterEntry(
            worker_queue=worker_queue,
            metadata=TypeMetadata(
                type_name="game",
                direction=SyncDirection.BIDIRECTIONAL,
                ack_timeout_ms=None
            )
        )
    }
    router = Router(type_config)
    # Start the IPC server
    asyncio.create_task(ipc_server())

if __name__ == "__main__":
    import uvicorn
    print("[Gateway] Launching uvicorn...", file=sys.stderr)
    uvicorn.run(app, host="0.0.0.0", port=8000)