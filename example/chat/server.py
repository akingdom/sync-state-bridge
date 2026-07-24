#!/usr/bin/env python3
import sys
from pathlib import Path

# Add repo root to Python path (absolute)
repo_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(repo_root))

import asyncio
import json
import uuid
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from sync_state import StateSync

app = FastAPI()
sync = StateSync(id_key="id", max_history=50)

# Store messages as dicts with stable UUIDs
messages = []

def get_messages():
    return messages

sync.register_snapshot_provider("message", get_messages)

@app.post("/send")
async def send_message(request: Request):
    data = await request.json()
    msg_text = data.get("text", "")
    if msg_text:
        msg_obj = {"id": str(uuid.uuid4()), "text": msg_text}
        messages.append(msg_obj)
        sync.mark_dirty("message")
        await sync.commit()
    return {"ok": True}

@app.get("/stream")
async def stream(versions: str = "{}"):
    try:
        client_versions = json.loads(versions)
    except:
        client_versions = {}
    return StreamingResponse(sync.stream_deltas(client_versions), media_type="text/event-stream")

@app.get("/")
async def index():
    with open("static/index.html") as f:
        return HTMLResponse(f.read())

# Mount static files (UI) and client library
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/client", StaticFiles(directory=str(repo_root / "client" )), name="client")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)