#!/usr/bin/env python3
import sys
from pathlib import Path

# Add repo root to Python path (absolute)
repo_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(repo_root))

import asyncio
import json
import random
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from sync_state import StateSync
from sync_state.js import get_client_js_content

app = FastAPI()
sync = StateSync(id_key="id", max_history=100)

# Game state
state = {
    "turn": "P1",
    "p1Target": [0,0,0,0,0,  0,1,1,0,0,  0,0,1,0,0,  0,0,0,0,0,  0,0,0,0,0],
    "p1Guessed": [None]*25,
    "p2Target": [0,0,0,0,0,  0,0,0,0,0,  0,0,1,1,0,  0,0,0,1,0,  0,0,0,0,0],
    "p2Guessed": [None]*25,
}

def get_state():
    # Return a list of entities for the "game" type
    return [{"id": "game", **state}]

sync.register_snapshot_provider("game", get_state)

@app.post("/move")
async def move(request: Request):
    data = await request.json()
    index = data.get("index")
    if index is None:
        return {"error": "No index"}
    # Process move (simplified: just toggle turn and make a guess)
    # In real game, this would be more complex
    if state["turn"] == "P1":
        is_hit = state["p2Target"][index] == 1
        state["p2Guessed"][index] = "hit" if is_hit else "miss"
        state["turn"] = "P2"
    else:
        # AI auto-turn
        unguessed = [i for i, g in enumerate(state["p1Guessed"]) if g is None]
        if unguessed:
            pick = random.choice(unguessed)
            is_hit = state["p1Target"][pick] == 1
            state["p1Guessed"][pick] = "hit" if is_hit else "miss"
        state["turn"] = "P1"
    sync.mark_dirty("game")
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

@app.get("/client/stateClient.js")
def serve_client_js():
    """
    Serves the client asset instantly from system RAM.
    """
    return HTMLResponse(
        content=get_client_js_content(),
        media_type="application/javascript"
    )

# Mount static files (UI) and client library
app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)