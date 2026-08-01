"""
sync_state/web/httphelper.py

FastAPI route builder for HTTP/SSE (Server-Sent Events) and POST /update.
This is the low-level helper; applications can use it directly or via HTTPServer.
"""

import json
import asyncio
from typing import List
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import StreamingResponse
from ..core.router import Router

def create_http_routes(
    router: Router,
    client_sendable_types: List[str],
    server_broadcast_types: List[str],
    path_prefix: str = "",
) -> APIRouter:
    """
    Create FastAPI routes for /stream and /update that interface with the given Router.

    Returns:
        APIRouter with routes and a broadcast function attached.
    """
    api = APIRouter(prefix=path_prefix)
    client_queues: List[asyncio.Queue] = []

    @api.post("/update")
    async def update_frame(request: Request) -> dict:
        try:
            frame = await request.json()
        except Exception:
            raise HTTPException(400, "Invalid JSON")
        type_name = frame.get("type")
        if not type_name:
            raise HTTPException(400, "Frame missing 'type'")
        if type_name not in client_sendable_types:
            raise HTTPException(403, f"Client cannot send type '{type_name}'")
        router.route(frame, source_transport="http_client")
        return {"ok": True}

    @api.get("/stream")
    async def stream_events(
        request: Request,
        client_id: str = Query("", description="Client identifier"),
    ):
        queue = asyncio.Queue()
        client_queues.append(queue)

        async def event_generator():
            try:
                yield f"event: manifest\ndata: {json.dumps({'schema_version': 1})}\n\n"
                while True:
                    try:
                        payload = await asyncio.wait_for(queue.get(), timeout=15)
                        yield f"event: delta\ndata: {payload}\n\n"
                    except asyncio.TimeoutError:
                        yield "event: keepalive\ndata: {}\n\n"
            finally:
                if queue in client_queues:
                    client_queues.remove(queue)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            }
        )

    def broadcast(payload: str) -> None:
        """Broadcast a JSON string to all SSE clients."""
        for q in client_queues:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                pass

    api.broadcast = broadcast
    return api