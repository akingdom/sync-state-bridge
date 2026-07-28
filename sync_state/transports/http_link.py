"""
sync_state/transports/http_link.py

HTTP/SSE adapter for web browsers. Binds to localhost by default.
Translates HTTP requests and SSE streams to/from internal framed messages
over a local queue.
"""

import asyncio
import json
import uuid
import logging
from typing import Optional
from fastapi import FastAPI, Query, Body, HTTPException
from fastapi.responses import StreamingResponse
import uvicorn

logger = logging.getLogger("sync_state.http_link")


class HTTPLink:
    """Adapter between HTTP/SSE and the router."""

    def __init__(self, router, host: str = "127.0.0.1", port: int = 8000):
        self.router = router
        self.host = host
        self.port = port
        self.app = FastAPI()
        self._setup_routes()

    def _setup_routes(self):
        app = self.app

        @app.post("/command")
        async def post_command(
            client_id: str = Query(...),
            payload: dict = Body(...)
        ):
            try:
                # Validate required fields
                if "action" not in payload:
                    raise HTTPException(400, "Missing 'action'")
                # Generate command_id if absent
                if "command_id" not in payload:
                    payload["command_id"] = str(uuid.uuid4())
                # Build frame bytes (JSON)
                frame_bytes = (json.dumps(payload) + "\n").encode()
                # Pass to router
                receipt = await self.router.handle_frame_from_http(client_id, frame_bytes)
                return receipt
            except HTTPException:
                # Re-raise HTTP exceptions directly
                raise
            except Exception as e:
                logger.exception("Command error")
                raise HTTPException(500, "Internal server error")

        @app.get("/stream")
        async def stream_events(client_id: str = Query(...)):
            queue = self.router.get_client_queue(client_id)
            async def event_generator():
                while True:
                    item = await queue.get()
                    if item is None:
                        break
                    event_type = item.get("event")
                    data = item.get("data")
                    yield f"event: {event_type}\ndata: {data}\n\n"
            return StreamingResponse(event_generator(), media_type="text/event-stream")

    async def run(self):
        config = uvicorn.Config(self.app, host=self.host, port=self.port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()