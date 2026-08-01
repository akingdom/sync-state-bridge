# filename: sync_state/web/http_sse_transport.py

import threading
import queue
import asyncio
import json
import logging
from typing import Callable, Dict, Any, Optional, List, Set
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from ..core.transport_adapter import TransportAdapter
from .client_js import get_client_js_content

logger = logging.getLogger(__name__)


class HTTPSSETransport(TransportAdapter):
    def __init__(self, host="127.0.0.1", port=8000, static_dir: Optional[str] = None):
        self.host = host
        self.port = port
        self.static_dir = static_dir
        self._callback = None
        self._in_queue = queue.Queue()
        self._out_queues: Set[asyncio.Queue] = set()
        self._thread = None
        self._server = None
        self._loop = None
        # EXPLICIT FIX: Disable lifespan in FastAPI to prevent TestClient / ASGI lifespan tasks
        self._app = FastAPI(lifespan=None)
        self._setup_routes()
        self._server: Optional[uvicorn.Server] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._frame_handler: Optional[Callable] = None

    def _setup_routes(self):
        app = self._app

        @app.post("/update")
        async def update_frame(payload: dict = None):
            if payload is None:
                raise HTTPException(400, "Missing payload")
            if "type" not in payload:
                raise HTTPException(400, "Frame missing 'type'")
            self._in_queue.put(payload)
            return {"status": "accepted"}

        @app.get("/stream")
        async def stream(request: Request, client_id: str = Query(...)):
            q = asyncio.Queue()
            self._out_queues.add(q)

            async def event_generator():
                try:
                    yield f"event: manifest\ndata: {json.dumps({'schema_version':1})}\n\n"
                    while self._running:
                        if await request.is_disconnected():
                            break
                        try:
                            frame = await asyncio.wait_for(q.get(), timeout=0.5)
                            yield f"event: delta\ndata: {json.dumps(frame)}\n\n"
                        except asyncio.TimeoutError:
                            yield "event: keepalive\ndata: {}\n\n"
                finally:
                    self._out_queues.discard(q)

            return StreamingResponse(
                event_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                }
            )

        @app.get("/client/stateClient.js")
        async def serve_client_js():
            return HTMLResponse(
                content=get_client_js_content(),
                media_type="application/javascript"
            )

    def add_route(self, path: str, handler, methods: List[str] = ["GET"]):
        self._app.add_api_route(path, handler, methods=methods)

    def finalize(self):
        if self.static_dir:
            self._app.mount("/", StaticFiles(directory=self.static_dir, html=True), name="static")

    def start(self):
        """Start the HTTP SSE Uvicorn server in a background thread."""
        def run():
            self._running = True
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            config = uvicorn.Config(
                self._app,
                host=self.host,
                port=self.port,
                log_level="error",
                loop="asyncio",
                lifespan="off"  # EXPLICIT FIX: Disable Uvicorn lifespan handling
            )
            server = uvicorn.Server(config)
            self._server = server
            try:
                loop.run_until_complete(server.serve())
            except Exception:
                pass

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def emit(self, frame: Dict[str, Any]) -> None:
        for q in list(self._out_queues):
            try:
                q.put_nowait(frame)
            except Exception:
                pass

    def on_frame(self, callback: Callable[[Dict], None]) -> None:
        self._callback = callback

    def stop(self):
        """Forcefully and cleanly tear down background threads and loops."""
        self._running = False
        if self._server:
            self._server.should_exit = True
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)