"""
sync_state/app/server.py

SyncServer: convenience base class that sets up the Router, optionally an IPC transport,
and optionally HTTP routes, and runs the server.
"""

import asyncio
import uvicorn
from typing import Optional, List
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ..core.router import Router
from ..core.ipc_transport import IPCTransport
from ..web.httphelper import create_http_routes


class SyncServer:
    """
    Convenience server for the sync system.

    - Creates a Router.
    - Optionally creates an IPC transport (listening on worker_ipc_port) if enable_worker_ipc is True.
    - Optionally mounts HTTP/SSE routes for web clients.
    - Optionally serves static files.
    - Runs the server.

    Subclass and override on_startup() for custom logic.
    """

    def __init__(
        self,
        enable_worker_ipc: bool = True,
        worker_ipc_port: int = 8766,
        host: str = "127.0.0.1",
        port: int = 8000,
        static_dir: Optional[str] = None,
        client_sendable_types: Optional[List[str]] = None,
        server_broadcast_types: Optional[List[str]] = None,
        http_path_prefix: str = "",
        router: Optional[Router] = None,
    ):
        self.host = host
        self.port = port
        self.static_dir = static_dir
        self.client_sendable_types = client_sendable_types or []  # outgoing from client
        self.server_broadcast_types = server_broadcast_types or []  # incoming to client
        self.http_path_prefix = http_path_prefix

        self.router = router or Router()
        self.ipc_transport = None
        if enable_worker_ipc:
            self.ipc_transport = IPCTransport(listen_on=f"127.0.0.1:{worker_ipc_port}")
            self.router.register_transport(
                self.ipc_transport,
                type_list=self.server_broadcast_types,
                direction="in",
            )
            self.router.register_transport(
                self.ipc_transport,
                type_list=self.client_sendable_types,
                direction="out",
            )

        self.app = FastAPI()
        self._broadcast = None

    async def on_startup(self):
        """Override for custom startup logic."""
        pass

    def run(self):
        """Start the server."""
        http_router = create_http_routes(
            self.router,
            client_sendable_types=self.client_sendable_types,
            server_broadcast_types=self.server_broadcast_types,
            path_prefix=self.http_path_prefix,
        )
        self.app.include_router(http_router)
        self._broadcast = http_router.broadcast

        if self.static_dir:
            self.app.mount("/", StaticFiles(directory=self.static_dir, html=True), name="static")

        loop = asyncio.get_event_loop()
        if self.ipc_transport:
            loop.create_task(self.ipc_transport.start())
        loop.create_task(self.on_startup())

        uvicorn.run(self.app, host=self.host, port=self.port)