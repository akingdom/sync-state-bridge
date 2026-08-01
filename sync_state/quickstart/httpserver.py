"""
sync_state/app/httpserver.py

HTTPServer: a subclass of SyncServer that runs without a worker IPC transport.
This is a web-only server, suitable for serving a client-side application
or for testing without a worker process.
"""

from .server import SyncServer


class HTTPServer(SyncServer):
    """
    Web-only server: no IPC connection to a worker.
    Only HTTP/SSE routes are served.
    """

    def __init__(self, **kwargs):
        # Force enable_worker_ipc to False
        kwargs["enable_worker_ipc"] = False
        super().__init__(**kwargs)