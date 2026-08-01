"""
sync_state/app/worker.py

SyncWorker: convenience base class for the worker process.
Connects to the server via IPC and runs a loop.
"""

import asyncio
from ..core.ipc_transport import IPCTransport


class SyncWorker:
    """
    Convenience worker for the sync system.

    - Connects to the server's IPC port.
    - Calls on_frame() for each incoming frame.
    - Calls on_tick() at a fixed interval.
    - Subclass and override on_start(), on_frame(), on_tick().
    """

    def __init__(
        self,
        gateway_ipc_port: int = 8766,
        tick_interval: float = 1/60,
    ):
        self.gateway_ipc_port = gateway_ipc_port
        self.tick_interval = tick_interval
        self.ipc = IPCTransport(connect_to=f"127.0.0.1:{gateway_ipc_port}")
        self._running = False

    def send(self, frame: dict):
        """Send a frame to the gateway."""
        self.ipc.emit(frame)

    async def on_start(self):
        """Override for custom startup logic."""
        pass

    async def on_frame(self, frame: dict):
        """Called for each incoming frame."""
        pass

    async def on_tick(self):
        """Called at each tick interval."""
        pass

    async def run(self):
        """Start the worker loop."""
        self._running = True
        await self.ipc.start()
        self.ipc.on_frame(lambda f: asyncio.create_task(self.on_frame(f)))

        await self.on_start()

        last_tick = asyncio.get_running_loop().time()
        while self._running:
            now = asyncio.get_running_loop().time()
            if now - last_tick >= self.tick_interval:
                await self.on_tick()
                last_tick = now
            await asyncio.sleep(0.001)

    def stop(self):
        self._running = False