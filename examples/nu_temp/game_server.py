#!/usr/bin/env python3
"""
Game Server – uses SyncStateBridge to run Router and SynchronisationKernel.
Exposes /stats for adaptive control and /healthz/metrics for passive monitoring.
Threaded version (the stable one).
"""

import threading
import asyncio
import sys
import time
from fastapi.responses import JSONResponse
from sync_state.core.sync_bridge import SyncStateBridge
from sync_state.core.ipc_transport import IPCTransport
from sync_state.web import HTTPSSETransport
from sync_state.observability import PassiveHealthMonitor

print("[GameServer] Starting...", file=sys.stderr)

def run_server():
    bridge = SyncStateBridge(kernel_capacity=5000)

    ipc = IPCTransport(listen_on="127.0.0.1:8766")
    http = HTTPSSETransport(host="127.0.0.1", port=8000, static_dir="static")

    qos_map = {
        "player": 3,
        "asteroid": 3,
        "bullet": 2,
        "particle": 1,
    }

    # Register all relevant types, including administrative ones
    bridge.register_transport(
        ipc,
        type_list=["asteroid", "particle", "player", "bullet"],
        direction="in",
    )
    bridge.register_transport(
        ipc,
        type_list=["controls", "game:reset", "game:spawn", "game:clear"],
        direction="out",
        qos_map=qos_map,
    )
    bridge.register_transport(
        http,
        type_list=["controls", "game:reset", "game:spawn", "game:clear"],
        direction="in",
    )
    bridge.register_transport(
        http,
        type_list=["asteroid", "particle", "player", "bullet"],
        direction="out",
        qos_map=qos_map,
    )

    bridge.start()

    # Add /stats endpoint
    async def stats_endpoint():
        rec = bridge.governor.evaluate(current_queue_depth=0)
        return JSONResponse(content=rec)

    # Add /healthz/metrics endpoint for passive monitoring
    monitor = PassiveHealthMonitor(bridge, http, ipc)

    async def metrics_endpoint():
        return JSONResponse(content=monitor.get_telemetry_snapshot())

    http.add_route("/stats", stats_endpoint)
    http.add_route("/healthz/metrics", metrics_endpoint)

    # Finalize routes and static directory mounting
    http.finalize()

    # Start transports
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(ipc.start())
    http.start()

    http.on_frame(lambda frame: bridge.submit(frame, http))
    ipc.on_frame(lambda frame: bridge.submit(frame, ipc))

    # Main loop – process HTTP frames from clients
    while True:
        try:
            frame = http._in_queue.get_nowait()
            if frame is None:
                break
            bridge.submit(frame, http)
        except:
            pass
        loop.run_until_complete(asyncio.sleep(0.001))

    loop.close()

if __name__ == "__main__":
    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[GameServer] Shutting down.")