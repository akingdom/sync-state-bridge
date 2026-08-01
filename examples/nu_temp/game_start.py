#!/usr/bin/env python3
# filename: game_start.py
from sync_state.quickstart.supervisor import Supervisor
import sys

if __name__ == "__main__":
    print("[Supervisor] Starting worker and gateway process tree...", file=sys.stderr)
    sup = Supervisor(
        worker_script="game_worker.py",
        gateway_script="game_server.py",
        restart_delay=1.0
    )
    try:
        sup.start()
    except KeyboardInterrupt:
        print("[Supervisor] Stopping processes...", file=sys.stderr)
        sup.stop()
