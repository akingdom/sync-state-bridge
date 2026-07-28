#!/usr/bin/env python3
"""
Launches worker and gateway under supervisor.
"""
from sync_state import Supervisor
import sys

if __name__ == "__main__":
    print("[Supervisor] Starting worker and gateway...", file=sys.stderr)
    sup = Supervisor(
        worker_script="worker.py",
        gateway_script="gateway.py",
        restart_delay=1.0
    )
    try:
        sup.start()
    except KeyboardInterrupt:
        print("[Supervisor] Shutting down.", file=sys.stderr)
        sup.stop()