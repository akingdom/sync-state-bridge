#!/usr/bin/env python3
from sync_state import Supervisor
import sys

if __name__ == "__main__":
    print("[Supervisor] Starting worker and gateway...")
    sup = Supervisor(
        worker_script="mvp_worker.py",
        gateway_script="mvp_gateway.py",
        restart_delay=1.0
    )
    try:
        sup.start()
    except KeyboardInterrupt:
        sup.stop()