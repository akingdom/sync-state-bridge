#!/usr/bin/env python3
"""
Launches worker and gateway under supervisor.
"""
from sync_state import Supervisor

if __name__ == "__main__":
    sup = Supervisor(
        worker_script="worker.py",
        gateway_script="gateway.py",
        restart_delay=1.0
    )
    try:
        sup.start()
    except KeyboardInterrupt:
        sup.stop()