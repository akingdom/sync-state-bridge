"""
sync_state/supervisor.py

Optional process supervisor to launch and monitor a Simulation Worker and HTTP Gateway.
Restarts child processes if they crash.
Extended to create a socket pair for IPC.
"""

import subprocess
import time
import logging
import sys
import socket
import os
from typing import Optional

logger = logging.getLogger("sync_state.supervisor")


class Supervisor:
    def __init__(
        self,
        worker_script: str,
        gateway_script: str,
        restart_delay: float = 2.0,
        worker_args: Optional[list] = None,
        gateway_args: Optional[list] = None,
    ):
        self.worker_script = worker_script
        self.gateway_script = gateway_script
        self.restart_delay = restart_delay
        self.worker_args = worker_args or []
        self.gateway_args = gateway_args or []
        self._running = False
        self._worker_proc: Optional[subprocess.Popen] = None
        self._gateway_proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        self._running = True
        while self._running:
            # Create socket pair for IPC
            sock1, sock2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
            # Pass FDs to child processes
            worker_cmd = [sys.executable, "-u", self.worker_script] + self.worker_args + ["--ipc-fd", str(sock1.fileno())]
            gateway_cmd = [sys.executable, "-u", self.gateway_script] + self.gateway_args + ["--ipc-fd", str(sock2.fileno())]

            # Popen with pass_fds
            self._worker_proc = subprocess.Popen(
                worker_cmd,
                pass_fds=[sock1.fileno()],
                close_fds=False
            )
            self._gateway_proc = subprocess.Popen(
                gateway_cmd,
                pass_fds=[sock2.fileno()],
                close_fds=False
            )
            # Close parent copies
            sock1.close()
            sock2.close()

            # Wait for either to exit
            while self._running:
                time.sleep(1)
                if self._worker_proc.poll() is not None:
                    logger.warning("Worker process exited. Restarting...")
                    break
                if self._gateway_proc.poll() is not None:
                    logger.warning("Gateway process exited. Restarting...")
                    break
            self._terminate_all()
            time.sleep(self.restart_delay)

    def _terminate_all(self):
        for proc in (self._worker_proc, self._gateway_proc):
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        self._worker_proc = None
        self._gateway_proc = None

    def stop(self):
        self._running = False
        self._terminate_all()