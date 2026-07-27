"""
sync_state/supervisor.py

Optional process supervisor to launch and monitor a Simulation Worker and HTTP Gateway.
Restarts child processes if they crash.
"""

import subprocess
import time
import logging
import sys
from typing import Optional

logger = logging.getLogger("sync_state.supervisor")


class Supervisor:
    """
    Manages two child processes: a worker and a gateway.
    Restarts either process if it exits unexpectedly.
    """

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
        """Launch both processes and monitor them indefinitely."""
        self._running = True
        while self._running:
            self._launch_worker()
            self._launch_gateway()
            # Wait for either to exit
            while self._running:
                time.sleep(1)
                if self._worker_proc and self._worker_proc.poll() is not None:
                    logger.warning("Worker process exited. Restarting...")
                    break
                if self._gateway_proc and self._gateway_proc.poll() is not None:
                    logger.warning("Gateway process exited. Restarting...")
                    break
            self._terminate_all()
            time.sleep(self.restart_delay)

    def _launch_worker(self) -> None:
        self._worker_proc = subprocess.Popen(
            [sys.executable, self.worker_script] + self.worker_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info("Worker started (PID %d)", self._worker_proc.pid)

    def _launch_gateway(self) -> None:
        self._gateway_proc = subprocess.Popen(
            [sys.executable, self.gateway_script] + self.gateway_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info("Gateway started (PID %d)", self._gateway_proc.pid)

    def _terminate_all(self) -> None:
        for proc in (self._worker_proc, self._gateway_proc):
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        self._worker_proc = None
        self._gateway_proc = None

    def stop(self) -> None:
        """Stop supervision and terminate child processes."""
        self._running = False
        self._terminate_all()