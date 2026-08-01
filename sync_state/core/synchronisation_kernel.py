"""
sync_state/core/synchronisation_kernel.py

KernelScheduler with conflation, TTL, and batching.
Manages QoS queues and dispatches jobs to transports.
"""

import time
import threading
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from collections import OrderedDict

logger = logging.getLogger("sync_state.kernel")


@dataclass
class SyncJob:
    job_id: str
    entity_id: Optional[str]
    qos_level: int  # 3: CRITICAL, 2: CONFLATABLE, 1: BEST_EFFORT
    payload: Dict[str, Any]
    target_transports: List[object]  # transports to deliver to
    created_at: float = field(default_factory=time.time)
    ttl_ms: float = 1000.0

    def is_expired(self) -> bool:
        return (time.time() - self.created_at) * 1000.0 > self.ttl_ms


class KernelScheduler:
    """
    Deterministic O(1) conflatable queue scheduler.
    Supports three QoS tiers with TTL and batching.
    """

    def __init__(self, capacity: int = 2000):
        self.capacity = capacity
        self._lock = threading.Lock()

        self._critical_jobs: List[SyncJob] = []
        self._conflatable_map: OrderedDict[str, SyncJob] = OrderedDict()
        self._best_effort_jobs: List[SyncJob] = []

        self._total_dropped = 0
        self._total_conflated = 0
        self._total_enqueued = 0

        # For drop rate estimation
        self._last_stats_time = time.time()
        self._last_drops = 0

    def submit(self, job: SyncJob) -> bool:
        with self._lock:
            self._total_enqueued += 1
            current_size = (len(self._critical_jobs) +
                            len(self._conflatable_map) +
                            len(self._best_effort_jobs))

            if job.qos_level == 1:  # BEST_EFFORT
                if current_size >= int(self.capacity * 0.8):
                    self._total_dropped += 1
                    logger.debug("BEST_EFFORT job dropped (queue >80%% full)")
                    return False
                self._best_effort_jobs.append(job)
                return True

            elif job.qos_level == 2:  # CONFLATABLE
                key = job.entity_id or f"anon_{time.time_ns()}"
                if key in self._conflatable_map:
                    self._total_conflated += 1
                    self._conflatable_map.move_to_end(key)
                self._conflatable_map[key] = job
                return True

            elif job.qos_level == 3:  # CRITICAL
                if current_size >= self.capacity:
                    # Drop oldest critical to make room
                    if self._critical_jobs:
                        self._critical_jobs.pop(0)
                        self._total_dropped += 1
                        logger.debug("Critical job dropped (queue full, oldest evicted)")
                    else:
                        # If no critical jobs, but other queues full, drop best effort
                        if self._best_effort_jobs:
                            self._best_effort_jobs.pop(0)
                            self._total_dropped += 1
                self._critical_jobs.append(job)
                return True

            return False

    def pop_batch(self, max_batch_size: int = 100) -> List[SyncJob]:
        """Pop up to max_batch_size jobs, respecting priority and TTL."""
        batch: List[SyncJob] = []
        with self._lock:
            # Critical first
            while self._critical_jobs and len(batch) < max_batch_size:
                job = self._critical_jobs.pop(0)
                if not job.is_expired():
                    batch.append(job)

            # Conflatable next
            while self._conflatable_map and len(batch) < max_batch_size:
                _, job = self._conflatable_map.popitem(last=False)
                if not job.is_expired():
                    batch.append(job)

            # Best effort last
            while self._best_effort_jobs and len(batch) < max_batch_size:
                job = self._best_effort_jobs.pop(0)
                if not job.is_expired():
                    batch.append(job)

        return batch

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            drop_rate = 0.0
            if self._last_stats_time and self._total_dropped > self._last_drops:
                elapsed = now - self._last_stats_time
                if elapsed > 0:
                    drop_rate = (self._total_dropped - self._last_drops) / elapsed
            self._last_stats_time = now
            self._last_drops = self._total_dropped

            return {
                "critical_depth": len(self._critical_jobs),
                "conflatable_depth": len(self._conflatable_map),
                "best_effort_depth": len(self._best_effort_jobs),
                "total_dropped": self._total_dropped,
                "total_conflated": self._total_conflated,
                "total_enqueued": self._total_enqueued,
                "drop_rate_1s": drop_rate,
                "capacity": self.capacity,
            }


class SynchronisationKernel:
    """
    Central scheduler that owns the KernelScheduler and dispatches jobs to transports.
    It runs a consumer thread that pops jobs and calls transport.emit().
    """

    def __init__(self, capacity: int = 2000):
        self.scheduler = KernelScheduler(capacity=capacity)
        self._transport_qos_maps: Dict[object, Dict[str, int]] = {}
        self._is_running = False
        self._stop_event = threading.Event()
        self._consumer_thread: Optional[threading.Thread] = None

    def register_transport(self, transport: object, qos_map: Dict[str, int]) -> None:
        """Register a transport with the kernel."""
        self._transport_qos_maps[transport] = qos_map

    def submit(self, frame: Dict[str, Any], target_transports: List[object]) -> None:
        """
        Submit a frame to the kernel for delivery to target transports.
        """
        if not self._is_running:
            raise RuntimeError("Kernel not started; frame dropped.")

        type_name = frame.get("type")
        if not type_name:
            raise ValueError("Frame missing 'type'")

        entity_id = frame.get("id")

        for transport in target_transports:
            qos_map = self._transport_qos_maps.get(transport)
            if qos_map is None:
                logger.error(f"Transport {transport} not registered with kernel; skipping.")
                continue
            qos_level = qos_map.get(type_name, 1)  # default BEST_EFFORT

            job = SyncJob(
                job_id=f"{time.time_ns()}_{id(frame)}",
                entity_id=entity_id,
                qos_level=qos_level,
                payload=frame,
                target_transports=[transport],  # each job per transport
            )
            accepted = self.scheduler.submit(job)
            if not accepted:
                logger.debug(f"Frame dropped for type {type_name} on transport {transport} (queue full)")

    def start(self) -> None:
        """Start the kernel (enable processing) and launch consumer thread."""
        if self._is_running:
            return
        self._is_running = True
        self._stop_event.clear()
        self._consumer_thread = threading.Thread(target=self._consumer_loop, daemon=True)
        self._consumer_thread.start()
        logger.info("Kernel started.")

    def stop(self) -> None:
        """Stop the kernel and wait for consumer thread."""
        self._stop_event.set()
        self._is_running = False
        if self._consumer_thread:
            self._consumer_thread.join(timeout=2.0)
            self._consumer_thread = None

    def _consumer_loop(self) -> None:
        """Background thread that pops batches and emits to transports."""
        while not self._stop_event.is_set():
            jobs = self.scheduler.pop_batch(max_batch_size=100)
            if not jobs:
                self._stop_event.wait(0.001)
                continue

            for job in jobs:
                for transport in job.target_transports:
                    try:
                        transport.emit(job.payload)
                    except Exception as e:
                        logger.error(f"Transport emit error: {e}")

    def stats(self) -> Dict[str, Any]:
        return self.scheduler.stats()