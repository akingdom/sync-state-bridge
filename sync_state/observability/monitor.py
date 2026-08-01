# filename: sync_state/observability/monitor.py
#!/usr/bin/env python3
"""
Generic Operational Telemetry Collector.
Contains zero knowledge of application entities or domain rules.
"""

import time
from typing import Dict, Any


class PassiveHealthMonitor:
    def __init__(self, bridge: Any, http_transport: Any, ipc_transport: Any):
        self.bridge = bridge
        self.http = http_transport
        self.ipc = ipc_transport
        self.start_time = time.time()

    def get_telemetry_snapshot(self) -> Dict[str, Any]:
        """
        Extracts raw, uninterpreted system metrics.
        """
        now = time.time()

        # Extract kernel stats safely
        stats_attr = getattr(self.bridge, "stats", {})
        kernel_stats = stats_attr() if callable(stats_attr) else (stats_attr if isinstance(stats_attr, dict) else {})

        # Extract transport connections safely
        out_queues = getattr(self.http, "_out_queues", {})
        http_clients = len(out_queues) if isinstance(out_queues, (dict, list, set)) else 0
        ipc_connected = getattr(self.ipc, "is_connected", True)
        if callable(ipc_connected):
            ipc_connected = ipc_connected()

        # Extract governor state safely
        governor_metrics = {}
        if hasattr(self.bridge, "governor"):
            gov = getattr(self.bridge, "governor")
            if hasattr(gov, "get_state") and callable(gov.get_state):
                governor_metrics = gov.get_state()

        return {
            "uptime_sec": round(now - self.start_time, 2),
            "status": "UP",
            "transports": {
                "http_clients": http_clients,
                "ipc_connected": bool(ipc_connected),
            },
            "kernel": {
                "queue_depth": kernel_stats.get("total_depth", kernel_stats.get("queue_depth", 0)),
                "drop_rate_1s": kernel_stats.get("drop_rate_1s", kernel_stats.get("drop_rate", 0.0)),
                "processed_frames": kernel_stats.get("total_frames", 0),
            },
            "governor": governor_metrics
        }
