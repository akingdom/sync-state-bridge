# filename: sync_state/core/governor.py
#!/usr/bin/env python3
"""
Domain-Agnostic Adaptive Governor.
Monitors system congestion and provides priority-based backpressure advice.
"""

import time
from typing import Dict, Any


class Governor:
    def __init__(self, target_capacity: int = 5000, max_qos_level: int = 3):
        self.target_capacity = target_capacity
        self.max_qos_level = max_qos_level
        self._current_drop_threshold = 0  # 0 = Drop nothing; 3 = Drop everything up to QoS 3

    def attach(self, stats_source):
        self._stats_source = stats_source

    def evaluate(self, current_queue_depth: int, ingress_rate: float = 0.0) -> Dict[str, Any]:
        """
        Calculates system pressure based on depth and rate.
        Returns generic policy recommendations.
        """
        saturation = current_queue_depth / float(self.target_capacity) if self.target_capacity > 0 else 0.0

        if saturation > 0.85:
            # High pressure: Shed low-priority frames (QoS 1)
            self._current_drop_threshold = 1
        elif saturation > 0.95:
            # Critical pressure: Shed up to medium-priority frames (QoS 2)
            self._current_drop_threshold = 2
        else:
            # Normal operation
            self._current_drop_threshold = 0

        health_status = "healthy"
        if saturation > 0.95:
            health_status = "critical"
        elif saturation > 0.85:
            health_status = "degraded"

        return {
            "health": health_status,
            "saturation_ratio": round(saturation, 3),
            "drop_threshold_qos": self._current_drop_threshold,
            "recommended_throttle": saturation > 0.90,
        }

