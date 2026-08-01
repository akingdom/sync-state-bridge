# filename: game_stats.py
#!/usr/bin/env python3
"""
Application-Level Telemetry Mapper for SyncState.
Maps generic library metrics to game-specific domain models.
"""

from typing import Dict, Any
from sync_state.observability.monitor import PassiveHealthMonitor


class GameStatsAdapter:
    # Application maps domain entities to abstract framework QoS levels
    GAME_QOS_MAPPING = {
        "player": 3,      # Critical state (Never drop)
        "asteroid": 3,    # High priority game state
        "bullet": 2,      # Medium priority combat state
        "particle": 1,    # Low priority visual effect (First to drop under load)
    }

    def __init__(self, health_monitor: PassiveHealthMonitor):
        self.monitor = health_monitor

    def get_game_health_report((self)) -> Dict[str, Any]:
        """
        Combines library telemetry with game-level domain semantics.
        """
        raw_telemetry = self.monitor.get_telemetry_snapshot()
        drop_threshold = raw_telemetry.get("governor", {}).get("drop_threshold_qos", 0)

        # Map generic drop threshold back to game domain entities
        affected_entities = [
            entity for entity, qos in self.GAME_QOS_MAPPING.items() 
            if qos <= drop_threshold
        ]

        return {
            "server_uptime": raw_telemetry["uptime_sec"],
            "connected_players": raw_telemetry["transports"]["http_clients"],
            "simulation_lagging": raw_telemetry["kernel"]["drop_rate_1s"] > 0,
            "degraded_entity_types": affected_entities,  # e.g., ["particle"] under load
            "raw_metrics": raw_telemetry
        }

