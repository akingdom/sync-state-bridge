from .qos import QoS, DropPolicy


class Presets:
    """Pre-configured QoS profiles for common network deployment scenarios."""

    @staticmethod
    def conservative() -> QoS:
        """Safe profile for general IP networking; ensures minimal delta loss."""
        return QoS(
            drop_policy=DropPolicy.CRITICAL,
            ideal_cadence_ms=100,
            ttl_ms=None
        )

    @staticmethod
    def low_bandwidth() -> QoS:
        """Optimized for low-rate interfaces like serial, BLE, or satellite links."""
        return QoS(
            drop_policy=DropPolicy.CONFLATABLE,
            ideal_cadence_ms=250,
            ttl_ms=500
        )

    @staticmethod
    def high_throughput() -> QoS:
        """High-frequency telemetry stream with aggressive delta drop allowance."""
        return QoS(
            drop_policy=DropPolicy.BEST_EFFORT,
            ideal_cadence_ms=20,
            ttl_ms=100
        )
