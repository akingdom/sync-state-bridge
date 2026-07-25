from typing import Any, Dict


class SerialTransportAdapter:
    def __init__(self, port: str, baudrate: int = 115200, max_queue_size: int = 25):
        self.port = port
        self.baudrate = baudrate
        self.max_queue_size = max_queue_size
        raise NotImplementedError("SerialTransportAdapter is currently a stub. Implementation planned for v0.2.0.")

    async def start(self) -> None:
        raise NotImplementedError

    async def send_delta(self, delta: Dict[str, Any]) -> None:
        # Lazy import to avoid hard dependency at module load
        try:
            import cobs  # noqa: F401
            import serial  # noqa: F401
        except ImportError as e:
            raise RuntimeError(
                "Serial transport dependencies missing. "
                "Install via 'pip install sync-state-bridge[serial]'"
            ) from e
        raise NotImplementedError
