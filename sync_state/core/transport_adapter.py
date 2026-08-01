"""
sync_state/core/transport_adapter.py
Base interface for all transports.
"""
from typing import Callable, Dict, Any

class TransportAdapter:
    """
    A transport adaptor is a pluggable communication channel that can send and receive frames.
    The Router uses this interface to forward frames.
    """
    def start(self):
        """Start the transport (e.g., listen for connections)."""
        raise NotImplementedError

    def emit(self, frame: Dict[str, Any]) -> None:
        """Send a frame to the other side of the transport."""
        raise NotImplementedError

    def on_frame(self, callback: Callable[[Dict], None]) -> None:
        """Register a callback to be called when a frame is received."""
        raise NotImplementedError

    def stop(self):
        """Shut down the transport."""
        raise NotImplementedError