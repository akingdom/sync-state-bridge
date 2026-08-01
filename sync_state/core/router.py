"""
sync_state/core/router.py

Pure frame forwarder: stores routing tables and, given a frame and source,
returns the list of target transports that should receive the frame.
Direction enforcement and authorization are performed here.
"""

import logging
from typing import Dict, List, Optional, Callable, Any, Set
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class RouterError(Exception):
    pass


class RouterConfigError(RouterError):
    pass


class RouterUnauthorizedError(RouterError):
    pass


@dataclass
class _RouteEntry:
    transport: object
    direction: str          # "in", "out", "both"
    ack_mode: str           # "none" or "verbose" (for future use)
    allow_multiple_sources: bool = False
    conflict_policy: Optional[Callable] = None
    policy_params: Optional[Dict] = None


class Router:
    """
    Core routing engine.

    Responsibilities:
        - Maintain a routing table: type → list of (transport, direction, ...).
        - Given a frame and source transport, return the list of target transports
          that have direction containing "out" and are not the source.
        - Enforce authorization: source must have "in" direction for the type.
        - Optionally supports multi-source conflict resolution (future).
    """

    def __init__(self):
        self._routes: Dict[str, List[_RouteEntry]] = {}
        self._allow_multiple_sources: Dict[str, bool] = {}
        self._conflict_policies: Dict[str, Callable] = {}

    def register_transport(
        self,
        transport: object,
        type_list: List[str],
        direction: str,
        ack_mode: str = "none",
        allow_multiple_sources: bool = False,
        conflict_policy: Optional[Callable] = None,
        policy_params: Optional[Dict] = None,
    ) -> None:
        """
        Register a transport for a set of types.

        Args:
            transport: The transport adapter (must have `emit` method).
            type_list: List of type names this transport handles.
            direction: "in", "out", or "both".
            ack_mode: "none" or "verbose" (for future use).
            allow_multiple_sources: If True, allow multiple "in" transports for these types.
            conflict_policy: Callable for resolving conflicts when multiple sources.
            policy_params: Optional parameters for the policy.
        """
        if direction not in ("in", "out", "both"):
            raise RouterConfigError(f"Invalid direction: {direction}")
        if ack_mode not in ("none", "verbose"):
            raise RouterConfigError(f"Invalid ack_mode: {ack_mode}")

        for type_name in type_list:
            routes = self._routes.setdefault(type_name, [])
            has_in = any(r.direction in ("in", "both") for r in routes)
            new_has_in = direction in ("in", "both")

            # Preserve multi-source permission if enabled by any registration
            if allow_multiple_sources:
                self._allow_multiple_sources[type_name] = True

            # Register conflict policy if explicitly provided
            if conflict_policy is not None:
                self._conflict_policies[type_name] = conflict_policy

            # Validate multi-inbound conflict rules when adding a second inbound transport
            if has_in and new_has_in:
                if not self._allow_multiple_sources.get(type_name, False):
                    raise RouterConfigError(
                        f"Type '{type_name}' already has an 'in' transport. "
                        "Set allow_multiple_sources=True to allow multiple sources."
                    )
                if type_name not in self._conflict_policies:
                    raise RouterConfigError(
                        f"Type '{type_name}' has multiple sources enabled, but no conflict_policy provided."
                    )

            routes.append(_RouteEntry(
                transport=transport,
                direction=direction,
                ack_mode=ack_mode,
                allow_multiple_sources=allow_multiple_sources,
                conflict_policy=conflict_policy,
                policy_params=policy_params,
            ))

    def route(self, frame: Dict, source_transport: Optional[object]) -> List[object]:
        """
        Determine which transports should receive this frame.

        Args:
            frame: The frame to route (must contain `type`).
            source_transport: The transport that originated the frame.

        Returns:
            List of transport adapters that should receive the frame.

        Raises:
            RouterUnauthorizedError: if source is not allowed to send this type.
        """
        type_name = frame.get("type")
        if not type_name:
            raise ValueError("Frame missing 'type'")

        routes = self._routes.get(type_name)
        if not routes:
            logger.debug(f"No routes for type '{type_name}'")
            return []

        # Determine source direction
        # Internal/kernel submission (source_transport=None) is implicitly authorized
        if source_transport is not None:
            source_entry = next((e for e in routes if e.transport is source_transport), None)
            if not source_entry:
                raise RouterUnauthorizedError(
                    f"Transport {source_transport} not registered for type '{type_name}'"
                )

            if source_entry.direction not in ("in", "both"):
                raise RouterUnauthorizedError(
                    f"Transport {source_transport} cannot send type '{type_name}'"
                )
    
        # Gather target transports (exclude source, require "out")
        targets = [
            e.transport for e in routes 
            if e.direction in ("out", "both") and e.transport is not source_transport
        ]
        return targets
