"""Connector registry with auto-discovery via entry points."""

from __future__ import annotations

import importlib.metadata
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brij.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

_registry: dict[str, type[BaseConnector]] = {}


def register(name: str, connector_class: type[BaseConnector]) -> None:
    """Register a connector class by name.

    Args:
        name: The name to register the connector under.
        connector_class: The connector class to register.
    """
    _registry[name] = connector_class
    logger.debug("Registered connector: %s", name)


def get(name: str) -> type[BaseConnector] | None:
    """Get a registered connector class by name.

    Args:
        name: The name of the connector.

    Returns:
        The connector class, or None if not found.
    """
    return _registry.get(name)


def list_connectors() -> dict[str, type[BaseConnector]]:
    """Return a copy of the connector registry."""
    return dict(_registry)


def discover() -> None:
    """Auto-discover connectors via importlib.metadata entry points.

    Looks for entry points in the "brij.connectors" group.
    """
    eps = importlib.metadata.entry_points()
    connector_eps = eps.select(group="brij.connectors")
    for ep in connector_eps:
        try:
            connector_class = ep.load()
            register(ep.name, connector_class)
        except Exception:
            logger.warning("Failed to load connector entry point: %s", ep.name, exc_info=True)
