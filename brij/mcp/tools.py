"""MCP tool implementations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from brij.mcp.responses import format_discover

if TYPE_CHECKING:
    from brij.core.store import Store

logger = logging.getLogger(__name__)


def discover(store: Store) -> str:
    """Return a natural-language catalog summary of all connected sources.

    Args:
        store: The Brij data store.

    Returns:
        Plain-text summary of sources, collections, entity counts,
        and top-level structure.
    """
    logger.info("Running discover tool")
    result = format_discover(store)
    logger.debug("Discover returned %d characters", len(result))
    return result
