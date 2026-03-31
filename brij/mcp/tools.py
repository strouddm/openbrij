"""MCP tool implementations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from brij.config import SearchConfig
from brij.mcp.responses import format_discover, format_search
from brij.search.engine import SearchEngine

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


def search(
    store: Store,
    query: str,
    sources: list[str] | None = None,
    limit: int = 5,
    offset: int = 0,
    search_config: SearchConfig | None = None,
) -> str:
    """Search connected data sources and return formatted results.

    Args:
        store: The Brij data store.
        query: The search query string.
        sources: Optional list of source IDs to filter by.
        limit: Maximum results to return (default 5).
        offset: Number of results to skip for pagination (default 0).
        search_config: Optional search configuration override.

    Returns:
        Plain-text formatted search results.
    """
    logger.info("Running search tool: query=%r, sources=%r, limit=%d, offset=%d",
                query, sources, limit, offset)

    config = search_config or SearchConfig()
    engine = SearchEngine(store, config)

    # Fetch enough results to cover offset + limit.
    fetch_limit = offset + limit
    all_results = engine.search(query, sources=sources, limit=fetch_limit)
    total_count = len(all_results)

    # Apply offset.
    paged_results = all_results[offset:]

    result = format_search(
        query=query,
        results=paged_results,
        total_count=total_count,
        offset=offset,
        limit=limit,
        store=store,
    )
    logger.debug("Search returned %d characters", len(result))
    return result
