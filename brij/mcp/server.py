"""MCP server for Brij.

Exposes Brij tools over the Model Context Protocol so AI agents
can discover and interact with connected personal data sources.
"""

from __future__ import annotations

import logging
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from brij.config import Config
from brij.core.store import Store
from brij.mcp.tools import discover, search

logger = logging.getLogger(__name__)

_db_path: str | Path | None = None

mcp = FastMCP("brij", instructions="Personal data connectivity layer for AI agents.")


def _get_store() -> Store:
    """Return a Store instance using the configured or default db path."""
    if _db_path is not None:
        return Store(_db_path)
    config = Config.load()
    return Store(config.db_path)


@mcp.tool()
def brij_discover() -> str:
    """Discover connected data sources.

    Returns a plain-text catalog summary listing source names,
    collection names, entity counts, and top-level structure.
    """
    store = _get_store()
    try:
        return discover(store)
    finally:
        store.close()


@mcp.tool()
def brij_search(
    query: str,
    sources: list[str] | None = None,
    limit: int = 5,
    offset: int = 0,
) -> str:
    """Search connected data sources.

    Returns natural language formatted results with source attribution
    and key field values for each match.

    Args:
        query: The search query string.
        sources: Optional list of source IDs to filter results.
        limit: Maximum number of results to return (default 5).
        offset: Number of results to skip for pagination (default 0).
    """
    store = _get_store()
    try:
        return search(store, query, sources=sources, limit=limit, offset=offset)
    finally:
        store.close()


def create_server(db_path: str | Path | None = None) -> FastMCP:
    """Create and return the MCP server instance.

    Args:
        db_path: Optional path to the SQLite database.
                 Defaults to ~/.brij/brij.db.

    Returns:
        The configured FastMCP server.
    """
    global _db_path
    if db_path is not None:
        _db_path = db_path
    return mcp
