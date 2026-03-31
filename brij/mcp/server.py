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
from brij.mcp.tools import discover

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
