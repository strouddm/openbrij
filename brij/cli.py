"""CLI for Brij — connect, status, search, serve."""

from __future__ import annotations

import json
import logging
import sys

import click

from brij.config import Config
from brij.connectors import discover as discover_connectors
from brij.connectors import get as get_connector
from brij.connectors import register
from brij.connectors.csv_local import CsvLocalConnector
from brij.core.store import Store

logger = logging.getLogger(__name__)


def _ensure_builtins_registered() -> None:
    """Register built-in connectors if not already present."""
    if get_connector("csv_local") is None:
        register("csv_local", CsvLocalConnector)


def _get_store(config: Config | None = None) -> Store:
    """Return a Store backed by the configured database path."""
    config = config or Config.load()
    return Store(config.db_path)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
def main(verbose: bool) -> None:
    """Brij — personal data connectivity layer for AI agents."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


@main.command()
@click.argument("connector_name")
@click.option("--path", required=True, help="Path to the data source (e.g. CSV file).")
def connect(connector_name: str, path: str) -> None:
    """Authenticate a connector and discover its data."""
    _ensure_builtins_registered()
    discover_connectors()
    connector_cls = get_connector(connector_name)
    if connector_cls is None:
        click.echo(f"Unknown connector: {connector_name}", err=True)
        sys.exit(1)

    connector = connector_cls()
    try:
        connector.authenticate({"path": path})
    except Exception as exc:
        click.echo(f"Authentication failed: {exc}", err=True)
        sys.exit(1)

    entities = connector.discover()
    if not entities:
        click.echo("No entities discovered.")
        return

    config = Config.load()
    config.brij_dir.mkdir(parents=True, exist_ok=True)
    store = _get_store(config)
    try:
        source_id = entities[0].source_id
        store.add_source(source_id, source_id, connector_name, json.dumps({"path": path}))

        for entity in entities:
            store.put_entity(entity)

        # Read records from discovered collections.
        collections = [e for e in entities if e.type == "collection"]
        record_count = 0
        for collection in collections:
            records = connector.read(collection.id)
            for record in records:
                store.put_entity(record)
                record_count += 1

        store.update_source_synced(source_id)
        click.echo(
            f"Connected {connector_name}: {len(entities)} entities discovered, "
            f"{record_count} records stored."
        )
    finally:
        store.close()


@main.command()
def status() -> None:
    """Show connected sources, entity counts, and index coverage."""
    config = Config.load()
    if not config.db_path.exists():
        click.echo("No database found. Connect a source first with: brij connect")
        return

    store = _get_store(config)
    try:
        sources = store.get_sources()
        if not sources:
            click.echo("No connected sources.")
            return

        total_entities = store.count_entities()
        total_signals = store.count_signals()
        embeddings = store.get_all_embeddings()

        click.echo(f"Sources: {len(sources)}")
        click.echo(f"Total entities: {total_entities}")
        click.echo(f"Total signals: {total_signals}")
        click.echo(f"Embeddings: {len(embeddings)}")
        click.echo()

        for src in sources:
            click.echo(f"  [{src['connector_type']}] {src['name']}")
            src_entities = store.get_entities_by_source(src["id"])
            type_counts: dict[str, int] = {}
            for entity in src_entities:
                type_counts[entity.type] = type_counts.get(entity.type, 0) + 1
            for etype, count in sorted(type_counts.items()):
                click.echo(f"    {etype}: {count}")
            if src.get("last_synced_at"):
                click.echo(f"    last synced: {src['last_synced_at']}")
    finally:
        store.close()


@main.command()
@click.argument("query")
@click.option("--source", "-s", multiple=True, help="Filter by source ID.")
@click.option("--limit", "-n", default=5, help="Max results (default 5).")
def search(query: str, source: tuple[str, ...], limit: int) -> None:
    """Search connected data sources."""
    config = Config.load()
    if not config.db_path.exists():
        click.echo("No database found. Connect a source first with: brij connect")
        return

    store = _get_store(config)
    try:
        from brij.mcp.tools import search as mcp_search

        sources_list = list(source) if source else None
        result = mcp_search(store, query, sources=sources_list, limit=limit)
        click.echo(result)
    finally:
        store.close()


@main.command()
def serve() -> None:
    """Start the MCP server."""
    from brij.mcp.server import create_server

    server = create_server()
    server.run()
