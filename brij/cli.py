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
from brij.connectors.google_drive import GoogleDriveConnector
from brij.connectors.google_sheets import GoogleSheetsConnector
from brij.core.store import Store
from brij.core.worker import IndexingWorker

logger = logging.getLogger(__name__)


def _ensure_builtins_registered() -> None:
    """Register built-in connectors if not already present."""
    if get_connector("csv_local") is None:
        register("csv_local", CsvLocalConnector)
    if get_connector("google_sheets") is None:
        register("google_sheets", GoogleSheetsConnector)
    if get_connector("google_drive") is None:
        register("google_drive", GoogleDriveConnector)


def _get_store(config: Config | None = None) -> Store:
    """Return a Store backed by the configured database path."""
    config = config or Config.load()
    return Store(config.db_path)


@click.group(context_settings={"max_content_width": 120})
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """Brij — personal data connectivity layer for AI agents."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    if verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")


def _setup_logging(ctx: click.Context, verbose: bool) -> None:
    """Enable debug logging if --verbose passed to this command or the parent group."""
    parent = ctx.parent
    parent_verbose = (
        parent.obj.get("verbose", False) if parent and parent.obj else False
    )
    if verbose or parent_verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s", force=True)
    elif not logging.root.handlers:
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


@main.command()
@click.argument("connector_name")
@click.option("--path", default=None, help="Path to the data source (e.g. CSV file).")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.pass_context
def connect(ctx: click.Context, connector_name: str, path: str | None, verbose: bool) -> None:
    """Authenticate a connector and discover its data."""
    _setup_logging(ctx, verbose)
    _ensure_builtins_registered()
    discover_connectors()
    connector_cls = get_connector(connector_name)
    if connector_cls is None:
        click.echo(f"Unknown connector: {connector_name}", err=True)
        sys.exit(1)

    # Build credentials dict based on connector type.
    _OAUTH_CONNECTORS = {"google_sheets", "google_drive"}
    if path is not None:
        credentials: dict = {"path": path}
    elif connector_name in _OAUTH_CONNECTORS:
        credentials = {}
    else:
        click.echo(f"--path is required for connector: {connector_name}", err=True)
        sys.exit(1)

    connector = connector_cls()
    try:
        connector.authenticate(credentials)
    except Exception as exc:
        click.echo(f"Authentication failed: {exc}", err=True)
        sys.exit(1)

    # For Google Sheets, let the user pick a single spreadsheet.
    selected_spreadsheet_id = None
    if connector_name == "google_sheets":
        spreadsheets = connector.list_spreadsheets()
        if not spreadsheets:
            click.echo("No spreadsheets found.")
            return
        click.echo("Available spreadsheets:")
        for i, s in enumerate(spreadsheets, 1):
            click.echo(f"  {i}. {s['name']}")
        choice = click.prompt(
            "Select a spreadsheet", type=click.IntRange(1, len(spreadsheets))
        )
        selected_spreadsheet_id = spreadsheets[choice - 1]["id"]

    if selected_spreadsheet_id is not None:
        entities = connector.discover(spreadsheet_id=selected_spreadsheet_id)
    else:
        entities = connector.discover()
    if not entities:
        click.echo("No entities discovered.")
        return

    config = Config.load()
    config.brij_dir.mkdir(parents=True, exist_ok=True)
    store = _get_store(config)
    try:
        source_id = entities[0].source_id
        creds_json = json.dumps(credentials)
        store.add_source(source_id, source_id, connector_name, creds_json)

        # Store Tier 1 metadata immediately.
        for entity in entities:
            store.put_entity(entity)

        collections = [e for e in entities if e.type == "collection"]
        task_id = store.create_indexing_task(
            source_id, connector_name, creds_json, total_collections=len(collections),
        )

        click.echo(
            f"Connected {connector_name}: {len(entities)} entities cataloged. "
            f"Indexing {len(collections)} collection(s) in the background."
        )

        # Spawn background worker for Tier 2/3 content extraction.
        worker = IndexingWorker(
            db_path=config.db_path,
            connector=connector,
            source_id=source_id,
            task_id=task_id,
            rate_limit_delay=0.0,
        )
        worker.start()
    finally:
        store.close()


@main.command()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.pass_context
def status(ctx: click.Context, verbose: bool) -> None:
    """Show connected sources, entity counts, and index coverage."""
    _setup_logging(ctx, verbose)
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

            # Show indexing progress.
            tasks = store.get_indexing_tasks_for_source(src["id"])
            if tasks:
                latest = tasks[0]
                status_label = latest["status"]
                total = latest["total_collections"]
                indexed = latest["collections_indexed"]
                records = latest["records_stored"]
                if status_label == "running":
                    click.echo(
                        f"    indexing: {indexed}/{total} collections, "
                        f"{records} records stored"
                    )
                elif status_label == "pending":
                    click.echo(f"    indexing: pending ({total} collections queued)")
                elif status_label == "completed":
                    click.echo(
                        f"    indexing: complete ({records} records stored)"
                    )
                elif status_label == "failed":
                    err = latest.get("error", "unknown error")
                    click.echo(f"    indexing: failed — {err}")

            if src.get("last_synced_at"):
                click.echo(f"    last synced: {src['last_synced_at']}")
    finally:
        store.close()


@main.command()
@click.argument("query")
@click.option("--source", "-s", multiple=True, help="Filter by source ID.")
@click.option("--limit", "-n", default=5, help="Max results (default 5).")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.pass_context
def search(
    ctx: click.Context,
    query: str,
    source: tuple[str, ...],
    limit: int,
    verbose: bool,
) -> None:
    """Search connected data sources."""
    _setup_logging(ctx, verbose)
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
