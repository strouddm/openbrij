"""MCP tool implementations."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from brij.config import SearchConfig
from brij.connectors.base import WriteError
from brij.core.models import Entity, Signal
from brij.mcp.responses import format_discover, format_search, format_write
from brij.search.engine import SearchEngine

if TYPE_CHECKING:
    from brij.connectors.base import BaseConnector
    from brij.core.store import Store

logger = logging.getLogger(__name__)

_VALID_WRITE_ACTIONS = ("create", "add", "update", "delete")


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


_BROWSE_DEFAULT_LIMIT = 50


def _bulk_retrieve(
    store: Store,
    sources: list[str] | None,
    limit: int,
    offset: int,
) -> str:
    """Return all records for the given sources, paginated.

    Args:
        store: The Brij data store.
        sources: Source IDs to retrieve from. If None, retrieves from all.
        limit: Page size (default 50 for bulk).
        offset: Number of records to skip.

    Returns:
        Plain-text formatted results.
    """
    all_records = store.get_entities_by_type("record")
    if sources:
        all_records = [r for r in all_records if r.source_id in set(sources)]

    total_count = len(all_records)
    paged = all_records[offset:offset + limit]

    return format_search(
        query="*",
        results=paged,
        total_count=total_count,
        offset=offset,
        limit=limit,
        store=store,
    )


def search(
    store: Store,
    query: str,
    sources: list[str] | None = None,
    limit: int = 20,
    offset: int = 0,
    search_config: SearchConfig | None = None,
    browse: bool = False,
) -> str:
    """Search connected data sources and return formatted results.

    When *browse* is True or *query* is ``"*"``, returns all records
    from the specified sources paginated at 50 per page (bulk retrieve).

    Args:
        store: The Brij data store.
        query: The search query string. Use ``"*"`` for bulk retrieval.
        sources: Optional list of source IDs to filter by.
        limit: Maximum results to return (default 20).
        offset: Number of results to skip for pagination (default 0).
        search_config: Optional search configuration override.
        browse: If True, return all records (like query="*").

    Returns:
        Plain-text formatted search results.
    """
    is_bulk = browse or (query.strip() == "*")

    if is_bulk:
        bulk_limit = min(limit, _BROWSE_DEFAULT_LIMIT) if limit != 20 else _BROWSE_DEFAULT_LIMIT
        logger.info(
            "Running bulk retrieve: sources=%r, limit=%d, offset=%d",
            sources, bulk_limit, offset,
        )
        result = _bulk_retrieve(store, sources, bulk_limit, offset)
        logger.debug("Bulk retrieve returned %d characters", len(result))
        return result

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


def _get_connector_for_source(store: Store, source_id: str) -> BaseConnector:
    """Instantiate and authenticate a connector for the given source.

    Looks up the source's connector type and resolves the file path
    from the collection entity's ``location`` signal.

    Args:
        store: The Brij data store.
        source_id: The source to get a connector for.

    Returns:
        An authenticated connector instance.

    Raises:
        WriteError: If the source or connector type is unknown.
    """
    sources = store.get_sources()
    source = next((s for s in sources if s["id"] == source_id), None)
    if source is None:
        raise WriteError(f"Unknown source: {source_id}")

    connector_type = source["connector_type"]
    if connector_type == "csv_local":
        collections = store.get_entities_by_type("collection")
        collection = next(
            (c for c in collections if c.source_id == source_id), None
        )
        if collection is None:
            raise WriteError(f"No collection found for source: {source_id}")

        path = collection.get_signal_value("location")
        if path is None:
            raise WriteError(f"No file path for collection: {collection.id}")

        from brij.connectors.csv_local import CsvLocalConnector

        conn = CsvLocalConnector()
        conn.authenticate({"path": path})
        return conn

    if connector_type == "google_drive":
        from brij.connectors.google_drive import GoogleDriveConnector

        conn = GoogleDriveConnector()
        conn.authenticate(
            json.loads(source.get("config", "{}")) if source.get("config") else {}
        )
        return conn

    raise WriteError(f"Unsupported connector type: {connector_type}")


def write(
    store: Store,
    action: str,
    source_id: str,
    collection_id: str | None = None,
    entity_id: str | None = None,
    data: dict | None = None,
) -> str:
    """Execute a write action against a connected data source.

    Changes flow through the connector's write path back to the
    original source, then the store is updated to match.

    Args:
        store: The Brij data store.
        action: One of ``"create"``, ``"add"``, ``"update"``, ``"delete"``.
        source_id: The source to write to.
        collection_id: Required for ``"add"`` — the target collection.
        entity_id: Required for ``"update"`` and ``"delete"`` — the target record.
        data: Field data for ``"create"``, ``"add"``, and ``"update"``.

    Returns:
        Plain-text confirmation of what changed.
    """
    logger.info("Running write tool: action=%s, source_id=%s", action, source_id)

    if action not in _VALID_WRITE_ACTIONS:
        return (
            f"Invalid action '{action}'. "
            f"Must be one of: {', '.join(_VALID_WRITE_ACTIONS)}"
        )

    data = data or {}
    connector = _get_connector_for_source(store, source_id)

    if action == "create":
        return _write_create(store, connector, source_id, data)
    if action == "add":
        if collection_id is None:
            return "collection_id is required for 'add' action."
        return _write_add(store, connector, source_id, collection_id, data)
    if action == "update":
        if entity_id is None:
            return "entity_id is required for 'update' action."
        return _write_update(store, connector, source_id, entity_id, data)
    # action == "delete"
    if entity_id is None:
        return "entity_id is required for 'delete' action."
    return _write_delete(store, connector, source_id, entity_id)


def _write_create(
    store: Store,
    connector: BaseConnector,
    source_id: str,
    data: dict,
) -> str:
    """Handle the 'create' action — create a new collection."""
    name = data.get("name", "")
    if not name:
        return "data must include 'name' for create action."

    fields = data.get("fields", [])
    if not fields:
        return "data must include a non-empty 'fields' list for create action."

    collection = connector.create_collection(name, {"fields": fields})
    store.put_entity(collection)

    # Create field entities for each column.
    for field_name in fields:
        field_id = connector.make_entity_id("field", f"{name}.csv:{field_name}")
        field_entity = Entity(
            id=field_id,
            type="field",
            source_id=source_id,
            parent_id=collection.id,
            signals=[
                Signal(kind="name", value=field_name),
                Signal(kind="type", value="text"),
            ],
        )
        store.put_entity(field_entity)

    return format_write(
        action="create",
        entity_id=collection.id,
        data={"name": name, "fields": fields},
    )


def _write_add(
    store: Store,
    connector: BaseConnector,
    source_id: str,
    collection_id: str,
    data: dict,
) -> str:
    """Handle the 'add' action — add a new record to a collection."""
    # Count existing records to determine the new row index.
    records = [
        e for e in store.get_entities_by_type("record")
        if e.parent_id == collection_id
    ]
    new_row_idx = len(records)

    # Write through to source.
    connector.write(collection_id, {"action": "add", "fields": data})

    # Create the record entity in the store.
    filename = collection_id.removeprefix("collection:")
    record_id = connector.make_entity_id("record", f"{filename}:{new_row_idx}")
    signals = [Signal(kind=f"field:{k}", value=str(v)) for k, v in data.items()]
    entity = Entity(
        id=record_id,
        type="record",
        source_id=source_id,
        parent_id=collection_id,
        signals=signals,
    )
    store.put_entity(entity)

    return format_write(action="add", entity_id=record_id, data=data)


def _write_update(
    store: Store,
    connector: BaseConnector,
    source_id: str,
    entity_id: str,
    data: dict,
) -> str:
    """Handle the 'update' action — modify fields on an existing record."""
    existing = store.get_entity(entity_id)
    if existing is None:
        return f"Entity not found: {entity_id}"

    # Write through to source.
    connector.write(entity_id, {"action": "update", "fields": data})

    # Update the entity's signals in the store.
    updated_signals: list[Signal] = []
    for sig in existing.signals:
        field_name = sig.kind.removeprefix("field:")
        if sig.kind.startswith("field:") and field_name in data:
            updated_signals.append(
                Signal(kind=sig.kind, value=str(data[field_name]))
            )
        else:
            updated_signals.append(sig)

    updated_entity = Entity(
        id=existing.id,
        type=existing.type,
        source_id=existing.source_id,
        parent_id=existing.parent_id,
        signals=updated_signals,
    )
    store.put_entity(updated_entity)

    return format_write(action="update", entity_id=entity_id, data=data)


def _write_delete(
    store: Store,
    connector: BaseConnector,
    source_id: str,
    entity_id: str,
) -> str:
    """Handle the 'delete' action — remove a record."""
    existing = store.get_entity(entity_id)
    if existing is None:
        return f"Entity not found: {entity_id}"

    # Write through to source.
    connector.write(entity_id, {"action": "delete"})

    # Remove from store.
    store.delete_entity(entity_id)

    return format_write(action="delete", entity_id=entity_id)
