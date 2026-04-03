"""Response formatting for MCP tools."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brij.core.models import Entity
    from brij.core.store import Store

logger = logging.getLogger(__name__)

# Approximate chars-per-token ratio for budget enforcement.
_CHARS_PER_TOKEN = 4
_DEFAULT_TOKEN_BUDGET = 4000
_SEARCH_TOKEN_BUDGET = 3000


def format_discover(store: Store, token_budget: int = _DEFAULT_TOKEN_BUDGET) -> str:
    """Build a plain-text catalog summary from the store.

    Returns source names, collection names, entity counts,
    and top-level structure.  Output stays under *token_budget* tokens
    (estimated at ~4 characters per token).

    Args:
        store: The Brij data store.
        token_budget: Maximum response size in approximate tokens.

    Returns:
        A human-readable catalog summary.
    """
    sources = store.get_sources()

    if not sources:
        return (
            "No data sources connected yet.\n\n"
            "Connect a source first, then run discover again to see what's available."
        )

    char_budget = token_budget * _CHARS_PER_TOKEN
    lines: list[str] = ["Data Catalog", "=" * 40, ""]

    for src in sources:
        source_id = src["id"]
        source_name = src["name"]
        connector_type = src["connector_type"]
        last_synced = src.get("last_synced_at") or "never"

        collections = store.get_entities_by_type("collection")
        collections = [c for c in collections if c.source_id == source_id]

        records = store.get_entities_by_type("record")
        records = [r for r in records if r.source_id == source_id]

        fields = store.get_entities_by_type("field")
        fields = [f for f in fields if f.source_id == source_id]

        lines.append(f"Source: {source_name}")
        lines.append(f"  Type: {connector_type}")
        lines.append(f"  Last synced: {last_synced}")
        lines.append("")

        if not collections:
            lines.append("  No collections found.")
            lines.append("")
            continue

        lines.append(f"  Collections ({len(collections)}):")
        for coll in collections:
            coll_name = coll.name or coll.id
            coll_records = [r for r in records if r.parent_id == coll.id]
            coll_fields = [f for f in fields if f.parent_id == coll.id]

            lines.append(f"    - {coll_name}")
            lines.append(f"      Records: {len(coll_records)}")

            if coll_fields:
                field_names = []
                for f in coll_fields:
                    fname = f.name or f.id
                    ftype = f.get_signal_value("type")
                    if ftype:
                        field_names.append(f"{fname} ({ftype})")
                    else:
                        field_names.append(fname)
                lines.append(f"      Fields: {', '.join(field_names)}")

            # Sample records (up to 5) so the agent understands the data shape.
            sample_records = coll_records[:5]
            if sample_records:
                sample_count = len(sample_records)
                total_count = len(coll_records)
                lines.append(f"      Sample records ({sample_count} of {total_count}):")
                for rec in sample_records:
                    field_sigs = [s for s in rec.signals if s.kind.startswith("field:")]
                    parts = [
                        f"{s.kind.removeprefix('field:')}: {s.value}" for s in field_sigs
                    ]
                    lines.append(f"        - {', '.join(parts)}")

            lines.append("")

        # Check budget before adding more sources.
        current = "\n".join(lines)
        if len(current) > char_budget - 200:
            lines.append("... (additional sources truncated to stay within budget)")
            break

    total_entities = store.count_entities()
    total_signals = store.count_signals()
    lines.append("-" * 40)
    lines.append(
        f"Total: {len(sources)} source(s), {total_entities} entities, {total_signals} signals"
    )

    result = "\n".join(lines)

    # Final trim if still over budget.
    max_chars = char_budget
    if len(result) > max_chars:
        result = result[:max_chars - 3] + "..."

    return result


def _format_entity_result(entity: Entity, store: Store) -> list[str]:
    """Format a single entity as human-readable lines.

    Includes source attribution, entity description, and key field values.
    """
    lines: list[str] = []

    name = entity.name or entity.id
    lines.append(f"- {name}")

    # Source attribution.
    sources = store.get_sources()
    source_map = {s["id"]: s["name"] for s in sources}
    source_name = source_map.get(entity.source_id, entity.source_id)
    lines.append(f"  Source: {source_name}")

    # Entity description from summary or preview signals.
    summary = entity.summary
    if summary:
        lines.append(f"  {summary}")

    # Key field values — show field:* signals.
    field_signals = [s for s in entity.signals if s.kind.startswith("field:")]
    for sig in field_signals:
        field_name = sig.kind.removeprefix("field:")
        lines.append(f"  {field_name}: {sig.value}")

    return lines


def format_search(
    query: str,
    results: list[Entity],
    total_count: int,
    offset: int,
    limit: int,
    store: Store,
    token_budget: int = _SEARCH_TOKEN_BUDGET,
) -> str:
    """Build a plain-text search response.

    Each result includes source name, entity description, and key field
    values.  Includes total match count and pagination note.  Signal
    internals (confidence, origin) never appear in the response.

    Output stays under *token_budget* tokens.
    """
    if not results:
        return (
            f'No results found for "{query}".\n\n'
            "Try different keywords or check that a data source is connected."
        )

    char_budget = token_budget * _CHARS_PER_TOKEN
    lines: list[str] = [f'Search results for "{query}"', "=" * 40, ""]

    for i, entity in enumerate(results, start=offset + 1):
        entry_lines = [f"{i}."] + _format_entity_result(entity, store)
        entry_lines.append("")

        # Check budget before adding this entry.
        candidate = "\n".join(lines + entry_lines)
        if len(candidate) > char_budget - 200:
            lines.append("... (results truncated to stay within budget)")
            lines.append("")
            break

        lines.extend(entry_lines)

    lines.append("-" * 40)

    if total_count > offset + limit:
        remaining = total_count - (offset + limit)
        lines.append(
            f"Showing {offset + 1}-{offset + len(results)} of {total_count} results. "
            f"{remaining} more available."
        )
    else:
        lines.append(f"Showing {offset + 1}-{offset + len(results)} of {total_count} results.")

    result = "\n".join(lines)

    if len(result) > char_budget:
        result = result[:char_budget - 3] + "..."

    return result


def format_write(
    action: str,
    entity_id: str | None = None,
    data: dict | None = None,
) -> str:
    """Build a plain-text confirmation for a write operation.

    Args:
        action: The write action performed (create, add, update, delete).
        entity_id: The entity affected by the write.
        data: Field data involved in the write.

    Returns:
        A human-readable confirmation string.
    """
    data = data or {}

    if action == "create":
        name = data.get("name", "")
        fields = data.get("fields", [])
        lines = [
            f"Created collection '{name}'.",
            f"Fields: {', '.join(fields)}" if fields else "",
            f"Entity ID: {entity_id}" if entity_id else "",
        ]
        return "\n".join(line for line in lines if line)

    if action == "add":
        fields_str = ", ".join(f"{k}: {v}" for k, v in data.items())
        lines = [
            "Added record.",
            f"Entity ID: {entity_id}" if entity_id else "",
            f"Fields: {fields_str}" if fields_str else "",
        ]
        return "\n".join(line for line in lines if line)

    if action == "update":
        fields_str = ", ".join(f"{k}: {v}" for k, v in data.items())
        lines = [
            f"Updated record {entity_id}.",
            f"Modified fields: {fields_str}" if fields_str else "",
        ]
        return "\n".join(line for line in lines if line)

    if action == "delete":
        return f"Deleted record {entity_id}."

    return f"Write completed (action={action})."
