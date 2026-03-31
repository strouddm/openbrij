"""Response formatting for MCP tools."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brij.core.store import Store

logger = logging.getLogger(__name__)

# Approximate chars-per-token ratio for budget enforcement.
_CHARS_PER_TOKEN = 4
_DEFAULT_TOKEN_BUDGET = 2000


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
