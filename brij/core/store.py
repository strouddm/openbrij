"""SQLite-backed storage for entities and signals."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from brij.core.models import Entity, Signal

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    connector_type TEXT NOT NULL,
    config TEXT,
    created_at TEXT NOT NULL,
    last_synced_at TEXT
);

CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    parent_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    value TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    origin TEXT NOT NULL DEFAULT 'source',
    created_at TEXT NOT NULL,
    UNIQUE(entity_id, kind, value),
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS embeddings (
    entity_id TEXT PRIMARY KEY,
    vector BLOB NOT NULL,
    model TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso)


class Store:
    """SQLite store for Brij entities and signals."""

    def __init__(self, db_path: str | Path) -> None:
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    # --- Entity CRUD ---

    def put_entity(self, entity: Entity) -> None:
        """Insert or replace an entity and all its signals."""
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO entities"
                " (id, type, source_id, parent_id, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    entity.id,
                    entity.type,
                    entity.source_id,
                    entity.parent_id,
                    entity.created_at.isoformat(),
                    entity.updated_at.isoformat(),
                ),
            )
            self._conn.execute("DELETE FROM signals WHERE entity_id = ?", (entity.id,))
            for signal in entity.signals:
                self._conn.execute(
                    """INSERT INTO signals (entity_id, kind, value, confidence, origin, created_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        entity.id,
                        signal.kind,
                        signal.value,
                        signal.confidence,
                        signal.origin,
                        signal.created_at.isoformat(),
                    ),
                )

    def get_entity(self, entity_id: str) -> Entity | None:
        """Return an entity with its signals, or None if not found."""
        row = self._conn.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_entity(row)

    def delete_entity(self, entity_id: str) -> bool:
        """Delete an entity and its signals. Returns True if the entity existed."""
        with self._conn:
            cursor = self._conn.execute(
                "DELETE FROM entities WHERE id = ?", (entity_id,)
            )
            return cursor.rowcount > 0

    def _row_to_entity(self, row: sqlite3.Row) -> Entity:
        signals = self._load_signals(row["id"])
        return Entity(
            id=row["id"],
            type=row["type"],
            source_id=row["source_id"],
            parent_id=row["parent_id"],
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
            signals=signals,
        )

    def _load_signals(self, entity_id: str) -> list[Signal]:
        rows = self._conn.execute(
            "SELECT kind, value, confidence, origin, created_at FROM signals WHERE entity_id = ?",
            (entity_id,),
        ).fetchall()
        return [
            Signal(
                kind=r["kind"],
                value=r["value"],
                confidence=r["confidence"],
                origin=r["origin"],
                created_at=_parse_dt(r["created_at"]),
            )
            for r in rows
        ]

    # --- Signal operations ---

    def add_signals(self, entity_id: str, signals: list[Signal]) -> None:
        """Append signals to an existing entity without replacing existing ones."""
        with self._conn:
            for signal in signals:
                self._conn.execute(
                    """INSERT OR IGNORE INTO signals
                       (entity_id, kind, value, confidence, origin, created_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        entity_id,
                        signal.kind,
                        signal.value,
                        signal.confidence,
                        signal.origin,
                        signal.created_at.isoformat(),
                    ),
                )

    # --- Query methods ---

    def get_entities_by_source(self, source_id: str) -> list[Entity]:
        """Return all entities belonging to a source."""
        rows = self._conn.execute(
            "SELECT * FROM entities WHERE source_id = ?", (source_id,)
        ).fetchall()
        return [self._row_to_entity(r) for r in rows]

    def get_children(self, parent_id: str) -> list[Entity]:
        """Return all entities with the given parent_id."""
        rows = self._conn.execute(
            "SELECT * FROM entities WHERE parent_id = ?", (parent_id,)
        ).fetchall()
        return [self._row_to_entity(r) for r in rows]

    def get_entities_by_type(self, entity_type: str) -> list[Entity]:
        """Return all entities of the given type."""
        rows = self._conn.execute(
            "SELECT * FROM entities WHERE type = ?", (entity_type,)
        ).fetchall()
        return [self._row_to_entity(r) for r in rows]

    # --- Source management ---

    def add_source(
        self,
        source_id: str,
        name: str,
        connector_type: str,
        config: str | None = None,
    ) -> None:
        """Register a data source."""
        with self._conn:
            self._conn.execute(
                """INSERT OR REPLACE INTO sources (id, name, connector_type, config, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (source_id, name, connector_type, config, _now_iso()),
            )

    def get_sources(self) -> list[dict]:
        """Return all registered sources."""
        rows = self._conn.execute("SELECT * FROM sources").fetchall()
        return [dict(r) for r in rows]

    def update_source_synced(self, source_id: str) -> None:
        """Update the last_synced_at timestamp for a source."""
        with self._conn:
            self._conn.execute(
                "UPDATE sources SET last_synced_at = ? WHERE id = ?",
                (_now_iso(), source_id),
            )

    # --- Statistics ---

    def count_entities(self) -> int:
        """Return the total number of entities."""
        row = self._conn.execute("SELECT COUNT(*) as cnt FROM entities").fetchone()
        return row["cnt"]

    def count_signals(self) -> int:
        """Return the total number of signals."""
        row = self._conn.execute("SELECT COUNT(*) as cnt FROM signals").fetchone()
        return row["cnt"]
