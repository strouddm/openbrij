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

CREATE TABLE IF NOT EXISTS indexing_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,
    connector_type TEXT NOT NULL,
    config TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    total_collections INTEGER NOT NULL DEFAULT 0,
    collections_indexed INTEGER NOT NULL DEFAULT 0,
    records_stored INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE
);

CREATE VIRTUAL TABLE IF NOT EXISTS signals_fts
    USING fts5(entity_id, kind, value, tokenize='porter');

CREATE TRIGGER IF NOT EXISTS signals_ai AFTER INSERT ON signals BEGIN
    INSERT INTO signals_fts(entity_id, kind, value)
    VALUES (new.entity_id, new.kind, new.value);
END;

CREATE TRIGGER IF NOT EXISTS signals_au AFTER UPDATE ON signals BEGIN
    DELETE FROM signals_fts
    WHERE entity_id = old.entity_id AND kind = old.kind AND value = old.value;
    INSERT INTO signals_fts(entity_id, kind, value)
    VALUES (new.entity_id, new.kind, new.value);
END;

CREATE TRIGGER IF NOT EXISTS signals_ad AFTER DELETE ON signals BEGIN
    DELETE FROM signals_fts
    WHERE entity_id = old.entity_id AND kind = old.kind AND value = old.value;
END;
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
        row = self._conn.execute("SELECT * FROM entities WHERE id = ?", (entity_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_entity(row)

    def delete_entity(self, entity_id: str) -> bool:
        """Delete an entity and its signals. Returns True if the entity existed."""
        with self._conn:
            cursor = self._conn.execute("DELETE FROM entities WHERE id = ?", (entity_id,))
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

    # --- Embedding storage ---

    def put_embedding(self, entity_id: str, vector: bytes, model: str) -> None:
        """Insert or replace the embedding for an entity."""
        with self._conn:
            self._conn.execute(
                """INSERT OR REPLACE INTO embeddings (entity_id, vector, model, created_at)
                   VALUES (?, ?, ?, ?)""",
                (entity_id, vector, model, _now_iso()),
            )

    def get_embedding(self, entity_id: str) -> dict | None:
        """Return the embedding row for an entity, or None."""
        row = self._conn.execute(
            "SELECT entity_id, vector, model, created_at FROM embeddings WHERE entity_id = ?",
            (entity_id,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def get_all_embeddings(self, source_id: str | None = None) -> list[dict]:
        """Return all embeddings, optionally filtered by source.

        Each dict has keys: entity_id, vector, model, created_at.
        """
        if source_id is not None:
            sql = """
                SELECT emb.entity_id, emb.vector, emb.model, emb.created_at
                FROM embeddings emb
                JOIN entities e ON emb.entity_id = e.id
                WHERE e.source_id = ?
            """
            rows = self._conn.execute(sql, (source_id,)).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT entity_id, vector, model, created_at FROM embeddings"
            ).fetchall()
        return [dict(r) for r in rows]

    # --- Full-text search ---

    # --- Indexing tasks ---

    def create_indexing_task(
        self,
        source_id: str,
        connector_type: str,
        config: str | None = None,
        total_collections: int = 0,
    ) -> int:
        """Create a new indexing task. Returns the task ID."""
        now = _now_iso()
        with self._conn:
            cursor = self._conn.execute(
                """INSERT INTO indexing_tasks
                   (source_id, connector_type, config, status,
                    total_collections, created_at, updated_at)
                   VALUES (?, ?, ?, 'pending', ?, ?, ?)""",
                (source_id, connector_type, config, total_collections, now, now),
            )
            return cursor.lastrowid  # type: ignore[return-value]

    def get_indexing_task(self, task_id: int) -> dict | None:
        """Return an indexing task by ID, or None."""
        row = self._conn.execute(
            "SELECT * FROM indexing_tasks WHERE id = ?", (task_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_indexing_tasks_for_source(self, source_id: str) -> list[dict]:
        """Return all indexing tasks for a source, newest first."""
        rows = self._conn.execute(
            "SELECT * FROM indexing_tasks WHERE source_id = ? ORDER BY created_at DESC",
            (source_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def update_indexing_task(
        self,
        task_id: int,
        *,
        status: str | None = None,
        total_collections: int | None = None,
        collections_indexed: int | None = None,
        records_stored: int | None = None,
        error: str | None = None,
    ) -> None:
        """Update fields on an indexing task."""
        updates: list[str] = []
        params: list = []
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if total_collections is not None:
            updates.append("total_collections = ?")
            params.append(total_collections)
        if collections_indexed is not None:
            updates.append("collections_indexed = ?")
            params.append(collections_indexed)
        if records_stored is not None:
            updates.append("records_stored = ?")
            params.append(records_stored)
        if error is not None:
            updates.append("error = ?")
            params.append(error)
        if not updates:
            return
        updates.append("updated_at = ?")
        params.append(_now_iso())
        params.append(task_id)
        with self._conn:
            self._conn.execute(
                f"UPDATE indexing_tasks SET {', '.join(updates)} WHERE id = ?",
                params,
            )

    def keyword_search(
        self,
        query: str,
        source_id: str | None = None,
        limit: int = 20,
    ) -> list[tuple[str, float]]:
        """Search signals using FTS5 full-text search.

        Returns a list of (entity_id, relevance_score) tuples,
        deduplicated by entity, ordered by descending relevance.
        """
        if not query or not query.strip():
            return []

        if source_id is not None:
            sql = """
                SELECT f.entity_id, MAX(rank) AS relevance
                FROM signals_fts f
                JOIN entities e ON f.entity_id = e.id
                WHERE signals_fts MATCH ? AND e.source_id = ?
                GROUP BY f.entity_id
                ORDER BY relevance
                LIMIT ?
            """
            rows = self._conn.execute(sql, (query, source_id, limit)).fetchall()
        else:
            sql = """
                SELECT entity_id, MAX(rank) AS relevance
                FROM signals_fts
                WHERE signals_fts MATCH ?
                GROUP BY entity_id
                ORDER BY relevance
                LIMIT ?
            """
            rows = self._conn.execute(sql, (query, limit)).fetchall()

        return [(row["entity_id"], -row["relevance"]) for row in rows]
