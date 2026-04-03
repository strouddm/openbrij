"""Tests for incremental sync: sync state persistence, incremental worker, and connector state."""

from __future__ import annotations

import textwrap
import time
from pathlib import Path

import pytest

from brij.connectors.base import BaseConnector, SyncResult
from brij.connectors.csv_local import CsvLocalConnector
from brij.core.models import Entity, Signal
from brij.core.store import Store
from brij.core.worker import IndexingWorker


# --- Helpers ---


class FakeConnector(BaseConnector):
    """Connector that returns canned records and tracks sync state."""

    def __init__(
        self,
        records_by_collection: dict[str, list[Entity]] | None = None,
        sync_result: SyncResult | None = None,
    ):
        self._records = records_by_collection or {}
        self._sync_result = sync_result or SyncResult()
        self._state: dict[str, str] = {}

    def authenticate(self, credentials: dict) -> None:
        pass

    def discover(self) -> list[Entity]:
        return []

    def read(self, entity_id: str) -> list[Entity]:
        return self._records.get(entity_id, [])

    def write(self, entity_id: str, data: dict) -> bool:
        return True

    def sync(self) -> SyncResult:
        return self._sync_result

    def get_sync_state(self) -> dict[str, str]:
        return dict(self._state)

    def set_sync_state(self, state: dict[str, str]) -> None:
        self._state = dict(state)


def _make_collection(collection_id: str, source_id: str = "src1") -> Entity:
    return Entity(
        id=collection_id,
        type="collection",
        source_id=source_id,
        signals=[Signal(kind="name", value=collection_id)],
    )


def _make_record(
    record_id: str, source_id: str = "src1", parent_id: str = "col1"
) -> Entity:
    return Entity(
        id=record_id,
        type="record",
        source_id=source_id,
        parent_id=parent_id,
        signals=[Signal(kind="field:name", value="test")],
    )


@pytest.fixture
def store():
    s = Store(":memory:")
    yield s
    s.close()


@pytest.fixture
def tmp_db(tmp_path):
    return tmp_path / "test.db"


# --- Store sync_state tests ---


class TestSyncStatePersistence:
    def test_put_and_get_sync_state(self, store):
        store.add_source("src1", "src1", "csv_local")
        store.put_sync_state("src1", {"last_modified": "2024-01-01T00:00:00+00:00"})

        state = store.get_sync_state("src1")
        assert state == {"last_modified": "2024-01-01T00:00:00+00:00"}

    def test_get_empty_state(self, store):
        store.add_source("src1", "src1", "csv_local")
        state = store.get_sync_state("src1")
        assert state == {}

    def test_put_multiple_keys(self, store):
        store.add_source("src1", "src1", "google_drive")
        store.put_sync_state("src1", {
            "change_token": "abc123",
            "last_modified:file1": "2024-01-01T00:00:00+00:00",
            "last_modified:file2": "2024-02-01T00:00:00+00:00",
        })

        state = store.get_sync_state("src1")
        assert len(state) == 3
        assert state["change_token"] == "abc123"
        assert state["last_modified:file1"] == "2024-01-01T00:00:00+00:00"

    def test_put_overwrites_existing(self, store):
        store.add_source("src1", "src1", "csv_local")
        store.put_sync_state("src1", {"last_modified": "2024-01-01T00:00:00+00:00"})
        store.put_sync_state("src1", {"last_modified": "2024-06-01T00:00:00+00:00"})

        state = store.get_sync_state("src1")
        assert state["last_modified"] == "2024-06-01T00:00:00+00:00"

    def test_state_isolated_per_source(self, store):
        store.add_source("src1", "src1", "csv_local")
        store.add_source("src2", "src2", "csv_local")
        store.put_sync_state("src1", {"key": "val1"})
        store.put_sync_state("src2", {"key": "val2"})

        assert store.get_sync_state("src1")["key"] == "val1"
        assert store.get_sync_state("src2")["key"] == "val2"


class TestDeleteEntitiesForIds:
    def test_delete_entities(self, store):
        store.put_entity(_make_record("r1"))
        store.put_entity(_make_record("r2"))
        store.put_entity(_make_record("r3"))

        deleted = store.delete_entities_for_ids(["r1", "r2"])
        assert deleted == 2
        assert store.get_entity("r1") is None
        assert store.get_entity("r2") is None
        assert store.get_entity("r3") is not None

    def test_delete_empty_list(self, store):
        assert store.delete_entities_for_ids([]) == 0

    def test_delete_removes_embeddings(self, store):
        store.put_entity(_make_record("r1"))
        store.put_embedding("r1", b"\x00" * 16, "test-model")
        assert store.get_embedding("r1") is not None

        store.delete_entities_for_ids(["r1"])
        assert store.get_embedding("r1") is None


# --- CSV connector sync state tests ---


class TestCsvSyncState:
    def test_get_sync_state_before_discover(self):
        conn = CsvLocalConnector()
        assert conn.get_sync_state() == {}

    def test_get_sync_state_after_discover(self, tmp_path):
        csv = tmp_path / "test.csv"
        csv.write_text("a,b\n1,2\n")

        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv)})
        conn.discover()

        state = conn.get_sync_state()
        assert "last_modified" in state

    def test_set_sync_state_restores_baseline(self, tmp_path):
        csv = tmp_path / "test.csv"
        csv.write_text("a,b\n1,2\n")

        # First connector discovers and gets state.
        conn1 = CsvLocalConnector()
        conn1.authenticate({"path": str(csv)})
        conn1.discover()
        state = conn1.get_sync_state()

        # Second connector restores state — sync should show no changes.
        conn2 = CsvLocalConnector()
        conn2.authenticate({"path": str(csv)})
        conn2.set_sync_state(state)
        result = conn2.sync()
        assert result.modified == []

    def test_set_sync_state_detects_changes(self, tmp_path):
        csv = tmp_path / "test.csv"
        csv.write_text("a,b\n1,2\n")

        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv)})
        # Set old state so file appears modified.
        conn.set_sync_state({"last_modified": "2020-01-01T00:00:00+00:00"})
        result = conn.sync()
        assert len(result.modified) == 1


# --- Incremental worker tests ---


class TestIncrementalWorker:
    def test_incremental_only_indexes_changed(self, tmp_db):
        """Worker with sync_result only re-indexes new/modified collections."""
        store = Store(tmp_db)
        store.add_source("src1", "src1", "fake", "{}")

        col1 = _make_collection("col1")
        col2 = _make_collection("col2")
        store.put_entity(col1)
        store.put_entity(col2)

        # Only col1 was modified.
        sync_result = SyncResult(modified=["col1"])

        new_records = [_make_record("r_new", parent_id="col1")]
        connector = FakeConnector(records_by_collection={"col1": new_records})

        task_id = store.create_indexing_task("src1", "fake", "{}")
        store.close()

        worker = IndexingWorker(
            db_path=tmp_db,
            connector=connector,
            source_id="src1",
            task_id=task_id,
            rate_limit_delay=0.0,
            sync_result=sync_result,
        )
        worker.start()
        worker.join(timeout=5.0)

        store = Store(tmp_db)
        task = store.get_indexing_task(task_id)
        assert task["status"] == "completed"
        assert task["total_collections"] == 1  # Only col1 was re-indexed.
        assert task["records_stored"] == 1
        assert store.get_entity("r_new") is not None
        store.close()

    def test_incremental_removes_deleted(self, tmp_db):
        """Worker removes deleted collections and their children."""
        store = Store(tmp_db)
        store.add_source("src1", "src1", "fake", "{}")

        col1 = _make_collection("col1")
        store.put_entity(col1)
        store.put_entity(_make_record("r1", parent_id="col1"))
        store.put_entity(_make_record("r2", parent_id="col1"))

        sync_result = SyncResult(deleted=["col1"])
        connector = FakeConnector()

        task_id = store.create_indexing_task("src1", "fake", "{}")
        store.close()

        worker = IndexingWorker(
            db_path=tmp_db,
            connector=connector,
            source_id="src1",
            task_id=task_id,
            rate_limit_delay=0.0,
            sync_result=sync_result,
        )
        worker.start()
        worker.join(timeout=5.0)

        store = Store(tmp_db)
        assert store.get_entity("col1") is None
        assert store.get_entity("r1") is None
        assert store.get_entity("r2") is None
        task = store.get_indexing_task(task_id)
        assert task["status"] == "completed"
        store.close()

    def test_incremental_replaces_old_records(self, tmp_db):
        """Modified collection's old records are removed before new ones stored."""
        store = Store(tmp_db)
        store.add_source("src1", "src1", "fake", "{}")

        col1 = _make_collection("col1")
        store.put_entity(col1)
        store.put_entity(_make_record("old_r1", parent_id="col1"))
        store.put_entity(_make_record("old_r2", parent_id="col1"))

        sync_result = SyncResult(modified=["col1"])
        new_records = [_make_record("new_r1", parent_id="col1")]
        connector = FakeConnector(records_by_collection={"col1": new_records})

        task_id = store.create_indexing_task("src1", "fake", "{}")
        store.close()

        worker = IndexingWorker(
            db_path=tmp_db,
            connector=connector,
            source_id="src1",
            task_id=task_id,
            rate_limit_delay=0.0,
            sync_result=sync_result,
        )
        worker.start()
        worker.join(timeout=5.0)

        store = Store(tmp_db)
        # Old records removed, new one present.
        assert store.get_entity("old_r1") is None
        assert store.get_entity("old_r2") is None
        assert store.get_entity("new_r1") is not None
        store.close()

    def test_incremental_persists_sync_state(self, tmp_db):
        """Worker persists connector sync state after completion."""
        store = Store(tmp_db)
        store.add_source("src1", "src1", "fake", "{}")

        col1 = _make_collection("col1")
        store.put_entity(col1)

        sync_result = SyncResult(modified=["col1"])
        connector = FakeConnector(records_by_collection={"col1": []})
        connector._state = {"change_token": "tok123"}

        task_id = store.create_indexing_task("src1", "fake", "{}")
        store.close()

        worker = IndexingWorker(
            db_path=tmp_db,
            connector=connector,
            source_id="src1",
            task_id=task_id,
            rate_limit_delay=0.0,
            sync_result=sync_result,
        )
        worker.start()
        worker.join(timeout=5.0)

        store = Store(tmp_db)
        state = store.get_sync_state("src1")
        assert state["change_token"] == "tok123"
        store.close()

    def test_incremental_with_new_collections(self, tmp_db):
        """Worker indexes new collections from sync result."""
        store = Store(tmp_db)
        store.add_source("src1", "src1", "fake", "{}")

        col_new = _make_collection("col_new")
        store.put_entity(col_new)

        sync_result = SyncResult(new=["col_new"])
        records = [_make_record("r1", parent_id="col_new")]
        connector = FakeConnector(records_by_collection={"col_new": records})

        task_id = store.create_indexing_task("src1", "fake", "{}")
        store.close()

        worker = IndexingWorker(
            db_path=tmp_db,
            connector=connector,
            source_id="src1",
            task_id=task_id,
            rate_limit_delay=0.0,
            sync_result=sync_result,
        )
        worker.start()
        worker.join(timeout=5.0)

        store = Store(tmp_db)
        task = store.get_indexing_task(task_id)
        assert task["status"] == "completed"
        assert task["records_stored"] == 1
        assert store.get_entity("r1") is not None
        store.close()

    def test_incremental_no_changes(self, tmp_db):
        """Worker completes immediately when sync result has no changes."""
        store = Store(tmp_db)
        store.add_source("src1", "src1", "fake", "{}")
        connector = FakeConnector()

        sync_result = SyncResult()  # No changes.
        task_id = store.create_indexing_task("src1", "fake", "{}")
        store.close()

        worker = IndexingWorker(
            db_path=tmp_db,
            connector=connector,
            source_id="src1",
            task_id=task_id,
            rate_limit_delay=0.0,
            sync_result=sync_result,
        )
        worker.start()
        worker.join(timeout=5.0)

        store = Store(tmp_db)
        task = store.get_indexing_task(task_id)
        assert task["status"] == "completed"
        assert task["records_stored"] == 0
        store.close()


# --- End-to-end CSV incremental sync test ---


class TestCsvIncrementalEndToEnd:
    def test_csv_sync_round_trip(self, tmp_path):
        """Full cycle: connect → modify file → sync → only changed re-indexed."""
        csv = tmp_path / "data.csv"
        csv.write_text("name,email\nAlice,a@x.com\n")
        db_path = tmp_path / "brij.db"

        # Phase 1: Initial connect.
        store = Store(db_path)
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv)})
        entities = conn.discover()

        source_id = entities[0].source_id
        store.add_source(source_id, source_id, "csv_local", '{"path": "' + str(csv) + '"}')
        for e in entities:
            store.put_entity(e)

        # Persist initial sync state.
        store.put_sync_state(source_id, conn.get_sync_state())
        store.close()

        # Phase 2: Modify the file.
        time.sleep(0.05)
        csv.write_text("name,email\nAlice,a@x.com\nBob,b@x.com\n")

        # Phase 3: Incremental sync.
        store = Store(db_path)
        conn2 = CsvLocalConnector()
        conn2.authenticate({"path": str(csv)})

        saved_state = store.get_sync_state(source_id)
        conn2.set_sync_state(saved_state)

        result = conn2.sync()
        assert len(result.modified) == 1

        # Persist updated state.
        store.put_sync_state(source_id, conn2.get_sync_state())

        # Phase 4: Sync again — no changes.
        conn3 = CsvLocalConnector()
        conn3.authenticate({"path": str(csv)})
        conn3.set_sync_state(store.get_sync_state(source_id))
        result2 = conn3.sync()
        assert result2.modified == []

        store.close()
