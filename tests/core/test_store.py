"""Tests for SQLite store — schema and entity CRUD."""

import pytest

from brij.core.models import Entity, Signal
from brij.core.store import Store


@pytest.fixture
def store():
    s = Store(":memory:")
    yield s
    s.close()


def _make_entity(
    id: str = "e1",
    type: str = "record",
    source_id: str = "s1",
    parent_id: str | None = None,
    signals: list[Signal] | None = None,
) -> Entity:
    return Entity(
        id=id,
        type=type,
        source_id=source_id,
        parent_id=parent_id,
        signals=signals or [],
    )


class TestPutAndGetEntity:
    def test_round_trip(self, store):
        entity = _make_entity(signals=[Signal(kind="name", value="Alice")])
        store.put_entity(entity)
        loaded = store.get_entity("e1")
        assert loaded is not None
        assert loaded.id == "e1"
        assert loaded.type == "record"
        assert loaded.source_id == "s1"
        assert len(loaded.signals) == 1
        assert loaded.signals[0].kind == "name"
        assert loaded.signals[0].value == "Alice"

    def test_get_nonexistent_returns_none(self, store):
        assert store.get_entity("nope") is None

    def test_put_replaces_signals(self, store):
        entity = _make_entity(signals=[Signal(kind="name", value="Alice")])
        store.put_entity(entity)

        entity.signals = [Signal(kind="name", value="Bob")]
        store.put_entity(entity)

        loaded = store.get_entity("e1")
        assert len(loaded.signals) == 1
        assert loaded.signals[0].value == "Bob"

    def test_put_preserves_parent_id(self, store):
        entity = _make_entity(parent_id="p1")
        store.put_entity(entity)
        loaded = store.get_entity("e1")
        assert loaded.parent_id == "p1"

    def test_multiple_signals(self, store):
        entity = _make_entity(
            signals=[
                Signal(kind="name", value="Alice"),
                Signal(kind="email", value="alice@example.com"),
                Signal(kind="field:phone", value="555-1234"),
            ]
        )
        store.put_entity(entity)
        loaded = store.get_entity("e1")
        assert len(loaded.signals) == 3


class TestDeleteEntity:
    def test_delete_existing(self, store):
        store.put_entity(_make_entity())
        assert store.delete_entity("e1") is True
        assert store.get_entity("e1") is None

    def test_delete_nonexistent(self, store):
        assert store.delete_entity("nope") is False

    def test_delete_cascades_signals(self, store):
        entity = _make_entity(signals=[Signal(kind="name", value="Alice")])
        store.put_entity(entity)
        store.delete_entity("e1")
        assert store.count_signals() == 0


class TestAddSignals:
    def test_appends_signals(self, store):
        entity = _make_entity(signals=[Signal(kind="name", value="Alice")])
        store.put_entity(entity)

        store.add_signals("e1", [Signal(kind="email", value="alice@example.com")])

        loaded = store.get_entity("e1")
        assert len(loaded.signals) == 2
        kinds = {s.kind for s in loaded.signals}
        assert kinds == {"name", "email"}

    def test_ignores_duplicate_signals(self, store):
        entity = _make_entity(signals=[Signal(kind="name", value="Alice")])
        store.put_entity(entity)

        store.add_signals("e1", [Signal(kind="name", value="Alice")])

        loaded = store.get_entity("e1")
        assert len(loaded.signals) == 1


class TestQueryMethods:
    def test_get_entities_by_source(self, store):
        store.put_entity(_make_entity(id="e1", source_id="s1"))
        store.put_entity(_make_entity(id="e2", source_id="s1"))
        store.put_entity(_make_entity(id="e3", source_id="s2"))

        results = store.get_entities_by_source("s1")
        assert len(results) == 2
        assert {e.id for e in results} == {"e1", "e2"}

    def test_get_children(self, store):
        store.put_entity(_make_entity(id="parent", type="collection"))
        store.put_entity(_make_entity(id="c1", parent_id="parent"))
        store.put_entity(_make_entity(id="c2", parent_id="parent"))
        store.put_entity(_make_entity(id="c3", parent_id="other"))

        children = store.get_children("parent")
        assert len(children) == 2
        assert {e.id for e in children} == {"c1", "c2"}

    def test_get_entities_by_type(self, store):
        store.put_entity(_make_entity(id="e1", type="record"))
        store.put_entity(_make_entity(id="e2", type="collection"))
        store.put_entity(_make_entity(id="e3", type="record"))

        records = store.get_entities_by_type("record")
        assert len(records) == 2
        assert {e.id for e in records} == {"e1", "e3"}

    def test_get_entities_by_source_empty(self, store):
        assert store.get_entities_by_source("nope") == []


class TestSourceManagement:
    def test_add_and_get_sources(self, store):
        store.add_source("s1", "My CSV", "csv")
        store.add_source("s2", "My Sheet", "google_sheets", config='{"key": "val"}')

        sources = store.get_sources()
        assert len(sources) == 2
        names = {s["name"] for s in sources}
        assert names == {"My CSV", "My Sheet"}

    def test_update_source_synced(self, store):
        store.add_source("s1", "My CSV", "csv")
        sources = store.get_sources()
        assert sources[0]["last_synced_at"] is None

        store.update_source_synced("s1")
        sources = store.get_sources()
        assert sources[0]["last_synced_at"] is not None


class TestStatistics:
    def test_counts_empty(self, store):
        assert store.count_entities() == 0
        assert store.count_signals() == 0

    def test_counts_after_inserts(self, store):
        store.put_entity(
            _make_entity(
                id="e1",
                signals=[
                    Signal(kind="name", value="Alice"),
                    Signal(kind="email", value="alice@example.com"),
                ],
            )
        )
        store.put_entity(_make_entity(id="e2"))

        assert store.count_entities() == 2
        assert store.count_signals() == 2

    def test_counts_after_delete(self, store):
        store.put_entity(
            _make_entity(signals=[Signal(kind="name", value="Alice")])
        )
        store.delete_entity("e1")
        assert store.count_entities() == 0
        assert store.count_signals() == 0
