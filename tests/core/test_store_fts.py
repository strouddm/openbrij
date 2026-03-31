"""Tests for SQLite store — FTS5 keyword search."""

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


class TestKeywordSearchByName:
    def test_finds_entity_by_name(self, store):
        store.put_entity(
            _make_entity(id="e1", signals=[Signal(kind="name", value="Alice Johnson")])
        )
        results = store.keyword_search("Alice")
        assert len(results) == 1
        assert results[0][0] == "e1"
        assert results[0][1] > 0

    def test_finds_entity_by_email(self, store):
        store.put_entity(
            _make_entity(
                id="e1",
                signals=[Signal(kind="email", value="alice@example.com")],
            )
        )
        results = store.keyword_search("alice")
        assert len(results) == 1
        assert results[0][0] == "e1"

    def test_finds_entity_by_field_value(self, store):
        store.put_entity(
            _make_entity(
                id="e1",
                signals=[Signal(kind="field:company", value="Acme Corp")],
            )
        )
        results = store.keyword_search("Acme")
        assert len(results) == 1
        assert results[0][0] == "e1"


class TestKeywordSearchDedup:
    def test_deduplicates_by_entity(self, store):
        store.put_entity(
            _make_entity(
                id="e1",
                signals=[
                    Signal(kind="name", value="Alice Johnson"),
                    Signal(kind="email", value="alice@example.com"),
                    Signal(kind="field:notes", value="Alice is great"),
                ],
            )
        )
        results = store.keyword_search("Alice")
        assert len(results) == 1
        assert results[0][0] == "e1"


class TestKeywordSearchSourceFilter:
    def test_source_filter(self, store):
        store.put_entity(
            _make_entity(
                id="e1",
                source_id="s1",
                signals=[Signal(kind="name", value="Alice")],
            )
        )
        store.put_entity(
            _make_entity(
                id="e2",
                source_id="s2",
                signals=[Signal(kind="name", value="Alice Smith")],
            )
        )

        results = store.keyword_search("Alice", source_id="s1")
        assert len(results) == 1
        assert results[0][0] == "e1"

        results = store.keyword_search("Alice", source_id="s2")
        assert len(results) == 1
        assert results[0][0] == "e2"


class TestKeywordSearchLimit:
    def test_limit_is_respected(self, store):
        for i in range(10):
            store.put_entity(
                _make_entity(
                    id=f"e{i}",
                    signals=[Signal(kind="name", value=f"Person {i}")],
                )
            )
        results = store.keyword_search("Person", limit=3)
        assert len(results) == 3


class TestKeywordSearchEdgeCases:
    def test_no_results_returns_empty(self, store):
        store.put_entity(
            _make_entity(id="e1", signals=[Signal(kind="name", value="Alice")])
        )
        results = store.keyword_search("Zebra")
        assert results == []

    def test_empty_query_returns_empty(self, store):
        store.put_entity(
            _make_entity(id="e1", signals=[Signal(kind="name", value="Alice")])
        )
        results = store.keyword_search("")
        assert results == []

    def test_whitespace_query_returns_empty(self, store):
        results = store.keyword_search("   ")
        assert results == []


class TestKeywordSearchStemming:
    def test_porter_stemmer_matches_variants(self, store):
        store.put_entity(
            _make_entity(
                id="e1",
                signals=[Signal(kind="field:notes", value="running quickly")],
            )
        )
        # Porter stemmer reduces "running" and "run" to the same stem
        results = store.keyword_search("run")
        assert len(results) == 1
        assert results[0][0] == "e1"

    def test_stemmer_matches_plural(self, store):
        store.put_entity(
            _make_entity(
                id="e1",
                signals=[Signal(kind="field:notes", value="multiple clients")],
            )
        )
        results = store.keyword_search("client")
        assert len(results) == 1
        assert results[0][0] == "e1"


class TestFtsSyncTriggers:
    def test_delete_entity_removes_from_fts(self, store):
        store.put_entity(
            _make_entity(id="e1", signals=[Signal(kind="name", value="Alice")])
        )
        store.delete_entity("e1")
        results = store.keyword_search("Alice")
        assert results == []

    def test_put_entity_replaces_fts_entries(self, store):
        store.put_entity(
            _make_entity(id="e1", signals=[Signal(kind="name", value="Alice")])
        )
        store.put_entity(
            _make_entity(id="e1", signals=[Signal(kind="name", value="Bob")])
        )
        assert store.keyword_search("Alice") == []
        results = store.keyword_search("Bob")
        assert len(results) == 1
        assert results[0][0] == "e1"

    def test_add_signals_updates_fts(self, store):
        store.put_entity(
            _make_entity(id="e1", signals=[Signal(kind="name", value="Alice")])
        )
        store.add_signals("e1", [Signal(kind="email", value="alice@example.com")])
        results = store.keyword_search("alice")
        assert len(results) == 1
