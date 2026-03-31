"""Tests for the keyword search engine."""

from __future__ import annotations

from pathlib import Path

import pytest

from brij.config import SearchConfig
from brij.connectors.csv_local import CsvLocalConnector
from brij.core.store import Store
from brij.search.engine import SearchEngine


@pytest.fixture()
def store() -> Store:
    s = Store(":memory:")
    yield s
    s.close()


@pytest.fixture()
def config() -> SearchConfig:
    return SearchConfig()


@pytest.fixture()
def clients_csv(tmp_path: Path) -> Path:
    """Realistic client CSV fixture with 7 rows."""
    content = (
        "name,email,phone,rate,status,notes\n"
        "Alice Johnson,alice@techvault.io,555-1234,125.50,"
        "Active,Python data pipeline consultant\n"
        "Bob Smith,bob@designlabs.co,555-5678,200.00,"
        "Inactive,Senior AWS architect\n"
        "Carol Davis,carol@brightspark.com,555-9012,95.75,"
        "Active,Mobile UX designer\n"
        "Dave Wilson,dave@netcore.io,555-3456,180.00,"
        "Churned,Left to start own company\n"
        "Eve Martinez,eve@quantaml.ai,555-7890,110.00,"
        "Active,Machine learning scientist\n"
        "Frank Lee,frank@greenleaf.org,555-2345,150.00,"
        "Active,React dashboard developer\n"
        "Grace Kim,grace@orbithq.com,555-6789,175.00,"
        "Active,Kubernetes DevOps lead\n"
    )
    path = tmp_path / "clients.csv"
    path.write_text(content)
    return path


def _load_csv(csv_path: Path, store: Store) -> str:
    """Connect CSV, discover, read, and store everything. Returns source_id."""
    conn = CsvLocalConnector()
    conn.authenticate({"path": str(csv_path)})

    discovered = conn.discover()
    for entity in discovered:
        store.put_entity(entity)

    collection_id = discovered[0].id
    records = conn.read(collection_id)
    for record in records:
        store.put_entity(record)

    return discovered[0].source_id


class TestSearchEngineReturnsEntities:
    """Search returns Entity objects, not just IDs."""

    def test_returns_entity_objects(
        self, clients_csv: Path, store: Store, config: SearchConfig
    ) -> None:
        _load_csv(clients_csv, store)
        engine = SearchEngine(store, config)

        results = engine.search("Alice")
        assert len(results) >= 1
        from brij.core.models import Entity

        assert all(isinstance(r, Entity) for r in results)

    def test_entity_has_signals(
        self, clients_csv: Path, store: Store, config: SearchConfig
    ) -> None:
        _load_csv(clients_csv, store)
        engine = SearchEngine(store, config)

        results = engine.search("Alice")
        alice = results[0]
        assert alice.get_signal_value("field:name") == "Alice Johnson"


class TestSearchEngineLimit:
    """Limit is respected."""

    def test_limit_caps_results(
        self, clients_csv: Path, store: Store, config: SearchConfig
    ) -> None:
        _load_csv(clients_csv, store)
        engine = SearchEngine(store, config)

        # "Active" appears in 5 records, so asking for limit=2 should cap.
        results = engine.search("Active", limit=2)
        assert len(results) <= 2

    def test_default_limit_from_config(
        self, clients_csv: Path, store: Store
    ) -> None:
        _load_csv(clients_csv, store)
        custom = SearchConfig(default_limit=3)
        engine = SearchEngine(store, custom)

        results = engine.search("Active")
        assert len(results) <= 3

    def test_limit_one(
        self, clients_csv: Path, store: Store, config: SearchConfig
    ) -> None:
        _load_csv(clients_csv, store)
        engine = SearchEngine(store, config)

        results = engine.search("Active", limit=1)
        assert len(results) == 1


class TestSearchEngineSourceFilter:
    """Source filter works."""

    def test_single_source_filter(
        self, clients_csv: Path, store: Store, config: SearchConfig
    ) -> None:
        source_id = _load_csv(clients_csv, store)
        engine = SearchEngine(store, config)

        results = engine.search("Alice", sources=[source_id])
        assert len(results) >= 1
        assert results[0].get_signal_value("field:name") == "Alice Johnson"

    def test_nonexistent_source_returns_empty(
        self, store: Store, config: SearchConfig
    ) -> None:
        engine = SearchEngine(store, config)

        results = engine.search("Alice", sources=["nonexistent-source"])
        assert results == []

    def test_source_filter_excludes_other_sources(
        self, clients_csv: Path, store: Store, config: SearchConfig, tmp_path: Path
    ) -> None:
        """Load two CSVs, filter to one source, only get results from that source."""
        source_id_1 = _load_csv(clients_csv, store)

        # Create a second CSV with different data.
        other = tmp_path / "other.csv"
        other.write_text("name,role\nZara Patel,Engineer\n")
        conn2 = CsvLocalConnector()
        conn2.authenticate({"path": str(other)})
        for entity in conn2.discover():
            store.put_entity(entity)
        collection = conn2.discover()[0]
        for record in conn2.read(collection.id):
            store.put_entity(record)

        # Search across everything, then filter to source 1.
        all_results = engine_search_all(store, config, "Active")
        filtered = SearchEngine(store, config).search("Active", sources=[source_id_1])

        # Filtered results should all belong to source 1.
        for entity in filtered:
            assert entity.source_id == source_id_1


class TestSearchEngineEdgeCases:
    """Edge cases for search engine."""

    def test_empty_query_returns_empty(
        self, store: Store, config: SearchConfig
    ) -> None:
        engine = SearchEngine(store, config)
        assert engine.search("") == []
        assert engine.search("   ") == []

    def test_no_results(
        self, clients_csv: Path, store: Store, config: SearchConfig
    ) -> None:
        _load_csv(clients_csv, store)
        engine = SearchEngine(store, config)

        results = engine.search("xyznonexistent")
        assert results == []


def engine_search_all(store: Store, config: SearchConfig, query: str) -> list:
    """Helper to search without source filter."""
    return SearchEngine(store, config).search(query)
