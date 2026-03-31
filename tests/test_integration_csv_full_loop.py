"""Integration test: CSV connect → store → search full loop."""

from __future__ import annotations

from pathlib import Path

import pytest

from brij.connectors.csv_local import CsvLocalConnector
from brij.core.store import Store


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


@pytest.fixture()
def store() -> Store:
    s = Store(":memory:")
    yield s
    s.close()


class TestCsvFullLoop:
    """End-to-end: connect CSV → store all entities → keyword search."""

    def _connect_and_store(self, csv_path: Path, store: Store) -> str:
        """Run the full connect → discover → read → store pipeline.

        Returns the source_id.
        """
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv_path)})

        # Discover collection and field entities.
        discovered = conn.discover()
        for entity in discovered:
            store.put_entity(entity)

        # Read all record entities and store them.
        collection_id = discovered[0].id
        records = conn.read(collection_id)
        for record in records:
            store.put_entity(record)

        return discovered[0].source_id

    def test_all_entities_stored(self, clients_csv: Path, store: Store) -> None:
        self._connect_and_store(clients_csv, store)
        # 1 collection + 6 fields + 7 records = 14
        assert store.count_entities() == 14

    def test_search_by_name(self, clients_csv: Path, store: Store) -> None:
        self._connect_and_store(clients_csv, store)

        results = store.keyword_search("Alice")
        entity_ids = [eid for eid, _ in results]
        assert len(entity_ids) >= 1

        # The record containing Alice should be found.
        alice = store.get_entity(entity_ids[0])
        assert alice is not None
        assert alice.get_signal_value("field:name") == "Alice Johnson"

    def test_search_by_email(self, clients_csv: Path, store: Store) -> None:
        self._connect_and_store(clients_csv, store)

        # FTS5 treats '@' as a token separator, so search by email prefix.
        results = store.keyword_search("carol")
        entity_ids = [eid for eid, _ in results]
        assert len(entity_ids) >= 1

        found = store.get_entity(entity_ids[0])
        assert found is not None
        assert found.get_signal_value("field:email") == "carol@brightspark.com"

    def test_search_by_field_value(self, clients_csv: Path, store: Store) -> None:
        self._connect_and_store(clients_csv, store)

        results = store.keyword_search("Kubernetes")
        entity_ids = [eid for eid, _ in results]
        assert len(entity_ids) >= 1

        grace = store.get_entity(entity_ids[0])
        assert grace is not None
        assert "Kubernetes" in (grace.get_signal_value("field:notes") or "")

    def test_search_by_status(self, clients_csv: Path, store: Store) -> None:
        self._connect_and_store(clients_csv, store)

        results = store.keyword_search("Inactive")
        entity_ids = [eid for eid, _ in results]
        assert len(entity_ids) >= 1

        bob = store.get_entity(entity_ids[0])
        assert bob is not None
        assert bob.get_signal_value("field:status") == "Inactive"

    def test_search_with_source_filter(self, clients_csv: Path, store: Store) -> None:
        source_id = self._connect_and_store(clients_csv, store)

        all_results = store.keyword_search("Alice")
        filtered = store.keyword_search("Alice", source_id=source_id)
        assert len(filtered) == len(all_results)

    def test_records_are_tier_3(self, clients_csv: Path, store: Store) -> None:
        self._connect_and_store(clients_csv, store)

        results = store.keyword_search("Eve")
        entity_ids = [eid for eid, _ in results]
        assert len(entity_ids) >= 1

        eve = store.get_entity(entity_ids[0])
        assert eve is not None
        assert eve.tier == 3

    def test_no_results_for_missing_term(self, clients_csv: Path, store: Store) -> None:
        self._connect_and_store(clients_csv, store)

        results = store.keyword_search("xyznonexistent")
        assert results == []
