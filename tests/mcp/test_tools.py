"""Tests for MCP tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from brij.connectors.csv_local import CsvLocalConnector
from brij.core.store import Store
from brij.mcp.responses import _CHARS_PER_TOKEN, _DEFAULT_TOKEN_BUDGET
from brij.mcp.tools import discover


@pytest.fixture()
def store() -> Store:
    s = Store(":memory:")
    yield s
    s.close()


@pytest.fixture()
def clients_csv(tmp_path: Path) -> Path:
    """CSV fixture with a few rows."""
    content = (
        "name,email,role\n"
        "Alice Johnson,alice@example.com,Engineer\n"
        "Bob Smith,bob@example.com,Designer\n"
        "Carol Davis,carol@example.com,Manager\n"
    )
    path = tmp_path / "clients.csv"
    path.write_text(content)
    return path


def _connect_source(csv_path: Path, store: Store) -> str:
    """Connect a CSV source and store all entities. Returns the source_id."""
    conn = CsvLocalConnector()
    conn.authenticate({"path": str(csv_path)})

    discovered = conn.discover()
    source_id = discovered[0].source_id

    store.add_source(source_id, csv_path.stem, "csv_local")

    for entity in discovered:
        store.put_entity(entity)

    collection_id = discovered[0].id
    records = conn.read(collection_id)
    for record in records:
        store.put_entity(record)

    return source_id


class TestDiscover:
    """Tests for the discover tool."""

    def test_no_sources_returns_helpful_message(self, store: Store) -> None:
        result = discover(store)
        assert "No data sources" in result
        assert "Connect a source" in result

    def test_one_source_returns_readable_summary(
        self, clients_csv: Path, store: Store
    ) -> None:
        _connect_source(clients_csv, store)

        result = discover(store)

        assert "Data Catalog" in result
        assert "clients" in result
        assert "csv_local" in result
        assert "Records: 3" in result

    def test_summary_includes_field_names(
        self, clients_csv: Path, store: Store
    ) -> None:
        _connect_source(clients_csv, store)

        result = discover(store)

        assert "name" in result
        assert "email" in result
        assert "role" in result

    def test_summary_includes_entity_counts(
        self, clients_csv: Path, store: Store
    ) -> None:
        _connect_source(clients_csv, store)

        result = discover(store)

        # Footer should show totals.
        assert "entities" in result
        assert "signals" in result

    def test_response_under_token_budget(
        self, clients_csv: Path, store: Store
    ) -> None:
        _connect_source(clients_csv, store)

        result = discover(store)

        max_chars = _DEFAULT_TOKEN_BUDGET * _CHARS_PER_TOKEN
        assert len(result) <= max_chars

    def test_multiple_sources(
        self, tmp_path: Path, store: Store
    ) -> None:
        """Discover with two sources lists both."""
        csv1 = tmp_path / "contacts.csv"
        csv1.write_text("name,phone\nAlice,555-1234\n")

        csv2 = tmp_path / "projects.csv"
        csv2.write_text("title,status\nAlpha,Active\n")

        _connect_source(csv1, store)
        _connect_source(csv2, store)

        result = discover(store)

        assert "contacts" in result
        assert "projects" in result

    def test_response_is_plain_text(
        self, clients_csv: Path, store: Store
    ) -> None:
        """Response should be plain text, not JSON."""
        _connect_source(clients_csv, store)

        result = discover(store)

        assert not result.strip().startswith("{")
        assert not result.strip().startswith("[")
