"""Tests for the MCP write tool."""

from __future__ import annotations

from pathlib import Path

import pytest

from brij.connectors.csv_local import CsvLocalConnector
from brij.core.store import Store
from brij.mcp.tools import search, write


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


class TestWriteAdd:
    """Tests for the write tool 'add' action."""

    def test_add_record_appears_in_search(
        self, clients_csv: Path, store: Store
    ) -> None:
        """Add a record via write tool, verify it appears in search."""
        source_id = _connect_source(clients_csv, store)
        collection_id = f"collection:{clients_csv.name}"

        result = write(
            store,
            action="add",
            source_id=source_id,
            collection_id=collection_id,
            data={"name": "Zara Test", "email": "zara@example.com", "role": "Tester"},
        )

        assert "Added" in result
        assert "Zara Test" in result

        # Verify the new record is found by search.
        search_result = search(store, "Zara")
        assert "Zara Test" in search_result

    def test_add_record_written_to_csv(
        self, clients_csv: Path, store: Store
    ) -> None:
        """Added record is also written through to the CSV file."""
        source_id = _connect_source(clients_csv, store)
        collection_id = f"collection:{clients_csv.name}"

        write(
            store,
            action="add",
            source_id=source_id,
            collection_id=collection_id,
            data={"name": "New Person", "email": "new@example.com", "role": "Intern"},
        )

        csv_content = clients_csv.read_text()
        assert "New Person" in csv_content
        assert "new@example.com" in csv_content

    def test_add_requires_collection_id(self, clients_csv: Path, store: Store) -> None:
        source_id = _connect_source(clients_csv, store)

        result = write(store, action="add", source_id=source_id, data={"name": "X"})
        assert "collection_id" in result


class TestWriteUpdate:
    """Tests for the write tool 'update' action."""

    def test_update_field_verified_in_store(
        self, clients_csv: Path, store: Store
    ) -> None:
        """Update a field, verify the change persists in the store."""
        source_id = _connect_source(clients_csv, store)

        # Alice is row 0.
        entity_id = f"record:{clients_csv.name}:0"

        result = write(
            store,
            action="update",
            source_id=source_id,
            entity_id=entity_id,
            data={"role": "Lead Engineer"},
        )

        assert "Updated" in result

        # Verify the change in the store.
        entity = store.get_entity(entity_id)
        assert entity is not None
        assert entity.get_signal_value("field:role") == "Lead Engineer"

    def test_update_field_written_to_csv(
        self, clients_csv: Path, store: Store
    ) -> None:
        """Updated field is also written through to the CSV file."""
        source_id = _connect_source(clients_csv, store)
        entity_id = f"record:{clients_csv.name}:0"

        write(
            store,
            action="update",
            source_id=source_id,
            entity_id=entity_id,
            data={"role": "Lead Engineer"},
        )

        csv_content = clients_csv.read_text()
        assert "Lead Engineer" in csv_content

    def test_update_nonexistent_entity(
        self, clients_csv: Path, store: Store
    ) -> None:
        source_id = _connect_source(clients_csv, store)

        result = write(
            store,
            action="update",
            source_id=source_id,
            entity_id="record:clients.csv:999",
            data={"role": "Ghost"},
        )
        assert "not found" in result

    def test_update_requires_entity_id(
        self, clients_csv: Path, store: Store
    ) -> None:
        source_id = _connect_source(clients_csv, store)

        result = write(
            store, action="update", source_id=source_id, data={"role": "X"}
        )
        assert "entity_id" in result


class TestWriteDelete:
    """Tests for the write tool 'delete' action."""

    def test_delete_record_gone_from_store(
        self, clients_csv: Path, store: Store
    ) -> None:
        """Delete a record, verify it's gone from the store."""
        source_id = _connect_source(clients_csv, store)

        # Bob is row 1.
        entity_id = f"record:{clients_csv.name}:1"

        result = write(
            store,
            action="delete",
            source_id=source_id,
            entity_id=entity_id,
        )

        assert "Deleted" in result

        # Verify gone from store.
        entity = store.get_entity(entity_id)
        assert entity is None

    def test_delete_record_removed_from_csv(
        self, clients_csv: Path, store: Store
    ) -> None:
        """Deleted record is also removed from the CSV file."""
        source_id = _connect_source(clients_csv, store)
        entity_id = f"record:{clients_csv.name}:1"

        write(
            store,
            action="delete",
            source_id=source_id,
            entity_id=entity_id,
        )

        csv_content = clients_csv.read_text()
        assert "Bob Smith" not in csv_content

    def test_delete_nonexistent_entity(
        self, clients_csv: Path, store: Store
    ) -> None:
        source_id = _connect_source(clients_csv, store)

        result = write(
            store,
            action="delete",
            source_id=source_id,
            entity_id="record:clients.csv:999",
        )
        assert "not found" in result

    def test_delete_requires_entity_id(
        self, clients_csv: Path, store: Store
    ) -> None:
        source_id = _connect_source(clients_csv, store)

        result = write(store, action="delete", source_id=source_id)
        assert "entity_id" in result


class TestWriteCreate:
    """Tests for the write tool 'create' action."""

    def test_create_collection(
        self, clients_csv: Path, store: Store, tmp_path: Path
    ) -> None:
        """Create a new collection and verify it exists in the store."""
        source_id = _connect_source(clients_csv, store)

        result = write(
            store,
            action="create",
            source_id=source_id,
            data={"name": "projects", "fields": ["title", "status", "owner"]},
        )

        assert "Created" in result
        assert "projects" in result

        # Collection entity exists in the store.
        entity = store.get_entity("collection:projects.csv")
        assert entity is not None
        assert entity.get_signal_value("name") == "projects.csv"

        # CSV file was created on disk.
        new_csv = tmp_path / "projects.csv"
        assert new_csv.exists()
        assert "title,status,owner" in new_csv.read_text()


class TestWriteInvalidAction:
    """Tests for invalid write actions."""

    def test_invalid_action_returns_error(
        self, clients_csv: Path, store: Store
    ) -> None:
        source_id = _connect_source(clients_csv, store)

        result = write(store, action="explode", source_id=source_id)
        assert "Invalid action" in result
