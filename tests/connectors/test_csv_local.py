"""Tests for the CSV local file connector — discover phase."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from brij.connectors.base import AuthenticationError
from brij.connectors.csv_local import CsvLocalConnector


# ---- Fixtures ----


@pytest.fixture()
def csv_file(tmp_path: Path) -> Path:
    """Create a simple fixture CSV with mixed column types."""
    content = textwrap.dedent("""\
        name,email,age,rate,active
        Alice,alice@example.com,30,125.50,true
        Bob,bob@example.com,45,200.00,false
        Carol,carol@example.com,28,95.75,true
    """)
    path = tmp_path / "clients.csv"
    path.write_text(content)
    return path


@pytest.fixture()
def empty_csv(tmp_path: Path) -> Path:
    """CSV with headers but no data rows."""
    path = tmp_path / "empty.csv"
    path.write_text("name,email\n")
    return path


@pytest.fixture()
def headers_only_csv(tmp_path: Path) -> Path:
    """CSV with only a header row and no trailing newline."""
    path = tmp_path / "headers.csv"
    path.write_text("col_a,col_b,col_c")
    return path


# ---- Authenticate ----


class TestAuthenticate:
    def test_valid_path(self, csv_file: Path) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv_file)})
        # No exception means success.

    def test_missing_path_key(self) -> None:
        conn = CsvLocalConnector()
        with pytest.raises(AuthenticationError, match="path"):
            conn.authenticate({})

    def test_nonexistent_file(self, tmp_path: Path) -> None:
        conn = CsvLocalConnector()
        with pytest.raises(AuthenticationError, match="File not found"):
            conn.authenticate({"path": str(tmp_path / "nope.csv")})

    def test_empty_path_string(self) -> None:
        conn = CsvLocalConnector()
        with pytest.raises(AuthenticationError, match="path"):
            conn.authenticate({"path": ""})


# ---- Discover ----


class TestDiscover:
    def test_collection_entity(self, csv_file: Path) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv_file)})
        entities = conn.discover()

        collection = entities[0]
        assert collection.type == "collection"
        assert collection.name == "clients.csv"
        assert collection.get_signal_value("type") == "csv"
        assert collection.get_signal_value("row_count") == "3"
        assert collection.get_signal_value("location") == str(csv_file.resolve())
        # modified signal should be a valid ISO timestamp
        assert collection.get_signal_value("modified") is not None

    def test_field_entities_match_headers(self, csv_file: Path) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv_file)})
        entities = conn.discover()

        fields = [e for e in entities if e.type == "field"]
        field_names = [e.name for e in fields]
        assert field_names == ["name", "email", "age", "rate", "active"]

    def test_field_entities_are_children_of_collection(self, csv_file: Path) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv_file)})
        entities = conn.discover()

        collection = entities[0]
        fields = [e for e in entities if e.type == "field"]
        for f in fields:
            assert f.parent_id == collection.id

    def test_field_type_inference(self, csv_file: Path) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv_file)})
        entities = conn.discover()

        fields = {e.name: e for e in entities if e.type == "field"}
        assert fields["name"].get_signal_value("type") == "text"
        assert fields["email"].get_signal_value("type") == "text"
        assert fields["age"].get_signal_value("type") == "integer"
        assert fields["rate"].get_signal_value("type") == "float"
        assert fields["active"].get_signal_value("type") == "boolean"

    def test_total_entity_count(self, csv_file: Path) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv_file)})
        entities = conn.discover()
        # 1 collection + 5 fields
        assert len(entities) == 6

    def test_empty_csv_has_zero_rows(self, empty_csv: Path) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(empty_csv)})
        entities = conn.discover()

        collection = entities[0]
        assert collection.get_signal_value("row_count") == "0"
        # Still has field entities for the headers
        fields = [e for e in entities if e.type == "field"]
        assert len(fields) == 2

    def test_headers_only_csv(self, headers_only_csv: Path) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(headers_only_csv)})
        entities = conn.discover()

        collection = entities[0]
        assert collection.get_signal_value("row_count") == "0"
        fields = [e for e in entities if e.type == "field"]
        assert len(fields) == 3

    def test_discover_before_authenticate_raises(self) -> None:
        conn = CsvLocalConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.discover()

    def test_source_id_set_on_all_entities(self, csv_file: Path) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(csv_file)})
        entities = conn.discover()
        for entity in entities:
            assert entity.source_id == f"csv:{csv_file.name}"
