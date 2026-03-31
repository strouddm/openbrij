"""Tests for connector base class and registry."""

from __future__ import annotations

from brij.connectors import base
from brij.connectors.base import BaseConnector, SyncResult
from brij.core.models import Entity, Signal

import brij.connectors as registry


# --- Concrete test connector ---


class StubConnector(BaseConnector):
    """Minimal concrete connector for testing."""

    def authenticate(self, credentials: dict) -> None:
        pass

    def discover(self) -> list[Entity]:
        return []

    def read(self, entity_id: str) -> list[Signal]:
        return []

    def write(self, entity_id: str, data: dict) -> bool:
        return True

    def sync(self) -> SyncResult:
        return SyncResult()


# --- make_entity_id ---


class TestMakeEntityId:
    def test_formats_type_and_id(self) -> None:
        result = BaseConnector.make_entity_id("collection", "my-file")
        assert result == "collection:my-file"

    def test_record_type(self) -> None:
        result = BaseConnector.make_entity_id("record", "row-42")
        assert result == "record:row-42"

    def test_preserves_special_characters(self) -> None:
        result = BaseConnector.make_entity_id("field", "col/name with spaces")
        assert result == "field:col/name with spaces"


# --- SyncResult ---


class TestSyncResult:
    def test_defaults_to_empty_lists(self) -> None:
        result = SyncResult()
        assert result.new == []
        assert result.modified == []
        assert result.deleted == []

    def test_accepts_values(self) -> None:
        result = SyncResult(new=["a"], modified=["b"], deleted=["c"])
        assert result.new == ["a"]
        assert result.modified == ["b"]
        assert result.deleted == ["c"]


# --- create_collection default ---


class TestCreateCollection:
    def test_raises_not_implemented(self) -> None:
        connector = StubConnector()
        try:
            connector.create_collection("test", {})
            assert False, "Expected NotImplementedError"
        except NotImplementedError as e:
            assert "StubConnector" in str(e)


# --- Exceptions ---


class TestExceptions:
    def test_authentication_error_is_connector_error(self) -> None:
        assert issubclass(base.AuthenticationError, base.ConnectorError)

    def test_entity_not_found_is_connector_error(self) -> None:
        assert issubclass(base.EntityNotFoundError, base.ConnectorError)

    def test_write_error_is_connector_error(self) -> None:
        assert issubclass(base.WriteError, base.ConnectorError)


# --- Registry ---


class TestRegistry:
    def setup_method(self) -> None:
        registry._registry.clear()

    def test_register_and_get(self) -> None:
        registry.register("stub", StubConnector)
        assert registry.get("stub") is StubConnector

    def test_get_unknown_returns_none(self) -> None:
        assert registry.get("nonexistent") is None

    def test_list_connectors(self) -> None:
        registry.register("stub", StubConnector)
        connectors = registry.list_connectors()
        assert "stub" in connectors
        assert connectors["stub"] is StubConnector

    def test_list_connectors_returns_copy(self) -> None:
        registry.register("stub", StubConnector)
        connectors = registry.list_connectors()
        connectors.clear()
        assert registry.get("stub") is StubConnector

    def test_discover_with_no_entry_points(self) -> None:
        registry.discover()
        # No brij.connectors entry points installed, so registry should remain empty
        assert registry.list_connectors() == {}

    def test_multiple_registrations(self) -> None:
        registry.register("stub1", StubConnector)
        registry.register("stub2", StubConnector)
        assert len(registry.list_connectors()) == 2
