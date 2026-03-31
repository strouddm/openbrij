"""Tests for Store embedding methods."""

import pytest

from brij.core.models import Entity, Signal
from brij.core.store import Store


@pytest.fixture()
def store():
    s = Store(":memory:")
    yield s
    s.close()


class TestPutEmbedding:
    """Tests for put_embedding and get_embedding."""

    def test_put_and_get_embedding(self, store):
        entity = Entity(id="e1", type="record", source_id="src-1")
        store.put_entity(entity)

        vector = b"\x00" * 128
        store.put_embedding("e1", vector, "test-model")

        row = store.get_embedding("e1")
        assert row is not None
        assert row["entity_id"] == "e1"
        assert row["vector"] == vector
        assert row["model"] == "test-model"

    def test_put_embedding_replaces_existing(self, store):
        entity = Entity(id="e1", type="record", source_id="src-1")
        store.put_entity(entity)

        store.put_embedding("e1", b"\x00" * 128, "model-a")
        store.put_embedding("e1", b"\xff" * 128, "model-b")

        row = store.get_embedding("e1")
        assert row["vector"] == b"\xff" * 128
        assert row["model"] == "model-b"

    def test_get_embedding_returns_none_when_missing(self, store):
        assert store.get_embedding("nonexistent") is None

    def test_embedding_deleted_on_entity_cascade(self, store):
        entity = Entity(id="e1", type="record", source_id="src-1")
        store.put_entity(entity)
        store.put_embedding("e1", b"\x00" * 128, "test-model")

        store.delete_entity("e1")
        assert store.get_embedding("e1") is None
