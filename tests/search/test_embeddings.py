"""Tests for embedding generation."""

import numpy as np
import pytest

from brij.core.models import Entity, Signal
from brij.core.store import Store
from brij.search.embeddings import DEFAULT_MODEL, EmbeddingEngine


@pytest.fixture()
def engine():
    return EmbeddingEngine()


@pytest.fixture()
def store():
    s = Store(":memory:")
    yield s
    s.close()


class TestEmbed:
    """Tests for the embed() method."""

    def test_embed_returns_bytes(self, engine):
        result = engine.embed("hello world")
        assert isinstance(result, bytes)

    def test_embed_output_has_expected_dimension(self, engine):
        result = engine.embed("hello world")
        vector = np.frombuffer(result, dtype=np.float32)
        # all-MiniLM-L6-v2 produces 384-dimensional embeddings
        assert vector.shape == (384,)

    def test_embed_same_text_produces_same_embedding(self, engine):
        a = engine.embed("the quick brown fox")
        b = engine.embed("the quick brown fox")
        assert a == b

    def test_embed_different_text_produces_different_embedding(self, engine):
        a = engine.embed("the quick brown fox")
        b = engine.embed("financial quarterly report")
        assert a != b


class TestEmbedBatch:
    """Tests for the embed_batch() method."""

    def test_embed_batch_returns_list_of_bytes(self, engine):
        results = engine.embed_batch(["hello", "world"])
        assert len(results) == 2
        assert all(isinstance(r, bytes) for r in results)

    def test_embed_batch_matches_individual_embeds(self, engine):
        texts = ["hello world", "goodbye world"]
        batch = engine.embed_batch(texts)
        individual = [engine.embed(t) for t in texts]
        for b, i in zip(batch, individual):
            np.testing.assert_array_equal(
                np.frombuffer(b, dtype=np.float32),
                np.frombuffer(i, dtype=np.float32),
            )

    def test_embed_batch_empty_list(self, engine):
        results = engine.embed_batch([])
        assert results == []


class TestEmbedEntity:
    """Tests for the embed_entity() method."""

    def test_embed_entity_stores_embedding(self, engine, store):
        entity = Entity(
            id="test-1",
            type="record",
            source_id="src-1",
            signals=[Signal(kind="name", value="Alice Johnson")],
        )
        store.put_entity(entity)
        engine.embed_entity(entity, store)

        row = store.get_embedding("test-1")
        assert row is not None
        assert row["model"] == DEFAULT_MODEL
        vector = np.frombuffer(row["vector"], dtype=np.float32)
        assert vector.shape == (384,)

    def test_embed_entity_concatenates_signals(self, engine, store):
        entity = Entity(
            id="test-2",
            type="record",
            source_id="src-1",
            signals=[
                Signal(kind="name", value="Bob Smith"),
                Signal(kind="summary", value="Senior engineer"),
                Signal(kind="field:email", value="bob@example.com"),
            ],
        )
        store.put_entity(entity)
        engine.embed_entity(entity, store)

        row = store.get_embedding("test-2")
        assert row is not None

    def test_embed_entity_skips_when_no_embeddable_signals(self, engine, store):
        entity = Entity(
            id="test-3",
            type="record",
            source_id="src-1",
            signals=[Signal(kind="tag", value="important")],
        )
        store.put_entity(entity)
        engine.embed_entity(entity, store)

        row = store.get_embedding("test-3")
        assert row is None

    def test_embed_entity_includes_preview_signal(self, engine, store):
        entity = Entity(
            id="test-4",
            type="record",
            source_id="src-1",
            signals=[Signal(kind="preview", value="A brief preview of the content")],
        )
        store.put_entity(entity)
        engine.embed_entity(entity, store)

        row = store.get_embedding("test-4")
        assert row is not None


class TestEngineInit:
    """Tests for EmbeddingEngine initialization."""

    def test_default_model_name(self, engine):
        assert engine.model_name == DEFAULT_MODEL
