"""Tests for approximate nearest neighbor index."""

from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pytest

from brij.config import SearchConfig
from brij.connectors.csv_local import CsvLocalConnector
from brij.core.store import Store
from brij.search.ann_index import FAISS_AVAILABLE, ANNIndex
from brij.search.embeddings import EmbeddingEngine
from brij.search.engine import SearchEngine


def _random_vector(dim: int = 384, seed: int | None = None) -> bytes:
    """Generate a random float32 vector as bytes."""
    rng = np.random.RandomState(seed)
    vec = rng.randn(dim).astype(np.float32)
    return vec.tobytes()


def _known_vector(value: float, dim: int = 384) -> bytes:
    """Generate a constant-valued float32 vector as bytes."""
    vec = np.full(dim, value, dtype=np.float32)
    return vec.tobytes()


class TestANNIndexBasics:
    """Core index operations."""

    def test_empty_index_returns_no_results(self) -> None:
        index = ANNIndex()
        results = index.search(_random_vector(), k=5)
        assert results == []

    def test_size_tracks_added_vectors(self) -> None:
        index = ANNIndex()
        assert index.size == 0
        index.add("e1", _random_vector(seed=1))
        assert index.size == 1
        index.add("e2", _random_vector(seed=2))
        assert index.size == 2

    def test_add_and_search_single(self) -> None:
        index = ANNIndex()
        vec = _random_vector(seed=42)
        index.add("e1", vec)

        results = index.search(vec, k=1)
        assert len(results) == 1
        assert results[0][0] == "e1"
        assert results[0][1] == pytest.approx(1.0, abs=1e-5)

    def test_search_returns_nearest(self) -> None:
        index = ANNIndex()

        # Create two distinct vectors.
        vec_a = np.zeros(384, dtype=np.float32)
        vec_a[0] = 1.0
        vec_b = np.zeros(384, dtype=np.float32)
        vec_b[1] = 1.0

        index.add("a", vec_a.tobytes())
        index.add("b", vec_b.tobytes())

        # Query close to vec_a.
        query = np.zeros(384, dtype=np.float32)
        query[0] = 1.0
        query[1] = 0.1
        results = index.search(query.tobytes(), k=2)

        assert results[0][0] == "a"
        assert results[1][0] == "b"
        assert results[0][1] > results[1][1]

    def test_k_larger_than_index_size(self) -> None:
        index = ANNIndex()
        index.add("e1", _random_vector(seed=1))
        results = index.search(_random_vector(seed=1), k=100)
        assert len(results) == 1


class TestAddBulk:
    """Bulk insertion."""

    def test_bulk_add_matches_individual(self) -> None:
        ids = [f"e{i}" for i in range(5)]
        vecs = [_random_vector(seed=i) for i in range(5)]

        idx_individual = ANNIndex()
        for eid, v in zip(ids, vecs):
            idx_individual.add(eid, v)

        idx_bulk = ANNIndex()
        idx_bulk.add_bulk(ids, vecs)

        assert idx_bulk.size == idx_individual.size

        query = _random_vector(seed=99)
        res_ind = idx_individual.search(query, k=3)
        res_bulk = idx_bulk.search(query, k=3)

        assert [r[0] for r in res_ind] == [r[0] for r in res_bulk]

    def test_bulk_add_empty(self) -> None:
        index = ANNIndex()
        index.add_bulk([], [])
        assert index.size == 0


class TestZeroVector:
    """Edge case: zero vectors should not cause errors."""

    def test_add_zero_vector(self) -> None:
        index = ANNIndex()
        zero = np.zeros(384, dtype=np.float32).tobytes()
        index.add("z", zero)
        results = index.search(zero, k=1)
        assert len(results) == 1

    def test_search_with_zero_query(self) -> None:
        index = ANNIndex()
        index.add("e1", _random_vector(seed=1))
        zero = np.zeros(384, dtype=np.float32).tobytes()
        results = index.search(zero, k=1)
        assert len(results) == 1


class TestFAISSDetection:
    """Verify FAISS availability flag."""

    def test_faiss_flag_is_boolean(self) -> None:
        assert isinstance(FAISS_AVAILABLE, bool)


class TestSearchEngineIntegration:
    """Integration test: ANN index produces same results as old brute-force."""

    def test_semantic_search_through_engine(self) -> None:
        """Verify SearchEngine still returns correct entities via ANN path."""
        store = Store(":memory:")
        config = SearchConfig()
        emb = EmbeddingEngine()

        csv_content = (
            "name,role,notes\n"
            "Alice,Engineer,Python data pipeline developer\n"
            "Bob,Architect,Cloud infrastructure and DevOps\n"
            "Eve,Scientist,Machine learning researcher\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        try:
            conn = CsvLocalConnector()
            conn.authenticate({"path": str(csv_path)})
            discovered = conn.discover()
            for entity in discovered:
                store.put_entity(entity)
            for record in conn.read(discovered[0].id):
                store.put_entity(record)
                emb.embed_entity(record, store)

            engine = SearchEngine(store, config, embedding_engine=emb)
            results = engine.search("artificial intelligence", limit=5)

            # Should find Eve (ML researcher) semantically.
            names = [e.get_signal_value("field:name") for e in results]
            assert "Eve" in names
        finally:
            csv_path.unlink()
            store.close()
