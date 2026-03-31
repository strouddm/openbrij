"""Tests for hybrid search merging keyword and semantic results."""

from __future__ import annotations

from pathlib import Path

import pytest

from brij.config import SearchConfig
from brij.connectors.csv_local import CsvLocalConnector
from brij.core.models import Entity, Signal
from brij.core.store import Store
from brij.search.embeddings import EmbeddingEngine
from brij.search.engine import SearchEngine, _normalize_scores


@pytest.fixture()
def store() -> Store:
    s = Store(":memory:")
    yield s
    s.close()


@pytest.fixture()
def config() -> SearchConfig:
    return SearchConfig()


@pytest.fixture()
def embedding_engine() -> EmbeddingEngine:
    return EmbeddingEngine()


@pytest.fixture()
def clients_csv(tmp_path: Path) -> Path:
    """CSV with data designed to test semantic vs keyword gaps."""
    content = (
        "name,email,role,notes\n"
        "Alice Johnson,alice@techvault.io,Engineer,Python data pipeline consultant\n"
        "Bob Smith,bob@designlabs.co,Architect,Senior cloud infrastructure expert\n"
        "Carol Davis,carol@brightspark.com,Designer,Mobile user experience specialist\n"
        "Dave Wilson,dave@netcore.io,Manager,Left to start own company\n"
        "Eve Martinez,eve@quantaml.ai,Scientist,Machine learning and AI researcher\n"
    )
    path = tmp_path / "clients.csv"
    path.write_text(content)
    return path


def _load_and_embed(
    csv_path: Path,
    store: Store,
    embedding_engine: EmbeddingEngine,
) -> str:
    """Connect CSV, store entities, and generate embeddings. Returns source_id."""
    conn = CsvLocalConnector()
    conn.authenticate({"path": str(csv_path)})

    discovered = conn.discover()
    for entity in discovered:
        store.put_entity(entity)

    collection_id = discovered[0].id
    records = conn.read(collection_id)
    for record in records:
        store.put_entity(record)
        embedding_engine.embed_entity(record, store)

    return discovered[0].source_id


class TestHybridFindsSemanticMatches:
    """Hybrid search finds results that keyword alone misses."""

    def test_semantic_match_on_meaning(
        self,
        clients_csv: Path,
        store: Store,
        config: SearchConfig,
        embedding_engine: EmbeddingEngine,
    ) -> None:
        _load_and_embed(clients_csv, store, embedding_engine)

        keyword_engine = SearchEngine(store, config)
        hybrid_engine = SearchEngine(store, config, embedding_engine=embedding_engine)

        # "artificial intelligence" won't keyword-match, but should
        # semantically match Eve's "Machine learning and AI researcher".
        keyword_results = keyword_engine.search("artificial intelligence", limit=5)
        hybrid_results = hybrid_engine.search("artificial intelligence", limit=5)

        keyword_ids = {e.id for e in keyword_results}
        hybrid_ids = {e.id for e in hybrid_results}

        # Hybrid should find at least one result that keyword missed.
        assert len(hybrid_ids) > len(keyword_ids)

    def test_synonym_match(
        self,
        clients_csv: Path,
        store: Store,
        config: SearchConfig,
        embedding_engine: EmbeddingEngine,
    ) -> None:
        _load_and_embed(clients_csv, store, embedding_engine)
        hybrid = SearchEngine(store, config, embedding_engine=embedding_engine)

        # "UX" is semantically close to "user experience" (Carol's notes).
        results = hybrid.search("UX", limit=5)
        result_names = [e.get_signal_value("field:name") for e in results]
        assert "Carol Davis" in result_names


class TestDeduplication:
    """Results are deduplicated by entity_id."""

    def test_no_duplicate_entity_ids(
        self,
        clients_csv: Path,
        store: Store,
        config: SearchConfig,
        embedding_engine: EmbeddingEngine,
    ) -> None:
        _load_and_embed(clients_csv, store, embedding_engine)
        engine = SearchEngine(store, config, embedding_engine=embedding_engine)

        results = engine.search("Engineer", limit=10)
        ids = [e.id for e in results]
        assert len(ids) == len(set(ids))


class TestConfigRatioRespected:
    """The configured semantic/keyword weight ratio affects ranking."""

    def test_keyword_heavy_config(
        self,
        clients_csv: Path,
        store: Store,
        embedding_engine: EmbeddingEngine,
    ) -> None:
        """With 100% keyword weight, results match keyword-only search."""
        _load_and_embed(clients_csv, store, embedding_engine)

        keyword_config = SearchConfig(keyword_weight=1.0, semantic_weight=0.0)
        keyword_only = SearchEngine(store, keyword_config)
        keyword_heavy = SearchEngine(store, keyword_config, embedding_engine=embedding_engine)

        query = "Alice"
        kw_results = keyword_only.search(query, limit=5)
        heavy_results = keyword_heavy.search(query, limit=5)

        # Same top result when keyword weight is 100%.
        assert kw_results[0].id == heavy_results[0].id

    def test_semantic_heavy_config(
        self,
        clients_csv: Path,
        store: Store,
        embedding_engine: EmbeddingEngine,
    ) -> None:
        """With high semantic weight, semantic-only matches still appear."""
        _load_and_embed(clients_csv, store, embedding_engine)

        sem_config = SearchConfig(keyword_weight=0.0, semantic_weight=1.0)
        engine = SearchEngine(store, sem_config, embedding_engine=embedding_engine)

        # "artificial intelligence" has no keyword hit but semantic match.
        results = engine.search("artificial intelligence", limit=5)
        assert len(results) > 0


class TestNormalizeScores:
    """Unit tests for score normalization."""

    def test_normalize_empty(self) -> None:
        assert _normalize_scores({}) == {}

    def test_normalize_single(self) -> None:
        result = _normalize_scores({"a": 5.0})
        assert result == {"a": 1.0}

    def test_normalize_range(self) -> None:
        result = _normalize_scores({"a": 1.0, "b": 3.0, "c": 5.0})
        assert result["a"] == pytest.approx(0.0)
        assert result["b"] == pytest.approx(0.5)
        assert result["c"] == pytest.approx(1.0)

    def test_normalize_all_same(self) -> None:
        result = _normalize_scores({"a": 3.0, "b": 3.0})
        assert result == {"a": 1.0, "b": 1.0}


class TestFallbackToKeywordOnly:
    """Without an embedding engine, search falls back to keyword-only."""

    def test_no_embedding_engine(
        self,
        clients_csv: Path,
        store: Store,
        config: SearchConfig,
    ) -> None:
        conn = CsvLocalConnector()
        conn.authenticate({"path": str(clients_csv)})
        discovered = conn.discover()
        for entity in discovered:
            store.put_entity(entity)
        collection_id = discovered[0].id
        for record in conn.read(collection_id):
            store.put_entity(record)

        engine = SearchEngine(store, config)
        results = engine.search("Alice")
        assert len(results) >= 1
        assert results[0].get_signal_value("field:name") == "Alice Johnson"
