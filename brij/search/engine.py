"""Search engine for querying stored entities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np

from brij.core.models import Entity
from brij.search.ann_index import ANNIndex

if TYPE_CHECKING:
    from brij.config import SearchConfig
    from brij.core.store import Store
    from brij.search.embeddings import EmbeddingEngine

logger = logging.getLogger(__name__)


def _normalize_scores(scored: dict[str, float]) -> dict[str, float]:
    """Normalize scores to 0-1 range."""
    if not scored:
        return {}
    values = list(scored.values())
    min_val = min(values)
    max_val = max(values)
    span = max_val - min_val
    if span == 0:
        return {eid: 1.0 for eid in scored}
    return {eid: (s - min_val) / span for eid, s in scored.items()}


def _cosine_similarity(a: bytes, b: bytes) -> float:
    """Compute cosine similarity between two serialized float32 vectors."""
    va = np.frombuffer(a, dtype=np.float32)
    vb = np.frombuffer(b, dtype=np.float32)
    dot = float(np.dot(va, vb))
    norm = float(np.linalg.norm(va) * np.linalg.norm(vb))
    if norm == 0:
        return 0.0
    return dot / norm


class SearchEngine:
    """Combines keyword and semantic search into hybrid results.

    When no embedding engine is provided, falls back to keyword-only search.
    """

    def __init__(
        self,
        store: Store,
        config: SearchConfig,
        embedding_engine: EmbeddingEngine | None = None,
    ) -> None:
        self._store = store
        self._config = config
        self._embedding_engine = embedding_engine

    def search(
        self,
        query: str,
        sources: list[str] | None = None,
        limit: int | None = None,
    ) -> list[Entity]:
        """Search for entities matching the query using hybrid ranking.

        Combines keyword (FTS5) and semantic (cosine similarity) results.
        Scores are normalized to 0-1 and merged at the configured ratio
        (default 70% semantic / 30% keyword). Deduplicates by entity_id,
        keeping the best combined score.

        Falls back to keyword-only when no embedding engine is available.

        Args:
            query: The search query string.
            sources: Optional list of source IDs to filter by.
            limit: Maximum number of results to return.
                   Defaults to config.default_limit.

        Returns:
            List of Entity objects ranked by hybrid relevance.
        """
        if limit is None:
            limit = self._config.default_limit

        if not query or not query.strip():
            return []

        # Keyword path.
        keyword_scored = self._keyword_search(query, sources, limit)

        # Semantic path (when embedding engine is available).
        semantic_scored: dict[str, float] = {}
        if self._embedding_engine is not None:
            semantic_scored = self._semantic_search(query, sources, limit)

        # Merge.
        if semantic_scored:
            merged = self._merge_scores(keyword_scored, semantic_scored)
        else:
            merged = keyword_scored

        ranked = sorted(merged.items(), key=lambda item: item[1], reverse=True)[:limit]

        entities: list[Entity] = []
        for entity_id, _score in ranked:
            entity = self._store.get_entity(entity_id)
            if entity is not None:
                entities.append(entity)

        logger.debug("Search for %r returned %d entities", query, len(entities))
        return entities

    def _keyword_search(
        self,
        query: str,
        sources: list[str] | None,
        limit: int,
    ) -> dict[str, float]:
        """Run keyword search, returning {entity_id: score}."""
        if sources:
            scored: dict[str, float] = {}
            for source_id in sources:
                hits = self._store.keyword_search(query, source_id=source_id, limit=limit)
                for entity_id, score in hits:
                    if entity_id not in scored or score > scored[entity_id]:
                        scored[entity_id] = score
            return scored
        else:
            return dict(self._store.keyword_search(query, limit=limit))

    def _semantic_search(
        self,
        query: str,
        sources: list[str] | None,
        limit: int,
    ) -> dict[str, float]:
        """Embed the query and find nearest neighbors via ANN index.

        Builds an ANNIndex from stored embeddings and uses it to find
        the top matches. Uses FAISS when available, otherwise falls back
        to brute-force inner-product search.
        """
        assert self._embedding_engine is not None
        query_vector = self._embedding_engine.embed(query)

        if sources:
            all_embeddings: list[dict] = []
            for source_id in sources:
                all_embeddings.extend(self._store.get_all_embeddings(source_id=source_id))
        else:
            all_embeddings = self._store.get_all_embeddings()

        if not all_embeddings:
            return {}

        index = ANNIndex()
        entity_ids = [emb["entity_id"] for emb in all_embeddings]
        vectors = [emb["vector"] for emb in all_embeddings]
        index.add_bulk(entity_ids, vectors)

        results = index.search(query_vector, k=limit)

        scored: dict[str, float] = {}
        for eid, score in results:
            if eid not in scored or score > scored[eid]:
                scored[eid] = score

        return scored

    def _merge_scores(
        self,
        keyword_scored: dict[str, float],
        semantic_scored: dict[str, float],
    ) -> dict[str, float]:
        """Normalize and merge keyword + semantic scores at configured ratio."""
        kw_norm = _normalize_scores(keyword_scored)
        sem_norm = _normalize_scores(semantic_scored)

        kw_weight = self._config.keyword_weight
        sem_weight = self._config.semantic_weight

        all_ids = set(kw_norm) | set(sem_norm)
        merged: dict[str, float] = {}
        for eid in all_ids:
            kw = kw_norm.get(eid, 0.0) * kw_weight
            sem = sem_norm.get(eid, 0.0) * sem_weight
            merged[eid] = kw + sem

        return merged
