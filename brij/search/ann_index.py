"""Approximate nearest neighbor index for semantic search.

Uses FAISS when available for fast ANN search. Falls back to brute-force
cosine similarity when FAISS is not installed, keeping the zero-dependency
path working.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

try:
    import faiss

    FAISS_AVAILABLE = True
except ImportError:  # pragma: no cover
    FAISS_AVAILABLE = False

# Dimension for all-MiniLM-L6-v2 embeddings.
_DEFAULT_DIM = 384


class ANNIndex:
    """Approximate nearest neighbor index backed by FAISS or brute-force fallback.

    The index maps integer positions to entity IDs. Call ``add`` to insert
    vectors, then ``search`` to find the nearest neighbors for a query vector.
    """

    def __init__(self, dimension: int = _DEFAULT_DIM) -> None:
        self._dimension = dimension
        self._entity_ids: list[str] = []

        if FAISS_AVAILABLE:
            self._index: faiss.IndexFlatIP | None = faiss.IndexFlatIP(dimension)
            logger.debug("Created FAISS IndexFlatIP (dim=%d)", dimension)
        else:
            self._index = None
            self._vectors: list[np.ndarray] = []
            logger.debug("FAISS not available, using brute-force fallback (dim=%d)", dimension)

    @property
    def size(self) -> int:
        """Return the number of vectors in the index."""
        return len(self._entity_ids)

    def add(self, entity_id: str, vector_bytes: bytes) -> None:
        """Add a single vector to the index.

        Args:
            entity_id: The entity this vector belongs to.
            vector_bytes: Serialized float32 numpy array.
        """
        vec = np.frombuffer(vector_bytes, dtype=np.float32).copy()
        # Normalize for inner-product similarity (equivalent to cosine on unit vectors).
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm

        self._entity_ids.append(entity_id)

        if self._index is not None:
            self._index.add(vec.reshape(1, -1))
        else:
            self._vectors.append(vec)

    def add_bulk(self, entity_ids: list[str], vectors_bytes: list[bytes]) -> None:
        """Add multiple vectors at once (more efficient for FAISS).

        Args:
            entity_ids: Entity IDs corresponding to each vector.
            vectors_bytes: Serialized float32 numpy arrays.
        """
        if not entity_ids:
            return

        vecs = np.array(
            [np.frombuffer(v, dtype=np.float32) for v in vectors_bytes],
            dtype=np.float32,
        )
        # L2-normalize each row for cosine similarity via inner product.
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        vecs = vecs / norms

        self._entity_ids.extend(entity_ids)

        if self._index is not None:
            self._index.add(vecs)
        else:
            for vec in vecs:
                self._vectors.append(vec)

    def search(self, query_bytes: bytes, k: int = 10) -> list[tuple[str, float]]:
        """Find the k nearest neighbors.

        Args:
            query_bytes: Serialized float32 query vector.
            k: Number of results to return.

        Returns:
            List of (entity_id, similarity_score) tuples, highest first.
        """
        if self.size == 0:
            return []

        query = np.frombuffer(query_bytes, dtype=np.float32).copy()
        norm = np.linalg.norm(query)
        if norm > 0:
            query = query / norm

        k = min(k, self.size)

        if self._index is not None:
            scores, indices = self._index.search(query.reshape(1, -1), k)
            results: list[tuple[str, float]] = []
            for score, idx in zip(scores[0], indices[0]):
                if idx >= 0:
                    results.append((self._entity_ids[idx], float(score)))
            return results
        else:
            return self._brute_force_search(query, k)

    def _brute_force_search(
        self, query: np.ndarray, k: int
    ) -> list[tuple[str, float]]:
        """Fallback brute-force inner-product search."""
        mat = np.array(self._vectors, dtype=np.float32)
        scores = mat @ query
        top_k = np.argsort(scores)[::-1][:k]
        return [(self._entity_ids[i], float(scores[i])) for i in top_k]
