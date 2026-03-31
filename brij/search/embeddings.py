"""Embedding generation for semantic search."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np
from sentence_transformers import SentenceTransformer

if TYPE_CHECKING:
    from brij.core.models import Entity
    from brij.core.store import Store

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "all-MiniLM-L6-v2"


class EmbeddingEngine:
    """Generates and stores embeddings using sentence-transformers."""

    def __init__(self, model_name: str = DEFAULT_MODEL) -> None:
        self._model_name = model_name
        self._model = SentenceTransformer(model_name)
        logger.info("Loaded embedding model: %s", model_name)

    @property
    def model_name(self) -> str:
        """Return the name of the loaded model."""
        return self._model_name

    def embed(self, text: str) -> bytes:
        """Embed a single text string, returning serialized numpy array bytes."""
        vector = self._model.encode(text, convert_to_numpy=True)
        return vector.astype(np.float32).tobytes()

    def embed_batch(self, texts: list[str]) -> list[bytes]:
        """Embed multiple texts, returning a list of serialized numpy array bytes."""
        vectors = self._model.encode(texts, convert_to_numpy=True)
        return [v.astype(np.float32).tobytes() for v in vectors]

    def embed_entity(self, entity: Entity, store: Store) -> None:
        """Generate an embedding for an entity's key signals and store it.

        Concatenates name, summary, preview, and field values into a single
        text representation, then embeds and persists via store.put_embedding().
        """
        parts: list[str] = []

        name = entity.get_signal_value("name")
        if name:
            parts.append(name)

        summary = entity.get_signal_value("summary")
        if summary:
            parts.append(summary)

        preview = entity.get_signal_value("preview")
        if preview:
            parts.append(preview)

        for signal in entity.signals:
            if signal.kind.startswith("field:"):
                parts.append(signal.value)

        if not parts:
            logger.debug("Entity %s has no embeddable signals, skipping", entity.id)
            return

        text = " ".join(parts)
        vector = self.embed(text)
        store.put_embedding(entity.id, vector, self._model_name)
        logger.debug("Stored embedding for entity %s", entity.id)
