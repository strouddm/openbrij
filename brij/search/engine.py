"""Search engine for querying stored entities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from brij.core.models import Entity

if TYPE_CHECKING:
    from brij.config import SearchConfig
    from brij.core.store import Store

logger = logging.getLogger(__name__)


class SearchEngine:
    """Wraps the store's keyword search to return full Entity objects.

    This is the keyword-only search path. Semantic search will be
    added in a future issue to produce hybrid results.
    """

    def __init__(self, store: Store, config: SearchConfig) -> None:
        self._store = store
        self._config = config

    def search(
        self,
        query: str,
        sources: list[str] | None = None,
        limit: int | None = None,
    ) -> list[Entity]:
        """Search for entities matching the query.

        Args:
            query: The search query string.
            sources: Optional list of source IDs to filter by.
                     When provided, results are restricted to these sources.
            limit: Maximum number of results to return.
                   Defaults to config.default_limit.

        Returns:
            List of Entity objects ranked by keyword relevance.
        """
        if limit is None:
            limit = self._config.default_limit

        if not query or not query.strip():
            return []

        if sources:
            # Run keyword search per source and merge by relevance.
            scored: dict[str, float] = {}
            for source_id in sources:
                hits = self._store.keyword_search(query, source_id=source_id, limit=limit)
                for entity_id, score in hits:
                    if entity_id not in scored or score > scored[entity_id]:
                        scored[entity_id] = score
            ranked = sorted(scored.items(), key=lambda item: item[1], reverse=True)[:limit]
        else:
            ranked = self._store.keyword_search(query, limit=limit)

        entities: list[Entity] = []
        for entity_id, _score in ranked:
            entity = self._store.get_entity(entity_id)
            if entity is not None:
                entities.append(entity)

        logger.debug("Search for %r returned %d entities", query, len(entities))
        return entities
