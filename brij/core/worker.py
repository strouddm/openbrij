"""Background worker for Tier 2/3 indexing."""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

from brij.connectors.base import BaseConnector, SyncResult
from brij.core.store import Store

logger = logging.getLogger(__name__)

# Default minimum seconds between connector.read() calls for rate limiting.
DEFAULT_RATE_LIMIT_DELAY = 0.5


class IndexingWorker:
    """Processes Tier 2/3 content extraction in a background thread.

    After ``brij connect`` stores Tier 1 metadata, the worker reads
    full records from each collection and stores them.  Progress is
    tracked in the ``indexing_tasks`` table so ``brij status`` can
    report it.

    Supports incremental mode: when a ``sync_result`` is provided,
    only new and modified collections are re-indexed and deleted
    entities are removed.
    """

    def __init__(
        self,
        db_path: str | Path,
        connector: BaseConnector,
        source_id: str,
        task_id: int,
        *,
        rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY,
        sync_result: SyncResult | None = None,
    ) -> None:
        self._db_path = db_path
        self._connector = connector
        self._source_id = source_id
        self._task_id = task_id
        self._rate_limit_delay = rate_limit_delay
        self._sync_result = sync_result
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Launch the background indexing thread."""
        self._thread = threading.Thread(
            target=self._run,
            name=f"brij-indexer-{self._source_id}",
            daemon=True,
        )
        self._thread.start()
        logger.debug("Background indexer started for source %s", self._source_id)

    def join(self, timeout: float | None = None) -> None:
        """Wait for the background thread to finish."""
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    @property
    def is_alive(self) -> bool:
        """Return True if the background thread is still running."""
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        """Main worker loop — process collections for this source."""
        store = Store(self._db_path)
        try:
            if self._sync_result is not None:
                self._process_incremental(store, self._sync_result)
            else:
                self._process(store)
        except Exception as exc:
            logger.error("Background indexing failed for %s: %s", self._source_id, exc)
            store.update_indexing_task(self._task_id, status="failed", error=str(exc))
        finally:
            # Persist connector sync state after indexing.
            state = self._connector.get_sync_state()
            if state:
                store.put_sync_state(self._source_id, state)
            store.close()

    def _process(self, store: Store) -> None:
        """Read records from each collection and store them."""
        store.update_indexing_task(self._task_id, status="running")

        collections = [
            e for e in store.get_entities_by_source(self._source_id)
            if e.type == "collection"
        ]

        store.update_indexing_task(
            self._task_id, total_collections=len(collections),
        )

        records_stored = 0
        collections_indexed = 0

        for collection in collections:
            try:
                records = self._connector.read(collection.id)
                for record in records:
                    store.put_entity(record)
                    records_stored += 1

                collections_indexed += 1
                store.update_indexing_task(
                    self._task_id,
                    collections_indexed=collections_indexed,
                    records_stored=records_stored,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to index collection %s: %s", collection.id, exc,
                )
                # Continue with other collections rather than aborting.
                collections_indexed += 1
                store.update_indexing_task(
                    self._task_id,
                    collections_indexed=collections_indexed,
                )

            # Respect rate limits between API calls.
            if self._rate_limit_delay > 0:
                time.sleep(self._rate_limit_delay)

        store.update_indexing_task(self._task_id, status="completed")
        store.update_source_synced(self._source_id)
        logger.info(
            "Background indexing complete for %s: %d collections, %d records",
            self._source_id,
            collections_indexed,
            records_stored,
        )

    def _process_incremental(self, store: Store, sync_result: SyncResult) -> None:
        """Re-index only new/modified collections and remove deleted ones."""
        store.update_indexing_task(self._task_id, status="running")

        changed_ids = set(sync_result.new + sync_result.modified)

        # Remove deleted entities and their children/embeddings.
        if sync_result.deleted:
            self._remove_deleted(store, sync_result.deleted)

        # Filter to only collections that changed.
        collections = [
            e for e in store.get_entities_by_source(self._source_id)
            if e.type == "collection" and e.id in changed_ids
        ]

        store.update_indexing_task(
            self._task_id, total_collections=len(collections),
        )

        records_stored = 0
        collections_indexed = 0

        for collection in collections:
            try:
                # Remove old records for this collection before re-reading.
                old_children = store.get_children(collection.id)
                old_child_ids = [c.id for c in old_children if c.type == "record"]
                if old_child_ids:
                    store.delete_entities_for_ids(old_child_ids)

                records = self._connector.read(collection.id)
                for record in records:
                    store.put_entity(record)
                    records_stored += 1

                collections_indexed += 1
                store.update_indexing_task(
                    self._task_id,
                    collections_indexed=collections_indexed,
                    records_stored=records_stored,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to re-index collection %s: %s", collection.id, exc,
                )
                collections_indexed += 1
                store.update_indexing_task(
                    self._task_id,
                    collections_indexed=collections_indexed,
                )

            if self._rate_limit_delay > 0:
                time.sleep(self._rate_limit_delay)

        store.update_indexing_task(self._task_id, status="completed")
        store.update_source_synced(self._source_id)
        logger.info(
            "Incremental indexing complete for %s: "
            "%d new, %d modified, %d deleted, %d records stored",
            self._source_id,
            len(sync_result.new),
            len(sync_result.modified),
            len(sync_result.deleted),
            records_stored,
        )

    def _remove_deleted(self, store: Store, deleted_ids: list[str]) -> None:
        """Remove deleted collections and all their children."""
        for collection_id in deleted_ids:
            children = store.get_children(collection_id)
            child_ids = [c.id for c in children]
            if child_ids:
                store.delete_entities_for_ids(child_ids)
            store.delete_entities_for_ids([collection_id])
            logger.debug("Removed deleted collection %s and %d children",
                         collection_id, len(child_ids))
