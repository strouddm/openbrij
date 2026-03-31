"""CSV local file connector."""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from brij.connectors.base import (
    AuthenticationError,
    BaseConnector,
    EntityNotFoundError,
    SyncResult,
)
from brij.core.models import Entity, Signal

logger = logging.getLogger(__name__)

# Number of rows sampled for column type inference.
_TYPE_SAMPLE_ROWS = 100


def _infer_column_type(values: list[str]) -> str:
    """Infer a column's data type from a sample of its values.

    Returns one of: integer, float, boolean, date, text.
    """
    non_empty = [v for v in values if v.strip()]
    if not non_empty:
        return "text"

    # Try integer
    if all(_is_int(v) for v in non_empty):
        return "integer"

    # Try float
    if all(_is_float(v) for v in non_empty):
        return "float"

    # Try boolean
    if all(v.strip().lower() in ("true", "false", "yes", "no", "1", "0") for v in non_empty):
        return "boolean"

    return "text"


def _is_int(value: str) -> bool:
    try:
        int(value.strip())
        return True
    except ValueError:
        return False


def _is_float(value: str) -> bool:
    try:
        float(value.strip())
        return True
    except ValueError:
        return False


class CsvLocalConnector(BaseConnector):
    """Connector for local CSV files."""

    def __init__(self) -> None:
        self._path: Path | None = None
        self._source_id: str = ""
        self._last_modified: datetime | None = None

    def authenticate(self, credentials: dict) -> None:
        """Validate that the CSV file exists.

        Args:
            credentials: Must contain ``{"path": "/path/to/file.csv"}``.

        Raises:
            AuthenticationError: If path is missing or the file does not exist.
        """
        raw_path = credentials.get("path")
        if not raw_path:
            raise AuthenticationError("credentials must include 'path'")

        path = Path(raw_path)
        if not path.is_file():
            raise AuthenticationError(f"File not found: {path}")

        self._path = path
        self._source_id = f"csv:{path.name}"
        logger.info("Authenticated CSV file: %s", path)

    def discover(self) -> list[Entity]:
        """Read the CSV header and return collection + field entities.

        Returns:
            A list containing one collection entity for the file and one
            field entity per column.
        """
        if self._path is None:
            raise AuthenticationError("authenticate() must be called before discover()")

        stat = self._path.stat()
        modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        self._last_modified = modified_at

        with open(self._path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            try:
                headers = next(reader)
            except StopIteration:
                headers = []

            # Count rows and sample values for type inference.
            row_count = 0
            samples: list[list[str]] = [[] for _ in headers]
            for row in reader:
                row_count += 1
                if row_count <= _TYPE_SAMPLE_ROWS:
                    for i, cell in enumerate(row):
                        if i < len(headers):
                            samples[i].append(cell)

        # -- Collection entity --
        collection_id = self.make_entity_id("collection", self._path.name)
        collection = Entity(
            id=collection_id,
            type="collection",
            source_id=self._source_id,
            signals=[
                Signal(kind="name", value=self._path.name),
                Signal(kind="type", value="csv"),
                Signal(kind="location", value=str(self._path.resolve())),
                Signal(kind="modified", value=modified_at.isoformat()),
                Signal(kind="row_count", value=str(row_count)),
            ],
        )

        # -- Field entities (one per column) --
        entities: list[Entity] = [collection]
        for idx, header in enumerate(headers):
            col_type = _infer_column_type(samples[idx] if idx < len(samples) else [])
            field_id = self.make_entity_id("field", f"{self._path.name}:{header}")
            entities.append(
                Entity(
                    id=field_id,
                    type="field",
                    source_id=self._source_id,
                    parent_id=collection_id,
                    signals=[
                        Signal(kind="name", value=header),
                        Signal(kind="type", value=col_type),
                    ],
                )
            )

        logger.info(
            "Discovered %d columns and %d rows in %s",
            len(headers),
            row_count,
            self._path.name,
        )
        return entities

    def read(self, entity_id: str) -> list[Entity]:
        """Read entities for a given entity ID.

        For a collection entity: returns one record entity per row, each with
        ``field:{column_name}`` signals for every cell value.

        For a record entity: returns a single-element list with that record's
        field value signals.

        Args:
            entity_id: ID of the collection or record entity.

        Returns:
            List of record entities with field value signals.

        Raises:
            AuthenticationError: If authenticate() has not been called.
            EntityNotFoundError: If the entity_id is not recognised.
        """
        if self._path is None:
            raise AuthenticationError("authenticate() must be called before read()")

        collection_id = self.make_entity_id("collection", self._path.name)

        if entity_id == collection_id:
            return self._read_all_rows()

        if entity_id.startswith("record:"):
            return self._read_single_row(entity_id)

        raise EntityNotFoundError(f"Unknown entity: {entity_id}")

    def _read_all_rows(self) -> list[Entity]:
        """Read every row in the CSV and return record entities."""
        assert self._path is not None  # noqa: S101
        entities: list[Entity] = []
        with open(self._path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row_idx, row in enumerate(reader):
                record_id = self.make_entity_id(
                    "record", f"{self._path.name}:{row_idx}"
                )
                collection_id = self.make_entity_id("collection", self._path.name)
                signals = [
                    Signal(kind=f"field:{col}", value=val)
                    for col, val in row.items()
                ]
                entities.append(
                    Entity(
                        id=record_id,
                        type="record",
                        source_id=self._source_id,
                        parent_id=collection_id,
                        signals=signals,
                    )
                )
        logger.info("Read %d rows from %s", len(entities), self._path.name)
        return entities

    def _read_single_row(self, entity_id: str) -> list[Entity]:
        """Read a single row by its record entity ID."""
        assert self._path is not None  # noqa: S101
        # Parse the row index from the entity_id: "record:<filename>:<row_idx>"
        suffix = entity_id[len("record:"):]
        expected_prefix = f"{self._path.name}:"
        if not suffix.startswith(expected_prefix):
            raise EntityNotFoundError(f"Unknown entity: {entity_id}")

        try:
            row_idx = int(suffix[len(expected_prefix):])
        except ValueError:
            raise EntityNotFoundError(f"Unknown entity: {entity_id}")

        with open(self._path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for current_idx, row in enumerate(reader):
                if current_idx == row_idx:
                    collection_id = self.make_entity_id("collection", self._path.name)
                    signals = [
                        Signal(kind=f"field:{col}", value=val)
                        for col, val in row.items()
                    ]
                    return [
                        Entity(
                            id=entity_id,
                            type="record",
                            source_id=self._source_id,
                            parent_id=collection_id,
                            signals=signals,
                        )
                    ]

        raise EntityNotFoundError(f"Row not found for entity: {entity_id}")

    def write(self, entity_id: str, data: dict) -> bool:
        """Write data to an entity (not yet implemented)."""
        raise NotImplementedError("write() will be implemented in a future issue")

    def sync(self) -> SyncResult:
        """Compare file modification time against stored timestamp.

        Checks whether the CSV file has been modified since the last discover
        or sync by comparing the current file mtime against the ``modified``
        signal stored on the collection entity.

        Returns:
            SyncResult with the collection ID in ``modified`` if the file changed,
            or an empty SyncResult if nothing changed.

        Raises:
            AuthenticationError: If authenticate() has not been called.
        """
        if self._path is None:
            raise AuthenticationError("authenticate() must be called before sync()")

        collection_id = self.make_entity_id("collection", self._path.name)
        current_mtime = datetime.fromtimestamp(
            self._path.stat().st_mtime, tz=timezone.utc
        )

        # If no baseline yet, capture it via discover.
        if self._last_modified is None:
            self.discover()

        if current_mtime > self._last_modified:  # type: ignore[operator]
            self._last_modified = current_mtime
            logger.info("CSV file modified: %s", self._path.name)
            return SyncResult(modified=[collection_id])

        return SyncResult()
