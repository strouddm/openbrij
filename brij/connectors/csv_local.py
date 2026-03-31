"""CSV local file connector — discover phase."""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from brij.connectors.base import (
    AuthenticationError,
    BaseConnector,
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

    # ----- Stubs for methods not yet implemented (Issue 8) -----

    def read(self, entity_id: str) -> list[Signal]:
        """Read signals for an entity (not yet implemented)."""
        raise NotImplementedError("read() will be implemented in Issue 8")

    def write(self, entity_id: str, data: dict) -> bool:
        """Write data to an entity (not yet implemented)."""
        raise NotImplementedError("write() will be implemented in a future issue")

    def sync(self) -> SyncResult:
        """Synchronize with the CSV file (not yet implemented)."""
        raise NotImplementedError("sync() will be implemented in Issue 8")
