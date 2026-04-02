"""Google Sheets connector with OAuth authentication."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from brij.connectors.base import (
    AuthenticationError,
    BaseConnector,
    EntityNotFoundError,
    SyncResult,
)
from brij.core.models import Entity, Signal

logger = logging.getLogger(__name__)

DEFAULT_CREDENTIALS_PATH = Path.home() / ".brij" / "google-credentials.json"
TOKEN_PATH = Path.home() / ".brij" / "google-sheets-token.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Number of rows sampled per tab for column type inference.
_TYPE_SAMPLE_ROWS = 100


def _infer_column_type(values: list[str]) -> str:
    """Infer a column's data type from a sample of its values.

    Returns one of: integer, float, boolean, text.
    """
    non_empty = [v for v in values if v.strip()]
    if not non_empty:
        return "text"

    if all(_is_int(v) for v in non_empty):
        return "integer"

    if all(_is_float(v) for v in non_empty):
        return "float"

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


class GoogleSheetsConnector(BaseConnector):
    """Connector for Google Sheets via the Sheets API with OAuth."""

    def __init__(self) -> None:
        self._service = None
        self._drive_service = None
        self._creds = None
        self._source_id: str = ""
        self._credentials_path: Path = DEFAULT_CREDENTIALS_PATH
        self._token_path: Path = TOKEN_PATH
        self._spreadsheets: list[dict] | None = None
        self._last_modified: dict[str, datetime] = {}

    def authenticate(self, credentials: dict) -> None:
        """Authenticate with Google Sheets API via OAuth.

        Args:
            credentials: Optional dict with ``credentials_path`` and/or
                ``token_path`` overrides. If omitted, defaults are used.

        Raises:
            AuthenticationError: If credentials file is missing or auth fails.
        """
        if credentials.get("credentials_path"):
            self._credentials_path = Path(credentials["credentials_path"])
        if credentials.get("token_path"):
            self._token_path = Path(credentials["token_path"])

        if not self._credentials_path.is_file():
            raise AuthenticationError(
                f"Google credentials file not found: {self._credentials_path}"
            )

        creds = None
        if self._token_path.is_file():
            creds = Credentials.from_authorized_user_file(str(self._token_path), SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as exc:
                    raise AuthenticationError(f"Token refresh failed: {exc}") from exc
            else:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        str(self._credentials_path), SCOPES
                    )
                    creds = flow.run_local_server(port=0)
                except Exception as exc:
                    raise AuthenticationError(f"OAuth flow failed: {exc}") from exc

            self._token_path.parent.mkdir(parents=True, exist_ok=True)
            self._token_path.write_text(creds.to_json())

        self._creds = creds
        try:
            self._service = build("sheets", "v4", credentials=creds)
        except Exception as exc:
            raise AuthenticationError(f"Failed to build Sheets service: {exc}") from exc

        self._source_id = "google_sheets:user"
        logger.info("Authenticated with Google Sheets API")

    def discover(self) -> list[Entity]:
        """List all spreadsheets accessible to the user.

        For each spreadsheet, creates a collection entity with sheet name,
        tab names, column headers per tab, and last modified date.
        Field entities are created for each column with name and inferred type.

        Returns:
            List of collection and field entities.

        Raises:
            AuthenticationError: If authenticate() has not been called.
        """
        if self._service is None:
            raise AuthenticationError("authenticate() must be called before discover()")

        try:
            self._drive_service = build("drive", "v3", credentials=self._creds)
        except Exception:
            self._drive_service = None

        spreadsheets = self._list_spreadsheets(self._drive_service)
        self._spreadsheets = spreadsheets

        entities: list[Entity] = []
        for spreadsheet in spreadsheets:
            spreadsheet_id = spreadsheet["id"]
            spreadsheet_name = spreadsheet["name"]
            modified_time = spreadsheet.get("modifiedTime", "")

            try:
                sheet_meta = (
                    self._service.spreadsheets()
                    .get(spreadsheetId=spreadsheet_id)
                    .execute()
                )
            except Exception:
                logger.warning("Failed to fetch metadata for %s", spreadsheet_name)
                continue

            sheets = sheet_meta.get("sheets", [])
            tab_names = [
                s.get("properties", {}).get("title", "") for s in sheets
            ]

            if modified_time:
                self._last_modified[spreadsheet_id] = datetime.fromisoformat(
                    modified_time.replace("Z", "+00:00")
                )

            collection_id = self.make_entity_id("collection", spreadsheet_id)
            collection_signals = [
                Signal(kind="name", value=spreadsheet_name),
                Signal(kind="type", value="google_sheets"),
                Signal(kind="spreadsheet_id", value=spreadsheet_id),
                Signal(kind="modified", value=modified_time),
                Signal(kind="tab_names", value=json.dumps(tab_names)),
            ]

            collection = Entity(
                id=collection_id,
                type="collection",
                source_id=self._source_id,
                signals=collection_signals,
            )
            entities.append(collection)

            for sheet in sheets:
                tab_title = sheet.get("properties", {}).get("title", "")
                range_name = f"'{tab_title}'!1:{_TYPE_SAMPLE_ROWS + 1}"

                try:
                    result = (
                        self._service.spreadsheets()
                        .values()
                        .get(spreadsheetId=spreadsheet_id, range=range_name)
                        .execute()
                    )
                    rows = result.get("values", [])
                except Exception:
                    logger.warning(
                        "Failed to read headers from %s/%s", spreadsheet_name, tab_title
                    )
                    continue

                if not rows:
                    continue

                headers = rows[0]
                data_rows = rows[1:]

                for col_idx, header in enumerate(headers):
                    if not header.strip():
                        continue
                    samples = [
                        row[col_idx]
                        for row in data_rows[:_TYPE_SAMPLE_ROWS]
                        if col_idx < len(row) and row[col_idx].strip()
                    ]
                    col_type = _infer_column_type(samples)
                    field_id = self.make_entity_id(
                        "field", f"{spreadsheet_id}:{tab_title}:{header}"
                    )
                    entities.append(
                        Entity(
                            id=field_id,
                            type="field",
                            source_id=self._source_id,
                            parent_id=collection_id,
                            signals=[
                                Signal(kind="name", value=header),
                                Signal(kind="type", value=col_type),
                                Signal(kind="tab", value=tab_title),
                            ],
                        )
                    )

        logger.info("Discovered %d entities from Google Sheets", len(entities))
        return entities

    def _list_spreadsheets(self, drive_service) -> list[dict]:
        """List spreadsheets accessible to the user via the Drive API.

        Args:
            drive_service: Google Drive API service object.

        Returns:
            List of dicts with id, name, and modifiedTime.
        """
        if drive_service is None:
            return []

        try:
            results = (
                drive_service.files()
                .list(
                    q="mimeType='application/vnd.google-apps.spreadsheet'",
                    fields="files(id, name, modifiedTime)",
                    pageSize=100,
                )
                .execute()
            )
            return results.get("files", [])
        except Exception:
            logger.warning("Failed to list spreadsheets via Drive API")
            return []

    def read(self, entity_id: str) -> list[Entity]:
        """Read record entities for a collection.

        For a collection entity: reads all rows from all tabs. Each row
        becomes a record entity with ``field:{column_name}`` signals for
        every cell value.

        Args:
            entity_id: The ID of the collection entity to read.

        Returns:
            List of record entities with field value signals.

        Raises:
            AuthenticationError: If authenticate() has not been called.
            EntityNotFoundError: If the entity_id is not a known collection.
        """
        if self._service is None:
            raise AuthenticationError("authenticate() must be called before read()")

        if not entity_id.startswith("collection:"):
            raise EntityNotFoundError(f"Unknown entity: {entity_id}")

        spreadsheet_id = entity_id[len("collection:"):]

        try:
            sheet_meta = (
                self._service.spreadsheets()
                .get(spreadsheetId=spreadsheet_id)
                .execute()
            )
        except Exception as exc:
            raise EntityNotFoundError(
                f"Failed to fetch spreadsheet {spreadsheet_id}: {exc}"
            ) from exc

        sheets = sheet_meta.get("sheets", [])
        entities: list[Entity] = []

        for sheet in sheets:
            tab_title = sheet.get("properties", {}).get("title", "")
            range_name = f"'{tab_title}'"

            try:
                result = (
                    self._service.spreadsheets()
                    .values()
                    .get(spreadsheetId=spreadsheet_id, range=range_name)
                    .execute()
                )
                rows = result.get("values", [])
            except Exception:
                logger.warning(
                    "Failed to read data from %s/%s", spreadsheet_id, tab_title
                )
                continue

            if not rows:
                continue

            headers = rows[0]
            data_rows = rows[1:]

            for row_idx, row in enumerate(data_rows):
                record_id = self.make_entity_id(
                    "record", f"{spreadsheet_id}:{tab_title}:{row_idx}"
                )
                signals = []
                for col_idx, header in enumerate(headers):
                    value = row[col_idx] if col_idx < len(row) else ""
                    signals.append(Signal(kind=f"field:{header}", value=value))

                entities.append(
                    Entity(
                        id=record_id,
                        type="record",
                        source_id=self._source_id,
                        parent_id=entity_id,
                        signals=signals,
                    )
                )

        logger.info("Read %d records from spreadsheet %s", len(entities), spreadsheet_id)
        return entities

    def write(self, entity_id: str, data: dict) -> bool:
        """Write data to Google Sheets (not yet implemented).

        Args:
            entity_id: The ID of the entity to write to.
            data: The data to write.

        Returns:
            True if the write succeeded.
        """
        if self._service is None:
            raise AuthenticationError("authenticate() must be called before write()")
        return False

    def sync(self) -> SyncResult:
        """Check for modified spreadsheets since last discover.

        Uses the Drive API modification timestamp to detect changes.

        Returns:
            SyncResult with modified spreadsheet collection IDs.

        Raises:
            AuthenticationError: If authenticate() has not been called.
        """
        if self._service is None:
            raise AuthenticationError("authenticate() must be called before sync()")

        if not self._last_modified:
            self.discover()

        spreadsheets = self._list_spreadsheets(self._drive_service)
        modified: list[str] = []

        for spreadsheet in spreadsheets:
            spreadsheet_id = spreadsheet["id"]
            modified_time_str = spreadsheet.get("modifiedTime", "")
            if not modified_time_str:
                continue

            current_mtime = datetime.fromisoformat(
                modified_time_str.replace("Z", "+00:00")
            )
            baseline = self._last_modified.get(spreadsheet_id)
            if baseline is not None and current_mtime > baseline:
                collection_id = self.make_entity_id("collection", spreadsheet_id)
                modified.append(collection_id)
                self._last_modified[spreadsheet_id] = current_mtime

        return SyncResult(modified=modified)
