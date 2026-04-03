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
    WriteError,
)
from brij.core.models import Entity, Signal

logger = logging.getLogger(__name__)

DEFAULT_CREDENTIALS_PATH = Path.home() / ".brij" / "google-credentials.json"
TOKEN_PATH = Path.home() / ".brij" / "google-sheets-token.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

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


def _find_header_row(rows: list[list[str]]) -> tuple[list[str], list[list[str]]]:
    """Locate the header row by skipping leading empty rows.

    Many shared Google Sheets have a blank or title-only first row
    (banners, merged cells, etc.).  This scans forward until it finds
    a row with at least two non-empty cells — that row is treated as
    the header.  Everything after it is data.

    Returns:
        (headers, data_rows) where *headers* may be empty if no
        suitable row is found.
    """
    for idx, row in enumerate(rows):
        non_empty = [c for c in row if c.strip()]
        if len(non_empty) >= 2:
            return row, rows[idx + 1:]
    return [], rows


def _dedupe_headers(headers: list[str]) -> list[str]:
    """Make duplicate or empty column headers unique.

    Google Sheets allows duplicate column names and blank headers.
    This appends ``_2``, ``_3``, … to repeats so every header maps
    to a distinct ``field:{name}`` signal kind.
    """
    seen: dict[str, int] = {}
    result: list[str] = []
    for h in headers:
        name = h.strip() if h.strip() else "unnamed"
        count = seen.get(name, 0) + 1
        seen[name] = count
        result.append(name if count == 1 else f"{name}_{count}")
    return result


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

    def list_spreadsheets(self) -> list[dict]:
        """Return available spreadsheets without discovering their contents.

        Each dict has ``id``, ``name``, and ``modifiedTime`` keys.

        Raises:
            AuthenticationError: If authenticate() has not been called.
        """
        if self._service is None:
            raise AuthenticationError(
                "authenticate() must be called before list_spreadsheets()"
            )

        self._ensure_drive_service()
        return self._list_spreadsheets(self._drive_service)

    def _ensure_drive_service(self) -> None:
        """Build the Drive service if it hasn't been created yet."""
        if self._drive_service is None:
            try:
                self._drive_service = build("drive", "v3", credentials=self._creds)
            except Exception:
                self._drive_service = None

    def discover(self, spreadsheet_id: str | None = None) -> list[Entity]:
        """Discover spreadsheet entities.

        When *spreadsheet_id* is provided, only that spreadsheet is
        discovered.  Otherwise all accessible spreadsheets are discovered.

        Args:
            spreadsheet_id: Optional ID of a single spreadsheet to discover.

        Returns:
            List of collection and field entities.

        Raises:
            AuthenticationError: If authenticate() has not been called.
        """
        if self._service is None:
            raise AuthenticationError("authenticate() must be called before discover()")

        self._ensure_drive_service()

        if spreadsheet_id is not None:
            spreadsheets = [{"id": spreadsheet_id, "name": "", "modifiedTime": ""}]
        else:
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

                headers, data_rows = _find_header_row(rows)

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

            raw_headers, data_rows = _find_header_row(rows)
            headers = _dedupe_headers(raw_headers)
            logger.debug(
                "Tab '%s': %d headers %s, %d data rows",
                tab_title,
                len(headers),
                headers,
                len(data_rows),
            )

            for row_idx, row in enumerate(data_rows):
                record_id = self.make_entity_id(
                    "record", f"{spreadsheet_id}:{tab_title}:{row_idx}"
                )
                signals = []
                for col_idx, header in enumerate(headers):
                    raw = row[col_idx] if col_idx < len(row) else ""
                    signals.append(
                        Signal(kind=f"field:{header}", value=str(raw))
                    )

                logger.debug(
                    "Record %s:%d — %d signals",
                    tab_title,
                    row_idx,
                    len(signals),
                )
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
        """Write data to a Google Sheets spreadsheet.

        The ``data`` dict must include an ``"action"`` key set to one of
        ``"add"``, ``"update"``, or ``"delete"``.  For ``"add"`` and
        ``"update"``, a ``"fields"`` dict mapping column names to values
        is required.  ``"update"`` and ``"delete"`` operate on a record
        entity identified by ``entity_id``.

        Args:
            entity_id: Collection ID (for add) or record ID (for update/delete).
            data: Action descriptor with ``action`` and optional ``fields``.

        Returns:
            True if the write succeeded.

        Raises:
            AuthenticationError: If authenticate() has not been called.
            WriteError: If the action is unknown or the API call fails.
            EntityNotFoundError: If the target entity does not exist.
        """
        if self._service is None:
            raise AuthenticationError("authenticate() must be called before write()")

        action = data.get("action")
        if action == "add":
            return self._add_record(entity_id, data.get("fields", {}))
        elif action == "update":
            return self._update_record(entity_id, data.get("fields", {}))
        elif action == "delete":
            return self._delete_record(entity_id)
        else:
            raise WriteError(f"Unknown write action: {action}")

    def _parse_record_id(self, entity_id: str) -> tuple[str, str, int]:
        """Extract spreadsheet ID, tab name, and row index from a record entity ID.

        Args:
            entity_id: A record entity ID like ``record:ssid:Tab:3``.

        Returns:
            Tuple of (spreadsheet_id, tab_name, row_index).

        Raises:
            EntityNotFoundError: If the entity ID format is invalid.
        """
        if not entity_id.startswith("record:"):
            raise EntityNotFoundError(f"Not a record entity: {entity_id}")
        parts = entity_id[len("record:"):].rsplit(":", 2)
        if len(parts) != 3:
            raise EntityNotFoundError(f"Invalid record entity ID: {entity_id}")
        spreadsheet_id, tab_name, row_str = parts
        try:
            row_idx = int(row_str)
        except ValueError:
            raise EntityNotFoundError(f"Invalid row index in entity ID: {entity_id}")
        return spreadsheet_id, tab_name, row_idx

    def _add_record(self, entity_id: str, fields: dict) -> bool:
        """Append a new row to a spreadsheet tab."""
        if not entity_id.startswith("collection:"):
            raise EntityNotFoundError(f"Expected a collection entity: {entity_id}")
        spreadsheet_id = entity_id[len("collection:"):]

        tab = fields.pop("_tab", "Sheet1") if "_tab" in fields else "Sheet1"

        try:
            result = (
                self._service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=f"'{tab}'!1:1")
                .execute()
            )
            headers = result.get("values", [[]])[0]
        except Exception as exc:
            raise WriteError(f"Failed to read headers: {exc}") from exc

        if not headers:
            raise WriteError(f"No headers found in {tab}")

        row = [str(fields.get(h, "")) for h in headers]
        try:
            self._service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab}'!A1",
                valueInputOption="RAW",
                body={"values": [row]},
            ).execute()
        except Exception as exc:
            raise WriteError(f"Failed to append row: {exc}") from exc

        logger.info("Added record to spreadsheet %s tab %s", spreadsheet_id, tab)
        return True

    def _update_record(self, entity_id: str, fields: dict) -> bool:
        """Update cells in a specific row."""
        spreadsheet_id, tab_name, row_idx = self._parse_record_id(entity_id)
        # Sheets rows are 1-indexed; row 0 is headers, so data row 0 is row 2.
        sheet_row = row_idx + 2

        try:
            result = (
                self._service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=f"'{tab_name}'!1:1")
                .execute()
            )
            headers = result.get("values", [[]])[0]
        except Exception as exc:
            raise WriteError(f"Failed to read headers: {exc}") from exc

        if not headers:
            raise WriteError(f"No headers found in {tab_name}")

        row = [str(fields.get(h, "")) for h in headers]
        range_str = f"'{tab_name}'!A{sheet_row}"
        try:
            self._service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_str,
                valueInputOption="RAW",
                body={"values": [row]},
            ).execute()
        except Exception as exc:
            raise WriteError(f"Failed to update row: {exc}") from exc

        logger.info("Updated record %s in spreadsheet %s", entity_id, spreadsheet_id)
        return True

    def _delete_record(self, entity_id: str) -> bool:
        """Delete a row from a spreadsheet tab."""
        spreadsheet_id, tab_name, row_idx = self._parse_record_id(entity_id)
        # Sheets rows are 1-indexed; row 0 is headers, so data row 0 is row 2.
        sheet_row = row_idx + 2

        try:
            sheet_meta = (
                self._service.spreadsheets()
                .get(spreadsheetId=spreadsheet_id)
                .execute()
            )
        except Exception as exc:
            raise WriteError(f"Failed to fetch spreadsheet metadata: {exc}") from exc

        sheet_id = None
        for sheet in sheet_meta.get("sheets", []):
            if sheet.get("properties", {}).get("title") == tab_name:
                sheet_id = sheet["properties"]["sheetId"]
                break

        if sheet_id is None:
            raise EntityNotFoundError(f"Tab not found: {tab_name}")

        request_body = {
            "requests": [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": sheet_row - 1,
                            "endIndex": sheet_row,
                        }
                    }
                }
            ]
        }
        try:
            self._service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body,
            ).execute()
        except Exception as exc:
            raise WriteError(f"Failed to delete row: {exc}") from exc

        logger.info("Deleted record %s from spreadsheet %s", entity_id, spreadsheet_id)
        return True

    def create_collection(self, name: str, schema: dict) -> Entity:
        """Create a new Google Sheets spreadsheet.

        Args:
            name: Title for the new spreadsheet.
            schema: Must contain ``"fields"`` — a list of column name strings.

        Returns:
            The created collection entity.

        Raises:
            AuthenticationError: If authenticate() has not been called.
            WriteError: If fields are empty or the API call fails.
        """
        if self._service is None:
            raise AuthenticationError(
                "authenticate() must be called before create_collection()"
            )

        fields = schema.get("fields", [])
        if not fields:
            raise WriteError("schema must include a non-empty 'fields' list")

        spreadsheet_body = {
            "properties": {"title": name},
            "sheets": [{"properties": {"title": "Sheet1"}}],
        }
        try:
            spreadsheet = (
                self._service.spreadsheets()
                .create(body=spreadsheet_body)
                .execute()
            )
        except Exception as exc:
            raise WriteError(f"Failed to create spreadsheet: {exc}") from exc

        spreadsheet_id = spreadsheet["spreadsheetId"]

        try:
            self._service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range="'Sheet1'!A1",
                valueInputOption="RAW",
                body={"values": [fields]},
            ).execute()
        except Exception as exc:
            raise WriteError(f"Failed to write headers: {exc}") from exc

        collection_id = self.make_entity_id("collection", spreadsheet_id)
        collection = Entity(
            id=collection_id,
            type="collection",
            source_id=self._source_id,
            signals=[
                Signal(kind="name", value=name),
                Signal(kind="type", value="google_sheets"),
                Signal(kind="spreadsheet_id", value=spreadsheet_id),
            ],
        )

        logger.info("Created spreadsheet %s (%s)", name, spreadsheet_id)
        return collection

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
