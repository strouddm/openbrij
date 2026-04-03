"""Google Drive connector — metadata catalog with auto-indexed Sheets."""

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
from brij.connectors.google_sheets import (
    _dedupe_headers,
    _find_header_row,
    _infer_column_type,
)
from brij.core.models import Entity, Signal

logger = logging.getLogger(__name__)

DEFAULT_CREDENTIALS_PATH = Path.home() / ".brij" / "google-credentials.json"
TOKEN_PATH = Path.home() / ".brij" / "google-drive-token.json"

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# Page size for Drive API list requests.
_PAGE_SIZE = 100

# Fields requested for each file from the Drive API.
_FILE_FIELDS = (
    "id, name, mimeType, modifiedTime, createdTime, size, "
    "owners, parents, fullFileExtension, shared"
)

FOLDER_MIME = "application/vnd.google-apps.folder"
SHEETS_MIME = "application/vnd.google-apps.spreadsheet"

# Number of rows sampled per tab for column type inference.
_TYPE_SAMPLE_ROWS = 100

# Map of Google MIME types to friendly labels for status display.
_MIME_LABELS: dict[str, str] = {
    "application/vnd.google-apps.document": "docs",
    "application/vnd.google-apps.spreadsheet": "sheets",
    "application/vnd.google-apps.presentation": "slides",
    "application/vnd.google-apps.form": "forms",
    "application/pdf": "pdfs",
    FOLDER_MIME: "folders",
}


def _mime_label(mime_type: str) -> str:
    """Return a human-readable label for a MIME type."""
    return _MIME_LABELS.get(mime_type, mime_type.split("/")[-1])


class GoogleDriveConnector(BaseConnector):
    """Connector for Google Drive — metadata catalog with auto-indexed Sheets.

    Lists every file and folder in the user's Drive, emitting metadata
    signals for each.  When a Google Sheets file is encountered, it is
    automatically read using the Sheets API: every tab's columns become
    field entities (discover) and every row becomes a record entity (read).
    """

    def __init__(self) -> None:
        self._service = None
        self._sheets_service = None
        self._creds = None
        self._source_id: str = ""
        self._credentials_path: Path = DEFAULT_CREDENTIALS_PATH
        self._token_path: Path = TOKEN_PATH
        self._last_modified: dict[str, datetime] = {}
        self._folder_cache: dict[str, dict[str, str]] = {}

    def authenticate(self, credentials: dict) -> None:
        """Authenticate with Google Drive API via OAuth.

        Args:
            credentials: Optional dict with ``credentials_path`` and/or
                ``token_path`` overrides.  If omitted, defaults are used.

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
            self._service = build("drive", "v3", credentials=creds)
        except Exception as exc:
            raise AuthenticationError(f"Failed to build Drive service: {exc}") from exc

        try:
            self._sheets_service = build("sheets", "v4", credentials=creds)
        except Exception as exc:
            raise AuthenticationError(f"Failed to build Sheets service: {exc}") from exc

        self._source_id = "google_drive:user"
        logger.info("Authenticated with Google Drive API")

    def discover(self, folder_id: str | None = None) -> list[Entity]:
        """Discover file and folder entities in Google Drive.

        Lists all files and folders, emitting metadata signals for each.
        Handles pagination for large drives (1000+ files).

        Args:
            folder_id: Optional folder ID to scope discovery.  When provided,
                only files inside that folder are discovered.

        Returns:
            List of collection entities (one per file/folder).

        Raises:
            AuthenticationError: If authenticate() has not been called.
        """
        if self._service is None:
            raise AuthenticationError("authenticate() must be called before discover()")

        files = self._list_files(folder_id=folder_id)
        self._build_folder_cache(files)

        entities: list[Entity] = []
        for file_meta in files:
            file_id = file_meta["id"]
            name = file_meta.get("name", "")
            mime_type = file_meta.get("mimeType", "")
            modified_time = file_meta.get("modifiedTime", "")
            size = file_meta.get("size", "0")
            owners = file_meta.get("owners", [])
            owner_name = owners[0].get("displayName", "") if owners else ""
            shared = str(file_meta.get("shared", False)).lower()
            extension = file_meta.get("fullFileExtension", "")
            parents = file_meta.get("parents", [])
            parent_folder = parents[0] if parents else ""

            if modified_time:
                self._last_modified[file_id] = datetime.fromisoformat(
                    modified_time.replace("Z", "+00:00")
                )

            collection_id = self.make_entity_id("collection", file_id)

            folder_signals = self._folder_hierarchy_signals(parent_folder)

            if mime_type == SHEETS_MIME and self._sheets_service is not None:
                sheet_entities = self._discover_sheet(
                    file_id, name, modified_time, collection_id, folder_signals
                )
                entities.extend(sheet_entities)
            else:
                signals = [
                    Signal(kind="name", value=name),
                    Signal(kind="type", value="google_drive"),
                    Signal(kind="file_id", value=file_id),
                    Signal(kind="mime_type", value=mime_type),
                    Signal(kind="modified", value=modified_time),
                    Signal(kind="size", value=str(size)),
                    Signal(kind="owner", value=owner_name),
                    Signal(kind="shared", value=shared),
                    Signal(kind="file_extension", value=extension),
                    Signal(kind="parent_folder", value=parent_folder),
                    *folder_signals,
                ]

                entity = Entity(
                    id=collection_id,
                    type="collection",
                    source_id=self._source_id,
                    signals=signals,
                )
                entities.append(entity)

        logger.info("Discovered %d entities from Google Drive", len(entities))
        return entities

    def read(self, entity_id: str) -> list[Entity]:
        """Read metadata for a file or folder entity.

        For a folder: lists files inside it and returns record entities.
        For a file: returns a single record entity with its metadata signals.

        This is a metadata-only connector (Tier 1) — no file content is read.

        Args:
            entity_id: The ID of the collection entity to read.

        Returns:
            List of record entities with metadata signals.

        Raises:
            AuthenticationError: If authenticate() has not been called.
            EntityNotFoundError: If the entity_id is not a known collection.
        """
        if self._service is None:
            raise AuthenticationError("authenticate() must be called before read()")

        if not entity_id.startswith("collection:"):
            raise EntityNotFoundError(f"Unknown entity: {entity_id}")

        file_id = entity_id[len("collection:"):]

        try:
            file_meta = (
                self._service.files()
                .get(fileId=file_id, fields=_FILE_FIELDS)
                .execute()
            )
        except Exception as exc:
            raise EntityNotFoundError(
                f"Failed to fetch file {file_id}: {exc}"
            ) from exc

        mime_type = file_meta.get("mimeType", "")

        if mime_type == FOLDER_MIME:
            return self._read_folder(file_id, entity_id)
        if mime_type == SHEETS_MIME and self._sheets_service is not None:
            return self._read_sheet(file_id, entity_id)
        return self._read_file(file_meta, entity_id)

    def _read_folder(self, folder_id: str, parent_entity_id: str) -> list[Entity]:
        """List files in a folder and return record entities."""
        files = self._list_files(folder_id=folder_id)
        entities: list[Entity] = []

        for idx, file_meta in enumerate(files):
            record_id = self.make_entity_id("record", f"{folder_id}:{idx}")
            signals = self._metadata_signals(file_meta)
            entities.append(
                Entity(
                    id=record_id,
                    type="record",
                    source_id=self._source_id,
                    parent_id=parent_entity_id,
                    signals=signals,
                )
            )

        logger.info("Read %d records from folder %s", len(entities), folder_id)
        return entities

    def _read_file(self, file_meta: dict, parent_entity_id: str) -> list[Entity]:
        """Return a single record entity for a file's metadata."""
        file_id = file_meta["id"]
        record_id = self.make_entity_id("record", f"{file_id}:0")
        signals = self._metadata_signals(file_meta)
        entity = Entity(
            id=record_id,
            type="record",
            source_id=self._source_id,
            parent_id=parent_entity_id,
            signals=signals,
        )
        return [entity]

    def _discover_sheet(
        self,
        file_id: str,
        name: str,
        modified_time: str,
        collection_id: str,
        folder_signals: list[Signal] | None = None,
    ) -> list[Entity]:
        """Discover a Google Sheets file: collection + field entities per column."""
        try:
            sheet_meta = (
                self._sheets_service.spreadsheets()
                .get(spreadsheetId=file_id)
                .execute()
            )
        except Exception:
            logger.warning("Failed to fetch Sheets metadata for %s, skipping auto-index", name)
            return []

        sheets = sheet_meta.get("sheets", [])
        tab_names = [s.get("properties", {}).get("title", "") for s in sheets]

        collection_signals = [
            Signal(kind="name", value=name),
            Signal(kind="type", value="google_sheets"),
            Signal(kind="spreadsheet_id", value=file_id),
            Signal(kind="modified", value=modified_time),
            Signal(kind="tab_names", value=json.dumps(tab_names)),
            *(folder_signals or []),
        ]

        entities: list[Entity] = [
            Entity(
                id=collection_id,
                type="collection",
                source_id=self._source_id,
                signals=collection_signals,
            )
        ]

        for sheet in sheets:
            tab_title = sheet.get("properties", {}).get("title", "")
            range_name = f"'{tab_title}'!1:{_TYPE_SAMPLE_ROWS + 1}"

            try:
                result = (
                    self._sheets_service.spreadsheets()
                    .values()
                    .get(spreadsheetId=file_id, range=range_name)
                    .execute()
                )
                rows = result.get("values", [])
            except Exception:
                logger.warning("Failed to read headers from %s/%s", name, tab_title)
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
                    "field", f"{file_id}:{tab_title}:{header}"
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

        return entities

    def _read_sheet(self, file_id: str, parent_entity_id: str) -> list[Entity]:
        """Read all rows from a Google Sheets file, returning record entities."""
        try:
            sheet_meta = (
                self._sheets_service.spreadsheets()
                .get(spreadsheetId=file_id)
                .execute()
            )
        except Exception as exc:
            raise EntityNotFoundError(
                f"Failed to fetch spreadsheet {file_id}: {exc}"
            ) from exc

        sheets = sheet_meta.get("sheets", [])
        entities: list[Entity] = []

        for sheet in sheets:
            tab_title = sheet.get("properties", {}).get("title", "")
            range_name = f"'{tab_title}'"

            try:
                result = (
                    self._sheets_service.spreadsheets()
                    .values()
                    .get(spreadsheetId=file_id, range=range_name)
                    .execute()
                )
                rows = result.get("values", [])
            except Exception:
                logger.warning("Failed to read data from %s/%s", file_id, tab_title)
                continue

            if not rows:
                continue

            raw_headers, data_rows = _find_header_row(rows)
            headers = _dedupe_headers(raw_headers)

            for row_idx, row in enumerate(data_rows):
                record_id = self.make_entity_id(
                    "record", f"{file_id}:{tab_title}:{row_idx}"
                )
                signals = []
                for col_idx, header in enumerate(headers):
                    raw = row[col_idx] if col_idx < len(row) else ""
                    signals.append(Signal(kind=f"field:{header}", value=str(raw)))

                entities.append(
                    Entity(
                        id=record_id,
                        type="record",
                        source_id=self._source_id,
                        parent_id=parent_entity_id,
                        signals=signals,
                    )
                )

        logger.info("Read %d records from spreadsheet %s", len(entities), file_id)
        return entities

    def _build_folder_cache(self, files: list[dict]) -> None:
        """Index folder IDs to names and parents from a file listing."""
        for f in files:
            if f.get("mimeType") == FOLDER_MIME:
                parents = f.get("parents", [])
                self._folder_cache[f["id"]] = {
                    "name": f.get("name", ""),
                    "parent": parents[0] if parents else "",
                }

    def _resolve_folder_name(self, folder_id: str) -> str:
        """Return the name for a folder ID, fetching from API if needed."""
        if folder_id in self._folder_cache:
            return self._folder_cache[folder_id]["name"]

        if self._service is None:
            return ""

        try:
            meta = (
                self._service.files()
                .get(fileId=folder_id, fields="id, name, parents")
                .execute()
            )
        except Exception:
            logger.debug("Could not resolve folder name for %s", folder_id)
            return ""

        parents = meta.get("parents", [])
        self._folder_cache[folder_id] = {
            "name": meta.get("name", ""),
            "parent": parents[0] if parents else "",
        }
        return meta.get("name", "")

    def _folder_hierarchy_signals(self, parent_folder_id: str) -> list[Signal]:
        """Build folder signals for each level in the folder ancestry.

        A file with parent path /Clients/Acme gets:
          Signal(kind="folder", value="Clients")
          Signal(kind="folder", value="Acme")
          Signal(kind="folder_path", value="Clients/Acme")
        """
        if not parent_folder_id:
            return []

        folder_names: list[str] = []
        current = parent_folder_id
        seen: set[str] = set()

        while current and current not in seen:
            seen.add(current)
            name = self._resolve_folder_name(current)
            if not name:
                break
            folder_names.append(name)
            cached = self._folder_cache.get(current, {})
            current = cached.get("parent", "")

        folder_names.reverse()

        signals: list[Signal] = [
            Signal(kind="folder", value=name) for name in folder_names
        ]
        if folder_names:
            signals.append(
                Signal(kind="folder_path", value="/".join(folder_names))
            )
        return signals

    def _metadata_signals(self, file_meta: dict) -> list[Signal]:
        """Build field:* signals from file metadata."""
        name = file_meta.get("name", "")
        mime_type = file_meta.get("mimeType", "")
        modified_time = file_meta.get("modifiedTime", "")
        size = file_meta.get("size", "0")
        owners = file_meta.get("owners", [])
        owner_name = owners[0].get("displayName", "") if owners else ""
        shared = str(file_meta.get("shared", False)).lower()

        return [
            Signal(kind="field:name", value=name),
            Signal(kind="field:mime_type", value=mime_type),
            Signal(kind="field:modified", value=modified_time),
            Signal(kind="field:size", value=str(size)),
            Signal(kind="field:owner", value=owner_name),
            Signal(kind="field:shared", value=shared),
        ]

    def write(self, entity_id: str, data: dict) -> bool:
        """Write operations are not supported for this read-only connector.

        Raises:
            WriteError: Always — this connector is read-only.
        """
        raise WriteError("Google Drive connector is read-only (metadata catalog)")

    def sync(self) -> SyncResult:
        """Check for modified files since last discover.

        Uses the Drive API modification timestamp to detect changes.

        Returns:
            SyncResult with modified file collection IDs.

        Raises:
            AuthenticationError: If authenticate() has not been called.
        """
        if self._service is None:
            raise AuthenticationError("authenticate() must be called before sync()")

        if not self._last_modified:
            self.discover()

        files = self._list_files()
        modified: list[str] = []
        new: list[str] = []

        for file_meta in files:
            file_id = file_meta["id"]
            modified_time_str = file_meta.get("modifiedTime", "")
            if not modified_time_str:
                continue

            current_mtime = datetime.fromisoformat(
                modified_time_str.replace("Z", "+00:00")
            )
            baseline = self._last_modified.get(file_id)
            if baseline is None:
                collection_id = self.make_entity_id("collection", file_id)
                new.append(collection_id)
                self._last_modified[file_id] = current_mtime
            elif current_mtime > baseline:
                collection_id = self.make_entity_id("collection", file_id)
                modified.append(collection_id)
                self._last_modified[file_id] = current_mtime

        return SyncResult(new=new, modified=modified)

    def _list_files(self, folder_id: str | None = None) -> list[dict]:
        """List files from Drive with pagination.

        Args:
            folder_id: Optional folder ID to scope the listing.

        Returns:
            List of file metadata dicts.
        """
        query_parts = ["trashed=false"]
        if folder_id:
            query_parts.append(f"'{folder_id}' in parents")
        query = " and ".join(query_parts)

        all_files: list[dict] = []
        page_token: str | None = None

        while True:
            try:
                kwargs: dict = {
                    "q": query,
                    "fields": f"nextPageToken, files({_FILE_FIELDS})",
                    "pageSize": _PAGE_SIZE,
                }
                if page_token:
                    kwargs["pageToken"] = page_token

                result = self._service.files().list(**kwargs).execute()
                all_files.extend(result.get("files", []))

                page_token = result.get("nextPageToken")
                if not page_token:
                    break
            except Exception:
                logger.warning("Failed to list files from Drive API")
                break

        return all_files
