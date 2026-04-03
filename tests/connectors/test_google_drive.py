"""Tests for the Google Drive connector."""

from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pdfplumber
import pytest

from brij.connectors.base import AuthenticationError, EntityNotFoundError, WriteError
from brij.connectors.google_drive import DOCS_MIME, PDF_MIME, GoogleDriveConnector, SHEETS_MIME

# Module path for patching local imports inside authenticate()
_MOD = "brij.connectors.google_drive"


def _make_pdf_bytes(text: str) -> bytes:
    """Create a minimal PDF containing *text* using pdfplumber-compatible format.

    Builds a raw PDF structure by hand so we avoid an extra dependency
    like reportlab.  The result is valid enough for pdfplumber to extract.
    """
    # Minimal valid PDF with a single text stream.
    stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET"
    stream_bytes = stream.encode("latin-1")

    lines = [
        b"%PDF-1.4",
        b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj",
        b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj",
        b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]"
        b"/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>endobj",
        b"4 0 obj<</Length " + str(len(stream_bytes)).encode() + b">>"
        b"stream\n" + stream_bytes + b"\nendstream endobj",
        b"5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj",
        b"xref",
        b"0 6",
        b"trailer<</Size 6/Root 1 0 R>>",
        b"startxref",
        b"0",
        b"%%EOF",
    ]
    return b"\n".join(lines)


# ---- Fixtures ----


@pytest.fixture()
def credentials_file(tmp_path: Path) -> Path:
    """Create a fake Google OAuth credentials file."""
    creds = {
        "installed": {
            "client_id": "test-client-id.apps.googleusercontent.com",
            "client_secret": "test-client-secret",
            "redirect_uris": ["http://localhost"],
        }
    }
    path = tmp_path / "google-credentials.json"
    path.write_text(json.dumps(creds))
    return path


@pytest.fixture()
def token_file(tmp_path: Path) -> Path:
    """Create a fake OAuth token file."""
    token = {
        "token": "fake-access-token",
        "refresh_token": "fake-refresh-token",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "test-client-id.apps.googleusercontent.com",
        "client_secret": "test-client-secret",
        "scopes": ["https://www.googleapis.com/auth/drive.readonly"],
    }
    path = tmp_path / "google-drive-token.json"
    path.write_text(json.dumps(token))
    return path


@pytest.fixture()
def mock_drive_service() -> MagicMock:
    """Create a mock Google Drive API service."""
    service = MagicMock()

    files_list_result = {
        "files": [
            {
                "id": "folder-1",
                "name": "My Documents",
                "mimeType": "application/vnd.google-apps.folder",
                "modifiedTime": "2025-01-15T10:30:00Z",
                "size": "0",
                "owners": [{"displayName": "Alice"}],
                "shared": False,
                "fullFileExtension": "",
                "parents": [],
            },
            {
                "id": "folder-2",
                "name": "Invoices",
                "mimeType": "application/vnd.google-apps.folder",
                "modifiedTime": "2025-01-16T10:00:00Z",
                "size": "0",
                "owners": [{"displayName": "Alice"}],
                "shared": False,
                "fullFileExtension": "",
                "parents": ["folder-1"],
            },
            {
                "id": "doc-1",
                "name": "Meeting Notes.docx",
                "mimeType": "application/vnd.google-apps.document",
                "modifiedTime": "2025-01-20T14:00:00Z",
                "size": "15360",
                "owners": [{"displayName": "Alice"}],
                "shared": True,
                "fullFileExtension": "docx",
                "parents": ["folder-1"],
            },
            {
                "id": "pdf-1",
                "name": "Invoice.pdf",
                "mimeType": "application/pdf",
                "modifiedTime": "2025-02-01T09:00:00Z",
                "size": "204800",
                "owners": [{"displayName": "Bob"}],
                "shared": False,
                "fullFileExtension": "pdf",
                "parents": ["folder-2"],
            },
            {
                "id": "sheet-1",
                "name": "Team Roster",
                "mimeType": SHEETS_MIME,
                "modifiedTime": "2025-02-10T08:00:00Z",
                "size": "0",
                "owners": [{"displayName": "Alice"}],
                "shared": True,
                "fullFileExtension": "",
                "parents": ["folder-1"],
            },
        ],
        "nextPageToken": None,
    }

    service.files().list().execute.return_value = files_list_result

    def list_side_effect(**kwargs):
        mock = MagicMock()
        q = kwargs.get("q", "")
        if "'folder-1' in parents" in q:
            mock.execute.return_value = {
                "files": [
                    f for f in files_list_result["files"]
                    if "folder-1" in f.get("parents", [])
                ],
            }
        elif "'folder-2' in parents" in q:
            mock.execute.return_value = {
                "files": [
                    f for f in files_list_result["files"]
                    if "folder-2" in f.get("parents", [])
                ],
            }
        else:
            mock.execute.return_value = files_list_result
        return mock

    service.files().list.side_effect = list_side_effect

    def get_side_effect(fileId, fields=None):
        mock = MagicMock()
        file_map = {f["id"]: f for f in files_list_result["files"]}
        if fileId in file_map:
            mock.execute.return_value = file_map[fileId]
        else:
            mock.execute.side_effect = Exception(f"File not found: {fileId}")
        return mock

    service.files().get.side_effect = get_side_effect

    # Export mock — returns plain text for Google Docs.
    _doc_texts = {
        "doc-1": b"Meeting notes from January. We discussed the roadmap and priorities.",
    }

    def export_side_effect(fileId, mimeType="text/plain"):
        mock = MagicMock()
        if fileId in _doc_texts:
            mock.execute.return_value = _doc_texts[fileId]
        else:
            mock.execute.side_effect = Exception(f"Export failed: {fileId}")
        return mock

    service.files().export.side_effect = export_side_effect

    # get_media mock — returns raw PDF bytes for PDF files.
    def get_media_side_effect(fileId):
        mock = MagicMock()
        if fileId == "pdf-1":
            mock.execute.return_value = _make_pdf_bytes(
                "Invoice from Acme Corp for services rendered in January."
            )
        else:
            mock.execute.side_effect = Exception(f"Download failed: {fileId}")
        return mock

    service.files().get_media.side_effect = get_media_side_effect

    return service


@pytest.fixture()
def mock_sheets_service() -> MagicMock:
    """Create a mock Google Sheets API service for auto-indexing."""
    service = MagicMock()

    sheet_metadata = {
        "sheets": [
            {"properties": {"title": "Members", "sheetId": 0}},
        ],
    }

    service.spreadsheets().get().execute.return_value = sheet_metadata

    def get_side_effect(spreadsheetId):
        mock = MagicMock()
        mock.execute.return_value = sheet_metadata
        return mock

    service.spreadsheets().get.side_effect = get_side_effect

    # Sample data for the Members tab
    tab_data = {
        "values": [
            ["Name", "Role", "Active"],
            ["Alice", "Engineer", "true"],
            ["Bob", "Designer", "false"],
            ["Carol", "PM", "true"],
        ],
    }

    def values_get_side_effect(spreadsheetId, range):
        mock = MagicMock()
        mock.execute.return_value = tab_data
        return mock

    service.spreadsheets().values().get.side_effect = values_get_side_effect

    return service


@pytest.fixture()
def authenticated_connector(
    credentials_file: Path,
    token_file: Path,
    mock_drive_service: MagicMock,
    mock_sheets_service: MagicMock,
) -> GoogleDriveConnector:
    """Return a connector with mocked authentication."""
    conn = GoogleDriveConnector()
    conn._credentials_path = credentials_file
    conn._token_path = token_file
    conn._service = mock_drive_service
    conn._sheets_service = mock_sheets_service
    conn._source_id = "google_drive:user"
    return conn


# ---- Authenticate ----


class TestAuthenticate:
    def test_missing_credentials_file(self, tmp_path: Path) -> None:
        conn = GoogleDriveConnector()
        with pytest.raises(AuthenticationError, match="credentials file not found"):
            conn.authenticate(
                {"credentials_path": str(tmp_path / "nonexistent.json")}
            )

    def test_default_credentials_path_missing(self) -> None:
        conn = GoogleDriveConnector()
        conn._credentials_path = Path("/tmp/definitely-does-not-exist-creds.json")
        with pytest.raises(AuthenticationError, match="credentials file not found"):
            conn.authenticate(
                {"credentials_path": "/tmp/definitely-does-not-exist-creds.json"}
            )

    @patch(f"{_MOD}.build")
    @patch(f"{_MOD}.Credentials.from_authorized_user_file")
    def test_successful_auth_with_existing_token(
        self,
        mock_from_file: MagicMock,
        mock_build: MagicMock,
        credentials_file: Path,
        token_file: Path,
    ) -> None:
        mock_creds = MagicMock()
        mock_creds.valid = True
        mock_from_file.return_value = mock_creds
        mock_build.return_value = MagicMock()

        conn = GoogleDriveConnector()
        conn.authenticate(
            {
                "credentials_path": str(credentials_file),
                "token_path": str(token_file),
            }
        )

        mock_from_file.assert_called_once()
        assert conn._service is not None
        assert conn._source_id == "google_drive:user"

    @patch(f"{_MOD}.InstalledAppFlow.from_client_secrets_file")
    def test_auth_failure_raises(
        self,
        mock_flow_cls: MagicMock,
        credentials_file: Path,
        tmp_path: Path,
    ) -> None:
        mock_flow = MagicMock()
        mock_flow.run_local_server.side_effect = Exception("OAuth cancelled")
        mock_flow_cls.return_value = mock_flow

        conn = GoogleDriveConnector()
        token_path = tmp_path / "token.json"

        with pytest.raises(AuthenticationError, match="OAuth flow failed"):
            conn.authenticate(
                {
                    "credentials_path": str(credentials_file),
                    "token_path": str(token_path),
                }
            )


# ---- Discover ----


class TestDiscover:
    def test_discover_before_authenticate_raises(self) -> None:
        conn = GoogleDriveConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.discover()

    def test_discover_returns_entities(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        collections = [e for e in entities if e.type == "collection"]
        fields = [e for e in entities if e.type == "field"]

        # 4 non-sheet files (2 folders + doc + pdf) + 1 sheet collection
        # + 3 field entities (Name, Role, Active)
        assert len(collections) == 5
        assert len(fields) == 3

    def test_discover_entity_names(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        names = [e.name for e in entities if e.type == "collection"]

        assert "My Documents" in names
        assert "Invoices" in names
        assert "Meeting Notes.docx" in names
        assert "Invoice.pdf" in names
        assert "Team Roster" in names

    def test_discover_entity_signals(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        folder = next(e for e in entities if e.name == "My Documents")

        assert folder.get_signal_value("type") == "google_drive"
        assert folder.get_signal_value("file_id") == "folder-1"
        assert folder.get_signal_value("mime_type") == "application/vnd.google-apps.folder"
        assert folder.get_signal_value("owner") == "Alice"
        assert folder.get_signal_value("shared") == "false"

    def test_discover_file_metadata(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")

        assert pdf.get_signal_value("type") == "pdf"
        assert pdf.get_signal_value("file_id") == "pdf-1"
        assert pdf.get_signal_value("word_count") is not None

    def test_discover_source_id_set(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        for entity in entities:
            assert entity.source_id == "google_drive:user"

    def test_discover_non_sheet_non_doc_non_pdf_entities_are_tier_1(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """Non-sheet, non-doc, non-pdf discovered entities should be Tier 1."""
        entities = authenticated_connector.discover()
        for entity in entities:
            if entity.type == "collection" and entity.get_signal_value("type") == "google_drive":
                assert entity.tier == 1

    def test_discover_with_folder_scope(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover(folder_id="folder-1")
        collections = [e for e in entities if e.type == "collection"]
        collection_names = [e.name for e in collections]

        assert "Invoices" in collection_names
        assert "Meeting Notes.docx" in collection_names
        assert "Team Roster" in collection_names

    def test_discover_empty_drive(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        def empty_list(**kwargs):
            mock = MagicMock()
            mock.execute.return_value = {"files": []}
            return mock

        authenticated_connector._service.files().list.side_effect = empty_list

        entities = authenticated_connector.discover()
        assert entities == []

    def test_discover_handles_pagination(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """Verify that discover follows nextPageToken for large drives."""
        call_count = 0

        def paginated_list(**kwargs):
            nonlocal call_count
            mock = MagicMock()
            call_count += 1
            if call_count == 1:
                mock.execute.return_value = {
                    "files": [
                        {
                            "id": "file-page1",
                            "name": "Page1.txt",
                            "mimeType": "text/plain",
                            "modifiedTime": "2025-01-01T00:00:00Z",
                            "size": "100",
                            "owners": [],
                            "shared": False,
                            "fullFileExtension": "txt",
                            "parents": [],
                        }
                    ],
                    "nextPageToken": "token-page-2",
                }
            else:
                mock.execute.return_value = {
                    "files": [
                        {
                            "id": "file-page2",
                            "name": "Page2.txt",
                            "mimeType": "text/plain",
                            "modifiedTime": "2025-01-02T00:00:00Z",
                            "size": "200",
                            "owners": [],
                            "shared": False,
                            "fullFileExtension": "txt",
                            "parents": [],
                        }
                    ],
                }
            return mock

        authenticated_connector._service.files().list.side_effect = paginated_list

        entities = authenticated_connector.discover()
        assert len(entities) == 2
        assert call_count == 2

    def test_discover_folder_signals_single_level(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """Files in a folder get a folder signal for that folder name."""
        entities = authenticated_connector.discover()
        doc = next(e for e in entities if e.name == "Meeting Notes.docx")

        folder_signals = [s for s in doc.signals if s.kind == "folder"]
        assert len(folder_signals) == 1
        assert folder_signals[0].value == "My Documents"
        assert doc.get_signal_value("folder_path") == "My Documents"

    def test_discover_folder_signals_nested(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """Files nested two levels deep get a folder signal for each level."""
        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")

        folder_signals = [s for s in pdf.signals if s.kind == "folder"]
        folder_values = [s.value for s in folder_signals]
        assert folder_values == ["My Documents", "Invoices"]
        assert pdf.get_signal_value("folder_path") == "My Documents/Invoices"

    def test_discover_root_file_has_no_folder_signals(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """A root-level folder has no folder hierarchy signals."""
        entities = authenticated_connector.discover()
        root_folder = next(e for e in entities if e.name == "My Documents")

        folder_signals = [s for s in root_folder.signals if s.kind == "folder"]
        assert folder_signals == []
        assert root_folder.get_signal_value("folder_path") is None

    def test_discover_sheet_gets_folder_signals(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """Sheets auto-indexed via the Sheets API still get folder signals."""
        entities = authenticated_connector.discover()
        sheet = next(e for e in entities if e.name == "Team Roster")

        folder_signals = [s for s in sheet.signals if s.kind == "folder"]
        assert len(folder_signals) == 1
        assert folder_signals[0].value == "My Documents"
        assert sheet.get_signal_value("folder_path") == "My Documents"

    def test_discover_modified_time_tracked(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        authenticated_connector.discover()
        assert "folder-1" in authenticated_connector._last_modified
        assert "folder-2" in authenticated_connector._last_modified
        assert "doc-1" in authenticated_connector._last_modified
        assert "pdf-1" in authenticated_connector._last_modified
        assert "sheet-1" in authenticated_connector._last_modified


# ---- Read ----


class TestRead:
    def test_read_before_authenticate_raises(self) -> None:
        conn = GoogleDriveConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.read("collection:folder-1")

    def test_read_unknown_entity_raises(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        with pytest.raises(EntityNotFoundError, match="Unknown entity"):
            authenticated_connector.read("field:something")

    def test_read_folder_returns_records(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:folder-1")

        # folder-2, doc-1, sheet-1 are children of folder-1
        assert len(entities) == 3
        assert all(e.type == "record" for e in entities)

    def test_read_folder_records_are_children(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:folder-1")
        for entity in entities:
            assert entity.parent_id == "collection:folder-1"

    def test_read_folder_records_have_metadata_signals(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:folder-1")

        first = entities[0]
        assert first.get_signal_value("field:name") is not None
        assert first.get_signal_value("field:mime_type") is not None
        assert first.get_signal_value("field:modified") is not None
        assert first.get_signal_value("field:size") is not None
        assert first.get_signal_value("field:owner") is not None
        assert first.get_signal_value("field:shared") is not None

    def test_read_folder_records_are_tier_3(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:folder-1")
        for entity in entities:
            assert entity.tier == 3

    def test_read_pdf_returns_single_record_with_text(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:pdf-1")

        assert len(entities) == 1
        assert entities[0].type == "record"
        assert entities[0].parent_id == "collection:pdf-1"
        text = entities[0].get_signal_value("field:text")
        assert text is not None
        assert "Invoice" in text

    def test_read_file_not_found_raises(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        with pytest.raises(EntityNotFoundError, match="Failed to fetch file"):
            authenticated_connector.read("collection:nonexistent-id")

    def test_read_records_have_source_id(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:folder-1")
        for entity in entities:
            assert entity.source_id == "google_drive:user"

    def test_read_signals_survive_store_roundtrip(
        self, authenticated_connector: GoogleDriveConnector, tmp_path: Path
    ) -> None:
        """Signals must persist through put_entity -> get_entity in the Store."""
        from brij.core.store import Store

        discovered = authenticated_connector.discover()
        records = authenticated_connector.read("collection:folder-1")

        store = Store(tmp_path / "test.db")
        try:
            store.add_source("google_drive:user", "test", "google_drive")
            for entity in discovered:
                store.put_entity(entity)
            for record in records:
                store.put_entity(record)

            total_entities = store.count_entities()
            total_signals = store.count_signals()

            # folder-1: 10 signals (no parent folders)
            # folder-2: 12 signals (10 base + folder "My Documents" + folder_path)
            # doc-1: 8 signals (name, type, doc_id, modified, word_count, preview
            #         + folder "My Documents" + folder_path)
            # pdf-1: 9 signals (name, type, file_id, modified, word_count, preview
            #         + folder "My Documents" + folder "Invoices" + folder_path)
            # sheet-1: 7 signals (5 base + folder "My Documents" + folder_path)
            # 3 field entities (3 signals each = 9)
            # 3 folder records from read (6 signals each = 18)
            assert total_entities == 11
            assert total_signals == 73

            first = store.get_entity(records[0].id)
            assert first is not None
            assert first.get_signal_value("field:name") is not None
        finally:
            store.close()


# ---- Write ----


class TestWrite:
    def test_write_raises_read_only(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        with pytest.raises(WriteError, match="read-only"):
            authenticated_connector.write(
                "collection:doc-1", {"action": "add", "fields": {}}
            )


# ---- Sync ----


class TestSync:
    def test_sync_before_authenticate_raises(self) -> None:
        conn = GoogleDriveConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.sync()

    def test_sync_detects_modified_file(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        authenticated_connector.discover()

        # Update mock to return newer modifiedTime for doc-1
        def updated_list(**kwargs):
            mock = MagicMock()
            mock.execute.return_value = {
                "files": [
                    {
                        "id": "doc-1",
                        "name": "Meeting Notes.docx",
                        "mimeType": "application/vnd.google-apps.document",
                        "modifiedTime": "2025-03-01T12:00:00Z",
                        "size": "15360",
                        "owners": [{"displayName": "Alice"}],
                        "shared": True,
                        "fullFileExtension": "docx",
                        "parents": ["folder-1"],
                    },
                ],
            }
            return mock

        authenticated_connector._service.files().list.side_effect = updated_list

        result = authenticated_connector.sync()
        assert "collection:doc-1" in result.modified

    def test_sync_detects_new_file(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        authenticated_connector.discover()

        def new_file_list(**kwargs):
            mock = MagicMock()
            mock.execute.return_value = {
                "files": [
                    {
                        "id": "new-file-1",
                        "name": "NewFile.txt",
                        "mimeType": "text/plain",
                        "modifiedTime": "2025-03-15T08:00:00Z",
                        "size": "500",
                        "owners": [],
                        "shared": False,
                        "fullFileExtension": "txt",
                        "parents": [],
                    },
                ],
            }
            return mock

        authenticated_connector._service.files().list.side_effect = new_file_list

        result = authenticated_connector.sync()
        assert "collection:new-file-1" in result.new

    def test_sync_no_changes(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        authenticated_connector.discover()

        # Same files with same timestamps
        result = authenticated_connector.sync()
        assert result.modified == []
        assert result.new == []

    def test_sync_updates_baseline(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        authenticated_connector.discover()

        def updated_list(**kwargs):
            mock = MagicMock()
            mock.execute.return_value = {
                "files": [
                    {
                        "id": "doc-1",
                        "name": "Meeting Notes.docx",
                        "mimeType": "application/vnd.google-apps.document",
                        "modifiedTime": "2025-03-01T12:00:00Z",
                        "size": "15360",
                        "owners": [{"displayName": "Alice"}],
                        "shared": True,
                        "fullFileExtension": "docx",
                        "parents": ["folder-1"],
                    },
                ],
            }
            return mock

        authenticated_connector._service.files().list.side_effect = updated_list

        result1 = authenticated_connector.sync()
        assert len(result1.modified) == 1

        # Second sync with same timestamp — no changes
        result2 = authenticated_connector.sync()
        assert result2.modified == []


# ---- Status integration ----


class TestStatusIntegration:
    def test_file_type_counts_from_entities(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """After discover, status should show file count by type."""
        entities = authenticated_connector.discover()

        # Count by mime_type (non-sheet/doc/pdf files) and type signal for the rest.
        type_counts: dict[str, int] = {}
        for entity in entities:
            if entity.type != "collection":
                continue
            mime = entity.get_signal_value("mime_type")
            type_val = entity.get_signal_value("type")
            if mime:
                label = mime.split("/")[-1]
                type_counts[label] = type_counts.get(label, 0) + 1
            elif type_val in ("google_sheets", "google_doc", "pdf"):
                type_counts[type_val] = type_counts.get(type_val, 0) + 1

        assert type_counts["vnd.google-apps.folder"] == 2
        assert type_counts["google_doc"] == 1
        assert type_counts["pdf"] == 1
        assert type_counts["google_sheets"] == 1


# ---- Auto-index Sheets ----


class TestAutoIndexSheets:
    """Tests for automatic Sheets indexing in the Drive connector."""

    def test_discover_sheet_creates_collection_with_sheets_type(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        sheet = next(e for e in entities if e.name == "Team Roster")

        assert sheet.type == "collection"
        assert sheet.get_signal_value("type") == "google_sheets"
        assert sheet.get_signal_value("spreadsheet_id") == "sheet-1"

    def test_discover_sheet_has_tab_names(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        sheet = next(e for e in entities if e.name == "Team Roster")

        tab_names = json.loads(sheet.get_signal_value("tab_names"))
        assert tab_names == ["Members"]

    def test_discover_sheet_creates_field_entities(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        fields = [e for e in entities if e.type == "field"]

        assert len(fields) == 3
        field_names = sorted(e.name for e in fields)
        assert field_names == ["Active", "Name", "Role"]

    def test_discover_sheet_field_entities_have_parent(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        sheet = next(e for e in entities if e.name == "Team Roster")
        fields = [e for e in entities if e.type == "field"]

        for field in fields:
            assert field.parent_id == sheet.id

    def test_discover_sheet_field_types_inferred(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        fields = {e.name: e for e in entities if e.type == "field"}

        assert fields["Name"].get_signal_value("type") == "text"
        assert fields["Role"].get_signal_value("type") == "text"
        assert fields["Active"].get_signal_value("type") == "boolean"

    def test_discover_sheet_field_tab_signal(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        fields = [e for e in entities if e.type == "field"]

        for field in fields:
            assert field.get_signal_value("tab") == "Members"

    def test_read_sheet_returns_record_entities(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:sheet-1")

        assert len(entities) == 3
        assert all(e.type == "record" for e in entities)

    def test_read_sheet_records_have_field_signals(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:sheet-1")

        first = entities[0]
        assert first.get_signal_value("field:Name") == "Alice"
        assert first.get_signal_value("field:Role") == "Engineer"
        assert first.get_signal_value("field:Active") == "true"

    def test_read_sheet_records_are_tier_3(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:sheet-1")
        for entity in entities:
            assert entity.tier == 3

    def test_read_sheet_records_have_parent_id(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:sheet-1")
        for entity in entities:
            assert entity.parent_id == "collection:sheet-1"

    def test_read_sheet_all_rows_present(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:sheet-1")

        names = [e.get_signal_value("field:Name") for e in entities]
        assert names == ["Alice", "Bob", "Carol"]

    def test_discover_sheet_api_failure_skips_gracefully(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """If Sheets API fails for a file, it is skipped without error."""
        def failing_get(spreadsheetId):
            mock = MagicMock()
            mock.execute.side_effect = Exception("API error")
            return mock

        authenticated_connector._sheets_service.spreadsheets().get.side_effect = failing_get

        entities = authenticated_connector.discover()
        # 4 non-sheet files: 2 folders + pdf + doc (google_doc)
        collections = [e for e in entities if e.type == "collection"]
        assert len(collections) == 4
        types = {e.get_signal_value("type") for e in collections}
        assert types == {"google_drive", "google_doc", "pdf"}

    def test_discover_without_sheets_service_treats_as_regular_file(
        self,
        credentials_file: Path,
        token_file: Path,
        mock_drive_service: MagicMock,
    ) -> None:
        """Without a Sheets service, spreadsheets become regular Tier 1 metadata."""
        conn = GoogleDriveConnector()
        conn._credentials_path = credentials_file
        conn._token_path = token_file
        conn._service = mock_drive_service
        conn._sheets_service = None
        conn._source_id = "google_drive:user"

        entities = conn.discover()
        collections = [e for e in entities if e.type == "collection"]
        assert len(collections) == 5
        # Doc & PDF are Tier 2 (have preview), sheet treated as regular file → Tier 1
        tier1 = [
            e for e in collections
            if e.get_signal_value("type") not in ("google_doc", "pdf")
        ]
        assert all(e.tier == 1 for e in tier1)


# ---- Auto-index Docs ----


class TestAutoIndexDocs:
    """Tests for automatic Google Docs text extraction in the Drive connector."""

    def test_discover_doc_creates_collection_with_doc_type(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        doc = next(e for e in entities if e.name == "Meeting Notes.docx")

        assert doc.type == "collection"
        assert doc.get_signal_value("type") == "google_doc"
        assert doc.get_signal_value("doc_id") == "doc-1"

    def test_discover_doc_has_preview_signal(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        doc = next(e for e in entities if e.name == "Meeting Notes.docx")

        preview = doc.get_signal_value("preview")
        assert preview is not None
        assert "Meeting notes" in preview

    def test_discover_doc_is_tier_2(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """A discovered Doc with a preview signal is Tier 2."""
        entities = authenticated_connector.discover()
        doc = next(e for e in entities if e.name == "Meeting Notes.docx")
        assert doc.tier == 2

    def test_discover_doc_has_word_count(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        doc = next(e for e in entities if e.name == "Meeting Notes.docx")

        word_count = doc.get_signal_value("word_count")
        assert word_count is not None
        assert int(word_count) > 0

    def test_discover_doc_has_folder_signals(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """Docs auto-indexed via export still get folder signals."""
        entities = authenticated_connector.discover()
        doc = next(e for e in entities if e.name == "Meeting Notes.docx")

        folder_signals = [s for s in doc.signals if s.kind == "folder"]
        assert len(folder_signals) == 1
        assert folder_signals[0].value == "My Documents"
        assert doc.get_signal_value("folder_path") == "My Documents"

    def test_discover_doc_preview_truncated_to_500_words(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """Preview should be at most 500 words even for long documents."""
        long_text = " ".join(f"word{i}" for i in range(1000))

        def long_export(fileId, mimeType="text/plain"):
            mock = MagicMock()
            mock.execute.return_value = long_text.encode("utf-8")
            return mock

        authenticated_connector._service.files().export.side_effect = long_export

        entities = authenticated_connector.discover()
        doc = next(e for e in entities if e.name == "Meeting Notes.docx")

        preview = doc.get_signal_value("preview")
        assert len(preview.split()) == 500

    def test_read_doc_returns_record_with_full_text(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:doc-1")

        assert len(entities) == 1
        record = entities[0]
        assert record.type == "record"
        assert record.parent_id == "collection:doc-1"
        text = record.get_signal_value("field:text")
        assert text is not None
        assert "Meeting notes" in text

    def test_read_doc_record_is_tier_3(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:doc-1")
        assert entities[0].tier == 3

    def test_discover_doc_export_failure_falls_back_to_metadata(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """If Drive export fails for a doc, fall back to Tier 1 metadata."""
        def failing_export(fileId, mimeType="text/plain"):
            mock = MagicMock()
            mock.execute.side_effect = Exception("Export failed")
            return mock

        authenticated_connector._service.files().export.side_effect = failing_export

        entities = authenticated_connector.discover()
        doc = next(e for e in entities if e.name == "Meeting Notes.docx")

        assert doc.get_signal_value("type") == "google_drive"
        assert doc.get_signal_value("mime_type") == DOCS_MIME
        assert doc.tier == 1

    def test_read_doc_export_failure_raises(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """If Drive export fails during read, raise EntityNotFoundError."""
        def failing_export(fileId, mimeType="text/plain"):
            mock = MagicMock()
            mock.execute.side_effect = Exception("Export failed")
            return mock

        authenticated_connector._service.files().export.side_effect = failing_export

        with pytest.raises(EntityNotFoundError, match="Failed to export"):
            authenticated_connector.read("collection:doc-1")


# ---- Auto-index PDFs ----


class TestAutoIndexPdfs:
    """Tests for automatic PDF text extraction in the Drive connector."""

    def test_discover_pdf_creates_collection_with_pdf_type(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")

        assert pdf.type == "collection"
        assert pdf.get_signal_value("type") == "pdf"
        assert pdf.get_signal_value("file_id") == "pdf-1"

    def test_discover_pdf_has_preview_signal(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")

        preview = pdf.get_signal_value("preview")
        assert preview is not None
        assert "Invoice" in preview

    def test_discover_pdf_is_tier_2(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """A discovered PDF with a preview signal is Tier 2."""
        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")
        assert pdf.tier == 2

    def test_discover_pdf_has_word_count(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")

        word_count = pdf.get_signal_value("word_count")
        assert word_count is not None
        assert int(word_count) > 0

    def test_discover_pdf_has_folder_signals(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """PDFs auto-indexed via extraction still get folder signals."""
        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")

        folder_signals = [s for s in pdf.signals if s.kind == "folder"]
        folder_values = [s.value for s in folder_signals]
        assert folder_values == ["My Documents", "Invoices"]
        assert pdf.get_signal_value("folder_path") == "My Documents/Invoices"

    def test_discover_pdf_preview_truncated_to_500_words(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """Preview should be at most 500 words even for long PDFs."""
        long_text = " ".join(f"word{i}" for i in range(1000))

        def long_pdf_media(fileId):
            mock = MagicMock()
            mock.execute.return_value = _make_pdf_bytes(long_text)
            return mock

        authenticated_connector._service.files().get_media.side_effect = long_pdf_media

        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")

        preview = pdf.get_signal_value("preview")
        assert len(preview.split()) == 500

    def test_read_pdf_returns_record_with_full_text(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:pdf-1")

        assert len(entities) == 1
        record = entities[0]
        assert record.type == "record"
        assert record.parent_id == "collection:pdf-1"
        text = record.get_signal_value("field:text")
        assert text is not None
        assert "Invoice" in text

    def test_read_pdf_record_is_tier_3(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        entities = authenticated_connector.read("collection:pdf-1")
        assert entities[0].tier == 3

    def test_discover_pdf_download_failure_falls_back_to_metadata(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """If PDF download fails, fall back to Tier 1 metadata."""
        def failing_media(fileId):
            mock = MagicMock()
            mock.execute.side_effect = Exception("Download failed")
            return mock

        authenticated_connector._service.files().get_media.side_effect = failing_media

        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")

        assert pdf.get_signal_value("type") == "google_drive"
        assert pdf.get_signal_value("mime_type") == PDF_MIME
        assert pdf.tier == 1

    def test_read_pdf_download_failure_raises(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """If PDF download fails during read, raise EntityNotFoundError."""
        def failing_media(fileId):
            mock = MagicMock()
            mock.execute.side_effect = Exception("Download failed")
            return mock

        authenticated_connector._service.files().get_media.side_effect = failing_media

        with pytest.raises(EntityNotFoundError, match="Failed to extract text from PDF"):
            authenticated_connector.read("collection:pdf-1")

    def test_discover_pdf_no_extractable_text_falls_back(
        self, authenticated_connector: GoogleDriveConnector
    ) -> None:
        """A PDF with no extractable text (scanned image) falls back to Tier 1."""
        # Return a minimal PDF with an empty content stream (no text operators).
        empty_stream = b""
        empty_pdf = (
            b"%PDF-1.4\n"
            b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
            b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
            b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]"
            b"/Contents 4 0 R/Resources<<>>>>endobj\n"
            b"4 0 obj<</Length 0>>stream\n" + empty_stream + b"\nendstream endobj\n"
            b"xref\n0 5\n"
            b"trailer<</Size 5/Root 1 0 R>>\n"
            b"startxref\n0\n%%EOF"
        )

        def empty_pdf_media(fileId):
            mock = MagicMock()
            mock.execute.return_value = empty_pdf
            return mock

        authenticated_connector._service.files().get_media.side_effect = empty_pdf_media

        entities = authenticated_connector.discover()
        pdf = next(e for e in entities if e.name == "Invoice.pdf")

        assert pdf.get_signal_value("type") == "google_drive"
        assert pdf.tier == 1
