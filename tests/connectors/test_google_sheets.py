"""Tests for the Google Sheets connector."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from brij.connectors.base import AuthenticationError, EntityNotFoundError
from brij.connectors.google_sheets import GoogleSheetsConnector

# Module path for patching local imports inside authenticate()
_MOD = "brij.connectors.google_sheets"


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
        "scopes": ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    }
    path = tmp_path / "google-sheets-token.json"
    path.write_text(json.dumps(token))
    return path


@pytest.fixture()
def mock_sheets_service() -> MagicMock:
    """Create a mock Google Sheets API service."""
    service = MagicMock()

    # Mock spreadsheets().get() for metadata
    sheet_meta = {
        "sheets": [
            {"properties": {"title": "Sheet1"}},
            {"properties": {"title": "Sheet2"}},
        ]
    }
    service.spreadsheets().get().execute.return_value = sheet_meta

    # Mock spreadsheets().values().get() for cell data
    def values_get(spreadsheetId, range):
        mock = MagicMock()
        if "Sheet1" in range:
            mock.execute.return_value = {
                "values": [
                    ["Name", "Age", "Active"],
                    ["Alice", "30", "true"],
                    ["Bob", "45", "false"],
                    ["Carol", "28", "true"],
                ]
            }
        elif "Sheet2" in range:
            mock.execute.return_value = {
                "values": [
                    ["Product", "Price"],
                    ["Widget", "9.99"],
                    ["Gadget", "24.50"],
                ]
            }
        else:
            mock.execute.return_value = {"values": []}
        return mock

    service.spreadsheets().values().get.side_effect = values_get

    return service


@pytest.fixture()
def mock_drive_service() -> MagicMock:
    """Create a mock Google Drive API service."""
    service = MagicMock()
    service.files().list().execute.return_value = {
        "files": [
            {
                "id": "spreadsheet-id-1",
                "name": "My Spreadsheet",
                "modifiedTime": "2025-01-15T10:30:00Z",
            },
        ]
    }
    return service


@pytest.fixture()
def authenticated_connector(
    credentials_file: Path,
    token_file: Path,
    mock_sheets_service: MagicMock,
) -> GoogleSheetsConnector:
    """Return a connector with mocked authentication."""
    conn = GoogleSheetsConnector()
    conn._credentials_path = credentials_file
    conn._token_path = token_file
    conn._service = mock_sheets_service
    conn._source_id = "google_sheets:user"
    return conn


# ---- Authenticate ----


class TestAuthenticate:
    def test_missing_credentials_file(self, tmp_path: Path) -> None:
        conn = GoogleSheetsConnector()
        with pytest.raises(AuthenticationError, match="credentials file not found"):
            conn.authenticate(
                {"credentials_path": str(tmp_path / "nonexistent.json")}
            )

    def test_default_credentials_path_missing(self) -> None:
        conn = GoogleSheetsConnector()
        conn._credentials_path = Path("/tmp/definitely-does-not-exist-creds.json")
        with pytest.raises(AuthenticationError, match="credentials file not found"):
            conn.authenticate({"credentials_path": "/tmp/definitely-does-not-exist-creds.json"})

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

        conn = GoogleSheetsConnector()
        conn.authenticate(
            {
                "credentials_path": str(credentials_file),
                "token_path": str(token_file),
            }
        )

        mock_from_file.assert_called_once()
        assert conn._service is not None
        assert conn._source_id == "google_sheets:user"

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

        conn = GoogleSheetsConnector()
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
    def test_discover_returns_collection_entity(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        collections = [e for e in entities if e.type == "collection"]
        assert len(collections) == 1
        assert collections[0].name == "My Spreadsheet"
        assert collections[0].get_signal_value("type") == "google_sheets"
        assert collections[0].get_signal_value("spreadsheet_id") == "spreadsheet-id-1"

    def test_discover_returns_field_entities(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        fields = [e for e in entities if e.type == "field"]
        field_names = [e.name for e in fields]
        # Sheet1 has Name, Age, Active; Sheet2 has Product, Price
        assert "Name" in field_names
        assert "Age" in field_names
        assert "Active" in field_names
        assert "Product" in field_names
        assert "Price" in field_names
        assert len(fields) == 5

    def test_field_entities_are_children_of_collection(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        collection = [e for e in entities if e.type == "collection"][0]
        fields = [e for e in entities if e.type == "field"]
        for f in fields:
            assert f.parent_id == collection.id

    def test_field_type_inference(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        fields = {e.name: e for e in entities if e.type == "field"}
        assert fields["Name"].get_signal_value("type") == "text"
        assert fields["Age"].get_signal_value("type") == "integer"
        assert fields["Active"].get_signal_value("type") == "boolean"
        assert fields["Price"].get_signal_value("type") == "float"

    def test_field_entities_have_tab_signal(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        fields = {e.name: e for e in entities if e.type == "field"}
        assert fields["Name"].get_signal_value("tab") == "Sheet1"
        assert fields["Product"].get_signal_value("tab") == "Sheet2"

    def test_collection_has_tab_names(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        collection = [e for e in entities if e.type == "collection"][0]
        tab_names = json.loads(collection.get_signal_value("tab_names"))
        assert tab_names == ["Sheet1", "Sheet2"]

    def test_collection_has_modified_time(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        collection = [e for e in entities if e.type == "collection"][0]
        assert collection.get_signal_value("modified") == "2025-01-15T10:30:00Z"

    def test_source_id_set_on_all_entities(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        for entity in entities:
            assert entity.source_id == "google_sheets:user"

    def test_discover_before_authenticate_raises(self) -> None:
        conn = GoogleSheetsConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.discover()

    def test_discover_empty_spreadsheet(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        authenticated_connector._service.spreadsheets().get().execute.return_value = {
            "sheets": [{"properties": {"title": "Empty"}}]
        }

        def empty_values(spreadsheetId, range):
            mock = MagicMock()
            mock.execute.return_value = {"values": []}
            return mock

        authenticated_connector._service.spreadsheets().values().get.side_effect = empty_values

        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        collections = [e for e in entities if e.type == "collection"]
        fields = [e for e in entities if e.type == "field"]
        assert len(collections) == 1
        assert len(fields) == 0

    def test_discover_no_spreadsheets(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        empty_drive = MagicMock()
        empty_drive.files().list().execute.return_value = {"files": []}

        with patch(f"{_MOD}.build", return_value=empty_drive):
            entities = authenticated_connector.discover()

        assert entities == []

    def test_total_entity_count(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            entities = authenticated_connector.discover()

        # 1 collection + 5 fields (3 from Sheet1 + 2 from Sheet2)
        assert len(entities) == 6


# ---- Read ----


class TestRead:
    def test_read_before_authenticate_raises(self) -> None:
        conn = GoogleSheetsConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.read("collection:spreadsheet-id-1")

    def test_read_returns_record_entities(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        assert all(e.type == "record" for e in entities)

    def test_read_creates_one_record_per_row(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        # Sheet1 has 3 data rows, Sheet2 has 2 data rows
        assert len(entities) == 5

    def test_read_records_have_field_signals(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        # First record is from Sheet1 (Alice, 30, true)
        first = entities[0]
        assert first.get_signal_value("field:Name") == "Alice"
        assert first.get_signal_value("field:Age") == "30"
        assert first.get_signal_value("field:Active") == "true"

    def test_read_records_are_tier_3(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        for entity in entities:
            assert entity.tier == 3

    def test_read_records_are_children_of_collection(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        for entity in entities:
            assert entity.parent_id == "collection:spreadsheet-id-1"

    def test_read_records_have_source_id(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        for entity in entities:
            assert entity.source_id == "google_sheets:user"

    def test_read_includes_all_tabs(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        # Sheet2 records should have Product/Price fields
        sheet2_records = [e for e in entities if e.get_signal_value("field:Product")]
        assert len(sheet2_records) == 2
        assert sheet2_records[0].get_signal_value("field:Product") == "Widget"
        assert sheet2_records[0].get_signal_value("field:Price") == "9.99"

    def test_read_unknown_entity_raises(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        with pytest.raises(EntityNotFoundError, match="Unknown entity"):
            authenticated_connector.read("field:something")

    def test_read_record_ids_include_tab_and_row(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        assert entities[0].id == "record:spreadsheet-id-1:Sheet1:0"
        assert entities[3].id == "record:spreadsheet-id-1:Sheet2:0"


# ---- Sync ----


class TestSync:
    def test_sync_before_authenticate_raises(self) -> None:
        conn = GoogleSheetsConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.sync()

    def test_sync_detects_modified_spreadsheet(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        # Update mock to return a newer modifiedTime
        updated_drive = MagicMock()
        updated_drive.files().list().execute.return_value = {
            "files": [
                {
                    "id": "spreadsheet-id-1",
                    "name": "My Spreadsheet",
                    "modifiedTime": "2025-02-20T12:00:00Z",
                },
            ]
        }
        authenticated_connector._drive_service = updated_drive

        result = authenticated_connector.sync()
        assert "collection:spreadsheet-id-1" in result.modified

    def test_sync_no_changes(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        # Same modifiedTime — no changes
        authenticated_connector._drive_service = mock_drive_service

        result = authenticated_connector.sync()
        assert result.modified == []

    def test_sync_updates_baseline_after_detection(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        # First sync: newer timestamp
        updated_drive = MagicMock()
        updated_drive.files().list().execute.return_value = {
            "files": [
                {
                    "id": "spreadsheet-id-1",
                    "name": "My Spreadsheet",
                    "modifiedTime": "2025-02-20T12:00:00Z",
                },
            ]
        }
        authenticated_connector._drive_service = updated_drive
        result1 = authenticated_connector.sync()
        assert len(result1.modified) == 1

        # Second sync with same timestamp: no changes
        result2 = authenticated_connector.sync()
        assert result2.modified == []
