"""Tests for the Google Sheets connector."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from brij.connectors.base import AuthenticationError, EntityNotFoundError, WriteError
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
        "scopes": [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
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


# ---- List Spreadsheets ----


class TestListSpreadsheets:
    def test_list_before_authenticate_raises(self) -> None:
        conn = GoogleSheetsConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.list_spreadsheets()

    def test_list_returns_spreadsheets(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            result = authenticated_connector.list_spreadsheets()

        assert len(result) == 1
        assert result[0]["id"] == "spreadsheet-id-1"
        assert result[0]["name"] == "My Spreadsheet"

    def test_list_empty(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        empty_drive = MagicMock()
        empty_drive.files().list().execute.return_value = {"files": []}

        with patch(f"{_MOD}.build", return_value=empty_drive):
            result = authenticated_connector.list_spreadsheets()

        assert result == []


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

    def test_discover_single_spreadsheet(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        """discover(spreadsheet_id=...) should only index that spreadsheet."""
        entities = authenticated_connector.discover(
            spreadsheet_id="spreadsheet-id-1"
        )

        collections = [e for e in entities if e.type == "collection"]
        assert len(collections) == 1
        assert collections[0].get_signal_value("spreadsheet_id") == "spreadsheet-id-1"

        fields = [e for e in entities if e.type == "field"]
        assert len(fields) == 5


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

    def test_read_all_records_have_signals(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock
    ) -> None:
        """Every record must carry a field signal for every column in its tab."""
        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            authenticated_connector.discover()

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        # Sheet1 rows have 3 columns, Sheet2 rows have 2 columns
        for entity in entities:
            assert len(entity.signals) > 0, f"Record {entity.id} has no signals"
            for signal in entity.signals:
                assert signal.kind.startswith("field:"), (
                    f"Unexpected signal kind: {signal.kind}"
                )

        # Sheet1: 3 rows × 3 columns = 9 signals; Sheet2: 2 rows × 2 columns = 4
        total_signals = sum(len(e.signals) for e in entities)
        assert total_signals == 13

    def test_read_signals_survive_store_roundtrip(
        self, authenticated_connector: GoogleSheetsConnector, mock_drive_service: MagicMock,
        tmp_path,
    ) -> None:
        """Signals must persist through put_entity → get_entity in the Store."""
        from brij.core.store import Store

        with patch(f"{_MOD}.build", return_value=mock_drive_service):
            discovered = authenticated_connector.discover()

        records = authenticated_connector.read("collection:spreadsheet-id-1")

        store = Store(tmp_path / "test.db")
        try:
            store.add_source("google_sheets:user", "test", "google_sheets")
            for entity in discovered:
                store.put_entity(entity)
            for record in records:
                store.put_entity(record)

            total_signals = store.count_signals()
            total_entities = store.count_entities()

            # 1 collection (5 signals) + 5 fields (3 each = 15) + 5 records
            # Sheet1 records: 3 × 3 = 9 signals, Sheet2 records: 2 × 2 = 4 signals
            assert total_entities == 11  # 1 + 5 + 5
            assert total_signals == 5 + 15 + 9 + 4  # = 33

            # Verify a record can be retrieved with its signals
            first = store.get_entity(records[0].id)
            assert first is not None
            assert first.get_signal_value("field:Name") == "Alice"
            assert first.get_signal_value("field:Age") == "30"
            assert first.get_signal_value("field:Active") == "true"
        finally:
            store.close()

    def test_read_dedupe_headers(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        """Duplicate column names get suffixed so signals are distinct."""
        service = authenticated_connector._service

        service.spreadsheets().get().execute.return_value = {
            "sheets": [{"properties": {"title": "Dupes"}}]
        }

        def dupes_values(spreadsheetId, range):
            mock = MagicMock()
            mock.execute.return_value = {
                "values": [
                    ["Name", "Name", ""],
                    ["Alice", "Smith", "extra"],
                ]
            }
            return mock

        service.spreadsheets().values().get.side_effect = dupes_values

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        assert len(entities) == 1
        rec = entities[0]
        assert rec.get_signal_value("field:Name") == "Alice"
        assert rec.get_signal_value("field:Name_2") == "Smith"
        assert rec.get_signal_value("field:unnamed") == "extra"

    def test_read_skips_empty_leading_rows(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        """Sheets with blank/title rows before the header should still work."""
        service = authenticated_connector._service

        service.spreadsheets().get().execute.return_value = {
            "sheets": [{"properties": {"title": "Banner"}}]
        }

        def banner_values(spreadsheetId, range):
            mock = MagicMock()
            mock.execute.return_value = {
                "values": [
                    [],
                    ["Title banner only"],
                    ["Name", "Role", "Location"],
                    ["Alice", "Eng", "NYC"],
                    ["Bob", "PM", "LA"],
                ]
            }
            return mock

        service.spreadsheets().values().get.side_effect = banner_values

        entities = authenticated_connector.read("collection:spreadsheet-id-1")
        assert len(entities) == 2
        assert entities[0].get_signal_value("field:Name") == "Alice"
        assert entities[0].get_signal_value("field:Role") == "Eng"
        assert entities[1].get_signal_value("field:Location") == "LA"


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


# ---- Write ----


class TestWrite:
    def test_write_before_authenticate_raises(self) -> None:
        conn = GoogleSheetsConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.write("collection:ss-1", {"action": "add", "fields": {"Name": "X"}})

    def test_write_unknown_action_raises(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        with pytest.raises(WriteError, match="Unknown write action"):
            authenticated_connector.write("collection:ss-1", {"action": "explode"})

    def test_add_record_appends_row(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        service = authenticated_connector._service

        # Mock headers read
        headers_mock = MagicMock()
        headers_mock.execute.return_value = {"values": [["Name", "Age", "Active"]]}
        service.spreadsheets().values().get.return_value = headers_mock

        # Mock append
        append_mock = MagicMock()
        append_mock.execute.return_value = {}
        service.spreadsheets().values().append.return_value = append_mock

        result = authenticated_connector.write(
            "collection:ss-1",
            {"action": "add", "fields": {"Name": "Dave", "Age": "35", "Active": "true"}},
        )

        assert result is True
        service.spreadsheets().values().append.assert_called_once_with(
            spreadsheetId="ss-1",
            range="'Sheet1'!A1",
            valueInputOption="RAW",
            body={"values": [["Dave", "35", "true"]]},
        )

    def test_add_record_missing_fields_uses_empty_string(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        service = authenticated_connector._service

        headers_mock = MagicMock()
        headers_mock.execute.return_value = {"values": [["Name", "Age", "Active"]]}
        service.spreadsheets().values().get.return_value = headers_mock

        append_mock = MagicMock()
        append_mock.execute.return_value = {}
        service.spreadsheets().values().append.return_value = append_mock

        result = authenticated_connector.write(
            "collection:ss-1",
            {"action": "add", "fields": {"Name": "Eve"}},
        )

        assert result is True
        call_body = service.spreadsheets().values().append.call_args
        assert call_body[1]["body"]["values"] == [["Eve", "", ""]]

    def test_update_record_updates_correct_cell(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        service = authenticated_connector._service

        headers_mock = MagicMock()
        headers_mock.execute.return_value = {"values": [["Name", "Age", "Active"]]}
        service.spreadsheets().values().get.return_value = headers_mock

        update_mock = MagicMock()
        update_mock.execute.return_value = {}
        service.spreadsheets().values().update.return_value = update_mock

        result = authenticated_connector.write(
            "record:ss-1:Sheet1:1",
            {"action": "update", "fields": {"Name": "Bobby", "Age": "46", "Active": "true"}},
        )

        assert result is True
        # Row index 1 → sheet row 3 (1-indexed, row 1 is headers)
        service.spreadsheets().values().update.assert_called_once_with(
            spreadsheetId="ss-1",
            range="'Sheet1'!A3",
            valueInputOption="RAW",
            body={"values": [["Bobby", "46", "true"]]},
        )

    def test_update_record_row_zero(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        service = authenticated_connector._service

        headers_mock = MagicMock()
        headers_mock.execute.return_value = {"values": [["Name", "Age", "Active"]]}
        service.spreadsheets().values().get.return_value = headers_mock

        update_mock = MagicMock()
        update_mock.execute.return_value = {}
        service.spreadsheets().values().update.return_value = update_mock

        authenticated_connector.write(
            "record:ss-1:Sheet1:0",
            {"action": "update", "fields": {"Name": "Alice2", "Age": "31", "Active": "yes"}},
        )

        service.spreadsheets().values().update.assert_called_once_with(
            spreadsheetId="ss-1",
            range="'Sheet1'!A2",
            valueInputOption="RAW",
            body={"values": [["Alice2", "31", "yes"]]},
        )

    def test_delete_record_removes_row(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        service = authenticated_connector._service

        sheet_meta = {
            "sheets": [
                {"properties": {"title": "Sheet1", "sheetId": 0}},
            ]
        }
        service.spreadsheets().get().execute.return_value = sheet_meta

        batch_mock = MagicMock()
        batch_mock.execute.return_value = {}
        service.spreadsheets().batchUpdate.return_value = batch_mock

        result = authenticated_connector.write(
            "record:ss-1:Sheet1:2",
            {"action": "delete"},
        )

        assert result is True
        service.spreadsheets().batchUpdate.assert_called_once_with(
            spreadsheetId="ss-1",
            body={
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": 0,
                                "dimension": "ROWS",
                                "startIndex": 3,
                                "endIndex": 4,
                            }
                        }
                    }
                ]
            },
        )

    def test_delete_unknown_tab_raises(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        service = authenticated_connector._service

        sheet_meta = {
            "sheets": [
                {"properties": {"title": "Sheet1", "sheetId": 0}},
            ]
        }
        service.spreadsheets().get().execute.return_value = sheet_meta

        with pytest.raises(EntityNotFoundError, match="Tab not found"):
            authenticated_connector.write(
                "record:ss-1:NoSuchTab:0",
                {"action": "delete"},
            )

    def test_write_invalid_record_id_raises(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        with pytest.raises(EntityNotFoundError, match="Not a record entity"):
            authenticated_connector.write(
                "field:something",
                {"action": "update", "fields": {"Name": "X"}},
            )

    def test_add_to_non_collection_raises(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        with pytest.raises(EntityNotFoundError, match="Expected a collection"):
            authenticated_connector.write(
                "record:ss-1:Sheet1:0",
                {"action": "add", "fields": {"Name": "X"}},
            )


# ---- Create Collection ----


class TestCreateCollection:
    def test_create_collection_before_authenticate_raises(self) -> None:
        conn = GoogleSheetsConnector()
        with pytest.raises(AuthenticationError, match="authenticate"):
            conn.create_collection("Test", {"fields": ["A", "B"]})

    def test_create_collection_empty_fields_raises(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        with pytest.raises(WriteError, match="non-empty"):
            authenticated_connector.create_collection("Test", {"fields": []})

    def test_create_collection_missing_fields_raises(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        with pytest.raises(WriteError, match="non-empty"):
            authenticated_connector.create_collection("Test", {})

    def test_create_collection_produces_entity(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        service = authenticated_connector._service

        create_mock = MagicMock()
        create_mock.execute.return_value = {"spreadsheetId": "new-ss-id"}
        service.spreadsheets().create.return_value = create_mock

        update_mock = MagicMock()
        update_mock.execute.return_value = {}
        service.spreadsheets().values().update.return_value = update_mock

        entity = authenticated_connector.create_collection(
            "My New Sheet", {"fields": ["Name", "Email", "Role"]}
        )

        assert entity.type == "collection"
        assert entity.id == "collection:new-ss-id"
        assert entity.name == "My New Sheet"
        assert entity.get_signal_value("type") == "google_sheets"
        assert entity.get_signal_value("spreadsheet_id") == "new-ss-id"
        assert entity.source_id == "google_sheets:user"

    def test_create_collection_writes_headers(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        service = authenticated_connector._service

        create_mock = MagicMock()
        create_mock.execute.return_value = {"spreadsheetId": "new-ss-id"}
        service.spreadsheets().create.return_value = create_mock

        update_mock = MagicMock()
        update_mock.execute.return_value = {}
        service.spreadsheets().values().update.return_value = update_mock

        authenticated_connector.create_collection(
            "Test", {"fields": ["Col1", "Col2"]}
        )

        service.spreadsheets().values().update.assert_called_once_with(
            spreadsheetId="new-ss-id",
            range="'Sheet1'!A1",
            valueInputOption="RAW",
            body={"values": [["Col1", "Col2"]]},
        )

    def test_create_collection_api_failure_raises(
        self, authenticated_connector: GoogleSheetsConnector
    ) -> None:
        service = authenticated_connector._service

        create_mock = MagicMock()
        create_mock.execute.side_effect = Exception("API error")
        service.spreadsheets().create.return_value = create_mock

        with pytest.raises(WriteError, match="Failed to create spreadsheet"):
            authenticated_connector.create_collection(
                "Test", {"fields": ["A"]}
            )
