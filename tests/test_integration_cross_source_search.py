"""Integration test: Google Drive (Sheets, Docs, PDFs) → store → cross-source search.

Proves that a single search query returns results from multiple file types
in one ranked list — the cross-source promise (Issue #52).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from brij.config import SearchConfig
from brij.connectors.csv_local import CsvLocalConnector
from brij.connectors.google_drive import SHEETS_MIME, GoogleDriveConnector
from brij.core.models import Entity
from brij.core.store import Store
from brij.search.engine import SearchEngine

# ---- Fixtures ----


@pytest.fixture()
def store() -> Store:
    s = Store(":memory:")
    yield s
    s.close()


@pytest.fixture()
def config() -> SearchConfig:
    return SearchConfig(default_limit=20)


@pytest.fixture()
def mock_drive_service() -> MagicMock:
    """Mock Google Drive API with a folder containing a Doc, PDF, and Sheet."""
    service = MagicMock()

    files_list_result = {
        "files": [
            {
                "id": "folder-projects",
                "name": "Projects",
                "mimeType": "application/vnd.google-apps.folder",
                "modifiedTime": "2025-01-10T10:00:00Z",
                "size": "0",
                "owners": [{"displayName": "Alice"}],
                "shared": False,
                "fullFileExtension": "",
                "parents": [],
            },
            {
                "id": "doc-proposal",
                "name": "Q1 Proposal",
                "mimeType": "application/vnd.google-apps.document",
                "modifiedTime": "2025-01-20T14:00:00Z",
                "size": "25600",
                "owners": [{"displayName": "Alice"}],
                "shared": True,
                "fullFileExtension": "docx",
                "parents": ["folder-projects"],
            },
            {
                "id": "pdf-invoice",
                "name": "Invoice 2025-001",
                "mimeType": "application/pdf",
                "modifiedTime": "2025-02-01T09:00:00Z",
                "size": "102400",
                "owners": [{"displayName": "Bob"}],
                "shared": False,
                "fullFileExtension": "pdf",
                "parents": ["folder-projects"],
            },
            {
                "id": "sheet-budget",
                "name": "Q1 Budget Tracker",
                "mimeType": SHEETS_MIME,
                "modifiedTime": "2025-02-10T08:00:00Z",
                "size": "0",
                "owners": [{"displayName": "Alice"}],
                "shared": True,
                "fullFileExtension": "",
                "parents": ["folder-projects"],
            },
        ],
        "nextPageToken": None,
    }

    def list_side_effect(**kwargs):
        mock = MagicMock()
        q = kwargs.get("q", "")
        if "'folder-projects' in parents" in q:
            mock.execute.return_value = {
                "files": [
                    f for f in files_list_result["files"]
                    if "folder-projects" in f.get("parents", [])
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

    # Export mock for Google Docs text extraction.
    _doc_texts = {
        "doc-proposal": b"Q1 proposal document with project goals and deliverables.",
    }

    def export_side_effect(fileId, mimeType="text/plain"):
        mock = MagicMock()
        if fileId in _doc_texts:
            mock.execute.return_value = _doc_texts[fileId]
        else:
            mock.execute.side_effect = Exception(f"Export failed: {fileId}")
        return mock

    service.files().export.side_effect = export_side_effect

    return service


@pytest.fixture()
def mock_sheets_service() -> MagicMock:
    """Mock Sheets API with budget data that overlaps with CSV content."""
    service = MagicMock()

    sheet_metadata = {
        "sheets": [
            {"properties": {"title": "Budget", "sheetId": 0}},
        ],
    }

    def get_side_effect(spreadsheetId):
        mock = MagicMock()
        mock.execute.return_value = sheet_metadata
        return mock

    service.spreadsheets().get.side_effect = get_side_effect

    tab_data = {
        "values": [
            ["Project", "Amount", "Status"],
            ["Website Redesign", "15000", "Active"],
            ["Mobile App", "25000", "Active"],
            ["Data Pipeline", "8000", "Complete"],
        ],
    }

    def values_get_side_effect(spreadsheetId, range):
        mock = MagicMock()
        mock.execute.return_value = tab_data
        return mock

    service.spreadsheets().values().get.side_effect = values_get_side_effect

    return service


@pytest.fixture()
def drive_connector(
    mock_drive_service: MagicMock,
    mock_sheets_service: MagicMock,
) -> GoogleDriveConnector:
    """Pre-authenticated Google Drive connector with mocked APIs."""
    conn = GoogleDriveConnector()
    conn._service = mock_drive_service
    conn._sheets_service = mock_sheets_service
    conn._source_id = "google_drive:user"
    return conn


@pytest.fixture()
def projects_csv(tmp_path: Path) -> Path:
    """CSV fixture with project data that overlaps with the Sheet content."""
    content = (
        "project,owner,budget,status\n"
        "Website Redesign,Alice,15000,Active\n"
        "API Integration,Carol,12000,Active\n"
        "Data Pipeline,Eve,8000,Complete\n"
    )
    path = tmp_path / "projects.csv"
    path.write_text(content)
    return path


# ---- Helpers ----


def _ingest_drive(connector: GoogleDriveConnector, store: Store) -> str:
    """Discover and read all Drive entities into the store. Returns source_id."""
    discovered = connector.discover()
    for entity in discovered:
        store.put_entity(entity)

    # Read records from each collection (folder, doc, pdf, sheet).
    for entity in discovered:
        if entity.type == "collection":
            try:
                records = connector.read(entity.id)
                for record in records:
                    store.put_entity(record)
            except Exception:
                pass

    return connector._source_id


def _ingest_csv(csv_path: Path, store: Store) -> str:
    """Connect CSV, discover, read, and store everything. Returns source_id."""
    conn = CsvLocalConnector()
    conn.authenticate({"path": str(csv_path)})

    discovered = conn.discover()
    for entity in discovered:
        store.put_entity(entity)

    collection_id = discovered[0].id
    records = conn.read(collection_id)
    for record in records:
        store.put_entity(record)

    return discovered[0].source_id


# ---- Tests ----


class TestCrossSourceSearch:
    """Search across Google Drive + CSV returns results from both in one ranked list."""

    def test_search_returns_results_from_multiple_sources(
        self,
        drive_connector: GoogleDriveConnector,
        projects_csv: Path,
        store: Store,
        config: SearchConfig,
    ) -> None:
        """A single query returns results from both Drive and CSV sources."""
        drive_source = _ingest_drive(drive_connector, store)
        csv_source = _ingest_csv(projects_csv, store)

        engine = SearchEngine(store, config)
        results = engine.search("Active")

        assert len(results) >= 2

        source_ids = {r.source_id for r in results}
        assert drive_source in source_ids
        assert csv_source in source_ids

    def test_search_returns_entity_objects(
        self,
        drive_connector: GoogleDriveConnector,
        projects_csv: Path,
        store: Store,
        config: SearchConfig,
    ) -> None:
        """All results are Entity objects with valid signals."""
        _ingest_drive(drive_connector, store)
        _ingest_csv(projects_csv, store)

        engine = SearchEngine(store, config)
        results = engine.search("Active")

        assert all(isinstance(r, Entity) for r in results)
        for entity in results:
            assert entity.signals

    def test_results_ranked_in_single_list(
        self,
        drive_connector: GoogleDriveConnector,
        projects_csv: Path,
        store: Store,
        config: SearchConfig,
    ) -> None:
        """Results come back as one list, not segregated by source."""
        _ingest_drive(drive_connector, store)
        _ingest_csv(projects_csv, store)

        engine = SearchEngine(store, config)
        results = engine.search("Active")

        # Results is a single flat list of entities.
        assert isinstance(results, list)
        assert len(results) >= 2

        # Not all results should be from the same source.
        source_ids = [r.source_id for r in results]
        assert len(set(source_ids)) > 1

    def test_shared_term_finds_both_sources(
        self,
        drive_connector: GoogleDriveConnector,
        projects_csv: Path,
        store: Store,
        config: SearchConfig,
    ) -> None:
        """A term present in both sources returns results from each."""
        _ingest_drive(drive_connector, store)
        _ingest_csv(projects_csv, store)

        engine = SearchEngine(store, config)
        # "Data Pipeline" appears in both the Sheet and the CSV.
        results = engine.search("Pipeline")

        assert len(results) >= 2
        source_ids = {r.source_id for r in results}
        assert len(source_ids) == 2

    def test_source_filter_narrows_to_one(
        self,
        drive_connector: GoogleDriveConnector,
        projects_csv: Path,
        store: Store,
        config: SearchConfig,
    ) -> None:
        """Filtering by source returns only results from that source."""
        drive_source = _ingest_drive(drive_connector, store)
        csv_source = _ingest_csv(projects_csv, store)

        engine = SearchEngine(store, config)

        drive_results = engine.search("Active", sources=[drive_source])
        csv_results = engine.search("Active", sources=[csv_source])

        assert all(r.source_id == drive_source for r in drive_results)
        assert all(r.source_id == csv_source for r in csv_results)

    def test_no_duplicates_in_results(
        self,
        drive_connector: GoogleDriveConnector,
        projects_csv: Path,
        store: Store,
        config: SearchConfig,
    ) -> None:
        """Each entity appears at most once in the result list."""
        _ingest_drive(drive_connector, store)
        _ingest_csv(projects_csv, store)

        engine = SearchEngine(store, config)
        results = engine.search("Active")

        entity_ids = [r.id for r in results]
        assert len(entity_ids) == len(set(entity_ids))


class TestCrossSourceFileTypes:
    """Results include entities from multiple Drive file types."""

    def test_sheet_records_appear_in_search(
        self,
        drive_connector: GoogleDriveConnector,
        store: Store,
        config: SearchConfig,
    ) -> None:
        """Sheet rows (auto-indexed) are searchable."""
        _ingest_drive(drive_connector, store)

        engine = SearchEngine(store, config)
        results = engine.search("Website Redesign")

        assert len(results) >= 1
        found = results[0]
        assert found.get_signal_value("field:Project") == "Website Redesign"

    def test_folder_records_appear_in_search(
        self,
        drive_connector: GoogleDriveConnector,
        store: Store,
        config: SearchConfig,
    ) -> None:
        """Folder contents (read as records) are searchable."""
        _ingest_drive(drive_connector, store)

        engine = SearchEngine(store, config)
        results = engine.search("Proposal")

        assert len(results) >= 1
        names = [r.get_signal_value("field:name") for r in results]
        assert any("Proposal" in (n or "") for n in names)

    def test_pdf_metadata_searchable(
        self,
        drive_connector: GoogleDriveConnector,
        store: Store,
        config: SearchConfig,
    ) -> None:
        """PDF file metadata (from folder read) is searchable."""
        _ingest_drive(drive_connector, store)

        engine = SearchEngine(store, config)
        results = engine.search("Invoice")

        assert len(results) >= 1


class TestCrossSourceFolderHierarchy:
    """Folder hierarchy signals survive the full pipeline and appear in search results."""

    def test_folder_signals_on_discovered_entities(
        self,
        drive_connector: GoogleDriveConnector,
        store: Store,
    ) -> None:
        """Files in folders have folder signals after store roundtrip."""
        _ingest_drive(drive_connector, store)

        # The doc should have folder signals from "Projects" parent.
        doc = store.get_entity("collection:doc-proposal")
        assert doc is not None
        folder_signals = [s for s in doc.signals if s.kind == "folder"]
        assert len(folder_signals) >= 1
        assert folder_signals[0].value == "Projects"
        assert doc.get_signal_value("folder_path") == "Projects"

    def test_sheet_has_folder_signals(
        self,
        drive_connector: GoogleDriveConnector,
        store: Store,
    ) -> None:
        """Sheets auto-indexed via Sheets API still get folder hierarchy signals."""
        _ingest_drive(drive_connector, store)

        sheet = store.get_entity("collection:sheet-budget")
        assert sheet is not None
        folder_signals = [s for s in sheet.signals if s.kind == "folder"]
        assert len(folder_signals) >= 1
        assert sheet.get_signal_value("folder_path") == "Projects"


class TestCrossSourceTiers:
    """Tier levels are correct for entities from different sources."""

    def test_drive_doc_is_tier_2(
        self,
        drive_connector: GoogleDriveConnector,
        store: Store,
    ) -> None:
        """Google Docs with extracted text are Tier 2 (has preview)."""
        _ingest_drive(drive_connector, store)

        doc = store.get_entity("collection:doc-proposal")
        assert doc is not None
        assert doc.tier == 2

    def test_drive_non_doc_collections_are_tier_1(
        self,
        drive_connector: GoogleDriveConnector,
        store: Store,
    ) -> None:
        """Non-sheet, non-doc Drive collections are Tier 1 (metadata only)."""
        _ingest_drive(drive_connector, store)

        pdf = store.get_entity("collection:pdf-invoice")
        assert pdf is not None
        assert pdf.tier == 1

    def test_sheet_records_are_tier_3(
        self,
        drive_connector: GoogleDriveConnector,
        store: Store,
    ) -> None:
        """Sheet rows read via Sheets API are Tier 3 (field-level data)."""
        _ingest_drive(drive_connector, store)

        engine = SearchEngine(store, SearchConfig(default_limit=20))
        results = engine.search("Website Redesign")

        sheet_records = [r for r in results if r.type == "record" and "sheet-budget" in r.id]
        assert len(sheet_records) >= 1
        for record in sheet_records:
            assert record.tier == 3

    def test_csv_records_are_tier_3(
        self,
        projects_csv: Path,
        store: Store,
    ) -> None:
        """CSV records are Tier 3."""
        _ingest_csv(projects_csv, store)

        engine = SearchEngine(store, SearchConfig(default_limit=20))
        results = engine.search("Alice")

        assert len(results) >= 1
        for record in results:
            if record.type == "record":
                assert record.tier == 3
