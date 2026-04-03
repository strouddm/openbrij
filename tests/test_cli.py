"""Tests for the Brij CLI."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from brij.cli import main
from brij.config import Config
from brij.core.store import Store


@pytest.fixture()
def clients_csv(tmp_path: Path) -> Path:
    content = (
        "name,email,phone\n"
        "Alice,alice@example.com,555-1234\n"
        "Bob,bob@example.com,555-5678\n"
    )
    path = tmp_path / "clients.csv"
    path.write_text(content)
    return path


@pytest.fixture()
def brij_dir(tmp_path: Path) -> Path:
    d = tmp_path / ".brij"
    d.mkdir()
    return d


@pytest.fixture()
def config(brij_dir: Path) -> Config:
    return Config(brij_dir=brij_dir)


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _patch_config(config: Config):
    return patch("brij.cli.Config.load", return_value=config)


class TestConnect:
    def test_connect_csv(
        self, runner: CliRunner, clients_csv: Path, config: Config
    ) -> None:
        with _patch_config(config):
            result = runner.invoke(main, ["connect", "csv_local", "--path", str(clients_csv)])

        assert result.exit_code == 0
        assert "Connected csv_local" in result.output
        assert "2 records stored" in result.output

    def test_connect_unknown_connector(self, runner: CliRunner, config: Config) -> None:
        with _patch_config(config):
            result = runner.invoke(main, ["connect", "nonexistent", "--path", "/tmp/x.csv"])

        assert result.exit_code != 0
        assert "Unknown connector" in result.output

    def test_connect_bad_path(self, runner: CliRunner, config: Config) -> None:
        with _patch_config(config):
            result = runner.invoke(main, ["connect", "csv_local", "--path", "/no/such/file.csv"])

        assert result.exit_code != 0
        assert "Authentication failed" in result.output

    def test_connect_csv_without_path_errors(
        self, runner: CliRunner, config: Config
    ) -> None:
        with _patch_config(config):
            result = runner.invoke(main, ["connect", "csv_local"])

        assert result.exit_code != 0
        assert "--path is required" in result.output

    def test_connect_google_sheets_without_path(
        self, runner: CliRunner, config: Config
    ) -> None:
        """google_sheets should use OAuth and prompt for spreadsheet selection."""
        mock_connector = MagicMock()
        mock_connector.list_spreadsheets.return_value = [
            {"id": "ss1", "name": "Budget", "modifiedTime": ""},
            {"id": "ss2", "name": "Contacts", "modifiedTime": ""},
        ]
        mock_connector.discover.return_value = []

        with _patch_config(config), patch(
            "brij.cli.get_connector", return_value=lambda: mock_connector
        ):
            result = runner.invoke(main, ["connect", "google_sheets"], input="1\n")

        assert result.exit_code == 0
        assert "Budget" in result.output
        assert "Contacts" in result.output
        mock_connector.authenticate.assert_called_once_with({})
        mock_connector.discover.assert_called_once_with(spreadsheet_id="ss1")

    def test_connect_google_sheets_selects_second(
        self, runner: CliRunner, config: Config
    ) -> None:
        """User selects the second spreadsheet from the list."""
        mock_connector = MagicMock()
        mock_connector.list_spreadsheets.return_value = [
            {"id": "ss1", "name": "Budget", "modifiedTime": ""},
            {"id": "ss2", "name": "Contacts", "modifiedTime": ""},
        ]
        mock_connector.discover.return_value = []

        with _patch_config(config), patch(
            "brij.cli.get_connector", return_value=lambda: mock_connector
        ):
            result = runner.invoke(main, ["connect", "google_sheets"], input="2\n")

        assert result.exit_code == 0
        mock_connector.discover.assert_called_once_with(spreadsheet_id="ss2")

    def test_connect_google_sheets_no_spreadsheets(
        self, runner: CliRunner, config: Config
    ) -> None:
        """When no spreadsheets are found, exit early."""
        mock_connector = MagicMock()
        mock_connector.list_spreadsheets.return_value = []

        with _patch_config(config), patch(
            "brij.cli.get_connector", return_value=lambda: mock_connector
        ):
            result = runner.invoke(main, ["connect", "google_sheets"])

        assert result.exit_code == 0
        assert "No spreadsheets found" in result.output
        mock_connector.discover.assert_not_called()

    def test_connect_google_sheets_with_path(
        self, runner: CliRunner, config: Config
    ) -> None:
        """google_sheets with --path should still prompt for selection."""
        mock_connector = MagicMock()
        mock_connector.list_spreadsheets.return_value = [
            {"id": "ss1", "name": "Budget", "modifiedTime": ""},
        ]
        mock_connector.discover.return_value = []

        with _patch_config(config), patch(
            "brij.cli.get_connector", return_value=lambda: mock_connector
        ):
            result = runner.invoke(
                main,
                ["connect", "google_sheets", "--path", "/tmp/creds.json"],
                input="1\n",
            )

        assert result.exit_code == 0
        mock_connector.authenticate.assert_called_once_with(
            {"path": "/tmp/creds.json"}
        )
        mock_connector.discover.assert_called_once_with(spreadsheet_id="ss1")


class TestStatus:
    def test_status_no_db(self, runner: CliRunner, config: Config) -> None:
        with _patch_config(config):
            result = runner.invoke(main, ["status"])

        assert result.exit_code == 0
        assert "No database found" in result.output

    def test_status_no_sources(self, runner: CliRunner, config: Config) -> None:
        # Create the DB but don't add any sources.
        Store(config.db_path).close()

        with _patch_config(config):
            result = runner.invoke(main, ["status"])

        assert result.exit_code == 0
        assert "No connected sources" in result.output

    def test_status_with_source(
        self, runner: CliRunner, clients_csv: Path, config: Config
    ) -> None:
        # First connect, then check status.
        with _patch_config(config):
            runner.invoke(main, ["connect", "csv_local", "--path", str(clients_csv)])
            result = runner.invoke(main, ["status"])

        assert result.exit_code == 0
        assert "Sources: 1" in result.output
        assert "csv_local" in result.output
        assert "collection:" in result.output or "record:" in result.output


class TestSearch:
    def test_search_no_db(self, runner: CliRunner, config: Config) -> None:
        with _patch_config(config):
            result = runner.invoke(main, ["search", "Alice"])

        assert result.exit_code == 0
        assert "No database found" in result.output

    def test_search_returns_results(
        self, runner: CliRunner, clients_csv: Path, config: Config
    ) -> None:
        with _patch_config(config):
            runner.invoke(main, ["connect", "csv_local", "--path", str(clients_csv)])
            result = runner.invoke(main, ["search", "Alice"])

        assert result.exit_code == 0
        assert "Alice" in result.output


class TestServe:
    def test_serve_invokes_mcp(self, runner: CliRunner) -> None:
        with patch("brij.mcp.server.create_server") as mock_create:
            mock_server = mock_create.return_value
            mock_server.run.return_value = None

            result = runner.invoke(main, ["serve"])

        assert result.exit_code == 0
        mock_create.assert_called_once()
        mock_server.run.assert_called_once()
