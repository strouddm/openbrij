"""Tests for the config module."""

from pathlib import Path

import pytest

from brij.config import Config, EnrichmentConfig, SearchConfig


class TestConfigDefaults:
    """Test that default configuration values are correct."""

    def test_load_returns_defaults_when_no_file_exists(self, tmp_path: Path) -> None:
        config = Config.load(tmp_path)

        assert config.brij_dir == tmp_path
        assert config.search.semantic_weight == 0.7
        assert config.search.keyword_weight == 0.3
        assert config.search.default_limit == 5
        assert config.search.max_response_tokens == 2000
        assert config.enrichment.enabled is False
        assert config.enrichment.provider == "anthropic"
        assert config.enrichment.model == "claude-sonnet-4-5-20241022"

    def test_db_path_is_under_brij_dir(self, tmp_path: Path) -> None:
        config = Config.load(tmp_path)

        assert config.db_path == tmp_path / "brij.db"


class TestConfigRoundTrip:
    """Test save and load round-trip."""

    def test_save_then_load_round_trips(self, tmp_path: Path) -> None:
        original = Config(brij_dir=tmp_path)
        original.save()

        loaded = Config.load(tmp_path)

        assert loaded.search.semantic_weight == original.search.semantic_weight
        assert loaded.search.keyword_weight == original.search.keyword_weight
        assert loaded.search.default_limit == original.search.default_limit
        assert loaded.search.max_response_tokens == original.search.max_response_tokens
        assert loaded.enrichment.enabled == original.enrichment.enabled
        assert loaded.enrichment.provider == original.enrichment.provider
        assert loaded.enrichment.model == original.enrichment.model

    def test_custom_values_are_preserved(self, tmp_path: Path) -> None:
        original = Config(
            brij_dir=tmp_path,
            search=SearchConfig(
                semantic_weight=0.5,
                keyword_weight=0.5,
                default_limit=10,
                max_response_tokens=4000,
            ),
            enrichment=EnrichmentConfig(
                enabled=True,
                provider="openai",
                model="gpt-4",
            ),
        )
        original.save()

        loaded = Config.load(tmp_path)

        assert loaded.search.semantic_weight == 0.5
        assert loaded.search.keyword_weight == 0.5
        assert loaded.search.default_limit == 10
        assert loaded.search.max_response_tokens == 4000
        assert loaded.enrichment.enabled is True
        assert loaded.enrichment.provider == "openai"
        assert loaded.enrichment.model == "gpt-4"

    def test_save_creates_brij_dir_if_missing(self, tmp_path: Path) -> None:
        nested = tmp_path / "nested" / "brij"
        config = Config(brij_dir=nested)
        config.save()

        assert (nested / "config.yaml").exists()

    def test_config_file_is_valid_yaml(self, tmp_path: Path) -> None:
        config = Config(brij_dir=tmp_path)
        config.save()

        import yaml

        with open(tmp_path / "config.yaml") as f:
            data = yaml.safe_load(f)

        assert "search" in data
        assert "enrichment" in data
