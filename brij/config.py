"""Configuration module for Brij."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

DEFAULT_BRIJ_DIR = Path.home() / ".brij"


@dataclass
class SearchConfig:
    """Configuration for search behavior."""

    semantic_weight: float = 0.7
    keyword_weight: float = 0.3
    default_limit: int = 5
    max_response_tokens: int = 2000


@dataclass
class EnrichmentConfig:
    """Configuration for AI enrichment."""

    enabled: bool = False
    provider: str = "anthropic"
    model: str = "claude-sonnet-4-5-20241022"


@dataclass
class Config:
    """Top-level Brij configuration.

    Config lives at ~/.brij/config.yaml by default.
    """

    brij_dir: Path = field(default_factory=lambda: DEFAULT_BRIJ_DIR)
    search: SearchConfig = field(default_factory=SearchConfig)
    enrichment: EnrichmentConfig = field(default_factory=EnrichmentConfig)

    @property
    def db_path(self) -> Path:
        """Path to the SQLite database file."""
        return self.brij_dir / "brij.db"

    @classmethod
    def load(cls, brij_dir: Path | None = None) -> Config:
        """Load configuration from YAML file, returning defaults if it doesn't exist."""
        brij_dir = brij_dir or DEFAULT_BRIJ_DIR
        config_path = brij_dir / "config.yaml"

        if not config_path.exists():
            logger.debug("No config file at %s, using defaults", config_path)
            return cls(brij_dir=brij_dir)

        logger.debug("Loading config from %s", config_path)
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}

        search_data = data.get("search", {})
        enrichment_data = data.get("enrichment", {})

        return cls(
            brij_dir=brij_dir,
            search=SearchConfig(**search_data),
            enrichment=EnrichmentConfig(**enrichment_data),
        )

    def save(self) -> None:
        """Write configuration to YAML file."""
        self.brij_dir.mkdir(parents=True, exist_ok=True)
        config_path = self.brij_dir / "config.yaml"

        data = {
            "search": asdict(self.search),
            "enrichment": asdict(self.enrichment),
        }

        logger.debug("Saving config to %s", config_path)
        with open(config_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
