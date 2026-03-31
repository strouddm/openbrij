# Product: What We're Building

Brij is an open-source personal data connectivity layer for AI agents, built on the Model Context Protocol (MCP).

## MVP Scope (v0.1)

- `BaseConnector` abstract class defining the connector interface
- SQLite storage layer for local data persistence
- One reference connector (e.g., local file system) proving the pattern works
- MCP server that exposes registered connectors to agents
- pytest suite covering all of the above

## Connector Interface Contract

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class ConnectorResult:
    """Standard return type for connector operations."""
    data: list[dict[str, Any]]
    source: str
    cursor: str | None = None  # For pagination


class BaseConnector(ABC):
    """Every connector implements this interface."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""

    @abstractmethod
    def fetch(self, query: str | None = None) -> ConnectorResult:
        """Retrieve data. Optional query for filtering."""

    @abstractmethod
    def disconnect(self) -> None:
        """Clean up resources."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique connector identifier (e.g., 'gmail', 'calendar')."""
```

## Data Rules

1. Data stays local (SQLite) unless the user explicitly configures otherwise.
2. No telemetry, no phone-home, no silent data transmission.
3. Credentials live in memory or user config — never in code, logs, or commits.
4. All data access logged at DEBUG with structured context. No PII in logs.
5. Fetch what you need. Do not cache speculatively.

## Out of Scope (for now)

- OAuth/auth provider flows
- Cloud or remote storage backends
- Web UI or CLI interface
- Plugin marketplace or registry
- Async connector interface (revisit when needed)
