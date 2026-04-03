"""Base connector class and shared exceptions."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from brij.core.models import Entity, Signal

logger = logging.getLogger(__name__)


# --- Exceptions ---


class ConnectorError(Exception):
    """Base exception for all connector errors."""


class AuthenticationError(ConnectorError):
    """Raised when authentication fails."""


class EntityNotFoundError(ConnectorError):
    """Raised when a requested entity does not exist."""


class WriteError(ConnectorError):
    """Raised when a write operation fails."""


# --- Data classes ---


@dataclass
class SyncResult:
    """Result of a connector sync operation."""

    new: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)


# --- Base connector ---


class BaseConnector(ABC):
    """Abstract base class for all Brij connectors.

    Subclasses must implement authenticate, discover, read, write, and sync.
    """

    @abstractmethod
    def authenticate(self, credentials: dict) -> None:
        """Authenticate with the data source.

        Args:
            credentials: Source-specific credentials dictionary.

        Raises:
            AuthenticationError: If authentication fails.
        """

    @abstractmethod
    def discover(self) -> list[Entity]:
        """Discover entities available in the data source.

        Returns:
            List of discovered entities.
        """

    @abstractmethod
    def read(self, entity_id: str) -> list[Signal]:
        """Read signals for a specific entity.

        Args:
            entity_id: The ID of the entity to read.

        Returns:
            List of signals for the entity.

        Raises:
            EntityNotFoundError: If the entity does not exist.
        """

    @abstractmethod
    def write(self, entity_id: str, data: dict) -> bool:
        """Write data to an entity in the source.

        Args:
            entity_id: The ID of the entity to write to.
            data: The data to write.

        Returns:
            True if the write succeeded.

        Raises:
            WriteError: If the write operation fails.
        """

    @abstractmethod
    def sync(self) -> SyncResult:
        """Synchronize with the data source.

        Returns:
            SyncResult with lists of new, modified, and deleted entity IDs.
        """

    def create_collection(self, name: str, schema: dict) -> Entity:
        """Create a new collection in the data source.

        Args:
            name: Name of the collection.
            schema: Schema definition for the collection.

        Returns:
            The created collection entity.

        Raises:
            NotImplementedError: If the connector does not support collection creation.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support collection creation"
        )

    def get_sync_state(self) -> dict[str, str]:
        """Return the current sync state for persistence.

        Subclasses should override to return connector-specific state
        (e.g. change tokens, last-modified timestamps).
        """
        return {}

    def set_sync_state(self, state: dict[str, str]) -> None:
        """Load persisted sync state into the connector.

        Subclasses should override to restore connector-specific state.
        """

    @staticmethod
    def make_entity_id(entity_type: str, source_specific_id: str) -> str:
        """Generate a formatted entity ID string.

        Args:
            entity_type: The type of entity (e.g. "collection", "record").
            source_specific_id: The source-specific identifier.

        Returns:
            Formatted ID string in the form "type:source_specific_id".
        """
        return f"{entity_type}:{source_specific_id}"
