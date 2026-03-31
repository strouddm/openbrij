"""Core data models: Entity and Signal."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

VALID_ENTITY_TYPES = frozenset({"source", "collection", "record", "field", "cluster"})
VALID_SIGNAL_ORIGINS = frozenset({"source", "inferred", "generated", "user"})


@dataclass
class Signal:
    """A single piece of information attached to an Entity."""

    kind: str
    value: str
    confidence: float = 1.0
    origin: str = "source"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not self.kind:
            raise ValueError("Signal kind must not be empty")
        if self.origin not in VALID_SIGNAL_ORIGINS:
            raise ValueError(
                f"Invalid signal origin '{self.origin}'. "
                f"Must be one of: {', '.join(sorted(VALID_SIGNAL_ORIGINS))}"
            )
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError(
                f"Signal confidence must be between 0.0 and 1.0, got {self.confidence}"
            )


@dataclass
class Entity:
    """A node in the Brij data graph."""

    id: str
    type: str
    source_id: str
    parent_id: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    signals: list[Signal] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("Entity id must not be empty")
        if self.type not in VALID_ENTITY_TYPES:
            raise ValueError(
                f"Invalid entity type '{self.type}'. "
                f"Must be one of: {', '.join(sorted(VALID_ENTITY_TYPES))}"
            )
        if not self.source_id:
            raise ValueError("Entity source_id must not be empty")

    @property
    def tier(self) -> int:
        """Determine the data tier of this entity.

        Tier 3: has field:* signals (full field-level data).
        Tier 2: has preview or summary signals.
        Tier 1: metadata only.
        """
        for signal in self.signals:
            if signal.kind.startswith("field:"):
                return 3
        for signal in self.signals:
            if signal.kind in ("preview", "summary"):
                return 2
        return 1

    @property
    def name(self) -> str | None:
        """Return the value of the 'name' signal, if present."""
        return self.get_signal_value("name")

    @property
    def summary(self) -> str | None:
        """Return the value of the 'summary' signal, if present."""
        return self.get_signal_value("summary")

    def get_signals(self, kind: str) -> list[Signal]:
        """Return all signals matching the given kind."""
        return [s for s in self.signals if s.kind == kind]

    def get_signal_value(self, kind: str) -> str | None:
        """Return the value of the first signal matching the given kind, or None."""
        for signal in self.signals:
            if signal.kind == kind:
                return signal.value
        return None
