"""Tests for Entity and Signal data models."""

import pytest

from brij.core.models import Entity, Signal


class TestSignalValidation:
    def test_valid_signal(self):
        signal = Signal(kind="name", value="Alice")
        assert signal.kind == "name"
        assert signal.value == "Alice"
        assert signal.confidence == 1.0
        assert signal.origin == "source"

    def test_empty_kind_rejected(self):
        with pytest.raises(ValueError, match="kind must not be empty"):
            Signal(kind="", value="test")

    def test_invalid_origin_rejected(self):
        with pytest.raises(ValueError, match="Invalid signal origin"):
            Signal(kind="name", value="Alice", origin="magic")

    def test_valid_origins(self):
        for origin in ("source", "inferred", "generated", "user"):
            signal = Signal(kind="name", value="test", origin=origin)
            assert signal.origin == origin

    def test_confidence_too_low(self):
        with pytest.raises(ValueError, match="confidence must be between"):
            Signal(kind="name", value="Alice", confidence=-0.1)

    def test_confidence_too_high(self):
        with pytest.raises(ValueError, match="confidence must be between"):
            Signal(kind="name", value="Alice", confidence=1.1)

    def test_confidence_boundaries(self):
        Signal(kind="name", value="Alice", confidence=0.0)
        Signal(kind="name", value="Alice", confidence=1.0)


class TestEntityValidation:
    def test_valid_entity(self):
        entity = Entity(id="e1", type="record", source_id="s1")
        assert entity.id == "e1"
        assert entity.type == "record"
        assert entity.signals == []

    def test_empty_id_rejected(self):
        with pytest.raises(ValueError, match="id must not be empty"):
            Entity(id="", type="record", source_id="s1")

    def test_invalid_type_rejected(self):
        with pytest.raises(ValueError, match="Invalid entity type"):
            Entity(id="e1", type="invalid", source_id="s1")

    def test_valid_types(self):
        for t in ("source", "collection", "record", "field", "cluster"):
            entity = Entity(id="e1", type=t, source_id="s1")
            assert entity.type == t

    def test_empty_source_id_rejected(self):
        with pytest.raises(ValueError, match="source_id must not be empty"):
            Entity(id="e1", type="record", source_id="")


class TestEntityTier:
    def test_tier_1_metadata_only(self):
        entity = Entity(id="e1", type="record", source_id="s1")
        assert entity.tier == 1

    def test_tier_1_with_name_signal(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[Signal(kind="name", value="Alice")],
        )
        assert entity.tier == 1

    def test_tier_2_with_preview(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[Signal(kind="preview", value="Some preview text")],
        )
        assert entity.tier == 2

    def test_tier_2_with_summary(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[Signal(kind="summary", value="A summary")],
        )
        assert entity.tier == 2

    def test_tier_3_with_field_signals(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[Signal(kind="field:email", value="alice@example.com")],
        )
        assert entity.tier == 3

    def test_tier_progresses_as_signals_added(self):
        entity = Entity(id="e1", type="record", source_id="s1")
        assert entity.tier == 1

        entity.signals.append(Signal(kind="summary", value="A summary"))
        assert entity.tier == 2

        entity.signals.append(Signal(kind="field:name", value="Alice"))
        assert entity.tier == 3

    def test_field_takes_priority_over_summary(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[
                Signal(kind="summary", value="A summary"),
                Signal(kind="field:email", value="alice@example.com"),
            ],
        )
        assert entity.tier == 3


class TestEntityConvenience:
    def test_name_property(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[Signal(kind="name", value="Alice")],
        )
        assert entity.name == "Alice"

    def test_name_property_missing(self):
        entity = Entity(id="e1", type="record", source_id="s1")
        assert entity.name is None

    def test_summary_property(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[Signal(kind="summary", value="A summary")],
        )
        assert entity.summary == "A summary"

    def test_summary_property_missing(self):
        entity = Entity(id="e1", type="record", source_id="s1")
        assert entity.summary is None

    def test_get_signals(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[
                Signal(kind="name", value="Alice"),
                Signal(kind="name", value="Ali"),
                Signal(kind="email", value="alice@example.com"),
            ],
        )
        names = entity.get_signals("name")
        assert len(names) == 2
        assert names[0].value == "Alice"
        assert names[1].value == "Ali"

    def test_get_signals_empty(self):
        entity = Entity(id="e1", type="record", source_id="s1")
        assert entity.get_signals("name") == []

    def test_get_signal_value(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[Signal(kind="email", value="alice@example.com")],
        )
        assert entity.get_signal_value("email") == "alice@example.com"

    def test_get_signal_value_returns_first(self):
        entity = Entity(
            id="e1", type="record", source_id="s1",
            signals=[
                Signal(kind="name", value="Alice"),
                Signal(kind="name", value="Ali"),
            ],
        )
        assert entity.get_signal_value("name") == "Alice"

    def test_get_signal_value_missing(self):
        entity = Entity(id="e1", type="record", source_id="s1")
        assert entity.get_signal_value("name") is None
