"""Tests for core/path.py — named routing-key segments + end-to-end Path() DI.

Regression: msg.path was never populated by any broker, so Path() DI always raised
KeyError in real use (the resolver tests pre-set path and masked it).
"""

from typing import Annotated

from rabbitkit.core.path import extract_path, to_binding_key
from rabbitkit.di.context import Path
from rabbitkit.di.resolver import DIResolver
from rabbitkit.testing import TestBroker


class TestToBindingKey:
    def test_no_named_segments_unchanged(self) -> None:
        assert to_binding_key("events.*.#") == "events.*.#"
        assert to_binding_key("plain.key") == "plain.key"
        assert to_binding_key("") == ""

    def test_named_to_star(self) -> None:
        assert to_binding_key("events.{level}.#") == "events.*.#"
        assert to_binding_key("order.{id}") == "order.*"
        assert to_binding_key("{a}.{b}") == "*.*"

    def test_mixed_named_literal_and_wildcards(self) -> None:
        assert to_binding_key("a.{b}.c.*") == "a.*.c.*"


class TestExtractPath:
    def test_no_named_segments_returns_empty(self) -> None:
        assert extract_path("a.b.c", "a.*.c") == {}
        assert extract_path("anything", "plain") == {}

    def test_single_named(self) -> None:
        assert extract_path("order.42", "order.{id}") == {"id": "42"}

    def test_named_before_hash(self) -> None:
        assert extract_path("events.info.svc.a", "events.{level}.#") == {"level": "info"}

    def test_multiple_named(self) -> None:
        assert extract_path("a.x.b.y", "a.{first}.b.{second}") == {"first": "x", "second": "y"}

    def test_stops_at_hash(self) -> None:
        # nothing is named after '#' (its position is variable)
        assert extract_path("a.b.c.d", "a.{x}.#") == {"x": "b"}

    def test_actual_key_shorter_than_pattern(self) -> None:
        assert extract_path("order", "order.{id}") == {}


class TestPathDIEndToEnd:
    """The bug: a real broker never filled msg.path, so Path() raised KeyError even
    with DI enabled. Proven here through TestBroker, which now extracts named segments
    on dispatch. (Path(), like all DI markers, requires an explicit DIResolver.)"""

    def test_path_di_resolves_from_routing_key(self) -> None:
        seen: dict[str, str] = {}
        broker = TestBroker(di_resolver=DIResolver())

        @broker.subscriber(queue="events", routing_key="events.{level}.#")
        def handle(body: bytes, level: Annotated[str, Path("level")]) -> None:
            seen["level"] = level

        broker.start()
        broker.publish("events", b"{}", routing_key="events.info.svc.a")

        assert seen["level"] == "info"
