"""Tests for core/path.py — named routing-key segments + end-to-end Path() DI.

Regression: msg.path was never populated by any broker, so Path() DI always raised
KeyError in real use (the resolver tests pre-set path and masked it).
"""

from typing import Annotated

from rabbitkit.core.path import extract_path, to_binding_key
from rabbitkit.di.context import Path
from rabbitkit.di.depends import Depends
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
    """Two bugs: (1) no broker filled msg.path, so Path() always raised KeyError;
    (2) DI markers only worked with an explicitly-passed DIResolver. Both fixed —
    DI markers now auto-enable, so a bare broker resolves them."""

    def test_path_di_resolves_without_explicit_resolver(self) -> None:
        seen: dict[str, str] = {}
        broker = TestBroker()  # no di_resolver — auto-detected from the Path() marker

        @broker.subscriber(queue="events", routing_key="events.{level}.#")
        def handle(body: bytes, level: Annotated[str, Path("level")]) -> None:
            seen["level"] = level

        broker.start()
        broker.publish("events", b"{}", routing_key="events.info.svc.a")

        assert seen["level"] == "info"

    def test_depends_di_works_without_explicit_resolver(self) -> None:
        seen: dict[str, str] = {}
        broker = TestBroker()  # no di_resolver

        def get_service() -> str:
            return "svc-1"

        @broker.subscriber(queue="jobs")
        def handle(body: bytes, svc: Annotated[str, Depends(get_service)]) -> None:
            seen["svc"] = svc

        broker.start()
        broker.publish("jobs", b"{}")

        assert seen["svc"] == "svc-1"

    def test_marker_free_handler_still_uses_fast_path(self) -> None:
        """Simple handlers must NOT change behavior: a bare RabbitMessage-typed
        second param still gets the message (the fast fallback), not a DI error."""
        seen: dict[str, object] = {}
        broker = TestBroker()

        @broker.subscriber(queue="plain")
        def handle(body: bytes) -> None:
            seen["body"] = body

        broker.start()
        broker.publish("plain", b"hello")

        assert seen["body"] == b"hello"
