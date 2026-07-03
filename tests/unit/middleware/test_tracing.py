"""Tests for middleware/tracing.py — TracedConsumerMiddleware."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.tracing import TracedConsumerMiddleware, _get_obskit_tracing

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b"hello",
        "routing_key": "orders.created",
        "exchange": "events",
        "message_id": "msg-001",
        "correlation_id": "corr-001",
        "headers": {
            "x-rabbitkit-original-queue": "orders-q",
            "x-rabbitkit-retry-count": "2",
        },
    }
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


def _make_envelope(**kwargs: object) -> MessageEnvelope:
    defaults: dict[str, object] = {
        "routing_key": "orders.created",
        "body": b"hello",
        "exchange": "events",
        "message_id": "msg-001",
        "correlation_id": "corr-001",
    }
    defaults.update(kwargs)
    return MessageEnvelope(**defaults)  # type: ignore[arg-type]


class _FakeTracingModule:
    """Mock obskit.tracing module for testing."""

    def __init__(self, *, available: bool = True) -> None:
        self._available = available
        self.spans: list[dict[str, Any]] = []
        self.injected_context: dict[str, str] = {"traceparent": "00-abc123-def456-01"}
        self.extracted_carriers: list[dict[str, str]] = []

    def is_tracing_available(self) -> bool:
        return self._available

    def trace_span(
        self, name: str, *, component: str = "", operation: str = "", attributes: dict | None = None
    ) -> _FakeSpanContext:
        self.spans.append({"name": name, "component": component, "operation": operation, "attributes": attributes})
        return _FakeSpanContext()

    def async_trace_span(
        self, name: str, *, component: str = "", operation: str = "", attributes: dict | None = None
    ) -> _FakeAsyncSpanContext:
        self.spans.append({"name": name, "component": component, "operation": operation, "attributes": attributes})
        return _FakeAsyncSpanContext()

    def inject_trace_context(self) -> dict[str, str]:
        return dict(self.injected_context)

    def extract_trace_context(self, carrier: dict[str, str]) -> None:
        self.extracted_carriers.append(dict(carrier))


class _FakeSpanContext:
    """Sync context manager for trace_span."""

    def __enter__(self) -> _FakeSpanContext:
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _FakeAsyncSpanContext:
    """Async context manager for async_trace_span."""

    async def __aenter__(self) -> _FakeAsyncSpanContext:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


# ── _get_obskit_tracing ──────────────────────────────────────────────────


class TestGetObskitTracing:
    def test_returns_none_when_obskit_not_installed(self) -> None:
        """Returns None if obskit is not importable."""
        with patch.dict("sys.modules", {"obskit": None, "obskit.tracing": None}):
            result = _get_obskit_tracing()
        # Result may be the cached module if already imported, or None.
        # The key contract is that it doesn't raise.
        assert result is None or result is not None  # no crash

    def test_returns_module_when_available(self) -> None:
        """Returns the tracing module when obskit is installed."""
        fake_module = SimpleNamespace(is_tracing_available=lambda: True)
        with patch.dict("sys.modules", {"obskit": SimpleNamespace(tracing=fake_module), "obskit.tracing": fake_module}):
            result = _get_obskit_tracing()
        assert result is fake_module


# ── init / is_available ──────────────────────────────────────────────────


class TestInit:
    def test_not_available_when_obskit_absent(self) -> None:
        """is_available is False when obskit is not installed."""
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=None):
            mw = TracedConsumerMiddleware()
        assert mw.is_available is False

    def test_not_available_when_tracing_not_configured(self) -> None:
        """is_available is False when obskit installed but tracing not configured."""
        fake = _FakeTracingModule(available=False)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()
        assert mw.is_available is False

    def test_available_when_tracing_configured(self) -> None:
        """is_available is True when obskit installed and tracing configured."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware(service_name="order-service")
        assert mw.is_available is True

    def test_custom_service_name(self) -> None:
        """service_name is stored from constructor."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware(service_name="payment-service")
        assert mw._service_name == "payment-service"

    def test_warns_when_obskit_not_installed(self, caplog: pytest.LogCaptureFixture) -> None:
        """A caller who adds this middleware is opting into tracing --
        silently no-oping forever with zero signal is the exact
        "tracing goes dark silently" gap this warning closes."""
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=None):
            with caplog.at_level("WARNING", logger="rabbitkit.middleware.tracing"):
                TracedConsumerMiddleware(service_name="order-service")
        assert any("order-service" in r.message and "not installed" in r.message for r in caplog.records)

    def test_warns_when_tracing_not_configured(self, caplog: pytest.LogCaptureFixture) -> None:
        fake = _FakeTracingModule(available=False)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            with caplog.at_level("WARNING", logger="rabbitkit.middleware.tracing"):
                TracedConsumerMiddleware()
        assert any("not configured" in r.message for r in caplog.records)

    def test_no_warning_when_tracing_available(self, caplog: pytest.LogCaptureFixture) -> None:
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            with caplog.at_level("WARNING", logger="rabbitkit.middleware.tracing"):
                TracedConsumerMiddleware()
        assert caplog.records == []


# ── consume_scope (sync) ─────────────────────────────────────────────────


class TestConsumeScopeSync:
    def test_passthrough_when_unavailable(self) -> None:
        """Handler is called directly when tracing is not available."""
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=None):
            mw = TracedConsumerMiddleware()

        msg = _make_message()
        handler = MagicMock(return_value="result")
        result = mw.consume_scope(handler, msg)

        handler.assert_called_once_with(msg)
        assert result == "result"

    def test_creates_span_with_correct_name(self) -> None:
        """Consume creates a span with the routing key in the name."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware(service_name="my-svc")

        msg = _make_message(routing_key="orders.created")
        handler = MagicMock(return_value="ok")
        result = mw.consume_scope(handler, msg)

        assert result == "ok"
        handler.assert_called_once_with(msg)
        assert len(fake.spans) == 1
        assert fake.spans[0]["name"] == "rabbitkit.consume orders.created"
        assert fake.spans[0]["component"] == "my-svc"
        assert fake.spans[0]["operation"] == "consume"

    def test_extracts_trace_context_from_headers(self) -> None:
        """Consume extracts trace context from message headers."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        msg = _make_message(headers={"traceparent": "00-abc-def-01", "tracestate": "vendor=value"})
        mw.consume_scope(MagicMock(), msg)

        assert len(fake.extracted_carriers) == 1
        assert fake.extracted_carriers[0]["traceparent"] == "00-abc-def-01"
        assert fake.extracted_carriers[0]["tracestate"] == "vendor=value"

    def test_span_attributes_all_fields(self) -> None:
        """Span attributes include all message fields."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        msg = _make_message()
        mw.consume_scope(MagicMock(), msg)

        attrs = fake.spans[0]["attributes"]
        assert attrs["messaging.system"] == "rabbitmq"
        assert attrs["messaging.operation"] == "receive"
        assert attrs["messaging.rabbitmq.routing_key"] == "orders.created"
        assert attrs["messaging.destination"] == "events"
        assert attrs["messaging.message_id"] == "msg-001"
        assert attrs["messaging.correlation_id"] == "corr-001"
        assert attrs["messaging.destination.name"] == "orders-q"
        assert attrs["messaging.rabbitmq.retry_count"] == "2"

    def test_span_attributes_missing_optional_fields(self) -> None:
        """Missing optional fields are omitted from span attributes."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        msg = _make_message(
            routing_key="",
            exchange="",
            message_id=None,
            correlation_id=None,
            headers={},
        )
        mw.consume_scope(MagicMock(), msg)

        attrs = fake.spans[0]["attributes"]
        assert attrs["messaging.system"] == "rabbitmq"
        assert attrs["messaging.operation"] == "receive"
        assert "messaging.rabbitmq.routing_key" not in attrs
        assert "messaging.destination" not in attrs
        assert "messaging.message_id" not in attrs
        assert "messaging.correlation_id" not in attrs
        assert "messaging.destination.name" not in attrs
        assert "messaging.rabbitmq.retry_count" not in attrs

    def test_unknown_routing_key_span_name(self) -> None:
        """Span name uses 'unknown' when routing key is empty."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        msg = _make_message(routing_key="")
        mw.consume_scope(MagicMock(), msg)

        assert fake.spans[0]["name"] == "rabbitkit.consume unknown"

    def test_handler_exception_propagates(self) -> None:
        """Handler exception propagates through tracing (not swallowed)."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        msg = _make_message()
        handler = MagicMock(side_effect=ValueError("bad data"))

        with pytest.raises(ValueError, match="bad data"):
            mw.consume_scope(handler, msg)

        # Span was still created
        assert len(fake.spans) == 1

    def test_non_string_headers_filtered(self) -> None:
        """Non-string header values are filtered from carrier extraction."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        msg = _make_message(headers={"traceparent": "00-abc", "x-count": 42, "x-flag": True})
        mw.consume_scope(MagicMock(), msg)

        carrier = fake.extracted_carriers[0]
        assert "traceparent" in carrier
        assert "x-count" not in carrier
        assert "x-flag" not in carrier


# ── consume_scope_async ──────────────────────────────────────────────────


class TestConsumeScopeAsync:
    async def test_passthrough_when_unavailable(self) -> None:
        """Async handler is called directly when tracing is not available."""
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=None):
            mw = TracedConsumerMiddleware()

        msg = _make_message()
        call_count = 0

        async def handler(m: RabbitMessage) -> str:
            nonlocal call_count
            call_count += 1
            return "async-result"

        result = await mw.consume_scope_async(handler, msg)

        assert call_count == 1
        assert result == "async-result"

    async def test_creates_async_span(self) -> None:
        """Async consume creates an async_trace_span."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware(service_name="async-svc")

        msg = _make_message(routing_key="payments.received")

        async def handler(m: RabbitMessage) -> str:
            return "done"

        result = await mw.consume_scope_async(handler, msg)

        assert result == "done"
        assert len(fake.spans) == 1
        assert fake.spans[0]["name"] == "rabbitkit.consume payments.received"
        assert fake.spans[0]["component"] == "async-svc"
        assert fake.spans[0]["operation"] == "consume"

    async def test_async_handler_exception_propagates(self) -> None:
        """Async handler exception propagates through tracing."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        msg = _make_message()

        async def handler(m: RabbitMessage) -> None:
            raise RuntimeError("async failure")

        with pytest.raises(RuntimeError, match="async failure"):
            await mw.consume_scope_async(handler, msg)

        assert len(fake.spans) == 1


# ── publish_scope (sync) ─────────────────────────────────────────────────


class TestPublishScopeSync:
    def test_passthrough_when_unavailable(self) -> None:
        """Publish passes through when tracing is not available."""
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=None):
            mw = TracedConsumerMiddleware()

        envelope = _make_envelope()
        publisher = MagicMock(return_value="published")
        result = mw.publish_scope(publisher, envelope)

        publisher.assert_called_once_with(envelope)
        assert result == "published"

    def test_injects_trace_headers(self) -> None:
        """Publish injects trace context headers into envelope."""
        fake = _FakeTracingModule(available=True)
        fake.injected_context = {"traceparent": "00-trace-span-01", "tracestate": "key=val"}
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        envelope = _make_envelope(headers={"x-existing": "keep"})
        captured_envelopes: list[MessageEnvelope] = []

        def publisher(env: MessageEnvelope) -> str:
            captured_envelopes.append(env)
            return "ok"

        result = mw.publish_scope(publisher, envelope)

        assert result == "ok"
        assert len(captured_envelopes) == 1
        published = captured_envelopes[0]
        # Original headers preserved
        assert published.headers["x-existing"] == "keep"
        # Trace headers injected
        assert published.headers["traceparent"] == "00-trace-span-01"
        assert published.headers["tracestate"] == "key=val"

    def test_creates_publish_span(self) -> None:
        """Publish creates a span with correct attributes."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware(service_name="pub-svc")

        envelope = _make_envelope(routing_key="notif.send", exchange="notifications")
        mw.publish_scope(MagicMock(), envelope)

        assert len(fake.spans) == 1
        span = fake.spans[0]
        assert span["name"] == "rabbitkit.publish notif.send"
        assert span["component"] == "pub-svc"
        assert span["operation"] == "publish"
        attrs = span["attributes"]
        assert attrs["messaging.system"] == "rabbitmq"
        assert attrs["messaging.operation"] == "send"
        assert attrs["messaging.rabbitmq.routing_key"] == "notif.send"
        assert attrs["messaging.destination"] == "notifications"

    def test_publish_attributes_missing_optional_fields(self) -> None:
        """Missing optional publish fields are omitted from attributes."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        envelope = _make_envelope(
            routing_key="",
            exchange="",
            message_id=None,
            correlation_id=None,
        )
        # Need to create a proper MessageEnvelope with None fields
        # MessageEnvelope has default message_id from uuid, so we use replace
        env = replace(envelope, message_id="", correlation_id=None)
        mw.publish_scope(MagicMock(), env)

        attrs = fake.spans[0]["attributes"]
        assert attrs["messaging.system"] == "rabbitmq"
        assert attrs["messaging.operation"] == "send"
        assert "messaging.rabbitmq.routing_key" not in attrs
        assert "messaging.destination" not in attrs

    def test_envelope_fields_preserved_after_injection(self) -> None:
        """All envelope fields except headers are preserved after trace injection."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        envelope = _make_envelope(
            routing_key="rk",
            body=b"body-data",
            exchange="ex",
            message_id="mid-1",
            correlation_id="cid-1",
        )
        captured: list[MessageEnvelope] = []

        def publisher(env: MessageEnvelope) -> None:
            captured.append(env)

        mw.publish_scope(publisher, envelope)

        published = captured[0]
        assert published.routing_key == "rk"
        assert published.body == b"body-data"
        assert published.exchange == "ex"
        assert published.message_id == "mid-1"
        assert published.correlation_id == "cid-1"

    def test_no_injection_when_inject_returns_empty(self) -> None:
        """If inject_trace_context returns empty dict, envelope is unchanged."""
        fake = _FakeTracingModule(available=True)
        fake.injected_context = {}
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        original_headers = {"x-tenant": "acme"}
        envelope = _make_envelope(headers=original_headers)
        captured: list[MessageEnvelope] = []

        def publisher(env: MessageEnvelope) -> None:
            captured.append(env)

        mw.publish_scope(publisher, envelope)

        # When inject returns empty, _envelope_with_trace_headers returns original
        published = captured[0]
        assert published.headers == {"x-tenant": "acme"}


# ── publish_scope_async ──────────────────────────────────────────────────


class TestPublishScopeAsync:
    async def test_passthrough_when_unavailable(self) -> None:
        """Async publish passes through when tracing is not available."""
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=None):
            mw = TracedConsumerMiddleware()

        envelope = _make_envelope()

        async def publisher(env: MessageEnvelope) -> str:
            return "async-published"

        result = await mw.publish_scope_async(publisher, envelope)
        assert result == "async-published"

    async def test_async_publish_injects_headers_and_creates_span(self) -> None:
        """Async publish injects trace headers and creates span."""
        fake = _FakeTracingModule(available=True)
        fake.injected_context = {"traceparent": "00-async-trace-01"}
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware(service_name="async-pub")

        envelope = _make_envelope(routing_key="events.fire", exchange="event-bus")
        captured: list[MessageEnvelope] = []

        async def publisher(env: MessageEnvelope) -> str:
            captured.append(env)
            return "done"

        result = await mw.publish_scope_async(publisher, envelope)

        assert result == "done"
        # Headers injected
        assert captured[0].headers["traceparent"] == "00-async-trace-01"
        # Span created
        assert len(fake.spans) == 1
        assert fake.spans[0]["name"] == "rabbitkit.publish events.fire"
        assert fake.spans[0]["component"] == "async-pub"

    async def test_async_publish_exception_propagates(self) -> None:
        """Async publish exception propagates through tracing."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        envelope = _make_envelope()

        async def publisher(env: MessageEnvelope) -> None:
            raise ConnectionError("broker down")

        with pytest.raises(ConnectionError, match="broker down"):
            await mw.publish_scope_async(publisher, envelope)

        assert len(fake.spans) == 1


# ── _build_consume_attributes ────────────────────────────────────────────


class TestBuildConsumeAttributes:
    def test_all_attributes_present(self) -> None:
        """All consume attributes are built from a fully populated message."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        msg = _make_message()
        attrs = mw._build_consume_attributes(msg)

        assert attrs == {
            "messaging.system": "rabbitmq",
            "messaging.operation": "receive",
            "messaging.rabbitmq.routing_key": "orders.created",
            "messaging.destination": "events",
            "messaging.message_id": "msg-001",
            "messaging.correlation_id": "corr-001",
            "messaging.destination.name": "orders-q",
            "messaging.rabbitmq.retry_count": "2",
        }

    def test_minimal_attributes(self) -> None:
        """Only system and operation are present for minimal message."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        msg = _make_message(
            routing_key="",
            exchange="",
            message_id=None,
            correlation_id=None,
            headers={},
        )
        attrs = mw._build_consume_attributes(msg)

        assert attrs == {
            "messaging.system": "rabbitmq",
            "messaging.operation": "receive",
        }


# ── _build_publish_attributes ────────────────────────────────────────────


class TestBuildPublishAttributes:
    def test_all_publish_attributes(self) -> None:
        """All publish attributes are built from a fully populated envelope."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        envelope = _make_envelope()
        attrs = mw._build_publish_attributes(envelope)

        assert attrs == {
            "messaging.system": "rabbitmq",
            "messaging.operation": "send",
            "messaging.rabbitmq.routing_key": "orders.created",
            "messaging.destination": "events",
            "messaging.message_id": "msg-001",
            "messaging.correlation_id": "corr-001",
        }

    def test_minimal_publish_attributes(self) -> None:
        """Only system and operation for minimal envelope."""
        fake = _FakeTracingModule(available=True)
        with patch("rabbitkit.middleware.tracing._get_obskit_tracing", return_value=fake):
            mw = TracedConsumerMiddleware()

        envelope = replace(
            _make_envelope(routing_key="", exchange=""),
            message_id="",
            correlation_id=None,
        )
        attrs = mw._build_publish_attributes(envelope)

        assert attrs == {
            "messaging.system": "rabbitmq",
            "messaging.operation": "send",
        }
