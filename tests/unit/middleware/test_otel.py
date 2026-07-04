"""Tests for middleware/otel.py — native OpenTelemetry tracing."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.otel import OTelTracingMiddleware, _get_otel

pytest.importorskip("opentelemetry.sdk")

from opentelemetry import trace as _trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

# One provider per process (OTel forbids re-set); share an exporter and clear it.
_EXPORTER = InMemorySpanExporter()
_PROVIDER = TracerProvider()
_PROVIDER.add_span_processor(SimpleSpanProcessor(_EXPORTER))
_trace.set_tracer_provider(_PROVIDER)


@pytest.fixture(autouse=True)
def _clear_spans() -> Any:
    _EXPORTER.clear()
    yield
    _EXPORTER.clear()


def _make_message(**kwargs: Any) -> RabbitMessage:
    defaults: dict[str, Any] = {
        "body": b"{}",
        "routing_key": "orders.created",
        "exchange": "events",
        "message_id": "msg-1",
        "correlation_id": "corr-1",
        "headers": {"x-rabbitkit-original-queue": "orders", "x-rabbitkit-retry-count": "2"},
    }
    defaults.update(kwargs)
    return RabbitMessage(**defaults)


class TestNoOpWithoutOtel:
    def test_warns_once_and_passes_through(self, caplog: pytest.LogCaptureFixture) -> None:
        with patch("rabbitkit.middleware.otel._get_otel", return_value=None):
            with caplog.at_level("WARNING", logger="rabbitkit.middleware.otel"):
                mw = OTelTracingMiddleware(service_name="svc")
        assert mw.is_available is False
        assert any("opentelemetry is not installed" in r.message for r in caplog.records)

        msg = _make_message()
        assert mw.consume_scope(lambda m: "ok", msg) == "ok"
        env = MessageEnvelope(routing_key="rk", body=b"x")
        assert mw.publish_scope(lambda e: e, env) is env  # untouched, same object

    async def test_async_passthrough_without_otel(self) -> None:
        with patch("rabbitkit.middleware.otel._get_otel", return_value=None):
            mw = OTelTracingMiddleware()

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        assert await mw.consume_scope_async(handler, _make_message()) == "ok"

        async def pub(e: MessageEnvelope) -> MessageEnvelope:
            return e

        env = MessageEnvelope(routing_key="rk", body=b"x")
        assert await mw.publish_scope_async(pub, env) is env

    def test_get_otel_returns_none_when_missing(self) -> None:
        with patch.dict("sys.modules", {"opentelemetry": None}):
            assert _get_otel() is None


class TestConsumeSpans:
    def test_consumer_span_with_attributes(self) -> None:
        mw = OTelTracingMiddleware()
        mw.consume_scope(lambda m: "ok", _make_message())

        spans = _EXPORTER.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.name == "orders receive"
        assert span.kind == _trace.SpanKind.CONSUMER
        assert span.attributes["messaging.system"] == "rabbitmq"
        assert span.attributes["messaging.destination.name"] == "orders"
        assert span.attributes["messaging.message.id"] == "msg-1"
        assert span.attributes["messaging.message.conversation_id"] == "corr-1"
        assert span.attributes["messaging.rabbitmq.retry_count"] == "2"

    def test_extracts_parent_from_traceparent_header(self) -> None:
        mw = OTelTracingMiddleware()
        trace_id = "0af7651916cd43dd8448eb211c80319c"
        msg = _make_message(
            headers={"traceparent": f"00-{trace_id}-b7ad6b7169203331-01"}
        )
        mw.consume_scope(lambda m: "ok", msg)

        span = _EXPORTER.get_finished_spans()[0]
        assert format(span.context.trace_id, "032x") == trace_id
        assert format(span.parent.span_id, "016x") == "b7ad6b7169203331"

    def test_handler_exception_recorded_and_reraised(self) -> None:
        mw = OTelTracingMiddleware()

        def boom(m: RabbitMessage) -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            mw.consume_scope(boom, _make_message())

        span = _EXPORTER.get_finished_spans()[0]
        assert span.status.status_code == _trace.StatusCode.ERROR
        assert any(e.name == "exception" for e in span.events)

    async def test_async_consumer_span(self) -> None:
        mw = OTelTracingMiddleware()

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(handler, _make_message())
        assert _EXPORTER.get_finished_spans()[0].kind == _trace.SpanKind.CONSUMER

    async def test_async_exception_recorded(self) -> None:
        mw = OTelTracingMiddleware()

        async def boom(m: RabbitMessage) -> None:
            raise RuntimeError("async boom")

        with pytest.raises(RuntimeError):
            await mw.consume_scope_async(boom, _make_message())
        assert _EXPORTER.get_finished_spans()[0].status.status_code == _trace.StatusCode.ERROR

    def test_non_string_header_values_ignored_by_extraction(self) -> None:
        mw = OTelTracingMiddleware()
        msg = _make_message(headers={"x-count": 7, "x-flag": True})
        assert mw.consume_scope(lambda m: "ok", msg) == "ok"  # no crash


class TestPublishSpans:
    def test_producer_span_and_header_injection(self) -> None:
        mw = OTelTracingMiddleware()
        captured: list[MessageEnvelope] = []

        env = MessageEnvelope(
            routing_key="orders.created", body=b"x", exchange="events", correlation_id="corr-9"
        )
        mw.publish_scope(lambda e: captured.append(e), env)

        span = _EXPORTER.get_finished_spans()[0]
        assert span.name == "events send"
        assert span.kind == _trace.SpanKind.PRODUCER
        assert span.attributes is not None
        assert span.attributes["messaging.message.conversation_id"] == "corr-9"
        # Context injected into a COPY -- original envelope untouched (frozen).
        assert "traceparent" in captured[0].headers
        assert "traceparent" not in env.headers
        # Injected traceparent carries the producer span's trace id.
        assert format(span.context.trace_id, "032x") in captured[0].headers["traceparent"]

    def test_publish_consume_round_trip_same_trace(self) -> None:
        """The whole point: publish-side injection -> consume-side extraction
        yields one continuous trace across the broker."""
        mw = OTelTracingMiddleware()
        captured: list[MessageEnvelope] = []

        env = MessageEnvelope(routing_key="orders.created", body=b"x", exchange="events")
        mw.publish_scope(lambda e: captured.append(e), env)

        msg = _make_message(headers=dict(captured[0].headers))
        mw.consume_scope(lambda m: "ok", msg)

        pub_span, con_span = _EXPORTER.get_finished_spans()
        assert con_span.context.trace_id == pub_span.context.trace_id
        assert con_span.parent.span_id == pub_span.context.span_id

    async def test_async_publish_injection(self) -> None:
        mw = OTelTracingMiddleware()
        captured: list[MessageEnvelope] = []

        async def pub(e: MessageEnvelope) -> None:
            captured.append(e)

        await mw.publish_scope_async(pub, MessageEnvelope(routing_key="rk", body=b"x"))
        assert "traceparent" in captured[0].headers

    def test_publish_exception_recorded_and_reraised(self) -> None:
        mw = OTelTracingMiddleware()

        def boom(e: MessageEnvelope) -> None:
            raise ConnectionError("down")

        with pytest.raises(ConnectionError):
            mw.publish_scope(boom, MessageEnvelope(routing_key="rk", body=b"x"))
        assert _EXPORTER.get_finished_spans()[0].status.status_code == _trace.StatusCode.ERROR

    async def test_async_publish_exception_recorded(self) -> None:
        mw = OTelTracingMiddleware()

        async def boom(e: MessageEnvelope) -> None:
            raise ConnectionError("down")

        with pytest.raises(ConnectionError):
            await mw.publish_scope_async(boom, MessageEnvelope(routing_key="rk", body=b"x"))
        assert _EXPORTER.get_finished_spans()[0].status.status_code == _trace.StatusCode.ERROR

    def test_no_injection_when_propagator_produces_nothing(self) -> None:
        mw = OTelTracingMiddleware()
        mw._propagate = MagicMock()
        mw._propagate.inject = lambda carrier: None  # injects nothing
        env = MessageEnvelope(routing_key="rk", body=b"x")
        assert mw._envelope_with_context(env) is env
