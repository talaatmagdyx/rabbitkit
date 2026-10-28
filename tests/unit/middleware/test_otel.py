"""Tests for middleware/otel.py — native OpenTelemetry tracing."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
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
        msg = _make_message(headers={"traceparent": f"00-{trace_id}-b7ad6b7169203331-01"})
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

        env = MessageEnvelope(routing_key="orders.created", body=b"x", exchange="events", correlation_id="corr-9")
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


class TestPublishOutcomeOnSpan:
    """broker.publish() returns a failed outcome instead of raising; the
    PRODUCER span must still say it failed."""

    @pytest.mark.parametrize(
        ("status", "reason"),
        [
            (PublishStatus.NOT_SENT, "admission_timeout"),
            (PublishStatus.UNKNOWN, "connection_lost"),
            (PublishStatus.TIMEOUT, "confirm_timeout"),
            (PublishStatus.RETURNED, "returned"),
            (PublishStatus.NACKED, None),
            (PublishStatus.ERROR, "exchange_not_found"),
        ],
    )
    def test_a_failed_outcome_marks_the_span_as_error(self, status: PublishStatus, reason: str | None) -> None:
        mw = OTelTracingMiddleware()
        outcome = PublishOutcome(status=status, reason=reason)
        assert mw.publish_scope(lambda e: outcome, MessageEnvelope(routing_key="rk", body=b"x")) is outcome
        span = _EXPORTER.get_finished_spans()[0]
        assert span.status.status_code == _trace.StatusCode.ERROR
        assert span.status.description == f"publish {status.value}"
        assert span.attributes is not None
        assert span.attributes["messaging.rabbitmq.publish.status"] == status.value
        assert span.attributes["error.type"] == (reason or status.value)
        if reason is not None:
            assert span.attributes["messaging.rabbitmq.publish.reason"] == reason
        else:
            assert "messaging.rabbitmq.publish.reason" not in span.attributes

    async def test_an_ok_outcome_keeps_the_span_unset(self) -> None:
        mw = OTelTracingMiddleware()

        async def confirmed(e: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        await mw.publish_scope_async(confirmed, MessageEnvelope(routing_key="rk", body=b"x"))
        span = _EXPORTER.get_finished_spans()[0]
        assert span.status.status_code == _trace.StatusCode.UNSET
        assert span.attributes is not None
        assert span.attributes["messaging.rabbitmq.publish.status"] == "confirmed"
        assert "error.type" not in span.attributes

    async def test_async_failed_outcome(self) -> None:
        mw = OTelTracingMiddleware()

        async def unknown(e: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.UNKNOWN, reason="connection_lost")

        await mw.publish_scope_async(unknown, MessageEnvelope(routing_key="rk", body=b"x"))
        assert _EXPORTER.get_finished_spans()[0].status.status_code == _trace.StatusCode.ERROR

    def test_a_non_outcome_result_is_left_alone(self) -> None:
        mw = OTelTracingMiddleware()
        mw.publish_scope(lambda e: None, MessageEnvelope(routing_key="rk", body=b"x"))
        span = _EXPORTER.get_finished_spans()[0]
        assert span.attributes is not None
        assert "messaging.rabbitmq.publish.status" not in span.attributes


class TestConsumerLinks:
    def test_link_mode_starts_a_new_trace_linked_to_the_producer(self) -> None:
        producer = OTelTracingMiddleware()
        consumer = OTelTracingMiddleware(consumer_context="link")
        captured: list[MessageEnvelope] = []
        producer.publish_scope(lambda e: captured.append(e), MessageEnvelope(routing_key="rk", body=b"x"))
        consumer.consume_scope(lambda m: "ok", _make_message(headers=dict(captured[0].headers)))
        pub_span, con_span = _EXPORTER.get_finished_spans()
        assert con_span.parent is None
        assert con_span.context.trace_id != pub_span.context.trace_id
        assert [link.context.span_id for link in con_span.links] == [pub_span.context.span_id]

    async def test_link_mode_without_a_producer_context_has_no_link(self) -> None:
        consumer = OTelTracingMiddleware(consumer_context="link")

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        await consumer.consume_scope_async(handler, _make_message(headers={}))
        assert list(_EXPORTER.get_finished_spans()[0].links) == []

    def test_parent_is_the_default(self) -> None:
        assert OTelTracingMiddleware()._consumer_context == "parent"

    def test_an_unknown_mode_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="consumer_context"):
            OTelTracingMiddleware(consumer_context="child")


class TestRetryCopiesKeepTheTrace:
    def test_retry_and_dlq_envelopes_carry_the_original_trace_headers(self) -> None:
        """RetryMiddleware copies the failed message's headers, so a retry or
        DLQ copy keeps the producer's traceparent/tracestate."""
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.core.errors import classify_error
        from rabbitkit.core.types import FailureCategory
        from rabbitkit.middleware.retry import RetryMiddleware

        producer = OTelTracingMiddleware()
        captured: list[MessageEnvelope] = []
        producer.publish_scope(lambda e: captured.append(e), MessageEnvelope(routing_key="rk", body=b"x"))
        trace_headers = {k: v for k, v in captured[0].headers.items() if k in ("traceparent", "tracestate")}
        assert trace_headers

        retry = RetryMiddleware(RetryConfig(max_retries=3))
        message = _make_message(headers={**captured[0].headers, "x-rabbitkit-original-queue": "orders"})
        exc = ValueError("boom")
        classified = classify_error(exc)
        retry_env = retry._build_retry_envelope(message, 0, exc, classified=classified)
        dlq_env = retry._build_dlq_envelope(message, 3, exc, classified, FailureCategory.EXHAUSTED)
        assert dlq_env is not None
        for envelope in (retry_env, dlq_env):
            for key, value in trace_headers.items():
                assert envelope.headers[key] == value
