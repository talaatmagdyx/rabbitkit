"""Tests for core/types.py — enums, MessageEnvelope, PublishOutcome."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from rabbitkit.core.types import (
    AckPolicy,
    ClassifiedError,
    ErrorSeverity,
    ExchangeType,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    QueueType,
    TopologyMode,
)

# ── Enum values ───────────────────────────────────────────────────────────


class TestExchangeType:
    def test_values(self) -> None:
        assert ExchangeType.DIRECT == "direct"
        assert ExchangeType.FANOUT == "fanout"
        assert ExchangeType.TOPIC == "topic"
        assert ExchangeType.HEADERS == "headers"

    def test_all_values(self) -> None:
        assert len(ExchangeType) == 4


class TestQueueType:
    def test_values(self) -> None:
        assert QueueType.CLASSIC == "classic"
        assert QueueType.QUORUM == "quorum"
        assert QueueType.STREAM == "stream"

    def test_all_values(self) -> None:
        assert len(QueueType) == 3


class TestAckPolicy:
    def test_values(self) -> None:
        assert AckPolicy.AUTO == "auto"
        assert AckPolicy.MANUAL == "manual"
        assert AckPolicy.NACK_ON_ERROR == "nack_on_error"
        assert AckPolicy.ACK_FIRST == "ack_first"

    def test_all_values(self) -> None:
        assert len(AckPolicy) == 4


class TestTopologyMode:
    def test_values(self) -> None:
        assert TopologyMode.AUTO_DECLARE == "auto_declare"
        assert TopologyMode.PASSIVE_ONLY == "passive_only"
        assert TopologyMode.MANUAL == "manual"

    def test_all_values(self) -> None:
        assert len(TopologyMode) == 3


class TestErrorSeverity:
    def test_values(self) -> None:
        assert ErrorSeverity.TRANSIENT == "transient"
        assert ErrorSeverity.PERMANENT == "permanent"

    def test_all_values(self) -> None:
        assert len(ErrorSeverity) == 2


class TestPublishStatus:
    def test_values(self) -> None:
        assert PublishStatus.CONFIRMED == "confirmed"
        assert PublishStatus.NACKED == "nacked"
        assert PublishStatus.TIMEOUT == "timeout"
        assert PublishStatus.RETURNED == "returned"
        assert PublishStatus.ERROR == "error"

    def test_all_values(self) -> None:
        assert len(PublishStatus) == 5


# ── PublishOutcome ────────────────────────────────────────────────────────


class TestPublishOutcome:
    def test_confirmed_is_ok(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.CONFIRMED)
        assert outcome.ok is True

    def test_nacked_is_not_ok(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.NACKED)
        assert outcome.ok is False

    def test_timeout_is_not_ok(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.TIMEOUT)
        assert outcome.ok is False

    def test_returned_is_not_ok(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.RETURNED)
        assert outcome.ok is False

    def test_error_is_not_ok(self) -> None:
        err = RuntimeError("fail")
        outcome = PublishOutcome(status=PublishStatus.ERROR, error=err)
        assert outcome.ok is False
        assert outcome.error is err

    def test_defaults(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.CONFIRMED)
        assert outcome.delivery_tag is None
        assert outcome.exchange == ""
        assert outcome.routing_key == ""
        assert outcome.error is None

    def test_timestamp_is_utc(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.CONFIRMED)
        assert outcome.timestamp.tzinfo == UTC

    def test_with_delivery_tag(self) -> None:
        outcome = PublishOutcome(
            status=PublishStatus.CONFIRMED,
            delivery_tag=42,
            exchange="test",
            routing_key="orders.created",
        )
        assert outcome.delivery_tag == 42
        assert outcome.exchange == "test"
        assert outcome.routing_key == "orders.created"

    def test_frozen(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.CONFIRMED)
        with pytest.raises(AttributeError):
            outcome.status = PublishStatus.NACKED  # type: ignore[misc]


# ── MessageEnvelope ───────────────────────────────────────────────────────


class TestMessageEnvelope:
    def test_required_fields(self) -> None:
        envelope = MessageEnvelope(routing_key="orders.created", body=b'{"id": 1}')
        assert envelope.routing_key == "orders.created"
        assert envelope.body == b'{"id": 1}'

    def test_defaults(self) -> None:
        envelope = MessageEnvelope(routing_key="rk", body=b"body")
        assert envelope.exchange == ""
        assert envelope.headers == {}
        assert envelope.correlation_id is None
        assert envelope.reply_to is None
        assert envelope.timestamp is None
        assert envelope.content_type == "application/json"
        assert envelope.content_encoding is None
        assert envelope.expiration is None
        assert envelope.priority is None
        assert envelope.mandatory is False
        assert envelope.delivery_mode == 2
        assert envelope.type is None
        assert envelope.user_id is None
        assert envelope.app_id is None

    def test_message_id_auto_generated(self) -> None:
        e1 = MessageEnvelope(routing_key="rk", body=b"body")
        e2 = MessageEnvelope(routing_key="rk", body=b"body")
        assert e1.message_id != e2.message_id
        assert len(e1.message_id) == 36  # UUID format

    def test_custom_message_id(self) -> None:
        envelope = MessageEnvelope(routing_key="rk", body=b"body", message_id="custom-123")
        assert envelope.message_id == "custom-123"

    def test_with_headers(self) -> None:
        envelope = MessageEnvelope(
            routing_key="rk",
            body=b"body",
            headers={"x-tenant": "acme", "x-version": "2"},
        )
        assert envelope.headers["x-tenant"] == "acme"

    def test_frozen(self) -> None:
        envelope = MessageEnvelope(routing_key="rk", body=b"body")
        with pytest.raises(AttributeError):
            envelope.routing_key = "other"  # type: ignore[misc]

    def test_full_construction(self) -> None:
        now = datetime.now(UTC)
        envelope = MessageEnvelope(
            routing_key="orders.created",
            body=b'{"id": 1}',
            exchange="orders",
            headers={"x-tenant": "acme"},
            message_id="msg-1",
            correlation_id="corr-1",
            reply_to="amq.rabbitmq.reply-to",
            timestamp=now,
            content_type="application/json",
            content_encoding="gzip",
            expiration="60000",
            priority=5,
            mandatory=True,
            delivery_mode=2,
            type="OrderCreated",
            user_id="guest",
            app_id="rabbitkit",
        )
        assert envelope.exchange == "orders"
        assert envelope.correlation_id == "corr-1"
        assert envelope.reply_to == "amq.rabbitmq.reply-to"
        assert envelope.timestamp == now
        assert envelope.content_encoding == "gzip"
        assert envelope.expiration == "60000"
        assert envelope.priority == 5
        assert envelope.mandatory is True
        assert envelope.type == "OrderCreated"
        assert envelope.user_id == "guest"
        assert envelope.app_id == "rabbitkit"


# ── ClassifiedError ───────────────────────────────────────────────────────


class TestClassifiedError:
    def test_transient(self) -> None:
        exc = ConnectionResetError("reset")
        classified = ClassifiedError(
            severity=ErrorSeverity.TRANSIENT,
            original=exc,
            reason="connection error",
        )
        assert classified.severity == ErrorSeverity.TRANSIENT
        assert classified.original is exc
        assert classified.reason == "connection error"

    def test_permanent(self) -> None:
        exc = ValueError("bad data")
        classified = ClassifiedError(
            severity=ErrorSeverity.PERMANENT,
            original=exc,
            reason="validation error",
        )
        assert classified.severity == ErrorSeverity.PERMANENT

    def test_frozen(self) -> None:
        classified = ClassifiedError(
            severity=ErrorSeverity.TRANSIENT,
            original=RuntimeError("x"),
            reason="test",
        )
        with pytest.raises(AttributeError):
            classified.severity = ErrorSeverity.PERMANENT  # type: ignore[misc]
