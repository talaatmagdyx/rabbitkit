"""Tests for core/types.py — enums, MessageEnvelope, PublishOutcome."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from rabbitkit.core.types import (
    AckPolicy,
    ClassifiedError,
    DeduplicationMarkPolicy,
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


class TestDeduplicationMarkPolicy:
    def test_values(self) -> None:
        assert DeduplicationMarkPolicy.ON_SUCCESS == "on_success"
        assert DeduplicationMarkPolicy.ON_START == "on_start"
        assert DeduplicationMarkPolicy.CLAIM == "claim"

    def test_all_values(self) -> None:
        assert len(DeduplicationMarkPolicy) == 3


class TestRejectWithoutDLXPolicy:
    def test_values(self) -> None:
        from rabbitkit.core.types import RejectWithoutDLXPolicy

        assert RejectWithoutDLXPolicy.AUTO_PROVISION == "auto_provision"
        assert RejectWithoutDLXPolicy.ERROR == "error"
        assert RejectWithoutDLXPolicy.DISCARD == "discard"

    def test_all_values(self) -> None:
        from rabbitkit.core.types import RejectWithoutDLXPolicy

        assert len(RejectWithoutDLXPolicy) == 3


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
        assert PublishStatus.SENT == "sent"
        assert PublishStatus.NACKED == "nacked"
        assert PublishStatus.TIMEOUT == "timeout"
        assert PublishStatus.RETURNED == "returned"
        assert PublishStatus.ERROR == "error"

    def test_all_values(self) -> None:
        assert len(PublishStatus) == 6


# ── PublishOutcome ────────────────────────────────────────────────────────


class TestPublishOutcome:
    def test_confirmed_is_ok(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.CONFIRMED)
        assert outcome.ok is True

    def test_sent_is_ok(self) -> None:
        """M4: SENT (fire-and-forget, confirm_delivery=False) is not a
        failure -- but is distinct from CONFIRMED via .status."""
        outcome = PublishOutcome(status=PublishStatus.SENT)
        assert outcome.ok is True
        assert outcome.status != PublishStatus.CONFIRMED

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

    def test_classification_transient_error(self) -> None:
        """Item 5: classify_error() reuse -- a transport-style OSError
        classifies TRANSIENT, without needing a new PublishStatus member."""
        outcome = PublishOutcome(status=PublishStatus.ERROR, error=ConnectionResetError("gone"))
        classified = outcome.classification
        assert classified is not None
        assert classified.severity == ErrorSeverity.TRANSIENT
        assert classified.original is outcome.error

    def test_classification_permanent_error(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.ERROR, error=ValueError("bad body"))
        classified = outcome.classification
        assert classified is not None
        assert classified.severity == ErrorSeverity.PERMANENT

    def test_classification_none_for_non_error_status(self) -> None:
        """Only an ERROR outcome has anything to classify."""
        outcome = PublishOutcome(status=PublishStatus.NACKED)
        assert outcome.classification is None

    def test_classification_none_when_error_status_has_no_exception(self) -> None:
        """An ERROR outcome that (unusually) captured no exception object."""
        outcome = PublishOutcome(status=PublishStatus.ERROR, error=None)
        assert outcome.classification is None

    def test_classification_returns_classified_error_type(self) -> None:
        outcome = PublishOutcome(status=PublishStatus.ERROR, error=RuntimeError("x"))
        assert isinstance(outcome.classification, ClassifiedError)

    def test_raise_for_status_returns_self_when_ok(self) -> None:
        """M1: raise_for_status is a no-op that chains on success."""
        outcome = PublishOutcome(status=PublishStatus.CONFIRMED)
        assert outcome.raise_for_status() is outcome

    def test_raise_for_status_raises_on_failure(self) -> None:
        """M1: opt-in exception carrying the outcome for inspection."""
        from rabbitkit.core.errors import PublishError

        outcome = PublishOutcome(status=PublishStatus.NACKED, routing_key="orders")
        with pytest.raises(PublishError) as exc_info:
            outcome.raise_for_status()
        assert exc_info.value.outcome is outcome

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

    def test_routing_key_over_255_bytes_raises(self) -> None:
        """AMQP shortstr fields are wire-limited to 255 bytes -- catch this at
        construction (every publish path funnels through here) instead of an
        opaque broker error at publish time."""
        with pytest.raises(ValueError, match="255"):
            MessageEnvelope(routing_key="x" * 256, body=b"body")

    def test_exchange_over_255_bytes_raises(self) -> None:
        with pytest.raises(ValueError, match="255"):
            MessageEnvelope(routing_key="rk", body=b"body", exchange="x" * 256)

    def test_routing_key_at_255_bytes_ok(self) -> None:
        MessageEnvelope(routing_key="x" * 255, body=b"body")  # should not raise


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
