"""Core enums and data types — SINGLE CANONICAL LOCATION for all enums.

Every enum, value object, and core data type lives here.
Imported everywhere else — never duplicated.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from rabbitkit.core.message import RabbitMessage

# ── AMQP protocol-level constants ────────────────────────────────────────

# RabbitMQ's "direct reply-to" pseudo-queue (an AMQP protocol feature, not a
# rabbitkit invention). It has two hard broker rules: consuming from it
# requires a no-ack consumer, and the broker rejects any Queue.Declare against
# it (active or passive). A more subtle rule transports must also honor:
# publishing a request with reply_to=DIRECT_REPLY_TO_QUEUE must happen on the
# SAME channel that registered the reply consumer — otherwise the broker
# raises "PRECONDITION_FAILED - fast reply consumer does not exist" on
# publish. Single canonical constant so rpc.py and both transports agree.
DIRECT_REPLY_TO_QUEUE = "amq.rabbitmq.reply-to"


class _RequeuedForRetrySentinel:
    """Sentinel type for :data:`REQUEUED_FOR_RETRY` (H8)."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "REQUEUED_FOR_RETRY"


# H8: returned by RetryMiddleware.consume_scope/consume_scope_async instead of
# ``None`` whenever a handler failure was routed for retry (delay-queue
# publish, or nack(requeue=True) if that publish itself failed) rather than
# actually succeeding. RetryMiddleware swallows the handler's exception in
# this case (by design — an OUTER ExceptionMiddleware must not treat a
# retry-in-progress as a terminal failure), so from an outer middleware's
# point of view, ``call_next(message)`` returns normally either way. That is
# indistinguishable from "the handler ran and returned None" UNLESS the
# outer middleware checks for this sentinel — which matters concretely for
# DeduplicationMiddleware(mark_policy="on_success"): without checking, it
# would mark the message as processed on a failed-then-retried attempt, so
# the later retry redelivery (same dedup key) is dropped as a duplicate and
# never actually processed (silent message loss). Any custom middleware
# wrapping a route that may contain a RetryMiddleware should treat a
# ``call_next`` result identical to this sentinel (``is REQUEUED_FOR_RETRY``)
# as "not yet done, expect another delivery" rather than "succeeded."
REQUEUED_FOR_RETRY = _RequeuedForRetrySentinel()


class AppState(str, Enum):
    """Application lifecycle states.

    Canonical home for this enum is ``core/types.py`` per the project rule that
    ``types.py`` is the SINGLE canonical location for all enums and data types.
    """

    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


class ExchangeType(str, Enum):
    """AMQP exchange types."""

    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"
    HEADERS = "headers"


class QueueType(str, Enum):
    """RabbitMQ queue types."""

    CLASSIC = "classic"
    QUORUM = "quorum"
    STREAM = "stream"


class AckPolicy(str, Enum):
    """Message acknowledgement policies.

    See Contract 1 in the plan for exact semantics:
    - AUTO: success→ack, exception→classify→nack/reject
    - MANUAL: handler owns ack/nack/reject entirely
    - NACK_ON_ERROR: success→ack, exception→nack(requeue=False)
    - ACK_FIRST: ack BEFORE handler runs (at-most-once)
    """

    AUTO = "auto"
    MANUAL = "manual"
    NACK_ON_ERROR = "nack_on_error"
    ACK_FIRST = "ack_first"


class TopologyMode(str, Enum):
    """Topology declaration modes.

    See Contract 6 in the plan for precedence rules:
    - AUTO_DECLARE: declare exchanges/queues/bindings on startup
    - PASSIVE_ONLY: all declarations use passive=True
    - MANUAL: skip all topology operations
    """

    AUTO_DECLARE = "auto_declare"
    PASSIVE_ONLY = "passive_only"
    MANUAL = "manual"


class ErrorSeverity(str, Enum):
    """Error classification severity levels."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"


class PublishStatus(str, Enum):
    """Result status of a publish operation."""

    CONFIRMED = "confirmed"
    #: M4: fire-and-forget publish (PublisherConfig.confirm_delivery=False)
    #: -- written to the socket, but the broker never acknowledged it.
    #: Distinct from CONFIRMED so code that specifically needs a real
    #: broker ack (e.g. deciding whether it's safe to ack/discard a source
    #: message after republishing it, as retry/result publishing do) can
    #: tell the two apart via ``.status`` instead of being told "confirmed"
    #: when nothing was actually confirmed.
    SENT = "sent"
    NACKED = "nacked"
    TIMEOUT = "timeout"
    RETURNED = "returned"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class PublishOutcome:
    """Result of a publish operation."""

    status: PublishStatus
    delivery_tag: int | None = None
    exchange: str = ""
    routing_key: str = ""
    error: BaseException | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def ok(self) -> bool:
        """True if the publish did not fail -- CONFIRMED (broker
        acknowledged it) or SENT (fire-and-forget, confirm_delivery=False --
        written to the socket but never broker-confirmed).

        M4: if you specifically need to know the broker actually confirmed
        the message (e.g. before treating a republish as durable enough to
        settle/discard the original), check ``status ==
        PublishStatus.CONFIRMED`` directly -- ``.ok`` alone can't
        distinguish "confirmed" from "sent, unconfirmed."
        """
        return self.status in (PublishStatus.CONFIRMED, PublishStatus.SENT)


@dataclass(frozen=True, slots=True)
class MessageEnvelope:
    """Outgoing message envelope.

    NOTE: AMQP header values are limited to:
    str, int, float, bool, bytes, datetime, Decimal, list/dict of these, or None.
    Arbitrary Python objects (sets, custom classes) will raise at publish time.
    Transport validates header values before sending.
    """

    routing_key: str
    body: bytes
    exchange: str = ""
    headers: dict[str, Any] = field(default_factory=dict)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str | None = None
    reply_to: str | None = None
    timestamp: datetime | None = None
    content_type: str = "application/json"
    content_encoding: str | None = None
    expiration: str | None = None
    priority: int | None = None
    mandatory: bool = False
    delivery_mode: int = 2  # 1=transient, 2=persistent
    type: str | None = None
    user_id: str | None = None
    app_id: str | None = None


@dataclass(frozen=True, slots=True)
class ClassifiedError:
    """Result of error classification."""

    severity: ErrorSeverity
    original: BaseException
    reason: str


@runtime_checkable
class AckStrategy(Protocol):
    """Settlement strategy for an ``AckPolicy``.

    Each strategy owns the success-path ack and the error-path settlement.
    Handler-raised ``AckMessage`` / ``NackMessage`` / ``RejectMessage`` are
    NOT policy-driven and stay in the pipeline.

    See Contract 1 in the plan for per-policy semantics.
    """

    @property
    def acks_first(self) -> bool:
        """True when the message is acked BEFORE the handler runs (ACK_FIRST)."""
        ...

    def on_success(self, msg: RabbitMessage) -> None:
        """Settle the message after a successful handler invocation."""
        ...

    def on_error(self, msg: RabbitMessage, exc: Exception) -> None:
        """Settle the message after an unhandled handler exception."""
        ...
