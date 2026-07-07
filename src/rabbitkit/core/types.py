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

# AMQP 0-9-1 encodes exchange names, queue names, and routing keys as
# shortstr: a 1-byte length prefix, so 255 bytes is a hard protocol ceiling
# (not a RabbitMQ convention). Exceeding it previously surfaced as an opaque
# frame-encoding error from the client library or a connection-level
# PRECONDITION_FAILED from the broker at declare/publish time, far from the
# line that actually set the oversized value.
AMQP_SHORTSTR_MAX_BYTES = 255


def validate_amqp_shortstr(field_name: str, value: str) -> None:
    """Raise ``ConfigValidationError`` (a ``ValueError`` subclass) if
    ``value`` exceeds the AMQP shortstr limit.

    Length is measured in encoded UTF-8 bytes (the wire unit), not
    characters -- a 255-character string using multi-byte code points can
    already be oversized.
    """
    encoded_len = len(value.encode("utf-8"))
    if encoded_len > AMQP_SHORTSTR_MAX_BYTES:
        # Function-level import: errors.py imports from this module, so the
        # exception type can't be imported at module scope without a cycle.
        from rabbitkit.core.errors import ConfigValidationError

        msg = (
            f"{field_name} is {encoded_len} bytes, exceeding the AMQP shortstr "
            f"limit of {AMQP_SHORTSTR_MAX_BYTES} bytes: {value[:40]!r}..."
        )
        raise ConfigValidationError(msg)


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


class DeduplicationMarkPolicy(str, Enum):
    """When DeduplicationMiddleware records the dedup key.

    - ON_SUCCESS (default): check before the handler (no write), mark only
      after it succeeds. Crash-safe; concurrent duplicates may both process.
    - ON_START: mark before the handler. Blocks concurrent duplicates but a
      crash mid-handler LOSES the message (redelivery is skipped as a
      duplicate). Advanced/dangerous — use only when duplicate execution is
      worse than message loss.
    - CLAIM: two-state — an "in-flight" claim (expires after
      ``processing_timeout``) before the handler, flipped to "completed"
      (full ``ttl``) on success. Blocks concurrent duplicates AND survives
      crashes, provided ``processing_timeout`` comfortably exceeds the
      worst-case handler duration.
    """

    ON_SUCCESS = "on_success"
    ON_START = "on_start"
    CLAIM = "claim"


class RejectWithoutDLXPolicy(str, Enum):
    """What to do when a route can ``reject(requeue=False)`` but its queue
    has no dead-letter exchange (RabbitMQ silently DISCARDS such rejects).

    - AUTO_PROVISION (default): declare ``{queue}.dlq`` and wire the source
      queue's DLX to it (default exchange + queue-name routing, same
      convention as retry topology). Safe by default — a poison message
      lands in the DLQ instead of vanishing.
    - ERROR: refuse to start — raises ``UnsafeTopologyError``. For teams
      that manage topology externally and want unsafe config to fail fast.
    - DISCARD: explicitly allow RabbitMQ to discard rejected messages
      (warns once per route). For low-value/ephemeral workloads only.

    Applied only under ``TopologyMode.AUTO_DECLARE`` — in PASSIVE_ONLY and
    MANUAL modes rabbitkit does not own queue arguments and cannot know
    whether an externally-managed DLX exists.
    """

    AUTO_PROVISION = "auto_provision"
    ERROR = "error"
    DISCARD = "discard"


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

    def raise_for_status(self) -> PublishOutcome:
        """Raise ``PublishError`` if the publish failed; else return self (M1).

        ``broker.publish()`` never raises — it returns this outcome so a
        failed publish (NACKED / TIMEOUT / RETURNED / ERROR) can't be lost by
        code that simply ignores the return value. Callers who prefer
        exceptions opt in::

            broker.publish(envelope).raise_for_status()

        Chains so ``outcome = broker.publish(...).raise_for_status()`` works.
        """
        if not self.ok:
            from rabbitkit.core.errors import PublishError

            raise PublishError(self)
        return self


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

    def __post_init__(self) -> None:
        # Catches an oversized routing_key/exchange at construction time --
        # the same choke point every publish (broker.publish, retry
        # republish, DLQ replay, batch) goes through -- instead of an
        # opaque broker connection error later.
        validate_amqp_shortstr("routing_key", self.routing_key)
        validate_amqp_shortstr("exchange", self.exchange)


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
