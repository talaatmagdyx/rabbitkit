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


class StreamOffsetType(str, Enum):
    """Where a stream consumer starts reading (RabbitMQ ``x-stream-offset``).

    - FIRST: the oldest message still retained by the stream.
    - LAST: the last chunk written (the most recent messages).
    - NEXT: only messages published after the consumer subscribes (the
      broker's default).
    - OFFSET: a numeric offset.
    - TIMESTAMP: the first chunk published at or after a point in time.
    """

    FIRST = "first"
    LAST = "last"
    NEXT = "next"
    OFFSET = "offset"
    TIMESTAMP = "timestamp"


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


class ErrorCategory(str, Enum):
    """What kind of failure a handler error is (the failure taxonomy).

    :class:`ErrorSeverity` still decides retry versus dead-letter; the
    category says *why*, for the ``x-rabbitkit-error-category`` header on
    retry and DLQ copies and as a bounded metric label.

    * ``RETRYABLE``: transient; a retry may succeed (``TimeoutError``,
      ``ConnectionError``/``OSError``, ``EOFError``).
    * ``PERMANENT``: a retry cannot succeed (business rule violation,
      handler bug). The default for unclassified exceptions.
    * ``INVALID_MESSAGE``: the message itself cannot be decoded or
      validated (body decode or validation failure, missing required
      header, unreadable compressed body, bad signature). Terminal.
    * ``UNKNOWN_OUTCOME``: the handler's side effects may or may not have
      happened (a processing deadline expired). Retried; the handler must
      be idempotent.
    * ``INFRASTRUCTURE``: a dependency is unavailable (AMQP client
      connection/channel errors, or an application's
      :class:`~rabbitkit.core.errors.InfrastructureError`). Retried, counted
      separately.
    * ``SHUTDOWN``: cancellation or interpreter shutdown
      (``asyncio.CancelledError``, ``KeyboardInterrupt``, ``SystemExit``).
      Never retried by rabbitkit: the delivery is requeued and the
      cancellation propagates.
    """

    RETRYABLE = "retryable"
    PERMANENT = "permanent"
    INVALID_MESSAGE = "invalid_message"
    UNKNOWN_OUTCOME = "unknown_outcome"
    INFRASTRUCTURE = "infrastructure"
    SHUTDOWN = "shutdown"

    @property
    def severity(self) -> ErrorSeverity:
        """The retry decision this category implies."""
        if self in (ErrorCategory.PERMANENT, ErrorCategory.INVALID_MESSAGE):
            return ErrorSeverity.PERMANENT
        return ErrorSeverity.TRANSIENT


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
    #: No publisher confirm arrived within the confirm timeout (or the
    #: per-call ``timeout``). The frames left the client, so the broker may
    #: or may not have the message: a flavour of :attr:`UNKNOWN`
    #: (``PublishOutcome.is_unknown`` is True for both).
    TIMEOUT = "timeout"
    RETURNED = "returned"
    #: An unexpected, unclassified failure. Every failure the transport can
    #: classify is reported as NOT_SENT, UNKNOWN, TIMEOUT, NACKED or RETURNED
    #: instead; ``PublishOutcome.reason`` says which ERROR it was when known
    #: (e.g. ``exchange_not_found``).
    ERROR = "error"
    #: The message definitely never left the client: admission timed out,
    #: the publisher is closed, no connection or channel was available, or a
    #: flow-control policy dropped it. Safe to publish again.
    NOT_SENT = "not_sent"
    #: The frames may have left the client but no confirm, return or nack
    #: was observed (connection or channel lost mid-publish, shutdown).
    #: Republish only with the SAME ``message_id`` so consumers can
    #: deduplicate; the library never republishes on its own.
    UNKNOWN = "unknown"


class PublishReason:
    """Bounded ``PublishOutcome.reason`` codes (snake_case, safe as a metric label).

    Plain string constants rather than an Enum so ``outcome.reason`` is an
    ordinary ``str`` (``"admission_timeout"``), whatever produced it.
    """

    # NOT_SENT
    ADMISSION_TIMEOUT = "admission_timeout"
    PUBLISHER_CLOSED = "publisher_closed"
    NOT_CONNECTED = "not_connected"
    CHANNEL_UNAVAILABLE = "channel_unavailable"
    EXCEEDS_BYTE_BUDGET = "exceeds_max_in_flight_bytes"
    BACKPRESSURE_DROPPED = "backpressure_dropped"
    #: The caller was cancelled before its message left the client.
    CANCELLED = "cancelled"
    # UNKNOWN
    CONNECTION_LOST = "connection_lost"
    CHANNEL_CLOSED = "channel_closed"
    SHUTDOWN = "shutdown"
    # TIMEOUT
    CONFIRM_TIMEOUT = "confirm_timeout"
    PUBLISH_TIMEOUT = "publish_timeout"
    # RETURNED / NACKED
    RETURNED = "returned"
    NACKED = "nacked"
    # ERROR
    EXCHANGE_NOT_FOUND = "exchange_not_found"
    ACCESS_REFUSED = "access_refused"
    PRECONDITION_FAILED = "precondition_failed"
    INVALID_ENVELOPE = "invalid_envelope"
    PUBLISH_ERROR = "publish_error"

    ALL: frozenset[str] = frozenset(
        {
            ADMISSION_TIMEOUT,
            PUBLISHER_CLOSED,
            NOT_CONNECTED,
            CHANNEL_UNAVAILABLE,
            EXCEEDS_BYTE_BUDGET,
            BACKPRESSURE_DROPPED,
            CONNECTION_LOST,
            CHANNEL_CLOSED,
            SHUTDOWN,
            CANCELLED,
            CONFIRM_TIMEOUT,
            PUBLISH_TIMEOUT,
            RETURNED,
            NACKED,
            EXCHANGE_NOT_FOUND,
            ACCESS_REFUSED,
            PRECONDITION_FAILED,
            INVALID_ENVELOPE,
            PUBLISH_ERROR,
        }
    )


class BulkPublishStatus(str, Enum):
    """Actionable per-item state of a bulk publish (``broker.publish_many``).

    Stricter than :class:`PublishStatus` and deliberately preserves
    uncertainty: ``SENT``, a confirm timeout, and a mid-publish exception all
    become ``UNKNOWN`` (the publish may have reached the broker) rather than
    a success or a definite failure. See :mod:`rabbitkit.core.bulk`.
    """

    CONFIRMED = "confirmed"
    UNROUTABLE = "unroutable"
    NACKED = "nacked"
    INVALID = "invalid"
    NOT_SENT = "not_sent"
    UNKNOWN = "unknown"


class SettlementAction(str, Enum):
    """Which consumer settlement a bulk call / coordinator command performs."""

    ACK = "ack"
    NACK = "nack"
    REJECT = "reject"


class SettlementItemStatus(str, Enum):
    """Per-item outcome of ``ack_many`` / ``nack_many``.

    ``DISPATCHED`` = handed to the transport (a local wire-write boundary).
    RabbitMQ never acknowledges a consumer ack, so no member here can mean
    "broker confirmed".
    """

    DISPATCHED = "dispatched"
    ALREADY_SETTLED = "already_settled"
    DUPLICATE = "duplicate"
    INVALID = "invalid"
    STALE = "stale"
    NOT_ATTEMPTED = "not_attempted"
    FAILED = "failed"


class DeliveryState(str, Enum):
    """Ledger state of one delivery inside a ``SettlementCoordinator``.

    A delivery being *finished* is not the same as being *safe to ack*. Only
    ``SUCCESS`` may advance the cumulative-ack frontier; only ``SUCCESS`` /
    ``NACK`` / ``REJECT`` are emitted at all::

        OUTSTANDING ──┬─> SUCCESS        ack-safe, advances the frontier
                      ├─> NACK           emitted individually, then settled
                      ├─> REJECT         emitted individually, then settled
                      ├─> RETRY_PENDING ─┐ owned elsewhere, blocks the frontier
                      ├─> FAILED         │ blocks the frontier, never emitted
                      └─> CANCELLED      │ blocks the frontier, never emitted
                                         └─> SUCCESS / NACK / REJECT

    ``SUCCESS`` is reachable only from ``OUTSTANDING`` or ``RETRY_PENDING``:
    a delivery whose handler failed or was cancelled can never be acked
    (invariant I3). ``FAILED`` and ``CANCELLED`` deliveries are left unacked
    so the broker redelivers them; ``release()`` drops them from the ledger
    when something else settled them.
    """

    #: Registered, handler still running. Blocks the frontier.
    OUTSTANDING = "outstanding"
    #: Handler succeeded — ack-safe (the plan's ACK_READY).
    SUCCESS = "success"
    #: Handed to the retry/terminal path; it owns settlement. Blocks.
    RETRY_PENDING = "retry_pending"
    #: Handler failed with no settlement decision. Blocks; never emitted.
    FAILED = "failed"
    #: Handler was cancelled (shutdown, timeout). Blocks; never emitted.
    CANCELLED = "cancelled"
    #: Approved for an individual nack.
    NACK = "nack"
    #: Approved for an individual reject.
    REJECT = "reject"

    @property
    def is_ack_safe(self) -> bool:
        """Only this state may advance the cumulative-ack frontier."""
        return self is DeliveryState.SUCCESS

    @property
    def is_emittable(self) -> bool:
        """True when the coordinator may put a frame on the wire for it."""
        return self in (DeliveryState.SUCCESS, DeliveryState.NACK, DeliveryState.REJECT)

    @property
    def blocks_frontier(self) -> bool:
        """True when a cumulative ack may not reach past this delivery."""
        return not self.is_emittable


class FlushReason(str, Enum):
    """Why a batch helper flushed (metrics label)."""

    SIZE = "size"
    INTERVAL = "interval"
    MANUAL = "manual"
    CLOSE = "close"


class ReliabilityProfile(str, Enum):
    """Opt-in reliability profiles (see :mod:`rabbitkit.core.profiles`)."""

    STANDARD = "standard"
    CRITICAL = "critical"


class PreflightStatus(str, Enum):
    """Outcome of one preflight check. ``UNVERIFIED`` is reported, never hidden."""

    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    FAILED = "failed"


class TerminalAction(str, Enum):
    """What ``RetryMiddleware`` does with a message whose failure is terminal
    (permanent, retries exhausted, or ``max_retry_age`` exceeded).

    * ``DEAD_LETTER`` (default): publish a copy to ``<queue>.dlq`` carrying
      the terminal failure metadata (error class, sanitized message,
      category, attempts, timestamps, original queue/exchange/routing key,
      message id), wait for the broker confirm, THEN ack the source. If that
      publish fails, fall back to ``REJECT`` so the message is never lost.
    * ``REJECT``: legacy behaviour — reject ``requeue=False`` and let the
      source queue's dead-letter exchange move it. The DLQ copy then carries
      only the broker's ``x-death`` and the PREVIOUS attempt's headers.
    """

    DEAD_LETTER = "dead_letter"
    REJECT = "reject"


class FailureCategory(str, Enum):
    """Failure category stamped on retry/DLQ copies as
    ``x-rabbitkit-failure-category``.

    ``TRANSIENT`` / ``TIMEOUT_UNKNOWN`` describe a failure that was retried
    (``TIMEOUT_UNKNOWN``: the handler timed out, so whether its side effects
    happened is unknown). ``PERMANENT`` / ``EXHAUSTED`` /
    ``RETRY_AGE_EXCEEDED`` describe why a message went to the DLQ.
    """

    TRANSIENT = "transient"
    TIMEOUT_UNKNOWN = "timeout-unknown"
    PERMANENT = "permanent"
    EXHAUSTED = "exhausted"
    RETRY_AGE_EXCEEDED = "retry-age-exceeded"


class DeadLetterStrategy(str, Enum):
    """Quorum-queue ``x-dead-letter-strategy``.

    ``AT_LEAST_ONCE`` keeps a dead-lettered message on the source until the
    dead-letter target confirms it (requires ``overflow="reject-publish"``;
    RabbitMQ 3.10+). ``AT_MOST_ONCE`` is the broker default: the message is
    dropped if the target is missing or full.
    """

    AT_MOST_ONCE = "at-most-once"
    AT_LEAST_ONCE = "at-least-once"


class HandoffState(str, Enum):
    """Retry-handoff tracker state (see :mod:`rabbitkit.core.retry_handoff`)."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    EXHAUSTED = "exhausted"


def safe_exception_repr(exc: BaseException) -> str:
    """``repr(exc)`` that cannot raise.

    Some transport errors break their own formatting: aiormq 7's
    ``DeliveryError.__str__`` (and ``__repr__``, which calls it) raises
    ``AttributeError`` when the error carries no frame. Anything that formats
    a publish error has to survive that, or the formatting replaces the
    original failure with an unrelated exception.
    """
    try:
        return repr(exc)
    except Exception:
        return f"<{type(exc).__name__} (unprintable)>"


@dataclass(frozen=True, slots=True, repr=False)
class PublishOutcome:
    """Result of a publish operation.

    ``reason`` is a bounded code from :class:`PublishReason` saying why a
    non-success status happened (``None`` when the transport did not
    classify it). ``reply_code`` / ``reply_text`` carry the broker's
    ``basic.return`` fields for a RETURNED publish (e.g. ``312`` /
    ``"NO_ROUTE"``) and the channel-close fields for a refused one.
    """

    status: PublishStatus
    delivery_tag: int | None = None
    exchange: str = ""
    routing_key: str = ""
    error: BaseException | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    reason: str | None = None
    reply_code: int | None = None
    reply_text: str | None = None

    def __repr__(self) -> str:
        # Hand-written so a broken error repr (see safe_exception_repr) can't
        # make the outcome itself unprintable: raise_for_status() builds its
        # message from this, and used to raise AttributeError on aiormq 7.
        error = None if self.error is None else safe_exception_repr(self.error)
        extra = ""
        if self.reason is not None:
            extra += f", reason={self.reason!r}"
        if self.reply_code is not None:
            extra += f", reply_code={self.reply_code!r}, reply_text={self.reply_text!r}"
        return (
            f"PublishOutcome(status={self.status!r}, delivery_tag={self.delivery_tag!r}, "
            f"exchange={self.exchange!r}, routing_key={self.routing_key!r}, error={error}, "
            f"timestamp={self.timestamp!r}{extra})"
        )

    @property
    def is_unknown(self) -> bool:
        """True when the broker may or may not have the message: ``UNKNOWN``
        or ``TIMEOUT`` (a confirm timeout is one kind of unknown outcome).

        Republish only with the same ``message_id`` so consumers can
        deduplicate.
        """
        return self.status in (PublishStatus.UNKNOWN, PublishStatus.TIMEOUT)

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
        failed publish (NACKED / TIMEOUT / RETURNED / ERROR / NOT_SENT /
        UNKNOWN) can't be lost by
        code that simply ignores the return value. Callers who prefer
        exceptions opt in::

            broker.publish(envelope).raise_for_status()

        Chains so ``outcome = broker.publish(...).raise_for_status()`` works.
        """
        if not self.ok:
            from rabbitkit.core.errors import PublishError

            raise PublishError(self)
        return self

    @property
    def classification(self) -> ClassifiedError | None:
        """Best-effort severity/reason for ``self.error``, via the existing
        ``classify_error()`` (production-hardening item 5).

        Deliberately NOT a new ``PublishStatus`` enum member: growing that
        small, closed "what happened" set (CONFIRMED/SENT/NACKED/TIMEOUT/
        RETURNED/ERROR) risks breaking exhaustive match/if-chains callers
        may have written against it, for a "why" signal better served by
        reusing the classification machinery already wired into the
        consume/retry path (``core/errors.py``) rather than duplicating it.
        A lazy import avoids a module-level cycle (``core/errors.py``
        already imports from this module) — same pattern as
        :meth:`raise_for_status`.

        ``None`` when there's nothing to classify: a non-ERROR status, or
        an ERROR outcome that (unusually) captured no exception.
        """
        if self.status is not PublishStatus.ERROR or self.error is None:
            return None
        from rabbitkit.core.errors import classify_error

        return classify_error(self.error)


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
    """Result of error classification.

    ``outcome_unknown`` is True when the failure does not say whether the
    handler's side effects happened -- a processing deadline expired
    (:class:`~rabbitkit.core.errors.HandlerTimeoutError`) while the work may
    have been half done or fully done. Such a failure is still ``TRANSIENT``
    (it is worth retrying), but a redelivery runs the handler again on top of
    whatever the first attempt did, so the handler must be idempotent.

    ``category`` refines ``severity`` (see :class:`ErrorCategory`). When it
    is omitted, or contradicts ``severity``, it is derived from ``severity``
    (``TRANSIENT`` → ``RETRYABLE``, or ``UNKNOWN_OUTCOME`` with
    ``outcome_unknown``; ``PERMANENT`` → ``PERMANENT``), so code that builds
    a ``ClassifiedError`` from a severity alone stays consistent.
    """

    severity: ErrorSeverity
    original: BaseException
    reason: str
    outcome_unknown: bool = False
    category: ErrorCategory = ErrorCategory.PERMANENT

    def __post_init__(self) -> None:
        category = self.category
        if category.severity is not self.severity:
            category = ErrorCategory.RETRYABLE if self.severity is ErrorSeverity.TRANSIENT else ErrorCategory.PERMANENT
        if self.outcome_unknown and category is ErrorCategory.RETRYABLE:
            category = ErrorCategory.UNKNOWN_OUTCOME
        object.__setattr__(self, "category", category)


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
