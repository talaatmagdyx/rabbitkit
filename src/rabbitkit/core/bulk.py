"""Bulk-publish contract — per-item outcomes, options, and the shared
preparation stage used by ``publish_many`` / ``iter_publish`` on both brokers.

Transport-free by design (hard invariant 1): nothing here touches pika or
aio-pika. The brokers own the I/O; this module owns *what an outcome means*
and *what must be true before an envelope may be submitted*.

Why a second status enum
------------------------
``PublishStatus`` (``core/types.py``) describes what the transport observed
for one publish call. A bulk caller needs a stricter, actionable vocabulary
that preserves uncertainty (safety invariant 2 — "a timeout/disconnect can
mean UNKNOWN, not definitely rejected"):

======================  =============================================  ======================================
State                   Meaning                                        Caller action
======================  =============================================  ======================================
``CONFIRMED``           Broker confirm observed, no mandatory return   Do not replay
``UNROUTABLE``          Mandatory return observed                      Fix routing before retry
``NACKED``              Negative publisher confirm observed            Retry only under an idempotency policy
``INVALID``             Local validation failed before submission      Fix input
``NOT_SENT``            Definitively never submitted                   Safe to resubmit
``UNKNOWN``             May have reached the broker; outcome unknown   Reconcile / retry with stable IDs
======================  =============================================  ======================================

Nothing here ever turns an UNKNOWN into a success or a failure: ``SENT``
(fire-and-forget), a confirm timeout, and a mid-publish exception all map
to ``UNKNOWN`` with a distinct ``reason`` code so callers can reconcile by
``message_id``/``attempt_id`` instead of guessing.
"""

from __future__ import annotations

import asyncio
import dataclasses
import threading
import time
import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from rabbitkit.core.errors import (
    BackpressureError,
    BrokerNotStartedError,
    ConfigValidationError,
    MessageTooLargeError,
)
from rabbitkit.core.types import BulkPublishStatus, MessageEnvelope, PublishOutcome, PublishStatus

# ── Status vocabulary ──────────────────────────────────────────────────────


#: Bounded reason codes. ``BulkPublishItem.reason`` is ALWAYS one of these
#: (never free text, never exception text) so it is safe as a metric label.
REASON_CONFIRMED = "confirmed"
REASON_SENT_UNCONFIRMED = "sent_unconfirmed"
REASON_CONFIRM_TIMEOUT = "confirm_timeout"
REASON_RETURNED = "returned"
REASON_NACKED = "nacked"
REASON_PUBLISH_ERROR = "publish_error"
REASON_BACKPRESSURE = "backpressure_dropped"
REASON_NOT_STARTED = "broker_not_started"
REASON_ADMISSION_TIMEOUT = "admission_timeout"
REASON_OVERALL_TIMEOUT = "overall_timeout"
REASON_CANCELLED = "cancelled"
REASON_INPUT_ERROR = "input_iteration_error"
REASON_BODY_TOO_LARGE = "body_too_large"
REASON_EXCEEDS_BUFFER = "exceeds_max_buffer_bytes"
REASON_INVALID_TYPE = "invalid_type"
REASON_INVALID_HEADERS = "invalid_headers"
REASON_INVALID_ENVELOPE = "invalid_envelope"
REASON_NO_OUTCOME = "no_outcome"

ALL_REASONS: frozenset[str] = frozenset(
    {
        REASON_CONFIRMED,
        REASON_SENT_UNCONFIRMED,
        REASON_CONFIRM_TIMEOUT,
        REASON_RETURNED,
        REASON_NACKED,
        REASON_PUBLISH_ERROR,
        REASON_BACKPRESSURE,
        REASON_NOT_STARTED,
        REASON_ADMISSION_TIMEOUT,
        REASON_OVERALL_TIMEOUT,
        REASON_CANCELLED,
        REASON_INPUT_ERROR,
        REASON_BODY_TOO_LARGE,
        REASON_EXCEEDS_BUFFER,
        REASON_INVALID_TYPE,
        REASON_INVALID_HEADERS,
        REASON_INVALID_ENVELOPE,
        REASON_NO_OUTCOME,
    }
)


# ── Options ────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class BulkPublishOptions:
    """Bounds for one ``publish_many`` / ``iter_publish`` operation.

    Every bound is explicit; none is a tuning recommendation. The defaults are
    conservative starting points — measure before changing them.

    Attributes:
        max_in_flight: Maximum envelopes submitted to the transport but not
            yet settled (async broker). The sync broker publishes one at a
            time; the value is validated but has no effect there.
        max_buffer_bytes: Maximum sum of in-flight body bytes. An envelope
            larger than this can never be admitted and is reported
            ``INVALID`` (``exceeds_max_buffer_bytes``) without being sent.
        admission_timeout: Seconds to wait for an in-flight/byte slot before
            reporting an item ``NOT_SENT`` (``admission_timeout``).
        confirm_timeout: Per-item confirm wait override (async broker). A
            timeout reports ``UNKNOWN`` (``confirm_timeout``) — the publish
            may have reached the broker. ``None`` uses the transport's
            configured ``PublisherConfig.confirm_timeout``.
        overall_timeout: Whole-operation deadline. On expiry admission stops;
            never-submitted items are ``NOT_SENT`` (``overall_timeout``),
            in-flight items are given ``drain_grace`` seconds to settle and
            then reported ``UNKNOWN`` (``overall_timeout``). ``None`` = no
            operation deadline (each item still bounded individually).
        drain_grace: Seconds to wait for in-flight items after the overall
            deadline before classifying them ``UNKNOWN``.
        max_items: Upper bound on ``publish_many`` input length. Exceeding it
            raises ``ValueError`` BEFORE anything is published — a bulk call
            is a bounded finite collection; use ``iter_publish`` for streams.
    """

    max_in_flight: int = 256
    max_buffer_bytes: int = 8 * 1024 * 1024
    admission_timeout: float = 5.0
    confirm_timeout: float | None = None
    overall_timeout: float | None = 30.0
    drain_grace: float = 2.0
    max_items: int = 100_000

    def __post_init__(self) -> None:
        if self.max_in_flight < 1:
            raise ConfigValidationError(f"BulkPublishOptions.max_in_flight must be >= 1, got {self.max_in_flight}")
        if self.max_buffer_bytes < 1:
            raise ConfigValidationError(
                f"BulkPublishOptions.max_buffer_bytes must be >= 1, got {self.max_buffer_bytes}"
            )
        if self.admission_timeout <= 0:
            raise ConfigValidationError(
                f"BulkPublishOptions.admission_timeout must be > 0, got {self.admission_timeout}"
            )
        if self.confirm_timeout is not None and self.confirm_timeout <= 0:
            raise ConfigValidationError(
                f"BulkPublishOptions.confirm_timeout must be > 0 when set, got {self.confirm_timeout}"
            )
        if self.overall_timeout is not None and self.overall_timeout <= 0:
            raise ConfigValidationError(
                f"BulkPublishOptions.overall_timeout must be > 0 when set, got {self.overall_timeout}"
            )
        if self.drain_grace < 0:
            raise ConfigValidationError(f"BulkPublishOptions.drain_grace must be >= 0, got {self.drain_grace}")
        if self.max_items < 1:
            raise ConfigValidationError(f"BulkPublishOptions.max_items must be >= 1, got {self.max_items}")


# ── Per-item result ────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class BulkPublishItem:
    """Outcome of ONE input envelope.

    Keyed by ``index`` (position in the caller's input), never solely by
    ``message_id`` — input IDs may legitimately repeat. ``attempt_id`` is a
    fresh UUID per submission so an UNKNOWN item can be reconciled against a
    downstream inbox even when the same ``message_id`` was published twice.
    """

    index: int
    status: BulkPublishStatus
    reason: str
    message_id: str = ""
    attempt_id: str = ""
    exchange: str = ""
    routing_key: str = ""
    body_bytes: int = 0
    error: BaseException | None = None
    submitted_at: float | None = None
    settled_at: float | None = None

    def __post_init__(self) -> None:
        if self.reason not in ALL_REASONS:
            raise ValueError(f"BulkPublishItem.reason must be a bounded reason code, got {self.reason!r}")

    @property
    def ok(self) -> bool:
        """True only for ``CONFIRMED``. UNKNOWN is never ok."""
        return self.status is BulkPublishStatus.CONFIRMED

    @property
    def duration(self) -> float | None:
        """Seconds from submission to settlement, when both are known."""
        if self.submitted_at is None or self.settled_at is None:
            return None
        return max(0.0, self.settled_at - self.submitted_at)

    @property
    def safe_to_resubmit(self) -> bool:
        """True when resubmitting cannot duplicate a broker-held message
        (``NOT_SENT`` / ``INVALID`` after fixing the input). UNKNOWN is
        deliberately False — it needs stable IDs plus deduplication."""
        return self.status in (BulkPublishStatus.NOT_SENT, BulkPublishStatus.INVALID)


class BulkPublishError(Exception):
    """Raised by :meth:`BulkPublishResult.raise_for_status` when not every
    item is CONFIRMED. Carries the full result for reconciliation."""

    def __init__(self, result: BulkPublishResult) -> None:
        self.result = result
        counts = ", ".join(f"{s.value}={n}" for s, n in sorted(result.counts.items(), key=lambda kv: kv[0].value))
        super().__init__(f"bulk publish incomplete: {counts}")


@dataclass(frozen=True, slots=True)
class BulkPublishResult:
    """Input-ordered per-item outcomes for one ``publish_many`` call."""

    items: tuple[BulkPublishItem, ...]
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        indices = [it.index for it in self.items]
        if indices != sorted(indices) or len(set(indices)) != len(indices):
            raise ValueError("BulkPublishResult.items must be input-ordered with unique indices")

    def __len__(self) -> int:
        return len(self.items)

    @property
    def counts(self) -> dict[BulkPublishStatus, int]:
        return dict(Counter(it.status for it in self.items))

    def by_status(self, status: BulkPublishStatus) -> tuple[BulkPublishItem, ...]:
        return tuple(it for it in self.items if it.status is status)

    @property
    def confirmed(self) -> tuple[BulkPublishItem, ...]:
        return self.by_status(BulkPublishStatus.CONFIRMED)

    @property
    def unknown(self) -> tuple[BulkPublishItem, ...]:
        return self.by_status(BulkPublishStatus.UNKNOWN)

    @property
    def resubmittable(self) -> tuple[BulkPublishItem, ...]:
        """Items that were definitively never sent (safe to resubmit)."""
        return tuple(it for it in self.items if it.safe_to_resubmit)

    @property
    def failed(self) -> tuple[BulkPublishItem, ...]:
        """Everything that is not CONFIRMED — including UNKNOWN."""
        return tuple(it for it in self.items if not it.ok)

    @property
    def all_confirmed(self) -> bool:
        return all(it.ok for it in self.items)

    def raise_for_status(self) -> BulkPublishResult:
        """Raise :class:`BulkPublishError` unless every item is CONFIRMED."""
        if not self.all_confirmed:
            raise BulkPublishError(self)
        return self


# ── Outcome mapping ────────────────────────────────────────────────────────


def classify_publish_outcome(outcome: PublishOutcome | None) -> tuple[BulkPublishStatus, str]:
    """Map a transport ``PublishOutcome`` onto the bulk vocabulary.

    Precedence rules (safety invariant 2):

    * ``RETURNED`` wins over everything — an unroutable message is not a
      successful routed publication even though the broker also confirms it.
    * ``SENT`` (confirms disabled) is ``UNKNOWN``, not confirmed.
    * ``TIMEOUT`` is ``UNKNOWN`` — the frame may have reached the broker.
    * ``ERROR`` is ``UNKNOWN`` unless the error proves nothing was submitted
      (backpressure drop, broker not started) or that the input itself was
      invalid (oversized body / ``ValueError``/``TypeError`` from validation).
    * ``None`` (a duck-typed publish fn returned nothing) is ``UNKNOWN``.
    """
    if outcome is None:
        return BulkPublishStatus.UNKNOWN, REASON_NO_OUTCOME
    status = outcome.status
    if status is PublishStatus.RETURNED:
        return BulkPublishStatus.UNROUTABLE, REASON_RETURNED
    if status is PublishStatus.CONFIRMED:
        return BulkPublishStatus.CONFIRMED, REASON_CONFIRMED
    if status is PublishStatus.SENT:
        return BulkPublishStatus.UNKNOWN, REASON_SENT_UNCONFIRMED
    if status is PublishStatus.NACKED:
        return BulkPublishStatus.NACKED, REASON_NACKED
    if status is PublishStatus.TIMEOUT:
        return BulkPublishStatus.UNKNOWN, REASON_CONFIRM_TIMEOUT
    # ERROR
    err = outcome.error
    if isinstance(err, BackpressureError):
        return BulkPublishStatus.NOT_SENT, REASON_BACKPRESSURE
    if isinstance(err, BrokerNotStartedError):
        return BulkPublishStatus.NOT_SENT, REASON_NOT_STARTED
    if isinstance(err, MessageTooLargeError):
        return BulkPublishStatus.INVALID, REASON_BODY_TOO_LARGE
    if isinstance(err, (ValueError, TypeError)):
        return BulkPublishStatus.INVALID, REASON_INVALID_ENVELOPE
    return BulkPublishStatus.UNKNOWN, REASON_PUBLISH_ERROR


def classify_publish_exception(exc: BaseException) -> tuple[BulkPublishStatus, str]:
    """Classify an exception RAISED by a publish call (rather than returned).

    Mirrors :func:`classify_publish_outcome`'s ERROR branch. A raised
    exception may come from a middleware BEFORE submission or from the
    transport AFTER the frame left — so anything not provably pre-submission
    is ``UNKNOWN``.
    """
    if isinstance(exc, asyncio.CancelledError):
        return BulkPublishStatus.UNKNOWN, REASON_CANCELLED
    if isinstance(exc, TimeoutError):
        return BulkPublishStatus.UNKNOWN, REASON_CONFIRM_TIMEOUT
    if isinstance(exc, BackpressureError):
        return BulkPublishStatus.NOT_SENT, REASON_BACKPRESSURE
    if isinstance(exc, BrokerNotStartedError):
        return BulkPublishStatus.NOT_SENT, REASON_NOT_STARTED
    if isinstance(exc, MessageTooLargeError):
        return BulkPublishStatus.INVALID, REASON_BODY_TOO_LARGE
    if isinstance(exc, (ValueError, TypeError)):
        return BulkPublishStatus.INVALID, REASON_INVALID_ENVELOPE
    return BulkPublishStatus.UNKNOWN, REASON_PUBLISH_ERROR


# ── Preparation stage ──────────────────────────────────────────────────────

_HEADER_SCALARS: tuple[type, ...] = (str, int, float, bool, bytes, datetime, Decimal, type(None))


def _validate_header_value(value: Any, depth: int = 0) -> bool:
    """AMQP field-table value check: scalars, or list/dict of them (bounded depth)."""
    if depth > 8:
        return False
    if isinstance(value, _HEADER_SCALARS):
        return True
    if isinstance(value, (list, tuple)):
        return all(_validate_header_value(v, depth + 1) for v in value)
    if isinstance(value, dict):
        return all(isinstance(k, str) and _validate_header_value(v, depth + 1) for k, v in value.items())
    return False


def new_attempt_id() -> str:
    """Fresh per-submission correlation id (UUID4 string)."""
    return str(uuid.uuid4())


class PublishPreparer:
    """Transport-independent preparation for one envelope.

    ``input → envelope validation → frozen copy → final byte/header checks``.
    Middleware/serialization run later, inside the broker's publish chain,
    exactly once per attempt. Everything here happens BEFORE admission so a
    bad item never consumes an in-flight slot.

    The prepared envelope is a *copy* whose ``headers`` dict is snapshotted —
    a caller mutating its original dict after ``publish_many`` returns (or
    while it is still running) cannot alter what is on the wire.
    """

    def __init__(
        self,
        *,
        max_message_bytes: int,
        max_buffer_bytes: int,
    ) -> None:
        self._max_message_bytes = max_message_bytes
        self._max_buffer_bytes = max_buffer_bytes

    def prepare(self, index: int, raw: Any) -> tuple[MessageEnvelope | None, BulkPublishItem | None]:
        """Return ``(envelope, None)`` when admissible, else ``(None, INVALID item)``."""
        if not isinstance(raw, MessageEnvelope):
            return None, BulkPublishItem(
                index=index,
                status=BulkPublishStatus.INVALID,
                reason=REASON_INVALID_TYPE,
                error=TypeError(f"publish_many items must be MessageEnvelope, got {type(raw).__name__}"),
            )
        body_len = len(raw.body)
        if self._max_message_bytes and body_len > self._max_message_bytes:
            return None, BulkPublishItem(
                index=index,
                status=BulkPublishStatus.INVALID,
                reason=REASON_BODY_TOO_LARGE,
                message_id=raw.message_id,
                exchange=raw.exchange,
                routing_key=raw.routing_key,
                body_bytes=body_len,
                error=MessageTooLargeError(
                    f"Message body ({body_len} bytes) exceeds PublisherConfig.max_message_bytes "
                    f"({self._max_message_bytes})."
                ),
            )
        if body_len > self._max_buffer_bytes:
            return None, BulkPublishItem(
                index=index,
                status=BulkPublishStatus.INVALID,
                reason=REASON_EXCEEDS_BUFFER,
                message_id=raw.message_id,
                exchange=raw.exchange,
                routing_key=raw.routing_key,
                body_bytes=body_len,
                error=ValueError(
                    f"Message body ({body_len} bytes) exceeds BulkPublishOptions.max_buffer_bytes "
                    f"({self._max_buffer_bytes}); it can never be admitted."
                ),
            )
        if raw.headers and not all(isinstance(k, str) and _validate_header_value(v) for k, v in raw.headers.items()):
            return None, BulkPublishItem(
                index=index,
                status=BulkPublishStatus.INVALID,
                reason=REASON_INVALID_HEADERS,
                message_id=raw.message_id,
                exchange=raw.exchange,
                routing_key=raw.routing_key,
                body_bytes=body_len,
                error=TypeError("headers must be str-keyed AMQP field-table values"),
            )
        # Snapshot: the caller cannot mutate the queued envelope's headers.
        frozen = dataclasses.replace(raw, headers=dict(raw.headers)) if raw.headers else raw
        return frozen, None


# ── Admission primitives ───────────────────────────────────────────────────


class ByteBudget:
    """Thread-safe bounded byte budget (sync admission control).

    ``acquire(n, timeout)`` blocks until ``n`` bytes fit under the limit or
    the timeout elapses (returns False — the caller reports ``NOT_SENT``).
    Never silently drops: a failed acquire is always visible to the caller.
    """

    def __init__(self, limit_bytes: int) -> None:
        if limit_bytes < 1:
            raise ValueError("limit_bytes must be >= 1")
        self._limit = limit_bytes
        self._used = 0
        self._cond = threading.Condition()

    @property
    def used(self) -> int:
        with self._cond:
            return self._used

    @property
    def limit(self) -> int:
        return self._limit

    def acquire(self, n: int, timeout: float) -> bool:
        if n > self._limit:
            return False
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._used + n > self._limit:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            self._used += n
            return True

    def release(self, n: int) -> None:
        with self._cond:
            self._used = max(0, self._used - n)
            self._cond.notify_all()


class AsyncByteBudget:
    """asyncio counterpart of :class:`ByteBudget`. Must be created inside a
    running event loop (it owns an ``asyncio.Condition``)."""

    def __init__(self, limit_bytes: int) -> None:
        if limit_bytes < 1:
            raise ValueError("limit_bytes must be >= 1")
        self._limit = limit_bytes
        self._used = 0
        self._cond = asyncio.Condition()

    @property
    def used(self) -> int:
        return self._used

    @property
    def limit(self) -> int:
        return self._limit

    async def acquire(self, n: int, timeout: float) -> bool:
        if n > self._limit:
            return False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        async with self._cond:
            while self._used + n > self._limit:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                except TimeoutError:
                    return False
            self._used += n
            return True

    async def release(self, n: int) -> None:
        async with self._cond:
            self._used = max(0, self._used - n)
            self._cond.notify_all()


def summarize_statuses(items: Iterable[BulkPublishItem]) -> dict[str, int]:
    """``{status_value: count}`` — handy for logs and metrics labels."""
    return {k.value: v for k, v in Counter(it.status for it in items).items()}
