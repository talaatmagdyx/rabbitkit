"""RetryMiddleware — routes failed messages to delay queues for retry.

TOPOLOGY SPEC (see Contract in plan):
- Per-queue delay queues: {source_queue}.retry.{attempt}
- Dead letter queue: {source_queue}.dlq
- Shared mode: rabbitkit.retry.{attempt}, rabbitkit.dlq

Mechanism: TTL + DLX (dead-letter exchange)
"""

from __future__ import annotations

import asyncio
import logging
import uuid
import warnings
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from rabbitkit.core.config import RetryConfig
from rabbitkit.core.errors import ErrorPredicate
from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage
from rabbitkit.core.quorum import dlq_delivery_limit
from rabbitkit.core.retry_handoff import RetryHandoffTracker
from rabbitkit.core.sanitizer import ErrorSanitizer
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import (
    REQUEUED_FOR_RETRY,
    ClassifiedError,
    ErrorCategory,
    ErrorSeverity,
    FailureCategory,
    HandoffState,
    MessageEnvelope,
    QueueType,
    TerminalAction,
)
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.middleware.error_classifier import ErrorClassifierMiddleware

logger = logging.getLogger(__name__)

# Item 7: DLQ-triage headers. error-message is length-capped (matching the
# item-8 client_properties precedent) so an exception with a huge/binary
# str() (e.g. wrapping a stack-trace-like payload) can't bloat every retry
# republish indefinitely.
_ERROR_MESSAGE_MAX_LEN = 512

# Handler-raised settlement signals: control flow, never retried (R-D1).
_SETTLEMENT_SIGNALS = (AckMessage, NackMessage, RejectMessage)

# Retry/DLQ metadata headers (spec §13.4 / §14.1).
_H_ORIGINAL_QUEUE = "x-rabbitkit-original-queue"
_H_ORIGINAL_MESSAGE_ID = "x-rabbitkit-original-message-id"
_H_ORIGINAL_EXPIRATION = "x-rabbitkit-original-expiration"
_H_FIRST_FAILED_AT = "x-rabbitkit-first-failed-at"
_H_ATTEMPTS = "x-rabbitkit-attempts"
_H_ERROR_CLASS = "x-rabbitkit-error-class"
_H_FAILURE_CATEGORY = "x-rabbitkit-failure-category"
_H_DEAD_LETTERED_AT = "x-rabbitkit-dead-lettered-at"
_H_ERROR_CATEGORY = "x-rabbitkit-error-category"


def _header_text(value: Any) -> str:
    if isinstance(value, bytes):
        value = value.decode(errors="replace")
    return value if isinstance(value, str) else ""


def _header_int(value: Any) -> int:
    """A header as a non-negative int; malformed values read as 0."""
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _parse_iso(value: Any) -> datetime | None:
    """An ISO-8601 header as an aware UTC datetime, or ``None``.

    A future timestamp is clamped to now (clock skew or a hostile producer
    must not create negative ages)."""
    text = _header_text(value)
    if isinstance(value, datetime):
        parsed = value
    elif not text:
        return None
    else:
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    now = datetime.now(UTC)
    return min(parsed, now)


def _xdeath_retry_floor(headers: dict[str, Any], source_queue: str) -> int:
    """Highest delay tier ``n`` of ``<source_queue>.retry.<n>[.s<k>]`` in the
    broker's ``x-death`` history (0 when none)."""
    deaths = headers.get("x-death")
    if not isinstance(deaths, list):
        return 0
    prefix = f"{source_queue}.retry."
    floor = 0
    for death in deaths:
        if not isinstance(death, dict):
            continue
        queue = _header_text(death.get("queue"))
        if not queue.startswith(prefix):
            continue
        tier = queue[len(prefix) :].split(".", 1)[0]
        if tier.isdigit():
            floor = max(floor, int(tier))
    return floor


def _is_timeout(exc: BaseException) -> bool:
    return isinstance(exc, TimeoutError)


def _outcome_ok(outcome: Any) -> bool:
    """A ``None`` outcome (a duck-typed publish fn that returns nothing) is
    UNVERIFIED, not success."""
    return outcome is not None and bool(outcome.ok)


def _shard_index(message_id: str, shards: int) -> int:
    """Stable shard pick (F4). Python's hash() is salted per process — a
    message's retry shard must be identical across every consumer process,
    or its cadence changes on each redelivery. md5 here is a stable bucket
    hash, not crypto."""
    if not message_id:
        return 0
    import hashlib

    return int(hashlib.md5(message_id.encode(), usedforsecurity=False).hexdigest(), 16) % shards


def _shard_queue_name(source_queue: str, attempt: int, shard: int) -> str:
    """Shard 0 keeps the legacy `{q}.retry.{n}` name (backward compatible —
    enabling sharded jitter on an existing topology is purely additive)."""
    base = f"{source_queue}.retry.{attempt}"
    return base if shard == 0 else f"{base}.s{shard}"


def _shard_ttl_multipliers(shards: int, jitter_factor: float) -> list[float]:
    """Shard 0 is exactly 1.0 (legacy TTL, no redeclare conflict); shards
    1..N-1 spread evenly across [1-jf, 1+jf]. Every queue's TTL is still
    UNIFORM for all messages in it — jitter comes from which shard a
    message hashes to, never from per-message TTL (head-of-line safety)."""
    if shards == 2:
        return [1.0, 1.0 + jitter_factor]
    rest = shards - 1
    return [1.0] + [1.0 + jitter_factor * (-1.0 + 2.0 * i / (rest - 1)) for i in range(rest)]


def retry_middleware_insertion_index(middlewares: Sequence[Any]) -> int:
    """Index at which an auto-wired ``RetryMiddleware`` should be inserted.

    Retry must be OUTER of ordinary user middlewares (e.g. ``TimeoutMiddleware``)
    so it can classify and re-queue exceptions they raise — this is the
    documented composition in ``middleware/timeout.py``
    (``middlewares=[retry_mw, timeout_mw]  # retry outermost``), which relies on
    retry seeing ``HandlerTimeoutError``.

    Retry must be INNER of any ``ExceptionMiddleware``, which is documented as
    the true outermost layer that "catches exceptions AFTER retry gives up"
    (``middleware/exception.py``) — it needs to see the ``_rabbitkit_terminal``
    exceptions retry re-raises on exhaustion/permanent failure.

    So retry is inserted right after any *leading* ``ExceptionMiddleware``
    instances, ahead of everything else.
    """
    from rabbitkit.middleware.exception import ExceptionMiddleware

    index = 0
    for mw in middlewares:
        if isinstance(mw, ExceptionMiddleware):
            index += 1
        else:
            break
    return index


def warn_retry_without_confirms(route_name: str, *, context: str = "result") -> None:
    """Warn when a route's ``@publisher()`` result forward runs on a broker
    with ``PublisherConfig.confirm_delivery=False`` (M4).

    The RETRY context no longer warns: retry envelopes are published with
    ``mandatory=True``, which forces per-publish confirm mode on both
    transports even when ``confirm_delivery=False`` — the delay-queue publish
    is always confirm-gated, so the warning was a false positive there.

    Both ``RetryMiddleware`` and the pipeline's result-publish step (Contract
    5) settle the SOURCE message as soon as their republish reports
    ``outcome.ok`` -- with confirms off, that publish reports
    ``PublishStatus.SENT`` (fire-and-forget, never broker-confirmed) rather
    than ``CONFIRMED``, and ``.ok`` is True for both. If that SENT publish is
    actually lost in flight (e.g. a connection drop right after), the source
    message is still settled -- a real loss, not just a delay.
    Enable ``confirm_delivery=True`` (the default) on any such broker if
    this matters for your workload.
    """
    import warnings

    what = "retry" if context == "retry" else "a @publisher() result forward"
    settles = "acks the source message" if context == "retry" else "settles the source message"
    warnings.warn(
        f"Route {route_name!r} uses {what} but the broker publishes with "
        f"confirm_delivery=False. The pipeline {settles} as soon as its internal republish "
        "is sent, without waiting for a broker confirm -- a publish lost in flight after "
        "that point is a real loss (the source is already settled). Set "
        "PublisherConfig(confirm_delivery=True) (the default) if durability here matters.",
        RuntimeWarning,
        stacklevel=3,
    )


def warn_retry_middleware_without_topology(route_name: str) -> None:
    """Warn when a route carries a ``RetryMiddleware`` but no retry topology.

    A ``RetryMiddleware`` publishes failed messages to ``<queue>.retry.<n>``
    delay queues. Those queues are only declared when retry is enabled via
    ``RabbitConfig.retry`` / ``@subscriber(retry=...)`` (which drives
    ``_declare_topology``). If a caller adds a ``RetryMiddleware`` manually to
    ``middlewares=[...]`` *without* also setting ``retry=``, the delay queues
    are never declared, so the retry publishes target non-existent queues on the
    default exchange and are silently dropped — the source message is acked and
    the retry is lost. Surface that half-configuration loudly.
    """
    import warnings

    warnings.warn(
        f"Route {route_name!r} has a RetryMiddleware but no retry topology was declared "
        "(no retry=RetryConfig(...) on the broker or subscriber). Its delay-queue publishes "
        "will target non-existent queues and be dropped. Set retry=RetryConfig(...) so the "
        "delay/DLQ topology is declared, or remove the manual RetryMiddleware.",
        RuntimeWarning,
        stacklevel=3,
    )


class RetryMiddleware(BaseMiddleware):
    """Routes failed messages to delay queues for retry.

    On exception:
    1. Classify error (transient/permanent)
    2. If transient + retries left → publish to delay queue + ack source
    3. If permanent or retries exhausted → tag as terminal + re-raise
    """

    def __init__(
        self,
        config: RetryConfig,
        *,
        publish_fn: Callable[[MessageEnvelope], Any] | None = None,
        publish_async_fn: Callable[[MessageEnvelope], Awaitable[Any]] | None = None,
        predicates: Sequence[ErrorPredicate] = (),
        metrics_collector: Any | None = None,
        metrics_config: Any | None = None,
        sanitizer: ErrorSanitizer | None = None,
        handoff_tracker: RetryHandoffTracker | None = None,
        on_handoff_failure: Callable[[float, RetryHandoffTracker], Any] | None = None,
        on_handoff_exhausted: Callable[[RetryHandoffTracker], Any] | None = None,
        sleep_on_handoff_failure: bool = True,
    ) -> None:
        self._config = config
        # Plan §5.3: allowlisted category/code + bounded, redacted summary on
        # the retry/DLQ triage headers. Never raw exception text by default.
        self._sanitizer = sanitizer or ErrorSanitizer(
            policy=config.error_detail, max_summary_len=_ERROR_MESSAGE_MAX_LEN
        )
        # Plan §5.2: retry-PUBLISH failures are tracked separately from
        # handler attempts, with capped exponential backoff and an exhausted
        # state an owner can act on (stop the consumer / close the channel).
        self._handoff = handoff_tracker or RetryHandoffTracker(config.handoff)
        self._on_handoff_failure = on_handoff_failure
        self._on_handoff_exhausted = on_handoff_exhausted
        # Async handlers await the backoff (a coroutine sleeping never blocks
        # the loop). The sync path NEVER sleeps here — with worker_count == 1
        # the handler runs on pika's I/O thread and a sleep would stall
        # heartbeats; the backoff is surfaced through on_handoff_failure
        # instead so the owner can pause admission.
        self._sleep_on_handoff_failure = sleep_on_handoff_failure
        self._exhausted_logged = False
        # predicates run first (True=transient, False=permanent, None=defer to the
        # built-in type tuples, then unknown_policy). Lets callers classify by
        # something other than exception type (e.g. an HTTP status attribute).
        self._classifier = ErrorClassifierMiddleware(
            predicates=predicates,
            unknown_policy=config.unknown_policy,
        )
        self._publish_fn = publish_fn
        self._publish_async_fn = publish_async_fn
        # M2: optional -- wired by the broker from a MetricsMiddleware already
        # present on the same route, so retried/dead-lettered counts are
        # observable. None (the default) is a no-op; RetryMiddleware itself
        # has no metrics opinion otherwise.
        self._metrics_collector = metrics_collector
        self._metrics_config = metrics_config

    def ensure_publish_fns(
        self,
        publish_fn: Callable[[MessageEnvelope], Any] | None = None,
        publish_async_fn: Callable[[MessageEnvelope], Any] | None = None,
    ) -> None:
        """Fill in whichever publish function is not already set.

        Called by the broker at start() for USER-CONSTRUCTED RetryMiddleware
        instances found in ``middlewares=[...]``: previously such an instance
        with no publish fn silently fell into the ack-without-publish drop
        path on every transient failure (now a nack+RuntimeWarning, but still
        a hot loop). Injecting the broker's own confirmed-publish fn makes a
        manually-added ``RetryMiddleware(config)`` behave identically to the
        auto-wired one. Explicitly-passed fns are never overwritten.
        """
        if self._publish_fn is None and publish_fn is not None:
            self._publish_fn = publish_fn
        if self._publish_async_fn is None and publish_async_fn is not None:
            self._publish_async_fn = publish_async_fn

    def _record_metric(
        self, metric_name: str | None, message: RabbitMessage, category: ErrorCategory | None = None
    ) -> None:
        if self._metrics_collector is None or metric_name is None:
            return
        queue = message.headers.get("x-rabbitkit-original-queue") or message.routing_key or "unknown"
        labels = {"queue": str(queue)}
        if category is not None:
            # Bounded: one of the six ErrorCategory values.
            labels["category"] = category.value
        self._metrics_collector.inc_counter(metric_name, labels)

    @property
    def config(self) -> RetryConfig:
        return self._config

    @property
    def handoff_tracker(self) -> RetryHandoffTracker:
        """Consecutive retry-publish failure state for this middleware."""
        return self._handoff

    @property
    def sanitizer(self) -> ErrorSanitizer:
        return self._sanitizer

    def _record_handoff_failure(self, message: RabbitMessage) -> float:
        """Count a failed delay-queue handoff; return the backoff to apply."""
        backoff = self._handoff.record_failure()
        if self._metrics_config is not None:
            self._record_metric(self._metrics_config.retry_handoff_failures_total, message)
            self._set_paused_gauge(message, 1.0)
        state = self._handoff.state
        if state is HandoffState.EXHAUSTED:
            # Exhausted = the destination has been broken for a long time.
            # Hold every failing delivery for the full backoff_max: with
            # prefetch N this caps the route at N handler runs per
            # backoff_max — effectively pausing the queue — until a handoff
            # succeeds again (record_success resets the streak).
            backoff = self._handoff.config.backoff_max
            if not self._exhausted_logged:
                self._exhausted_logged = True
                logger.critical(
                    "Retry handoff EXHAUSTED for queue %s after %d consecutive failures: the delay "
                    "queue is missing or the broker refuses publishes. Failing deliveries are now held "
                    "%.1fs each before requeue (consumption effectively paused). Restore the retry "
                    "topology (or clear the broker alarm); wire on_handoff_exhausted to stop the route.",
                    message.headers.get(_H_ORIGINAL_QUEUE) or message.routing_key,
                    self._handoff.consecutive_failures,
                    backoff,
                )
        logger.warning(
            "Retry handoff failed (consecutive=%d, state=%s, backoff=%.2fs): routing_key=%s",
            self._handoff.consecutive_failures,
            state.value,
            backoff,
            message.routing_key,
        )
        if self._on_handoff_failure is not None:
            try:
                self._on_handoff_failure(backoff, self._handoff)
            except Exception:
                logger.exception("on_handoff_failure hook raised")
        if state is HandoffState.EXHAUSTED and self._on_handoff_exhausted is not None:
            try:
                self._on_handoff_exhausted(self._handoff)
            except Exception:
                logger.exception("on_handoff_exhausted hook raised")
        return backoff

    def _record_handoff_success(self, message: RabbitMessage) -> None:
        was_paused = self._handoff.is_paused
        self._handoff.record_success()
        if self._exhausted_logged:
            self._exhausted_logged = False
            logger.warning("Retry handoff recovered: delay-queue publishes are confirmed again")
        if was_paused and self._metrics_config is not None:
            self._set_paused_gauge(message, 0.0)

    def _set_paused_gauge(self, message: RabbitMessage, value: float) -> None:
        if self._metrics_collector is None or self._metrics_config is None:
            return
        set_gauge = getattr(self._metrics_collector, "set_gauge", None)
        if set_gauge is None:
            return
        queue = message.headers.get("x-rabbitkit-original-queue") or message.routing_key or "unknown"
        set_gauge(self._metrics_config.retry_handoff_paused, {"queue": str(queue)}, value)

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        """Sync retry scope.

        H8: on a caught, requeued failure, returns ``REQUEUED_FOR_RETRY``
        (never ``None``) — see that sentinel's docstring in ``core/types.py``.
        ``_handle_retry_sync`` either returns normally (requeued: routed to a
        delay queue, or nacked for immediate redelivery if that publish
        itself failed) or re-raises (terminal: permanent/exhausted) via
        ``_terminal``, so reaching this ``return`` unambiguously
        means "requeued" — an outer middleware (e.g. DeduplicationMiddleware)
        MUST NOT treat this the same as the handler actually succeeding.

        Handler settlement signals (``AckMessage`` / ``NackMessage`` /
        ``RejectMessage``) are control flow, not failures: they pass through
        untouched whatever the classifier, predicates or ``unknown_policy``
        say, so the pipeline settles exactly as the handler asked (R-D1).
        """
        try:
            return call_next(message)
        except _SETTLEMENT_SIGNALS:
            raise
        except Exception as exc:
            self._handle_retry_sync(exc, message)
            return REQUEUED_FOR_RETRY

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Async retry scope. See :meth:`consume_scope` (H8) for why this
        returns ``REQUEUED_FOR_RETRY`` rather than ``None``, and why
        settlement signals pass through."""
        try:
            return await call_next(message)
        except _SETTLEMENT_SIGNALS:
            raise
        except Exception as exc:
            await self._handle_retry_async(exc, message)
            return REQUEUED_FOR_RETRY

    def _decide(self, exc: Exception, message: RabbitMessage) -> tuple[ClassifiedError, int, FailureCategory | None]:
        """Classify *exc*: ``(classified, retry_count, terminal_category)``.

        ``terminal_category`` is ``None`` when the failure should be retried.
        """
        classified = self._classifier.classify(exc)
        retry_count = self._get_retry_count(message)
        if classified.severity is not ErrorSeverity.TRANSIENT:
            return classified, retry_count, FailureCategory.PERMANENT
        if retry_count >= self._config.max_retries:
            return classified, retry_count, FailureCategory.EXHAUSTED
        if self._retry_age_exceeded(message):
            return classified, retry_count, FailureCategory.RETRY_AGE_EXCEEDED
        return classified, retry_count, None

    def _handle_retry_sync(self, exc: Exception, message: RabbitMessage) -> None:
        """Handle exception in sync context."""
        classified, retry_count, terminal = self._decide(exc, message)
        if terminal is None:
            self._route_to_delay_queue_sync(message, retry_count, exc, classified=classified)
            return
        self._terminal_sync(exc, classified, terminal, retry_count, message)

    async def _handle_retry_async(self, exc: Exception, message: RabbitMessage) -> None:
        """Handle exception in async context."""
        classified, retry_count, terminal = self._decide(exc, message)
        if terminal is None:
            await self._route_to_delay_queue_async(message, retry_count, exc, classified=classified)
            return
        await self._terminal_async(exc, classified, terminal, retry_count, message)

    def _get_retry_count(self, message: RabbitMessage) -> int:
        """Get current retry count, clamped to ``[0, max_retries]`` (H5).

        The count is the larger of the ``retry_header`` value and the delay
        tiers the broker's ``x-death`` history shows this message expired
        out of (``<queue>.retry.<n>`` → at least ``n`` retries happened).
        A republish that resets the plain header but keeps the broker's
        history therefore cannot restart the budget (13.4).

        Both are read verbatim from an inbound AMQP message — nothing
        distinguishes a value written by this middleware's own delay-queue
        round trip from one set directly by an untrusted producer (there is
        no broker-side attestation of provenance for a plain header). Without
        clamping, a producer could set it negative (``attempt = retry_count +
        1`` in :meth:`_build_retry_envelope` would then be <= 0, producing a
        delay-queue routing key like ``...retry.-4`` that was never declared
        — the retry publish silently targets a non-existent queue and the
        message is lost rather than retried) or absurdly large (forcing every
        message straight to the DLQ, skipping retries entirely). Clamping
        makes ``max_retries`` an enforced ceiling regardless of what the
        header claims. A malformed (non-numeric) header value is treated the
        same as missing (0) rather than raising.

        With ``RetryConfig.trust_retry_header=False`` the header is not
        read at all: only the broker's ``x-death`` history counts, so a
        producer cannot skip retries by claiming a high count (G-19).

        For a broker-enforced backstop on top of this (e.g. against a
        misbehaving consumer that never expires/dead-letters a message),
        prefer quorum source queues with ``x-delivery-limit`` — see
        ``docs/retry-and-dlq.md``. ``max_retry_age`` bounds the total time.
        """
        retry_count = 0
        if self._config.trust_retry_header:
            retry_count = _header_int(message.headers.get(self._config.retry_header, 0))
        source_queue = _header_text(message.headers.get(_H_ORIGINAL_QUEUE))
        if source_queue:
            retry_count = max(retry_count, _xdeath_retry_floor(message.headers, source_queue))
        return max(0, min(retry_count, self._config.max_retries))

    def _retry_age_exceeded(self, message: RabbitMessage) -> bool:
        max_age = self._config.max_retry_age
        if max_age is None:
            return False
        first = _parse_iso(message.headers.get(_H_FIRST_FAILED_AT))
        if first is None:
            return False  # this is the first failure — the clock starts now
        age = (datetime.now(UTC) - first).total_seconds()
        # A timestamp in the future (clock skew, a hostile producer) counts
        # as "just started", never as negative age.
        return age > max_age

    # ── envelope construction ─────────────────────────────────────────────

    def _stable_message_id(self, message: RabbitMessage, headers: dict[str, Any]) -> str:
        """The message id every copy (retry and DLQ) carries.

        Recorded once, on the first handoff, as
        ``x-rabbitkit-original-message-id``. A message published without a
        ``message_id`` gets a fresh one at that point — otherwise the client
        library stamps a random id on EVERY republish and the attempts of one
        logical message can't be correlated (12.2).
        """
        original = _header_text(headers.get(_H_ORIGINAL_MESSAGE_ID))
        if not original:
            original = message.message_id or uuid.uuid4().hex
            headers[_H_ORIGINAL_MESSAGE_ID] = original
        return message.message_id or original

    def _stamp_failure(
        self,
        headers: dict[str, Any],
        message: RabbitMessage,
        exc: Exception,
        classified: ClassifiedError,
        category: FailureCategory,
        attempts: int,
        now: str,
    ) -> None:
        """Write the CURRENT attempt's failure metadata onto *headers*."""
        if "x-rabbitkit-original-exchange" not in headers:
            headers["x-rabbitkit-original-exchange"] = message.exchange
        if "x-rabbitkit-original-routing-key" not in headers:
            headers["x-rabbitkit-original-routing-key"] = message.routing_key
        if _H_ORIGINAL_QUEUE not in headers:
            headers[_H_ORIGINAL_QUEUE] = ""  # set by broker at consume time
        # Item 7 / plan §5.3: allowlisted category/code + bounded, REDACTED
        # summary. ``error_detail="omit"`` writes no message text at all;
        # ``"raw"`` is the legacy opt-in. A previous attempt's message header
        # is dropped so a sanitized copy cannot carry forward stale raw text.
        headers.pop("x-rabbitkit-error-message", None)
        sanitized = self._sanitizer.sanitize(exc, classified.severity)
        headers.update(sanitized.as_headers())
        # The failure taxonomy (retryable / permanent / invalid_message /
        # unknown_outcome / infrastructure / shutdown), not just the severity.
        headers[_H_ERROR_CATEGORY] = classified.category.value
        headers[_H_ERROR_CLASS] = sanitized.code
        headers[_H_FAILURE_CATEGORY] = category.value
        # Attempts = handler executions so far, including this one. Never
        # lowered: a copy that already claims more keeps its claim.
        headers[_H_ATTEMPTS] = max(attempts, _header_int(headers.get(_H_ATTEMPTS, 0)))
        # first-failed-at is set once and preserved across every retry;
        # last-failed-at is overwritten on every failure.
        if _parse_iso(headers.get(_H_FIRST_FAILED_AT)) is None:
            headers[_H_FIRST_FAILED_AT] = now
        headers["x-rabbitkit-last-failed-at"] = now
        if message.expiration and _H_ORIGINAL_EXPIRATION not in headers:
            headers[_H_ORIGINAL_EXPIRATION] = message.expiration

    def _copy_envelope(
        self, message: RabbitMessage, *, routing_key: str, headers: dict[str, Any], message_id: str
    ) -> MessageEnvelope:
        """A republish of *message* that keeps every AMQP property except
        ``expiration`` (see the note in the body).

        The body and ``content_encoding`` are the WIRE values (G-07): an
        ``on_receive`` hook may have rewritten ``message.body`` in place
        (``CompressionMiddleware`` decompresses it but leaves
        ``content_encoding="gzip"``). Republishing that decoded body under
        the original encoding made every retried compressed message
        undecodable on its next delivery, so it was dead-lettered instead of
        retried. The wire bytes are also what a signature covers.
        """
        return MessageEnvelope(
            routing_key=routing_key,
            body=message.wire_body,
            exchange="",  # direct to the queue by name
            headers=headers,
            message_id=message_id,
            correlation_id=message.correlation_id,
            content_type=message.content_type or "application/octet-stream",
            content_encoding=message.wire_content_encoding,
            # Preserve the remaining original message properties -- these used
            # to be silently dropped on every retry republish, so e.g. a
            # priority-queue message lost its priority on its first retry, and
            # an RPC request's reply_to/type/app_id/user_id never survived
            # long enough for the eventual (retried) reply to route back.
            reply_to=message.reply_to,
            priority=message.priority,
            # The producer's timestamp is the event time; a consumer that
            # orders or ages messages by it must see the original (R-D5).
            timestamp=message.timestamp,
            # A known transient (1) stays transient; unknown stays persistent.
            delivery_mode=message.delivery_mode if message.delivery_mode in (1, 2) else 2,
            # expiration is deliberately NOT preserved: a producer-set
            # per-message TTL shorter than the delay tier's x-message-ttl
            # would expire the message INSIDE the delay queue and dead-letter
            # it back to the source early, collapsing the whole backoff
            # ladder into a fast retry storm (and on the DLQ it would silently
            # delete the failure record). The original value is kept in the
            # x-rabbitkit-original-expiration header.
            type=message.type,
            app_id=message.app_id,
            user_id=message.user_id,
            # M4: mandatory so a runtime-deleted/missing queue comes back as
            # RETURNED (outcome not-ok) instead of being broker-confirmed into
            # the void. Requires publisher confirms + basic.return handling,
            # which both transports wire up.
            mandatory=True,
        )

    def _build_retry_envelope(
        self,
        message: RabbitMessage,
        retry_count: int,
        exc: Exception,
        *,
        classified: ClassifiedError | None = None,
    ) -> MessageEnvelope:
        """Build envelope for the delay queue."""
        if classified is None:
            classified = self._classifier.classify(exc)
        attempt = retry_count + 1
        # Always per-queue (shared mode is rejected by RetryConfig — H3).
        source_queue = message.headers.get(_H_ORIGINAL_QUEUE, "unknown")

        headers = dict(message.headers)
        message_id = self._stable_message_id(message, headers)
        if self._config.jitter_mode == "sharded":
            shard = _shard_index(message_id, self._config.jitter_shards)
            delay_queue_rk = _shard_queue_name(str(source_queue), attempt, shard)
        else:
            delay_queue_rk = f"{source_queue}.retry.{attempt}"

        # Never lower an existing count (13.4): a header that already claims
        # more retries than this copy would get keeps its claim.
        headers[self._config.retry_header] = max(
            attempt, _header_int(message.headers.get(self._config.retry_header, 0))
        )
        category = FailureCategory.TIMEOUT_UNKNOWN if _is_timeout(exc) else FailureCategory.TRANSIENT
        self._stamp_failure(headers, message, exc, classified, category, attempt, datetime.now(UTC).isoformat())
        return self._copy_envelope(message, routing_key=delay_queue_rk, headers=headers, message_id=message_id)

    def _build_dlq_envelope(
        self,
        message: RabbitMessage,
        retry_count: int,
        exc: Exception,
        classified: ClassifiedError,
        category: FailureCategory,
    ) -> MessageEnvelope | None:
        """The metadata-stamped DLQ copy, or ``None`` when the source queue
        (and so the DLQ name) is unknown."""
        source_queue = _header_text(message.headers.get(_H_ORIGINAL_QUEUE))
        if not source_queue:
            return None
        headers = dict(message.headers)
        message_id = self._stable_message_id(message, headers)
        now = datetime.now(UTC).isoformat()
        self._stamp_failure(headers, message, exc, classified, category, retry_count + 1, now)
        headers[_H_DEAD_LETTERED_AT] = now
        return self._copy_envelope(message, routing_key=f"{source_queue}.dlq", headers=headers, message_id=message_id)

    # ── retry handoff ─────────────────────────────────────────────────────

    def _route_to_delay_queue_sync(
        self, message: RabbitMessage, retry_count: int, exc: Exception, *, classified: ClassifiedError | None = None
    ) -> None:
        """Publish to delay queue and ack source (sync)."""
        if classified is None:
            classified = self._classifier.classify(exc)
        envelope = self._build_retry_envelope(message, retry_count, exc, classified=classified)

        if self._publish_fn is None:
            # No sync publish fn wired (manual middleware construction, or a
            # RetryMiddleware carrying only publish_async_fn on a sync
            # broker). Acking here would DROP the message — nothing was ever
            # published to the delay queue. Nack for redelivery and warn
            # loudly; broker-wired routes never hit this (the broker injects
            # its own publish fn at start()).
            if not message.is_settled:
                message.nack(requeue=True)
            warnings.warn(
                "RetryMiddleware has no publish_fn wired for a sync route — the "
                "transient failure was nacked (requeued), NOT routed to a delay "
                "queue. Pass publish_fn=broker.publish, or set retry= on the "
                "route/broker and let rabbitkit wire it.",
                RuntimeWarning,
                stacklevel=2,
            )
            return

        if not _outcome_ok(self._safe_publish_sync(envelope)):
            # Delay-queue publish failed — DO NOT ack, or the message is
            # lost forever (never retried, never dead-lettered). Nack with
            # requeue so the broker redelivers it. The sync path never
            # sleeps here: with worker_count == 1 the handler runs on pika's
            # I/O thread and a sleep would stall heartbeats; the backoff is
            # surfaced through on_handoff_failure instead.
            if not message.is_settled:
                message.nack(requeue=True)
            logger.warning(
                "Retry publish failed or unverified; nacked for redelivery: routing_key=%s",
                envelope.routing_key,
            )
            self._record_handoff_failure(message)
            return

        # Ack source message (it's safely in the delay queue now)
        if not message.is_settled:
            message.ack()
        self._record_retried(message, retry_count, envelope, classified.category)

    async def _route_to_delay_queue_async(
        self, message: RabbitMessage, retry_count: int, exc: Exception, *, classified: ClassifiedError | None = None
    ) -> None:
        """Publish to delay queue and ack source (async)."""
        if classified is None:
            classified = self._classifier.classify(exc)
        envelope = self._build_retry_envelope(message, retry_count, exc, classified=classified)

        if self._publish_async_fn is None:
            # See the sync variant: acking without a publish would DROP the
            # message. Nack for redelivery and warn loudly.
            if not message.is_settled:
                await message.nack_async(requeue=True)
            warnings.warn(
                "RetryMiddleware has no publish_async_fn wired for an async route — "
                "the transient failure was nacked (requeued), NOT routed to a delay "
                "queue. Pass publish_async_fn=broker.publish, or set retry= on the "
                "route/broker and let rabbitkit wire it.",
                RuntimeWarning,
                stacklevel=2,
            )
            return

        if not _outcome_ok(await self._safe_publish_async(envelope)):
            logger.warning(
                "Retry publish failed or unverified; holding the delivery before requeueing it: routing_key=%s",
                envelope.routing_key,
            )
            backoff = self._record_handoff_failure(message)
            # R-D3: brake BEFORE the message can be redelivered. The delivery
            # is still held (unacked), so the broker cannot hand it — or, with
            # prefetch, a copy of the same failure — straight back; sleeping
            # after the nack (the old order) let the redelivery run in
            # parallel, ~930 handler executions/s against a deleted delay
            # queue. The sleep is bounded (backoff_max) and cancellable; the
            # nack runs in ``finally`` so shutdown still requeues it.
            try:
                if self._sleep_on_handoff_failure and backoff > 0 and not message.is_settled:
                    await asyncio.sleep(backoff)
            finally:
                # Delay-queue publish failed — DO NOT ack (see sync variant).
                if not message.is_settled:
                    await message.nack_async(requeue=True)
            return

        if not message.is_settled:
            await message.ack_async()
        self._record_retried(message, retry_count, envelope, classified.category)

    def _safe_publish_sync(self, envelope: MessageEnvelope) -> Any:
        assert self._publish_fn is not None
        try:
            return self._publish_fn(envelope)
        except Exception as exc_pub:
            # A RAISED publish is a handoff failure too (connection drop,
            # channel error) — same treatment as a not-ok outcome, and the
            # handoff may or may not have reached the broker (duplicates are
            # possible on recovery; the source is never acked here).
            logger.warning("Retry publish raised %s; treating as handoff failure", type(exc_pub).__name__)
            return None

    async def _safe_publish_async(self, envelope: MessageEnvelope) -> Any:
        assert self._publish_async_fn is not None
        try:
            return await self._publish_async_fn(envelope)
        except Exception as exc_pub:
            logger.warning("Retry publish raised %s; treating as handoff failure", type(exc_pub).__name__)
            return None

    def _record_retried(
        self, message: RabbitMessage, retry_count: int, envelope: MessageEnvelope, category: ErrorCategory
    ) -> None:
        self._record_handoff_success(message)
        if self._metrics_config is not None:
            self._record_metric(self._metrics_config.messages_retried_total, message, category)
        logger.info(
            "Retrying message (attempt %d/%d): routing_key=%s",
            retry_count + 1,
            self._config.max_retries,
            envelope.routing_key,
        )

    # ── terminal handoff ──────────────────────────────────────────────────

    def _terminal_envelope(
        self,
        exc: Exception,
        classified: ClassifiedError,
        category: FailureCategory,
        retry_count: int,
        message: RabbitMessage,
        have_publisher: bool,
    ) -> MessageEnvelope | None:
        if self._config.terminal_action is not TerminalAction.DEAD_LETTER or not have_publisher:
            return None
        return self._build_dlq_envelope(message, retry_count, exc, classified, category)

    def _terminal_sync(
        self,
        exc: Exception,
        classified: ClassifiedError,
        category: FailureCategory,
        retry_count: int,
        message: RabbitMessage,
    ) -> None:
        envelope = self._terminal_envelope(
            exc, classified, category, retry_count, message, self._publish_fn is not None
        )
        dead_lettered = False
        if envelope is not None and _outcome_ok(self._safe_publish_sync(envelope)):
            if not message.is_settled:
                message.ack()
            dead_lettered = True
        elif envelope is not None:
            self._log_dlq_fallback(envelope)
        self._mark_terminal_and_raise(
            exc,
            classified,
            retry_count,
            message,
            category,
            dead_lettered=dead_lettered,
            handoff_failed=envelope is not None and not dead_lettered,
        )

    async def _terminal_async(
        self,
        exc: Exception,
        classified: ClassifiedError,
        category: FailureCategory,
        retry_count: int,
        message: RabbitMessage,
    ) -> None:
        envelope = self._terminal_envelope(
            exc, classified, category, retry_count, message, self._publish_async_fn is not None
        )
        dead_lettered = False
        if envelope is not None and _outcome_ok(await self._safe_publish_async(envelope)):
            if not message.is_settled:
                await message.ack_async()
            dead_lettered = True
        elif envelope is not None:
            self._log_dlq_fallback(envelope)
        self._mark_terminal_and_raise(
            exc,
            classified,
            retry_count,
            message,
            category,
            dead_lettered=dead_lettered,
            handoff_failed=envelope is not None and not dead_lettered,
        )

    @staticmethod
    def _log_dlq_fallback(envelope: MessageEnvelope) -> None:
        logger.warning(
            "DLQ handoff to %r failed or unverified; handing the delivery back to the pipeline, which "
            "dead-letters it through the broker (reject) if the DLQ exists and requeues it otherwise. "
            "A broker dead-lettered copy carries only x-death, not the terminal failure metadata.",
            envelope.routing_key,
        )

    def _mark_terminal_and_raise(
        self,
        exc: Exception,
        classified: ClassifiedError,
        retry_count: int,
        message: RabbitMessage,
        category: FailureCategory,
        *,
        dead_lettered: bool = False,
        handoff_failed: bool = False,
    ) -> None:
        """Mark exception as terminal and re-raise.

        M2: this is the point where a message is committed to being
        dead-lettered -- permanent errors dead-letter on the first attempt,
        exhausted-retry errors dead-letter after ``max_retries`` -- so
        ``messages_dead_lettered_total`` is recorded here (labelled with the
        error category).

        With ``dead_lettered=True`` the confirmed DLQ copy exists and the
        source is already acked: ``_rabbitkit_dead_lettered`` tells the
        pipeline there is nothing left to settle. Otherwise the pipeline
        settles it: reject (``requeue=False``) so the source queue's DLX
        moves it, after checking that the DLQ exists where the transport can
        (a missing DLQ means requeue with backoff, never a drop).

        ``handoff_failed=True`` (``_rabbitkit_dead_letter_failed``) means a
        DLQ copy was attempted and NOT confirmed: an outer
        ``ExceptionMiddleware(swallow_permanent=True)`` must not swallow it,
        or the pipeline would ack a message that exists nowhere else.

        The log line uses the same sanitizer as the headers (F30): raw
        exception text routinely embeds credentials.
        """
        exc._rabbitkit_terminal = True  # type: ignore[attr-defined]
        exc._rabbitkit_dead_lettered = dead_lettered  # type: ignore[attr-defined]
        exc._rabbitkit_dead_letter_failed = handoff_failed  # type: ignore[attr-defined]
        if self._metrics_config is not None:
            self._record_metric(self._metrics_config.messages_dead_lettered_total, message, classified.category)
        sanitized = self._sanitizer.sanitize(exc, classified.severity)
        logger.warning(
            "Terminal failure (%s, category=%s, attempts=%d, retries=%d/%d, %s): %s: %s",
            category.value,
            classified.category.value,
            retry_count + 1,
            retry_count,
            self._config.max_retries,
            "dead-lettered with metadata" if dead_lettered else "handing back to the pipeline (reject to DLX)",
            sanitized.code,
            sanitized.summary or "<omitted>",
        )
        raise exc


def dlq_queue_definition(
    name: str, *, quorum: bool, broker_version: str | None, initial_group_size: int | None = None
) -> RabbitQueue:
    """The DLQ declaration both the retry router and the safety DLX use.

    A quorum DLQ on RabbitMQ 4.x gets ``x-delivery-limit: -1``. Without it
    the broker's default limit of 20 applies, and since rabbitkit's DLQs have
    no dead-letter exchange, the 21st requeue (20 peeks, say) DELETES the
    failed message the DLQ exists to keep. 3.x gets no argument: there, no
    limit is already unlimited and ``-1`` would drop on the first return.

    The argument can't be added to a queue that already exists (406), so
    brokers declare a DLQ that carries it only when the queue is missing;
    see :func:`rabbitkit.core.quorum.dlq_delivery_limit`.
    """
    if not quorum:
        return RabbitQueue(name=name, durable=True, queue_type=QueueType.CLASSIC)
    return RabbitQueue(
        name=name,
        durable=True,
        queue_type=QueueType.QUORUM,
        delivery_limit=dlq_delivery_limit(broker_version),
        quorum_initial_group_size=initial_group_size,
    )


class RetryRouter:
    """Declares delay queue topology at startup.

    Called by broker.start() for each route that has retry enabled.
    RetryRouter is the SINGLE OWNER of all retry/DLQ topology for a route.

    DLQ routing:
    - The source queue is re-declared with ``x-dead-letter-exchange=""``
      and ``x-dead-letter-routing-key=<dlq_name>`` so that messages
      rejected/nacked with ``requeue=False`` are automatically routed to
      the DLQ by RabbitMQ — no application-level routing needed.
    - Use ``get_source_queue_dlq_arguments()`` to obtain the extra arguments
      that must be added to the source queue declaration.
    """

    def __init__(self, config: RetryConfig) -> None:
        self._config = config

    def get_dlq_name(self, source_queue_name: str) -> str:
        """Return the DLQ name for a given source queue.

        Always per-queue — shared mode (per_queue=False) is rejected by
        RetryConfig (H3), so there is no shared-DLQ branch.
        """
        return f"{source_queue_name}.dlq"

    def get_source_queue_dlq_arguments(self, source_queue_name: str) -> dict[str, str]:
        """Return x-dead-letter arguments to add to the source queue declaration.

        When these arguments are present on the source queue, RabbitMQ
        automatically forwards messages that are rejected/nacked with
        requeue=False to the DLQ — making the DLQ actually reachable.
        """
        dlq_name = self.get_dlq_name(source_queue_name)
        return {
            "x-dead-letter-exchange": "",  # default exchange
            "x-dead-letter-routing-key": dlq_name,  # route directly by queue name
        }

    def get_delay_queue_definitions(
        self,
        source_queue_name: str,
        source_exchange_name: str,  # kept for signature stability (M5) — see docstring
        *,
        source_queue_type: QueueType | None = None,
        broker_version: str | None = None,
    ) -> list[RabbitQueue]:
        """Generate delay queue definitions for a source queue.

        *broker_version* (the RabbitMQ version string the connection
        reported) decides the DLQ's delivery limit when it is a quorum queue;
        see :func:`dlq_queue_definition`.

        Returns list of RabbitQueue objects for delay queues + DLQ.
        The DLQ is now reachable because ``get_source_queue_dlq_arguments()``
        wires the source queue's x-dead-letter-exchange to it.

        M5: on TTL expiry, a delay queue dead-letters back to the SOURCE
        QUEUE via the **default exchange** (``x-dead-letter-exchange=""``)
        with the queue's own name as the routing key — never the source
        queue's real exchange. On the default exchange a routing key that
        matches a queue's name always delivers directly to that queue,
        completely independent of how the queue is actually bound elsewhere.
        The previous version dead-lettered to ``source_exchange_name``
        using ``source_queue_name`` as the routing key — for a source queue
        bound to its real exchange via a topic pattern (e.g.
        ``orders.*.created``) rather than literally by its own name, that
        routing key almost never matches the binding, so the retried
        message silently vanished instead of coming back after the delay.
        ``source_exchange_name`` is intentionally unused now — kept as a
        parameter so existing call sites don't need updating.
        """
        queues: list[RabbitQueue] = []

        if self._config.jitter_mode == "sharded":
            multipliers = _shard_ttl_multipliers(self._config.jitter_shards, self._config.jitter_factor)
        else:
            multipliers = [1.0]

        for attempt in range(1, self._config.max_retries + 1):
            delay_ms = self._get_delay_ms(attempt - 1)

            # Always per-queue (shared mode is rejected by RetryConfig — H3).
            delay_type = self._resolve_queue_type(self._config.delay_queue_type, source_queue_type, default="classic")
            for shard, mult in enumerate(multipliers):
                arguments: dict[str, Any] = {
                    # Uniform per-queue TTL (head-of-line safety); shards
                    # stagger TTLs ACROSS queues, never within one (F4).
                    "x-message-ttl": max(1, int(delay_ms * mult)),
                    "x-dead-letter-exchange": "",  # default exchange (M5)
                    "x-dead-letter-routing-key": source_queue_name,
                    # Plan §5.1: configurable. The legacy default is
                    # CLASSIC (delay queues hold messages only for the
                    # delay window, have no consumers, and quorum queues
                    # historically had weaker per-message TTL behavior).
                    # "quorum" makes the whole retry chain replicated for
                    # critical work; "inherit" follows the source queue.
                    "x-queue-type": delay_type,
                }
                if delay_type == "quorum":
                    arguments.update(self._quorum_delay_arguments())
                queues.append(
                    RabbitQueue(
                        name=_shard_queue_name(source_queue_name, attempt, shard), durable=True, arguments=arguments
                    )
                )

        # DLQ — declared as a plain durable queue.
        # The source queue's x-dead-letter-exchange (set via
        # get_source_queue_dlq_arguments) routes nacked/rejected messages here.
        dlq_name = self.get_dlq_name(source_queue_name)
        # The DLQ inherits QUORUM from a quorum source: it stores failed
        # messages indefinitely — the exact data the replicated source queue
        # was chosen to protect. Leaving it classic silently downgrades those
        # messages to unreplicated single-node storage. (Streams are not
        # inherited: basic_get-based DLQ inspection/replay doesn't apply to
        # stream semantics.)
        dlq_type = self._resolve_queue_type(self._config.dlq_queue_type, source_queue_type, default="classic")
        queues.append(
            dlq_queue_definition(
                dlq_name,
                quorum=dlq_type == "quorum",
                broker_version=broker_version,
                initial_group_size=self._config.quorum_initial_group_size,
            )
        )

        return queues

    def _quorum_delay_arguments(self) -> dict[str, Any]:
        """Optional quorum arguments for delay queues (none by default, so an
        existing topology is never re-declared with different arguments)."""
        extra: dict[str, Any] = {}
        if self._config.quorum_initial_group_size is not None:
            extra["x-quorum-initial-group-size"] = self._config.quorum_initial_group_size
        strategy = self._config.delay_queue_dead_letter_strategy
        if strategy is not None:
            extra["x-dead-letter-strategy"] = strategy
            if strategy == "at-least-once":
                extra["x-overflow"] = "reject-publish"
        return extra

    @staticmethod
    def _resolve_queue_type(setting: str, source_queue_type: QueueType | None, *, default: str) -> str:
        """``"inherit"`` → the source queue's type (quorum/classic; streams
        fall back to *default* — a stream cannot be a delay/DLQ target)."""
        if setting == "inherit":
            if source_queue_type == QueueType.QUORUM:
                return "quorum"
            if source_queue_type == QueueType.CLASSIC:
                return "classic"
            return default
        return setting

    def _get_delay_ms(self, index: int) -> int:
        """Get delay in milliseconds for retry attempt."""
        delays = self._config.delays
        idx = min(index, len(delays) - 1)
        return delays[idx] * 1000
