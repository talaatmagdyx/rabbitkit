"""RetryMiddleware — routes failed messages to delay queues for retry.

TOPOLOGY SPEC (see Contract in plan):
- Per-queue delay queues: {source_queue}.retry.{attempt}
- Dead letter queue: {source_queue}.dlq
- Shared mode: rabbitkit.retry.{attempt}, rabbitkit.dlq

Mechanism: TTL + DLX (dead-letter exchange)
"""

from __future__ import annotations

import logging
import random
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from rabbitkit.core.config import RetryConfig
from rabbitkit.core.errors import ErrorPredicate
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import REQUEUED_FOR_RETRY, ErrorSeverity, MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.middleware.error_classifier import ErrorClassifierMiddleware

logger = logging.getLogger(__name__)


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
    return [1.0] + [
        1.0 + jitter_factor * (-1.0 + 2.0 * i / (rest - 1)) for i in range(rest)
    ]


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


def warn_retry_without_confirms(route_name: str, *, context: str = "retry") -> None:
    """Warn when a route republishes internally (retry delay-queue, or a
    ``@publisher()`` result forward) but its broker publishes with
    ``PublisherConfig.confirm_delivery=False`` (M4).

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
    ) -> None:
        self._config = config
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

    def _record_metric(self, metric_name: str | None, message: RabbitMessage) -> None:
        if self._metrics_collector is None or metric_name is None:
            return
        queue = message.headers.get("x-rabbitkit-original-queue") or message.routing_key or "unknown"
        self._metrics_collector.inc_counter(metric_name, {"queue": str(queue)})

    @property
    def config(self) -> RetryConfig:
        return self._config

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
        ``_mark_terminal_and_raise``, so reaching this ``return`` unambiguously
        means "requeued" — an outer middleware (e.g. DeduplicationMiddleware)
        MUST NOT treat this the same as the handler actually succeeding.
        """
        try:
            return call_next(message)
        except Exception as exc:
            self._handle_retry_sync(exc, message)
            return REQUEUED_FOR_RETRY

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Async retry scope. See :meth:`consume_scope` (H8) for why this
        returns ``REQUEUED_FOR_RETRY`` rather than ``None``."""
        try:
            return await call_next(message)
        except Exception as exc:
            await self._handle_retry_async(exc, message)
            return REQUEUED_FOR_RETRY

    def _handle_retry_sync(self, exc: Exception, message: RabbitMessage) -> None:
        """Handle exception in sync context."""
        classified = self._classifier.classify(exc)
        retry_count = self._get_retry_count(message)

        if classified.severity == ErrorSeverity.TRANSIENT and retry_count < self._config.max_retries:
            # Route to delay queue
            self._route_to_delay_queue_sync(message, retry_count)
            return

        # Terminal: permanent or exhausted
        self._mark_terminal_and_raise(exc, classified.severity, retry_count, message)

    async def _handle_retry_async(self, exc: Exception, message: RabbitMessage) -> None:
        """Handle exception in async context."""
        classified = self._classifier.classify(exc)
        retry_count = self._get_retry_count(message)

        if classified.severity == ErrorSeverity.TRANSIENT and retry_count < self._config.max_retries:
            await self._route_to_delay_queue_async(message, retry_count)
            return

        self._mark_terminal_and_raise(exc, classified.severity, retry_count, message)

    def _get_retry_count(self, message: RabbitMessage) -> int:
        """Get current retry count from message headers, clamped to
        ``[0, max_retries]`` (H5).

        The header is read verbatim from an inbound AMQP message — nothing
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
        header claims, independent of its configured value being read from a
        trusted or untrusted source. A malformed (non-numeric) header value
        is treated the same as missing (0) rather than raising, so a garbage
        header degrades to "start of the retry sequence" instead of crashing
        the pipeline.

        For a broker-enforced backstop on top of this (e.g. against a
        misbehaving consumer that never expires/dead-letters a message),
        prefer quorum source queues with ``x-delivery-limit`` — see
        ``docs/retry-and-dlq.md``.
        """
        raw = message.headers.get(self._config.retry_header, 0)
        try:
            retry_count = int(raw)
        except (TypeError, ValueError):
            retry_count = 0
        return max(0, min(retry_count, self._config.max_retries))

    def _compute_delay(self, retry_count: int) -> int:
        """Compute delay for this retry attempt (with jitter)."""
        delays = self._config.delays
        idx = min(retry_count, len(delays) - 1)
        base_delay = delays[idx]

        # Apply jitter
        jitter = base_delay * self._config.jitter_factor
        return max(1, int(base_delay + random.uniform(-jitter, jitter)))  # noqa: S311 — jitter, not crypto

    def _build_retry_envelope(self, message: RabbitMessage, retry_count: int) -> MessageEnvelope:
        """Build envelope for the delay queue."""
        # Determine delay queue name
        attempt = retry_count + 1
        # Always per-queue (shared mode is rejected by RetryConfig — H3).
        source_queue = message.headers.get("x-rabbitkit-original-queue", "unknown")
        if self._config.jitter_mode == "sharded":
            shard = _shard_index(message.message_id or "", self._config.jitter_shards)
            delay_queue_rk = _shard_queue_name(str(source_queue), attempt, shard)
        else:
            delay_queue_rk = f"{source_queue}.retry.{attempt}"

        # Preserve original headers + add retry metadata
        headers = dict(message.headers)
        headers[self._config.retry_header] = retry_count + 1
        if "x-rabbitkit-original-exchange" not in headers:
            headers["x-rabbitkit-original-exchange"] = message.exchange
        if "x-rabbitkit-original-routing-key" not in headers:
            headers["x-rabbitkit-original-routing-key"] = message.routing_key
        if "x-rabbitkit-original-queue" not in headers:
            headers["x-rabbitkit-original-queue"] = ""  # set by broker at consume time

        return MessageEnvelope(
            routing_key=delay_queue_rk,
            body=message.body,
            exchange="",  # direct to delay queue by name
            headers=headers,
            message_id=message.message_id or "",
            correlation_id=message.correlation_id,
            content_type=message.content_type or "application/octet-stream",
            content_encoding=message.content_encoding,
            # Preserve the remaining original message properties -- these used
            # to be silently dropped on every retry republish, so e.g. a
            # priority-queue message lost its priority on its first retry, and
            # an RPC request's reply_to/type/app_id/user_id never survived
            # long enough for the eventual (retried) reply to route back.
            reply_to=message.reply_to,
            priority=message.priority,
            expiration=message.expiration,
            type=message.type,
            app_id=message.app_id,
            user_id=message.user_id,
            # M4: mandatory so a runtime-deleted/missing delay queue comes back
            # as RETURNED (outcome not-ok) instead of being broker-confirmed
            # into the void. The route-to-delay-queue path checks outcome.ok
            # and nack-requeues on failure, so this turns silent loss into a
            # redelivery. (Requires publisher confirms + basic.return handling,
            # which both transports wire up.)
            mandatory=True,
        )

    def _route_to_delay_queue_sync(self, message: RabbitMessage, retry_count: int) -> None:
        """Publish to delay queue and ack source (sync)."""
        envelope = self._build_retry_envelope(message, retry_count)

        if self._publish_fn is not None:
            outcome = self._publish_fn(envelope)
            if outcome is not None and not outcome.ok:
                # Delay-queue publish failed — DO NOT ack, or the message is
                # lost forever (never retried, never dead-lettered). Nack with
                # requeue so the broker redelivers it.
                if not message.is_settled:
                    message.nack(requeue=True)
                logger.warning(
                    "Retry publish failed; nacked for redelivery: routing_key=%s",
                    envelope.routing_key,
                )
                return

        # Ack source message (it's safely in the delay queue now)
        if not message.is_settled:
            message.ack()

        if self._metrics_config is not None:
            self._record_metric(self._metrics_config.messages_retried_total, message)

        logger.info(
            "Retrying message (attempt %d/%d): routing_key=%s",
            retry_count + 1,
            self._config.max_retries,
            envelope.routing_key,
        )

    async def _route_to_delay_queue_async(self, message: RabbitMessage, retry_count: int) -> None:
        """Publish to delay queue and ack source (async)."""
        envelope = self._build_retry_envelope(message, retry_count)

        if self._publish_async_fn is not None:
            outcome = await self._publish_async_fn(envelope)
            if outcome is not None and not outcome.ok:
                # Delay-queue publish failed — DO NOT ack (see sync variant).
                if not message.is_settled:
                    await message.nack_async(requeue=True)
                logger.warning(
                    "Retry publish failed; nacked for redelivery: routing_key=%s",
                    envelope.routing_key,
                )
                return

        if not message.is_settled:
            await message.ack_async()

        if self._metrics_config is not None:
            self._record_metric(self._metrics_config.messages_retried_total, message)

        logger.info(
            "Retrying message (attempt %d/%d): routing_key=%s",
            retry_count + 1,
            self._config.max_retries,
            envelope.routing_key,
        )

    def _mark_terminal_and_raise(
        self,
        exc: Exception,
        severity: ErrorSeverity,
        retry_count: int,
        message: RabbitMessage,
    ) -> None:
        """Mark exception as terminal and re-raise.

        M2: this is the point where a message is committed to being
        dead-lettered -- permanent errors dead-letter on the first attempt,
        exhausted-retry errors dead-letter after ``max_retries`` -- so
        ``messages_dead_lettered_total`` is recorded here rather than at the
        actual reject() call (which happens later, in the pipeline's
        exception handling, and doesn't know WHY the reject is happening).
        """
        exc._rabbitkit_terminal = True  # type: ignore[attr-defined]
        if self._metrics_config is not None:
            self._record_metric(self._metrics_config.messages_dead_lettered_total, message)
        logger.warning(
            "Terminal failure (%s, retries=%d/%d): %s: %s",
            severity.value,
            retry_count,
            self._config.max_retries,
            type(exc).__name__,
            exc,
        )
        raise


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
            "x-dead-letter-exchange": "",          # default exchange
            "x-dead-letter-routing-key": dlq_name, # route directly by queue name
        }

    def get_delay_queue_definitions(
        self,
        source_queue_name: str,
        source_exchange_name: str,  # kept for signature stability (M5) — see docstring
    ) -> list[RabbitQueue]:
        """Generate delay queue definitions for a source queue.

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
            for shard, mult in enumerate(multipliers):
                queue = RabbitQueue(
                    name=_shard_queue_name(source_queue_name, attempt, shard),
                    durable=True,
                    arguments={
                        # Uniform per-queue TTL (head-of-line safety); shards
                        # stagger TTLs ACROSS queues, never within one (F4).
                        "x-message-ttl": max(1, int(delay_ms * mult)),
                        "x-dead-letter-exchange": "",  # default exchange (M5)
                        "x-dead-letter-routing-key": source_queue_name,
                        "x-queue-type": "classic",  # classic for delay queues
                    },
                )
                queues.append(queue)

        # DLQ — declared as a plain durable queue.
        # The source queue's x-dead-letter-exchange (set via
        # get_source_queue_dlq_arguments) routes nacked/rejected messages here.
        dlq_name = self.get_dlq_name(source_queue_name)
        dlq = RabbitQueue(
            name=dlq_name,
            durable=True,
        )
        queues.append(dlq)

        return queues

    def _get_delay_ms(self, index: int) -> int:
        """Get delay in milliseconds for retry attempt."""
        delays = self._config.delays
        idx = min(index, len(delays) - 1)
        return delays[idx] * 1000
