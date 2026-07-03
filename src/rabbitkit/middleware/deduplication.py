"""DeduplicationMiddleware — idempotent message processing via Redis SETNX.

Checks whether a message has already been processed by storing a dedup key
in Redis with TTL.  Duplicate messages are silently acked and skipped.

If Redis is unavailable, behaviour depends on ``fallback_on_redis_error``:
  - ``True`` (default): process the message anyway (at-least-once)
  - ``False``: re-raise the Redis error (fail fast)

Mark policy (``DeduplicationConfig.mark_policy``):
  - ``"on_success"`` (default): check for the key before the handler (no
    write), mark it only after the handler returns successfully. Crash-safe:
    a consumer killed mid-handler (OOM/SIGKILL) leaves no mark, so the
    broker's redelivery is processed rather than dropped as a duplicate.
    Risk: concurrent deliveries of the same message may both pass the dedup
    check and both process (at-least-once).
  - ``"on_start"``: mark before calling the handler, preventing concurrent
    duplicate processing. WARNING — can cause MESSAGE LOSS: if the process
    crashes after marking but before the handler finishes, the broker's
    redelivery is skipped as a duplicate. Use only when duplicate execution
    is worse than losing a message. Also: if the handler fails and no
    RetryMiddleware is on the route (or the route's classifier calls it
    permanent), the retry may be skipped — see the H8 note below for the one
    case this middleware CAN detect and correct for.
  - ``"claim"``: two-state. Before the handler, atomically claim the key as
    ``in-flight`` with ``processing_timeout`` as its TTL; on success flip it
    to ``completed`` with the full ``ttl``. A concurrent duplicate that sees
    a live in-flight claim is handled per ``on_in_flight``:
    ``"nack_requeue"`` (default — the copy comes back and retries, so it is
    NOT lost if the claiming consumer dies) or ``"ack_skip"``. A crash
    mid-handler simply lets the claim expire, after which the redelivery is
    processed. Blocks concurrent duplicates AND is crash-safe — provided
    ``processing_timeout`` comfortably exceeds the worst-case handler
    duration; a handler that outlives its claim lets a duplicate start
    while it is still running.

Composing with RetryMiddleware (H8)
------------------------------------
If this middleware is OUTER of a ``RetryMiddleware`` on the same route (i.e.
listed before it in ``middlewares=[...]``), a transient failure that
``RetryMiddleware`` requeues (delay-queue publish, or nack+redeliver if that
publish itself failed) is invisible from here as an exception —
``RetryMiddleware`` deliberately swallows it so an outer
``ExceptionMiddleware`` doesn't treat a retry-in-progress as terminal. That
would otherwise look exactly like "the handler ran and returned `None`",
which under ``mark_policy="on_success"`` would incorrectly mark the message
as processed — dropping the later retry redelivery (same dedup key) as a
duplicate instead of actually processing it (silent message loss). Both
``consume_scope`` implementations here check for
``rabbitkit.core.types.REQUEUED_FOR_RETRY`` (the sentinel
``RetryMiddleware.consume_scope``/``consume_scope_async`` return instead of
``None`` in that case) and delete/skip the dedup key instead of marking it,
for both ``mark_policy`` values — including ``"on_start"``, where this
retroactively undoes the premature mark once retry signals a requeue. A
custom middleware that also wraps ``call_next`` and cares about this
distinction should check for the same sentinel.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.core.config import DeduplicationConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import REQUEUED_FOR_RETRY
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)

# Redis values for mark_policy="claim". Anything OTHER than the in-flight
# marker (including the legacy "1" written by on_success/on_start) is treated
# as completed, so switching an existing deployment to "claim" is safe.
_IN_FLIGHT = "in-flight"
_COMPLETED = "completed"


class DeduplicationMiddleware(BaseMiddleware):
    """Idempotent consumer middleware backed by Redis SETNX.

    Usage::

        import redis
        mw = DeduplicationMiddleware(
            redis_client=redis.Redis(),
            config=DeduplicationConfig(key_source="message_id", ttl=86400),
        )

    To prevent concurrent duplicate processing at the cost of retry safety::

        mw = DeduplicationMiddleware(
            redis_client=redis.Redis(),
            config=DeduplicationConfig(mark_policy="on_start"),
        )
    """

    def __init__(
        self,
        redis_client: Any,
        config: DeduplicationConfig | None = None,
        *,
        key_fn: Callable[[RabbitMessage], str] | None = None,
        metrics_collector: Any | None = None,
        metrics_config: Any | None = None,
    ) -> None:
        self._redis = redis_client
        self._config = config or DeduplicationConfig()
        self._key_fn = key_fn
        # M9: optional -- emits `dedup_fallback_total` every time a Redis
        # error causes this middleware to skip idempotency enforcement for a
        # message (fallback_on_redis_error=True, the default). None is a no-op.
        self._metrics_collector = metrics_collector
        self._metrics_config = metrics_config
        # Optional in-process LRU pre-filter — short-circuits Redis for keys we've
        # already confirmed as processed. Only allocated when local_cache_size > 0.
        # Evicts the oldest entry (FIFO) when capacity is reached.
        self._local_cache: OrderedDict[str, None] | None = (
            OrderedDict() if self._config.local_cache_size > 0 else None
        )
        # The cache is mutated from sync worker-pool daemon threads; OrderedDict
        # mutation is not atomic (move_to_end/popitem can corrupt mid-eviction).
        self._local_lock = threading.Lock()

    def _record_fallback(self, message: RabbitMessage) -> None:
        """M9: log at ERROR (not WARNING — idempotency being silently
        disabled for a message is an operational event worth alerting on,
        not routine noise) and emit `dedup_fallback_total` if a metrics
        collector is wired in."""
        logger.error(
            "Redis error during dedup check/mark; processing message anyway "
            "(fallback_on_redis_error=True) — idempotency is NOT enforced for "
            "this message. For workloads where a duplicate is unacceptable "
            "(e.g. financial), set fallback_on_redis_error=False to fail closed instead.",
            exc_info=True,
        )
        if self._metrics_collector is not None and self._metrics_config is not None:
            queue = message.headers.get("x-rabbitkit-original-queue") or message.routing_key or "unknown"
            self._metrics_collector.inc_counter(
                self._metrics_config.dedup_fallback_total, {"queue": str(queue)}
            )

    # ── Local LRU helpers ─────────────────────────────────────────────────

    def _local_is_dup(self, key: str) -> bool:
        """True if key is already in the local cache (= confirmed processed)."""
        if self._local_cache is None:
            return False
        with self._local_lock:
            return key in self._local_cache

    def _local_mark(self, key: str) -> None:
        """Record key in the local LRU; evicts oldest when at capacity."""
        if self._local_cache is None:
            return
        with self._local_lock:
            self._local_cache[key] = None
            self._local_cache.move_to_end(key)
            if len(self._local_cache) > self._config.local_cache_size:
                self._local_cache.popitem(last=False)

    def _local_remove(self, key: str) -> None:
        """Remove key from local cache (called when handler fails, key deleted from Redis)."""
        if self._local_cache is not None:
            with self._local_lock:
                self._local_cache.pop(key, None)

    def _cleanup_key_after_non_success(self, key: str) -> None:
        """Delete *key* from Redis + the local cache after a non-success
        (handler exception, or a requeue signaled via REQUEUED_FOR_RETRY —
        H8) so a later redelivery of the SAME message is not treated as a
        duplicate and dropped."""
        try:
            self._redis.delete(key)
        except Exception:
            logger.warning("Redis error during dedup key cleanup after non-success", exc_info=True)
        self._local_remove(key)

    async def _cleanup_key_after_non_success_async(self, key: str) -> None:
        """Async variant of :meth:`_cleanup_key_after_non_success`."""
        try:
            await self._redis.delete(key)
        except Exception:
            logger.warning("Redis error during dedup key cleanup after non-success", exc_info=True)
        self._local_remove(key)

    # ── Key extraction ────────────────────────────────────────────────────

    def _extract_key(self, message: RabbitMessage) -> str:
        """Build the dedup key from the message.

        Resolution:
        1. Custom ``key_fn`` (highest priority)
        2. ``config.key_source``:
           - ``"message_id"`` → ``message.message_id``
           - ``"correlation_id"`` → ``message.correlation_id``
           - ``"body_hash"`` → SHA-256 hex digest of ``message.body``
        If the selected id field is empty/None we fall back to a SHA-256 body
        hash (with a warning) instead of collapsing every id-less message to a
        single constant key.
        """
        if self._key_fn is not None:
            raw = self._key_fn(message)
        elif self._config.key_source == "message_id":
            raw = message.message_id or ""
        elif self._config.key_source == "correlation_id":
            raw = message.correlation_id or ""
        elif self._config.key_source == "body_hash":
            raw = hashlib.sha256(message.body).hexdigest()
        else:
            # Unknown key_source — fall back to message_id
            logger.warning(
                "Unknown key_source %r, falling back to message_id",
                self._config.key_source,
            )
            raw = message.message_id or ""

        # Empty raw key (id field missing) → fall back to body hash so distinct
        # id-less messages are NOT collapsed onto a single constant key.
        if not raw:
            logger.warning(
                "key_source=%r resolved to an empty id for message (routing_key=%r); falling back to body hash.",
                self._config.key_source,
                message.routing_key,
            )
            raw = hashlib.sha256(message.body).hexdigest()

        return f"{self._config.key_prefix}:{raw}"

    def _mark_key(self, key: str, message: RabbitMessage) -> bool:
        """Attempt to mark key as processed (sync). Returns True if this is a new key.

        Checks the local LRU cache first to avoid a Redis round-trip for keys
        we've already confirmed as processed in this process.
        """
        if self._local_is_dup(key):
            return False
        try:
            result = bool(self._redis.set(key, "1", nx=True, ex=self._config.ttl))
        except Exception:
            if not self._config.fallback_on_redis_error:
                raise
            self._record_fallback(message)
            return True
        if result:
            self._local_mark(key)
        return result

    async def _mark_key_async(self, key: str, message: RabbitMessage) -> bool:
        """Attempt to mark key as processed (async). Returns True if this is a new key.

        Checks the local LRU cache first to avoid a Redis round-trip for keys
        we've already confirmed as processed in this process.
        """
        if self._local_is_dup(key):
            return False
        try:
            result = bool(await self._redis.set(key, "1", nx=True, ex=self._config.ttl))
        except Exception:
            if not self._config.fallback_on_redis_error:
                raise
            self._record_fallback(message)
            return True
        if result:
            self._local_mark(key)
        return result

    # ── Consume-side hooks ────────────────────────────────────────────────

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        """Sync: check dedup → skip duplicate → call handler."""
        key = self._extract_key(message)

        if self._config.mark_policy == "claim":
            return self._consume_claim_sync(call_next, message, key)

        if self._config.mark_policy == "on_start":
            # _mark_key checks local cache first, then Redis
            is_new = self._mark_key(key, message)
            if not is_new:
                logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
                if not message.is_settled:
                    message.ack()
                return None
            result = call_next(message)
            if result is REQUEUED_FOR_RETRY:
                # H8: an inner RetryMiddleware requeued the failed handler
                # rather than succeeding — undo the premature on_start mark
                # so the retry redelivery is not dropped as a duplicate.
                self._cleanup_key_after_non_success(key)
            return result

        # mark_policy == "on_success": check (no write) → handler → mark.
        # The key is written only AFTER the handler returns successfully, so a
        # consumer killed mid-handler (OOM/SIGKILL) leaves no mark and the
        # broker's redelivery is processed instead of dropped as a duplicate.
        # A handler exception likewise leaves nothing behind — no cleanup
        # needed (and deleting here could erase a concurrent delivery's
        # legitimate success-mark).
        if self._local_is_dup(key):
            logger.debug("Duplicate message detected in local cache (key=%s); acking and skipping", key)
            if not message.is_settled:
                message.ack()
            return None

        try:
            already_seen = bool(self._redis.exists(key))
        except Exception:
            if not self._config.fallback_on_redis_error:
                raise
            self._record_fallback(message)
            return call_next(message)

        if already_seen:
            logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
            if not message.is_settled:
                message.ack()
            return None

        result = call_next(message)
        if result is REQUEUED_FOR_RETRY:
            # H8: an inner RetryMiddleware requeued the failed handler instead
            # of it actually succeeding. Nothing was marked yet, so the retry
            # redelivery (same dedup key) passes the dedup check — just skip
            # the mark.
            return result
        # Handler succeeded — mark now. Never raise past this point, even with
        # fallback_on_redis_error=False: the handler's side effects are done,
        # and raising would nack → redeliver → a GUARANTEED duplicate
        # execution, worse than the unmarked-key window it would signal.
        try:
            self._redis.set(key, "1", nx=True, ex=self._config.ttl)
        except Exception:
            self._record_fallback(message)
            return result
        self._local_mark(key)
        return result

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Async: check dedup → skip duplicate → call handler."""
        key = self._extract_key(message)

        if self._config.mark_policy == "claim":
            return await self._consume_claim_async(call_next, message, key)

        if self._config.mark_policy == "on_start":
            # _mark_key_async checks local cache first, then Redis
            is_new = await self._mark_key_async(key, message)
            if not is_new:
                logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
                if not message.is_settled:
                    await message.ack_async()
                return None
            result = await call_next(message)
            if result is REQUEUED_FOR_RETRY:
                # H8: an inner RetryMiddleware requeued the failed handler
                # rather than succeeding — undo the premature on_start mark
                # so the retry redelivery is not dropped as a duplicate.
                await self._cleanup_key_after_non_success_async(key)
            return result

        # mark_policy == "on_success": check (no write) → handler → mark.
        # See the sync variant above for the crash-safety rationale.
        if self._local_is_dup(key):
            logger.debug("Duplicate message detected in local cache (key=%s); acking and skipping", key)
            if not message.is_settled:
                await message.ack_async()
            return None

        try:
            already_seen = bool(await self._redis.exists(key))
        except Exception:
            if not self._config.fallback_on_redis_error:
                raise
            self._record_fallback(message)
            return await call_next(message)

        if already_seen:
            logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
            if not message.is_settled:
                await message.ack_async()
            return None

        result = await call_next(message)
        if result is REQUEUED_FOR_RETRY:
            # H8: an inner RetryMiddleware requeued the failed handler instead
            # of it actually succeeding. Nothing was marked yet, so the retry
            # redelivery (same dedup key) passes the dedup check — just skip
            # the mark.
            return result
        # Handler succeeded — mark now. Never raise past this point, even with
        # fallback_on_redis_error=False: raising would nack → redeliver → a
        # GUARANTEED duplicate execution.
        try:
            await self._redis.set(key, "1", nx=True, ex=self._config.ttl)
        except Exception:
            self._record_fallback(message)
            return result
        self._local_mark(key)
        return result

    # ── mark_policy == "claim" ────────────────────────────────────────────

    @staticmethod
    def _is_in_flight(raw: Any) -> bool:
        """True when a GET result is a live in-flight claim.

        ``None`` (the key expired between the failed SET NX and this GET)
        also counts — requeueing lets the redelivery claim it cleanly.
        Any other value (``"completed"``, or the legacy ``"1"`` written by
        on_success/on_start deployments) means completed.
        """
        if raw is None:
            return True
        value = raw.decode() if isinstance(raw, bytes) else raw
        return bool(value == _IN_FLIGHT)

    def _handle_in_flight_duplicate_sync(self, message: RabbitMessage, key: str) -> None:
        """A concurrent copy hit another consumer's live claim (sync)."""
        if self._config.on_in_flight == "ack_skip":
            logger.debug("Duplicate of in-flight message (key=%s); acking and skipping", key)
            if not message.is_settled:
                message.ack()
            return
        # "nack_requeue" (default): the copy comes back and retries, so it is
        # NOT lost if the claiming consumer dies mid-handler.
        # ponytail: immediate requeue — the duplicate redelivers in a tight
        # loop until the claim resolves, bounded by prefetch and the
        # handler's duration; add a delay queue if that churn ever matters.
        logger.debug("Duplicate of in-flight message (key=%s); nack-requeueing", key)
        if not message.is_settled:
            message.nack(requeue=True)

    def _consume_claim_sync(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
        key: str,
    ) -> Any:
        """Sync claim flow: atomically claim in-flight → handler → flip to
        completed. Crash mid-handler lets the claim expire; the redelivery
        then re-claims and processes."""
        if self._local_is_dup(key):
            logger.debug("Duplicate message detected in local cache (key=%s); acking and skipping", key)
            if not message.is_settled:
                message.ack()
            return None

        try:
            claimed = bool(
                self._redis.set(key, _IN_FLIGHT, nx=True, ex=self._config.processing_timeout)
            )
        except Exception:
            if not self._config.fallback_on_redis_error:
                raise
            self._record_fallback(message)
            return call_next(message)

        if not claimed:
            try:
                raw = self._redis.get(key)
            except Exception:
                if not self._config.fallback_on_redis_error:
                    raise
                self._record_fallback(message)
                return call_next(message)
            if self._is_in_flight(raw):
                self._handle_in_flight_duplicate_sync(message, key)
                return None
            logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
            if not message.is_settled:
                message.ack()
            return None

        try:
            result = call_next(message)
        except Exception:
            # Release the claim so a retry redelivery can re-claim immediately
            # instead of waiting out processing_timeout.
            self._cleanup_key_after_non_success(key)
            raise
        if result is REQUEUED_FOR_RETRY:
            # H8: release the claim for the delayed retry redelivery.
            self._cleanup_key_after_non_success(key)
            return result
        # Handler succeeded — flip the claim to completed with the full TTL.
        # Never raise past this point (side effects are committed; raising
        # would nack → redeliver → a guaranteed duplicate execution).
        try:
            self._redis.set(key, _COMPLETED, ex=self._config.ttl)
        except Exception:
            self._record_fallback(message)
            return result
        self._local_mark(key)
        return result

    async def _handle_in_flight_duplicate_async(self, message: RabbitMessage, key: str) -> None:
        """A concurrent copy hit another consumer's live claim (async)."""
        if self._config.on_in_flight == "ack_skip":
            logger.debug("Duplicate of in-flight message (key=%s); acking and skipping", key)
            if not message.is_settled:
                await message.ack_async()
            return
        logger.debug("Duplicate of in-flight message (key=%s); nack-requeueing", key)
        if not message.is_settled:
            await message.nack_async(requeue=True)

    async def _consume_claim_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
        key: str,
    ) -> Any:
        """Async variant of :meth:`_consume_claim_sync`."""
        if self._local_is_dup(key):
            logger.debug("Duplicate message detected in local cache (key=%s); acking and skipping", key)
            if not message.is_settled:
                await message.ack_async()
            return None

        try:
            claimed = bool(
                await self._redis.set(key, _IN_FLIGHT, nx=True, ex=self._config.processing_timeout)
            )
        except Exception:
            if not self._config.fallback_on_redis_error:
                raise
            self._record_fallback(message)
            return await call_next(message)

        if not claimed:
            try:
                raw = await self._redis.get(key)
            except Exception:
                if not self._config.fallback_on_redis_error:
                    raise
                self._record_fallback(message)
                return await call_next(message)
            if self._is_in_flight(raw):
                await self._handle_in_flight_duplicate_async(message, key)
                return None
            logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
            if not message.is_settled:
                await message.ack_async()
            return None

        try:
            result = await call_next(message)
        except Exception:
            await self._cleanup_key_after_non_success_async(key)
            raise
        if result is REQUEUED_FOR_RETRY:
            await self._cleanup_key_after_non_success_async(key)
            return result
        try:
            await self._redis.set(key, _COMPLETED, ex=self._config.ttl)
        except Exception:
            self._record_fallback(message)
            return result
        self._local_mark(key)
        return result
