"""DeduplicationMiddleware — idempotent message processing via Redis SETNX.

Checks whether a message has already been processed by storing a dedup key
in Redis with TTL.  Duplicate messages are silently acked and skipped.

If Redis is unavailable, behaviour depends on ``fallback_on_redis_error``:
  - ``True`` (default): process the message anyway (at-least-once)
  - ``False``: re-raise the Redis error (fail fast)

Mark policy (``DeduplicationConfig.mark_policy``):
  - ``"on_success"`` (default): mark the key only after the handler succeeds.
    Safer for retry flows — a failed handler can be retried. Risk: concurrent
    delivery of the same message may both pass the dedup check.
  - ``"on_start"``: mark before calling the handler, preventing concurrent
    duplicate processing. Risk: if the handler fails and no RetryMiddleware
    is on the route (or the route's classifier calls it permanent), the
    retry may be skipped — see the H8 note below for the one case this
    middleware CAN detect and correct for.

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
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.core.config import DeduplicationConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import REQUEUED_FOR_RETRY
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)


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
        return key in self._local_cache

    def _local_mark(self, key: str) -> None:
        """Record key in the local LRU; evicts oldest when at capacity."""
        if self._local_cache is None:
            return
        self._local_cache[key] = None
        self._local_cache.move_to_end(key)
        if len(self._local_cache) > self._config.local_cache_size:
            self._local_cache.popitem(last=False)

    def _local_remove(self, key: str) -> None:
        """Remove key from local cache (called when handler fails, key deleted from Redis)."""
        if self._local_cache is not None:
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

        # mark_policy == "on_success": local cache check, then Redis, then handler
        if self._local_is_dup(key):
            logger.debug("Duplicate message detected in local cache (key=%s); acking and skipping", key)
            if not message.is_settled:
                message.ack()
            return None

        try:
            already_seen = not bool(self._redis.set(key, "1", nx=True, ex=self._config.ttl))
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

        try:
            result = call_next(message)
        except Exception:
            # Handler failed — delete the key so a retry can re-enter
            self._cleanup_key_after_non_success(key)
            raise
        if result is REQUEUED_FOR_RETRY:
            # H8: an inner RetryMiddleware requeued the failed handler
            # (delay-queue publish, or nack+redeliver) instead of it actually
            # succeeding. Delete the key so the retry redelivery (same dedup
            # key) is NOT treated as a duplicate and silently dropped.
            self._cleanup_key_after_non_success(key)
            return result
        # Handler succeeded — record in local cache so next duplicate skips Redis
        self._local_mark(key)
        return result

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Async: check dedup → skip duplicate → call handler."""
        key = self._extract_key(message)

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

        # mark_policy == "on_success": local cache check, then Redis, then handler
        if self._local_is_dup(key):
            logger.debug("Duplicate message detected in local cache (key=%s); acking and skipping", key)
            if not message.is_settled:
                await message.ack_async()
            return None

        try:
            already_seen = not bool(await self._redis.set(key, "1", nx=True, ex=self._config.ttl))
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

        try:
            result = await call_next(message)
        except Exception:
            # Handler failed — delete the key so a retry can re-enter
            await self._cleanup_key_after_non_success_async(key)
            raise
        if result is REQUEUED_FOR_RETRY:
            # H8: an inner RetryMiddleware requeued the failed handler
            # (delay-queue publish, or nack+redeliver) instead of it actually
            # succeeding. Delete the key so the retry redelivery (same dedup
            # key) is NOT treated as a duplicate and silently dropped.
            await self._cleanup_key_after_non_success_async(key)
            return result
        # Handler succeeded — record in local cache so next duplicate skips Redis
        self._local_mark(key)
        return result
