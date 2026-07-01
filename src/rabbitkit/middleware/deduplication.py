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
    duplicate processing. Risk: if the handler fails the retry may be skipped.
"""

from __future__ import annotations

import hashlib
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.core.config import DeduplicationConfig
from rabbitkit.core.message import RabbitMessage
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
    ) -> None:
        self._redis = redis_client
        self._config = config or DeduplicationConfig()
        self._key_fn = key_fn
        # Optional in-process LRU pre-filter — short-circuits Redis for keys we've
        # already confirmed as processed. Only allocated when local_cache_size > 0.
        # Evicts the oldest entry (FIFO) when capacity is reached.
        self._local_cache: OrderedDict[str, None] | None = (
            OrderedDict() if self._config.local_cache_size > 0 else None
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

    def _mark_key(self, key: str) -> bool:
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
            logger.warning("Redis error during dedup mark; processing message anyway", exc_info=True)
            return True
        if result:
            self._local_mark(key)
        return result

    async def _mark_key_async(self, key: str) -> bool:
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
            logger.warning("Redis error during dedup mark; processing message anyway", exc_info=True)
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
            is_new = self._mark_key(key)
            if not is_new:
                logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
                if not message.is_settled:
                    message.ack()
                return None
            return call_next(message)

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
            logger.warning("Redis error during dedup check; processing message anyway", exc_info=True)
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
            try:
                self._redis.delete(key)
            except Exception:
                logger.warning("Redis error during dedup key cleanup after handler failure", exc_info=True)
            raise
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
            is_new = await self._mark_key_async(key)
            if not is_new:
                logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
                if not message.is_settled:
                    await message.ack_async()
                return None
            return await call_next(message)

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
            logger.warning("Redis error during dedup check; processing message anyway", exc_info=True)
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
            try:
                await self._redis.delete(key)
            except Exception:
                logger.warning("Redis error during dedup key cleanup after handler failure", exc_info=True)
            raise
        # Handler succeeded — record in local cache so next duplicate skips Redis
        self._local_mark(key)
        return result
