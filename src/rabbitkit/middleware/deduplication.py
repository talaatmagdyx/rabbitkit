"""DeduplicationMiddleware — idempotent message processing via Redis SETNX.

Checks whether a message has already been processed by storing a dedup key
in Redis with TTL.  Duplicate messages are silently acked and skipped.

If Redis is unavailable, behaviour depends on ``fallback_on_redis_error``:
  - ``True`` (default): process the message anyway (at-least-once)
  - ``False``: re-raise the Redis error (fail fast)
"""

from __future__ import annotations

import hashlib
import logging
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

    # ── Key extraction ────────────────────────────────────────────────────

    def _extract_key(self, message: RabbitMessage) -> str:
        """Build the dedup key from the message.

        Resolution:
        1. Custom ``key_fn`` (highest priority)
        2. ``config.key_source``:
           - ``"message_id"`` → ``message.message_id``
           - ``"correlation_id"`` → ``message.correlation_id``
           - ``"body_hash"`` → SHA-256 hex digest of ``message.body``
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

        return f"{self._config.key_prefix}:{raw}"

    # ── Consume-side hooks ────────────────────────────────────────────────

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        """Sync: check dedup → skip duplicate → call handler."""
        key = self._extract_key(message)

        try:
            is_new = self._redis.set(key, "1", nx=True, ex=self._config.ttl)
        except Exception:
            if not self._config.fallback_on_redis_error:
                raise
            logger.warning("Redis error during dedup check; processing message anyway", exc_info=True)
            return call_next(message)

        if is_new is None:
            # Duplicate — ack and skip
            logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
            if not message.is_settled:
                message.ack()
            return None

        return call_next(message)

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Async: check dedup → skip duplicate → call handler."""
        key = self._extract_key(message)

        try:
            is_new = await self._redis.set(key, "1", nx=True, ex=self._config.ttl)
        except Exception:
            if not self._config.fallback_on_redis_error:
                raise
            logger.warning("Redis error during dedup check; processing message anyway", exc_info=True)
            return await call_next(message)

        if is_new is None:
            # Duplicate
            logger.debug("Duplicate message detected (key=%s); acking and skipping", key)
            if not message.is_settled:
                await message.ack_async()
            return None

        return await call_next(message)
