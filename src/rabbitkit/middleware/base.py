"""Base middleware protocol — lifecycle hooks for message processing.

Both sync and async variants provided.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope


class BaseMiddleware:
    """Base middleware with lifecycle hooks.

    Hooks:
    - on_receive(msg): notification when message is received (before processing)
    - consume_scope(call_next, msg): wrap handler execution
    - after_processed(msg, exc): post-processing notification
    - publish_scope(call_next, envelope): wrap outgoing publish

    All hooks have no-op defaults. Subclasses override what they need.
    Both sync and async variants provided.
    """

    # ── Consume-side hooks ───────────────────────────────────────────────

    def on_receive(self, message: RabbitMessage) -> None:
        """Called when a message is received, before processing.

        H7 — two things every on_receive override must account for:

        1. Runs in a fixed, flat pre-pass entirely BEFORE consume_scope is
           entered for ANY middleware on the route. An exception raised here
           is NOT caught by any middleware's consume_scope — not even
           RetryMiddleware's — so it is never retry-eligible via the
           delay-queue mechanism; it settles per the route's AckPolicy using
           the pipeline's default classifier instead. If your check should be
           retryable, implement it in consume_scope, not here.
        2. Runs in REVERSE registration order — the mirror of publish_scope's
           outer→inner composition — so a receive-side transform that undoes
           a publish-side one (e.g. decompress undoing compress) runs in the
           mathematically correct relative order. This does NOT mean any two
           on_receive-based middlewares can be listed in either order and
           both work — e.g. SigningMiddleware + CompressionMiddleware only
           work as ``[CompressionMiddleware, SigningMiddleware]`` (compression
           outer), because the signature covers content_encoding, a field
           compression itself sets. See HandlerPipeline._run_consume_sync's
           docstring for the full explanation.
        """

    async def on_receive_async(self, message: RabbitMessage) -> None:
        """Async variant of on_receive."""
        self.on_receive(message)

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        """Wrap handler execution (sync). Must call call_next(message)."""
        return call_next(message)

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Wrap handler execution (async). Must call await call_next(message)."""
        return await call_next(message)

    def after_processed(
        self,
        message: RabbitMessage,
        exc: BaseException | None = None,
    ) -> None:
        """Called after message processing completes (success or failure)."""

    async def after_processed_async(
        self,
        message: RabbitMessage,
        exc: BaseException | None = None,
    ) -> None:
        """Async variant of after_processed."""
        self.after_processed(message, exc)

    # ── Publish-side hooks ───────────────────────────────────────────────

    def publish_scope(
        self,
        call_next: Callable[[MessageEnvelope], Any],
        envelope: MessageEnvelope,
    ) -> Any:
        """Wrap outgoing publish (sync). Must call call_next(envelope)."""
        return call_next(envelope)

    async def publish_scope_async(
        self,
        call_next: Callable[[MessageEnvelope], Awaitable[Any]],
        envelope: MessageEnvelope,
    ) -> Any:
        """Wrap outgoing publish (async). Must call await call_next(envelope)."""
        return await call_next(envelope)


class NoOpMiddleware(BaseMiddleware):
    """Null Object middleware — zero-overhead pass-through.

    Use as a default when no middleware is configured, eliminating
    ``if collector is None: return call_next(...)`` branches on the hot path.
    """

    def consume_scope(self, call_next: Any, message: Any) -> Any:
        return call_next(message)

    async def consume_scope_async(self, call_next: Any, message: Any) -> Any:
        return await call_next(message)

    def publish_scope(self, call_next: Any, envelope: Any) -> Any:
        return call_next(envelope)

    async def publish_scope_async(self, call_next: Any, envelope: Any) -> Any:
        return await call_next(envelope)
