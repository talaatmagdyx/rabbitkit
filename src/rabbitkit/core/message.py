"""Rich incoming message with runtime-aware ack/nack/reject.

See Contract 2 in the plan for sync/async ack design.

Sync transport sets _ack_fn. Async transport sets _ack_async_fn.
Pipeline calls the appropriate variant internally.
MANUAL mode handlers choose ack() or ack_async() based on their runtime.
Idempotent: double-ack is a no-op (guarded by _disposition state).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any


def is_rabbit_message_annotation(ann: Any) -> bool:
    """True if ``ann`` is/mentions :class:`RabbitMessage`.

    Handles both the resolved class and the string form (``"RabbitMessage"``)
    produced by ``from __future__ import annotations`` when the hint can't be
    resolved by ``typing.get_type_hints`` (e.g. a handler in a module that didn't
    import the name). Recognizing the string form prevents valid
    ``(body: bytes, msg: RabbitMessage)`` handlers from being mis-classified as
    having two body-like parameters / wrong body-type detection.
    """
    if ann is RabbitMessage:
        return True
    if isinstance(ann, str):
        return ann == "RabbitMessage" or ann.endswith(".RabbitMessage")
    return False


class RabbitMessage:
    """Rich incoming message with transport-aware settlement.

    The message object wraps raw AMQP delivery data and provides:
    - Typed access to headers, properties, routing info
    - Sync and async ack/nack/reject methods
    - Idempotent settlement (double-ack is a no-op)
    - Topic wildcard path extraction
    """

    __slots__ = (
        "_ack_async_fn",
        "_ack_fn",
        "_disposition",
        "_nack_async_fn",
        "_nack_fn",
        "_reject_async_fn",
        "_reject_fn",
        "app_id",
        "body",
        "consumer_tag",
        "content_encoding",
        "content_type",
        "correlation_id",
        "delivery_tag",
        "exchange",
        "headers",
        "message_id",
        "path",
        "raw_message",
        "redelivered",
        "reply_to",
        "routing_key",
        "timestamp",
        "type",
    )

    def __init__(
        self,
        *,
        body: bytes,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
        timestamp: datetime | None = None,
        type: str | None = None,  # noqa: A002 — AMQP property name
        app_id: str | None = None,
        routing_key: str = "",
        exchange: str = "",
        delivery_tag: int | None = None,
        redelivered: bool = False,
        consumer_tag: str | None = None,
        path: dict[str, str] | None = None,
        raw_message: Any = None,
    ) -> None:
        self.body = body
        self.headers: dict[str, Any] = headers or {}
        self.message_id = message_id
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.timestamp = timestamp
        self.type = type
        self.app_id = app_id
        self.routing_key = routing_key
        self.exchange = exchange
        self.delivery_tag = delivery_tag
        self.redelivered = redelivered
        self.consumer_tag = consumer_tag
        self.path: dict[str, str] = path or {}
        self.raw_message = raw_message

        # Transport-injected settlement functions (internal)
        self._ack_fn: Callable[[], None] | None = None
        self._ack_async_fn: Callable[[], Awaitable[None]] | None = None
        self._nack_fn: Callable[[bool], None] | None = None
        self._nack_async_fn: Callable[[bool], Awaitable[None]] | None = None
        self._reject_fn: Callable[[bool], None] | None = None
        self._reject_async_fn: Callable[[bool], Awaitable[None]] | None = None
        self._disposition: str = "pending"

    @property
    def is_settled(self) -> bool:
        """True if the message has been acked, nacked, or rejected."""
        return self._disposition != "pending"

    # ── Sync settlement ───────────────────────────────────────────────────

    def ack(self) -> None:
        """Synchronous ack. Raises RuntimeError on async-only transport.

        Sets disposition only after the transport call succeeds, so a failed
        ack (channel closed, frame error) leaves the message unsettled and the
        exception propagates to the recovery loop instead of being swallowed.
        """
        if self._disposition != "pending":
            return  # idempotent guard
        if self._ack_fn is None:
            msg = "Cannot sync-ack an async transport message. Use await msg.ack_async()."
            raise RuntimeError(msg)
        self._ack_fn()  # may raise — disposition stays "pending" on failure
        self._disposition = "acked"

    def nack(self, requeue: bool = True) -> None:
        """Synchronous nack. Raises RuntimeError on async-only transport."""
        if self._disposition != "pending":
            return
        if self._nack_fn is None:
            msg = "Cannot sync-nack an async transport message. Use await msg.nack_async()."
            raise RuntimeError(msg)
        self._nack_fn(requeue)
        self._disposition = "nacked"

    def reject(self, requeue: bool = False) -> None:
        """Synchronous reject. Raises RuntimeError on async-only transport."""
        if self._disposition != "pending":
            return
        if self._reject_fn is None:
            msg = "Cannot sync-reject an async transport message. Use await msg.reject_async()."
            raise RuntimeError(msg)
        self._reject_fn(requeue)
        self._disposition = "rejected"

    # ── Async settlement ──────────────────────────────────────────────────

    async def ack_async(self) -> None:
        """Async ack. Falls back to sync if async fn not set."""
        if self._disposition != "pending":
            return
        if self._ack_async_fn:
            await self._ack_async_fn()
        elif self._ack_fn:
            self._ack_fn()
        else:
            raise RuntimeError("Cannot async-ack: no settlement fn set. Use msg.ack() on a sync transport.")
        self._disposition = "acked"

    async def nack_async(self, requeue: bool = True) -> None:
        """Async nack. Falls back to sync if async fn not set."""
        if self._disposition != "pending":
            return
        if self._nack_async_fn:
            await self._nack_async_fn(requeue)
        elif self._nack_fn:
            self._nack_fn(requeue)
        else:
            raise RuntimeError("Cannot async-nack: no settlement fn set. Use msg.nack() on a sync transport.")
        self._disposition = "nacked"

    async def reject_async(self, requeue: bool = False) -> None:
        """Async reject. Falls back to sync if async fn not set."""
        if self._disposition != "pending":
            return
        if self._reject_async_fn:
            await self._reject_async_fn(requeue)
        elif self._reject_fn:
            self._reject_fn(requeue)
        else:
            raise RuntimeError("Cannot async-reject: no settlement fn set. Use msg.reject() on a sync transport.")
        self._disposition = "rejected"


# ── Exception-based ack control ──────────────────────────────────────────


class AckMessage(Exception):
    """Raise from handler to ack the message."""


class NackMessage(Exception):
    """Raise from handler to nack the message."""

    def __init__(self, requeue: bool = True) -> None:
        super().__init__()
        self.requeue = requeue


class RejectMessage(Exception):
    """Raise from handler to reject the message."""

    def __init__(self, requeue: bool = False) -> None:
        super().__init__()
        self.requeue = requeue
