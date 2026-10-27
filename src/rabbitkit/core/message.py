"""Rich incoming message with runtime-aware ack/nack/reject.

See Contract 2 in the plan for sync/async ack design.

Sync transport sets _ack_fn. Async transport sets _ack_async_fn.
Pipeline calls the appropriate variant internally.
MANUAL mode handlers choose ack() or ack_async() based on their runtime.
Idempotent: double-ack is a no-op (guarded by _disposition state). The async
variants claim the disposition before awaiting the transport, so concurrent
settlements never send two frames for one delivery tag (G-15).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any


class SettlementError(RuntimeError):
    """Invalid message-settlement call: calling the sync settlement methods
    (``ack()`` ...) on a message from an async-only transport, or the async
    variants (``ack_async()`` ...) on a message with no settlement function
    wired at all.

    Defined here rather than in ``core/errors.py`` because ``errors.py``
    (indirectly, via ``types.py``) imports this module -- it is re-exported
    from ``rabbitkit.core.errors`` and the package root, which are the
    import paths to use. Subclasses ``RuntimeError`` so pre-existing
    ``except RuntimeError`` handlers keep working.
    """


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
        "_channel_alive",
        "_disposition",
        "_nack_async_fn",
        "_nack_fn",
        "_reject_async_fn",
        "_reject_fn",
        "_settle_unknown",
        "app_id",
        "body",
        "consumer_tag",
        "content_encoding",
        "content_type",
        "correlation_id",
        "delivery_mode",
        "delivery_tag",
        "exchange",
        "expiration",
        "headers",
        "message_id",
        "path",
        "priority",
        "raw_message",
        "redelivered",
        "reply_to",
        "routing_key",
        "timestamp",
        "type",
        "user_id",
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
        priority: int | None = None,
        expiration: str | None = None,
        user_id: str | None = None,
        delivery_mode: int | None = None,
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
        self.priority = priority
        self.expiration = expiration
        self.user_id = user_id
        # 1 = transient, 2 = persistent, None = the publisher set none
        # (the broker treats that as transient).
        self.delivery_mode = delivery_mode
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
        # G-15: True when a claimed async settlement's transport call raised
        # or was cancelled -- the frame may or may not have reached the broker.
        self._settle_unknown = False
        # Transport-injected liveness probe for the channel this delivery
        # arrived on (``lambda: channel.is_open``). Delivery tags are only
        # meaningful on their own channel generation; ``ack_many`` /
        # ``nack_many`` consult this to report a STALE handle instead of
        # issuing a tag onto a rebuilt channel (where it would settle a
        # DIFFERENT message). ``None`` = unknown (TestBroker, hand-built).
        self._channel_alive: Callable[[], bool] | None = None

    @property
    def is_settled(self) -> bool:
        """True if the message has been acked, nacked, or rejected."""
        return self._disposition != "pending"

    @property
    def disposition(self) -> str:
        """Final settlement state: "pending", "acked", "nacked", or "rejected" (M2)."""
        return self._disposition

    @property
    def settlement_outcome_unknown(self) -> bool:
        """True if an async settlement was claimed but its send raised or was cancelled.

        The ack/nack/reject frame may or may not have reached the broker.
        ``disposition`` keeps the claimed value and no other settlement is
        sent for this delivery; if the frame was lost, the broker redelivers
        the message when its channel closes (at-least-once).
        """
        return self._settle_unknown

    @property
    def channel_alive(self) -> bool | None:
        """Whether the channel this delivery arrived on is still open.

        ``None`` when the transport did not wire a probe (in-memory
        ``TestBroker``, hand-constructed messages). ``False`` means the
        delivery tag is STALE — the broker has already (or will) redeliver
        this message on a new channel and settling it here would be a
        protocol error or, worse, settle a different message.
        """
        if self._channel_alive is None:
            return None
        try:
            return bool(self._channel_alive())
        except Exception:
            return False

    # ── Sync settlement ───────────────────────────────────────────────────

    def ack(self) -> None:
        """Synchronous ack. Raises SettlementError on async-only transport.

        Sets disposition only after the transport call succeeds, so a failed
        ack (channel closed, frame error) leaves the message unsettled and the
        exception propagates to the recovery loop instead of being swallowed.
        """
        if self._disposition != "pending":
            return  # idempotent guard
        if self._ack_fn is None:
            msg = "Cannot sync-ack an async transport message. Use await msg.ack_async()."
            raise SettlementError(msg)
        self._ack_fn()  # may raise — disposition stays "pending" on failure
        self._disposition = "acked"

    def nack(self, requeue: bool = True) -> None:
        """Synchronous nack. Raises SettlementError on async-only transport."""
        if self._disposition != "pending":
            return
        if self._nack_fn is None:
            msg = "Cannot sync-nack an async transport message. Use await msg.nack_async()."
            raise SettlementError(msg)
        self._nack_fn(requeue)
        self._disposition = "nacked"

    def reject(self, requeue: bool = False) -> None:
        """Synchronous reject. Raises SettlementError on async-only transport."""
        if self._disposition != "pending":
            return
        if self._reject_fn is None:
            msg = "Cannot sync-reject an async transport message. Use await msg.reject_async()."
            raise SettlementError(msg)
        self._reject_fn(requeue)
        self._disposition = "rejected"

    # ── Async settlement ──────────────────────────────────────────────────
    #
    # G-15: the async variants CLAIM the settlement before awaiting the
    # transport. The old guard checked ``_disposition`` and set it only after
    # the await, so two coroutines (a concurrent ack/nack, or ``stop()``
    # nacking a handler that was cancelled mid-ack) could both pass the check
    # and send two frames for one delivery tag -- the broker answers the
    # second with PRECONDITION_FAILED ("unknown delivery tag"), which closes
    # the channel and redelivers every unacked message on it. Check-and-claim
    # has no ``await`` in between, so it is atomic on the event loop.
    #
    # If the send then raises or is cancelled, the frame may or may not have
    # reached the broker. The claim is kept (a second, different frame could
    # contradict one that landed) and ``settlement_outcome_unknown`` turns
    # True; if the frame did not land, the broker redelivers the message when
    # the channel closes. The exception still propagates.

    async def _settle_async(
        self,
        disposition: str,
        async_fn: Callable[..., Awaitable[None]] | None,
        sync_fn: Callable[..., None] | None,
        args: tuple[bool, ...],
        verb: str,
    ) -> None:
        if self._disposition != "pending":
            return  # already claimed: never a second frame for this delivery
        if async_fn is None and sync_fn is None:
            raise SettlementError(f"Cannot async-{verb}: no settlement fn set. Use msg.{verb}() on a sync transport.")
        self._disposition = disposition  # claim BEFORE any await
        try:
            if async_fn is not None:
                await async_fn(*args)
            else:
                assert sync_fn is not None  # narrowed by the check above
                sync_fn(*args)
        except BaseException:
            self._settle_unknown = True
            raise

    async def ack_async(self) -> None:
        """Async ack. Falls back to sync if async fn not set.

        The settlement is claimed before the transport call (see
        ``settlement_outcome_unknown``): a concurrent or later
        ``ack_async``/``nack_async``/``reject_async`` is a no-op.
        """
        await self._settle_async("acked", self._ack_async_fn, self._ack_fn, (), "ack")

    async def nack_async(self, requeue: bool = True) -> None:
        """Async nack. Falls back to sync if async fn not set. Claims first (see ``ack_async``)."""
        await self._settle_async("nacked", self._nack_async_fn, self._nack_fn, (requeue,), "nack")

    async def reject_async(self, requeue: bool = False) -> None:
        """Async reject. Falls back to sync if async fn not set. Claims first (see ``ack_async``)."""
        await self._settle_async("rejected", self._reject_async_fn, self._reject_fn, (requeue,), "reject")


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
