"""DLQ Inspector — peek, replay, and purge dead-letter queues.

Provides inspection and recovery tools for messages stuck in DLQs.

**Operational realism:**
- ``peek()`` returns materialized snapshots, not live references
- ``peek()`` may affect message ordering (basic.get + requeue changes position)
- ``replay()`` preserves original headers (pass ``reset_retry_count=True`` to
  grant the replayed message a fresh retry ladder)
- ``replay()`` acks a DLQ original only after the republish outcome is OK;
  failed republishes are nack-requeued so they stay on the DLQ
- ``purge()`` is immediate and unfiltered — use ``replay()`` for selective recovery
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope

logger = logging.getLogger(__name__)

# Matches RetryConfig.retry_header's default. If you customized retry_header,
# strip your header via a predicate/pre-processing step instead.
_RETRY_COUNT_HEADER = "x-rabbitkit-retry-count"


class ReplayResult(int):
    """Replay report that IS the replayed count (int-compatible, so existing
    ``count = inspector.replay(...)`` callers keep working), with extras:

    - ``failed``: messages whose republish outcome was not OK — they were
      nack-requeued and REMAIN ON THE DLQ.
    - ``requeued``: non-matching messages returned to the DLQ (predicate
      returned False).
    """

    failed: int
    requeued: int

    def __new__(cls, replayed: int, failed: int = 0, requeued: int = 0) -> ReplayResult:
        obj = super().__new__(cls, replayed)
        obj.failed = failed
        obj.requeued = requeued
        return obj

    def __repr__(self) -> str:
        return f"ReplayResult(replayed={int(self)}, failed={self.failed}, requeued={self.requeued})"


class DLQInspector:
    """Dead-letter queue inspection and replay.

    Accepts a transport that supports ``basic_get``, ``publish``,
    ``purge_queue`` methods. Works with both sync and async transports.

    Usage::

        inspector = DLQInspector(transport)

        # Peek at messages without consuming them
        messages = inspector.peek("orders-queue.dlq", limit=5)

        # Replay matching messages back to source queue
        count = inspector.replay(
            "orders-queue.dlq",
            predicate=lambda msg: msg.headers.get("x-error") == "timeout",
            target_queue="orders-queue",
        )

        # Purge entire DLQ
        count = inspector.purge("orders-queue.dlq")
    """

    def __init__(self, transport: Any) -> None:
        self._transport = transport

    # ── Sync methods ─────────────────────────────────────────────────────

    def peek(self, queue: str, limit: int = 10) -> list[RabbitMessage]:
        """Fetch up to ``limit`` messages from the queue, then requeue them.

        Messages are nacked with ``requeue=True`` so they return to the queue.
        Ordering may change after this operation.

        Returns a list of message snapshots.
        """
        messages: list[RabbitMessage] = []

        for _ in range(limit):
            msg = self._transport.basic_get(queue)
            if msg is None:
                break
            messages.append(msg)

        # Requeue all peeked messages
        for msg in messages:
            if not msg.is_settled:
                msg.nack(requeue=True)

        return messages

    @staticmethod
    def _build_replay_envelope(
        msg: RabbitMessage,
        target_queue: str | None,
        target_exchange: str | None,
        reset_retry_count: bool,
    ) -> MessageEnvelope:
        """Build the republish envelope for one DLQ message.

        ``mandatory=True`` so an unroutable target comes back as a
        ``RETURNED`` outcome instead of being broker-confirmed into the void.
        """
        rk = target_queue or msg.headers.get("x-rabbitkit-original-queue", msg.routing_key)
        headers = dict(msg.headers)
        if reset_retry_count:
            headers.pop(_RETRY_COUNT_HEADER, None)
        return MessageEnvelope(
            routing_key=rk,
            body=msg.body,
            exchange=target_exchange if target_exchange is not None else "",
            headers=headers,
            message_id=msg.message_id or "",
            correlation_id=msg.correlation_id,
            content_type=msg.content_type or "application/octet-stream",
            content_encoding=msg.content_encoding,
            # Preserve the remaining original message properties -- these used
            # to be silently dropped on replay, e.g. a priority-queue message
            # lost its priority, and an RPC request's reply_to/type/app_id/
            # user_id never survived the replay for the reply to route back.
            reply_to=msg.reply_to,
            priority=msg.priority,
            expiration=msg.expiration,
            type=msg.type,
            app_id=msg.app_id,
            user_id=msg.user_id,
            mandatory=True,
        )

    def replay(
        self,
        queue: str,
        predicate: Callable[[RabbitMessage], bool] | None = None,
        target_queue: str | None = None,
        target_exchange: str | None = None,
        *,
        reset_retry_count: bool = False,
        limit: int | None = None,
    ) -> ReplayResult:
        """Replay messages from the DLQ.

        Fetches messages, applies optional predicate filter, publishes
        matching messages to the target, and acks each original **only after
        its republish outcome is OK**. A failed republish (NACKED / TIMEOUT /
        RETURNED / ERROR) is nack-requeued, so the message stays on the DLQ
        instead of being lost.

        Non-matching messages are nacked with ``requeue=True``.

        Args:
            queue: Source DLQ to replay from.
            predicate: Optional filter — only replay messages where
                predicate returns True. All messages replayed if None.
            target_queue: Target queue routing key. Defaults to the
                original queue from message headers.
            target_exchange: Target exchange. Defaults to "".
            reset_retry_count: Strip the ``x-rabbitkit-retry-count`` header
                so the replayed message gets a fresh retry ladder. Default
                False preserves headers verbatim — meaning a previously
                max-retried message is terminal after ONE failed attempt and
                returns to the DLQ.
            limit: Maximum number of messages to fetch this call (None =
                drain until empty). Set this when a LIVE consumer on the
                target can fail a replayed message back into this same DLQ
                faster than the drain completes — the held-until-drained
                termination guarantee below covers self-refetch, but not a
                message that genuinely re-arrives via dead-lettering
                mid-drain, which an unbounded loop would replay again.

        Returns:
            :class:`ReplayResult` — int-compatible replayed count, with
            ``.failed`` (left on the DLQ) and ``.requeued`` (non-matching).

        Loop Engineering Review, Reliability: a non-matching or
        failed-publish message is **not** nacked (requeued) until after this
        method's fetch loop has fully exhausted the queue. ``basic_get`` has
        no natural "already seen this delivery" tracking of its own -- if
        such a message were requeued immediately, and nothing else is
        consuming from this queue, the very next ``basic_get`` call in this
        same loop could immediately re-fetch that exact message, forever.
        Held-but-unsettled messages are invisible to further ``basic_get``
        calls (the broker still considers them delivered-but-unacked), so
        deferring the nack until after the loop truly exits guarantees
        termination regardless of how many messages the predicate rejects or
        the publisher fails.
        """
        replayed = 0
        held_for_requeue: list[RabbitMessage] = []
        failed = 0
        requeued = 0
        fetched = 0

        while limit is None or fetched < limit:
            msg = self._transport.basic_get(queue)
            if msg is None:
                break
            fetched += 1

            # Apply predicate filter -- hold, don't nack yet (see docstring).
            if predicate is not None and not predicate(msg):
                held_for_requeue.append(msg)
                requeued += 1
                continue

            envelope = self._build_replay_envelope(msg, target_queue, target_exchange, reset_retry_count)
            outcome = self._transport.publish(envelope)
            # A None outcome (duck-typed transport returning nothing) is an
            # UNVERIFIED publish — treat as failure, never ack against it.
            if outcome is None or not outcome.ok:
                # Republish failed — DO NOT ack, or the message is lost
                # forever. Hold for nack-requeue so it stays on the DLQ.
                logger.error(
                    "DLQ replay publish failed (status=%s); message stays on %r: routing_key=%s message_id=%s",
                    getattr(outcome, "status", "unknown"),
                    queue,
                    envelope.routing_key,
                    envelope.message_id,
                )
                held_for_requeue.append(msg)
                failed += 1
                continue

            # Ack the original — it is safely republished now
            if not msg.is_settled:
                msg.ack()

            replayed += 1

        # The fetch loop is done (basic_get returned None) -- now it's safe
        # to requeue held messages; this loop can no longer re-fetch them.
        for msg in held_for_requeue:
            if not msg.is_settled:
                msg.nack(requeue=True)

        return ReplayResult(replayed, failed=failed, requeued=requeued)

    def purge(self, queue: str) -> int:
        """Purge all messages from the queue.

        Returns the number of messages purged.
        """
        return int(self._transport.purge_queue(queue))

    # ── Async methods ────────────────────────────────────────────────────

    async def peek_async(self, queue: str, limit: int = 10) -> list[RabbitMessage]:
        """Async variant of ``peek``."""
        messages: list[RabbitMessage] = []

        for _ in range(limit):
            msg = await self._transport.basic_get(queue)
            if msg is None:
                break
            messages.append(msg)

        # Requeue all peeked messages
        for msg in messages:
            if not msg.is_settled:
                await msg.nack_async(requeue=True)

        return messages

    async def replay_async(
        self,
        queue: str,
        predicate: Callable[[RabbitMessage], bool] | None = None,
        target_queue: str | None = None,
        target_exchange: str | None = None,
        *,
        reset_retry_count: bool = False,
        limit: int | None = None,
    ) -> ReplayResult:
        """Async variant of ``replay`` -- see its docstring for the
        outcome-checked ack, ``reset_retry_count``, and why non-matching /
        failed messages are held, not nacked, until the fetch loop has fully
        exhausted the queue (termination guarantee)."""
        replayed = 0
        held_for_requeue: list[RabbitMessage] = []
        failed = 0
        requeued = 0
        fetched = 0

        while limit is None or fetched < limit:
            msg = await self._transport.basic_get(queue)
            if msg is None:
                break
            fetched += 1

            if predicate is not None and not predicate(msg):
                held_for_requeue.append(msg)
                requeued += 1
                continue

            envelope = self._build_replay_envelope(msg, target_queue, target_exchange, reset_retry_count)
            outcome = await self._transport.publish(envelope)
            # None outcome = unverified publish = failure (see sync variant).
            if outcome is None or not outcome.ok:
                logger.error(
                    "DLQ replay publish failed (status=%s); message stays on %r: routing_key=%s message_id=%s",
                    getattr(outcome, "status", "unknown"),
                    queue,
                    envelope.routing_key,
                    envelope.message_id,
                )
                held_for_requeue.append(msg)
                failed += 1
                continue

            if not msg.is_settled:
                await msg.ack_async()

            replayed += 1

        for msg in held_for_requeue:
            if not msg.is_settled:
                await msg.nack_async(requeue=True)

        return ReplayResult(replayed, failed=failed, requeued=requeued)

    async def purge_async(self, queue: str) -> int:
        """Async variant of ``purge``."""
        return int(await self._transport.purge_queue(queue))
