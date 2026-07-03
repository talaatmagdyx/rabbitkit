"""DLQ Inspector — peek, replay, and purge dead-letter queues.

Provides inspection and recovery tools for messages stuck in DLQs.

**Operational realism:**
- ``peek()`` returns materialized snapshots, not live references
- ``peek()`` may affect message ordering (basic.get + requeue changes position)
- ``replay()`` preserves original headers
- ``purge()`` is immediate and unfiltered — use ``replay()`` for selective recovery
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope

logger = logging.getLogger(__name__)


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

    def replay(
        self,
        queue: str,
        predicate: Callable[[RabbitMessage], bool] | None = None,
        target_queue: str | None = None,
        target_exchange: str | None = None,
    ) -> int:
        """Replay messages from the DLQ.

        Fetches messages, applies optional predicate filter, publishes
        matching messages to the target, and acks the originals.

        Non-matching messages are nacked with ``requeue=True``.

        Args:
            queue: Source DLQ to replay from.
            predicate: Optional filter — only replay messages where
                predicate returns True. All messages replayed if None.
            target_queue: Target queue routing key. Defaults to the
                original queue from message headers.
            target_exchange: Target exchange. Defaults to "".

        Returns:
            Number of messages replayed.

        Loop Engineering Review, Reliability: a non-matching message is
        **not** nacked (requeued) until after this method's fetch loop has
        fully exhausted the queue. ``basic_get`` has no natural "already
        seen this delivery" tracking of its own -- if a non-matching
        message were requeued immediately, and nothing else is consuming
        from this queue, the very next ``basic_get`` call in this same loop
        could immediately re-fetch that exact message, forever, for any
        predicate that ever returns ``False``. Held-but-unsettled messages
        are invisible to further ``basic_get`` calls (the broker still
        considers them delivered-but-unacked), so deferring the nack until
        after the loop truly exits guarantees termination regardless of how
        many messages the predicate rejects.
        """
        replayed = 0
        non_matching: list[RabbitMessage] = []

        while True:
            msg = self._transport.basic_get(queue)
            if msg is None:
                break

            # Apply predicate filter -- hold, don't nack yet (see docstring).
            if predicate is not None and not predicate(msg):
                non_matching.append(msg)
                continue

            # Determine target
            rk = target_queue or msg.headers.get("x-rabbitkit-original-queue", msg.routing_key)
            exchange = target_exchange if target_exchange is not None else ""

            # Publish to target
            envelope = MessageEnvelope(
                routing_key=rk,
                body=msg.body,
                exchange=exchange,
                headers=dict(msg.headers),
                message_id=msg.message_id or "",
                correlation_id=msg.correlation_id,
                content_type=msg.content_type or "application/octet-stream",
                content_encoding=msg.content_encoding,
            )
            self._transport.publish(envelope)

            # Ack the original
            if not msg.is_settled:
                msg.ack()

            replayed += 1

        # The fetch loop is done (basic_get returned None) -- now it's safe
        # to requeue non-matching messages; this loop can no longer re-fetch them.
        for msg in non_matching:
            if not msg.is_settled:
                msg.nack(requeue=True)

        return replayed

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
    ) -> int:
        """Async variant of ``replay`` -- see its docstring for why
        non-matching messages are held, not nacked, until the fetch loop
        has fully exhausted the queue (termination guarantee)."""
        replayed = 0
        non_matching: list[RabbitMessage] = []

        while True:
            msg = await self._transport.basic_get(queue)
            if msg is None:
                break

            if predicate is not None and not predicate(msg):
                non_matching.append(msg)
                continue

            rk = target_queue or msg.headers.get("x-rabbitkit-original-queue", msg.routing_key)
            exchange = target_exchange if target_exchange is not None else ""

            envelope = MessageEnvelope(
                routing_key=rk,
                body=msg.body,
                exchange=exchange,
                headers=dict(msg.headers),
                message_id=msg.message_id or "",
                correlation_id=msg.correlation_id,
                content_type=msg.content_type or "application/octet-stream",
                content_encoding=msg.content_encoding,
            )
            await self._transport.publish(envelope)

            if not msg.is_settled:
                await msg.ack_async()

            replayed += 1

        for msg in non_matching:
            if not msg.is_settled:
                await msg.nack_async(requeue=True)

        return replayed

    async def purge_async(self, queue: str) -> int:
        """Async variant of ``purge``."""
        return int(await self._transport.purge_queue(queue))
