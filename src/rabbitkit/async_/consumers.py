"""Consumer supervision for the async transport (G-26, CONSUMER-007).

RabbitMQ cancels a consumer on its own (``basic.cancel`` from the server)
when the queue is deleted, when a quorum or mirrored queue loses its leader,
or when an operator closes it. aiormq removes the consumer tag from the
channel and, before aiormq 7, says nothing else; rabbitkit kept reporting the
route as consuming and never subscribed again, so the process sat idle while
health stayed green.

:class:`ConsumerSupervisor` keeps one :class:`ConsumerRecord` per consumed
queue and:

* detects a server-side cancel: through aiormq's
  ``on_consumer_cancel_callbacks`` when the installed aiormq has them
  (aiormq >= 7, immediate), otherwise by polling the channel's consumer table
  (two consecutive misses on an open channel, ``WATCH_INTERVAL`` apart);
* reports it: WARNING log, the ``on_cancelled`` hooks, ``cancelled_queues``;
* re-subscribes with the same consumer tag once a passive declare finds the
  queue again, backing off from ``RESUBSCRIBE_BACKOFF_INITIAL`` to
  ``RESUBSCRIBE_BACKOFF_MAX`` seconds between probes, then fires
  ``on_resumed``.

aio-pika 10.x re-subscribes broker-cancelled consumers on its own as well.
Both paths use the same tag, so whichever runs first wins: the other sees the
tag in the channel's consumer table, or gets ``DuplicateConsumerTag`` before
any frame is sent, and treats the consumer as resumed. The supervisor
subscribes with ``robust=False`` so it never replaces aio-pika's own restore
record for the tag (aio-pika cancels a consumer whose record changed under
it). While the channel is closed (a reconnect) the supervisor does nothing:
aio-pika's reconnect restores the consumers it tracks.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

#: Seconds between supervision passes (and between two polls in poll mode).
WATCH_INTERVAL = 1.0
#: First delay before probing for a cancelled consumer's queue; doubles per miss.
RESUBSCRIBE_BACKOFF_INITIAL = 1.0
#: Cap on the delay between probes for a cancelled consumer's queue.
RESUBSCRIBE_BACKOFF_MAX = 30.0
#: Bound on waiting for a robust channel's underlay (it waits for the connection).
_UNDERLAY_TIMEOUT = 1.0
#: Poll mode: consecutive "tag missing on an open channel" observations needed.
_MISSES_TO_CANCEL = 2


@dataclass(slots=True, eq=False)
class ConsumerRecord:
    """One consumer the transport started, and what is needed to restart it."""

    queue: str
    tag: str
    channel: Any
    queue_obj: Any
    callback: Callable[[Any], Awaitable[None]]
    no_ack: bool = False
    #: Only consumers on a declared queue are re-subscribed (not reply-to).
    supervised: bool = True
    #: Set when rabbitkit itself cancels the consumer; never re-subscribe after.
    stopped: bool = False
    #: The server cancelled the consumer and it has not been re-subscribed yet.
    broker_cancelled: bool = False
    #: The aiormq channel the cancel callback is attached to (None: poll mode).
    watched_underlay: Any = None
    #: The tag was seen on ``watched_underlay`` after the callback was attached,
    #: so a later server cancel is guaranteed to reach the callback.
    watch_confirmed: bool = False
    missed_polls: int = 0
    retry_delay: float = RESUBSCRIBE_BACKOFF_INITIAL
    next_attempt: float = 0.0


class ConsumerSupervisor:
    """Detects server-cancelled consumers and re-subscribes them (see module doc)."""

    def __init__(self, queue_exists: Callable[[str], Awaitable[bool]]) -> None:
        self._queue_exists = queue_exists
        self._records: dict[str, ConsumerRecord] = {}
        self._cancelled_callbacks: list[Callable[[str], None]] = []
        self._resumed_callbacks: list[Callable[[str], None]] = []
        self._task: asyncio.Task[None] | None = None
        self._wakeup: asyncio.Event | None = None

    # ── registration ──────────────────────────────────────────────────────

    def on_cancelled(self, callback: Callable[[str], None]) -> None:
        """Call ``callback(queue)`` when the server cancels a consumer."""
        self._cancelled_callbacks.append(callback)

    def on_resumed(self, callback: Callable[[str], None]) -> None:
        """Call ``callback(queue)`` when a server-cancelled consumer is consuming again."""
        self._resumed_callbacks.append(callback)

    async def register(self, record: ConsumerRecord) -> None:
        """Supervise *record*; call right after its ``basic.consume`` succeeded.

        The cancel callback is attached at once, so a server cancel that
        arrives before the first supervision pass is not missed.
        """
        self._records[record.queue] = record
        if not record.supervised:
            return
        underlay = await self._underlay(record.channel)
        if underlay is not None:
            self._watch(record, underlay)
            if record.tag in underlay.consumers:
                record.watch_confirmed = record.watched_underlay is underlay
        self._ensure_running()

    def unregister(self, queue: str) -> ConsumerRecord | None:
        """Forget *queue*'s consumer; it is marked stopped so nothing restarts it."""
        record = self._records.pop(queue, None)
        if record is not None:
            record.stopped = True
        return record

    @property
    def cancelled_queues(self) -> frozenset[str]:
        """Queues whose consumer the server cancelled and that are not consuming again yet."""
        return frozenset(q for q, r in self._records.items() if r.broker_cancelled)

    # ── lifecycle ─────────────────────────────────────────────────────────

    def _ensure_running(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._wakeup = asyncio.Event()
        self._task = asyncio.get_running_loop().create_task(self._run(), name="rabbitkit-consumer-supervisor")

    async def stop(self) -> None:
        """Stop supervising; forget every record (they are marked stopped)."""
        for queue in list(self._records):
            self.unregister(queue)
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _run(self) -> None:
        wakeup = self._wakeup
        assert wakeup is not None  # set by _ensure_running before the task starts
        while True:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(WATCH_INTERVAL):
                    await wakeup.wait()
            wakeup.clear()
            await self.check_once()

    async def check_once(self) -> None:
        """One supervision pass over every record."""
        for record in list(self._records.values()):
            try:
                await self._check(record)
            except Exception:
                # Task boundary: one record's failure must not stop supervision
                # of the others; the next pass retries it. CancelledError is a
                # BaseException and propagates.
                logger.warning("Consumer supervision of queue=%s failed; retrying", record.queue, exc_info=True)

    # ── detection ─────────────────────────────────────────────────────────

    def _on_cancel_frame(self, frame: Any) -> None:
        """aiormq >= 7 ``on_consumer_cancel_callbacks`` entry (server Basic.Cancel)."""
        tag = getattr(frame, "consumer_tag", None)
        for record in list(self._records.values()):
            if record.tag == tag and record.supervised and not record.stopped:
                if not record.broker_cancelled:
                    self._mark_cancelled(record)
                if self._wakeup is not None:
                    self._wakeup.set()
                return

    def _watch(self, record: ConsumerRecord, underlay: Any) -> None:
        if record.watched_underlay is underlay:
            return
        callbacks = getattr(underlay, "on_consumer_cancel_callbacks", None)
        if callbacks is None:
            return  # aiormq < 7: poll mode
        callbacks.add(self._on_cancel_frame)
        record.watched_underlay = underlay
        record.watch_confirmed = False

    async def _underlay(self, channel: Any) -> Any | None:
        try:
            async with asyncio.timeout(_UNDERLAY_TIMEOUT):
                return await channel.get_underlay_channel()
        except Exception:
            return None  # not open (or reconnecting): nothing to check now

    async def _check(self, record: ConsumerRecord) -> None:
        if record.stopped or not record.supervised:
            return
        if bool(getattr(record.channel, "is_closed", False)):
            record.missed_polls = 0
            return  # reconnecting: aio-pika restores what it tracks
        underlay = await self._underlay(record.channel)
        if underlay is None or bool(getattr(underlay, "is_closed", False)):
            return
        self._watch(record, underlay)
        present = record.tag in underlay.consumers
        if present and record.watched_underlay is underlay:
            record.watch_confirmed = True
        if not record.broker_cancelled:
            if present or record.watch_confirmed:
                # Consuming, or callback mode with the consumer seen on this
                # channel since the callback was attached: the callback is
                # authoritative, and a restore in progress is not a cancel.
                record.missed_polls = 0
                return
            record.missed_polls += 1
            if record.missed_polls < _MISSES_TO_CANCEL:
                return
            self._mark_cancelled(record)
        if present:
            self._mark_resumed(record)  # aio-pika re-subscribed it
            return
        await self._try_resubscribe(record, underlay)

    # ── recovery ──────────────────────────────────────────────────────────

    async def _try_resubscribe(self, record: ConsumerRecord, underlay: Any) -> None:
        loop = asyncio.get_running_loop()
        now = loop.time()
        if now < record.next_attempt:
            return
        record.next_attempt = now + record.retry_delay
        record.retry_delay = min(record.retry_delay * 2, RESUBSCRIBE_BACKOFF_MAX)
        if not await self._queue_exists(record.queue):
            logger.debug("queue=%s still missing; next probe in %.1fs", record.queue, record.next_attempt - now)
            return
        if record.stopped:
            return
        if record.tag not in underlay.consumers:
            from aiormq.exceptions import DuplicateConsumerTag

            try:
                await record.queue_obj.consume(
                    record.callback, consumer_tag=record.tag, no_ack=record.no_ack, robust=False
                )
            except DuplicateConsumerTag:
                pass  # aio-pika re-subscribed it first
        if record.stopped:
            # rabbitkit cancelled it while we were subscribing: undo.
            with contextlib.suppress(Exception):
                await underlay.basic_cancel(record.tag)
            return
        self._mark_resumed(record)

    def _mark_cancelled(self, record: ConsumerRecord) -> None:
        record.broker_cancelled = True
        record.missed_polls = 0
        record.retry_delay = RESUBSCRIBE_BACKOFF_INITIAL
        record.next_attempt = asyncio.get_running_loop().time() + RESUBSCRIBE_BACKOFF_INITIAL
        logger.warning(
            "Consumer for queue=%s (tag=%s) was cancelled by the broker (queue deleted, leader change "
            "or operator action); it is reported as not consuming and will re-subscribe when the "
            "queue exists again",
            record.queue,
            record.tag,
        )
        self._fire(self._cancelled_callbacks, record.queue)

    def _mark_resumed(self, record: ConsumerRecord) -> None:
        record.broker_cancelled = False
        record.missed_polls = 0
        record.retry_delay = RESUBSCRIBE_BACKOFF_INITIAL
        logger.info("Consumer for queue=%s (tag=%s) is consuming again", record.queue, record.tag)
        self._fire(self._resumed_callbacks, record.queue)

    @staticmethod
    def _fire(callbacks: list[Callable[[str], None]], queue: str) -> None:
        for callback in callbacks:
            try:
                callback(queue)
            except Exception:
                logger.warning("consumer state callback raised (queue=%s)", queue, exc_info=True)


__all__ = [
    "RESUBSCRIBE_BACKOFF_INITIAL",
    "RESUBSCRIBE_BACKOFF_MAX",
    "WATCH_INTERVAL",
    "ConsumerRecord",
    "ConsumerSupervisor",
]
