"""Consumer supervision for the sync transport (parity with the async one).

RabbitMQ cancels a consumer on its own (``basic.cancel`` from the server)
when the queue is deleted, when a quorum or mirrored queue loses its leader,
or when an operator closes it. pika's ``BlockingChannel`` drops the consumer
and calls the channel's ``add_on_cancel_callback`` callbacks; before this
module nothing was registered there, so ``SyncBroker`` kept reporting the
route as consuming and never subscribed again.

:class:`SyncConsumerSupervisor` keeps one :class:`SyncConsumerRecord` per
consumed queue and:

* is told about a server-side cancel by the transport's pika cancel callback
  (dispatched by pika on the connection's owner thread, inside
  ``process_data_events``);
* reports it: WARNING log, the ``on_cancelled`` hooks, ``cancelled_queues``;
* on every pass of the consume loop (:meth:`check_once`, owner thread only),
  re-subscribes with the same consumer tag once a passive declare finds the
  queue again, backing off from ``RESUBSCRIBE_BACKOFF_INITIAL`` to
  ``RESUBSCRIBE_BACKOFF_MAX`` seconds between probes, then fires
  ``on_resumed``. It never declares (re-creates) the queue.

Nothing here touches pika: the transport passes the probe and the
re-subscribe as callables, and both run on the thread that calls
:meth:`check_once`, which must be the connection's owner thread.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

#: First delay before probing for a cancelled consumer's queue; doubles per miss.
RESUBSCRIBE_BACKOFF_INITIAL = 1.0
#: Cap on the delay between probes for a cancelled consumer's queue.
RESUBSCRIBE_BACKOFF_MAX = 30.0


@dataclass(slots=True, eq=False)
class SyncConsumerRecord:
    """One consumer the sync transport started, and what is needed to restart it."""

    queue: str
    tag: str
    #: The pika ``on_message_callback`` the consumer was started with.
    on_message: Callable[[Any, Any, Any, bytes], None]
    #: ``basic_qos`` prefetch, for a channel opened to re-subscribe.
    prefetch: int = 10
    no_ack: bool = False
    #: Only consumers on a declared queue are re-subscribed (not reply-to).
    supervised: bool = True
    #: The consume arguments, by reference: a stream route's mapping is kept
    #: current by its offset tracker, so a re-subscribe resumes at the
    #: processed offset instead of the server default ("next").
    arguments: Mapping[str, Any] | None = None
    #: The server cancelled the consumer and it has not been re-subscribed yet.
    broker_cancelled: bool = False
    retry_delay: float = RESUBSCRIBE_BACKOFF_INITIAL
    next_attempt: float = 0.0


class SyncConsumerSupervisor:
    """Tracks server-cancelled consumers and re-subscribes them (see module doc).

    Args:
        queue_exists: Passive-declare probe: True when the queue exists again.
        resubscribe: Re-issue ``basic.consume`` for a record; True on success.
        clock: Monotonic clock (injectable for tests).
    """

    def __init__(
        self,
        queue_exists: Callable[[str], bool],
        resubscribe: Callable[[SyncConsumerRecord], bool],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._queue_exists = queue_exists
        self._resubscribe = resubscribe
        self._clock = clock
        self._records: dict[str, SyncConsumerRecord] = {}
        self._cancelled_callbacks: list[Callable[[str], None]] = []
        self._resumed_callbacks: list[Callable[[str], None]] = []

    # ── registration ──────────────────────────────────────────────────────

    def on_cancelled(self, callback: Callable[[str], None]) -> None:
        """Call ``callback(queue)`` when the server cancels a consumer."""
        self._cancelled_callbacks.append(callback)

    def on_resumed(self, callback: Callable[[str], None]) -> None:
        """Call ``callback(queue)`` when a server-cancelled consumer is consuming again."""
        self._resumed_callbacks.append(callback)

    def register(self, record: SyncConsumerRecord) -> None:
        """Track *record*; call right after its ``basic.consume`` succeeded."""
        self._records[record.queue] = record

    def unregister(self, queue: str) -> SyncConsumerRecord | None:
        """Forget *queue*'s consumer (rabbitkit cancelled it); nothing restarts it."""
        return self._records.pop(queue, None)

    def clear(self) -> None:
        """Forget every record (the connection is gone; the broker re-subscribes all routes)."""
        self._records.clear()

    def record(self, queue: str) -> SyncConsumerRecord | None:
        """The record for *queue*, if it is tracked."""
        return self._records.get(queue)

    @property
    def cancelled_queues(self) -> frozenset[str]:
        """Queues whose consumer the server cancelled and that are not consuming again yet.

        Safe to read from any thread (a snapshot of the records is taken first).
        """
        return frozenset(r.queue for r in list(self._records.values()) if r.broker_cancelled)

    # ── detection ─────────────────────────────────────────────────────────

    def broker_cancelled(self, tag: str) -> None:
        """The server sent ``basic.cancel`` for consumer *tag* (pika's cancel callback)."""
        for record in self._records.values():
            if record.tag == tag:
                if record.supervised and not record.broker_cancelled:
                    self._mark_cancelled(record)
                return

    # ── recovery (owner thread) ───────────────────────────────────────────

    def check_once(self) -> None:
        """Probe and re-subscribe every cancelled consumer whose backoff has elapsed.

        Runs on the connection's owner thread (the consume loop). A connection
        error from the probe propagates so the broker's recovery loop
        reconnects; the transport's callables handle channel-level errors.
        """
        for record in list(self._records.values()):
            if record.broker_cancelled:
                self._try_resubscribe(record)

    def _try_resubscribe(self, record: SyncConsumerRecord) -> None:
        now = self._clock()
        if now < record.next_attempt:
            return
        record.next_attempt = now + record.retry_delay
        record.retry_delay = min(record.retry_delay * 2, RESUBSCRIBE_BACKOFF_MAX)
        if not self._queue_exists(record.queue):
            logger.debug("queue=%s still missing; next probe in %.1fs", record.queue, record.next_attempt - now)
            return
        if self._records.get(record.queue) is not record:
            return  # cancelled by rabbitkit while probing
        if self._resubscribe(record):
            self._mark_resumed(record)

    def _mark_cancelled(self, record: SyncConsumerRecord) -> None:
        record.broker_cancelled = True
        record.retry_delay = RESUBSCRIBE_BACKOFF_INITIAL
        record.next_attempt = self._clock() + RESUBSCRIBE_BACKOFF_INITIAL
        logger.warning(
            "Consumer for queue=%s (tag=%s) was cancelled by the broker (queue deleted, leader change "
            "or operator action); it is reported as not consuming and will re-subscribe when the "
            "queue exists again",
            record.queue,
            record.tag,
        )
        self._fire(self._cancelled_callbacks, record.queue)

    def _mark_resumed(self, record: SyncConsumerRecord) -> None:
        record.broker_cancelled = False
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
    "SyncConsumerRecord",
    "SyncConsumerSupervisor",
]
