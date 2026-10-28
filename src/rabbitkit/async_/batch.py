"""BatchPublisher — transparent batch publish with amortized confirm wait.

Collects messages from concurrent callers into fixed-size batches, then
publishes the whole batch on a single pooled channel.  The broker-side confirm
wait is paid once per batch rather than once per message, dramatically reducing
the per-message cost at high concurrency.

Usage::

    from rabbitkit.core.config import BatchPublishConfig

    broker = AsyncBroker(
        config,
        batch_config=BatchPublishConfig(batch_size=64, flush_interval_ms=20),
    )
    await broker.start()
    # broker.publish() is now transparently batched
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from rabbitkit.core.config import BatchPublishConfig
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishReason, PublishStatus

if TYPE_CHECKING:
    from rabbitkit.async_.pool import InFlightByteLimiter
    from rabbitkit.async_.transport import AsyncTransportImpl

logger = logging.getLogger(__name__)

# Capped backoff for channel acquisition (see _acquire_channel). A flush
# worker retries acquiring a publisher channel across a short outage, for at
# most the admission timeout, then fails the batch NOT_SENT and keeps running.
_ACQUIRE_BACKOFF_INITIAL = 0.05
_ACQUIRE_BACKOFF_MAX = 5.0

# Item lifecycle. QUEUED items have not been handed to the transport, so they
# can still be withdrawn (NOT_SENT); FLUSHING items may already be on the wire.
_QUEUED = 0
_FLUSHING = 1
_DONE = 2


class _BatchItem:
    """One caller's publish while it moves through the batch queue."""

    __slots__ = ("caller", "deadline", "envelope", "future", "limiter", "nbytes", "state")

    def __init__(
        self,
        envelope: MessageEnvelope,
        future: asyncio.Future[PublishOutcome],
        deadline: float,
        nbytes: int,
        limiter: InFlightByteLimiter | None,
    ) -> None:
        self.envelope = envelope
        self.future = future
        self.deadline = deadline
        self.nbytes = nbytes
        self.limiter = limiter
        self.state = _QUEUED
        # What publish() awaits. Separate from ``future`` (the item's final
        # outcome, which stop() waits on): a deadline may answer the caller
        # (TIMEOUT) and a cancelled caller cancels only this one, while the
        # item stays with its worker until it really resolves.
        self.caller: asyncio.Future[PublishOutcome] | None = None

    def outcome(self, status: PublishStatus, reason: str | None, error: BaseException | None = None) -> PublishOutcome:
        return PublishOutcome(
            status=status,
            exchange=self.envelope.exchange,
            routing_key=self.envelope.routing_key,
            error=error,
            reason=reason,
        )

    def finish(self, outcome: PublishOutcome) -> None:
        """Resolve the item once: release its bytes and settle the caller's future."""
        if self.state == _DONE:
            return
        self.state = _DONE
        if self.nbytes and self.limiter is not None:
            self.limiter.release(self.nbytes)
        if not self.future.done():
            self.future.set_result(outcome)
        caller = self.caller
        if caller is not None and not caller.done():
            caller.set_result(outcome)


def _expire(item: _BatchItem) -> None:
    """A caller's deadline passed: withdraw a still-queued item (NOT_SENT), or
    report a flushing one as TIMEOUT. A flushing item stays with its worker,
    which finishes it (and releases its bytes) when the confirm resolves."""
    if item.state == _QUEUED:
        item.finish(item.outcome(PublishStatus.NOT_SENT, PublishReason.ADMISSION_TIMEOUT))
    elif item.state == _FLUSHING and item.caller is not None and not item.caller.done():
        item.caller.set_result(item.outcome(PublishStatus.TIMEOUT, PublishReason.PUBLISH_TIMEOUT))


class AsyncBatchPublisher:
    """Transparent batch-publish wrapper for AsyncTransportImpl.

    N concurrent ``broker.publish()`` calls are coalesced into one batch,
    published on a single channel, and their confirms are gathered together.
    Reduces per-message pool acquire/release and amortizes confirm round-trips.

    The caller's coroutine blocks until its message is included in a flushed
    batch and the confirm resolves — semantics are identical to direct publish,
    including the outcome contract: an unroutable mandatory publish is
    RETURNED, a lost channel is UNKNOWN, and a message that never left the
    queue is NOT_SENT.

    Every wait is bounded: queue admission and byte budget by the admission
    timeout (``PoolConfig.channel_acquire_timeout``), the whole call by the
    per-call ``timeout`` or, by default, admission + flush interval +
    confirm timeout. A call that runs out of time while its message is still
    queued withdraws it (NOT_SENT ``admission_timeout``); once the message is
    being flushed it reports TIMEOUT (``publish_timeout``).

    Unlike ``highload.batch.BatchPublisher`` (a timing/buffering helper that
    publishes each message individually), this class pipelines confirms: all
    messages in a batch share one channel and their ACKs are awaited together.

    Blast-radius note (M17): because a batch shares one channel, an
    AMQP-channel-level failure (e.g. a refused publish that closes the channel)
    fails EVERY in-flight publish in that batch — each caller's future is
    settled (never silently lost), and the channel is replaced before the
    next batch. To bound the blast radius, lower
    ``BatchPublishConfig.batch_size`` — or use direct ``broker.publish()`` for
    publishes that must fail independently.
    """

    def __init__(
        self,
        transport: AsyncTransportImpl,
        config: BatchPublishConfig,
        *,
        admission_timeout: float = 10.0,
        confirm_timeout: float = 5.0,
        byte_limiter: InFlightByteLimiter | None = None,
    ) -> None:
        self._transport = transport
        self._config = config
        self._admission_timeout = admission_timeout
        self._confirm_timeout = confirm_timeout
        self._byte_limiter = byte_limiter
        self._pending: asyncio.Queue[_BatchItem] = asyncio.Queue(maxsize=config.max_in_flight)
        self._flush_tasks: list[asyncio.Task[None]] = []
        self._closed = False
        # Items a worker has dequeued and not yet resolved (stop() resolves them).
        self._active: set[_BatchItem] = set()

    def _worker_count(self) -> int:
        if self._config.flush_workers > 0:
            return self._config.flush_workers
        return min(16, max(1, self._config.max_in_flight // self._config.batch_size))

    def _default_wait(self) -> float:
        return self._admission_timeout + self._config.flush_interval_ms / 1000.0 + self._confirm_timeout

    async def _acquire_channel(self) -> Any:
        """Acquire a publisher channel, surviving a short connection failure.

        Retries with capped backoff for at most the admission timeout, then
        returns ``None`` so the worker fails the batch NOT_SENT and keeps
        running (it is not supervised, so it must never die, and it must
        never retry forever while callers wait). ``CancelledError`` (from
        ``stop()``) propagates.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._admission_timeout
        backoff = _ACQUIRE_BACKOFF_INITIAL
        while True:
            try:
                return await self._transport._conn_pool.acquire_publisher_channel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # connection down / channel-open timeout — retry
                if loop.time() + backoff >= deadline:
                    logger.error("Batch flush channel acquire failed (%s); giving up for this batch", exc)
                    return None
                logger.warning("Batch flush channel acquire failed (%s); retrying in %.2fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _ACQUIRE_BACKOFF_MAX)

    async def start(self) -> None:
        """Start N concurrent flush loops (one per channel slot)."""
        n = self._worker_count()
        self._flush_tasks = [
            asyncio.create_task(self._flush_loop(), name=f"rabbitkit.batch-flush-{i}") for i in range(n)
        ]

    async def stop(self, timeout: float | None = 0.0) -> None:
        """Stop accepting, flush what is queued (bounded by *timeout*), then
        resolve every remaining item: still queued → NOT_SENT
        (``publisher_closed``), mid-flush → UNKNOWN (``shutdown``).

        Callers always get an outcome; nothing is failed with a bare
        exception. ``timeout=None`` waits for the queue to drain.
        """
        self._closed = True
        if self._flush_tasks:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(timeout):
                    await self._pending.join()
                    while True:
                        flushing = [i.future for i in self._active if i.state != _DONE]
                        if not flushing:
                            break
                        await asyncio.wait(flushing)
        for task in self._flush_tasks:
            task.cancel()
        for task in self._flush_tasks:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await task
        self._flush_tasks = []
        for item in list(self._active):
            item.finish(self._shutdown_outcome(item))
        self._active.clear()
        while True:
            try:
                item = self._pending.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._pending.task_done()
            item.finish(item.outcome(PublishStatus.NOT_SENT, PublishReason.PUBLISHER_CLOSED))

    @staticmethod
    def _shutdown_outcome(item: _BatchItem) -> PublishOutcome:
        if item.state == _FLUSHING:
            return item.outcome(PublishStatus.UNKNOWN, PublishReason.SHUTDOWN)
        return item.outcome(PublishStatus.NOT_SENT, PublishReason.PUBLISHER_CLOSED)

    async def publish(self, envelope: MessageEnvelope, *, timeout: float | None = None) -> PublishOutcome:
        """Enqueue *envelope* and wait (bounded) for its batch's outcome."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[PublishOutcome] = loop.create_future()
        deadline = loop.time() + (self._default_wait() if timeout is None else timeout)
        item = _BatchItem(envelope, fut, deadline, 0, self._byte_limiter)
        if self._closed:
            return item.outcome(PublishStatus.NOT_SENT, PublishReason.PUBLISHER_CLOSED)

        admission = max(0.0, min(self._admission_timeout, deadline - loop.time()))
        limiter = self._byte_limiter
        if limiter is not None:
            size = len(envelope.body)
            if size > limiter.limit:
                return item.outcome(PublishStatus.NOT_SENT, PublishReason.EXCEEDS_BYTE_BUDGET)
            try:
                admitted = await limiter.acquire(size, admission)
            except Exception as exc:  # ChannelPoolClosedError: publisher shutting down
                return item.outcome(PublishStatus.NOT_SENT, PublishReason.PUBLISHER_CLOSED, exc)
            if not admitted:
                return item.outcome(PublishStatus.NOT_SENT, PublishReason.ADMISSION_TIMEOUT)
            item.nbytes = size

        try:
            try:
                self._pending.put_nowait(item)
            except asyncio.QueueFull:
                async with asyncio.timeout(max(0.0, min(self._admission_timeout, deadline - loop.time()))):
                    await self._pending.put(item)
        except TimeoutError:
            item.finish(item.outcome(PublishStatus.NOT_SENT, PublishReason.ADMISSION_TIMEOUT))
            return fut.result()
        except BaseException:
            item.finish(item.outcome(PublishStatus.NOT_SENT, PublishReason.CANCELLED))
            raise

        # The deadline is one plain loop timer that answers the caller's own
        # future (_expire), not an asyncio.timeout + shield per call: those cost
        # ~4 µs/message on this hot path. A cancelled caller cancels only its
        # own future; the item is never cancelled (a worker may already be
        # publishing it), and finish() still resolves it and releases its bytes.
        if fut.done():  # pragma: no cover — nothing yields between enqueue and here
            return fut.result()
        caller: asyncio.Future[PublishOutcome] = loop.create_future()
        item.caller = caller
        timer = loop.call_at(deadline, _expire, item)
        try:
            return await caller
        except asyncio.CancelledError:
            if item.state == _QUEUED:
                item.finish(item.outcome(PublishStatus.NOT_SENT, PublishReason.CANCELLED))
            raise
        finally:
            timer.cancel()

    async def _next_item(self) -> _BatchItem:
        """Next live item from the queue, skipping ones their caller withdrew."""
        while True:
            item = await self._pending.get()
            self._pending.task_done()
            if item.state == _QUEUED:
                self._active.add(item)
                return item

    def _next_item_nowait(self) -> _BatchItem | None:
        while True:
            try:
                item = self._pending.get_nowait()
            except asyncio.QueueEmpty:
                return None
            self._pending.task_done()
            if item.state == _QUEUED:
                self._active.add(item)
                return item

    async def _collect(self, batch: list[_BatchItem]) -> None:
        """Wait for the first item, then fill *batch* in place (fast drain,
        then stragglers for up to ``flush_interval_ms``). In place, so a
        worker cancelled mid-collection still sees what it dequeued."""
        batch.append(await self._next_item())
        # Fast drain: grab all immediately-available items without yielding.
        while len(batch) < self._config.batch_size:
            item = self._next_item_nowait()
            if item is None:
                break
            batch.append(item)
        if len(batch) < self._config.batch_size:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._config.flush_interval_ms / 1000.0
            while len(batch) < self._config.batch_size:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    async with asyncio.timeout(remaining):
                        batch.append(await self._next_item())
                except TimeoutError:
                    break

    async def _flush_loop(self) -> None:
        """Each worker holds one channel until it breaks — no acquire/release per batch."""
        pool = self._transport._conn_pool
        channel: Any = None
        batch: list[_BatchItem] = []
        try:
            channel = await self._acquire_channel()
            while True:
                batch = []
                await self._collect(batch)
                if channel is not None and channel.is_closed:
                    old, channel = channel, None
                    with contextlib.suppress(Exception):
                        await pool.release_publisher_channel(old)
                if channel is None:
                    channel = await self._acquire_channel()
                if channel is None:
                    for item in batch:
                        item.finish(item.outcome(PublishStatus.NOT_SENT, PublishReason.CHANNEL_UNAVAILABLE))
                    self._active.difference_update(batch)
                    batch = []
                    continue
                try:
                    await self._flush(channel, batch)
                except Exception as exc:
                    # Unexpected failure of the flush itself: the channel's state
                    # is unknown, so replace it, and report what was not resolved.
                    for item in batch:
                        if item.state == _FLUSHING:
                            item.finish(item.outcome(PublishStatus.UNKNOWN, PublishReason.CONNECTION_LOST, exc))
                        else:
                            item.finish(item.outcome(PublishStatus.ERROR, PublishReason.PUBLISH_ERROR, exc))
                    old, channel = channel, None
                    pool.discard_publisher_channel(old)
                self._active.difference_update(batch)
                batch = []
        except BaseException:
            # Cancelled (stop()) or crashed: resolve this worker's items, and
            # never put a channel with in-flight confirms back in the pool.
            mid_flush = any(item.state == _FLUSHING for item in batch)
            for item in batch:
                item.finish(self._shutdown_outcome(item))
            self._active.difference_update(batch)
            if channel is not None and mid_flush:
                old, channel = channel, None
                pool.discard_publisher_channel(old)
            raise
        finally:
            if channel is not None:
                with contextlib.suppress(Exception):
                    await pool.release_publisher_channel(channel)

    async def _flush(self, channel: Any, batch: list[_BatchItem]) -> None:
        """Publish all envelopes on the worker's channel and resolve each item."""
        loop = asyncio.get_running_loop()
        live: list[_BatchItem] = []
        for item in batch:
            if item.state != _QUEUED:
                continue
            if item.deadline - loop.time() <= 0:
                item.finish(item.outcome(PublishStatus.NOT_SENT, PublishReason.ADMISSION_TIMEOUT))
                continue
            item.state = _FLUSHING
            live.append(item)
        outcomes: list[Any] = await asyncio.gather(
            *[
                self._transport._publish_on_channel(channel, item.envelope, timeout=item.deadline - loop.time())
                for item in live
            ],
            return_exceptions=True,
        )
        for item, outcome in zip(live, outcomes, strict=False):
            if isinstance(outcome, PublishOutcome):
                item.finish(outcome)
            elif isinstance(outcome, BaseException):
                # The frames may have left: a raised publish is UNKNOWN. This
                # includes a CancelledError leaked by a closing channel.
                item.finish(item.outcome(PublishStatus.UNKNOWN, PublishReason.CONNECTION_LOST, outcome))
            else:
                item.finish(item.outcome(PublishStatus.UNKNOWN, None))

        # M17: _publish_on_channel never closes the channel itself on a
        # confirm timeout (that used to fire from INSIDE one of the concurrent
        # calls above, corrupting sibling in-flight confirms sharing this same
        # channel). Instead, close it HERE — every concurrent publish in this
        # batch has already resolved by this point. The flush loop replaces a
        # closed channel before the next batch.
        if not channel.is_closed and any(
            isinstance(o, PublishOutcome) and o.status == PublishStatus.TIMEOUT for o in outcomes
        ):
            with contextlib.suppress(Exception):
                await channel.close()
