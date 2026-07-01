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
from rabbitkit.core.types import MessageEnvelope, PublishOutcome

if TYPE_CHECKING:
    from rabbitkit.async_.transport import AsyncTransportImpl

logger = logging.getLogger(__name__)


class AsyncBatchPublisher:
    """Transparent batch-publish wrapper for AsyncTransportImpl.

    N concurrent ``broker.publish()`` calls are coalesced into one batch,
    published on a single channel, and their confirms are gathered together.
    Reduces per-message pool acquire/release and amortizes confirm round-trips.

    The caller's coroutine blocks until its message is included in a flushed
    batch and the confirm resolves — semantics are identical to direct publish.

    Unlike ``highload.batch.BatchPublisher`` (a timing/buffering helper that
    publishes each message individually), this class pipelines confirms: all
    messages in a batch share one channel and their ACKs are awaited together.
    """

    def __init__(self, transport: AsyncTransportImpl, config: BatchPublishConfig) -> None:
        self._transport = transport
        self._config = config
        self._pending: asyncio.Queue[tuple[MessageEnvelope, asyncio.Future[PublishOutcome]]] = (
            asyncio.Queue(maxsize=config.max_in_flight)
        )
        self._flush_tasks: list[asyncio.Task[None]] = []

    def _worker_count(self) -> int:
        if self._config.flush_workers > 0:
            return self._config.flush_workers
        return min(16, max(1, self._config.max_in_flight // self._config.batch_size))

    async def start(self) -> None:
        """Start N concurrent flush loops (one per channel slot)."""
        n = self._worker_count()
        self._flush_tasks = [
            asyncio.create_task(self._flush_loop(), name=f"rabbitkit.batch-flush-{i}")
            for i in range(n)
        ]

    async def stop(self) -> None:
        """Cancel all flush loops and drain any remaining queued messages."""
        for task in self._flush_tasks:
            task.cancel()
        for task in self._flush_tasks:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await task
        self._flush_tasks = []
        while not self._pending.empty():
            try:
                _, fut = self._pending.get_nowait()
                if not fut.done():
                    fut.set_exception(RuntimeError("BatchPublisher stopped before flush"))
            except asyncio.QueueEmpty:  # pragma: no cover — TOCTOU guard, unreachable in asyncio
                break

    async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Enqueue *envelope* and wait for it to be included in a batch flush."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[PublishOutcome] = loop.create_future()
        await self._pending.put((envelope, fut))
        return await fut

    async def _flush_loop(self) -> None:
        """Each worker holds one channel for its lifetime — no acquire/release per batch."""
        interval = self._config.flush_interval_ms / 1000.0
        channel: Any = None
        batch: list[tuple[MessageEnvelope, asyncio.Future[PublishOutcome]]] = []
        try:
            channel = await self._transport._conn_pool.acquire_publisher_channel()
            while True:
                batch = []

                # Block until the first item arrives
                batch.append(await self._pending.get())

                # Fast drain: grab all immediately-available items without yielding.
                # At high concurrency the queue is almost always non-empty here, so
                # this avoids the coroutine/timeout overhead of repeated wait_for calls.
                while len(batch) < self._config.batch_size:
                    try:
                        batch.append(self._pending.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                # If still under batch_size, wait briefly for stragglers
                if len(batch) < self._config.batch_size:
                    loop = asyncio.get_running_loop()
                    deadline = loop.time() + interval
                    while len(batch) < self._config.batch_size:
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            break
                        try:
                            async with asyncio.timeout(remaining):
                                batch.append(await self._pending.get())
                        except TimeoutError:
                            break

                try:
                    await self._flush(channel, batch)
                except BaseException as exc:
                    # Settle any futures _flush didn't resolve (including on
                    # CancelledError). Convert CancelledError to RuntimeError so
                    # it can be set on the Future (set_exception rejects BaseException).
                    err: BaseException = (
                        exc if isinstance(exc, Exception) else RuntimeError("Batch publisher cancelled")
                    )
                    for _, fut in batch:
                        if not fut.done():
                            fut.set_exception(err)
                    if isinstance(exc, Exception):
                        # Channel error — replace it. Set channel=None first so
                        # the finally block doesn't double-release if acquire fails.
                        old, channel = channel, None
                        with contextlib.suppress(Exception):
                            await self._transport._conn_pool.release_publisher_channel(old)
                        channel = await self._transport._conn_pool.acquire_publisher_channel()
                    else:
                        raise  # CancelledError must propagate
                else:
                    # _publish_on_channel closes the channel on confirm timeout;
                    # detect that and replace it before the next batch.
                    if channel.is_closed:
                        old, channel = channel, None
                        with contextlib.suppress(Exception):
                            await self._transport._conn_pool.release_publisher_channel(old)
                        channel = await self._transport._conn_pool.acquire_publisher_channel()
        except BaseException:
            # Any exception escaping the loop (e.g. CancelledError at pending.get()
            # or straggler wait) — settle any dequeued-but-unresolved futures.
            for _, fut in batch:
                if not fut.done():
                    fut.set_exception(RuntimeError("Batch publisher cancelled"))
            raise
        finally:
            if channel is not None:
                with contextlib.suppress(Exception):
                    await self._transport._conn_pool.release_publisher_channel(channel)

    async def _flush(
        self,
        channel: Any,
        batch: list[tuple[MessageEnvelope, asyncio.Future[PublishOutcome]]],
    ) -> None:
        """Publish all envelopes on the worker's persistent channel and resolve futures."""
        outcomes: list[Any] = await asyncio.gather(
            *[self._transport._publish_on_channel(channel, env) for env, _ in batch],
            return_exceptions=True,
        )
        for (_, fut), outcome in zip(batch, outcomes, strict=False):
            if not fut.done():
                if isinstance(outcome, BaseException):
                    fut.set_exception(outcome)
                else:
                    fut.set_result(outcome)
