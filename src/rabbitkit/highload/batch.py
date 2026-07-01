"""Batch publish and multi-ack — high-throughput helpers.

``BatchPublisher`` buffers outgoing envelopes and flushes them as a batch,
optionally confirming delivery after flush.

``BatchAcker`` accumulates delivery tags and issues a single
``ack(max_tag, multiple=True)`` when the batch fills or is flushed.

Both are channel-scoped — never cross channels.

NOTE (I-7): do NOT mix the sync and async APIs on a single instance. The sync
``add``/``flush``/``close`` use a ``threading.Lock`` and a ``threading.Timer``;
the async ``add_async``/``flush_async``/``close_async`` use an ``asyncio.Lock``
and an ``asyncio.Task``. Each API path cancels the *other* path's timer/task on
close so a stray leftover does not fire after shutdown, but the buffer is not
safe to mutate concurrently from both worlds at once. Pick one mode per
instance.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from typing import Any

from rabbitkit.core.config import BatchAckConfig, BatchPublishConfig
from rabbitkit.core.types import MessageEnvelope

logger = logging.getLogger(__name__)


# ── BatchPublisher ───────────────────────────────────────────────────────


class BatchPublisher:
    """Buffer outgoing envelopes and flush as a batch.

    When ``flush_interval_ms > 0`` (default 50 ms), a background timer
    fires periodically to flush any buffered envelopes even if
    ``batch_size`` has not been reached.  The timer starts lazily on the
    first call to ``add()`` and is cancelled by ``close()`` / ``close_async()``.

    ``max_in_flight`` is reserved for future async-confirm support and has
    no runtime effect in the current synchronous-confirm model.

    NOTE (throughput): this is a *buffering/timing* helper, not wire-level
    batching. ``flush`` publishes each buffered envelope via ``publish_fn``, so
    if ``publish_fn`` awaits a confirm per message the confirms do not pipeline —
    you get ergonomics, not extra throughput. For high-volume confirmed
    publishing, pipeline confirms yourself (publish many, then await) or use the
    transactional outbox; for safety-critical events, always use the outbox.

    Usage::

        bp = BatchPublisher(
            config=BatchPublishConfig(batch_size=50, flush_interval_ms=100),
            publish_fn=transport.publish,
            confirm_fn=transport.wait_for_confirms,  # optional
        )
        bp.add(envelope1)
        bp.add(envelope2)
        ...
        bp.flush()   # publishes all buffered
        bp.close()   # flush remaining + cancel timer
    """

    def __init__(
        self,
        publish_fn: Callable[[MessageEnvelope], Any],
        config: BatchPublishConfig | None = None,
        confirm_fn: Callable[[], Any] | None = None,
    ) -> None:
        self._config = config or BatchPublishConfig()
        self._publish_fn = publish_fn
        self._confirm_fn = confirm_fn
        self._buffer: list[MessageEnvelope] = []
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._closed = False
        self._flush_task: asyncio.Task[None] | None = None
        # I-7: an asyncio.Lock guards _buffer/_flush_task mutations in the async
        # path so the background interval loop and add_async/flush_async/close_async
        # cannot race and lose messages. Lazily created inside the event loop.
        self._async_lock: asyncio.Lock | None = None

    @property
    def pending(self) -> int:
        """Number of envelopes buffered but not yet flushed."""
        return len(self._buffer)

    def _ensure_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    # ── Timer helpers ────────────────────────────────────────────────────

    def _schedule_timer(self) -> None:
        # MH-1: defensive — only arm a new timer when none is already running
        # so a concurrent add()/timer-callback cannot double-schedule an orphan
        # timer that fires forever (leaking daemon threads).
        if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
            interval = self._config.flush_interval_ms / 1000.0
            self._timer = threading.Timer(interval, self._timer_callback)
            self._timer.daemon = True
            self._timer.start()

    def _timer_callback(self) -> None:
        # I-7: clear the timer slot under the lock so concurrent flush()/add()
        # see a consistent None and cannot double-schedule.
        with self._lock:
            self._timer = None
        self.flush()
        # MH-1: reschedule UNDER the lock with a None-guard. A concurrent add()
        # that armed a timer during flush() (outside the lock) leaves
        # self._timer set, so we skip rescheduling here instead of starting an
        # orphan timer that fires forever.
        with self._lock:
            if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
                self._schedule_timer()

    # ── Sync API ─────────────────────────────────────────────────────────

    def add(self, envelope: MessageEnvelope) -> None:
        """Add an envelope to the batch buffer.

        Auto-flushes when ``batch_size`` is reached.  Starts the interval
        timer on the first call when ``flush_interval_ms > 0``.
        """
        with self._lock:
            self._buffer.append(envelope)
            should_flush = len(self._buffer) >= self._config.batch_size
            if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
                self._schedule_timer()
        if should_flush:
            self.flush()

    def flush(self) -> int:
        """Publish all buffered envelopes.

        Returns the number of envelopes published.
        """
        with self._lock:
            if not self._buffer:
                return 0
            count = len(self._buffer)
            batch = list(self._buffer)
            self._buffer.clear()

        for envelope in batch:
            self._publish_fn(envelope)

        if self._confirm_fn is not None:
            self._confirm_fn()

        logger.debug("Batch-published %d envelopes", count)
        return count

    def close(self) -> int:
        """Flush remaining envelopes, cancel the interval timer, and clean up.

        Returns the number of envelopes flushed.
        """
        self._closed = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        # I-7: also cancel a stray async interval task so closing via the sync
        # API does not leave the async loop running.
        if self._flush_task is not None:
            self._flush_task.cancel()
            self._flush_task = None
        return self.flush()

    # ── Async API ────────────────────────────────────────────────────────

    async def _interval_loop_async(self) -> None:
        interval = self._config.flush_interval_ms / 1000.0
        try:
            while True:
                await asyncio.sleep(interval)
                await self.flush_async()
        except asyncio.CancelledError:
            pass

    async def add_async(self, envelope: MessageEnvelope) -> None:
        """Async: add envelope; auto-flush at batch_size.

        Starts the async interval loop on the first call when
        ``flush_interval_ms > 0``.
        """
        # I-7: guard _buffer / _flush_task mutations with the async lock so a
        # concurrent interval-loop flush_async cannot interleave with this add
        # and lose/duplicate buffered envelopes.
        should_flush = False
        async with self._ensure_async_lock():
            self._buffer.append(envelope)
            if len(self._buffer) >= self._config.batch_size:
                should_flush = True
            elif self._config.flush_interval_ms > 0 and self._flush_task is None:
                self._flush_task = asyncio.create_task(self._interval_loop_async())
        if should_flush:
            await self.flush_async()

    async def flush_async(self) -> int:
        """Async: publish all buffered envelopes."""
        # I-7: take the batch snapshot under the async lock so concurrent
        # flush_async / add_async calls cannot both grab the same buffer.
        async with self._ensure_async_lock():
            if not self._buffer:
                return 0
            count = len(self._buffer)
            batch = list(self._buffer)
            self._buffer.clear()

        for envelope in batch:
            result = self._publish_fn(envelope)
            if hasattr(result, "__await__"):
                await result

        if self._confirm_fn is not None:
            result = self._confirm_fn()
            if hasattr(result, "__await__"):
                await result

        logger.debug("Async batch-published %d envelopes", count)
        return count

    async def close_async(self) -> int:
        """Async: cancel the interval loop, flush remaining, and clean up."""
        self._closed = True
        async with self._ensure_async_lock():
            if self._flush_task is not None:
                self._flush_task.cancel()
                self._flush_task = None
        # I-7: also cancel a stray sync timer so closing via the async API does
        # not leave the sync timer firing after shutdown.
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        return await self.flush_async()


# ── BatchAcker ───────────────────────────────────────────────────────────


class BatchAcker:
    """Accumulate delivery tags and ack in batches.

    Uses ``multiple=True`` on the maximum delivery tag in the batch.

    When ``flush_interval_ms > 0`` (default 200 ms), a background timer
    fires periodically to ack any buffered tags even if ``batch_size`` has
    not been reached.  The timer starts lazily on the first call to
    ``add()`` and is cancelled by ``close()`` / ``close_async()``.

    **Ownership rules:**
    - Channel-scoped — NEVER cross channels
    - Handlers MUST NOT call ``msg.ack()`` when BatchAcker is active
    - Compatible with AUTO and NACK_ON_ERROR policies only

    Usage::

        ba = BatchAcker(
            config=BatchAckConfig(batch_size=50, flush_interval_ms=200),
            ack_fn=channel.basic_ack,
        )
        ba.add(delivery_tag=1)
        ba.add(delivery_tag=2)
        ...
        ba.flush()  # ack(max_tag, multiple=True)
        ba.close()  # flush remaining + cancel timer
    """

    def __init__(
        self,
        ack_fn: Callable[..., Any],
        config: BatchAckConfig | None = None,
    ) -> None:
        self._config = config or BatchAckConfig()
        self._ack_fn = ack_fn
        self._tags: list[int] = []
        self._max_tag: int = 0  # O(1) max tracking (perf)
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._closed = False
        self._flush_task: asyncio.Task[None] | None = None
        # I-7: asyncio.Lock guarding _tags/_flush_task in the async path.
        # Lazily created inside the event loop.
        self._async_lock: asyncio.Lock | None = None

    @property
    def pending(self) -> int:
        """Number of delivery tags buffered."""
        return len(self._tags)

    def _ensure_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    # ── Timer helpers ────────────────────────────────────────────────────

    def _schedule_timer(self) -> None:
        # MH-1: defensive — only arm a new timer when none is already running
        # so a concurrent add()/timer-callback cannot double-schedule an orphan
        # timer that fires forever (leaking daemon threads).
        if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
            interval = self._config.flush_interval_ms / 1000.0
            self._timer = threading.Timer(interval, self._timer_callback)
            self._timer.daemon = True
            self._timer.start()

    def _timer_callback(self) -> None:
        # I-7: clear the timer slot under the lock for consistency.
        with self._lock:
            self._timer = None
        self.flush()
        # MH-1: reschedule UNDER the lock with a None-guard so a concurrent
        # add() that armed a timer during flush() doesn't leave us starting an
        # orphan timer that fires forever.
        with self._lock:
            if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
                self._schedule_timer()

    # ── Sync API ─────────────────────────────────────────────────────────

    def add(self, delivery_tag: int) -> None:
        """Add a delivery tag to the batch.

        Auto-flushes when ``batch_size`` is reached.  Starts the interval
        timer on the first call when ``flush_interval_ms > 0``.
        """
        with self._lock:
            self._tags.append(delivery_tag)
            if delivery_tag > self._max_tag:
                self._max_tag = delivery_tag
            should_flush = len(self._tags) >= self._config.batch_size
            if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
                self._schedule_timer()
        if should_flush:
            self.flush()

    def flush(self) -> int:
        """Ack all buffered tags using the max tag with multiple=True.

        Returns the number of tags acked.
        """
        with self._lock:
            if not self._tags:
                return 0
            count = len(self._tags)
            max_tag = max(self._tags)
            self._tags.clear()

        self._ack_fn(max_tag, multiple=True)
        logger.debug("Batch-acked %d messages (max_tag=%d)", count, max_tag)
        return count

    def close(self) -> int:
        """Flush remaining tags, cancel the interval timer, and clean up."""
        self._closed = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        # I-7: also cancel a stray async interval task.
        if self._flush_task is not None:
            self._flush_task.cancel()
            self._flush_task = None
        return self.flush()

    # ── Async API ────────────────────────────────────────────────────────

    async def _interval_loop_async(self) -> None:
        interval = self._config.flush_interval_ms / 1000.0
        try:
            while True:
                await asyncio.sleep(interval)
                await self.flush_async()
        except asyncio.CancelledError:
            pass

    async def add_async(self, delivery_tag: int) -> None:
        """Async: add tag; auto-flush at batch_size.

        Starts the async interval loop on the first call when
        ``flush_interval_ms > 0``.
        """
        # I-7: guard _tags / _flush_task mutations with the async lock.
        should_flush = False
        async with self._ensure_async_lock():
            self._tags.append(delivery_tag)
            if len(self._tags) >= self._config.batch_size:
                should_flush = True
            elif self._config.flush_interval_ms > 0 and self._flush_task is None:
                self._flush_task = asyncio.create_task(self._interval_loop_async())
        if should_flush:
            await self.flush_async()

    async def flush_async(self) -> int:
        """Async: ack all buffered tags."""
        # I-7: take the batch snapshot under the async lock.
        async with self._ensure_async_lock():
            if not self._tags:
                return 0
            count = len(self._tags)
            max_tag = max(self._tags)
            self._tags.clear()

        result = self._ack_fn(max_tag, multiple=True)
        if hasattr(result, "__await__"):
            await result

        logger.debug("Async batch-acked %d messages (max_tag=%d)", count, max_tag)
        return count

    async def close_async(self) -> int:
        """Async: cancel the interval loop, flush remaining, and clean up."""
        self._closed = True
        async with self._ensure_async_lock():
            if self._flush_task is not None:
                self._flush_task.cancel()
                self._flush_task = None
        # I-7: also cancel a stray sync timer.
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        return await self.flush_async()
