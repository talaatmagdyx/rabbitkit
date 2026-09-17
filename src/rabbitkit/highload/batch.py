"""Batch publish and batch ack — high-throughput helpers.

``BatchPublisher`` buffers outgoing envelopes and flushes them as a batch,
optionally confirming delivery after flush. Every flush produces a
:class:`FlushReport` with a per-item disposition; a mid-flush failure never
loses the unsent tail (plan §4.2).

``BatchAcker`` accumulates delivery tags. In the default ``"individual"``
mode every tag is acked with its own ``multiple=False`` frame — selected
completions are NOT a cumulative watermark (plan §4.1). The legacy
``"cumulative"`` mode (``ack(max_tag, multiple=True)``) is opt-in and
requires an explicit ordered/exclusive-ownership attestation.

``CoalescingAcker`` is the safe way to get wire-level coalescing with
arbitrary completion order: it wraps a channel-wide
:class:`~rabbitkit.core.settlement.SettlementCoordinator` that knows every
outstanding delivery and only emits ``multiple=True`` through a tag when
every lower outstanding tag is approved.

All three are channel-scoped — never cross channels.

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
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from rabbitkit.core.bulk import (
    REASON_CANCELLED,
    REASON_NOT_STARTED,
    REASON_PUBLISH_ERROR,
    classify_publish_exception,
    classify_publish_outcome,
)
from rabbitkit.core.config import BatchAckConfig, BatchPublishConfig
from rabbitkit.core.settlement import (
    SettlementCommand,
    SettlementCoordinator,
    apply_commands,
)
from rabbitkit.core.types import (
    BulkPublishStatus,
    FlushReason,
    MessageEnvelope,
    PublishOutcome,
    SettlementAction,
)

logger = logging.getLogger(__name__)


class BatchClosedError(RuntimeError):
    """``add()`` after ``close()`` — the buffer no longer accepts work."""


# ── Flush accounting ─────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class FlushItem:
    """Disposition of one envelope in a flush."""

    index: int
    envelope: MessageEnvelope
    status: BulkPublishStatus
    reason: str
    outcome: PublishOutcome | None = None
    error: BaseException | None = None

    @property
    def ok(self) -> bool:
        return self.status is BulkPublishStatus.CONFIRMED


@dataclass(frozen=True, slots=True)
class FlushReport:
    """Complete per-item accounting for one flush.

    ``unsent`` is the tail that was never handed to ``publish_fn`` because an
    earlier publish raised. It is NOT re-buffered automatically (safety
    invariant 7 — no blind replay of a partially successful batch); the
    caller decides.
    """

    items: tuple[FlushItem, ...] = ()
    unsent: tuple[MessageEnvelope, ...] = ()
    confirm_error: BaseException | None = None
    started_at: float = 0.0
    finished_at: float = 0.0

    @property
    def attempted(self) -> int:
        return len(self.items)

    @property
    def published(self) -> int:
        """Items whose publish did not fail locally (CONFIRMED or UNKNOWN)."""
        return sum(1 for it in self.items if it.status in (BulkPublishStatus.CONFIRMED, BulkPublishStatus.UNKNOWN))

    @property
    def confirmed(self) -> int:
        return sum(1 for it in self.items if it.ok)

    @property
    def unknown(self) -> tuple[FlushItem, ...]:
        return tuple(it for it in self.items if it.status is BulkPublishStatus.UNKNOWN)

    @property
    def failed(self) -> tuple[FlushItem, ...]:
        return tuple(it for it in self.items if not it.ok)

    @property
    def complete(self) -> bool:
        return not self.unsent and self.confirm_error is None and all(it.ok for it in self.items)


class BatchFlushError(RuntimeError):
    """A flush did not complete cleanly. ``report`` has the full accounting
    (including the unsent tail). Raised by ``flush()`` only when a publish
    call RAISED or the confirm step raised — a non-ok ``PublishOutcome``
    returned by ``publish_fn`` is recorded in the report, not raised."""

    def __init__(self, report: FlushReport, cause: BaseException) -> None:
        self.report = report
        self.cause = cause
        super().__init__(
            f"batch flush interrupted after {report.attempted} of "
            f"{report.attempted + len(report.unsent)} envelopes: {type(cause).__name__}"
        )


# ── BatchPublisher ───────────────────────────────────────────────────────


class BatchPublisher:
    """Buffer outgoing envelopes and flush as a batch.

    When ``flush_interval_ms > 0`` (default 50 ms), a background timer
    fires periodically to flush any buffered envelopes even if
    ``batch_size`` has not been reached.  The timer starts lazily on the
    first call to ``add()`` and is cancelled by ``close()`` / ``close_async()``.

    ``max_in_flight`` is reserved for future async-confirm support and has
    no runtime effect in the current synchronous-confirm model.

    Flush semantics (plan §4.2):

    * ``flush()`` returns the number of envelopes whose publish did not fail
      locally (CONFIRMED + UNKNOWN) — the legacy int contract — and stores
      the full :class:`FlushReport` on :attr:`last_flush`. Use
      :meth:`flush_report` to get the report directly.
    * A ``publish_fn`` that RAISES mid-batch stops the flush; the items
      already published keep their real outcomes, the raising item is
      ``UNKNOWN`` (it may have reached the broker), and the unsent tail is
      preserved on ``report.unsent`` — then ``BatchFlushError`` is raised.
      Nothing is silently put back into the buffer.
    * A ``publish_fn`` that returns a non-ok ``PublishOutcome`` (NACKED,
      RETURNED, TIMEOUT...) is recorded per item — success is never inferred
      from loop iterations.
    * Background-timer failures are exposed via :attr:`last_error` and the
      optional ``on_error`` callback instead of dying silently on the timer
      thread.

    NOTE (throughput): this is a *buffering/timing* helper, not wire-level
    batching. ``flush`` publishes each buffered envelope via ``publish_fn``, so
    if ``publish_fn`` awaits a confirm per message the confirms do not pipeline —
    you get ergonomics, not extra throughput. For high-volume confirmed
    publishing use ``broker.publish_many`` (bounded, per-item outcomes) or the
    pipelined ``AsyncBatchPublisher`` / ``SyncBatchPublisher``.
    """

    def __init__(
        self,
        publish_fn: Callable[[MessageEnvelope], Any],
        config: BatchPublishConfig | None = None,
        confirm_fn: Callable[[], Any] | None = None,
        *,
        on_error: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._config = config or BatchPublishConfig()
        self._publish_fn = publish_fn
        self._confirm_fn = confirm_fn
        self._on_error = on_error
        self._buffer: list[MessageEnvelope] = []
        self._lock = threading.Lock()
        # Serializes flushes: a timer flush and a manual flush can never
        # interleave publishes from two snapshots.
        self._flush_lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._closed = False
        self._flush_task: asyncio.Task[None] | None = None
        self._async_lock: asyncio.Lock | None = None
        self._async_flush_lock: asyncio.Lock | None = None
        self.last_flush: FlushReport | None = None
        self.last_error: BaseException | None = None

    @property
    def pending(self) -> int:
        """Number of envelopes buffered but not yet flushed."""
        return len(self._buffer)

    @property
    def closed(self) -> bool:
        return self._closed

    def _ensure_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    def _ensure_async_flush_lock(self) -> asyncio.Lock:
        if self._async_flush_lock is None:
            self._async_flush_lock = asyncio.Lock()
        return self._async_flush_lock

    def _record_error(self, exc: BaseException) -> None:
        self.last_error = exc
        if self._on_error is not None:
            try:
                self._on_error(exc)
            except Exception:
                logger.exception("BatchPublisher on_error callback raised")

    # ── Timer helpers ────────────────────────────────────────────────────

    def _schedule_timer(self) -> None:
        if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
            interval = self._config.flush_interval_ms / 1000.0
            self._timer = threading.Timer(interval, self._timer_callback)
            self._timer.daemon = True
            self._timer.start()

    def _timer_callback(self) -> None:
        with self._lock:
            self._timer = None
        try:
            self.flush()
        except BaseException as exc:
            # Never let a timer-thread failure vanish: record + log, keep the
            # helper usable (unsent tail is in last_flush.unsent).
            self._record_error(exc)
            logger.error("BatchPublisher interval flush failed: %s", exc, exc_info=True)
        with self._lock:
            if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
                self._schedule_timer()

    # ── Sync API ─────────────────────────────────────────────────────────

    def add(self, envelope: MessageEnvelope) -> None:
        """Add an envelope to the batch buffer.

        Auto-flushes when ``batch_size`` is reached.  Starts the interval
        timer on the first call when ``flush_interval_ms > 0``. Raises
        :class:`BatchClosedError` after ``close()``.
        """
        with self._lock:
            if self._closed:
                raise BatchClosedError("BatchPublisher is closed; add() rejected")
            self._buffer.append(envelope)
            should_flush = len(self._buffer) >= self._config.batch_size
            if self._timer is None and self._config.flush_interval_ms > 0:
                self._schedule_timer()
        if should_flush:
            self.flush()

    def _run_flush(self, batch: list[MessageEnvelope], *, is_async: bool = False) -> FlushReport:
        """Publish *batch* item by item with full accounting (sync)."""
        started = time.monotonic()
        items: list[FlushItem] = []
        for i, envelope in enumerate(batch):
            try:
                result = self._publish_fn(envelope)
            except BaseException as exc:
                status, reason = classify_publish_exception(exc)
                items.append(FlushItem(index=i, envelope=envelope, status=status, reason=reason, error=exc))
                report = FlushReport(
                    items=tuple(items),
                    unsent=tuple(batch[i + 1 :]),
                    started_at=started,
                    finished_at=time.monotonic(),
                )
                self.last_flush = report
                raise BatchFlushError(report, exc) from exc
            outcome = result if isinstance(result, PublishOutcome) else None
            status, reason = classify_publish_outcome(outcome)
            items.append(FlushItem(index=i, envelope=envelope, status=status, reason=reason, outcome=outcome))

        confirm_error: BaseException | None = None
        if self._confirm_fn is not None:
            try:
                self._confirm_fn()
            except BaseException as exc:
                confirm_error = exc
        report = FlushReport(
            items=tuple(items), confirm_error=confirm_error, started_at=started, finished_at=time.monotonic()
        )
        self.last_flush = report
        if confirm_error is not None:
            raise BatchFlushError(report, confirm_error) from confirm_error
        logger.debug("Batch-published %d envelopes (%d confirmed)", report.attempted, report.confirmed)
        return report

    def flush_report(self) -> FlushReport:
        """Publish all buffered envelopes and return the full :class:`FlushReport`.

        Raises :class:`BatchFlushError` (carrying the report) if a publish or
        the confirm step raised.
        """
        with self._flush_lock:
            with self._lock:
                if not self._buffer:
                    return FlushReport(started_at=time.monotonic(), finished_at=time.monotonic())
                batch = list(self._buffer)
                self._buffer.clear()
            return self._run_flush(batch)

    def flush(self) -> int:
        """Publish all buffered envelopes.

        Returns the number of envelopes whose publish did not fail locally.
        See :meth:`flush_report` for per-item detail (also on :attr:`last_flush`).
        """
        return self.flush_report().published

    def close(self) -> int:
        """Flush remaining envelopes, cancel the interval timer, and clean up.

        Idempotent. Returns the number of envelopes flushed on THIS call.
        """
        with self._lock:
            self._closed = True
            timer, self._timer = self._timer, None
            task, self._flush_task = self._flush_task, None
        if timer is not None:
            timer.cancel()
        if task is not None:
            task.cancel()
        return self.flush()

    # ── Async API ────────────────────────────────────────────────────────

    async def _interval_loop_async(self) -> None:
        interval = self._config.flush_interval_ms / 1000.0
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.flush_async()
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    self._record_error(exc)
                    logger.error("BatchPublisher async interval flush failed: %s", exc, exc_info=True)
        except asyncio.CancelledError:
            pass

    async def add_async(self, envelope: MessageEnvelope) -> None:
        """Async: add envelope; auto-flush at batch_size. Raises
        :class:`BatchClosedError` after ``close_async()``."""
        should_flush = False
        async with self._ensure_async_lock():
            if self._closed:
                raise BatchClosedError("BatchPublisher is closed; add_async() rejected")
            self._buffer.append(envelope)
            if len(self._buffer) >= self._config.batch_size:
                should_flush = True
            elif self._config.flush_interval_ms > 0 and self._flush_task is None:
                self._flush_task = asyncio.create_task(self._interval_loop_async())
        if should_flush:
            await self.flush_async()

    async def _run_flush_async(self, batch: list[MessageEnvelope]) -> FlushReport:
        started = time.monotonic()
        items: list[FlushItem] = []
        for i, envelope in enumerate(batch):
            try:
                result = self._publish_fn(envelope)
                if hasattr(result, "__await__"):
                    result = await result
            except BaseException as exc:
                status, reason = classify_publish_exception(exc)
                items.append(FlushItem(index=i, envelope=envelope, status=status, reason=reason, error=exc))
                report = FlushReport(
                    items=tuple(items),
                    unsent=tuple(batch[i + 1 :]),
                    started_at=started,
                    finished_at=time.monotonic(),
                )
                self.last_flush = report
                raise BatchFlushError(report, exc) from exc
            outcome = result if isinstance(result, PublishOutcome) else None
            status, reason = classify_publish_outcome(outcome)
            items.append(FlushItem(index=i, envelope=envelope, status=status, reason=reason, outcome=outcome))

        confirm_error: BaseException | None = None
        if self._confirm_fn is not None:
            try:
                result = self._confirm_fn()
                if hasattr(result, "__await__"):
                    await result
            except BaseException as exc:
                confirm_error = exc
        report = FlushReport(
            items=tuple(items), confirm_error=confirm_error, started_at=started, finished_at=time.monotonic()
        )
        self.last_flush = report
        if confirm_error is not None:
            raise BatchFlushError(report, confirm_error) from confirm_error
        logger.debug("Async batch-published %d envelopes (%d confirmed)", report.attempted, report.confirmed)
        return report

    async def flush_report_async(self) -> FlushReport:
        """Async: publish all buffered envelopes and return the :class:`FlushReport`."""
        async with self._ensure_async_flush_lock():
            async with self._ensure_async_lock():
                if not self._buffer:
                    return FlushReport(started_at=time.monotonic(), finished_at=time.monotonic())
                batch = list(self._buffer)
                self._buffer.clear()
            return await self._run_flush_async(batch)

    async def flush_async(self) -> int:
        """Async: publish all buffered envelopes; returns the published count."""
        return (await self.flush_report_async()).published

    async def close_async(self) -> int:
        """Async: cancel the interval loop, flush remaining, and clean up. Idempotent."""
        async with self._ensure_async_lock():
            self._closed = True
            task, self._flush_task = self._flush_task, None
            timer, self._timer = self._timer, None
        if task is not None:
            task.cancel()
        if timer is not None:
            timer.cancel()
        return await self.flush_async()


# ── BatchAcker ───────────────────────────────────────────────────────────


class BatchAcker:
    """Accumulate delivery tags and ack them in batches.

    **Default mode ``"individual"`` (safe):** ``flush()`` issues one
    ``ack_fn(tag, multiple=False)`` per buffered tag. Submitting completed
    tags 1 and 3 acks exactly 1 and 3 — tag 2, still processing on the same
    channel, is untouched. This is a bulk API even though it emits one frame
    per tag; correctness comes before wire savings.

    **Mode ``"cumulative"`` (opt-in, legacy):** ``ack_fn(max_tag,
    multiple=True)``. RabbitMQ settles every outstanding tag <= ``max_tag``
    on the channel, so this is only safe when this acker is the channel's
    sole settler and completions arrive in tag order. ``BatchAckConfig``
    refuses the mode without ``ordered_exclusive_owner=True``. For safe
    coalescing under arbitrary completion order use :class:`CoalescingAcker`.

    When ``flush_interval_ms > 0`` (default 200 ms), a background timer
    fires periodically to ack any buffered tags even if ``batch_size`` has
    not been reached.  The timer starts lazily on the first call to
    ``add()`` and is cancelled by ``close()`` / ``close_async()``.

    **Ownership rules:**
    - Channel-scoped — NEVER cross channels
    - Handlers MUST NOT call ``msg.ack()`` when BatchAcker is active
    - Compatible with AUTO and NACK_ON_ERROR policies only

    Usage (sync / pika) — ``ack_fn`` MUST NOT be a raw ``channel.basic_ack``.
    The interval timer fires ``flush()`` from a background
    ``threading.Timer`` thread, not pika's connection I/O thread; pika
    channel methods are not thread-safe. Marshal onto the I/O thread, e.g.
    via ``connection.add_callback_threadsafe``::

        def safe_ack(delivery_tag: int, multiple: bool = False) -> None:
            connection.add_callback_threadsafe(
                lambda: channel.basic_ack(delivery_tag=delivery_tag, multiple=multiple)
            )

        ba = BatchAcker(config=BatchAckConfig(batch_size=50), ack_fn=safe_ack)
        ba.add(delivery_tag=1)
        ba.add(delivery_tag=3)
        ba.flush()  # ack(1, multiple=False); ack(3, multiple=False)
        ba.close()  # flush remaining + cancel timer

    The async path (``add_async``/``flush_async``) schedules on the same event
    loop the aio-pika channel runs on, so an aio-pika ``channel.basic_ack``
    coroutine function can be passed directly as ``ack_fn``.
    """

    def __init__(
        self,
        ack_fn: Callable[..., Any],
        config: BatchAckConfig | None = None,
        *,
        on_error: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._config = config or BatchAckConfig()
        self._ack_fn = ack_fn
        self._on_error = on_error
        self._tags: list[int] = []
        self._seen: set[int] = set()
        self._lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._closed = False
        self._flush_task: asyncio.Task[None] | None = None
        self._async_lock: asyncio.Lock | None = None
        self._async_flush_lock: asyncio.Lock | None = None
        self.last_error: BaseException | None = None
        self.acked_total = 0
        if self._config.mode == "cumulative":
            logger.warning(
                "BatchAcker in cumulative mode: ack(max_tag, multiple=True) settles EVERY outstanding "
                "delivery on the channel up to max_tag. Caller attested ordered/exclusive ownership."
            )

    @property
    def pending(self) -> int:
        """Number of delivery tags buffered."""
        return len(self._tags)

    @property
    def mode(self) -> str:
        return self._config.mode

    @property
    def closed(self) -> bool:
        return self._closed

    def _ensure_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    def _ensure_async_flush_lock(self) -> asyncio.Lock:
        if self._async_flush_lock is None:
            self._async_flush_lock = asyncio.Lock()
        return self._async_flush_lock

    def _record_error(self, exc: BaseException) -> None:
        self.last_error = exc
        if self._on_error is not None:
            try:
                self._on_error(exc)
            except Exception:
                logger.exception("BatchAcker on_error callback raised")

    # ── Timer helpers ────────────────────────────────────────────────────

    def _schedule_timer(self) -> None:
        if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
            interval = self._config.flush_interval_ms / 1000.0
            self._timer = threading.Timer(interval, self._timer_callback)
            self._timer.daemon = True
            self._timer.start()

    def _timer_callback(self) -> None:
        with self._lock:
            self._timer = None
        try:
            self.flush()
        except BaseException as exc:
            self._record_error(exc)
            logger.error("BatchAcker interval flush failed: %s", exc, exc_info=True)
        with self._lock:
            if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
                self._schedule_timer()

    # ── Sync API ─────────────────────────────────────────────────────────

    def add(self, delivery_tag: int) -> None:
        """Add a delivery tag to the batch.

        Auto-flushes when ``batch_size`` is reached.  Starts the interval
        timer on the first call when ``flush_interval_ms > 0``. A tag already
        buffered is ignored (a double completion must not double-ack).
        Raises :class:`BatchClosedError` after ``close()``.
        """
        with self._lock:
            if self._closed:
                raise BatchClosedError("BatchAcker is closed; add() rejected")
            if delivery_tag in self._seen:
                return
            self._seen.add(delivery_tag)
            self._tags.append(delivery_tag)
            should_flush = len(self._tags) >= self._config.batch_size
            if self._timer is None and self._config.flush_interval_ms > 0:
                self._schedule_timer()
        if should_flush:
            self.flush()

    def _take_snapshot(self) -> list[int]:
        with self._lock:
            tags = list(self._tags)
            self._tags.clear()
            self._seen.clear()
            return tags

    def _emit(self, tags: list[int]) -> int:
        if self._config.mode == "cumulative":
            max_tag = max(tags)
            self._ack_fn(max_tag, multiple=True)
            self.acked_total += len(tags)
            logger.debug("Batch-acked %d messages cumulatively (max_tag=%d)", len(tags), max_tag)
            return len(tags)
        acked = 0
        for i, tag in enumerate(sorted(tags)):
            try:
                self._ack_fn(tag, multiple=False)
            except BaseException:
                # Put the not-yet-acked remainder back so nothing is lost;
                # the tag that raised is NOT re-queued (its state is unknown).
                remainder = sorted(tags)[i + 1 :]
                with self._lock:
                    for t in remainder:
                        if t not in self._seen:
                            self._seen.add(t)
                            self._tags.append(t)
                raise
            acked += 1
        self.acked_total += acked
        logger.debug("Batch-acked %d messages individually", acked)
        return acked

    def flush(self) -> int:
        """Ack all buffered tags. Returns the number of tags acked."""
        with self._flush_lock:
            tags = self._take_snapshot()
            if not tags:
                return 0
            return self._emit(tags)

    def close(self) -> int:
        """Flush remaining tags, cancel the interval timer, and clean up. Idempotent."""
        with self._lock:
            self._closed = True
            timer, self._timer = self._timer, None
            task, self._flush_task = self._flush_task, None
        if timer is not None:
            timer.cancel()
        if task is not None:
            task.cancel()
        return self.flush()

    # ── Async API ────────────────────────────────────────────────────────

    async def _interval_loop_async(self) -> None:
        interval = self._config.flush_interval_ms / 1000.0
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.flush_async()
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    self._record_error(exc)
                    logger.error("BatchAcker async interval flush failed: %s", exc, exc_info=True)
        except asyncio.CancelledError:
            pass

    async def add_async(self, delivery_tag: int) -> None:
        """Async: add tag; auto-flush at batch_size."""
        should_flush = False
        async with self._ensure_async_lock():
            if self._closed:
                raise BatchClosedError("BatchAcker is closed; add_async() rejected")
            if delivery_tag not in self._seen:
                self._seen.add(delivery_tag)
                self._tags.append(delivery_tag)
            if len(self._tags) >= self._config.batch_size:
                should_flush = True
            elif self._config.flush_interval_ms > 0 and self._flush_task is None:
                self._flush_task = asyncio.create_task(self._interval_loop_async())
        if should_flush:
            await self.flush_async()

    async def _emit_async(self, tags: list[int]) -> int:
        if self._config.mode == "cumulative":
            max_tag = max(tags)
            result = self._ack_fn(max_tag, multiple=True)
            if hasattr(result, "__await__"):
                await result
            self.acked_total += len(tags)
            logger.debug("Async batch-acked %d messages cumulatively (max_tag=%d)", len(tags), max_tag)
            return len(tags)
        acked = 0
        ordered = sorted(tags)
        for i, tag in enumerate(ordered):
            try:
                result = self._ack_fn(tag, multiple=False)
                if hasattr(result, "__await__"):
                    await result
            except BaseException:
                remainder = ordered[i + 1 :]
                async with self._ensure_async_lock():
                    for t in remainder:
                        if t not in self._seen:
                            self._seen.add(t)
                            self._tags.append(t)
                raise
            acked += 1
        self.acked_total += acked
        logger.debug("Async batch-acked %d messages individually", acked)
        return acked

    async def flush_async(self) -> int:
        """Async: ack all buffered tags."""
        async with self._ensure_async_flush_lock():
            async with self._ensure_async_lock():
                tags = list(self._tags)
                self._tags.clear()
                self._seen.clear()
            if not tags:
                return 0
            return await self._emit_async(tags)

    async def close_async(self) -> int:
        """Async: cancel the interval loop, flush remaining, and clean up. Idempotent."""
        async with self._ensure_async_lock():
            self._closed = True
            task, self._flush_task = self._flush_task, None
            timer, self._timer = self._timer, None
        if task is not None:
            task.cancel()
        if timer is not None:
            timer.cancel()
        return await self.flush_async()


# ── CoalescingAcker ──────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class CoalescingFlushReport:
    reason: FlushReason
    commands: tuple[SettlementCommand, ...] = ()
    errors: tuple[tuple[SettlementCommand, BaseException], ...] = field(default_factory=tuple)

    @property
    def settled_tags(self) -> int:
        return sum(len(c.covers) for c, _ in self.ok_commands)

    @property
    def coalesced_tags(self) -> int:
        return sum(len(c.covers) for c, _ in self.ok_commands if c.multiple)

    @property
    def ok_commands(self) -> tuple[tuple[SettlementCommand, None], ...]:
        failed = {id(c) for c, _ in self.errors}
        return tuple((c, None) for c in self.commands if id(c) not in failed)


class CoalescingAcker:
    """Safe cumulative acking for ONE channel generation (plan §8.3).

    Every delivery on the channel must be :meth:`register`-ed before its
    handler runs, then reported via :meth:`complete` / :meth:`fail` /
    :meth:`retry_pending` / :meth:`release`. ``flush()`` asks the
    :class:`~rabbitkit.core.settlement.SettlementCoordinator` for the
    commands that are provably safe and emits them through the three
    callables — ``ack_fn(tag, multiple)``, ``nack_fn(tag, requeue)``,
    ``reject_fn(tag, requeue)`` — which must marshal onto the transport
    owner (see :class:`BatchAcker`'s notes on pika thread safety).

    ``on_reconnect()`` invalidates the ledger; old tags are never replayed.
    Sync-only by design: the emit callables are what cross into the
    transport, so an aio-pika user passes ``lambda t, m:
    loop.create_task(channel.basic_ack(t, multiple=m))``-style adapters.
    """

    def __init__(
        self,
        *,
        ack_fn: Callable[[int, bool], Any],
        nack_fn: Callable[[int, bool], Any],
        reject_fn: Callable[[int, bool], Any],
        config: BatchAckConfig | None = None,
        max_hold: int = 2,
        coalesce: bool = True,
        on_flush: Callable[[CoalescingFlushReport], None] | None = None,
    ) -> None:
        self._config = config or BatchAckConfig()
        self._ack_fn = ack_fn
        self._nack_fn = nack_fn
        self._reject_fn = reject_fn
        self._on_flush = on_flush
        self._coordinator = SettlementCoordinator(coalesce=coalesce, max_hold=max_hold)
        self._lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._closed = False
        self._approved_since_flush = 0
        self.last_error: BaseException | None = None
        self.settled_total = 0
        self.coalesced_total = 0

    @property
    def coordinator(self) -> SettlementCoordinator:
        return self._coordinator

    @property
    def pending(self) -> int:
        return self._coordinator.pending

    @property
    def closed(self) -> bool:
        return self._closed

    # ── ledger API ───────────────────────────────────────────────────────

    def register(self, delivery_tag: int) -> None:
        with self._lock:
            if self._closed:
                raise BatchClosedError("CoalescingAcker is closed; register() rejected")
        self._coordinator.register(delivery_tag)
        self._arm_timer()

    def complete(self, delivery_tag: int) -> None:
        self._coordinator.mark_success(delivery_tag)
        self._after_intent()

    def fail(self, delivery_tag: int, *, requeue: bool = True, reject: bool = False) -> None:
        if reject:
            self._coordinator.mark_reject(delivery_tag, requeue=requeue)
        else:
            self._coordinator.mark_nack(delivery_tag, requeue=requeue)
        self._after_intent()

    def retry_pending(self, delivery_tag: int) -> None:
        """The retry middleware owns this delivery now; it blocks coalescing
        above it until :meth:`release`."""
        self._coordinator.mark_retry_pending(delivery_tag)

    def release(self, delivery_tag: int) -> None:
        self._coordinator.release(delivery_tag)

    def _after_intent(self) -> None:
        with self._lock:
            self._approved_since_flush += 1
            should_flush = self._approved_since_flush >= self._config.batch_size
        if should_flush:
            self.flush(FlushReason.SIZE)

    # ── timer ────────────────────────────────────────────────────────────

    def _arm_timer(self) -> None:
        with self._lock:
            if self._timer is None and self._config.flush_interval_ms > 0 and not self._closed:
                self._timer = threading.Timer(self._config.flush_interval_ms / 1000.0, self._timer_callback)
                self._timer.daemon = True
                self._timer.start()

    def _timer_callback(self) -> None:
        with self._lock:
            self._timer = None
        try:
            self.flush(FlushReason.INTERVAL)
        except BaseException as exc:
            self.last_error = exc
            logger.error("CoalescingAcker interval flush failed: %s", exc, exc_info=True)
        if self._coordinator.pending:
            self._arm_timer()

    # ── flush / lifecycle ────────────────────────────────────────────────

    def flush(self, reason: FlushReason = FlushReason.MANUAL) -> CoalescingFlushReport:
        with self._flush_lock:
            with self._lock:
                self._approved_since_flush = 0
            commands = self._coordinator.drain_plan() if reason is FlushReason.CLOSE else self._coordinator.plan()
            results = apply_commands(commands, ack=self._ack_fn, nack=self._nack_fn, reject=self._reject_fn)
            errors = tuple((c, e) for c, e in results if e is not None)
            report = CoalescingFlushReport(reason=reason, commands=tuple(commands), errors=errors)
            self.settled_total += report.settled_tags
            self.coalesced_total += report.coalesced_tags
            if errors:
                self.last_error = errors[0][1]
            if self._on_flush is not None:
                try:
                    self._on_flush(report)
                except Exception:
                    logger.exception("CoalescingAcker on_flush callback raised")
            return report

    def on_reconnect(self) -> tuple[int, ...]:
        """Channel rebuilt: drop every pending tag (never replay them)."""
        dropped = self._coordinator.invalidate()
        if dropped:
            logger.warning("CoalescingAcker dropped %d pending tags on reconnect (broker will redeliver)", len(dropped))
        with self._lock:
            self._approved_since_flush = 0
        return dropped

    def close(self) -> CoalescingFlushReport:
        with self._lock:
            self._closed = True
            timer, self._timer = self._timer, None
        if timer is not None:
            timer.cancel()
        return self.flush(FlushReason.CLOSE)


__all__ = [
    "REASON_CANCELLED",
    "REASON_NOT_STARTED",
    "REASON_PUBLISH_ERROR",
    "BatchAcker",
    "BatchClosedError",
    "BatchFlushError",
    "BatchPublisher",
    "CoalescingAcker",
    "CoalescingFlushReport",
    "FlushItem",
    "FlushReason",
    "FlushReport",
    "SettlementAction",
]
