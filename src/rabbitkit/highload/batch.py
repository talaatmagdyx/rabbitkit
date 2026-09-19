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
every lower outstanding tag is approved. ONE acker per channel — pass
``channel_key=`` so that is enforced rather than merely intended.

``CoalescingAckerGroup`` owns one acker per channel for you (rabbitkit gives
each subscriber queue its own channel), so a multi-queue consumer keeps the
ledgers isolated without hand-rolling the bookkeeping::

    Channel A → CoalescingAcker A → SettlementCoordinator A
    Channel B → CoalescingAcker B → SettlementCoordinator B

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


class ChannelMismatchError(RuntimeError):
    """A delivery from a different channel was handed to a bound
    :class:`CoalescingAcker`.

    Delivery tags are a PER-CHANNEL counter: tag 7 on channel A and tag 7 on
    channel B are different messages. Mixing two channels in one ledger makes
    a cumulative ack computed from channel A's prefix settle channel B's
    messages — silent, wrong, and unrecoverable. Bind the acker (constructor
    ``channel_key=`` or the first ``register(..., channel_key=...)``) and this
    is caught at registration instead.
    """


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

    **One acker per channel.** The emit callables are bound to one channel
    and delivery tags are a per-channel counter, so feeding two channels'
    tags into one acker would ack the wrong messages. Pass ``channel_key``
    (the channel object itself is the natural key — all pika/aio-pika
    channels are identity-hashable) and that convention becomes an enforced
    invariant: a delivery from any other channel raises
    :class:`ChannelMismatchError` at :meth:`register` time, before it can
    corrupt the ledger. An unbound acker binds to the first ``channel_key``
    it is given. :class:`CoalescingAckerGroup` does this for you.

    ``on_reconnect()`` invalidates the ledger; old tags are never replayed.

    **The emit callables must be thread-safe.** ``flush_interval_ms`` fires
    them from a background ``threading.Timer`` thread, NOT the transport
    owner — the same hazard :class:`BatchAcker` documents. A bare
    ``loop.create_task(...)`` is silently never scheduled from that thread,
    so acks simply stop (most visible at ``prefetch=1``, where the broker
    then waits forever for an ack that never leaves). Marshal explicitly::

        # aio-pika / asyncio
        def emit(coro):
            loop.call_soon_threadsafe(lambda: loop.create_task(coro))

        acker = CoalescingAcker(
            ack_fn=lambda t, m: emit(channel.basic_ack(delivery_tag=t, multiple=m)),
            ...
        )

        # pika
        def safe_ack(tag, multiple):
            connection.add_callback_threadsafe(
                lambda: channel.basic_ack(delivery_tag=tag, multiple=multiple)
            )

    Set ``flush_interval_ms=0`` and flush yourself if you would rather not
    deal with the timer thread at all.
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
        channel_key: Any = None,
        max_pending: int = 0,
        collector: Any = None,
        metrics_config: Any = None,
        on_flush: Callable[[CoalescingFlushReport], None] | None = None,
    ) -> None:
        self._config = config or BatchAckConfig()
        self._channel_key = channel_key
        self._ack_fn = ack_fn
        self._nack_fn = nack_fn
        self._reject_fn = reject_fn
        self._on_flush = on_flush
        self._collector = collector
        self._metrics_config = metrics_config
        self._coordinator = SettlementCoordinator(coalesce=coalesce, max_hold=max_hold, max_pending=max_pending)
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
    def channel_key(self) -> Any:
        """The channel this acker is bound to, or ``None`` while unbound."""
        return self._channel_key

    @property
    def metrics(self) -> dict[str, float]:
        """Coordinator instrumentation for this channel: ``registered``,
        ``pending``, ``outstanding``, ``ack_ready``, ``frontier``,
        ``settled``, ``coalesced``, ``frames_sent``, ``coalescing_ratio``,
        ``gap_count``, ``oldest_pending_age``, ``invalidations``,
        ``dropped_on_invalidate``."""
        stats = self._coordinator.stats
        stats["gap_count"] = self._coordinator.gap_count
        return stats

    def _emit_metrics(self) -> None:
        """Push :attr:`metrics` into a ``MetricsCollector`` when one was
        supplied. Labels are bounded (none) — this is per-channel state, so
        wire one collector per process, not per delivery."""
        if self._collector is None or self._metrics_config is None:
            return
        cfg, stats = self._metrics_config, self.metrics
        set_gauge = getattr(self._collector, "set_gauge", None)
        if set_gauge is None:
            return
        for name, key in (
            (cfg.settlement_pending, "pending"),
            (cfg.settlement_ack_ready, "ack_ready"),
            (cfg.settlement_frontier, "frontier"),
            (cfg.settlement_gap_count, "gap_count"),
            (cfg.settlement_oldest_pending_age_seconds, "oldest_pending_age"),
            (cfg.settlement_coalescing_ratio, "coalescing_ratio"),
        ):
            set_gauge(name, {}, float(stats[key]))

    @property
    def pending(self) -> int:
        return self._coordinator.pending

    @property
    def closed(self) -> bool:
        return self._closed

    # ── ledger API ───────────────────────────────────────────────────────

    def register(self, delivery_tag: int, *, channel_key: Any = None) -> None:
        """Register a delivery BEFORE its handler runs.

        Pass ``channel_key`` (normally the channel object the delivery
        arrived on) to have the one-acker-per-channel rule enforced: an
        unbound acker binds to it, a bound acker raises
        :class:`ChannelMismatchError` on anything else. Omitting it keeps the
        legacy, unchecked behavior.
        """
        with self._lock:
            if self._closed:
                raise BatchClosedError("CoalescingAcker is closed; register() rejected")
            if channel_key is not None:
                bound = self._channel_key
                if bound is None:
                    self._channel_key = channel_key  # first registration binds
                elif bound is not channel_key and bound != channel_key:
                    raise ChannelMismatchError(
                        f"delivery_tag {delivery_tag} arrived on {channel_key!r} but this "
                        f"CoalescingAcker is bound to {bound!r}. Delivery tags are a "
                        "per-channel counter — use one acker per channel "
                        "(CoalescingAckerGroup does this for you)."
                    )
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

    def abandon(self, delivery_tag: int) -> None:
        """The handler failed with no settlement decision (``FAILED``).

        Blocks the frontier and is NEVER acked — the delivery stays unacked
        so the broker redelivers it when the channel closes. Use this instead
        of leaving the tag ``OUTSTANDING`` so the ledger records that nobody
        is still working on it.
        """
        self._coordinator.mark_failed(delivery_tag)

    def cancel(self, delivery_tag: int) -> None:
        """The handler was cancelled (shutdown, timeout) — ``CANCELLED``.

        Same treatment as :meth:`abandon`: blocks the frontier, never acked.
        """
        self._coordinator.mark_cancelled(delivery_tag)

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
            if self._collector is not None:
                inc = getattr(self._collector, "inc_counter", None)
                if inc is not None and self._metrics_config is not None and report.coalesced_tags:
                    inc(self._metrics_config.settlement_coalesced_total, {}, float(report.coalesced_tags))
                self._emit_metrics()
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


# ── CoalescingAckerGroup ─────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class GroupFlushReport:
    """Aggregate of one :meth:`CoalescingAckerGroup.flush` across channels."""

    reason: FlushReason
    reports: tuple[CoalescingFlushReport, ...] = ()

    @property
    def channels(self) -> int:
        return len(self.reports)

    @property
    def settled_tags(self) -> int:
        return sum(r.settled_tags for r in self.reports)

    @property
    def coalesced_tags(self) -> int:
        return sum(r.coalesced_tags for r in self.reports)

    @property
    def errors(self) -> tuple[tuple[SettlementCommand, BaseException], ...]:
        return tuple(err for r in self.reports for err in r.errors)


class CoalescingAckerGroup:
    """One :class:`CoalescingAcker` per channel, created on demand.

    rabbitkit gives every subscriber queue its own channel, and delivery tags
    are a PER-CHANNEL counter — so a consumer with several queues needs
    several ledgers::

        Channel A → CoalescingAcker A → SettlementCoordinator A
        Channel B → CoalescingAcker B → SettlementCoordinator B

    This group keeps that isolation for you: ``factory(channel)`` builds a
    fully wired acker (its ``ack_fn``/``nack_fn``/``reject_fn`` bound to THAT
    channel — only you know how to reach your transport), the group caches it
    per channel, and every per-delivery call carries the channel so a
    cross-channel mistake raises :class:`ChannelMismatchError` instead of
    acking the wrong messages.

    Channels are used as dict keys (all pika/aio-pika channel objects are
    identity-hashable). The group holds a strong reference to each channel it
    settles on, so call :meth:`on_reconnect` when one is rebuilt (or
    :meth:`reset` when the whole connection is) to drop it.

    Usage::

        def build(channel: Any) -> CoalescingAcker:
            # `emit` MUST be thread-safe — the flush timer runs off-loop.
            # See CoalescingAcker's docstring.
            return CoalescingAcker(
                ack_fn=lambda t, m: emit(channel.basic_ack(t, multiple=m)),
                nack_fn=lambda t, r: emit(channel.basic_nack(t, requeue=r)),
                reject_fn=lambda t, r: emit(channel.basic_reject(t, requeue=r)),
                config=BatchAckConfig(batch_size=50, flush_interval_ms=200),
                channel_key=channel,
            )

        group = CoalescingAckerGroup(factory=build)

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        async def handle(body: bytes, msg: RabbitMessage) -> None:
            channel = msg.raw_message.channel
            group.register(channel, msg.delivery_tag)
            ...
            group.complete(channel, msg.delivery_tag)
    """

    def __init__(self, factory: Callable[[Any], CoalescingAcker]) -> None:
        self._factory = factory
        self._ackers: dict[Any, CoalescingAcker] = {}
        self._lock = threading.Lock()
        self._closed = False
        # Totals survive a channel being retired on reconnect/close, so a
        # reconnect does not silently reset the metrics.
        self._retired_settled = 0
        self._retired_coalesced = 0

    # ── inspection ───────────────────────────────────────────────────────

    @property
    def channels(self) -> int:
        """Number of channels with a live acker."""
        with self._lock:
            return len(self._ackers)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def pending(self) -> int:
        """Registered-but-unsettled deliveries across every channel."""
        return sum(a.pending for a in self._snapshot())

    @property
    def settled_total(self) -> int:
        return self._retired_settled + sum(a.settled_total for a in self._snapshot())

    @property
    def coalesced_total(self) -> int:
        return self._retired_coalesced + sum(a.coalesced_total for a in self._snapshot())

    @property
    def last_error(self) -> BaseException | None:
        for acker in self._snapshot():
            if acker.last_error is not None:
                return acker.last_error
        return None

    @property
    def metrics(self) -> dict[str, float]:
        """Per-channel metrics summed across the group (``coalescing_ratio``
        is recomputed from the totals, not averaged)."""
        total: dict[str, float] = {}
        for acker in self._snapshot():
            for key, value in acker.metrics.items():
                if key == "coalescing_ratio":
                    continue
                total[key] = total.get(key, 0.0) + value
        total["channels"] = float(self.channels)
        total["settled"] = float(self.settled_total)
        total["coalesced"] = float(self.coalesced_total)
        frames = total.get("frames_sent", 0.0)
        total["coalescing_ratio"] = (total["settled"] / frames) if frames else 0.0
        return total

    def _snapshot(self) -> tuple[CoalescingAcker, ...]:
        with self._lock:
            return tuple(self._ackers.values())

    def _retire(self, acker: CoalescingAcker) -> None:
        self._retired_settled += acker.settled_total
        self._retired_coalesced += acker.coalesced_total

    # ── per-channel access ───────────────────────────────────────────────

    def for_channel(self, channel: Any) -> CoalescingAcker:
        """Return (creating on first use) the acker that owns *channel*."""
        with self._lock:
            if self._closed:
                raise BatchClosedError("CoalescingAckerGroup is closed; for_channel() rejected")
            acker = self._ackers.get(channel)
            if acker is not None:
                return acker
        # Build outside the lock: the factory touches the transport and must
        # not serialize every other channel behind it.
        acker = self._factory(channel)
        with self._lock:
            existing = self._ackers.get(channel)
            if existing is not None:  # another thread won the race
                return existing
            self._ackers[channel] = acker
            return acker

    # ── per-delivery API (channel-checked) ───────────────────────────────

    def register(self, channel: Any, delivery_tag: int) -> None:
        """Register a delivery BEFORE its handler runs, on *channel*'s ledger."""
        self.for_channel(channel).register(delivery_tag, channel_key=channel)

    def complete(self, channel: Any, delivery_tag: int) -> None:
        self.for_channel(channel).complete(delivery_tag)

    def fail(self, channel: Any, delivery_tag: int, *, requeue: bool = True, reject: bool = False) -> None:
        self.for_channel(channel).fail(delivery_tag, requeue=requeue, reject=reject)

    def retry_pending(self, channel: Any, delivery_tag: int) -> None:
        self.for_channel(channel).retry_pending(delivery_tag)

    def abandon(self, channel: Any, delivery_tag: int) -> None:
        """Handler failed with no settlement decision — never acked."""
        self.for_channel(channel).abandon(delivery_tag)

    def cancel(self, channel: Any, delivery_tag: int) -> None:
        """Handler was cancelled — never acked."""
        self.for_channel(channel).cancel(delivery_tag)

    def release(self, channel: Any, delivery_tag: int) -> None:
        self.for_channel(channel).release(delivery_tag)

    # ── lifecycle ────────────────────────────────────────────────────────

    def flush(self, reason: FlushReason = FlushReason.MANUAL) -> GroupFlushReport:
        """Flush every channel's acker; never let one channel's failure skip
        the others (each acker records its own errors in its report)."""
        return GroupFlushReport(reason=reason, reports=tuple(a.flush(reason) for a in self._snapshot()))

    def on_reconnect(self, channel: Any) -> tuple[int, ...]:
        """One channel was rebuilt: drop its ledger and retire its acker.

        Returns the dropped delivery tags (the broker redelivers them). The
        replacement channel is a different object, so the next
        :meth:`for_channel` builds a fresh acker for it. Unknown channels are
        a no-op, so this is safe to call from a reconnect hook that does not
        track which channels were in use.
        """
        with self._lock:
            acker = self._ackers.pop(channel, None)
        if acker is None:
            return ()
        dropped = acker.on_reconnect()
        self._retire(acker)
        return dropped

    def reset(self) -> int:
        """The whole connection was rebuilt: drop every ledger. Returns the
        total number of dropped tags."""
        with self._lock:
            ackers = tuple(self._ackers.values())
            self._ackers.clear()
        dropped = 0
        for acker in ackers:
            dropped += len(acker.on_reconnect())
            self._retire(acker)
        return dropped

    def close(self) -> GroupFlushReport:
        """Close every acker (draining what is approved) and clear the group."""
        with self._lock:
            self._closed = True
            ackers = tuple(self._ackers.values())
            self._ackers.clear()
        reports = []
        for acker in ackers:
            reports.append(acker.close())
            self._retire(acker)
        return GroupFlushReport(reason=FlushReason.CLOSE, reports=tuple(reports))


__all__ = [
    "REASON_CANCELLED",
    "REASON_NOT_STARTED",
    "REASON_PUBLISH_ERROR",
    "BatchAcker",
    "BatchClosedError",
    "BatchFlushError",
    "BatchPublisher",
    "ChannelMismatchError",
    "CoalescingAcker",
    "CoalescingAckerGroup",
    "CoalescingFlushReport",
    "FlushItem",
    "FlushReason",
    "FlushReport",
    "GroupFlushReport",
    "SettlementAction",
]
