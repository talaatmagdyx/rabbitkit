"""Async-native safe cumulative acking — no timer thread, no ``marshal``.

Transport-free (hard invariant 1): the emit callables are plain awaitables,
so nothing here imports ``aio_pika``.

Why this exists rather than reusing :class:`~rabbitkit.highload.batch.CoalescingAcker`:

1. **The threading model leaked into application code.** The sync acker
   flushes from a ``threading.Timer`` thread, so an asyncio user had to pass
   ``marshal=loop.call_soon_threadsafe`` and make every emit callable
   thread-safe. Getting that wrong fails *silently*: asyncio only checks the
   calling thread when the loop is in debug mode, so a cross-thread
   ``create_task`` is queued without waking the loop. A busy loop happens to
   pick it up; an idle one never does, and at ``prefetch=1`` the consumer
   deadlocks waiting for the delivery its own un-emitted ack would release.

2. **A synchronous driver cannot implement the commit protocol.** Settlement
   on aio-pika is a coroutine. A sync callable can only *schedule* it and
   return, which tells you nothing about whether the frame reached the
   broker. The plan/emit/commit rule needs that answer before it can decide
   whether the next command is safe to send, so the driver has to be able to
   await. See :func:`~rabbitkit.core.settlement.emit_batch_async`.

Here the acker lives entirely on the owner loop: the interval flush is an
``asyncio.Task``, ``flush()`` is a coroutine, and an ``asyncio.Lock``
serialises flushes.

Example::

    acker = AsyncCoalescingAcker(
        ack_fn=lambda tag, multiple: channel.channel.basic_ack(tag, multiple),
        nack_fn=lambda tag, requeue: channel.channel.basic_nack(tag, requeue=requeue),
        reject_fn=lambda tag, requeue: channel.channel.basic_reject(tag, requeue=requeue),
        config=BatchAckConfig(batch_size=100, flush_interval_ms=200),
    )

    @broker.subscriber(queue="orders")
    async def handle(body: bytes, msg: RabbitMessage) -> None:
        acker.register(msg.delivery_tag, channel_key=msg.channel)
        try:
            await do_work(body)
        except Exception:
            acker.fail(msg.delivery_tag, requeue=True)
            raise
        acker.complete(msg.delivery_tag)

    await acker.start()   # arms the interval flush
    ...
    await acker.close()   # drains what is provably safe, leaves the rest
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.core.config import BatchAckConfig, MetricsConfig
from rabbitkit.core.settlement import (
    EmissionReport,
    SettlementCoordinator,
    emit_batch_async,
)
from rabbitkit.highload.batch import (
    ChannelMismatchError,
    CoalescingFlushReport,
    FlushReason,
)

logger = logging.getLogger(__name__)

AsyncSettleFn = Callable[[int, bool], Awaitable[Any]]


class AsyncCoalescingAcker:
    """Safe cumulative acking for ONE channel generation, on the event loop.

    Mirrors :class:`~rabbitkit.highload.batch.CoalescingAcker` — same ledger,
    same safety rules, same one-acker-per-channel requirement — but every
    entry point that touches the wire is a coroutine, so the whole
    plan/emit/commit cycle runs on the connection's own loop.

    Args:
        ack_fn: ``async (delivery_tag, multiple) -> None``.
        nack_fn: ``async (delivery_tag, requeue) -> None``.
        reject_fn: ``async (delivery_tag, requeue) -> None``.
        config: batch size and flush interval.
        channel_key: the channel these tags belong to. Delivery tags are a
            per-channel counter, so mixing channels would settle the wrong
            messages; a foreign tag raises :class:`ChannelMismatchError` at
            :meth:`register` time. An unbound acker binds to the first key
            it sees.
        max_pending: hard ceiling on the ledger; 0 means unbounded.
        on_flush: called with each :class:`CoalescingFlushReport`.
        collector / metrics_config: optional metrics wiring.
    """

    def __init__(
        self,
        *,
        ack_fn: AsyncSettleFn,
        nack_fn: AsyncSettleFn,
        reject_fn: AsyncSettleFn,
        config: BatchAckConfig | None = None,
        channel_key: object | None = None,
        max_pending: int = 0,
        on_flush: Callable[[CoalescingFlushReport], None] | None = None,
        collector: Any | None = None,
        metrics_config: MetricsConfig | None = None,
    ) -> None:
        self._ack_fn = ack_fn
        self._nack_fn = nack_fn
        self._reject_fn = reject_fn
        self._config = config or BatchAckConfig()
        self._channel_key = channel_key
        self._on_flush = on_flush
        self._collector = collector
        self._metrics_config = metrics_config
        self._coordinator = SettlementCoordinator(
            coalesce=True,
            max_hold=self._config.batch_size,
            max_pending=max_pending,
        )
        self._flush_lock = asyncio.Lock()
        self._timer_task: asyncio.Task[None] | None = None
        self._closed = False
        self._approved_since_flush = 0
        self.settled_total = 0
        self.coalesced_total = 0
        self.unresolved_total = 0
        self.last_error: BaseException | None = None

    # ── inspection ────────────────────────────────────────────────────────

    @property
    def pending(self) -> int:
        return self._coordinator.pending

    @property
    def generation(self) -> int:
        return self._coordinator.generation

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def channel_key(self) -> object | None:
        return self._channel_key

    def metrics(self) -> dict[str, float]:
        stats = dict(self._coordinator.stats)
        stats["unresolved_total"] = float(self.unresolved_total)
        return stats

    # ── ledger ────────────────────────────────────────────────────────────

    def register(self, delivery_tag: int, *, channel_key: object | None = None) -> None:
        """Record a delivery before its handler runs."""
        if channel_key is not None:
            if self._channel_key is None:
                self._channel_key = channel_key
            elif channel_key is not self._channel_key:
                raise ChannelMismatchError(
                    f"delivery_tag={delivery_tag} belongs to a different channel; "
                    "use one AsyncCoalescingAcker per channel"
                )
        self._coordinator.register(delivery_tag)

    def complete(self, delivery_tag: int) -> None:
        """The handler succeeded: the delivery is approved for an ack."""
        self._coordinator.mark_success(delivery_tag)
        self._approved_since_flush += 1

    def fail(self, delivery_tag: int, *, requeue: bool = True, reject: bool = False) -> None:
        """The handler failed: nack (or reject) this delivery."""
        if reject:
            self._coordinator.mark_reject(delivery_tag, requeue=requeue)
        else:
            self._coordinator.mark_nack(delivery_tag, requeue=requeue)
        self._approved_since_flush += 1

    def retry_pending(self, delivery_tag: int) -> None:
        """Handed to the retry machinery: blocks coalescing until resolved."""
        self._coordinator.mark_retry_pending(delivery_tag)

    def release(self, delivery_tag: int) -> None:
        """Settled elsewhere: drop it without emitting anything."""
        self._coordinator.release(delivery_tag)

    # ── flushing ──────────────────────────────────────────────────────────

    async def maybe_flush(self) -> CoalescingFlushReport | None:
        """Flush once ``batch_size`` deliveries have been approved."""
        if self._approved_since_flush >= self._config.batch_size:
            return await self.flush(FlushReason.SIZE)
        return None

    async def flush(self, reason: FlushReason = FlushReason.MANUAL) -> CoalescingFlushReport:
        """Emit every command that is provably safe, stopping at the first
        failure. Settles the ledger only for frames the broker accepted."""
        async with self._flush_lock:
            self._approved_since_flush = 0
            batch = (
                self._coordinator.prepare_drain()
                if reason is FlushReason.CLOSE
                else self._coordinator.prepare()
            )
            emission = await emit_batch_async(
                batch,
                self._coordinator,
                ack=self._ack_fn,
                nack=self._nack_fn,
                reject=self._reject_fn,
            )
            return self._finish(reason, batch.commands, batch.generation, emission)

    def _finish(
        self,
        reason: FlushReason,
        planned: tuple[Any, ...],
        generation: int,
        emission: EmissionReport,
    ) -> CoalescingFlushReport:
        errors = ((emission.failed, emission.error),) if emission.failed is not None and emission.error else ()
        report = CoalescingFlushReport(
            reason=reason,
            commands=planned,
            errors=errors,
            emitted=emission.emitted,
            not_attempted=emission.not_attempted,
            invalidated=emission.invalidated,
        )
        self.settled_total += report.settled_tags
        self.coalesced_total += report.coalesced_tags
        self.unresolved_total += len(emission.unresolved_tags)
        if emission.error is not None:
            self.last_error = emission.error
            assert emission.failed is not None
            logger.error(
                "AsyncCoalescingAcker failed to emit %s(delivery_tag=%d, multiple=%s) "
                "covering %d tag(s) on generation %d: %s",
                emission.failed.kind.value,
                emission.failed.delivery_tag,
                emission.failed.multiple,
                len(emission.failed.covers),
                emission.failed.generation,
                emission.error,
                exc_info=emission.error,
            )
        if emission.not_attempted:
            logger.warning(
                "AsyncCoalescingAcker withheld %d command(s) covering %d tag(s) after that failure. "
                "The broker will redeliver anything left unacknowledged.",
                len(emission.not_attempted),
                sum(len(c.covers) for c in emission.not_attempted),
            )
        if emission.invalidated:
            logger.warning(
                "AsyncCoalescingAcker invalidated generation %d: a failed nack/reject may still be "
                "unacknowledged, so no later cumulative ack on this channel is safe.",
                generation,
            )
        self._emit_metrics(report)
        if self._on_flush is not None:
            try:
                self._on_flush(report)
            except Exception:
                logger.exception("AsyncCoalescingAcker on_flush callback raised")
        return report

    def _emit_metrics(self, report: CoalescingFlushReport) -> None:
        if self._collector is None or self._metrics_config is None:
            return
        inc = getattr(self._collector, "inc_counter", None)
        set_gauge = getattr(self._collector, "set_gauge", None)
        if inc is not None and report.coalesced_tags:
            inc(self._metrics_config.settlement_coalesced_total, {}, float(report.coalesced_tags))
        if set_gauge is not None:
            stats = self._coordinator.stats
            set_gauge(self._metrics_config.settlement_pending, {}, stats["pending"])
            set_gauge(self._metrics_config.settlement_ack_ready, {}, stats["ack_ready"])
            set_gauge(self._metrics_config.settlement_frontier, {}, stats["frontier"])

    # ── lifecycle ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Arm the interval flush. No-op when ``flush_interval_ms`` is 0.

        The timer is an ``asyncio.Task`` on the caller's loop, so there is no
        thread boundary to marshal across and no ``marshal=`` parameter.
        """
        if self._config.flush_interval_ms <= 0 or self._timer_task is not None:
            return
        self._timer_task = asyncio.create_task(self._interval_loop())

    async def _interval_loop(self) -> None:
        interval = self._config.flush_interval_ms / 1000.0
        try:
            while not self._closed:
                await asyncio.sleep(interval)
                if self._closed:
                    return
                try:
                    await self.flush(FlushReason.INTERVAL)
                except Exception as exc:  # never let the timer task die
                    self.last_error = exc
                    logger.error("AsyncCoalescingAcker interval flush failed: %s", exc, exc_info=True)
        except asyncio.CancelledError:
            raise

    def on_reconnect(self) -> tuple[int, ...]:
        """Channel rebuilt: drop every pending tag (never replay them)."""
        dropped = self._coordinator.invalidate()
        if dropped:
            logger.warning(
                "AsyncCoalescingAcker dropped %d pending tags on reconnect (broker will redeliver)",
                len(dropped),
            )
        self._approved_since_flush = 0
        return dropped

    async def close(self) -> CoalescingFlushReport:
        """Stop the timer and drain what is provably safe.

        Blocking deliveries are deliberately left unacknowledged so the broker
        redelivers them. Closing never acks merely to empty the ledger.
        """
        self._closed = True
        task, self._timer_task = self._timer_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        return await self.flush(FlushReason.CLOSE)

    async def __aenter__(self) -> AsyncCoalescingAcker:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
