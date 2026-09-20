"""AsyncCoalescingAcker — the commit protocol on the event loop.

The sync acker cannot implement plan/emit/commit against aio-pika, because
settlement there is a coroutine: a synchronous emit callable can only
schedule it and return, so the driver never learns whether the frame landed
and cannot decide whether the next command is safe. These tests pin the
behaviour that awaiting buys.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from rabbitkit.core.config import BatchAckConfig
from rabbitkit.highload import AsyncCoalescingAcker
from rabbitkit.highload.batch import ChannelMismatchError, FlushReason


class AsyncWire:
    """Records awaited settlement frames; can fail a chosen one."""

    def __init__(self, fail_at: int | None = None, exc: BaseException | None = None) -> None:
        self.calls: list[tuple[str, int, bool]] = []
        self._fail_at = fail_at
        self._exc = exc or RuntimeError("wire failure")
        self._n = 0

    async def _record(self, kind: str, tag: int, flag: bool) -> None:
        await asyncio.sleep(0)  # a real settlement yields
        self.calls.append((kind, tag, flag))
        should_fail = self._fail_at is not None and self._n == self._fail_at
        self._n += 1
        if should_fail:
            raise self._exc

    async def ack(self, tag: int, multiple: bool) -> None:
        await self._record("ack", tag, multiple)

    async def nack(self, tag: int, requeue: bool) -> None:
        await self._record("nack", tag, requeue)

    async def reject(self, tag: int, requeue: bool) -> None:
        await self._record("reject", tag, requeue)


def _acker(wire: AsyncWire, **kw: Any) -> AsyncCoalescingAcker:
    kw.setdefault("config", BatchAckConfig(batch_size=1000, flush_interval_ms=0))
    return AsyncCoalescingAcker(ack_fn=wire.ack, nack_fn=wire.nack, reject_fn=wire.reject, **kw)


async def _load_nack_in_the_middle(acker: AsyncCoalescingAcker) -> None:
    for tag in (101, 102, 103, 104, 105):
        acker.register(tag)
    acker.complete(101)
    acker.complete(102)
    acker.fail(103, requeue=True)
    acker.complete(104)
    acker.complete(105)


class TestTheOverAckRegressionOnAsync:
    async def test_a_cumulative_ack_never_follows_a_failed_nack(self) -> None:
        wire = AsyncWire(fail_at=1)
        acker = _acker(wire)
        await _load_nack_in_the_middle(acker)

        report = await acker.flush()

        assert wire.calls == [("ack", 102, True), ("nack", 103, True)]
        assert ("ack", 105, True) not in wire.calls
        assert len(report.not_attempted) == 1
        assert report.settled_tags == 2

    async def test_a_clean_flush_emits_all_three_in_order(self) -> None:
        wire = AsyncWire()
        acker = _acker(wire)
        await _load_nack_in_the_middle(acker)

        report = await acker.flush()

        assert wire.calls == [("ack", 102, True), ("nack", 103, True), ("ack", 105, True)]
        assert report.settled_tags == 5
        assert acker.pending == 0

    async def test_a_failed_nack_invalidates_the_generation(self) -> None:
        wire = AsyncWire(fail_at=1)
        acker = _acker(wire)
        gen = acker.generation
        await _load_nack_in_the_middle(acker)

        report = await acker.flush()

        assert report.invalidated is True
        assert acker.generation == gen + 1
        assert acker.pending == 0

    async def test_a_failed_ack_leaves_the_generation_alone(self) -> None:
        wire = AsyncWire(fail_at=0)
        acker = _acker(wire)
        for tag in (1, 2, 3):
            acker.register(tag)
            acker.complete(tag)
        gen = acker.generation

        report = await acker.flush()

        assert report.invalidated is False
        assert acker.generation == gen


class TestNoThreadBoundary:
    async def test_the_interval_flush_runs_on_the_caller_loop(self) -> None:
        """The whole point: no timer thread, so no marshal and no silent
        cross-thread scheduling."""
        loop = asyncio.get_running_loop()
        seen: list[object] = []

        async def ack(tag: int, multiple: bool) -> None:
            seen.append(asyncio.get_running_loop())

        acker = AsyncCoalescingAcker(
            ack_fn=ack,
            nack_fn=ack,
            reject_fn=ack,
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=20),
        )
        await acker.start()
        acker.register(1)
        acker.complete(1)
        await asyncio.sleep(0.08)
        await acker.close()

        assert seen, "the interval flush must have fired"
        assert all(x is loop for x in seen), "every emit ran on the owner loop"

    async def test_no_marshal_parameter_exists(self) -> None:
        """A regression guard: reintroducing marshal= would mean the timer
        went back to a thread."""
        import inspect

        params = inspect.signature(AsyncCoalescingAcker.__init__).parameters
        assert "marshal" not in params

    async def test_start_is_idempotent(self) -> None:
        acker = _acker(AsyncWire(), config=BatchAckConfig(batch_size=10, flush_interval_ms=50))
        await acker.start()
        await acker.start()
        await acker.close()

    async def test_start_is_a_no_op_without_an_interval(self) -> None:
        acker = _acker(AsyncWire())
        await acker.start()
        await acker.close()


class TestChannelIsolation:
    async def test_a_foreign_delivery_tag_is_rejected(self) -> None:
        acker = _acker(AsyncWire())
        chan_a, chan_b = object(), object()
        acker.register(1, channel_key=chan_a)
        with pytest.raises(ChannelMismatchError):
            acker.register(2, channel_key=chan_b)

    async def test_an_unbound_acker_binds_to_the_first_channel(self) -> None:
        acker = _acker(AsyncWire())
        chan = object()
        assert acker.channel_key is None
        acker.register(1, channel_key=chan)
        assert acker.channel_key is chan

    async def test_reconnect_drops_everything_pending(self) -> None:
        acker = _acker(AsyncWire())
        for tag in (1, 2, 3):
            acker.register(tag)
        acker.complete(1)
        dropped = acker.on_reconnect()
        assert set(dropped) == {1, 2, 3}
        assert acker.pending == 0


class TestCloseSemantics:
    async def test_close_drains_approved_work_individually(self) -> None:
        wire = AsyncWire()
        acker = _acker(wire)
        for tag in (1, 2, 3):
            acker.register(tag)
            acker.complete(tag)

        report = await acker.close()

        assert report.reason is FlushReason.CLOSE
        assert all(not c.multiple for c, _ in report.ok_commands)
        assert acker.closed

    async def test_close_never_acks_a_blocking_delivery(self) -> None:
        wire = AsyncWire()
        acker = _acker(wire)
        acker.register(1)
        acker.register(2)
        acker.complete(1)
        # tag 2 is still outstanding — it must be left for redelivery.
        await acker.close()

        assert ("ack", 2, False) not in wire.calls
        assert ("ack", 2, True) not in wire.calls

    async def test_the_context_manager_starts_and_closes(self) -> None:
        wire = AsyncWire()
        async with _acker(wire) as acker:
            acker.register(1)
            acker.complete(1)
        assert acker.closed
        assert ("ack", 1, False) in wire.calls


class TestObservability:
    async def test_a_failure_is_logged_once_with_the_withheld_count(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        wire = AsyncWire(fail_at=1)
        acker = _acker(wire)
        await _load_nack_in_the_middle(acker)

        with caplog.at_level(logging.WARNING):
            await acker.flush()

        messages = [r.getMessage() for r in caplog.records]
        assert any("failed to emit nack" in m for m in messages), messages
        assert any("withheld 1 command" in m for m in messages), messages
        assert any("invalidated generation" in m for m in messages), messages

    async def test_unresolved_tags_are_counted_not_settled(self) -> None:
        wire = AsyncWire(fail_at=1)
        acker = _acker(wire)
        await _load_nack_in_the_middle(acker)

        await acker.flush()

        assert acker.unresolved_total == 1
        assert acker.settled_total == 2, "only the frames that landed count"

    async def test_metrics_expose_the_new_counters(self) -> None:
        acker = _acker(AsyncWire())
        acker.register(1)
        acker.complete(1)
        await acker.flush()
        m = acker.metrics()
        assert "unresolved_total" in m
        assert "frames_failed" in m

    async def test_on_flush_receives_the_report(self) -> None:
        seen: list[Any] = []
        wire = AsyncWire()
        acker = _acker(wire, on_flush=seen.append)
        acker.register(1)
        acker.complete(1)
        await acker.flush()
        assert len(seen) == 1
        assert seen[0].settled_tags == 1

    async def test_a_raising_on_flush_never_breaks_the_flush(self) -> None:
        def boom(_report: Any) -> None:
            raise RuntimeError("callback exploded")

        wire = AsyncWire()
        acker = _acker(wire, on_flush=boom)
        acker.register(1)
        acker.complete(1)
        report = await acker.flush()  # must not raise
        assert report.settled_tags == 1


class TestSizeTrigger:
    async def test_maybe_flush_waits_for_the_batch_size(self) -> None:
        wire = AsyncWire()
        acker = _acker(wire, config=BatchAckConfig(batch_size=3, flush_interval_ms=0))
        for tag in (1, 2):
            acker.register(tag)
            acker.complete(tag)
        assert await acker.maybe_flush() is None
        assert wire.calls == []

        acker.register(3)
        acker.complete(3)
        report = await acker.maybe_flush()
        assert report is not None
        assert report.reason is FlushReason.SIZE
        assert report.settled_tags == 3


class TestCancellation:
    async def test_cancellation_mid_flush_propagates_and_settles_nothing_extra(self) -> None:
        """A cancelled settlement has an UNKNOWN outcome. It must not be
        recorded as settled, and it must not be swallowed."""

        async def hang(_tag: int, _flag: bool) -> None:
            await asyncio.sleep(3600)

        acker = AsyncCoalescingAcker(
            ack_fn=hang,
            nack_fn=hang,
            reject_fn=hang,
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=0),
        )
        acker.register(1)
        acker.complete(1)

        task = asyncio.create_task(acker.flush())
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert acker.settled_total == 0


class TestLedgerEdges:
    async def test_fail_with_reject_emits_a_reject_frame(self) -> None:
        wire = AsyncWire()
        acker = _acker(wire)
        acker.register(1)
        acker.fail(1, requeue=False, reject=True)
        await acker.flush()
        assert wire.calls == [("reject", 1, False)]

    async def test_retry_pending_blocks_coalescing(self) -> None:
        """A delivery handed to the retry machinery must pin everything above
        it: acking past an unresolved retry would settle work still in play."""
        wire = AsyncWire()
        acker = _acker(wire)
        for tag in (1, 2, 3):
            acker.register(tag)
        acker.complete(1)
        acker.retry_pending(2)
        acker.complete(3)

        await acker.flush()

        assert ("ack", 3, True) not in wire.calls
        assert ("ack", 2, True) not in wire.calls

    async def test_release_drops_a_tag_without_emitting(self) -> None:
        wire = AsyncWire()
        acker = _acker(wire)
        acker.register(1)
        acker.release(1)
        await acker.flush()
        assert wire.calls == []
        assert acker.pending == 0


class TestMetricsCollector:
    async def test_gauges_and_counters_are_emitted(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        counters: list[tuple[str, float]] = []
        gauges: list[tuple[str, float]] = []

        class Collector:
            def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
                counters.append((name, value))

            def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
                gauges.append((name, value))

        cfg = MetricsConfig()
        acker = _acker(AsyncWire(), collector=Collector(), metrics_config=cfg)
        for tag in (1, 2, 3):
            acker.register(tag)
            acker.complete(tag)

        await acker.flush()

        assert any(n == cfg.settlement_coalesced_total for n, _ in counters)
        emitted = {n for n, _ in gauges}
        assert cfg.settlement_pending in emitted
        assert cfg.settlement_ack_ready in emitted
        assert cfg.settlement_frontier in emitted

    async def test_a_collector_without_the_methods_is_tolerated(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        acker = _acker(AsyncWire(), collector=object(), metrics_config=MetricsConfig())
        acker.register(1)
        acker.complete(1)
        await acker.flush()  # must not raise

    async def test_no_metrics_config_means_no_emission(self) -> None:
        acker = _acker(AsyncWire(), collector=object())
        acker.register(1)
        acker.complete(1)
        await acker.flush()  # must not raise


class TestIntervalLoopResilience:
    async def test_a_failing_interval_flush_keeps_the_timer_alive(self) -> None:
        """One bad flush must not kill the loop — otherwise acking silently
        stops forever and the consumer stalls at its prefetch ceiling."""
        calls: list[int] = []

        async def flaky(tag: int, _flag: bool) -> None:
            calls.append(tag)
            raise RuntimeError("transient")

        acker = AsyncCoalescingAcker(
            ack_fn=flaky,
            nack_fn=flaky,
            reject_fn=flaky,
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=15),
        )
        await acker.start()
        acker.register(1)
        acker.complete(1)
        await asyncio.sleep(0.05)
        acker.register(2)
        acker.complete(2)
        await asyncio.sleep(0.05)
        await acker.close()

        assert len(calls) >= 2, "the timer must have fired again after the failure"
        assert isinstance(acker.last_error, RuntimeError)

    async def test_closing_stops_the_interval_loop(self) -> None:
        wire = AsyncWire()
        acker = _acker(wire, config=BatchAckConfig(batch_size=1000, flush_interval_ms=15))
        await acker.start()
        await acker.close()
        before = len(wire.calls)
        await asyncio.sleep(0.05)
        assert len(wire.calls) == before, "no flush may happen after close()"


class TestIntervalLoopInternals:
    async def test_an_unexpected_flush_error_does_not_kill_the_timer(self) -> None:
        """`flush()` returns a report for wire failures rather than raising, so
        this guard only catches genuine bugs — but if it ever stopped working
        the acker would go quiet and the consumer would stall at its prefetch
        ceiling with no error anywhere."""
        acker = _acker(AsyncWire(), config=BatchAckConfig(batch_size=1000, flush_interval_ms=10))
        calls: list[int] = []

        async def exploding_flush(_reason: Any = None) -> None:
            calls.append(1)
            raise RuntimeError("bug in flush")

        acker.flush = exploding_flush  # type: ignore[method-assign]
        await acker.start()
        await asyncio.sleep(0.06)
        acker._closed = True
        task = acker._timer_task
        if task is not None:
            task.cancel()

        assert len(calls) >= 2, "the loop must survive the first error"
        assert isinstance(acker.last_error, RuntimeError)

    async def test_closing_during_the_sleep_skips_the_flush(self) -> None:
        wire = AsyncWire()
        acker = _acker(wire, config=BatchAckConfig(batch_size=1000, flush_interval_ms=40))
        await acker.start()
        acker.register(1)
        acker.complete(1)
        # Flip the flag while the loop is parked in its sleep.
        acker._closed = True
        await asyncio.sleep(0.08)
        assert wire.calls == [], "a closed acker must not emit from the timer"
        task = acker._timer_task
        if task is not None:
            task.cancel()
