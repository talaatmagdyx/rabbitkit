"""Plan §4 regression tests for highload/batch.py — safe BatchAcker default,
BatchPublisher partial-flush accounting, lifecycle races, CoalescingAcker."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.config import BatchAckConfig, BatchPublishConfig
from rabbitkit.core.errors import ConfigValidationError
from rabbitkit.core.types import BulkPublishStatus, FlushReason, MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.highload.batch import (
    BatchAcker,
    BatchClosedError,
    BatchFlushError,
    BatchPublisher,
    CoalescingAcker,
)


def _env(i: int) -> MessageEnvelope:
    return MessageEnvelope(routing_key=f"rk-{i}", body=b"x", message_id=f"m{i}")


# ── BatchAcker: selected completions are not a cumulative watermark ───────


class TestBatchAckerSafeDefault:
    def test_default_mode_is_individual(self) -> None:
        assert BatchAckConfig().mode == "individual"
        assert BatchAcker(ack_fn=MagicMock()).mode == "individual"

    def test_reproduction_tags_1_and_3_do_not_settle_2(self) -> None:
        """Plan §4.1: the old helper emitted ack(3, multiple=True) here, acking
        an unsubmitted, still-processing tag 2."""
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(batch_size=100, flush_interval_ms=0))
        ba.add(1)
        ba.add(3)
        assert ba.flush() == 2
        assert [(*c.args, c.kwargs.get("multiple")) for c in ack_fn.call_args_list] == [(1, False), (3, False)]
        assert not any(c.kwargs.get("multiple") for c in ack_fn.call_args_list)

    def test_individual_acks_emitted_in_tag_order(self) -> None:
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(flush_interval_ms=0))
        for t in (30, 10, 20):
            ba.add(t)
        ba.flush()
        assert [c.args[0] for c in ack_fn.call_args_list] == [10, 20, 30]
        assert ba.acked_total == 3

    def test_duplicate_tag_is_acked_once(self) -> None:
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(flush_interval_ms=0))
        ba.add(7)
        ba.add(7)
        assert ba.pending == 1
        ba.flush()
        ack_fn.assert_called_once_with(7, multiple=False)

    def test_add_after_close_rejected(self) -> None:
        ba = BatchAcker(ack_fn=MagicMock(), config=BatchAckConfig(flush_interval_ms=0))
        ba.close()
        assert ba.closed
        with pytest.raises(BatchClosedError):
            ba.add(1)

    def test_close_is_idempotent(self) -> None:
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(flush_interval_ms=0))
        ba.add(1)
        assert ba.close() == 1
        assert ba.close() == 0
        ack_fn.assert_called_once()

    def test_ack_failure_keeps_remainder_buffered_and_raises(self) -> None:
        calls: list[int] = []

        def ack_fn(tag: int, multiple: bool = False) -> None:
            calls.append(tag)
            if tag == 2:
                raise ConnectionError("channel closed")

        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(flush_interval_ms=0))
        for t in (1, 2, 3, 4):
            ba.add(t)
        with pytest.raises(ConnectionError):
            ba.flush()
        assert calls == [1, 2]
        # 3 and 4 are retained; 2 (unknown state) is not re-queued
        assert sorted(ba._tags) == [3, 4]

    def test_timer_error_is_exposed_not_swallowed(self) -> None:
        errors: list[BaseException] = []

        def ack_fn(tag: int, multiple: bool = False) -> None:
            raise RuntimeError("io thread gone")

        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(flush_interval_ms=10), on_error=errors.append)
        ba.add(1)
        deadline = time.monotonic() + 2
        while ba.last_error is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert isinstance(ba.last_error, RuntimeError)
        assert errors and isinstance(errors[0], RuntimeError)
        ba._closed = True  # stop the timer without another flush attempt
        if ba._timer is not None:
            ba._timer.cancel()

    def test_concurrent_add_and_flush_never_double_acks(self) -> None:
        acked: list[int] = []
        lock = threading.Lock()

        def ack_fn(tag: int, multiple: bool = False) -> None:
            with lock:
                acked.append(tag)

        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(batch_size=7, flush_interval_ms=5))
        n = 500

        def producer(start: int) -> None:
            for t in range(start, start + n):
                ba.add(t)

        threads = [threading.Thread(target=producer, args=(i * n + 1,)) for i in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        ba.close()
        assert sorted(acked) == list(range(1, 4 * n + 1))
        assert len(acked) == len(set(acked))


class TestBatchAckerCumulativeOptIn:
    def test_config_requires_attestation(self) -> None:
        with pytest.raises(ConfigValidationError, match="ordered_exclusive_owner"):
            BatchAckConfig(mode="cumulative")
        with pytest.raises(ConfigValidationError):
            BatchAckConfig(mode="watermark")
        with pytest.raises(ConfigValidationError):
            BatchAckConfig(batch_size=0)
        with pytest.raises(ConfigValidationError):
            BatchAckConfig(flush_interval_ms=-1)

    def test_cumulative_emits_max_tag_multiple(self, caplog: pytest.LogCaptureFixture) -> None:
        ack_fn = MagicMock()
        cfg = BatchAckConfig(mode="cumulative", ordered_exclusive_owner=True, flush_interval_ms=0)
        with caplog.at_level("WARNING"):
            ba = BatchAcker(ack_fn=ack_fn, config=cfg)
        assert any("cumulative" in r.message for r in caplog.records)
        for t in (1, 2, 3):
            ba.add(t)
        assert ba.flush() == 3
        ack_fn.assert_called_once_with(3, multiple=True)

    async def test_cumulative_async(self) -> None:
        acked: list[tuple[int, bool]] = []

        async def ack_fn(tag: int, multiple: bool = False) -> None:
            acked.append((tag, multiple))

        cfg = BatchAckConfig(mode="cumulative", ordered_exclusive_owner=True, flush_interval_ms=0)
        ba = BatchAcker(ack_fn=ack_fn, config=cfg)
        await ba.add_async(5)
        await ba.add_async(9)
        assert await ba.flush_async() == 2
        assert acked == [(9, True)]


class TestBatchAckerAsyncSafeDefault:
    async def test_individual_async(self) -> None:
        acked: list[tuple[int, bool]] = []

        async def ack_fn(tag: int, multiple: bool = False) -> None:
            acked.append((tag, multiple))

        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(flush_interval_ms=0))
        await ba.add_async(3)
        await ba.add_async(1)
        assert await ba.flush_async() == 2
        assert acked == [(1, False), (3, False)]

    async def test_add_async_after_close_rejected(self) -> None:
        ba = BatchAcker(ack_fn=MagicMock(), config=BatchAckConfig(flush_interval_ms=0))
        await ba.close_async()
        with pytest.raises(BatchClosedError):
            await ba.add_async(1)

    async def test_async_failure_keeps_remainder(self) -> None:
        async def ack_fn(tag: int, multiple: bool = False) -> None:
            if tag == 2:
                raise RuntimeError("closed")

        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(flush_interval_ms=0))
        for t in (1, 2, 3):
            await ba.add_async(t)
        with pytest.raises(RuntimeError):
            await ba.flush_async()
        assert ba._tags == [3]

    async def test_async_interval_error_exposed(self) -> None:
        async def ack_fn(tag: int, multiple: bool = False) -> None:
            raise RuntimeError("boom")

        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(flush_interval_ms=5))
        await ba.add_async(1)
        for _ in range(100):
            if ba.last_error is not None:
                break
            await asyncio.sleep(0.005)
        assert isinstance(ba.last_error, RuntimeError)
        ba._tags.clear()
        ba._seen.clear()
        await ba.close_async()


# ── BatchPublisher: preserve partial results ──────────────────────────────


def _ok(env: MessageEnvelope) -> PublishOutcome:
    return PublishOutcome(status=PublishStatus.CONFIRMED, routing_key=env.routing_key)


class TestBatchPublisherFlushAccounting:
    def test_flush_report_per_item(self) -> None:
        outcomes = iter(
            [
                PublishOutcome(status=PublishStatus.CONFIRMED),
                PublishOutcome(status=PublishStatus.NACKED),
                PublishOutcome(status=PublishStatus.RETURNED),
            ]
        )
        bp = BatchPublisher(publish_fn=lambda e: next(outcomes), config=BatchPublishConfig(flush_interval_ms=0))
        for i in range(3):
            bp.add(_env(i))
        report = bp.flush_report()
        assert report.attempted == 3
        assert [it.status for it in report.items] == [
            BulkPublishStatus.CONFIRMED,
            BulkPublishStatus.NACKED,
            BulkPublishStatus.UNROUTABLE,
        ]
        assert report.confirmed == 1 and report.published == 1
        assert not report.complete and report.unsent == ()
        assert bp.last_flush is report

    def test_flush_int_counts_only_non_failed(self) -> None:
        """Success is never inferred from loop iterations."""
        bp = BatchPublisher(
            publish_fn=lambda e: PublishOutcome(status=PublishStatus.NACKED),
            config=BatchPublishConfig(flush_interval_ms=0),
        )
        bp.add(_env(0))
        bp.add(_env(1))
        assert bp.flush() == 0

    def test_failure_on_item_2_of_5_leaves_exact_outcomes(self) -> None:
        """Plan §4.2 acceptance: item 2 raises → items 0,1 have real outcomes,
        item 2 is UNKNOWN, items 3,4 are retained unsent, nothing re-buffered."""
        published: list[str] = []

        def pub(e: MessageEnvelope) -> PublishOutcome:
            if e.routing_key == "rk-2":
                raise ConnectionResetError("socket")
            published.append(e.routing_key)
            return _ok(e)

        bp = BatchPublisher(publish_fn=pub, config=BatchPublishConfig(flush_interval_ms=0))
        for i in range(5):
            bp.add(_env(i))
        with pytest.raises(BatchFlushError) as ei:
            bp.flush()
        report = ei.value.report
        assert published == ["rk-0", "rk-1"]
        assert [it.status for it in report.items] == [
            BulkPublishStatus.CONFIRMED,
            BulkPublishStatus.CONFIRMED,
            BulkPublishStatus.UNKNOWN,
        ]
        assert isinstance(report.items[2].error, ConnectionResetError)
        assert [e.routing_key for e in report.unsent] == ["rk-3", "rk-4"]
        assert bp.pending == 0  # not blindly put back
        assert bp.last_flush is report
        assert isinstance(ei.value.cause, ConnectionResetError)
        assert "after 3 of 5 envelopes" in str(ei.value)  # 3 attempted (2 ok + the one that raised)

    def test_confirm_fn_error_is_reported(self) -> None:
        confirm = MagicMock(side_effect=TimeoutError("no confirms"))
        bp = BatchPublisher(publish_fn=_ok, confirm_fn=confirm, config=BatchPublishConfig(flush_interval_ms=0))
        bp.add(_env(0))
        with pytest.raises(BatchFlushError) as ei:
            bp.flush()
        assert isinstance(ei.value.report.confirm_error, TimeoutError)
        assert ei.value.report.items[0].ok  # the publish itself was fine

    def test_add_after_close_rejected(self) -> None:
        bp = BatchPublisher(publish_fn=_ok, config=BatchPublishConfig(flush_interval_ms=0))
        bp.close()
        with pytest.raises(BatchClosedError):
            bp.add(_env(0))
        assert bp.closed

    def test_close_idempotent(self) -> None:
        pub = MagicMock(side_effect=_ok)
        bp = BatchPublisher(publish_fn=pub, config=BatchPublishConfig(flush_interval_ms=0))
        bp.add(_env(0))
        assert bp.close() == 1
        assert bp.close() == 0
        assert pub.call_count == 1

    def test_timer_error_exposed(self) -> None:
        errors: list[BaseException] = []

        def pub(e: MessageEnvelope) -> PublishOutcome:
            raise RuntimeError("down")

        bp = BatchPublisher(publish_fn=pub, config=BatchPublishConfig(flush_interval_ms=10), on_error=errors.append)
        bp.add(_env(0))
        deadline = time.monotonic() + 2
        while bp.last_error is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert isinstance(bp.last_error, BatchFlushError)
        assert errors
        bp._closed = True
        if bp._timer is not None:
            bp._timer.cancel()

    def test_concurrent_manual_and_timer_flush_publish_each_once(self) -> None:
        seen: list[str] = []
        lock = threading.Lock()

        def pub(e: MessageEnvelope) -> PublishOutcome:
            with lock:
                seen.append(e.message_id)
            return _ok(e)

        bp = BatchPublisher(publish_fn=pub, config=BatchPublishConfig(batch_size=5, flush_interval_ms=3))
        n = 300

        def producer(start: int) -> None:
            for i in range(start, start + n):
                bp.add(_env(i))
                if i % 17 == 0:
                    bp.flush()

        threads = [threading.Thread(target=producer, args=(k * n,)) for k in range(3)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        bp.close()
        assert sorted(seen) == sorted(f"m{i}" for i in range(3 * n))

    def test_non_outcome_return_is_unknown(self) -> None:
        bp = BatchPublisher(publish_fn=lambda e: None, config=BatchPublishConfig(flush_interval_ms=0))
        bp.add(_env(0))
        report = bp.flush_report()
        assert report.items[0].status is BulkPublishStatus.UNKNOWN
        assert report.published == 1  # not a local failure — may have reached the broker


class TestBatchPublisherAsyncAccounting:
    async def test_async_partial_failure(self) -> None:
        async def pub(e: MessageEnvelope) -> PublishOutcome:
            if e.routing_key == "rk-1":
                raise RuntimeError("x")
            return _ok(e)

        bp = BatchPublisher(publish_fn=pub, config=BatchPublishConfig(flush_interval_ms=0))
        for i in range(3):
            await bp.add_async(_env(i))
        with pytest.raises(BatchFlushError) as ei:
            await bp.flush_async()
        r = ei.value.report
        assert [it.status for it in r.items] == [BulkPublishStatus.CONFIRMED, BulkPublishStatus.UNKNOWN]
        assert [e.routing_key for e in r.unsent] == ["rk-2"]

    async def test_async_report_and_close(self) -> None:
        async def pub(e: MessageEnvelope) -> PublishOutcome:
            return _ok(e)

        bp = BatchPublisher(publish_fn=pub, config=BatchPublishConfig(flush_interval_ms=0))
        await bp.add_async(_env(0))
        report = await bp.flush_report_async()
        assert report.complete and report.confirmed == 1
        assert await bp.close_async() == 0
        with pytest.raises(BatchClosedError):
            await bp.add_async(_env(1))

    async def test_async_confirm_error(self) -> None:
        async def pub(e: MessageEnvelope) -> PublishOutcome:
            return _ok(e)

        async def confirm() -> None:
            raise TimeoutError("confirm")

        bp = BatchPublisher(publish_fn=pub, confirm_fn=confirm, config=BatchPublishConfig(flush_interval_ms=0))
        await bp.add_async(_env(0))
        with pytest.raises(BatchFlushError):
            await bp.flush_async()

    async def test_async_interval_error_exposed(self) -> None:
        async def pub(e: MessageEnvelope) -> PublishOutcome:
            raise RuntimeError("down")

        bp = BatchPublisher(publish_fn=pub, config=BatchPublishConfig(flush_interval_ms=5))
        await bp.add_async(_env(0))
        for _ in range(100):
            if bp.last_error is not None:
                break
            await asyncio.sleep(0.005)
        assert isinstance(bp.last_error, BatchFlushError)
        await bp.close_async()


# ── CoalescingAcker ───────────────────────────────────────────────────────


class _Wire:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, bool]] = []

    def ack(self, tag: int, multiple: bool) -> None:
        self.calls.append(("ack", tag, multiple))

    def nack(self, tag: int, requeue: bool) -> None:
        self.calls.append(("nack", tag, requeue))

    def reject(self, tag: int, requeue: bool) -> None:
        self.calls.append(("reject", tag, requeue))


def _coalescer(wire: _Wire, **kw: Any) -> CoalescingAcker:
    kw.setdefault("config", BatchAckConfig(batch_size=100, flush_interval_ms=0))
    return CoalescingAcker(ack_fn=wire.ack, nack_fn=wire.nack, reject_fn=wire.reject, **kw)


class TestCoalescingAcker:
    def test_spec_table(self) -> None:
        wire = _Wire()
        ca = _coalescer(wire, max_hold=0)
        for t in (101, 102, 103, 104):
            ca.register(t)
        ca.complete(101)
        ca.complete(103)
        ca.retry_pending(104)
        report = ca.flush()
        assert wire.calls == [("ack", 101, False), ("ack", 103, False)]
        assert report.reason is FlushReason.MANUAL and report.settled_tags == 2 and report.coalesced_tags == 0
        assert ca.pending == 2

    def test_prefix_coalesces_and_counts(self) -> None:
        wire = _Wire()
        ca = _coalescer(wire)
        for t in (1, 2, 3):
            ca.register(t)
            ca.complete(t)
        report = ca.flush()
        assert wire.calls == [("ack", 3, True)]
        assert report.coalesced_tags == 3 and ca.coalesced_total == 3 and ca.settled_total == 3

    def test_size_trigger_flushes_automatically(self) -> None:
        wire = _Wire()
        ca = _coalescer(wire, config=BatchAckConfig(batch_size=2, flush_interval_ms=0))
        ca.register(1)
        ca.register(2)
        ca.complete(1)
        assert wire.calls == []
        ca.complete(2)
        assert wire.calls == [("ack", 2, True)]

    def test_fail_paths(self) -> None:
        wire = _Wire()
        ca = _coalescer(wire)
        for t in (1, 2):
            ca.register(t)
        ca.fail(1, requeue=False)
        ca.fail(2, reject=True, requeue=True)
        ca.flush()
        assert wire.calls == [("nack", 1, False), ("reject", 2, True)]

    def test_reconnect_drops_ledger(self) -> None:
        wire = _Wire()
        ca = _coalescer(wire)
        ca.register(1)
        ca.complete(1)
        dropped = ca.on_reconnect()
        assert dropped == (1,)
        ca.flush()
        assert wire.calls == []  # never replay old tags
        ca.register(1)  # new generation
        ca.complete(1)
        ca.flush()
        assert wire.calls == [("ack", 1, False)]

    def test_close_drains_individually_and_rejects_register(self) -> None:
        wire = _Wire()
        ca = _coalescer(wire)
        for t in (1, 2, 3):
            ca.register(t)
        ca.complete(1)
        ca.complete(2)
        report = ca.close()
        assert report.reason is FlushReason.CLOSE
        assert wire.calls == [("ack", 1, False), ("ack", 2, False)]
        assert ca.pending == 1 and ca.closed
        with pytest.raises(BatchClosedError):
            ca.register(4)

    def test_wire_error_recorded(self) -> None:
        wire = _Wire()

        def bad_ack(tag: int, multiple: bool) -> None:
            raise RuntimeError("closed")

        ca = CoalescingAcker(
            ack_fn=bad_ack, nack_fn=wire.nack, reject_fn=wire.reject, config=BatchAckConfig(flush_interval_ms=0)
        )
        ca.register(1)
        ca.complete(1)
        report = ca.flush()
        assert report.errors and isinstance(report.errors[0][1], RuntimeError)
        assert isinstance(ca.last_error, RuntimeError)
        assert report.settled_tags == 0

    def test_interval_timer_flushes(self) -> None:
        wire = _Wire()
        ca = _coalescer(wire, config=BatchAckConfig(batch_size=100, flush_interval_ms=5))
        ca.register(1)
        ca.complete(1)
        deadline = time.monotonic() + 2
        while not wire.calls and time.monotonic() < deadline:
            time.sleep(0.005)
        assert wire.calls == [("ack", 1, False)]
        ca.close()

    def test_on_flush_hook(self) -> None:
        wire = _Wire()
        reports: list[Any] = []
        ca = _coalescer(wire, on_flush=reports.append)
        ca.register(1)
        ca.complete(1)
        ca.flush()
        assert reports and reports[0].settled_tags == 1

    def test_coalesce_disabled(self) -> None:
        wire = _Wire()
        ca = _coalescer(wire, coalesce=False)
        for t in (1, 2):
            ca.register(t)
            ca.complete(t)
        ca.flush()
        assert wire.calls == [("ack", 1, False), ("ack", 2, False)]
        assert not ca.coordinator.coalesce_enabled
