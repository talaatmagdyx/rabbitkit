"""Tests for async_/batch.py — AsyncBatchPublisher."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.async_.batch import _FLUSHING, _QUEUED, AsyncBatchPublisher, _BatchItem
from rabbitkit.async_.pool import InFlightByteLimiter
from rabbitkit.core.config import BatchPublishConfig
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus


def _env(routing_key: str = "q", body: bytes = b"x") -> MessageEnvelope:
    return MessageEnvelope(routing_key=routing_key, body=body)


def _ok() -> PublishOutcome:
    return PublishOutcome(status=PublishStatus.CONFIRMED, exchange="", routing_key="q")


def _make_transport(
    publish_fn: Any = None,
    channel: Any = None,
) -> tuple[Any, Any, Any]:
    """Return (transport, channel, pool) with sensible defaults."""
    if channel is None:
        channel = MagicMock()
        channel.is_closed = False
        channel.close = AsyncMock()

    pool = MagicMock()
    pool.acquire_publisher_channel = AsyncMock(return_value=channel)
    pool.release_publisher_channel = AsyncMock()
    pool.discard_publisher_channel = MagicMock()

    transport = MagicMock()
    transport._conn_pool = pool
    if publish_fn is None:
        transport._publish_on_channel = AsyncMock(return_value=_ok())
    else:
        transport._publish_on_channel = publish_fn

    return transport, channel, pool


def _item(env: MessageEnvelope | None = None, *, deadline_in: float = 30.0) -> _BatchItem:
    loop = asyncio.get_running_loop()
    return _BatchItem(env or _env(), loop.create_future(), loop.time() + deadline_in, 0, None)


async def _wait_until(predicate: Any, timeout: float = 2.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


# ── _worker_count ────────────────────────────────────────────────────────────


class TestWorkerCount:
    def test_explicit_workers_returned_directly(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=3))
        assert pub._worker_count() == 3

    def test_auto_formula(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(batch_size=100, max_in_flight=1000, flush_workers=0))
        assert pub._worker_count() == 10  # min(16, 1000//100)

    def test_auto_capped_at_16(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(batch_size=1, max_in_flight=10_000, flush_workers=0))
        assert pub._worker_count() == 16

    def test_auto_minimum_1(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(batch_size=1000, max_in_flight=1, flush_workers=0))
        assert pub._worker_count() == 1


# ── start / stop ─────────────────────────────────────────────────────────────


class TestStartStop:
    @pytest.mark.asyncio
    async def test_start_spawns_correct_task_count(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=3, batch_size=1, max_in_flight=30))
        await pub.start()
        assert len(pub._flush_tasks) == 3
        await pub.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_all_tasks(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=2, batch_size=1, max_in_flight=20))
        await pub.start()
        tasks = list(pub._flush_tasks)
        await pub.stop()
        assert all(t.done() for t in tasks)
        assert pub._flush_tasks == []

    @pytest.mark.asyncio
    async def test_stop_resolves_queued_items_not_sent(self) -> None:
        """G-12: an item still queued at stop() never left the client: the
        caller gets NOT_SENT(publisher_closed), not a bare RuntimeError."""
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=0, batch_size=100, max_in_flight=100))
        item = _item()
        pub._pending.put_nowait(item)  # no workers: nothing drains it
        await pub.stop()
        outcome = item.future.result()
        assert outcome.status is PublishStatus.NOT_SENT
        assert outcome.reason == "publisher_closed"

    @pytest.mark.asyncio
    async def test_publish_after_stop_is_not_sent(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1))
        await pub.start()
        await pub.stop()
        outcome = await pub.publish(_env())
        assert outcome.status is PublishStatus.NOT_SENT
        assert outcome.reason == "publisher_closed"
        transport._publish_on_channel.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stop_with_a_timeout_flushes_the_queue_first(self) -> None:
        gate = asyncio.Event()

        async def slow_publish(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            await gate.wait()
            return _ok()

        transport, _, _ = _make_transport(publish_fn=slow_publish)
        pub = AsyncBatchPublisher(
            transport, BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=0, max_in_flight=10)
        )
        await pub.start()
        callers = [asyncio.create_task(pub.publish(_env())) for _ in range(3)]
        await asyncio.sleep(0.01)
        asyncio.get_running_loop().call_later(0.02, gate.set)
        async with asyncio.timeout(5):
            await pub.stop(timeout=2.0)
        results = await asyncio.gather(*callers)
        assert [r.status for r in results] == [PublishStatus.CONFIRMED] * 3

    @pytest.mark.asyncio
    async def test_stop_deadline_reports_mid_flush_items_unknown(self) -> None:
        async def hanging(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            await asyncio.Event().wait()
            raise AssertionError  # pragma: no cover

        transport, channel, pool = _make_transport(publish_fn=hanging)
        pub = AsyncBatchPublisher(
            transport, BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=0, max_in_flight=10)
        )
        await pub.start()
        flushing = asyncio.create_task(pub.publish(_env("a")))
        await _wait_until(lambda: any(i.state == _FLUSHING for i in pub._active))
        queued = asyncio.create_task(pub.publish(_env("b")))
        await asyncio.sleep(0.01)
        async with asyncio.timeout(5):
            await pub.stop(timeout=0.05)
        a, b = await asyncio.gather(flushing, queued)
        assert (a.status, a.reason) == (PublishStatus.UNKNOWN, "shutdown")
        assert (b.status, b.reason) == (PublishStatus.NOT_SENT, "publisher_closed")
        pool.discard_publisher_channel.assert_called_once_with(channel)  # confirm state unknown
        pool.release_publisher_channel.assert_not_awaited()


# ── publish ──────────────────────────────────────────────────────────────────


class TestPublish:
    @pytest.mark.asyncio
    async def test_single_message_confirmed(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=5, max_in_flight=10),
        )
        await pub.start()
        result = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert result.status == PublishStatus.CONFIRMED
        await pub.stop()

    @pytest.mark.asyncio
    async def test_five_messages_resolved_concurrently(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=10, flush_interval_ms=20, max_in_flight=100),
        )
        await pub.start()
        results = await asyncio.wait_for(asyncio.gather(*[pub.publish(_env()) for _ in range(5)]), timeout=5.0)
        assert all(r.status == PublishStatus.CONFIRMED for r in results)
        await pub.stop()

    @pytest.mark.asyncio
    async def test_a_raised_publish_is_unknown_not_an_exception(self) -> None:
        """The frames may have left: the caller gets UNKNOWN, never a raise."""

        async def fail(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            raise ValueError("publish error")

        transport, _, _ = _make_transport(publish_fn=fail)
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=5, max_in_flight=10),
        )
        await pub.start()
        result = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert result.status is PublishStatus.UNKNOWN
        assert result.reason == "connection_lost"
        assert isinstance(result.error, ValueError)
        await pub.stop()

    @pytest.mark.asyncio
    async def test_a_leaked_cancelled_error_is_unknown(self) -> None:
        async def leak(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            raise asyncio.CancelledError  # aio-pika closing a channel under the publish

        transport, _, _ = _make_transport(publish_fn=leak)
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1, batch_size=1, max_in_flight=10))
        await pub.start()
        result = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert result.status is PublishStatus.UNKNOWN
        await pub.stop()

    @pytest.mark.asyncio
    async def test_returned_outcome_passes_through(self) -> None:
        returned = PublishOutcome(status=PublishStatus.RETURNED, reason="returned", reply_code=312)
        transport, _, _ = _make_transport(publish_fn=AsyncMock(return_value=returned))
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1, batch_size=1, max_in_flight=10))
        await pub.start()
        result = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert result is returned
        await pub.stop()

    @pytest.mark.asyncio
    async def test_per_call_timeout_reaches_the_transport(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1, batch_size=1, max_in_flight=10))
        await pub.start()
        await asyncio.wait_for(pub.publish(_env(), timeout=3.0), timeout=5.0)
        _args, kwargs = transport._publish_on_channel.call_args
        assert 0 < kwargs["timeout"] <= 3.0
        await pub.stop()


# ── bounded waits (G-14) ─────────────────────────────────────────────────────


class TestBoundedWaits:
    @pytest.mark.asyncio
    async def test_full_queue_admission_times_out_not_sent(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(max_in_flight=1), admission_timeout=0.05)
        pub._pending.put_nowait(_item())  # queue full, no workers
        outcome = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "admission_timeout")

    @pytest.mark.asyncio
    async def test_expiry_while_queued_withdraws_the_message(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1, batch_size=1, max_in_flight=10))
        outcome = await asyncio.wait_for(pub.publish(_env(), timeout=0.05), timeout=5.0)
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "admission_timeout")
        await pub.start()  # the worker now sees the withdrawn item and skips it
        await asyncio.sleep(0.02)
        await pub.stop()
        transport._publish_on_channel.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_expiry_while_flushing_is_a_publish_timeout(self) -> None:
        gate = asyncio.Event()

        async def slow(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            await gate.wait()
            return _ok()

        transport, _, _ = _make_transport(publish_fn=slow)
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1, batch_size=1, max_in_flight=10))
        await pub.start()
        outcome = await asyncio.wait_for(pub.publish(_env(), timeout=0.1), timeout=5.0)
        assert (outcome.status, outcome.reason) == (PublishStatus.TIMEOUT, "publish_timeout")
        assert outcome.is_unknown
        gate.set()
        await pub.stop(timeout=1.0)

    @pytest.mark.asyncio
    async def test_cancelled_caller_withdraws_a_queued_message(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1, batch_size=1, max_in_flight=10))
        task = asyncio.create_task(pub.publish(_env()))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await pub.start()
        await asyncio.sleep(0.02)
        await pub.stop()
        transport._publish_on_channel.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancelled_caller_does_not_cancel_a_message_being_flushed(self) -> None:
        gate = asyncio.Event()
        finished: list[bool] = []

        async def slow(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            await gate.wait()
            finished.append(True)
            return _ok()

        transport, _, _ = _make_transport(publish_fn=slow)
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1, batch_size=1, max_in_flight=10))
        await pub.start()
        task = asyncio.create_task(pub.publish(_env()))
        await _wait_until(lambda: any(i.state == _FLUSHING for i in pub._active))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        gate.set()
        await _wait_until(lambda: bool(finished))
        await pub.stop()

    @pytest.mark.asyncio
    async def test_cancelled_while_waiting_for_queue_space_releases_bytes(self) -> None:
        transport, _, _ = _make_transport()
        limiter = InFlightByteLimiter(100)
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(max_in_flight=1), byte_limiter=limiter)
        pub._pending.put_nowait(_item())
        task = asyncio.create_task(pub.publish(_env(body=b"12345")))
        await asyncio.sleep(0.01)
        assert limiter.used == 5
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert limiter.used == 0

    @pytest.mark.asyncio
    async def test_an_item_past_its_deadline_is_not_flushed(self) -> None:
        transport, channel, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig())
        late = _item(deadline_in=-1.0)
        await pub._flush(channel, [late])
        assert late.future.result().status is PublishStatus.NOT_SENT
        transport._publish_on_channel.assert_not_awaited()


# ── byte budget (G-14) ───────────────────────────────────────────────────────


class TestByteBudget:
    @pytest.mark.asyncio
    async def test_message_larger_than_the_budget_is_not_sent(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(), byte_limiter=InFlightByteLimiter(4))
        outcome = await pub.publish(_env(body=b"12345"))
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "exceeds_max_in_flight_bytes")

    @pytest.mark.asyncio
    async def test_budget_blocks_until_released_then_times_out(self) -> None:
        gate = asyncio.Event()

        async def slow(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            await gate.wait()
            return _ok()

        transport, _, _ = _make_transport(publish_fn=slow)
        limiter = InFlightByteLimiter(5)
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, max_in_flight=10),
            admission_timeout=0.05,
            byte_limiter=limiter,
        )
        await pub.start()
        first = asyncio.create_task(pub.publish(_env(body=b"12345")))
        await _wait_until(lambda: limiter.used == 5)
        second = await pub.publish(_env(body=b"1"))
        assert (second.status, second.reason) == (PublishStatus.NOT_SENT, "admission_timeout")
        gate.set()
        assert (await first).status is PublishStatus.CONFIRMED
        assert limiter.used == 0  # released when the item reached its outcome
        await pub.stop()

    @pytest.mark.asyncio
    async def test_closed_budget_is_not_sent(self) -> None:
        transport, _, _ = _make_transport()
        limiter = InFlightByteLimiter(5)
        limiter.close()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(), byte_limiter=limiter)
        outcome = await pub.publish(_env())
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "publisher_closed")


# ── channel acquisition ──────────────────────────────────────────────────────


class TestAcquireChannel:
    """A flush worker retries acquiring a channel across a short outage, but
    only for the admission timeout: then it fails the batch NOT_SENT and keeps
    running (it is not supervised, so it must never die or wait forever)."""

    @pytest.mark.asyncio
    async def test_retries_on_failure_and_eventually_succeeds(self) -> None:
        transport, _, pool = _make_transport()
        good_channel = MagicMock()
        good_channel.is_closed = False
        pool.acquire_publisher_channel = AsyncMock(
            side_effect=[RuntimeError("down"), RuntimeError("still down"), good_channel]
        )
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1))

        with patch("asyncio.sleep", new=AsyncMock()):
            channel = await pub._acquire_channel()

        assert channel is good_channel
        assert pool.acquire_publisher_channel.await_count == 3

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates_without_retry(self) -> None:
        transport, _, pool = _make_transport()
        pool.acquire_publisher_channel = AsyncMock(side_effect=asyncio.CancelledError())
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1))

        with pytest.raises(asyncio.CancelledError):
            await pub._acquire_channel()

        pool.acquire_publisher_channel.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_gives_up_after_the_admission_timeout(self) -> None:
        transport, _, pool = _make_transport()
        pool.acquire_publisher_channel = AsyncMock(side_effect=RuntimeError("down"))
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=1), admission_timeout=0.12)
        async with asyncio.timeout(2):
            assert await pub._acquire_channel() is None
        assert pool.acquire_publisher_channel.await_count >= 2

    @pytest.mark.asyncio
    async def test_worker_fails_the_batch_not_sent_and_keeps_running(self) -> None:
        transport, channel, pool = _make_transport()
        attempts = 0

        async def acquire() -> Any:
            nonlocal attempts
            attempts += 1
            if attempts <= 2:  # the start-up attempt and the first batch's attempt
                raise RuntimeError("down")
            return channel

        pool.acquire_publisher_channel = acquire
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, max_in_flight=10),
            admission_timeout=0.01,
        )
        await pub.start()
        failed = await asyncio.wait_for(pub.publish(_env(), timeout=2.0), timeout=5.0)
        assert (failed.status, failed.reason) == (PublishStatus.NOT_SENT, "channel_unavailable")
        ok = await asyncio.wait_for(pub.publish(_env(), timeout=2.0), timeout=5.0)
        assert ok.status is PublishStatus.CONFIRMED
        await pub.stop()


# ── channel-per-worker ───────────────────────────────────────────────────────


class TestChannelPerWorker:
    @pytest.mark.asyncio
    async def test_channel_acquired_once_per_worker_not_per_batch(self) -> None:
        """Each worker acquires its channel at startup, not on every flush."""
        transport, _channel, pool = _make_transport()
        n_workers = 2
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=n_workers, batch_size=1, flush_interval_ms=5, max_in_flight=20),
        )
        await pub.start()
        await asyncio.sleep(0)  # workers run up to their first blocking get()
        assert pool.acquire_publisher_channel.call_count == n_workers

        await asyncio.wait_for(asyncio.gather(*[pub.publish(_env()) for _ in range(4)]), timeout=5.0)
        assert pool.acquire_publisher_channel.call_count == n_workers
        await pub.stop()

    @pytest.mark.asyncio
    async def test_channel_released_on_stop(self) -> None:
        transport, channel, pool = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=5, max_in_flight=10),
        )
        await pub.start()
        await asyncio.sleep(0)  # let worker acquire channel
        await pub.stop()
        pool.release_publisher_channel.assert_called_once_with(channel)


# ── confirm timeouts on a shared channel (M17) ───────────────────────────────


class TestConfirmTimeoutDoesNotCorruptSiblings:
    """M17: a confirm timeout on one message in a batch must not fail the
    OTHER concurrent messages sharing the same channel. The channel is closed
    only after every concurrent publish in the batch has already resolved."""

    @pytest.mark.asyncio
    async def test_sibling_outcomes_unaffected_by_one_timeout(self) -> None:
        async def publish_on_channel(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            if env.routing_key == "slow":
                return PublishOutcome(status=PublishStatus.TIMEOUT, exchange="", routing_key="slow")
            return _ok()

        transport, channel, _ = _make_transport(publish_fn=publish_on_channel)
        pub = AsyncBatchPublisher(transport, BatchPublishConfig())
        batch = [_item(_env(rk)) for rk in ("ok-1", "slow", "ok-2")]

        await pub._flush(channel, batch)

        results = {it.envelope.routing_key: it.future.result() for it in batch}
        assert results["ok-1"].status == PublishStatus.CONFIRMED
        assert results["ok-2"].status == PublishStatus.CONFIRMED
        assert results["slow"].status == PublishStatus.TIMEOUT

    @pytest.mark.asyncio
    async def test_channel_closed_after_gather_when_any_outcome_timed_out(self) -> None:
        timeout = PublishOutcome(status=PublishStatus.TIMEOUT, exchange="", routing_key="q")
        transport, channel, _ = _make_transport(publish_fn=AsyncMock(return_value=timeout))
        pub = AsyncBatchPublisher(transport, BatchPublishConfig())

        await pub._flush(channel, [_item()])

        channel.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_channel_not_closed_when_all_outcomes_ok(self) -> None:
        transport, channel, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig())

        await pub._flush(channel, [_item() for _ in range(3)])

        channel.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_non_outcome_result_is_unknown(self) -> None:
        transport, channel, _ = _make_transport(publish_fn=AsyncMock(return_value=None))
        pub = AsyncBatchPublisher(transport, BatchPublishConfig())
        item = _item()
        await pub._flush(channel, [item])
        assert item.future.result().status is PublishStatus.UNKNOWN

    @pytest.mark.asyncio
    async def test_publish_on_channel_no_longer_closes_channel_itself(self) -> None:
        """M17: the transport-level confirm-timeout handler must not close the
        channel itself -- that decision belongs to the caller."""
        from rabbitkit.async_.transport import AsyncTransportImpl
        from rabbitkit.core.config import ConnectionConfig, SecurityConfig

        transport = AsyncTransportImpl(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
            confirm_timeout=0.01,
        )
        channel = MagicMock()
        channel.is_closed = False
        channel.close = AsyncMock()

        async def slow_publish(*args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(1.0)

        exchange = MagicMock()
        exchange.publish = slow_publish
        channel.get_exchange = AsyncMock(return_value=exchange)

        outcome = await transport._publish_on_channel(
            channel, MessageEnvelope(routing_key="q", body=b"x", exchange="ex")
        )

        assert outcome.status == PublishStatus.TIMEOUT
        channel.close.assert_not_called()


# ── closed-channel recovery ──────────────────────────────────────────────────


class TestClosedChannelRecovery:
    @pytest.mark.asyncio
    async def test_reacquires_when_channel_closed_after_flush(self) -> None:
        channel1 = MagicMock()
        channel1.is_closed = False
        channel2 = MagicMock()
        channel2.is_closed = False
        acquired: list[Any] = []

        async def acquire() -> Any:
            ch = channel1 if not acquired else channel2
            acquired.append(ch)
            return ch

        publish_calls: list[Any] = []

        async def mock_publish(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            publish_calls.append(ch)
            if ch is channel1:
                channel1.is_closed = True
            return _ok()

        transport, _, pool = _make_transport(publish_fn=mock_publish)
        pool.acquire_publisher_channel = acquire
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=5, max_in_flight=10),
        )
        await pub.start()

        await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        await asyncio.wait_for(pub.publish(_env()), timeout=5.0)

        assert acquired == [channel1, channel2]
        assert publish_calls == [channel1, channel2]
        pool.release_publisher_channel.assert_any_await(channel1)  # the closed one was handed back (discarded)
        await pub.stop()


# ── fast drain ───────────────────────────────────────────────────────────────


class TestFastDrain:
    @pytest.mark.asyncio
    async def test_all_queued_items_batched_without_timeout_wait(self) -> None:
        transport, _channel, pool = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=20, flush_interval_ms=50, max_in_flight=100),
        )
        items = [_item() for _ in range(10)]
        for it in items:
            pub._pending.put_nowait(it)

        await pub.start()
        results = await asyncio.wait_for(asyncio.gather(*[it.future for it in items]), timeout=5.0)
        assert all(r.status == PublishStatus.CONFIRMED for r in results)
        assert pool.acquire_publisher_channel.call_count == 1
        await pub.stop()


# ── cancellation of a worker ─────────────────────────────────────────────────


class TestCancelledWorker:
    @pytest.mark.asyncio
    async def test_cancelled_during_flush_resolves_items_unknown(self) -> None:
        """A worker cancelled mid-flush settles every item in its batch
        (UNKNOWN: the frames may have left) and discards its channel."""
        entered = asyncio.Event()

        async def hanging(ch: Any, env: MessageEnvelope, **_: Any) -> PublishOutcome:
            entered.set()
            await asyncio.Event().wait()
            raise AssertionError  # pragma: no cover

        transport, channel, pool = _make_transport(publish_fn=hanging)
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=10, flush_interval_ms=0, max_in_flight=100),
        )
        items = [_item() for _ in range(3)]
        for it in items:
            pub._pending.put_nowait(it)
        await pub.start()
        await asyncio.wait_for(entered.wait(), timeout=5.0)

        for task in pub._flush_tasks:
            task.cancel()
        for task in pub._flush_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        for it in items:
            assert it.future.result().status is PublishStatus.UNKNOWN
            assert it.future.result().reason == "shutdown"
        pool.discard_publisher_channel.assert_called_once_with(channel)

    @pytest.mark.asyncio
    async def test_cancelled_during_straggler_wait_resolves_items_not_sent(self) -> None:
        """Items dequeued but never flushed never left the client."""
        transport, channel, pool = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=10, flush_interval_ms=5000, max_in_flight=100),
        )
        await pub.start()
        item = _item()
        pub._pending.put_nowait(item)
        await _wait_until(lambda: item in pub._active)

        for task in pub._flush_tasks:
            task.cancel()
        for task in pub._flush_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert item.state != _QUEUED
        outcome = item.future.result()
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "publisher_closed")
        pool.discard_publisher_channel.assert_not_called()
        pool.release_publisher_channel.assert_awaited_once_with(channel)


# ── straggler deadline ───────────────────────────────────────────────────────


class TestDeadlineExpiresBeforeStraggler:
    @pytest.mark.asyncio
    async def test_remaining_zero_breaks_straggler_loop_immediately(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=10, flush_interval_ms=0, max_in_flight=100),
        )
        await pub.start()
        result = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert result.status == PublishStatus.CONFIRMED
        await pub.stop()


# ── unexpected flush failure ─────────────────────────────────────────────────


class TestChannelErrorRecovery:
    @pytest.mark.asyncio
    async def test_channel_replaced_when_flush_raises_exception(self) -> None:
        """_publish_on_channel raising synchronously makes _flush itself raise:
        the items are resolved, the channel is discarded and replaced."""
        channel1 = MagicMock()
        channel1.is_closed = False
        channel2 = MagicMock()
        channel2.is_closed = False
        acquired: list[Any] = []

        async def acquire() -> Any:
            ch = channel1 if not acquired else channel2
            acquired.append(ch)
            return ch

        calls = 0

        def publish_on_channel_sync_raise(ch: Any, env: MessageEnvelope, **_: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("sync error from publish_on_channel")

            async def ok_coro() -> PublishOutcome:
                return _ok()

            return ok_coro()

        transport, _, pool = _make_transport(publish_fn=publish_on_channel_sync_raise)
        pool.acquire_publisher_channel = acquire
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=5, max_in_flight=10),
        )
        await pub.start()

        first = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert first.status is PublishStatus.UNKNOWN  # it was marked flushing
        assert isinstance(first.error, RuntimeError)
        pool.discard_publisher_channel.assert_called_once_with(channel1)

        second = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert second.status == PublishStatus.CONFIRMED
        assert acquired == [channel1, channel2]
        await pub.stop()

    @pytest.mark.asyncio
    async def test_unflushed_items_of_a_failed_flush_are_errors(self) -> None:
        transport, channel, pool = _make_transport()
        pub = AsyncBatchPublisher(
            transport, BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=0, max_in_flight=10)
        )

        async def broken_flush(ch: Any, batch: list[_BatchItem]) -> None:
            raise RuntimeError("flush bug")

        with patch.object(pub, "_flush", side_effect=broken_flush):
            await pub.start()
            outcome = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert (outcome.status, outcome.reason) == (PublishStatus.ERROR, "publish_error")
        pool.discard_publisher_channel.assert_called_once_with(channel)
        await pub.stop()


class TestItemLifecycleEdges:
    @pytest.mark.asyncio
    async def test_finish_is_idempotent_and_releases_bytes_once(self) -> None:
        limiter = InFlightByteLimiter(10)
        assert await limiter.acquire(4, timeout=0.1)
        loop = asyncio.get_running_loop()
        item = _BatchItem(_env(), loop.create_future(), loop.time() + 1, 4, limiter)
        item.finish(item.outcome(PublishStatus.CONFIRMED, None))
        item.finish(item.outcome(PublishStatus.UNKNOWN, None))
        assert item.future.result().status is PublishStatus.CONFIRMED
        assert limiter.used == 0

    @pytest.mark.asyncio
    async def test_stop_resolves_items_a_dead_worker_left_behind(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig())
        orphan = _item()
        pub._active.add(orphan)
        await pub.stop()
        assert orphan.future.result().status is PublishStatus.NOT_SENT

    @pytest.mark.asyncio
    async def test_flush_skips_items_already_resolved(self) -> None:
        transport, channel, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig())
        withdrawn = _item()
        withdrawn.finish(withdrawn.outcome(PublishStatus.NOT_SENT, "admission_timeout"))
        await pub._flush(channel, [withdrawn])
        transport._publish_on_channel.assert_not_awaited()
