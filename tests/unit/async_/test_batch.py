"""Tests for async_/batch.py — AsyncBatchPublisher."""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.async_.batch import AsyncBatchPublisher
from rabbitkit.core.config import BatchPublishConfig
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus


def _env() -> MessageEnvelope:
    return MessageEnvelope(routing_key="q", body=b"x")


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

    pool = MagicMock()
    pool.acquire_publisher_channel = AsyncMock(return_value=channel)
    pool.release_publisher_channel = AsyncMock()

    transport = MagicMock()
    transport._conn_pool = pool
    if publish_fn is None:
        transport._publish_on_channel = AsyncMock(return_value=_ok())
    else:
        transport._publish_on_channel = publish_fn

    return transport, channel, pool


# ── _worker_count ────────────────────────────────────────────────────────────


class TestWorkerCount:
    def test_explicit_workers_returned_directly(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(transport, BatchPublishConfig(flush_workers=3))
        assert pub._worker_count() == 3

    def test_auto_formula(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport, BatchPublishConfig(batch_size=100, max_in_flight=1000, flush_workers=0)
        )
        assert pub._worker_count() == 10  # min(16, 1000//100)

    def test_auto_capped_at_16(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport, BatchPublishConfig(batch_size=1, max_in_flight=10_000, flush_workers=0)
        )
        assert pub._worker_count() == 16

    def test_auto_minimum_1(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport, BatchPublishConfig(batch_size=1000, max_in_flight=1, flush_workers=0)
        )
        assert pub._worker_count() == 1


# ── start / stop ─────────────────────────────────────────────────────────────


class TestStartStop:
    @pytest.mark.asyncio
    async def test_start_spawns_correct_task_count(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport, BatchPublishConfig(flush_workers=3, batch_size=1, max_in_flight=30)
        )
        await pub.start()
        assert len(pub._flush_tasks) == 3
        await pub.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_all_tasks(self) -> None:
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport, BatchPublishConfig(flush_workers=2, batch_size=1, max_in_flight=20)
        )
        await pub.start()
        tasks = list(pub._flush_tasks)
        await pub.stop()
        assert all(t.done() for t in tasks)
        assert pub._flush_tasks == []

    @pytest.mark.asyncio
    async def test_stop_fails_pending_futures(self) -> None:
        """Items still in the queue when stop() is called get a RuntimeError."""
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport, BatchPublishConfig(flush_workers=0, batch_size=100, max_in_flight=100)
        )
        # Don't call start() so no workers drain the queue
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[PublishOutcome] = loop.create_future()
        pub._pending.put_nowait((_env(), fut))
        await pub.stop()
        assert fut.done()
        assert isinstance(fut.exception(), RuntimeError)


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
        results = await asyncio.wait_for(
            asyncio.gather(*[pub.publish(_env()) for _ in range(5)]), timeout=5.0
        )
        assert all(r.status == PublishStatus.CONFIRMED for r in results)
        await pub.stop()

    @pytest.mark.asyncio
    async def test_exception_propagates_to_caller(self) -> None:
        async def fail(ch: Any, env: MessageEnvelope) -> PublishOutcome:
            raise ValueError("publish error")

        transport, _, _ = _make_transport(publish_fn=fail)
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=5, max_in_flight=10),
        )
        await pub.start()
        with pytest.raises(ValueError, match="publish error"):
            await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        await pub.stop()


class TestAcquireChannelRetriesWithBackoff:
    """Batch-outage wedge fix: a flush worker must retry acquiring a
    publisher channel across a broker outage instead of dying -- if it died,
    nothing would ever drain _pending after the connection recovered, and
    every publish() future would hang forever (flush workers are not
    supervised/restarted)."""

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

        pool.acquire_publisher_channel.assert_awaited_once()  # no retry attempted


# ── channel-per-worker ───────────────────────────────────────────────────────


class TestChannelPerWorker:
    @pytest.mark.asyncio
    async def test_channel_acquired_once_per_worker_not_per_batch(self) -> None:
        """Each worker acquires its channel at startup, not on every flush."""
        transport, _channel, pool = _make_transport()
        n_workers = 2
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(
                flush_workers=n_workers, batch_size=1, flush_interval_ms=5, max_in_flight=20
            ),
        )
        await pub.start()
        # Yield so workers run up to their first blocking get()
        await asyncio.sleep(0)

        assert pool.acquire_publisher_channel.call_count == n_workers

        # Publish 4 messages (4 batches of 1) — channel count must stay at n_workers
        await asyncio.wait_for(
            asyncio.gather(*[pub.publish(_env()) for _ in range(4)]), timeout=5.0
        )
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


# ── closed-channel recovery ──────────────────────────────────────────────────


class TestConfirmTimeoutDoesNotCorruptSiblings:
    """M17: a confirm timeout on one message in a batch must not fail the
    OTHER concurrent messages sharing the same channel. The channel is closed
    only after every concurrent publish in the batch has already resolved."""

    async def test_sibling_outcomes_unaffected_by_one_timeout(self) -> None:
        def _timeout_outcome() -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.TIMEOUT, exchange="", routing_key="slow")

        async def publish_on_channel(ch: Any, env: MessageEnvelope) -> PublishOutcome:
            if env.routing_key == "slow":
                return _timeout_outcome()
            return _ok()

        transport = MagicMock()
        transport._publish_on_channel = publish_on_channel

        channel = MagicMock()
        channel.is_closed = False
        channel.close = AsyncMock()

        pub = AsyncBatchPublisher(transport, BatchPublishConfig())

        envelopes = [
            MessageEnvelope(routing_key="ok-1", body=b"1"),
            MessageEnvelope(routing_key="slow", body=b"2"),
            MessageEnvelope(routing_key="ok-2", body=b"3"),
        ]
        batch = [(env, asyncio.get_event_loop().create_future()) for env in envelopes]

        await pub._flush(channel, batch)

        results = {env.routing_key: fut.result() for env, fut in batch}
        # The two healthy siblings confirmed cleanly -- NOT corrupted by the
        # slow message's timeout (the pre-fix bug: closing the channel mid-
        # gather would have failed these too).
        assert results["ok-1"].status == PublishStatus.CONFIRMED
        assert results["ok-2"].status == PublishStatus.CONFIRMED
        assert results["slow"].status == PublishStatus.TIMEOUT

    async def test_channel_closed_after_gather_when_any_outcome_timed_out(self) -> None:
        async def publish_on_channel(ch: Any, env: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.TIMEOUT, exchange="", routing_key="q")

        transport = MagicMock()
        transport._publish_on_channel = publish_on_channel

        channel = MagicMock()
        channel.is_closed = False
        channel.close = AsyncMock()

        pub = AsyncBatchPublisher(transport, BatchPublishConfig())
        batch = [(_env(), asyncio.get_event_loop().create_future())]

        await pub._flush(channel, batch)

        channel.close.assert_called_once()

    async def test_channel_not_closed_when_all_outcomes_ok(self) -> None:
        """No unnecessary close when nothing timed out."""
        transport = MagicMock()
        transport._publish_on_channel = AsyncMock(return_value=_ok())

        channel = MagicMock()
        channel.is_closed = False
        channel.close = AsyncMock()

        pub = AsyncBatchPublisher(transport, BatchPublishConfig())
        batch = [(_env(), asyncio.get_event_loop().create_future()) for _ in range(3)]

        await pub._flush(channel, batch)

        channel.close.assert_not_called()

    async def test_publish_on_channel_no_longer_closes_channel_itself(self) -> None:
        """M17: the transport-level confirm-timeout handler must not close the
        channel itself -- that decision now belongs to the caller (see the
        _flush tests above), since _publish_on_channel doesn't know whether
        it's the sole user of the channel."""
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

        envelope = MessageEnvelope(routing_key="q", body=b"x", exchange="ex")
        outcome = await transport._publish_on_channel(channel, envelope)

        assert outcome.status == PublishStatus.TIMEOUT
        channel.close.assert_not_called()


class TestClosedChannelRecovery:
    @pytest.mark.asyncio
    async def test_reacquires_when_channel_closed_after_flush(self) -> None:
        """If _publish_on_channel closes the channel (e.g. timeout), the worker
        detects is_closed=True and acquires a fresh channel before the next batch."""
        channel1 = MagicMock()
        channel2 = MagicMock()
        channel2.is_closed = False
        acquire_calls: list[Any] = []

        # First acquire → channel1, second → channel2
        async def acquire() -> Any:
            if not acquire_calls:
                acquire_calls.append(channel1)
                channel1.is_closed = False  # open initially
                return channel1
            acquire_calls.append(channel2)
            return channel2

        pool = MagicMock()
        pool.acquire_publisher_channel = acquire
        pool.release_publisher_channel = AsyncMock()

        publish_calls: list[Any] = []

        async def mock_publish(ch: Any, env: MessageEnvelope) -> PublishOutcome:
            publish_calls.append(ch)
            # Simulate timeout side-effect: close the channel after first use
            if ch is channel1:
                channel1.is_closed = True
            return _ok()

        transport = MagicMock()
        transport._conn_pool = pool
        transport._publish_on_channel = mock_publish

        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=5, max_in_flight=10),
        )
        await pub.start()

        # First publish: channel1 used, then detected as closed
        await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        # Second publish: should use channel2
        await asyncio.wait_for(pub.publish(_env()), timeout=5.0)

        assert len(acquire_calls) == 2
        assert publish_calls[0] is channel1
        assert publish_calls[1] is channel2
        await pub.stop()


# ── fast drain ───────────────────────────────────────────────────────────────


class TestFastDrain:
    @pytest.mark.asyncio
    async def test_all_queued_items_batched_without_timeout_wait(self) -> None:
        """Items already in the queue are grabbed with get_nowait() in one batch."""
        async def mock_publish(ch: Any, env: MessageEnvelope) -> PublishOutcome:
            return _ok()

        transport, _channel, pool = _make_transport(publish_fn=mock_publish)

        pub = AsyncBatchPublisher(
            transport,
            # Large batch_size so all 10 items fit in one batch
            BatchPublishConfig(
                flush_workers=1, batch_size=20, flush_interval_ms=50, max_in_flight=100
            ),
        )
        # Pre-load 10 items BEFORE starting (worker will fast-drain them all)
        loop = asyncio.get_running_loop()
        futures = []
        for _ in range(10):
            fut: asyncio.Future[PublishOutcome] = loop.create_future()
            pub._pending.put_nowait((_env(), fut))
            futures.append(fut)

        await pub.start()
        results = await asyncio.wait_for(asyncio.gather(*futures), timeout=5.0)
        assert all(r.status == PublishStatus.CONFIRMED for r in results)
        # All 10 were in the queue at once → only 1 acquire (one batch)
        assert pool.acquire_publisher_channel.call_count == 1
        await pub.stop()


# ── CancelledError during flush ───────────────────────────────────────────────


class TestCancelledFlush:
    @pytest.mark.asyncio
    async def test_cancelled_during_flush_resolves_futures(self) -> None:
        """CancelledError raised inside _flush must settle all batch futures with
        RuntimeError so callers are never left pending after task cancellation."""
        blocked = asyncio.Event()
        entered_flush = asyncio.Event()

        async def hanging_publish(ch: Any, env: MessageEnvelope) -> PublishOutcome:
            entered_flush.set()
            await blocked.wait()  # hangs until the task is cancelled
            return _ok()  # pragma: no cover

        transport, _, _ = _make_transport(publish_fn=hanging_publish)
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=10, flush_interval_ms=50, max_in_flight=100),
        )
        await pub.start()

        loop = asyncio.get_running_loop()
        futures: list[asyncio.Future[PublishOutcome]] = []
        for _ in range(3):
            fut: asyncio.Future[PublishOutcome] = loop.create_future()
            pub._pending.put_nowait((_env(), fut))
            futures.append(fut)

        # Wait until the worker is blocked inside _flush (hanging on blocked.wait())
        await asyncio.wait_for(entered_flush.wait(), timeout=5.0)

        # Cancel the flush task directly (simulates asyncio.CancelledError mid-flush)
        for task in pub._flush_tasks:
            task.cancel()
        for task in pub._flush_tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

        # All three futures must be resolved — none left pending
        assert all(fut.done() for fut in futures), "Some futures were left pending after task cancellation"
        for fut in futures:
            exc = fut.exception()
            assert isinstance(exc, RuntimeError), f"Expected RuntimeError, got {type(exc).__name__}: {exc}"


# ── Lines 85-86: QueueEmpty path in stop() drain loop ────────────────────────


class TestStopEmptyQueue:
    """Lines 85-86: stop() drain loop hits asyncio.QueueEmpty immediately when
    the pending queue is empty — the ``except QueueEmpty: break`` branch."""

    @pytest.mark.asyncio
    async def test_stop_with_empty_queue_hits_queue_empty_branch(self) -> None:
        """start() then stop() with no messages enqueued exercises the
        QueueEmpty break path in the drain while-loop."""
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=10, flush_interval_ms=5, max_in_flight=10),
        )
        await pub.start()
        # Drain loop runs: pending queue is empty → QueueEmpty → break
        await pub.stop()
        # No futures to check; just verify we get here without error
        assert pub._flush_tasks == []

    @pytest.mark.asyncio
    async def test_stop_empty_queue_no_side_effects(self) -> None:
        """stop() with an empty queue must be a clean no-op for the drain."""
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=2, batch_size=5, flush_interval_ms=5, max_in_flight=20),
        )
        await pub.start()
        # Queue is empty — stop() drain must not raise
        await pub.stop()
        assert pub._flush_tasks == []


# ── Line 124: remaining <= 0 break in straggler-wait loop ────────────────────


class TestDeadlineExpiresBeforeStraggler:
    """Line 124: ``if remaining <= 0: break`` — the deadline has already
    elapsed when the ``remaining`` check is reached at the top of the straggler
    while loop, so the batch is flushed under-full without entering
    ``asyncio.timeout``."""

    @pytest.mark.asyncio
    async def test_remaining_zero_breaks_straggler_loop_immediately(self) -> None:
        """Use flush_interval_ms=0 (0 ms interval) so ``deadline = loop.time()``
        is set to the current time.  By the time the straggler while loop runs
        its first iteration, ``remaining = deadline - loop.time() <= 0``,
        triggering the ``if remaining <= 0: break`` branch (line 124) without
        ever entering the ``asyncio.timeout`` block."""
        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            # flush_interval_ms=0 → interval=0.0 → deadline already elapsed
            BatchPublishConfig(flush_workers=1, batch_size=10, flush_interval_ms=0, max_in_flight=100),
        )
        await pub.start()

        # Publish one message — the worker collects it, enters the straggler
        # wait with interval=0, finds remaining<=0 immediately, breaks, and
        # flushes the single-item batch.
        result = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert result.status == PublishStatus.CONFIRMED

        await pub.stop()


# ── Lines 146-149: non-CancelledError exception recovery in _flush_worker ────


class TestChannelErrorRecovery:
    """Lines 146-149: when _flush() itself raises a non-CancelledError
    exception (e.g. InvalidStateError because a future was already cancelled),
    the worker releases the broken channel and re-acquires a fresh one."""

    @pytest.mark.asyncio
    async def test_channel_replaced_when_flush_raises_exception(self) -> None:
        """Lines 146-149: _flush raises a plain Exception when
        _publish_on_channel raises synchronously (before returning a coroutine),
        causing the list comprehension inside _flush to propagate the error.
        The worker must then release the old channel, acquire a replacement,
        and continue processing the next batch."""
        channel1 = MagicMock()
        channel1.is_closed = False
        channel2 = MagicMock()
        channel2.is_closed = False

        acquire_calls: list[Any] = []

        async def acquire() -> Any:
            if not acquire_calls:
                acquire_calls.append(channel1)
                return channel1
            acquire_calls.append(channel2)
            return channel2

        pool = MagicMock()
        pool.acquire_publisher_channel = acquire
        pool.release_publisher_channel = AsyncMock()

        call_count = 0

        def publish_on_channel_sync_raise(ch: Any, env: MessageEnvelope) -> Any:
            """Raises synchronously (not as a coroutine) on the first call."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Raise synchronously — before returning a coroutine.
                # This causes the list comprehension in _flush to propagate
                # the error, which triggers the except BaseException branch
                # (lines 143-149) in _flush_loop.
                raise RuntimeError("sync error from publish_on_channel")

            async def ok_coro() -> PublishOutcome:
                return _ok()

            return ok_coro()

        transport = MagicMock()
        transport._conn_pool = pool
        transport._publish_on_channel = publish_on_channel_sync_raise

        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(flush_workers=1, batch_size=1, flush_interval_ms=5, max_in_flight=10),
        )
        await pub.start()

        # First publish: _flush raises → future gets RuntimeError, channel replaced.
        loop = asyncio.get_running_loop()
        fut1: asyncio.Future[PublishOutcome] = loop.create_future()
        pub._pending.put_nowait((_env(), fut1))

        # Wait until the future is settled (worker processed and recovered).
        for _ in range(50):
            await asyncio.sleep(0.01)
            if fut1.done():
                break

        assert fut1.done()
        assert isinstance(fut1.exception(), RuntimeError)

        # Two channels acquired: initial + replacement.
        assert len(acquire_calls) == 2

        # Second publish on the replacement channel succeeds.
        result = await asyncio.wait_for(pub.publish(_env()), timeout=5.0)
        assert result.status == PublishStatus.CONFIRMED

        await pub.stop()


# ── Line 165: fut.set_exception for batch held in local variable during ──────
# ── straggler wait cancellation (outer except BaseException) ────────────────


class TestCancelledFlushLocalBatch:
    """Line 165: when the task is cancelled during the straggler wait
    (lines 121-129), the outer ``except BaseException`` handler at line 160
    fires.  At that point ``batch`` contains items that were dequeued by
    ``await self._pending.get()`` (line 106) but never flushed — those
    futures must be settled with RuntimeError (line 165)."""

    @pytest.mark.asyncio
    async def test_cancelled_during_straggler_wait_settles_batch_futures(self) -> None:
        """Task cancelled while blocked in the straggler-wait ``await
        self._pending.get()`` (line 127) after the first item was already
        collected.  The outer except fires with a non-empty batch whose
        futures were never flushed → line 165 settles them."""
        # Use batch_size=10 and flush_interval_ms=5000 (5 s) so the worker:
        #   1. dequeues the first item (line 106) into batch
        #   2. fast-drains with get_nowait() — queue empty → break
        #   3. enters the straggler-wait loop (len(batch) < batch_size)
        #   4. blocks at ``await self._pending.get()`` under asyncio.timeout
        # We cancel the task while it's blocked there.

        straggler_blocked = asyncio.Event()
        original_timeout = asyncio.timeout

        # Patch asyncio.timeout to signal us when the straggler wait starts.
        def tracking_timeout(delay: float | None) -> Any:
            if delay is not None and delay > 1.0:
                # The straggler-wait timeout is flush_interval_ms/1000 = 5 s.
                straggler_blocked.set()
            return original_timeout(delay)

        transport, _, _ = _make_transport()
        pub = AsyncBatchPublisher(
            transport,
            BatchPublishConfig(
                flush_workers=1,
                batch_size=10,
                flush_interval_ms=5000,  # long enough to block
                max_in_flight=100,
            ),
        )
        await pub.start()

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[PublishOutcome] = loop.create_future()
        pub._pending.put_nowait((_env(), fut))

        # Wait until the worker has entered the straggler wait (it will block
        # on asyncio.timeout(5.0) waiting for more items).
        await asyncio.sleep(0.1)  # give worker time to dequeue and enter straggler

        # Cancel the task while it's blocked in the straggler wait.
        for task in pub._flush_tasks:
            task.cancel()
        for task in pub._flush_tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

        # The future must be settled by the outer except BaseException handler
        # (line 165) since _flush was never called.
        assert fut.done()
        exc = fut.exception()
        assert isinstance(exc, RuntimeError)
