"""async_/pool.py: publish admission — byte budget, bounded channel waits,
generation-safe release, exhaustion without rebuild (G-04, G-05, G-14)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.async_.pool import (
    AsyncChannelPool,
    AsyncConnectionPool,
    ChannelPoolClosedError,
    ChannelPoolExhaustedError,
    InFlightByteLimiter,
)
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig


def _channel(*, closed: bool = False) -> AsyncMock:
    ch = AsyncMock()
    ch.is_closed = closed

    async def _close() -> None:
        ch.is_closed = True

    ch.close = AsyncMock(side_effect=_close)
    return ch


def _conn(*channels: Any) -> AsyncMock:
    conn = AsyncMock()
    conn.is_closed = False
    if channels:
        conn.channel = AsyncMock(side_effect=list(channels))
    else:
        conn.channel = AsyncMock(side_effect=lambda **_: _channel())
    return conn


# ── InFlightByteLimiter ──────────────────────────────────────────────────────


class TestInFlightByteLimiter:
    def test_rejects_a_non_positive_limit(self) -> None:
        with pytest.raises(ValueError, match=">= 1"):
            InFlightByteLimiter(0)

    @pytest.mark.asyncio
    async def test_fast_path_and_release(self) -> None:
        limiter = InFlightByteLimiter(10)
        assert await limiter.acquire(4, timeout=0.1)
        assert (limiter.used, limiter.limit) == (4, 10)
        limiter.release(4)
        assert limiter.used == 0

    @pytest.mark.asyncio
    async def test_larger_than_the_limit_is_refused_at_once(self) -> None:
        assert not await InFlightByteLimiter(3).acquire(4, timeout=10.0)

    @pytest.mark.asyncio
    async def test_waits_then_times_out(self) -> None:
        limiter = InFlightByteLimiter(5)
        await limiter.acquire(5, timeout=0.1)
        assert not await limiter.acquire(1, timeout=0.02)
        assert limiter.used == 5
        assert not limiter._waiters

    @pytest.mark.asyncio
    async def test_waiters_are_granted_in_fifo_order(self) -> None:
        limiter = InFlightByteLimiter(5)
        await limiter.acquire(5, timeout=0.1)
        order: list[str] = []

        async def take(name: str, n: int) -> None:
            assert await limiter.acquire(n, timeout=1.0)
            order.append(name)

        first = asyncio.create_task(take("big", 4))
        await asyncio.sleep(0)
        second = asyncio.create_task(take("small", 1))
        await asyncio.sleep(0)
        assert order == []  # small would fit later but must not overtake big
        limiter.release(5)
        await asyncio.gather(first, second)
        assert order == ["big", "small"]
        assert limiter.used == 5

    @pytest.mark.asyncio
    async def test_a_cancelled_waiter_unblocks_the_queue(self) -> None:
        limiter = InFlightByteLimiter(5)
        await limiter.acquire(3, timeout=0.1)
        head = asyncio.create_task(limiter.acquire(5, timeout=5.0))  # cannot fit yet
        await asyncio.sleep(0)
        behind = asyncio.create_task(limiter.acquire(2, timeout=5.0))  # fits, but queued behind head
        await asyncio.sleep(0)
        head.cancel()
        with pytest.raises(asyncio.CancelledError):
            await head
        assert await asyncio.wait_for(behind, timeout=1.0)
        assert limiter.used == 5

    @pytest.mark.asyncio
    async def test_granted_just_as_the_wait_expires_gives_the_bytes_back(self) -> None:
        limiter = InFlightByteLimiter(5)
        await limiter.acquire(5, timeout=0.1)
        task = asyncio.create_task(limiter.acquire(5, timeout=5.0))
        await asyncio.sleep(0)
        limiter.release(5)  # grants the waiter (future resolved) ...
        task.cancel()  # ... but the waiter is cancelled before it resumes
        with pytest.raises(asyncio.CancelledError):
            await task
        assert limiter.used == 0

    @pytest.mark.asyncio
    async def test_close_fails_waiters_and_later_acquires(self) -> None:
        limiter = InFlightByteLimiter(5)
        await limiter.acquire(5, timeout=0.1)
        waiter = asyncio.create_task(limiter.acquire(1, timeout=5.0))
        await asyncio.sleep(0)
        limiter.close()
        with pytest.raises(ChannelPoolClosedError):
            await waiter
        with pytest.raises(ChannelPoolClosedError):
            await limiter.acquire(1, timeout=0.1)

    @pytest.mark.asyncio
    async def test_grant_skips_finished_waiters(self) -> None:
        limiter = InFlightByteLimiter(5)
        loop = asyncio.get_running_loop()
        done: asyncio.Future[None] = loop.create_future()
        done.cancel()
        limiter._waiters.append((1, done))
        limiter._grant()
        assert not limiter._waiters


# ── AsyncChannelPool ─────────────────────────────────────────────────────────


class TestChannelPoolWaiting:
    @pytest.mark.asyncio
    async def test_exhaustion_raises_a_typed_timeout(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1, acquire_timeout=0.02)
        await pool.acquire()
        with pytest.raises(ChannelPoolExhaustedError):
            await pool.acquire()
        assert issubclass(ChannelPoolExhaustedError, TimeoutError)  # old except TimeoutError still works

    @pytest.mark.asyncio
    async def test_per_call_timeout_caps_the_wait(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1, acquire_timeout=30.0)
        await pool.acquire()
        loop = asyncio.get_running_loop()
        start = loop.time()
        with pytest.raises(ChannelPoolExhaustedError):
            await pool.acquire(timeout=0.02)
        assert loop.time() - start < 1.0

    @pytest.mark.asyncio
    async def test_non_positive_budget_fails_without_opening(self) -> None:
        conn = _conn()
        pool = AsyncChannelPool(conn, pool_size=1)
        with pytest.raises(ChannelPoolExhaustedError):
            await pool.acquire(timeout=0)
        conn.channel.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_discarded_channel_wakes_a_waiter_to_open_a_new_one(self) -> None:
        """A freed slot (not only a returned channel) must wake a waiter;
        with the old queue-based wait it slept until its timeout."""
        pool = AsyncChannelPool(_conn(), pool_size=1, acquire_timeout=5.0)
        held = await pool.acquire()
        waiter = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        pool.discard(held)
        fresh = await asyncio.wait_for(waiter, timeout=1.0)
        assert fresh is not held
        assert pool.created_count == 1

    @pytest.mark.asyncio
    async def test_releasing_a_closed_channel_wakes_a_waiter(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1, acquire_timeout=5.0)
        held = await pool.acquire()
        waiter = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        held.is_closed = True
        await pool.release(held)
        assert await asyncio.wait_for(waiter, timeout=1.0) is not held

    @pytest.mark.asyncio
    async def test_a_cancelled_woken_waiter_passes_the_wake_on(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1, acquire_timeout=5.0)
        held = await pool.acquire()
        first = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        second = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        await pool.release(held)  # wakes `first` ...
        first.cancel()  # ... which is cancelled before it runs
        with pytest.raises(asyncio.CancelledError):
            await first
        assert await asyncio.wait_for(second, timeout=1.0) is held

    @pytest.mark.asyncio
    async def test_cancelled_while_waiting_leaves_counts_intact(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1, acquire_timeout=5.0)
        await pool.acquire()
        waiter = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert (pool.created_count, pool.in_use_count) == (1, 1)

    @pytest.mark.asyncio
    async def test_cancelled_while_opening_returns_the_slot(self) -> None:
        conn = AsyncMock()
        gate = asyncio.Event()

        async def slow_channel(**_: Any) -> Any:
            await gate.wait()
            return _channel()  # pragma: no cover

        conn.channel = slow_channel
        pool = AsyncChannelPool(conn, pool_size=1, acquire_timeout=5.0)
        task = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        assert pool.created_count == 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert pool.created_count == 0

    @pytest.mark.asyncio
    async def test_caller_deadline_while_opening_is_admission_not_connection_failure(self) -> None:
        conn = AsyncMock()

        async def slow_channel(**_: Any) -> Any:
            await asyncio.sleep(5)
            return _channel()  # pragma: no cover

        conn.channel = slow_channel
        pool = AsyncChannelPool(conn, pool_size=1, acquire_timeout=5.0)
        with pytest.raises(ChannelPoolExhaustedError):
            await pool.acquire(timeout=0.02)
        assert pool.created_count == 0

    @pytest.mark.asyncio
    async def test_own_open_timeout_still_raises_plain_timeout(self) -> None:
        """The pool's own channel-open bound expiring means a wedged
        connection: a plain TimeoutError, which the connection pool rebuilds on."""
        conn = AsyncMock()

        async def slow_channel(**_: Any) -> Any:
            await asyncio.sleep(5)
            return _channel()  # pragma: no cover

        conn.channel = slow_channel
        pool = AsyncChannelPool(conn, pool_size=1, acquire_timeout=0.02)
        with pytest.raises(TimeoutError) as info:
            await pool.acquire()
        assert not isinstance(info.value, ChannelPoolExhaustedError)


class TestTryAcquireIdle:
    @pytest.mark.asyncio
    async def test_checks_out_an_idle_channel_without_awaiting(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1)
        channel = await pool.acquire()
        await pool.release(channel)
        assert pool.try_acquire_idle() is channel
        assert pool.owns(channel)
        assert pool.try_acquire_idle() is None

    @pytest.mark.asyncio
    async def test_returns_none_on_a_closed_pool(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1)
        await pool.release(await pool.acquire())
        await pool.close_all()
        assert pool.try_acquire_idle() is None

    @pytest.mark.asyncio
    async def test_a_closed_idle_channel_makes_the_next_open_a_rebuild(self) -> None:
        rebuilt = MagicMock()
        stale, fresh = _channel(), _channel()
        pool = AsyncChannelPool(_conn(stale, fresh), pool_size=1, on_channel_rebuilt=rebuilt)
        await pool.release(await pool.acquire())
        stale.is_closed = True
        assert pool.try_acquire_idle() is None
        assert await pool.acquire() is fresh
        rebuilt.assert_called_once()


class TestChannelPoolClose:
    @pytest.mark.asyncio
    async def test_close_all_wakes_waiters_with_closed_error(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1, acquire_timeout=5.0)
        await pool.acquire()
        waiter = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        await pool.close_all()
        with pytest.raises(ChannelPoolClosedError):
            await asyncio.wait_for(waiter, timeout=1.0)
        assert pool.closed
        with pytest.raises(ChannelPoolClosedError):
            await pool.acquire()

    @pytest.mark.asyncio
    async def test_release_after_close_all_is_ignored(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=2)
        held = await pool.acquire()
        await pool.close_all()
        await pool.release(held)
        assert (pool.created_count, pool.size) == (0, 0)

    @pytest.mark.asyncio
    async def test_a_channel_opened_during_close_is_not_handed_out(self) -> None:
        conn = AsyncMock()
        gate = asyncio.Event()
        late = _channel()

        async def slow_channel(**_: Any) -> Any:
            await gate.wait()
            return late

        conn.channel = slow_channel
        pool = AsyncChannelPool(conn, pool_size=1, acquire_timeout=5.0)
        task = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        await pool.close_all()
        gate.set()
        with pytest.raises(ChannelPoolClosedError):
            await task
        await asyncio.sleep(0)
        late.close.assert_awaited_once()
        assert pool.created_count == 0

    @pytest.mark.asyncio
    async def test_release_while_closed_discards_an_owned_channel(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1)
        held = await pool.acquire()
        pool._closed = True  # closing, but the channel is still ours
        await pool.release(held)
        await asyncio.sleep(0)
        assert pool.created_count == 0
        held.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_discard_of_a_foreign_channel_only_closes_it(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1)
        await pool.acquire()
        foreign = _channel()
        pool.discard(foreign)
        await asyncio.sleep(0)
        foreign.close.assert_awaited_once()
        assert pool.created_count == 1

    @pytest.mark.asyncio
    async def test_owns_and_in_use_count(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1)
        held = await pool.acquire()
        assert pool.owns(held)
        assert pool.in_use_count == 1
        await pool.release(held)
        assert not pool.owns(held)


# ── AsyncConnectionPool ──────────────────────────────────────────────────────


def _connection_pool(**pool_kwargs: Any) -> AsyncConnectionPool:
    opened = MagicMock()
    rebuilt = MagicMock()
    return AsyncConnectionPool(
        connection_config=ConnectionConfig(),
        security_config=SecurityConfig(),
        pool_config=PoolConfig(**pool_kwargs),
        on_channel_opened=opened,
        on_channel_rebuilt=rebuilt,
    )


def _patch_connect(conn: Any) -> Any:
    return patch.object(AsyncConnectionPool, "_create_connection", new=AsyncMock(return_value=conn))


class TestExhaustionNeverRebuilds:
    @pytest.mark.asyncio
    async def test_exhausted_pool_waits_then_raises_without_touching_the_connection(self) -> None:
        """G-05: pool exhaustion is capacity, not connection death. The old
        code rebuilt the publisher connection here, closing every in-use
        channel and failing healthy in-flight publishes."""
        conn = _conn()
        pool = _connection_pool(channel_pool_size=1, channel_acquire_timeout=0.05)
        with _patch_connect(conn):
            busy = await pool.acquire_publisher_channel()
            with patch.object(pool, "_rebuild_publisher_connection", new=AsyncMock()) as rebuild:
                with pytest.raises(ChannelPoolExhaustedError):
                    await pool.acquire_publisher_channel()
        rebuild.assert_not_awaited()
        busy.close.assert_not_awaited()
        assert pool._publisher_connection is conn

    @pytest.mark.asyncio
    async def test_mandatory_exhaustion_never_rebuilds_either(self) -> None:
        conn = _conn()
        pool = _connection_pool(channel_pool_size=1, channel_acquire_timeout=0.05)
        with _patch_connect(conn):
            await pool.acquire_mandatory_channel()
            with patch.object(pool, "_rebuild_publisher_connection", new=AsyncMock()) as rebuild:
                with pytest.raises(ChannelPoolExhaustedError):
                    await pool.acquire_mandatory_channel(timeout=0.02)
        rebuild.assert_not_awaited()


class TestRebuildIsGenerationSafe:
    @pytest.mark.asyncio
    async def test_stale_release_after_rebuild_does_not_corrupt_the_new_pool(self) -> None:
        old_conn = _conn()
        new_conn = _conn()
        pool = _connection_pool(channel_pool_size=2)
        with _patch_connect(old_conn):
            stale = await pool.acquire_publisher_channel()
        with _patch_connect(new_conn):
            await pool._rebuild_publisher_connection(stale=old_conn)
            fresh = await pool.acquire_publisher_channel()
        new_pool = pool._publisher_channel_pool
        assert new_pool is not None
        assert new_pool.created_count == 1

        await pool.release_publisher_channel(stale)  # from the old generation

        assert new_pool.created_count == 1  # used to drop to 0 and break the bound
        assert new_pool.owns(fresh)

    @pytest.mark.asyncio
    async def test_rebuilt_pool_keeps_the_metric_hooks(self) -> None:
        old_conn = _conn()
        new_conn = _conn()
        pool = _connection_pool()
        with _patch_connect(old_conn):
            await pool.get_publisher_connection()
        with _patch_connect(new_conn):
            await pool._rebuild_publisher_connection(stale=old_conn)
            await pool.acquire_publisher_channel()
        assert pool._on_channel_opened is not None
        new_pool = pool._publisher_channel_pool
        assert new_pool is not None
        assert new_pool._on_channel_opened is pool._on_channel_opened
        assert new_pool._on_channel_rebuilt is pool._on_channel_rebuilt
        pool._on_channel_opened.assert_called()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_a_waiter_on_a_replaced_pool_retries_on_the_replacement(self) -> None:
        old_conn = _conn()
        new_conn = _conn()
        pool = _connection_pool(channel_pool_size=1, channel_acquire_timeout=5.0)
        with _patch_connect(old_conn):
            await pool.acquire_publisher_channel()
            waiter = asyncio.create_task(pool.acquire_publisher_channel())
            await asyncio.sleep(0)
        with _patch_connect(new_conn):
            await pool._rebuild_publisher_connection(stale=old_conn)
            channel = await asyncio.wait_for(waiter, timeout=1.0)
        assert pool._publisher_channel_pool is not None
        assert pool._publisher_channel_pool.owns(channel)

    @pytest.mark.asyncio
    async def test_a_waiter_woken_mid_rebuild_waits_for_the_replacement(self) -> None:
        old_conn = _conn()
        new_conn = _conn()
        pool = _connection_pool(channel_pool_size=1, channel_acquire_timeout=5.0)

        async def slow_connect(_: Any) -> Any:
            await asyncio.sleep(0.05)
            return new_conn

        with _patch_connect(old_conn):
            await pool.acquire_publisher_channel()
            waiter = asyncio.create_task(pool.acquire_publisher_channel())
            await asyncio.sleep(0)
        with patch.object(AsyncConnectionPool, "_create_connection", new=slow_connect):
            await pool._rebuild_publisher_connection(stale=old_conn)
            channel = await asyncio.wait_for(waiter, timeout=1.0)
        assert pool._publisher_channel_pool is not None
        assert pool._publisher_channel_pool.owns(channel)

    @pytest.mark.asyncio
    async def test_a_waiter_whose_deadline_ends_mid_rebuild_is_admission_timeout(self) -> None:
        old_conn = _conn()
        pool = _connection_pool(channel_pool_size=1, channel_acquire_timeout=5.0)
        release = asyncio.Event()

        async def stuck_connect(_: Any) -> Any:
            await release.wait()
            return _conn()

        with _patch_connect(old_conn):
            await pool.acquire_publisher_channel()
            waiter = asyncio.create_task(pool.acquire_publisher_channel(timeout=0.1))
            await asyncio.sleep(0)
        with patch.object(AsyncConnectionPool, "_create_connection", new=stuck_connect):
            rebuild = asyncio.create_task(pool._rebuild_publisher_connection(stale=old_conn))
            with pytest.raises(ChannelPoolExhaustedError):
                await asyncio.wait_for(waiter, timeout=2.0)
            release.set()
            await rebuild

    @pytest.mark.asyncio
    async def test_closed_for_shutdown_is_not_retried(self) -> None:
        pool = _connection_pool(channel_pool_size=1, channel_acquire_timeout=5.0)
        with _patch_connect(_conn()):
            await pool.acquire_publisher_channel()
            waiter = asyncio.create_task(pool.acquire_publisher_channel())
            await asyncio.sleep(0)
            await pool.close_publisher_channels()
            with pytest.raises(ChannelPoolClosedError):
                await asyncio.wait_for(waiter, timeout=1.0)

    @pytest.mark.asyncio
    async def test_no_pool_available_raises_closed(self) -> None:
        pool = _connection_pool()
        with pytest.raises(ChannelPoolClosedError):
            await pool._acquire_healing(lambda: None, None, "Publisher")

    @pytest.mark.asyncio
    async def test_deadline_spent_rebuilding_is_admission_timeout(self) -> None:
        pool = _connection_pool()
        broken = AsyncMock()
        broken.acquire = AsyncMock(side_effect=RuntimeError("dead"))
        broken.try_acquire_idle = MagicMock(return_value=None)
        broken.closed = False
        pool._publisher_connection = _conn()
        pool._publisher_channel_pool = broken

        async def slow_rebuild(stale: Any) -> None:
            await asyncio.sleep(0.05)

        with patch.object(pool, "_rebuild_publisher_connection", side_effect=slow_rebuild):
            with pytest.raises(ChannelPoolExhaustedError):
                await pool.acquire_publisher_channel(timeout=0.01)

    @pytest.mark.asyncio
    async def test_rebuild_failure_leaving_no_connection_reconnects_lazily(self) -> None:
        pool = _connection_pool()
        broken = AsyncMock()
        broken.acquire = AsyncMock(side_effect=RuntimeError("dead"))
        broken.try_acquire_idle = MagicMock(return_value=None)
        pool._publisher_connection = _conn()
        pool._publisher_channel_pool = broken
        fresh_conn = _conn()

        async def rebuild_drops_everything(stale: Any) -> None:
            pool._publisher_connection = None
            pool._publisher_channel_pool = None

        with patch.object(pool, "_rebuild_publisher_connection", side_effect=rebuild_drops_everything):
            with _patch_connect(fresh_conn):
                channel = await pool.acquire_publisher_channel()
        assert pool._publisher_connection is fresh_conn
        assert pool._publisher_channel_pool is not None
        assert pool._publisher_channel_pool.owns(channel)


class TestDiscardAndClosePublishers:
    @pytest.mark.asyncio
    async def test_discard_frees_the_slot_and_closes(self) -> None:
        pool = _connection_pool(channel_pool_size=1)
        with _patch_connect(_conn()):
            ch = await pool.acquire_publisher_channel()
            pool.discard_publisher_channel(ch)
            await asyncio.sleep(0)
            ch.close.assert_awaited_once()
            mch = await pool.acquire_mandatory_channel()
            pool.discard_mandatory_channel(mch)
            await asyncio.sleep(0)
            mch.close.assert_awaited_once()
        assert pool._publisher_channel_pool is not None
        assert pool._publisher_channel_pool.created_count == 0

    @pytest.mark.asyncio
    async def test_discard_without_a_pool_still_closes(self) -> None:
        pool = _connection_pool()
        ch = _channel()
        pool.discard_publisher_channel(ch)
        await asyncio.sleep(0)
        ch.close.assert_awaited_once()
        closed = _channel(closed=True)
        pool.discard_mandatory_channel(closed)
        closed.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_close_publisher_channels_keeps_the_connection(self) -> None:
        conn = _conn()
        pool = _connection_pool()
        with _patch_connect(conn):
            ch = await pool.acquire_publisher_channel()
            await pool.acquire_mandatory_channel()
            await pool.close_publisher_channels()
        ch.close.assert_awaited_once()
        conn.close.assert_not_awaited()
        assert pool._publisher_connection is conn
        with pytest.raises(ChannelPoolClosedError):
            await pool.acquire_publisher_channel()


class TestCoverageEdges:
    @pytest.mark.asyncio
    async def test_head_waiter_that_still_does_not_fit_keeps_its_place(self) -> None:
        limiter = InFlightByteLimiter(5)
        await limiter.acquire(5, timeout=0.1)
        waiter = asyncio.create_task(limiter.acquire(4, timeout=1.0))
        await asyncio.sleep(0)
        limiter.release(2)  # 3 used: 4 more still does not fit
        await asyncio.sleep(0)
        assert not waiter.done()
        limiter.release(3)
        assert await waiter

    @pytest.mark.asyncio
    async def test_timed_out_waiters_leave_no_dead_entries(self) -> None:
        pool = AsyncChannelPool(_conn(), pool_size=1, acquire_timeout=0.01)
        await pool.acquire()
        for _ in range(3):
            with pytest.raises(ChannelPoolExhaustedError):
                await pool.acquire()
        assert not pool._waiters

    def test_no_mandatory_pool_without_a_connection(self) -> None:
        assert _connection_pool()._current_mandatory_pool() is None
