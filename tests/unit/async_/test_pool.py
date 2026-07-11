"""Tests for async_/pool.py — AsyncChannelPool, AsyncConnectionPool."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from rabbitkit.async_.pool import AsyncChannelPool, AsyncConnectionPool
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig

# ── AsyncChannelPool ────────────────────────────────────────────────────


class TestAsyncChannelPool:
    @pytest.mark.asyncio
    async def test_acquire_creates_channel(self) -> None:
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        ch = await pool.acquire()
        assert ch is mock_channel
        assert pool.created_count == 1
        mock_conn.channel.assert_called_once_with(publisher_confirms=True)  # default: confirms ON

    @pytest.mark.asyncio
    async def test_publisher_confirms_threaded_to_channel(self) -> None:
        """confirm_delivery=False reaches the channel as publisher_confirms=False."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5, publisher_confirms=False)
        await pool.acquire()

        mock_conn.channel.assert_called_once_with(publisher_confirms=False)

    @pytest.mark.asyncio
    async def test_release_returns_to_pool(self) -> None:
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        ch = await pool.acquire()
        assert pool.size == 0

        await pool.release(ch)
        assert pool.size == 1

    @pytest.mark.asyncio
    async def test_acquire_reuses_released_channel(self) -> None:
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        ch1 = await pool.acquire()
        await pool.release(ch1)

        ch2 = await pool.acquire()
        assert ch2 is mock_channel
        # Only one channel created
        assert pool.created_count == 1

    @pytest.mark.asyncio
    async def test_acquire_skips_closed_channel(self) -> None:
        mock_conn = AsyncMock()

        closed_channel = AsyncMock()
        closed_channel.is_closed = True

        open_channel = AsyncMock()
        open_channel.is_closed = False

        mock_conn.channel = AsyncMock(return_value=open_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        # Put a closed channel in the pool
        pool._pool.put_nowait(closed_channel)

        # Should skip closed and create new
        ch = await pool.acquire()
        assert not ch.is_closed

    @pytest.mark.asyncio
    async def test_close_all(self) -> None:
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close = AsyncMock()
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        ch = await pool.acquire()
        await pool.release(ch)

        await pool.close_all()
        assert pool.size == 0
        assert pool.created_count == 0

    @pytest.mark.asyncio
    async def test_release_closed_channel_discards(self) -> None:
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = True
        mock_channel.close = AsyncMock()
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        ch = await pool.acquire()
        await pool.release(ch)

        # Closed channel should not be in pool
        assert pool.size == 0


class TestAsyncChannelPoolLifecycleCallbacks:
    """Item 3: on_channel_opened/on_channel_rebuilt hooks -- opened fires on
    every channel this pool creates; rebuilt fires only when the creation
    replaces a channel discovered closed (not ordinary pool growth)."""

    @pytest.mark.asyncio
    async def test_fresh_growth_fires_opened_not_rebuilt(self) -> None:
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)
        opened: list[int] = []
        rebuilt: list[int] = []

        pool = AsyncChannelPool(
            mock_conn,
            pool_size=5,
            on_channel_opened=lambda: opened.append(1),
            on_channel_rebuilt=lambda: rebuilt.append(1),
        )

        await pool.acquire()

        assert opened == [1]
        assert rebuilt == []

    @pytest.mark.asyncio
    async def test_replacing_closed_pooled_channel_fires_opened_and_rebuilt(self) -> None:
        mock_conn = AsyncMock()
        closed_channel = AsyncMock()
        closed_channel.is_closed = True
        fresh_channel = AsyncMock()
        fresh_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=fresh_channel)
        opened: list[int] = []
        rebuilt: list[int] = []

        pool = AsyncChannelPool(
            mock_conn,
            pool_size=5,
            on_channel_opened=lambda: opened.append(1),
            on_channel_rebuilt=lambda: rebuilt.append(1),
        )
        pool._pool.put_nowait(closed_channel)

        ch = await pool.acquire()

        assert ch is fresh_channel
        assert opened == [1]
        assert rebuilt == [1]

    @pytest.mark.asyncio
    async def test_no_callbacks_is_a_noop(self) -> None:
        """Callbacks are optional -- omitting them must not raise."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        await pool.acquire()  # must not raise


# ── AsyncConnectionPool ────────────────────────────────────────────────


class TestAsyncConnectionPool:
    @pytest.mark.asyncio
    async def test_get_publisher_connection(self) -> None:
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_conn = AsyncMock()
        mock_conn.is_closed = False

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_conn):
                conn = await pool.get_publisher_connection()
                assert conn is not None
                assert conn is mock_conn

    @pytest.mark.asyncio
    async def test_get_publisher_connection_bounded_by_timeout(self) -> None:
        """Batch-outage wedge fix: get_publisher_connection() is reachable
        from acquire_publisher_channel() after a rebuild has torn the pool
        down (leaving _publisher_channel_pool None) -- an unbounded connect
        here would defeat the caller's own capped-backoff retry loop (e.g.
        AsyncBatchPublisher._acquire_channel) by making a single attempt take
        as long as _create_connection()'s own internal 30-attempt retry
        loop (~10+ minutes), instead of failing fast enough for the outer
        loop to actually retry on a sane cadence."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
            pool_config=PoolConfig(channel_acquire_timeout=0.05),
        )

        async def _hangs_forever(**kwargs: Any) -> Any:
            await asyncio.sleep(60.0)  # far longer than channel_acquire_timeout
            raise AssertionError("should have been cancelled by the timeout")

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", side_effect=_hangs_forever):
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(pool.get_publisher_connection(), timeout=2.0)

    @pytest.mark.asyncio
    async def test_acquire_publisher_channel_rebuilds_and_retries_once_on_failure(self) -> None:
        """acquire_publisher_channel() self-heals: a channel-pool failure
        triggers _rebuild_publisher_connection() and one retry against the
        fresh pool, rather than propagating the first failure straight to
        the caller."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        bad_channel_pool = AsyncMock()
        bad_channel_pool.acquire = AsyncMock(side_effect=RuntimeError("dead connection"))
        bad_conn = AsyncMock()
        bad_conn.is_closed = False
        pool._publisher_connection = bad_conn
        pool._publisher_channel_pool = bad_channel_pool

        good_channel = AsyncMock()
        good_conn = AsyncMock()
        good_conn.channel = AsyncMock(return_value=good_channel)

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=good_conn):
                channel = await pool.acquire_publisher_channel()

        assert channel is good_channel
        assert pool._publisher_connection is good_conn
        bad_channel_pool.acquire.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rebuild_publisher_connection_single_rebuild_coordination(self) -> None:
        """_rebuild_publisher_connection(): a caller whose stale reference no
        longer matches self._publisher_connection (another worker already
        rebuilt it) must be a no-op, not a second rebuild."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )
        current_conn = AsyncMock()
        current_conn.is_closed = False
        pool._publisher_connection = current_conn
        pool._publisher_channel_pool = AsyncMock()

        with patch("aio_pika.connect_robust", new_callable=AsyncMock) as mock_connect:
            await pool._rebuild_publisher_connection(stale=object())  # some other, already-replaced connection

        mock_connect.assert_not_awaited()
        assert pool._publisher_connection is current_conn  # untouched

    @pytest.mark.asyncio
    async def test_channel_lifecycle_callbacks_forwarded_to_channel_pool(self) -> None:
        """Item 3: AsyncConnectionPool forwards on_channel_opened/rebuilt to
        every AsyncChannelPool it constructs, so AsyncTransportImpl's
        metric hooks reach channel-pool creations too."""
        opened: list[int] = []
        rebuilt: list[int] = []
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
            on_channel_opened=lambda: opened.append(1),
            on_channel_rebuilt=lambda: rebuilt.append(1),
        )

        mock_conn = AsyncMock()
        mock_conn.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_conn):
                await pool.acquire_publisher_channel()

        assert opened == [1]
        assert rebuilt == []

    @pytest.mark.asyncio
    async def test_publisher_connection_reused(self) -> None:
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_conn = AsyncMock()
        mock_conn.is_closed = False

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_conn) as mock_connect:
                conn1 = await pool.get_publisher_connection()
                conn2 = await pool.get_publisher_connection()
                assert conn1 is conn2
                assert mock_connect.call_count == 1

    @pytest.mark.asyncio
    async def test_separate_pub_consume_connections(self) -> None:
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        conn_a = AsyncMock()
        conn_a.is_closed = False
        conn_b = AsyncMock()
        conn_b.is_closed = False

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, side_effect=[conn_a, conn_b]):
                pub_conn = await pool.get_publisher_connection()
                con_conn = await pool.get_consumer_connection()

                assert pub_conn is not con_conn

    @pytest.mark.asyncio
    async def test_close_all(self) -> None:
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_pub = AsyncMock()
        mock_pub.is_closed = False
        mock_pub.close = AsyncMock()
        mock_con = AsyncMock()
        mock_con.is_closed = False
        mock_con.close = AsyncMock()

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, side_effect=[mock_pub, mock_con]):
                await pool.get_publisher_connection()
                await pool.get_consumer_connection()

                await pool.close_all()

                mock_pub.close.assert_called_once()
                mock_con.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_eager_establishes_both_connections(self) -> None:
        """connect() eagerly creates both publisher and consumer connections."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        conn_a = AsyncMock()
        conn_a.is_closed = False
        conn_b = AsyncMock()
        conn_b.is_closed = False

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, side_effect=[conn_a, conn_b]) as mock_connect:
                await pool.connect()

                # Both connections created eagerly
                assert mock_connect.call_count == 2
                assert pool._publisher_connection is conn_a
                assert pool._consumer_connection is conn_b
                assert pool._publisher_channel_pool is not None

    @pytest.mark.asyncio
    async def test_connect_idempotent(self) -> None:
        """connect() does not create new connections if already connected."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        conn_a = AsyncMock()
        conn_a.is_closed = False
        conn_b = AsyncMock()
        conn_b.is_closed = False

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, side_effect=[conn_a, conn_b]) as mock_connect:
                await pool.connect()
                await pool.connect()  # second call should be no-op
                assert mock_connect.call_count == 2  # same count — no new conns

    @pytest.mark.asyncio
    async def test_acquire_publisher_channel_lazy_init(self) -> None:
        """acquire_publisher_channel() lazily initialises the channel pool."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_conn = AsyncMock()
        mock_conn.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_conn):
                # Pool not yet initialised
                assert pool._publisher_channel_pool is None
                ch = await pool.acquire_publisher_channel()
                assert ch is not None
                assert pool._publisher_channel_pool is not None

    @pytest.mark.asyncio
    async def test_release_publisher_channel_when_pool_is_none(self) -> None:
        """release_publisher_channel is a no-op when pool not yet created."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        # Should not raise even when _publisher_channel_pool is None
        await pool.release_publisher_channel(mock_channel)

    @pytest.mark.asyncio
    async def test_release_publisher_channel_when_pool_exists(self) -> None:
        """release_publisher_channel delegates to the channel pool (line 202)."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_conn = AsyncMock()
        mock_conn.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_conn):
                # This triggers lazy init of _publisher_channel_pool
                ch = await pool.acquire_publisher_channel()
                assert pool._publisher_channel_pool is not None

                # Now release it — should call the pool's release
                await pool.release_publisher_channel(ch)
                # Channel should be back in the pool
                assert pool._publisher_channel_pool.size == 1

    @pytest.mark.asyncio
    async def test_create_connection_raises_import_error_without_aio_pika(self) -> None:
        """_create_connection raises ImportError when aio-pika is not available (lines 226-227)."""
        import sys

        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        with patch.dict(sys.modules, {"aio_pika": None}):
            with pytest.raises(ImportError, match="aio-pika is required"):
                await pool._create_connection()

    @pytest.mark.asyncio
    async def test_close_all_handles_close_exception(self) -> None:
        """close_all() logs and swallows exceptions during connection close."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_pub = AsyncMock()
        mock_pub.is_closed = False
        mock_pub.close = AsyncMock(side_effect=RuntimeError("close failed"))
        mock_con = AsyncMock()
        mock_con.is_closed = False
        mock_con.close = AsyncMock()

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, side_effect=[mock_pub, mock_con]):
                await pool.get_publisher_connection()
                await pool.get_consumer_connection()

                # Should not raise even though pub.close() raises
                await pool.close_all()

        # Both connections should be set to None after close
        assert pool._publisher_connection is None
        assert pool._consumer_connection is None

    @pytest.mark.asyncio
    async def test_close_all_sets_connections_to_none(self) -> None:
        """close_all() sets _publisher_connection and _consumer_connection to None."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_pub = AsyncMock()
        mock_pub.is_closed = False
        mock_pub.close = AsyncMock()
        mock_con = AsyncMock()
        mock_con.is_closed = False
        mock_con.close = AsyncMock()

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}
        ):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, side_effect=[mock_pub, mock_con]):
                await pool.get_publisher_connection()
                await pool.get_consumer_connection()

                assert pool._publisher_connection is not None
                assert pool._consumer_connection is not None

                await pool.close_all()

                assert pool._publisher_connection is None
                assert pool._consumer_connection is None

    @pytest.mark.asyncio
    async def test_create_connection_retries_then_succeeds(self) -> None:
        """Lines 342-353: retry loop sleeps and retries on connection error, succeeds on second attempt."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_conn = AsyncMock()

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs",
            return_value={"url": "amqp://guest:guest@localhost/"},
        ):
            with patch(
                "aio_pika.connect_robust",
                new_callable=AsyncMock,
                side_effect=[ConnectionRefusedError("refused"), mock_conn],
            ):
                with patch("asyncio.sleep", new_callable=AsyncMock):
                    conn = await pool._create_connection()

        assert conn is mock_conn

    @pytest.mark.asyncio
    async def test_create_connection_raises_after_max_attempts(self) -> None:
        """Lines 343-344: re-raises the last error after max_attempts exhausted."""
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        with patch(
            "rabbitkit.async_.pool.make_aio_pika_connect_kwargs",
            return_value={"url": "amqp://guest:guest@localhost/"},
        ):
            with patch(
                "aio_pika.connect_robust",
                new_callable=AsyncMock,
                side_effect=[ConnectionRefusedError("all failed")] * 30,
            ):
                with patch("asyncio.sleep", new_callable=AsyncMock):
                    with pytest.raises(ConnectionRefusedError, match="all failed"):
                        await pool._create_connection()


class TestAsyncChannelPoolExhausted:
    """Tests for channel pool exhaustion and release edge cases."""

    @pytest.mark.asyncio
    async def test_acquire_waits_when_pool_exhausted(self) -> None:
        """When pool is exhausted, acquire waits for a channel to be released."""
        import asyncio

        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=1, acquire_timeout=1.0)

        # Acquire the single channel (pool now at capacity)
        ch = await pool.acquire()
        assert pool.created_count == 1

        # Release the channel back so a second acquire can get it
        asyncio.get_event_loop().call_later(0.05, lambda: asyncio.ensure_future(pool.release(ch)))

        # This should wait and then get the channel back
        ch2 = await pool.acquire()
        assert ch2 is not None

    @pytest.mark.asyncio
    async def test_acquire_raises_timeout_when_pool_exhausted(self) -> None:
        """acquire() raises asyncio.TimeoutError when pool is fully exhausted."""
        import asyncio

        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=1, acquire_timeout=0.05)

        # Acquire the only channel (pool now at capacity, nothing put back)
        _ch = await pool.acquire()

        with pytest.raises(asyncio.TimeoutError):
            await pool.acquire()

    @pytest.mark.asyncio
    async def test_release_discards_when_queue_full(self) -> None:
        """release() discards channel when pool queue is full."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close = AsyncMock()
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        # Create pool with size=1 so queue fills immediately
        pool = AsyncChannelPool(mock_conn, pool_size=1)

        ch = await pool.acquire()
        # Manually fill the queue to capacity
        pool._pool.put_nowait(ch)
        # Now releasing ch again should hit QueueFull path
        # (channel is open but queue is full — it will be closed and discarded)
        another_channel = AsyncMock()
        another_channel.is_closed = False
        another_channel.close = AsyncMock()

        await pool.release(another_channel)
        # The pool should still be at its natural size
        assert pool.size <= 1

    @pytest.mark.asyncio
    async def test_release_discards_when_channel_closed_and_close_raises(self) -> None:
        """release() handles exception when closing a discarded closed channel."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = True  # channel is already closed
        mock_channel.close = AsyncMock(side_effect=RuntimeError("already closed"))
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        # Manually set _created so decrement makes sense
        pool._created = 1

        # Should not raise even when channel.close() raises
        await pool.release(mock_channel)
        assert pool.created_count == 0  # decremented

    @pytest.mark.asyncio
    async def test_release_discards_open_channel_when_close_raises(self) -> None:
        """release() swallows exception when channel is open but close() raises (lines 93-94)."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        # Channel appears open (not closed), but close() raises
        mock_channel.is_closed = False
        mock_channel.close = AsyncMock(side_effect=RuntimeError("close failed"))
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=1)

        # Acquire the only channel to set _created = 1
        ch = await pool.acquire()
        # Fill the pool queue to capacity so put_nowait raises QueueFull
        pool._pool.put_nowait(ch)
        # Reset pool._created so the decrement makes sense
        pool._created = 2

        # Now release another open channel — queue is full, so it tries to close it,
        # but close() raises. Should be silently swallowed.
        another_open_channel = AsyncMock()
        another_open_channel.is_closed = False
        another_open_channel.close = AsyncMock(side_effect=RuntimeError("close error"))

        # Should not raise
        await pool.release(another_open_channel)
        assert pool.created_count == 1  # decremented from 2

    @pytest.mark.asyncio
    async def test_close_all_handles_exception_in_channel_close(self) -> None:
        """close_all() swallows exceptions during channel.close()."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close = AsyncMock(side_effect=RuntimeError("close error"))
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        ch = await pool.acquire()
        await pool.release(ch)

        # Should not raise even when channel.close() raises
        await pool.close_all()
        assert pool.created_count == 0


# ── I-6: acquire() must decrement _created on a closed pooled channel ──────


class TestAsyncChannelPoolCreatedLeak:
    """I-6: pulling a closed channel from the pool must not inflate _created."""

    async def test_acquire_closed_pooled_channel_decrements_created(self) -> None:
        """A closed channel found in the pool is discarded and _created is
        decremented before a fresh channel is created, so the count stays
        consistent (regression for the acquire() _created leak)."""
        from unittest.mock import AsyncMock

        mock_conn = AsyncMock()

        closed_channel = AsyncMock()
        closed_channel.is_closed = True

        fresh_channel = AsyncMock()
        fresh_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=fresh_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        # Pre-load the pool with a closed channel and pretend it was created.
        pool._pool.put_nowait(closed_channel)
        pool._created = 1

        # acquire() pulls the closed channel, decrements _created, then creates
        # a fresh one (incrementing back to 1).
        ch = await pool.acquire()
        assert ch is fresh_channel
        assert pool.created_count == 1  # not 2 — the stale slot was reclaimed
        mock_conn.channel.assert_called_once_with(publisher_confirms=True)

    async def test_acquire_closed_then_open_keeps_created_consistent(self) -> None:
        """Repeated acquire/release with a closed channel in the pool does not
        monotonically grow _created."""
        from unittest.mock import AsyncMock

        mock_conn = AsyncMock()

        closed_channel = AsyncMock()
        closed_channel.is_closed = True

        fresh_channel = AsyncMock()
        fresh_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=fresh_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)
        pool._pool.put_nowait(closed_channel)
        pool._created = 1

        ch1 = await pool.acquire()  # discards closed, creates fresh
        assert pool.created_count == 1
        await pool.release(ch1)  # back in pool, _created stays 1
        assert pool.created_count == 1
        ch2 = await pool.acquire()  # reuses the released open channel
        assert ch2 is fresh_channel
        assert pool.created_count == 1  # no inflation


# ── perf-M-2: channel creation must not happen under the pool lock ──────────


class TestAsyncChannelPoolCreateOutsideLock:
    """perf-M-2: ``acquire()`` creates the AMQP channel OUTSIDE the pool lock so
    concurrent acquires don't serialize on the network round-trip during
    warmup/refill.
    """

    @pytest.mark.asyncio
    async def test_concurrent_acquire_does_not_serialize_on_channel_creation(self) -> None:
        import asyncio
        from unittest.mock import MagicMock

        in_flight = 0
        max_concurrent = 0
        guard = asyncio.Lock()

        async def channel(**kwargs: object) -> Any:
            nonlocal in_flight, max_concurrent
            async with guard:
                in_flight += 1
                max_concurrent = max(max_concurrent, in_flight)
            await asyncio.sleep(0.05)  # simulate a network round-trip
            async with guard:
                in_flight -= 1
            ch = MagicMock()
            ch.is_closed = False
            return ch

        conn = MagicMock()
        conn.channel = channel
        pool = AsyncChannelPool(conn, pool_size=5, acquire_timeout=5.0)

        channels = await asyncio.gather(*[pool.acquire() for _ in range(5)])

        assert len(channels) == 5
        assert pool.created_count == 5
        # Under the lock, creation would be strictly serial (max_concurrent == 1).
        assert max_concurrent > 1, "channel creation was serialized under the pool lock"

    @pytest.mark.asyncio
    async def test_acquire_creation_failure_returns_reserved_slot(self) -> None:
        """If channel() raises, the reserved _created slot is returned so the
        pool doesn't permanently lose capacity."""
        from unittest.mock import MagicMock

        calls = 0

        async def channel(**kwargs: object) -> Any:
            nonlocal calls
            calls += 1
            raise ConnectionError("broker refused channel")

        conn = MagicMock()
        conn.channel = channel
        pool = AsyncChannelPool(conn, pool_size=3, acquire_timeout=0.2)

        with pytest.raises(ConnectionError):
            await pool.acquire()

        # The reserved slot was returned — created_count is back to 0, so a later
        # acquire can still try (it will fail again, but won't deadlock on a
        # phantom slot).
        assert pool.created_count == 0
        assert calls == 1


# ── prewarm_channels ──────────────────────────────────────────────────────────


class TestPrewarmChannels:
    @pytest.mark.asyncio
    async def test_prewarm_creates_and_releases_all_channels(self) -> None:
        """prewarm_channels=True acquires all pool channels during connect() so
        the first batch of publishers never pays channel-creation latency."""
        from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig

        pool_size = 4
        channel_count = 0

        async def make_channel(**kwargs: object) -> Any:
            nonlocal channel_count
            channel_count += 1
            ch = AsyncMock()
            ch.is_closed = False
            return ch

        conn = AsyncMock()
        conn.channel = make_channel
        conn.is_closed = False

        pool_cfg = PoolConfig(channel_pool_size=pool_size, prewarm_channels=True)

        with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=conn):
            cp = AsyncConnectionPool(
                ConnectionConfig(),
                SecurityConfig(),
                pool_config=pool_cfg,
            )
            await cp.connect()

        # All pool_size channels were created during prewarm
        assert channel_count == pool_size
        # After prewarm + release, the pool queue is full (channels returned)
        assert cp._publisher_channel_pool is not None
        assert cp._publisher_channel_pool.size == pool_size

    @pytest.mark.asyncio
    async def test_prewarm_idempotent_on_double_connect(self) -> None:
        """Calling connect() twice with prewarm_channels=True must NOT double-create channels."""
        from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig

        pool_size = 3
        channel_count = 0

        async def make_channel(**kwargs: object) -> Any:
            nonlocal channel_count
            channel_count += 1
            ch = AsyncMock()
            ch.is_closed = False
            return ch

        conn = AsyncMock()
        conn.channel = make_channel
        conn.is_closed = False

        pool_cfg = PoolConfig(channel_pool_size=pool_size, prewarm_channels=True)

        with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=conn):
            cp = AsyncConnectionPool(
                ConnectionConfig(),
                SecurityConfig(),
                pool_config=pool_cfg,
            )
            await cp.connect()
            first_count = channel_count
            await cp.connect()  # second call — must be a no-op for prewarm

        assert channel_count == first_count == pool_size


# ── Lines 122-124: stale channel pulled from pool after exhaustion wait ──────


class TestAsyncChannelPoolStaleAfterWait:
    """Lines 122-124: when acquire() waits for a channel (pool exhausted) and
    then gets a *closed* channel from the queue, it must decrement _created and
    recurse so the caller gets a live channel."""

    @pytest.mark.asyncio
    async def test_acquire_stale_channel_after_exhaustion_wait_recurses(self) -> None:
        """Pool exhausted → waits → receives a closed channel → discards it and
        creates a fresh one via the recursive acquire() call."""
        import asyncio

        mock_conn = AsyncMock()

        closed_channel = AsyncMock()
        closed_channel.is_closed = True

        fresh_channel = AsyncMock()
        fresh_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=fresh_channel)

        # pool_size=1 so the pool hits the exhausted-wait path immediately after
        # the first acquire uses the single slot.
        pool = AsyncChannelPool(mock_conn, pool_size=1, acquire_timeout=2.0)

        # Acquire the only slot so _created == pool_size.
        first_ch = await pool.acquire()
        assert pool.created_count == 1

        # Schedule: release the closed_channel back into the pool shortly, then
        # release the real first channel so the recursive acquire() can succeed.
        async def release_sequence() -> None:
            await asyncio.sleep(0.02)
            # Put the *closed* channel into the queue — this is what the waiting
            # acquire() will pick up first.
            pool._pool.put_nowait(closed_channel)
            # Allow _created to reflect the slot being "returned" so the
            # recursive acquire() can create a new one.
            async with pool._lock:
                pool._created -= 1
            # Give the recursive acquire() a moment, then release the first
            # channel so the recursive call gets a slot.
            await asyncio.sleep(0.02)
            await pool.release(first_ch)

        release_task = asyncio.create_task(release_sequence())

        ch = await pool.acquire()
        await release_task

        assert ch is fresh_channel
        assert not ch.is_closed


# ── Lines 157-161: acquire_ctx() async context manager ───────────────────────


class TestAsyncChannelPoolAcquireCtx:
    """Lines 157-161: acquire_ctx() must acquire, yield, and release the channel."""

    @pytest.mark.asyncio
    async def test_acquire_ctx_yields_channel_and_releases(self) -> None:
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        async with pool.acquire_ctx() as ch:
            assert ch is mock_channel
            # Channel should be in _in_use while held
            assert mock_channel in pool._in_use

        # After the context exits, channel should be released back to the pool
        assert mock_channel not in pool._in_use
        assert pool.size == 1

    @pytest.mark.asyncio
    async def test_acquire_ctx_releases_on_exception(self) -> None:
        """acquire_ctx() must still release the channel even when the body raises."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        with pytest.raises(ValueError, match="boom"):
            async with pool.acquire_ctx() as ch:
                assert ch is mock_channel
                raise ValueError("boom")

        # Channel still released despite the exception
        assert mock_channel not in pool._in_use
        assert pool.size == 1


# ── Lines 169-175: close_all() for in-use channels ───────────────────────────


class TestAsyncChannelPoolCloseAllInUse:
    """Lines 169-175: close_all() must close channels that are currently
    checked out (tracked in _in_use), not just idle ones in the queue."""

    @pytest.mark.asyncio
    async def test_close_all_closes_in_use_channels(self) -> None:
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close = AsyncMock()
        mock_conn.channel = AsyncMock(return_value=mock_channel)

        pool = AsyncChannelPool(mock_conn, pool_size=5)

        # Acquire but do NOT release — the channel stays in _in_use
        ch = await pool.acquire()
        assert ch is mock_channel
        assert mock_channel in pool._in_use

        await pool.close_all()

        # close() must have been called on the in-use channel
        mock_channel.close.assert_called_once()
        # _in_use must be cleared and _created reset
        assert pool._in_use == set()
        assert pool.created_count == 0
