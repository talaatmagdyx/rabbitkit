"""Tests for async_/pool.py — AsyncChannelPool, AsyncConnectionPool."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from rabbitkit.async_.pool import AsyncChannelPool, AsyncConnectionPool
from rabbitkit.core.config import ConnectionConfig, SecurityConfig

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

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_conn):
                conn = await pool.get_publisher_connection()
                assert conn is not None
                assert conn is mock_conn

    @pytest.mark.asyncio
    async def test_publisher_connection_reused(self) -> None:
        pool = AsyncConnectionPool(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        mock_conn = AsyncMock()
        mock_conn.is_closed = False

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
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

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
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

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
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

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
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

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
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

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
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

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
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

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
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

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, side_effect=[mock_pub, mock_con]):
                await pool.get_publisher_connection()
                await pool.get_consumer_connection()

                assert pool._publisher_connection is not None
                assert pool._consumer_connection is not None

                await pool.close_all()

                assert pool._publisher_connection is None
                assert pool._consumer_connection is None


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
