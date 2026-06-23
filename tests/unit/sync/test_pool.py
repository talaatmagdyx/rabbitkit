"""Tests for sync/pool.py — SyncChannelPool, SyncConnectionPool."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.core.config import ConnectionConfig, SecurityConfig, SocketConfig
from rabbitkit.sync.pool import SyncChannelPool, SyncConnectionPool

# ── SyncChannelPool ──────────────────────────────────────────────────────


class TestSyncChannelPool:
    def test_acquire_creates_channel(self) -> None:
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel

        pool = SyncChannelPool(mock_conn, pool_size=5)

        ch = pool.acquire()
        assert ch is mock_channel
        assert pool.created_count == 1

    def test_release_returns_to_pool(self) -> None:
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel

        pool = SyncChannelPool(mock_conn, pool_size=5)

        ch = pool.acquire()
        assert pool.size == 0

        pool.release(ch)
        assert pool.size == 1

    def test_acquire_reuses_released_channel(self) -> None:
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel

        pool = SyncChannelPool(mock_conn, pool_size=5)

        ch1 = pool.acquire()
        pool.release(ch1)

        ch2 = pool.acquire()
        assert ch2 is mock_channel
        # Only one channel created
        assert pool.created_count == 1

    def test_acquire_skips_closed_channel(self) -> None:
        mock_conn = MagicMock()

        closed_channel = MagicMock()
        closed_channel.is_open = False

        open_channel = MagicMock()
        open_channel.is_open = True

        mock_conn.channel.return_value = open_channel

        pool = SyncChannelPool(mock_conn, pool_size=5)

        # Put a closed channel in the pool
        pool._pool.put_nowait(closed_channel)

        # Should skip closed and create new
        ch = pool.acquire()
        assert ch.is_open

    def test_close_all(self) -> None:
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel

        pool = SyncChannelPool(mock_conn, pool_size=5)

        ch = pool.acquire()
        pool.release(ch)

        pool.close_all()
        assert pool.size == 0
        assert pool.created_count == 0

    def test_release_closed_channel_discards(self) -> None:
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = False
        mock_conn.channel.return_value = mock_channel

        pool = SyncChannelPool(mock_conn, pool_size=5)

        ch = pool.acquire()
        pool.release(ch)

        # Closed channel should not be in pool
        assert pool.size == 0


# ── SyncConnectionPool ──────────────────────────────────────────────────


class TestSyncConnectionPool:
    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def test_get_publisher_connection(self) -> None:
        pool = SyncConnectionPool(
            connection_config=ConnectionConfig(),
            socket_config=SocketConfig(),
            security_config=SecurityConfig(),
        )

        with patch("pika.BlockingConnection") as mock_conn:
            mock_conn.return_value.is_open = True

            conn = pool.get_publisher_connection()
            assert conn is not None

    def test_publisher_connection_reused(self) -> None:
        pool = SyncConnectionPool(
            connection_config=ConnectionConfig(),
            socket_config=SocketConfig(),
            security_config=SecurityConfig(),
        )

        with patch("pika.BlockingConnection") as mock_conn:
            mock_conn.return_value.is_open = True

            conn1 = pool.get_publisher_connection()
            conn2 = pool.get_publisher_connection()
            assert conn1 is conn2

    def test_separate_pub_consume_connections(self) -> None:
        pool = SyncConnectionPool(
            connection_config=ConnectionConfig(),
            socket_config=SocketConfig(),
            security_config=SecurityConfig(),
        )

        with patch("pika.BlockingConnection") as mock_conn:
            # Return different mocks for each call
            conn_a = MagicMock()
            conn_b = MagicMock()
            conn_a.is_open = True
            conn_b.is_open = True
            mock_conn.side_effect = [conn_a, conn_b]

            pub_conn = pool.get_publisher_connection()
            con_conn = pool.get_consumer_connection()

            assert pub_conn is not con_conn

    def test_close_all(self) -> None:
        pool = SyncConnectionPool(
            connection_config=ConnectionConfig(),
            socket_config=SocketConfig(),
            security_config=SecurityConfig(),
        )

        with patch("pika.BlockingConnection") as mock_conn:
            mock_pub = MagicMock()
            mock_pub.is_open = True
            mock_con = MagicMock()
            mock_con.is_open = True
            mock_conn.side_effect = [mock_pub, mock_con]

            pool.get_publisher_connection()
            pool.get_consumer_connection()

            pool.close_all()

            mock_pub.close.assert_called_once()
            mock_con.close.assert_called_once()

    def test_close_all_handles_exception(self) -> None:
        """close_all() logs and continues if conn.close() raises."""
        pool = SyncConnectionPool(
            connection_config=ConnectionConfig(),
            socket_config=SocketConfig(),
            security_config=SecurityConfig(),
        )

        with patch("pika.BlockingConnection") as mock_conn:
            mock_pub = MagicMock()
            mock_pub.is_open = True
            mock_pub.close.side_effect = RuntimeError("close failed")
            mock_conn.return_value = mock_pub

            pool.get_publisher_connection()

            # Should not raise even though close() fails
            pool.close_all()

    def test_create_connection_raises_without_pika(self) -> None:
        """_create_connection() raises ImportError if pika is not installed."""
        pool = SyncConnectionPool(
            connection_config=ConnectionConfig(),
            socket_config=SocketConfig(),
            security_config=SecurityConfig(),
        )
        import builtins
        real_import = builtins.__import__

        def import_blocker(name: str, *args, **kwargs):
            if name == "pika":
                raise ImportError("No module named 'pika'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=import_blocker):
            with pytest.raises(ImportError, match="pika is required"):
                pool._create_connection()


# ── SyncChannelPool — additional edge cases ──────────────────────────────


class TestSyncChannelPoolEdgeCases:
    """Additional edge cases for SyncChannelPool."""

    def test_acquire_blocks_when_pool_exhausted(self) -> None:
        """acquire() blocks on pool.get() when pool is exhausted and at limit.

        This exercises line 62: `return self._pool.get()` by ensuring:
        - get_nowait() raises queue.Empty (pool is empty)
        - _created == pool_size (can't create more channels)
        - A background thread puts a channel back into the pool so the
          blocking get() eventually returns.
        """
        import threading

        mock_conn = MagicMock()
        pool = SyncChannelPool(mock_conn, pool_size=1)

        # Pre-set _created == pool_size so no new channel is created in the lock block
        pool._created = 1

        ready_channel = MagicMock()
        ready_channel.is_open = True

        # Pool is empty at this point, so get_nowait() will raise Empty.
        # A background thread will push the channel into the queue shortly
        # after acquire() starts blocking on self._pool.get().
        def release_after_delay() -> None:
            import time
            time.sleep(0.05)
            pool._pool.put_nowait(ready_channel)

        t = threading.Thread(target=release_after_delay, daemon=True)
        t.start()

        result = pool.acquire()

        t.join(timeout=2.0)
        assert result is ready_channel

    def test_release_discards_when_pool_full(self) -> None:
        """release() discards the channel when pool is full (queue.Full)."""
        mock_conn = MagicMock()
        pool = SyncChannelPool(mock_conn, pool_size=1)

        open_ch1 = MagicMock()
        open_ch1.is_open = True
        open_ch2 = MagicMock()
        open_ch2.is_open = True

        # Fill pool to capacity
        pool._pool.put_nowait(open_ch1)
        pool._created = 2  # pretend two channels exist

        # Releasing ch2 when pool is full should discard it
        # Since ch2.is_open=True, put_nowait raises Full, then we try to close it
        # but since is_open=True we close it, and decrement _created
        pool.release(open_ch2)

        # open_ch2 should have been closed since pool was full
        open_ch2.close.assert_called_once()

    def test_release_open_channel_close_exception_ignored(self) -> None:
        """release() ignores exceptions when closing a discarded channel."""
        mock_conn = MagicMock()
        pool = SyncChannelPool(mock_conn, pool_size=1)

        open_ch1 = MagicMock()
        open_ch1.is_open = True
        open_ch2 = MagicMock()
        open_ch2.is_open = True
        open_ch2.close.side_effect = RuntimeError("close failed")

        # Fill pool to trigger discard
        pool._pool.put_nowait(open_ch1)
        pool._created = 2

        # Should not raise even though close() fails
        pool.release(open_ch2)

    def test_close_all_ignores_exception_on_channel_close(self) -> None:
        """close_all() ignores exceptions when closing channels."""
        mock_conn = MagicMock()
        pool = SyncChannelPool(mock_conn, pool_size=5)

        bad_channel = MagicMock()
        bad_channel.is_open = True
        bad_channel.close.side_effect = RuntimeError("fail")
        pool._pool.put_nowait(bad_channel)
        pool._created = 1

        # Should not raise
        pool.close_all()
        assert pool.created_count == 0
