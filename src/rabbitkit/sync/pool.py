"""Sync connection and channel pools.

Minimal in 0.1.0 — internal performance utilities.
Do not oversell as a promised optimization layer.

Model A: one-connection-per-thread. Pools enforce this by assigning
dedicated connections per role (publisher vs consumer).
"""

from __future__ import annotations

import logging
import queue
import threading
from contextlib import contextmanager
from typing import Any

from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig, SocketConfig
from rabbitkit.sync.connection import make_pika_connection_params

logger = logging.getLogger(__name__)


class SyncChannelPool:
    """Thread-safe channel pool.

    Manages a pool of pika channels on a single connection.
    Channels are acquired and released by callers.

    ``acquire_timeout`` bounds how long ``acquire()`` blocks when all channels
    are checked out; it raises ``TimeoutError`` on expiry (mirrors the async
    pool) rather than blocking forever — which would deadlock a worker that
    tries to publish while processing.

    Note: the default ``SyncTransport`` does not route publishes through this
    pool (it uses a single dedicated publisher channel); the pool is kept as a
    reusable utility for advanced/pooled usage and is covered by unit tests so
    it is not silently dead code.
    """

    def __init__(
        self,
        connection: Any,  # pika.BlockingConnection
        pool_size: int = 10,
        acquire_timeout: float = 10.0,
    ) -> None:
        self._connection = connection
        self._pool_size = pool_size
        self._acquire_timeout = acquire_timeout
        self._pool: queue.Queue[Any] = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._created = 0
        # Channels currently checked out by callers (leak detection / close_all).
        self._in_use: set[Any] = set()

    def acquire(self) -> Any:
        """Acquire a channel from the pool.

        Creates a new channel if the pool is empty and under the size limit.
        Blocks up to ``acquire_timeout`` if the pool is exhausted; raises
        ``TimeoutError`` on expiry so callers don't deadlock.
        """
        try:
            channel = self._pool.get_nowait()
            if channel.is_open:
                with self._lock:
                    self._in_use.add(channel)
                return channel
            # I-6: a pooled channel was found closed — it still counted
            # against _created when it was released, so decrement before
            # discarding it (otherwise acquire() leaks a slot each time a
            # closed-idle channel is pulled from the pool).
            with self._lock:
                self._created = max(0, self._created - 1)
        except queue.Empty:
            pass

        # perf-M-2: create the channel OUTSIDE the lock (network round-trip) so
        # concurrent acquire() calls don't serialize on channel creation during
        # warmup/refill. The slot is reserved atomically under the lock (so we
        # never over-create); the channel-open I/O happens outside the lock, and
        # we re-acquire only to publish _in_use. If creation fails, the reserved
        # slot is returned. The I-6 closed-idle decrement above stays under the
        # lock.
        with self._lock:
            if self._created < self._pool_size:
                need_create = True
                self._created += 1
            else:
                need_create = False

        if need_create:
            try:
                channel = self._connection.channel()
            except BaseException:
                with self._lock:
                    self._created = max(0, self._created - 1)
                raise
            with self._lock:
                self._in_use.add(channel)
                return channel

        # Pool exhausted — block until one is released, bounded by acquire_timeout
        logger.warning(
            "SyncChannelPool exhausted (pool_size=%d, created=%d). "
            "Waiting up to %.1fs for a channel to be released. "
            "Consider increasing PoolConfig.channel_pool_size.",
            self._pool_size,
            self._created,
            self._acquire_timeout,
        )
        try:
            channel = self._pool.get(timeout=self._acquire_timeout)
        except queue.Empty as e:
            raise TimeoutError(
                f"Timed out after {self._acquire_timeout}s waiting for a pooled "
                "channel. Increase PoolConfig.channel_pool_size."
            ) from e
        if not channel.is_open:
            # Discard the stale channel and recurse to try again.
            with self._lock:
                self._created = max(0, self._created - 1)
            return self.acquire()
        with self._lock:
            self._in_use.add(channel)
        return channel

    def release(self, channel: Any) -> None:
        """Release a channel back to the pool."""
        with self._lock:
            self._in_use.discard(channel)
        if channel.is_open:
            try:
                self._pool.put_nowait(channel)
                return
            except queue.Full:
                pass
        # Channel is closed or pool is full — discard
        try:
            if channel.is_open:
                channel.close()
        except Exception:
            pass
        with self._lock:
            self._created = max(0, self._created - 1)

    @contextmanager
    def acquire_ctx(self) -> Any:
        """Context manager for acquire/release — prevents leaks."""
        channel = self.acquire()
        try:
            yield channel
        finally:
            self.release(channel)

    def close_all(self) -> None:
        """Close all channels in the pool (including checked-out ones)."""
        with self._lock:
            in_use = list(self._in_use)
            self._in_use.clear()
        for channel in in_use:
            try:
                if channel.is_open:
                    channel.close()
            except Exception:  # pragma: no cover — best effort
                pass
            with self._lock:
                self._created = max(0, self._created - 1)
        while not self._pool.empty():
            try:
                channel = self._pool.get_nowait()
                if channel.is_open:
                    channel.close()
            except (queue.Empty, Exception):
                pass
        with self._lock:
            self._created = 0

    @property
    def size(self) -> int:
        """Number of channels currently in the pool (available)."""
        return self._pool.qsize()

    @property
    def created_count(self) -> int:
        """Total number of channels created."""
        return self._created


class SyncConnectionPool:
    """Separate publisher/consumer connections.

    Provides dedicated connections for publishing and consuming
    to avoid head-of-line blocking.
    """

    def __init__(
        self,
        connection_config: ConnectionConfig,
        socket_config: SocketConfig,
        security_config: SecurityConfig,
        pool_config: PoolConfig | None = None,
    ) -> None:
        self._connection_config = connection_config
        self._socket_config = socket_config
        self._security_config = security_config
        self._pool_config = pool_config or PoolConfig()

        self._publisher_connections: list[Any] = []
        self._consumer_connections: list[Any] = []
        self._lock = threading.Lock()

    def get_publisher_connection(self) -> Any:
        """Get a connection dedicated for publishing.

        Creates the connection lazily on first call.
        """
        with self._lock:
            if not self._publisher_connections:
                conn = self._create_connection()
                self._publisher_connections.append(conn)
            return self._publisher_connections[0]

    def get_consumer_connection(self) -> Any:
        """Get a connection dedicated for consuming.

        Creates the connection lazily on first call.
        """
        with self._lock:
            if not self._consumer_connections:
                conn = self._create_connection()
                self._consumer_connections.append(conn)
            return self._consumer_connections[0]

    def close_all(self) -> None:
        """Close all connections."""
        with self._lock:
            for conn in self._publisher_connections + self._consumer_connections:
                try:
                    if conn.is_open:
                        conn.close()
                except Exception as e:
                    logger.warning("Error closing connection: %s", e)

            self._publisher_connections.clear()
            self._consumer_connections.clear()

    def _create_connection(self) -> Any:
        """Create a new pika connection."""
        try:
            import pika
        except ImportError:
            raise ImportError(
                "pika is required for sync transport. Install it with: pip install rabbitkit[sync]"
            ) from None

        params = make_pika_connection_params(
            self._connection_config,
            self._socket_config,
            self._security_config,
        )
        return pika.BlockingConnection(params)
