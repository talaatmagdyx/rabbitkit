"""Async connection and channel pools.

Minimal in 0.1.0 — internal performance utilities.
Do not oversell as a promised optimization layer.

Uses asyncio.Queue for channel pooling and dedicated
connections for publisher vs consumer separation.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from rabbitkit.async_.connection import make_aio_pika_connect_kwargs
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig

logger = logging.getLogger(__name__)


class AsyncChannelPool:
    """Async channel pool.

    Manages a pool of aio-pika channels on a single connection.
    Channels are acquired and released by callers.

    ``acquire_timeout`` controls how long to wait when all channels are
    checked out.  Raises ``asyncio.TimeoutError`` on exhaustion rather than
    blocking forever (which would deadlock if a handler tries to publish).
    """

    def __init__(
        self,
        connection: Any,  # aio_pika.RobustConnection
        pool_size: int = 10,
        acquire_timeout: float = 10.0,
        publisher_confirms: bool = True,
    ) -> None:
        self._connection = connection
        self._pool_size = pool_size
        self._acquire_timeout = acquire_timeout
        self._publisher_confirms = publisher_confirms
        self._pool: asyncio.Queue[Any] = asyncio.Queue(maxsize=pool_size)
        self._lock = asyncio.Lock()
        self._created = 0

    async def acquire(self) -> Any:
        """Acquire a channel from the pool.

        Creates a new channel if the pool is empty and under the size limit.
        Waits up to ``acquire_timeout`` seconds when all channels are in use;
        raises ``asyncio.TimeoutError`` if the wait expires, preventing
        deadlocks when handlers publish while processing messages.
        """
        try:
            channel = self._pool.get_nowait()
            if not channel.is_closed:
                return channel
            # Channel is closed, create a new one
        except asyncio.QueueEmpty:
            pass

        async with self._lock:
            if self._created < self._pool_size:
                channel = await self._connection.channel(
                    publisher_confirms=self._publisher_confirms
                )
                self._created += 1
                return channel

        # Pool exhausted — wait with timeout to avoid deadlocks
        logger.warning(
            "Channel pool exhausted (pool_size=%d, created=%d). "
            "Waiting up to %.1fs for a channel to be released. "
            "Consider increasing PoolConfig.channel_pool_size.",
            self._pool_size,
            self._created,
            self._acquire_timeout,
        )
        return await asyncio.wait_for(
            self._pool.get(), timeout=self._acquire_timeout
        )

    async def release(self, channel: Any) -> None:
        """Release a channel back to the pool."""
        if not channel.is_closed:
            try:
                self._pool.put_nowait(channel)
                return
            except asyncio.QueueFull:
                pass
        # Channel is closed or pool is full — discard
        try:
            if not channel.is_closed:
                await channel.close()
        except Exception:
            pass
        async with self._lock:
            self._created = max(0, self._created - 1)

    async def close_all(self) -> None:
        """Close all channels in the pool."""
        while not self._pool.empty():
            try:
                channel = self._pool.get_nowait()
                if not channel.is_closed:
                    await channel.close()
            except (asyncio.QueueEmpty, Exception):
                pass
        async with self._lock:
            self._created = 0

    @property
    def size(self) -> int:
        """Number of channels currently in the pool (available)."""
        return self._pool.qsize()

    @property
    def created_count(self) -> int:
        """Total number of channels created."""
        return self._created


class AsyncConnectionPool:
    """Separate publisher/consumer connections with channel pools.

    Provides dedicated connections for publishing and consuming to avoid
    head-of-line blocking, and exposes ``AsyncChannelPool`` instances so
    callers never share a single channel across concurrent operations.

    Usage::

        pool = AsyncConnectionPool(connection_config, security_config, pool_config)
        await pool.connect()

        async with pool.acquire_publisher_channel() as ch:
            await ch.publish(...)

        await pool.close_all()
    """

    def __init__(
        self,
        connection_config: ConnectionConfig,
        security_config: SecurityConfig,
        pool_config: PoolConfig | None = None,
        publisher_confirms: bool = True,
    ) -> None:
        self._connection_config = connection_config
        self._security_config = security_config
        self._pool_config = pool_config or PoolConfig()
        self._publisher_confirms = publisher_confirms

        self._publisher_connection: Any | None = None
        self._consumer_connection: Any | None = None
        self._publisher_channel_pool: AsyncChannelPool | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Establish publisher and consumer connections eagerly."""
        async with self._lock:
            if self._publisher_connection is None:
                self._publisher_connection = await self._create_connection()
                self._publisher_channel_pool = AsyncChannelPool(
                    self._publisher_connection,
                    pool_size=self._pool_config.channel_pool_size,
                    acquire_timeout=self._pool_config.channel_acquire_timeout,
                    publisher_confirms=self._publisher_confirms,
                )
            if self._consumer_connection is None:
                self._consumer_connection = await self._create_connection()

    async def get_publisher_connection(self) -> Any:
        """Get a connection dedicated for publishing.

        Creates the connection lazily on first call.
        """
        async with self._lock:
            if self._publisher_connection is None:
                self._publisher_connection = await self._create_connection()
                self._publisher_channel_pool = AsyncChannelPool(
                    self._publisher_connection,
                    pool_size=self._pool_config.channel_pool_size,
                    acquire_timeout=self._pool_config.channel_acquire_timeout,
                    publisher_confirms=self._publisher_confirms,
                )
            return self._publisher_connection

    async def get_consumer_connection(self) -> Any:
        """Get a connection dedicated for consuming.

        Creates the connection lazily on first call.
        """
        async with self._lock:
            if self._consumer_connection is None:
                self._consumer_connection = await self._create_connection()
            return self._consumer_connection

    async def acquire_publisher_channel(self) -> Any:
        """Acquire a channel from the publisher channel pool."""
        if self._publisher_channel_pool is None:
            await self.get_publisher_connection()
        assert self._publisher_channel_pool is not None
        return await self._publisher_channel_pool.acquire()

    async def release_publisher_channel(self, channel: Any) -> None:
        """Return a publisher channel to the pool."""
        if self._publisher_channel_pool is not None:
            await self._publisher_channel_pool.release(channel)

    async def close_all(self) -> None:
        """Close all channel pools and connections."""
        async with self._lock:
            if self._publisher_channel_pool is not None:
                await self._publisher_channel_pool.close_all()
                self._publisher_channel_pool = None

            for conn in [self._publisher_connection, self._consumer_connection]:
                if conn is not None:
                    try:
                        if not conn.is_closed:
                            await conn.close()
                    except Exception as e:
                        logger.warning("Error closing connection: %s", e)

            self._publisher_connection = None
            self._consumer_connection = None

    async def _create_connection(self) -> Any:
        """Create a new aio-pika connection."""
        try:
            import aio_pika
        except ImportError:
            raise ImportError(
                "aio-pika is required for async transport. "
                "Install it with: pip install rabbitkit[async]"
            ) from None

        kwargs = make_aio_pika_connect_kwargs(
            self._connection_config,
            self._security_config,
        )
        return await aio_pika.connect_robust(**kwargs)
