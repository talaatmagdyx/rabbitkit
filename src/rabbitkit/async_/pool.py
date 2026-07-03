"""Async connection and channel pools.

Minimal in 0.1.0 — internal performance utilities.
Do not oversell as a promised optimization layer.

Uses asyncio.Queue for channel pooling and dedicated
connections for publisher vs consumer separation.
"""

from __future__ import annotations

import asyncio
import logging
import random
from contextlib import asynccontextmanager
from typing import Any

from rabbitkit.async_.connection import get_connection_errors, make_aio_pika_connect_kwargs
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
        # Channels currently checked out by callers; closed in close_all() so they
        # are not orphaned if a caller forgets to release (leak detection).
        self._in_use: set[Any] = set()

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
                async with self._lock:
                    self._in_use.add(channel)
                return channel
            # I-6: a pooled channel was found closed — it still counted
            # against _created when it was released, so decrement before
            # discarding it (otherwise acquire() leaks a slot each time a
            # closed-idle channel is pulled from the pool).
            async with self._lock:
                self._created = max(0, self._created - 1)
        except asyncio.QueueEmpty:
            pass

        # perf-M-2: create the channel OUTSIDE the lock (network round-trip) so
        # concurrent acquire() calls don't serialize on channel creation during
        # warmup/refill. The slot is reserved atomically under the lock (so we
        # never over-create); the channel-open I/O happens outside the lock, and
        # we re-acquire only to publish _in_use. If creation fails, the reserved
        # slot is returned. The I-6 closed-idle decrement above stays under the
        # lock.
        async with self._lock:
            if self._created < self._pool_size:
                need_create = True
                self._created += 1
            else:
                need_create = False

        if need_create:
            try:
                channel = await self._connection.channel(publisher_confirms=self._publisher_confirms)
            except BaseException:
                # creation failed — give the reserved slot back.
                async with self._lock:
                    self._created = max(0, self._created - 1)
                raise
            async with self._lock:
                self._in_use.add(channel)
                return channel

        # Pool exhausted — wait with timeout to avoid deadlocks. R-timeout:
        # ``asyncio.timeout`` (3.11+) replaces ``asyncio.wait_for`` to avoid
        # the wrapper-task overhead.
        logger.warning(
            "Channel pool exhausted (pool_size=%d, created=%d). "
            "Waiting up to %.1fs for a channel to be released. "
            "Consider increasing PoolConfig.channel_pool_size.",
            self._pool_size,
            self._created,
            self._acquire_timeout,
        )
        try:
            async with asyncio.timeout(self._acquire_timeout):
                channel = await self._pool.get()
        except TimeoutError:
            raise TimeoutError(
                f"Timed out after {self._acquire_timeout}s waiting for a channel "
                f"from the pool (pool_size={self._pool_size})."
            ) from None
        if channel.is_closed:
            async with self._lock:
                self._created = max(0, self._created - 1)
            return await self.acquire()
        async with self._lock:
            self._in_use.add(channel)
        return channel

    async def release(self, channel: Any) -> None:
        """Release a channel back to the pool."""
        async with self._lock:
            self._in_use.discard(channel)
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

    @asynccontextmanager
    async def acquire_ctx(self) -> Any:
        """Async context manager for acquire/release — prevents leaks.

        Usage::

            async with pool.acquire_ctx() as ch:
                await ch.publish(...)
        """
        channel = await self.acquire()
        try:
            yield channel
        finally:
            await self.release(channel)

    async def close_all(self) -> None:
        """Close all channels in the pool, including checked-out ones."""
        async with self._lock:
            in_use = list(self._in_use)
            self._in_use.clear()
        for channel in in_use:
            try:
                if not channel.is_closed:
                    await channel.close()
            except Exception:  # pragma: no cover — best effort
                pass
            async with self._lock:
                self._created = max(0, self._created - 1)
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
        self._prewarmed = False

    async def connect(self) -> None:
        """Establish publisher and consumer connections eagerly."""
        do_prewarm = False
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
            if self._pool_config.prewarm_channels and not self._prewarmed:
                self._prewarmed = True
                do_prewarm = True

        if do_prewarm and self._publisher_channel_pool is not None:
            pool = self._publisher_channel_pool
            channels = await asyncio.gather(
                *(pool.acquire() for _ in range(self._pool_config.channel_pool_size)),
                return_exceptions=True,
            )
            for ch in channels:
                if not isinstance(ch, BaseException):
                    await pool.release(ch)

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
                "aio-pika is required for async transport. Install it with: pip install rabbitkit[async]"
            ) from None

        # M9: cycle through cluster endpoints on the initial connect so a dead
        # configured primary doesn't take the client down at startup. Once
        # connect_robust succeeds it pins to that node for reconnects (aio-pika
        # has no multi-host reconnect) — put a load balancer / DNS in front for
        # per-reconnect failover across nodes.
        endpoints = self._connection_config.cluster_endpoints()

        # H-SRE3: connect_robust handles reconnects AFTER the first connection
        # with a FIXED interval, so a fleet of clients starting at once thunder
        # the broker. Apply an outer retry with full jitter for the INITIAL
        # connect only; bounded so we never spin forever.
        backoff = self._connection_config.reconnect_backoff_base
        max_backoff = self._connection_config.reconnect_backoff_max
        connection_errors = get_connection_errors()
        max_attempts = 30
        for attempt in range(1, max_attempts + 1):
            host, port = endpoints[(attempt - 1) % len(endpoints)]
            kwargs = make_aio_pika_connect_kwargs(
                self._connection_config,
                self._security_config,
                host_override=host,
                port_override=port,
            )
            try:
                return await aio_pika.connect_robust(**kwargs)
            except connection_errors as e:
                if attempt == max_attempts:
                    raise
                sleep_for = random.uniform(0.0, backoff)  # noqa: S311
                logger.warning(
                    "aio-pika initial connect failed (attempt %d), retrying in %.2fs: %s",
                    attempt,
                    sleep_for,
                    e,
                )
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, max_backoff)
        raise RuntimeError("unreachable")  # pragma: no cover
