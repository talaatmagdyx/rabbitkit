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
from typing import Any

from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig, SocketConfig
from rabbitkit.sync.connection import make_pika_connection_params

logger = logging.getLogger(__name__)


class SyncChannelPool:
    """Thread-safe channel pool.

    Manages a pool of pika channels on a single connection.
    Channels are acquired and released by callers.
    """

    def __init__(
        self,
        connection: Any,  # pika.BlockingConnection
        pool_size: int = 10,
    ) -> None:
        self._connection = connection
        self._pool_size = pool_size
        self._pool: queue.Queue[Any] = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._created = 0

    def acquire(self) -> Any:
        """Acquire a channel from the pool.

        Creates a new channel if the pool is empty and under the size limit.
        Blocks if the pool is exhausted.
        """
        try:
            channel = self._pool.get_nowait()
            if channel.is_open:
                return channel
            # Channel is closed, create a new one
        except queue.Empty:
            pass

        with self._lock:
            if self._created < self._pool_size:
                channel = self._connection.channel()
                self._created += 1
                return channel

        # Pool exhausted — block until one is released
        return self._pool.get()

    def release(self, channel: Any) -> None:
        """Release a channel back to the pool."""
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

    def close_all(self) -> None:
        """Close all channels in the pool."""
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
                "pika is required for sync transport. "
                "Install it with: pip install rabbitkit[sync]"
            ) from None

        params = make_pika_connection_params(
            self._connection_config,
            self._socket_config,
            self._security_config,
        )
        return pika.BlockingConnection(params)
