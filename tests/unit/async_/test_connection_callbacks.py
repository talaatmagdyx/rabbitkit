"""Reconnect/blocked callbacks must follow every connection the pool creates.

`AsyncTransportImpl.connect()` used to register `reconnect_callbacks` once, on
the initial publisher/consumer pair. Any connection the pool built later — a
rebuilt publisher connection, or a lazily re-created one — carried no
callbacks at all, so `on_reconnect` stopped firing for it: `reconnects_total`
undercounted and any ledger-invalidation wiring hung off that hook silently
stopped running.

The pool now fires `on_connection_created` for EVERY connection it makes, and
a connection created after `connect()` finished is treated as a reconnect.

Known limitation, verified against a live broker and documented in
`docs/observability.md`: when the BROKER closes the connection, aio-pika
9.6 recovers underneath the same `RobustConnection` object without re-running
its counted connect path (`connection_attempt` stays put), so it never fires
`reconnect_callbacks` and rabbitkit never creates a replacement connection.
That case is invisible to `on_reconnect` regardless of this wiring.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.async_.pool import AsyncConnectionPool
from rabbitkit.async_.transport import AsyncTransportImpl
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig


def _pool(**kw: Any) -> AsyncConnectionPool:
    return AsyncConnectionPool(ConnectionConfig(), SecurityConfig(), PoolConfig(), **kw)


def _connection() -> MagicMock:
    conn = MagicMock()
    conn.reconnect_callbacks = MagicMock()
    conn.connection_blocked = MagicMock()
    conn.connection_unblocked = MagicMock()
    return conn


class TestPoolFiresOnConnectionCreated:
    async def test_hook_fires_for_every_created_connection(self) -> None:
        created: list[Any] = []
        pool = _pool(on_connection_created=created.append)
        conns = [_connection(), _connection(), _connection()]
        with patch("aio_pika.connect_robust", new=AsyncMock(side_effect=conns)):
            first = await pool._create_connection()
            second = await pool._create_connection()
            third = await pool._create_connection()
        assert created == [first, second, third]
        assert len({id(c) for c in created}) == 3

    async def test_connection_is_still_returned(self) -> None:
        pool = _pool(on_connection_created=lambda c: None)
        conn = _connection()
        with patch("aio_pika.connect_robust", new=AsyncMock(return_value=conn)):
            assert await pool._create_connection() is conn

    async def test_without_a_hook_nothing_changes(self) -> None:
        pool = _pool()
        conn = _connection()
        with patch("aio_pika.connect_robust", new=AsyncMock(return_value=conn)):
            assert await pool._create_connection() is conn

    async def test_a_raising_hook_never_fails_the_connect(self) -> None:
        def boom(_: Any) -> None:
            raise RuntimeError("callback exploded")

        pool = _pool(on_connection_created=boom)
        conn = _connection()
        with patch("aio_pika.connect_robust", new=AsyncMock(return_value=conn)):
            assert await pool._create_connection() is conn

    async def test_hook_not_fired_when_the_connect_fails(self) -> None:
        created: list[Any] = []
        pool = _pool(on_connection_created=created.append)
        # A non-connection error propagates immediately (the retry ladder only
        # covers connection errors), so this stays fast and deterministic.
        with patch("aio_pika.connect_robust", new=AsyncMock(side_effect=ValueError("bad config"))):
            with pytest.raises(ValueError, match="bad config"):
                await pool._create_connection()
        assert created == []


class TestTransportAttachesCallbacks:
    def _transport(self) -> AsyncTransportImpl:
        return AsyncTransportImpl(connection_config=ConnectionConfig(), security_config=SecurityConfig())

    def test_pool_is_wired_to_the_transport_hook(self) -> None:
        transport = self._transport()
        assert transport._conn_pool._on_connection_created == transport._attach_connection_callbacks

    def test_callbacks_are_registered_on_a_new_connection(self) -> None:
        transport = self._transport()
        conn = _connection()
        transport._attach_connection_callbacks(conn)
        conn.reconnect_callbacks.add.assert_called_once_with(transport._aio_reconnected)
        conn.connection_blocked.add.assert_called_once_with(transport._aio_blocked)
        conn.connection_unblocked.add.assert_called_once_with(transport._aio_unblocked)

    def test_a_connection_created_before_connect_is_not_a_reconnect(self) -> None:
        transport = self._transport()
        fired: list[int] = []
        transport.on_reconnect(lambda: fired.append(1))
        assert transport._connected is False
        transport._attach_connection_callbacks(_connection())
        assert fired == [], "the initial connections must not count as reconnects"

    def test_a_replacement_connection_fires_the_reconnect_hook(self) -> None:
        transport = self._transport()
        fired: list[int] = []
        transport.on_reconnect(lambda: fired.append(1))
        transport._connected = True  # connect() has completed
        transport._attach_connection_callbacks(_connection())
        assert fired == [1], "a connection replaced under us is a reconnect"

    def test_each_replacement_fires_once(self) -> None:
        transport = self._transport()
        fired: list[int] = []
        transport.on_reconnect(lambda: fired.append(1))
        transport._connected = True
        for _ in range(3):
            transport._attach_connection_callbacks(_connection())
        assert fired == [1, 1, 1]

    def test_a_connection_missing_the_collections_is_tolerated(self) -> None:
        """Older/alternative aio-pika builds may not expose them."""
        transport = self._transport()
        bare = MagicMock(spec=[])  # no reconnect_callbacks / blocked / unblocked
        transport._attach_connection_callbacks(bare)  # must not raise

    def test_a_raising_registration_is_tolerated(self) -> None:
        transport = self._transport()
        conn = _connection()
        conn.reconnect_callbacks.add.side_effect = RuntimeError("closed collection")
        transport._attach_connection_callbacks(conn)  # must not raise
        conn.connection_blocked.add.assert_called_once()  # and keeps going

    def test_a_raising_reconnect_callback_never_escapes(self) -> None:
        transport = self._transport()

        def boom() -> None:
            raise RuntimeError("user callback exploded")

        transport.on_reconnect(boom)
        transport._connected = True
        transport._attach_connection_callbacks(_connection())  # must not raise
