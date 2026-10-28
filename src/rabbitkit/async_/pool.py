"""Async connection and channel pools.

Minimal in 0.1.0 — internal performance utilities.
Do not oversell as a promised optimization layer.

Uses asyncio.Queue for channel pooling and dedicated
connections for publisher vs consumer separation.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
import logging
import random
import time
from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any

from rabbitkit.async_.connection import (
    get_connection_errors,
    make_aio_pika_connect_kwargs,
    permanent_connection_error,
)
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig
from rabbitkit.core.errors import PermanentConnectionError, TerminalConnectionError

logger = logging.getLogger(__name__)

#: Seconds between "Channel pool exhausted" warnings from one pool.
_EXHAUSTED_WARNING_INTERVAL = 60.0

#: Bound on closing a discarded channel in the background.
_DISCARD_CLOSE_TIMEOUT = 5.0


#: Strong references to fire-and-forget channel closes (see AsyncConnectionPool._discard).
_DETACHED_CLOSES: set[asyncio.Task[None]] = set()


async def _close_quietly(channel: Any) -> None:
    with contextlib.suppress(Exception):
        async with asyncio.timeout(_DISCARD_CLOSE_TIMEOUT):
            await channel.close()


def _close_detached(closable: Any) -> None:
    """Close a channel or connection without waiting for the broker.

    A dead peer never answers Close, so awaiting it blocks for the whole
    bound. aio-pika marks a connection closed (which stops its reconnect loop)
    synchronously at the start of ``close()``, so starting the close is what
    matters; the task itself is bounded and tracked.
    """
    if getattr(closable, "is_closed", True) is True:
        return
    task = asyncio.get_running_loop().create_task(_close_quietly(closable))
    _DETACHED_CLOSES.add(task)
    task.add_done_callback(_DETACHED_CLOSES.discard)


class ChannelPoolExhaustedError(TimeoutError):
    """No pooled channel became free within the admission timeout.

    Capacity, not connection health: the caller reports ``NOT_SENT``
    (``admission_timeout``) and the connection is never rebuilt for it.
    Subclasses ``TimeoutError`` so code that caught the old bare timeout
    keeps working.
    """


class ChannelPoolClosedError(RuntimeError):
    """The pool was closed (shutdown or a connection rebuild) while waiting."""


class InFlightByteLimiter:
    """Async FIFO budget on the sum of in-flight message bytes.

    ``acquire`` waits (cancellable, no polling) until ``n`` bytes fit or the
    timeout expires; ``release`` is synchronous so it can run on every
    terminal path, including a cancelled one. A request larger than the
    whole limit is refused immediately.
    """

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        self._limit = limit
        self._used = 0
        self._closed = False
        self._waiters: collections.deque[tuple[int, asyncio.Future[None]]] = collections.deque()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def used(self) -> int:
        return self._used

    def close(self) -> None:
        """Fail every waiter and later acquire with :class:`ChannelPoolClosedError`."""
        self._closed = True
        while self._waiters:
            _n, fut = self._waiters.popleft()
            if not fut.done():
                fut.set_exception(ChannelPoolClosedError("publisher is closed"))

    async def acquire(self, n: int, timeout: float | None) -> bool:
        """Reserve ``n`` bytes. False when ``n`` exceeds the limit or the wait expires.

        Raises:
            ChannelPoolClosedError: the limiter was closed (publisher shutdown).
        """
        if self._closed:
            raise ChannelPoolClosedError("publisher is closed")
        if n > self._limit:
            return False
        if not self._waiters and self._used + n <= self._limit:
            self._used += n
            return True
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        entry = (n, fut)
        self._waiters.append(entry)
        try:
            async with asyncio.timeout(timeout):
                await fut
        except BaseException as exc:
            if fut.done() and not fut.cancelled() and fut.exception() is None:
                self.release(n)  # granted just as we gave up: hand it back
            else:
                with contextlib.suppress(ValueError):
                    self._waiters.remove(entry)
                self._grant()  # we may have been blocking the head
            if isinstance(exc, TimeoutError):
                return False
            raise
        return True

    def release(self, n: int) -> None:
        """Return ``n`` bytes and wake waiters that now fit (FIFO)."""
        self._used = max(0, self._used - n)
        self._grant()

    def _grant(self) -> None:
        while self._waiters:
            n, fut = self._waiters[0]
            if fut.done():
                self._waiters.popleft()
                continue
            if self._used + n > self._limit:
                return
            self._waiters.popleft()
            self._used += n
            fut.set_result(None)


class AsyncChannelPool:
    """Async channel pool.

    Manages a pool of aio-pika channels on a single connection.
    Channels are acquired and released by callers.

    ``acquire_timeout`` controls how long to wait when all channels are
    checked out. Exhaustion raises :class:`ChannelPoolExhaustedError` (a
    ``TimeoutError``) rather than blocking forever, which would deadlock if
    a handler tries to publish.

    Accounting invariants: ``_created`` counts channels this pool owns
    (idle, checked out, or being opened), never more than ``pool_size``.
    Bookkeeping never awaits, so a cancelled caller cannot leave it
    half-updated. ``release`` only touches channels this pool handed out
    and still owns: a stale release (a channel from before ``close_all`` or
    from a replaced pool) closes the channel and changes nothing else.
    """

    def __init__(
        self,
        connection: Any,  # aio_pika.RobustConnection
        pool_size: int = 10,
        acquire_timeout: float = 10.0,
        publisher_confirms: bool = True,
        on_channel_opened: Callable[[], None] | None = None,
        on_channel_rebuilt: Callable[[], None] | None = None,
        on_return_raises: bool = False,
    ) -> None:
        self._connection = connection
        self._pool_size = pool_size
        self._acquire_timeout = acquire_timeout
        self._publisher_confirms = publisher_confirms
        self._on_return_raises = on_return_raises
        self._exhausted_warned_at = -_EXHAUSTED_WARNING_INTERVAL
        # Idle channels. Only get_nowait/put_nowait are used; waiting is done
        # on ``_waiters`` so a slot freed by a DISCARDED channel wakes a waiter
        # too (it can open a new channel), not only a returned one.
        self._pool: asyncio.Queue[Any] = asyncio.Queue(maxsize=pool_size)
        # Kept for callers that serialise against the pool; bookkeeping
        # itself is synchronous and needs no lock.
        self._lock = asyncio.Lock()
        self._created = 0
        # Channels currently checked out by callers; closed in close_all() so they
        # are not orphaned if a caller forgets to release (leak detection).
        self._in_use: set[Any] = set()
        self._waiters: collections.deque[asyncio.Future[None]] = collections.deque()
        self._closed = False
        # try_acquire_idle() found closed channels: the next opened one is a rebuild.
        self._found_closed_pending = False
        self._background: set[asyncio.Task[None]] = set()
        # Item 3: channel-lifecycle metric hooks (optional, no-op if None).
        self._on_channel_opened = on_channel_opened
        self._on_channel_rebuilt = on_channel_rebuilt

    @property
    def closed(self) -> bool:
        """True once :meth:`close_all` ran; every later acquire raises."""
        return self._closed

    @property
    def in_use_count(self) -> int:
        """Channels currently checked out."""
        return len(self._in_use)

    def owns(self, channel: Any) -> bool:
        """True if *channel* is checked out from this pool right now."""
        return channel in self._in_use

    def _wake_one(self) -> None:
        while self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(None)
                return

    def _wake_all(self) -> None:
        while self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(None)

    def try_acquire_idle(self) -> Any:
        """Check out a live idle channel without awaiting; ``None`` if none is idle.

        The publish hot path: the same first step :meth:`acquire` takes,
        minus two coroutine layers. Closed channels found on the way are
        remembered so the channel :meth:`acquire` opens next counts as a
        rebuild, exactly as if :meth:`acquire` had found them itself.
        """
        if self._closed:
            return None
        channel, found_closed = self._take_idle()
        if found_closed:
            self._found_closed_pending = True
        if channel is not None:
            self._in_use.add(channel)
        return channel

    def _take_idle(self) -> tuple[Any, bool]:
        """Pop a live idle channel. Returns ``(channel | None, found_closed)``."""
        found_closed = False
        while True:
            try:
                channel = self._pool.get_nowait()
            except asyncio.QueueEmpty:
                return None, found_closed
            if not channel.is_closed:
                return channel, found_closed
            # I-6: a pooled channel was found closed — it still counted
            # against _created, so free its slot before discarding it.
            self._created = max(0, self._created - 1)
            found_closed = True

    async def acquire(self, *, timeout: float | None = None, _found_closed: bool = False) -> Any:
        """Acquire a channel from the pool.

        Reuses an idle channel, else opens one while under ``pool_size``,
        else waits (FIFO-ish, cancellable) for a release. ``timeout``
        (default ``acquire_timeout``) bounds the whole call.

        Raises:
            ChannelPoolExhaustedError: no channel became free in time.
            ChannelPoolClosedError: the pool is (or got) closed.
            Exception: opening a new channel failed or timed out (the
                connection is likely dead); the reserved slot is returned.

        ``_found_closed`` is internal: the channel this call opens replaces a
        dead pooled one, so it counts as a rebuild (channel_rebuilds_total).
        """
        budget = self._acquire_timeout if timeout is None else min(timeout, self._acquire_timeout)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + budget
        if self._found_closed_pending:
            self._found_closed_pending = False
            _found_closed = True
        while True:
            if self._closed:
                raise ChannelPoolClosedError("channel pool is closed")
            channel, found_closed = self._take_idle()
            _found_closed = _found_closed or found_closed
            if channel is not None:
                self._in_use.add(channel)
                return channel
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise ChannelPoolExhaustedError(
                    f"Timed out after {budget}s waiting for a channel from the pool (pool_size={self._pool_size})."
                )
            if self._created < self._pool_size:
                caller_bound = timeout is not None and timeout < self._acquire_timeout
                return await self._open_channel(remaining, rebuilt=_found_closed, caller_bound=caller_bound)
            self._log_exhausted()
            fut: asyncio.Future[None] = loop.create_future()
            self._waiters.append(fut)
            try:
                async with asyncio.timeout(remaining):
                    await fut
            except BaseException as exc:
                # Woken, but cancelled or timed out before resuming: pass the
                # wake-up on so the capacity it signalled is not lost.
                if fut.done() and not fut.cancelled():
                    self._wake_one()
                else:
                    with contextlib.suppress(ValueError):
                        self._waiters.remove(fut)  # no dead entries pile up
                if isinstance(exc, TimeoutError):
                    raise ChannelPoolExhaustedError(
                        f"Timed out after {budget}s waiting for a channel from the pool (pool_size={self._pool_size})."
                    ) from None
                raise

    async def _open_channel(self, remaining: float, *, rebuilt: bool, caller_bound: bool) -> Any:
        # perf-M-2: reserve the slot synchronously, open the channel (network
        # round-trip) without holding anything, and give the slot back if the
        # open fails or is cancelled. ``caller_bound``: the caller's own
        # deadline, not the pool's open bound, limits this attempt.
        self._created += 1
        try:
            # Bound channel creation: aio-pika ``RobustConnection.channel()``
            # BLOCKS (does not raise) while the connection is down — no
            # heartbeat has detected the drop yet — waiting for a
            # ``channel.open-ok`` that never arrives. The timeout turns the hang
            # into a raise so ``acquire_publisher_channel`` can rebuild the
            # connection.
            async with asyncio.timeout(min(self._acquire_timeout, remaining)):
                channel = await self._connection.channel(**self._channel_kwargs())
        except BaseException as exc:
            self._created = max(0, self._created - 1)
            self._wake_one()
            if caller_bound and isinstance(exc, TimeoutError):
                # The CALLER's deadline ran out, not the connection: admission
                # timeout, which must not trigger a connection rebuild.
                raise ChannelPoolExhaustedError("publish deadline expired while opening a channel") from exc
            raise
        if self._closed:
            # close_all() ran while we were opening: do not hand it out.
            self._created = max(0, self._created - 1)
            self._close_in_background(channel)
            raise ChannelPoolClosedError("channel pool is closed")
        self._in_use.add(channel)
        if self._on_channel_opened is not None:
            self._on_channel_opened()
        if rebuilt and self._on_channel_rebuilt is not None:
            self._on_channel_rebuilt()
        return channel

    def _log_exhausted(self) -> None:
        # Once a minute at WARNING, then DEBUG: under a burst every waiting
        # publish lands here (2000 concurrent mandatory publishes logged 11874
        # warnings), and a wait that runs out still raises below.
        now = time.monotonic()
        log = logger.debug
        if now - self._exhausted_warned_at >= _EXHAUSTED_WARNING_INTERVAL:
            self._exhausted_warned_at = now
            log = logger.warning
        log(
            "Channel pool exhausted (pool_size=%d, created=%d). "
            "Waiting up to %.1fs for a channel to be released. "
            "Consider increasing PoolConfig.channel_pool_size.",
            self._pool_size,
            self._created,
            self._acquire_timeout,
        )

    def _channel_kwargs(self) -> dict[str, bool]:
        kwargs = {"publisher_confirms": self._publisher_confirms}
        if self._on_return_raises:
            kwargs["on_return_raises"] = True
        return kwargs

    def _close_in_background(self, channel: Any) -> None:
        """Close *channel* without making the caller wait (or be cancellable mid-close)."""
        if getattr(channel, "is_closed", True) is True:
            return
        task = asyncio.get_running_loop().create_task(_close_quietly(channel))
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    def discard(self, channel: Any) -> None:
        """Drop a checked-out channel whose state is unknown (cancelled or
        failed mid-publish): it is never reused, its slot is freed at once,
        and it is closed in the background. Synchronous, so it is safe in a
        cancellation path. A channel this pool does not own is only closed."""
        if channel in self._in_use:
            self._in_use.discard(channel)
            self._created = max(0, self._created - 1)
            self._wake_one()
        self._close_in_background(channel)

    async def release(self, channel: Any) -> None:
        """Release a channel back to the pool.

        Generation-safe: a channel the pool does not currently own (released
        twice, or checked out before :meth:`close_all` / a connection
        rebuild) is closed and otherwise ignored, so it can never decrement
        a newer generation's counter. A closed channel frees its slot.
        """
        if channel not in self._in_use:
            self._close_in_background(channel)
            return
        self._in_use.discard(channel)
        if not channel.is_closed and not self._closed:
            try:
                self._pool.put_nowait(channel)
            except asyncio.QueueFull:  # pragma: no cover — guarded by _created <= pool_size
                pass
            else:
                self._wake_one()
                return
        # Channel is closed (or the pool is) — discard and free the slot.
        self._created = max(0, self._created - 1)
        self._wake_one()
        self._close_in_background(channel)

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
        """Close all channels in the pool, including checked-out ones.

        Marks the pool closed first: waiting acquirers are woken and raise
        :class:`ChannelPoolClosedError`, later acquires raise it at once, and
        later releases of channels handed out before the close are ignored.
        """
        self._closed = True
        self._wake_all()
        in_use = list(self._in_use)
        self._in_use.clear()
        idle: list[Any] = []
        while True:
            try:
                idle.append(self._pool.get_nowait())
            except asyncio.QueueEmpty:
                break
        self._created = 0
        for channel in in_use + idle:
            try:
                if not channel.is_closed:
                    await channel.close()
            except Exception:  # best effort — the connection may already be gone
                pass

    def abandon(self) -> None:
        """:meth:`close_all` without waiting for the broker (a dead connection).

        Same bookkeeping, at once: the pool is closed, waiters are woken with
        :class:`ChannelPoolClosedError`, and every channel's close is started
        in the background instead of awaited.
        """
        self._closed = True
        self._wake_all()
        channels = list(self._in_use)
        self._in_use.clear()
        while True:
            try:
                channels.append(self._pool.get_nowait())
            except asyncio.QueueEmpty:
                break
        self._created = 0
        for channel in channels:
            _close_detached(channel)

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
        on_channel_opened: Callable[[], None] | None = None,
        on_channel_rebuilt: Callable[[], None] | None = None,
        on_connection_created: Callable[[Any], None] | None = None,
        on_reconnect_attempt: Callable[[Any], None] | None = None,
        on_connection_failed: Callable[[TerminalConnectionError], None] | None = None,
        on_connection_state: Callable[[str, bool, BaseException | None], None] | None = None,
    ) -> None:
        self._connection_config = connection_config
        self._security_config = security_config
        self._pool_config = pool_config or PoolConfig()
        self._publisher_confirms = publisher_confirms
        # Item 3: forwarded to every AsyncChannelPool this pool constructs.
        self._on_channel_opened = on_channel_opened
        self._on_channel_rebuilt = on_channel_rebuilt
        # Fired for EVERY connection this pool creates — the initial pair, a
        # lazily re-created one, and a rebuilt publisher connection. The
        # transport uses it to attach its reconnect/blocked callbacks, which
        # previously were registered once in connect() and so were silently
        # lost the moment a connection object was replaced.
        self._on_connection_created = on_connection_created
        # G-08: a failed reconnect attempt (a ReconnectAttempt) and the one
        # terminal failure (TerminalConnectionError) of any connection.
        self._on_reconnect_attempt = on_reconnect_attempt
        self._on_connection_failed = on_connection_failed
        # G-22: ``(role, up, error)`` whenever a connection comes up (created
        # or reconnected) or a live one drops; the metrics follow it.
        self._on_connection_state = on_connection_state
        # Set once a connection gives up for good (permanent failure or spent
        # reconnect budget) after startup. Sticky: every later connect raises
        # it instead of retrying, so the transport stays "not connected".
        self._terminal_error: TerminalConnectionError | None = None
        self._connected_once = False

        self._publisher_connection: Any | None = None
        self._consumer_connection: Any | None = None
        self._publisher_channel_pool: AsyncChannelPool | None = None
        # mandatory=True publishes: confirmed channels that raise on Basic.Return,
        # one publish per channel at a time — see acquire_mandatory_channel()
        self._mandatory_channel_pool: AsyncChannelPool | None = None
        self._lock = asyncio.Lock()
        self._prewarmed = False
        # Bug 2 (F20): a rebuild that cannot connect leaves the publisher
        # connection None; this task restores it in the background (backoff +
        # jitter) so readiness recovers without publish traffic.
        self._restore_task: asyncio.Task[None] | None = None
        self._closing = False

    @property
    def terminal_error(self) -> TerminalConnectionError | None:
        """Why this pool's connections stopped reconnecting for good, else ``None``."""
        return self._terminal_error

    def _record_terminal(self, error: TerminalConnectionError) -> None:
        """A connection gave up for good after startup (G-08): remember it and
        tell the transport. Only the first failure is reported."""
        if self._terminal_error is not None:
            return
        self._terminal_error = error
        if self._on_connection_failed is not None:
            try:
                self._on_connection_failed(error)
            except Exception:  # never let a hook break the reconnect path
                logger.exception("on_connection_failed callback raised")

    def _fire_connection_state(self, role: str, up: bool, error: BaseException | None) -> None:
        if self._on_connection_state is None:
            return
        try:
            self._on_connection_state(role, up, error)
        except Exception:  # never let a hook break a connect or the reconnect path
            logger.exception("on_connection_state callback raised")

    def _connection_lost(self, role: str, error: BaseException | None) -> None:
        self._fire_connection_state(role, False, error)

    def _connection_restored(self, role: str) -> None:
        self._fire_connection_state(role, True, None)

    def channel_counts(self) -> tuple[int, int]:
        """``(owned, in_use)`` publisher channels across both publisher pools.

        *owned* counts channels a pool holds (idle, checked out or opening),
        bounded by ``channel_pool_size`` per pool; *in_use* the checked-out ones.
        """
        owned = in_use = 0
        for pool in (self._publisher_channel_pool, self._mandatory_channel_pool):
            if pool is not None and not pool.closed:
                owned += pool.created_count
                in_use += pool.in_use_count
        return owned, in_use

    async def connect(self) -> None:
        """Establish publisher and consumer connections eagerly.

        All or nothing (G-10): if the second connection fails, the first is
        closed before the error propagates.
        """
        self._closing = False
        try:
            await self._connect_unguarded()
        except BaseException:
            with contextlib.suppress(Exception):
                async with asyncio.timeout(self._connection_config.socket_timeout):
                    await self.close_all()
            raise
        self._connected_once = True

    async def _connect_unguarded(self) -> None:
        do_prewarm = False
        async with self._lock:
            if self._publisher_connection is None:
                self._publisher_connection = await self._create_connection()
                self._publisher_channel_pool = AsyncChannelPool(
                    self._publisher_connection,
                    pool_size=self._pool_config.channel_pool_size,
                    acquire_timeout=self._pool_config.channel_acquire_timeout,
                    publisher_confirms=self._publisher_confirms,
                    on_channel_opened=self._on_channel_opened,
                    on_channel_rebuilt=self._on_channel_rebuilt,
                )
            if self._consumer_connection is None:
                self._consumer_connection = await self._create_connection(role="consumer")
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

        Bounded the same way ``_rebuild_publisher_connection`` bounds its own
        ``_create_connection()`` call: this is also reachable from
        ``acquire_publisher_channel()`` after a rebuild has torn down the
        pool (leaving ``_publisher_channel_pool is None``), so an unbounded
        30-attempt/backoff connect here would defeat the caller's OWN
        capped-backoff retry loop (e.g. ``AsyncBatchPublisher._acquire_channel``)
        by making a single attempt take up to ~10+ minutes instead of failing
        fast enough for that outer loop to actually retry on a sane cadence.
        """
        async with self._lock:
            if self._publisher_connection is None:
                timeout = self._pool_config.channel_acquire_timeout
                self._publisher_connection = await asyncio.wait_for(self._create_connection(), timeout=timeout)
                self._publisher_channel_pool = AsyncChannelPool(
                    self._publisher_connection,
                    pool_size=self._pool_config.channel_pool_size,
                    acquire_timeout=timeout,
                    publisher_confirms=self._publisher_confirms,
                    on_channel_opened=self._on_channel_opened,
                    on_channel_rebuilt=self._on_channel_rebuilt,
                )
            return self._publisher_connection

    async def get_consumer_connection(self) -> Any:
        """Get a connection dedicated for consuming.

        Creates the connection lazily on first call.
        """
        async with self._lock:
            if self._consumer_connection is None:
                self._consumer_connection = await self._create_connection(role="consumer")
            return self._consumer_connection

    async def acquire_publisher_channel(self, *, timeout: float | None = None) -> Any:
        """Acquire a channel from the publisher channel pool.

        ``timeout`` (default ``PoolConfig.channel_acquire_timeout``) bounds the
        wait for a free channel. Pool exhaustion raises
        :class:`ChannelPoolExhaustedError` and does NOT touch the connection:
        a busy pool is not a broken connection, and rebuilding it closed every
        in-flight publish's channel (G-05).

        Self-heals a wedged connection: after a broker outage aio-pika's
        RobustConnection can be left in a state where ``channel()`` hangs (no
        heartbeat has yet detected the drop) or keeps failing even once the
        broker is back. When OPENING a channel fails, the publisher connection
        and its channel pools are rebuilt from scratch and the acquire is
        retried once, so the caller gets a channel on a fresh connection
        rather than looping on a dead one. Nothing has been sent at this
        point, so the retry cannot duplicate a message.
        """
        pool = self._publisher_channel_pool
        if pool is not None:
            channel = pool.try_acquire_idle()
            if channel is not None:
                return channel
        else:
            await self.get_publisher_connection()
        return await self._acquire_healing(self._current_publisher_pool, timeout, "Publisher")

    def _current_publisher_pool(self) -> AsyncChannelPool | None:
        return self._publisher_channel_pool

    def _current_mandatory_pool(self) -> AsyncChannelPool | None:
        if self._publisher_connection is None:
            return None
        return self._mandatory_pool()

    async def _acquire_healing(
        self,
        current_pool: Callable[[], AsyncChannelPool | None],
        timeout: float | None,
        label: str,
    ) -> Any:
        loop = asyncio.get_running_loop()
        budget = self._pool_config.channel_acquire_timeout if timeout is None else timeout
        deadline = loop.time() + budget
        conn = self._publisher_connection
        pool = current_pool()
        if pool is None:
            raise ChannelPoolClosedError(f"{label} channel pool is not available")
        try:
            return await pool.acquire(timeout=budget)
        except ChannelPoolExhaustedError:
            raise
        except ChannelPoolClosedError:
            # Closed under us. A concurrent rebuild replaced it: retry once on
            # the replacement. Closed for shutdown (same pool still current, or
            # none): give up. A rebuild closes the old pool before the new
            # connection exists (the slot is None meanwhile), so wait for an
            # in-progress rebuild to finish before deciding.
            if self._lock.locked():
                try:
                    async with asyncio.timeout(max(deadline - loop.time(), 0.0)), self._lock:
                        pass
                except TimeoutError:
                    raise ChannelPoolExhaustedError(
                        "publish deadline expired while rebuilding the publisher connection"
                    ) from None
            replacement = current_pool()
            if replacement is None or replacement is pool or replacement.closed:
                raise
        except Exception as exc:
            logger.warning("%s channel acquire failed (%s); rebuilding connection", label, exc)
            await self._within(deadline, self._rebuild_publisher_connection(stale=conn))
        if self._publisher_connection is None:
            await self._within(deadline, self.get_publisher_connection())
        retry_pool = current_pool()
        if retry_pool is None:  # pragma: no cover — defensive; a failed rebuild raises above
            raise ChannelPoolClosedError(f"{label} channel pool is not available")
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise ChannelPoolExhaustedError("publish deadline expired while rebuilding the publisher connection")
        return await retry_pool.acquire(timeout=remaining)

    async def _within(self, deadline: float, step: Coroutine[Any, Any, Any]) -> None:
        """Run a reconnect step bounded by the caller's own admission deadline.

        Without this, callers queued behind a reconnect that cannot succeed
        (a partition) resolved one ``channel_acquire_timeout`` after another
        instead of each within its own budget. Running out of time is an
        admission timeout; a reconnect that fails on its own still raises.
        """
        loop = asyncio.get_running_loop()
        bound = asyncio.timeout(max(deadline - loop.time(), 0.0))
        try:
            async with bound:
                await step
        except TimeoutError:
            if bound.expired():
                raise ChannelPoolExhaustedError(
                    "publish deadline expired while reconnecting the publisher connection"
                ) from None
            raise

    def _schedule_publisher_restore(self) -> None:
        """Restore a missing publisher connection in the background (bug 2)."""
        if self._closing or self._terminal_error is not None:
            return
        if self._restore_task is not None and not self._restore_task.done():
            return
        self._restore_task = asyncio.get_running_loop().create_task(
            self._restore_publisher_connection(), name="rabbitkit.publisher-restore"
        )

    async def _restore_publisher_connection(self) -> None:
        backoff = self._connection_config.reconnect_backoff_base
        cap = self._connection_config.reconnect_backoff_max
        while self._publisher_connection is None and not self._closing and self._terminal_error is None:
            await asyncio.sleep(random.uniform(0.0, backoff))  # noqa: S311 - jitter, not crypto
            if self._closing or self._publisher_connection is not None:
                return
            try:
                await self.get_publisher_connection()
            except PermanentConnectionError as exc:
                self._record_terminal(exc)
                return
            except Exception as exc:  # broker still unreachable: back off and retry
                logger.warning("Publisher connection restore failed (%s); retrying", type(exc).__name__)
                backoff = min(backoff * 2, cap)
            else:
                logger.info("Publisher connection restored")

    async def _rebuild_publisher_connection(self, stale: Any) -> None:
        """Replace the (likely dead) publisher connection + channel pool.

        Single-rebuild coordination: N flush workers can fail on the same dead
        connection concurrently, but only the first rebuilds it — the rest see
        ``self._publisher_connection is not stale`` and return, then retry
        ``acquire`` on the fresh pool. Every teardown/create is time-bounded so
        a rebuild attempted while the broker is still down fails fast (the
        caller retries with backoff) instead of hanging on a dead-connection
        close or on ``connect_robust``.

        The replacement pool keeps the channel-lifecycle metric hooks (they
        used to be dropped, so ``channels_opened_total`` stopped counting after
        the first rebuild). Channels still checked out from the old pools are
        closed with it; their later release is ignored by the new pool.
        """
        timeout = self._pool_config.channel_acquire_timeout
        async with self._lock:
            if self._publisher_connection is not stale:
                return  # another worker already rebuilt it
            old_pool, self._publisher_channel_pool = self._publisher_channel_pool, None
            old_mandatory, self._mandatory_channel_pool = self._mandatory_channel_pool, None
            old_conn, self._publisher_connection = self._publisher_connection, None
            # The old connection is presumed dead: closing it (and its
            # channels) in the background keeps a peer that never answers
            # Close from holding the lock, and with it every waiting caller.
            for stale_pool in (old_pool, old_mandatory):
                if stale_pool is not None:
                    stale_pool.abandon()
            if old_conn is not None:
                _close_detached(old_conn)
            try:
                self._publisher_connection = await asyncio.wait_for(self._create_connection(), timeout=timeout)
            except BaseException:
                self._schedule_publisher_restore()
                raise
            self._publisher_channel_pool = AsyncChannelPool(
                self._publisher_connection,
                pool_size=self._pool_config.channel_pool_size,
                acquire_timeout=timeout,
                publisher_confirms=self._publisher_confirms,
                on_channel_opened=self._on_channel_opened,
                on_channel_rebuilt=self._on_channel_rebuilt,
            )

    async def release_publisher_channel(self, channel: Any) -> None:
        """Return a publisher channel to the pool it came from.

        Generation-safe: a channel from a pool that a rebuild has since
        replaced is not counted against the new pool (it is just closed).
        """
        if self._publisher_channel_pool is not None:
            await self._publisher_channel_pool.release(channel)

    def discard_publisher_channel(self, channel: Any) -> None:
        """Drop a publisher channel whose confirm state is unknown (never reused)."""
        self._discard(channel, self._publisher_channel_pool)

    def discard_mandatory_channel(self, channel: Any) -> None:
        """Drop a mandatory-publish channel whose confirm state is unknown."""
        self._discard(channel, self._mandatory_channel_pool)

    @staticmethod
    def _discard(channel: Any, pool: AsyncChannelPool | None) -> None:
        if pool is not None:
            pool.discard(channel)
            return
        if getattr(channel, "is_closed", True) is not True:
            task = asyncio.get_running_loop().create_task(_close_quietly(channel))
            _DETACHED_CLOSES.add(task)
            task.add_done_callback(_DETACHED_CLOSES.discard)

    def _mandatory_pool(self) -> AsyncChannelPool:
        if self._mandatory_channel_pool is None:
            assert self._publisher_connection is not None
            self._mandatory_channel_pool = AsyncChannelPool(
                self._publisher_connection,
                pool_size=self._pool_config.channel_pool_size,
                acquire_timeout=self._pool_config.channel_acquire_timeout,
                publisher_confirms=True,
                on_return_raises=True,
                on_channel_opened=self._on_channel_opened,
                on_channel_rebuilt=self._on_channel_rebuilt,
            )
        return self._mandatory_channel_pool

    async def acquire_mandatory_channel(self, *, timeout: float | None = None) -> Any:
        """A channel for one ``mandatory=True`` publish: confirms on, and
        ``on_return_raises=True`` so an unroutable return raises.

        One publish per channel at a time, like the confirmed pool. A publish
        the broker refuses (403, 404, 406: an unknown exchange, a forged
        ``user_id``) closes its channel, and every operation still in flight
        on that channel fails with it. One shared mandatory channel therefore
        let one refused publish fail every concurrent innocent one. Closed
        channels are discarded on release. Exhaustion and self-healing behave
        exactly as in :meth:`acquire_publisher_channel`.
        """
        if self._publisher_connection is None:
            await self.get_publisher_connection()
        else:
            pool = self._mandatory_channel_pool
            if pool is not None:
                channel = pool.try_acquire_idle()
                if channel is not None:
                    return channel
        return await self._acquire_healing(self._current_mandatory_pool, timeout, "Mandatory")

    async def release_mandatory_channel(self, channel: Any) -> None:
        """Return a mandatory-publish channel to its pool (a closed one is discarded)."""
        if self._mandatory_channel_pool is not None:
            await self._mandatory_channel_pool.release(channel)

    async def close_publisher_channels(self) -> None:
        """Close both publisher channel pools but keep the connections.

        Used by the publisher shutdown: every publish still waiting for a
        channel is released (``ChannelPoolClosedError``), every publish still
        waiting for a confirm sees its channel close, and later acquires fail
        at once, without anything reconnecting.
        """
        for pool in (self._publisher_channel_pool, self._mandatory_channel_pool):
            if pool is not None:
                await pool.close_all()

    async def close_all(self, timeout: float | None = None) -> None:
        """Close all channel pools and connections, within *timeout* seconds.

        Past the timeout (a partition: the broker never answers Close) the
        rest is abandoned: pools are marked closed and the connections' close
        is started in the background, which stops their reconnect loops.
        """
        self._closing = True
        if self._restore_task is not None:
            self._restore_task.cancel()
            self._restore_task = None
        try:
            async with asyncio.timeout(timeout):
                await self._close_all_locked()
        except TimeoutError:
            logger.warning("Closing RabbitMQ connections timed out after %ss; abandoning them", timeout)
            self._abandon_all()

    def _abandon_all(self) -> None:
        for pool in (self._publisher_channel_pool, self._mandatory_channel_pool):
            if pool is not None:
                pool.abandon()
        for conn in (self._publisher_connection, self._consumer_connection):
            if conn is not None:
                _close_detached(conn)
        self._publisher_channel_pool = None
        self._mandatory_channel_pool = None
        self._publisher_connection = None
        self._consumer_connection = None

    async def _close_all_locked(self) -> None:
        async with self._lock:
            if self._publisher_channel_pool is not None:
                await self._publisher_channel_pool.close_all()
                self._publisher_channel_pool = None
            if self._mandatory_channel_pool is not None:
                await self._mandatory_channel_pool.close_all()
                self._mandatory_channel_pool = None

            for conn in [self._publisher_connection, self._consumer_connection]:
                if conn is not None:
                    try:
                        if not conn.is_closed:
                            await conn.close()
                    except Exception as e:
                        logger.warning("Error closing connection: %s", e)

            self._publisher_connection = None
            self._consumer_connection = None

    async def _create_connection(self, *, role: str = "publisher") -> Any:
        """Create a new aio-pika connection.

        G-08: a permanent failure (refused login or vhost, TLS verification)
        raises a :class:`~rabbitkit.core.errors.PermanentConnectionError` at
        once instead of being retried, and the connection object is a
        :class:`~rabbitkit.async_.reconnect.RabbitkitRobustConnection`, so its
        reconnects after a later drop back off exponentially, re-resolve
        credentials and nodes on every attempt, and stop for good on a
        permanent failure or a spent reconnect budget. *role* names the
        connection in hooks and logs (only the consumer connection passes it).
        """
        try:
            import aio_pika
        except ImportError:
            raise ImportError(
                "aio-pika is required for async transport. Install it with: pip install rabbitkit[async]"
            ) from None
        from rabbitkit.async_.reconnect import RabbitkitRobustConnection, ReconnectPolicy

        if self._terminal_error is not None:
            failed = self._terminal_error
            raise type(failed)(str(failed), reason=failed.reason, endpoint=failed.endpoint) from failed

        # M9: cycle through cluster endpoints on the initial connect so a dead
        # configured primary doesn't take the client down at startup. After a
        # later drop the connection itself moves through the endpoints
        # (RabbitkitRobustConnection), starting from the one that worked.
        endpoints = self._connection_config.cluster_endpoints()

        # H-SRE3: an outer retry with full jitter for the INITIAL connect,
        # bounded so we never spin forever. Reconnects after it are driven by
        # the connection's ReconnectPolicy.
        backoff = self._connection_config.reconnect_backoff_base
        max_backoff = self._connection_config.reconnect_backoff_max
        connection_errors = get_connection_errors()
        vhost = self._connection_config.vhost
        max_attempts = 30
        for attempt in range(1, max_attempts + 1):
            index = (attempt - 1) % len(endpoints)
            host, port = endpoints[index]
            kwargs = make_aio_pika_connect_kwargs(
                self._connection_config,
                self._security_config,
                host_override=host,
                port_override=port,
            )
            built: list[Any] = []
            policy = ReconnectPolicy(
                self._connection_config,
                role=role,
                endpoint_index=index,
                on_attempt=self._on_reconnect_attempt,
                on_terminal=self._record_terminal,
                on_instance=built.append,
                on_lost=self._connection_lost,
                on_restored=self._connection_restored,
            )
            kwargs["connection_class"] = functools.partial(
                RabbitkitRobustConnection,
                rk_policy=policy,
                rk_client_properties=kwargs.get("client_properties"),
            )
            try:
                connection = await aio_pika.connect_robust(**kwargs)
            except BaseException as e:
                # aio-pika leaves an abandoned connection's reconnect task
                # running (after a cancelled connect it retries forever in the
                # background): close whatever was built.
                for orphan in built:
                    with contextlib.suppress(Exception):
                        async with asyncio.timeout(self._connection_config.socket_timeout):
                            await orphan.close()
                if not isinstance(e, Exception):
                    raise
                permanent = permanent_connection_error(e, endpoint=f"{host}:{port}", vhost=vhost)
                if permanent is not None:
                    if self._connected_once:
                        permanent.role = role
                        self._record_terminal(permanent)
                    raise permanent from e
                if not isinstance(e, connection_errors) or attempt == max_attempts:
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
            else:
                if self._on_connection_created is not None:
                    try:
                        self._on_connection_created(connection)
                    except Exception:  # pragma: no cover — never fail a connect on a hook
                        logger.exception("on_connection_created callback raised")
                self._fire_connection_state(role, True, None)
                return connection
        raise RuntimeError("unreachable")  # pragma: no cover
