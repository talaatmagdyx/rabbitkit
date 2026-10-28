"""AsyncTransport — aio-pika transport adapter.

Uses aio_pika.connect_robust() for auto-reconnection.
aio-pika owns connection/channel/consumer restoration natively.

Architecture:
- Publisher connection: dedicated connection + AsyncChannelPool for concurrent
  publishes without blocking consumer channels or topology operations.
- Consumer connection: dedicated connection for all subscribe operations.
  Each queue gets its own channel (required for per-queue QoS).
- Topology connection: reuses consumer connection for declare/bind operations.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any

from rabbitkit.async_.connection import get_connection_errors
from rabbitkit.async_.consumers import ConsumerRecord, ConsumerSupervisor
from rabbitkit.async_.pool import (
    AsyncConnectionPool,
    ChannelPoolClosedError,
    ChannelPoolExhaustedError,
    InFlightByteLimiter,
)
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig
from rabbitkit.core.errors import ConfigurationError, TerminalConnectionError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.topology_dispatch import TopoAction, TopologyDispatcher
from rabbitkit.core.types import (
    DIRECT_REPLY_TO_QUEUE,
    MessageEnvelope,
    PublishOutcome,
    PublishReason,
    PublishStatus,
    TopologyMode,
    safe_exception_repr,
)

if TYPE_CHECKING:
    from rabbitkit.async_.reconnect import ReconnectAttempt

logger = logging.getLogger(__name__)

# How long a publish waits for aio-pika's own connect_robust() to restore a
# reconnecting publisher connection BEFORE sending (bounded further by the
# per-call timeout). Expiry reports NOT_SENT(not_connected).
_PUBLISH_RECOVERY_WAIT_SECONDS = 5.0

# Publisher shutdown: how long close_publishing() waits for force-closed
# publishes to unwind, and bounds each channel close it does.
_SHUTDOWN_UNWIND_SECONDS = 1.0

# Outcomes after which an exclusive pooled channel is still safe to reuse:
# the broker answered this publish and the channel stayed open.
_REUSABLE_STATUSES = frozenset(
    {PublishStatus.CONFIRMED, PublishStatus.SENT, PublishStatus.NACKED, PublishStatus.RETURNED}
)
# Outcomes that are a broker answer to a confirmed publish (confirm latency).
_BROKER_ANSWERS = frozenset({PublishStatus.CONFIRMED, PublishStatus.NACKED, PublishStatus.RETURNED})


def _is_connection_error(exc: BaseException) -> bool:
    """True if *exc* looks like a dead connection/channel, not a message- or
    broker-decision-level failure (item 6). Reuses the same tuple
    ``sync/transport.py``'s reconnect loop classifies against — unlike
    ``core/pipeline.py``'s ``_is_channel_gone`` (name-matched, since core/
    cannot import aio-pika), this file already depends on aio-pika directly.
    """
    return isinstance(exc, get_connection_errors())


@functools.cache
def _basic_return_type() -> type[Any]:
    from pamqp.commands import Basic

    return Basic.Return


def _is_basic_return(frame: Any) -> bool:
    """True if *frame* is an AMQP ``Basic.Return`` (never true for a mock).

    Called on every confirmed publish: the pamqp import is resolved once."""
    return isinstance(frame, _basic_return_type())


def _return_fields(exc: BaseException) -> tuple[int | None, str | None]:
    """``(reply_code, reply_text)`` of the ``Basic.Return`` behind an
    ``aio_pika.exceptions.PublishError`` (its ``frame``, or the returned
    message's ``delivery``)."""
    for frame in (getattr(exc, "frame", None), getattr(getattr(exc, "message", None), "delivery", None)):
        if _is_basic_return(frame):
            return getattr(frame, "reply_code", None), getattr(frame, "reply_text", None)
    return None, None


def _exact_expiration_message_class() -> Any:
    """An ``aio_pika.Message`` that sends ``expiration`` exactly as given.

    aio-pika (9.x and 10.x) takes expiration in float seconds and encodes it
    with ``int()`` truncation, so some values lose a millisecond on the wire:
    "1001" ms -> 1.001 s -> "1000". A replayed or retried message must keep
    the TTL it had, so this puts the millisecond string back verbatim.
    Built lazily so importing this module never needs aio-pika.
    """
    import aio_pika

    base = aio_pika.Message
    if not isinstance(base, type):  # patched out (tests): nothing to subclass
        return base
    return _exact_expiration_subclass(base)


@functools.cache
def _exact_expiration_subclass(base: type[Any]) -> type[Any]:
    class _ExactExpirationMessage(base):  # type: ignore[misc]
        __slots__ = ("_expiration_ms",)

        def __init__(self, body: bytes, *, expiration_ms: str | None, **kwargs: Any) -> None:
            super().__init__(body, **kwargs)
            self._expiration_ms = expiration_ms

        @property
        def properties(self) -> Any:
            properties = super().properties
            properties.expiration = self._expiration_ms
            return properties

    return _ExactExpirationMessage


def _connection_live(conn: Any) -> bool:
    """True when an aio-pika connection has a live AMQP connection under it."""
    try:
        if bool(getattr(conn, "is_closed", False)):
            return False
        connected = getattr(conn, "connected", None)
        is_set = getattr(connected, "is_set", None)
        if callable(is_set) and not is_set():
            return False  # dropped; RobustConnection is reconnecting
    except Exception:  # pragma: no cover — defensive
        return False
    return True


class AsyncTransportImpl:
    """aio-pika-based async transport adapter.

    Uses connect_robust() for transparent auto-reconnect.
    Publisher and consumer traffic run on separate connections to prevent
    head-of-line blocking and QoS interference.
    """

    def __init__(
        self,
        connection_config: ConnectionConfig | None = None,
        security_config: SecurityConfig | None = None,
        pool_config: PoolConfig | None = None,
        topology_mode: TopologyMode = TopologyMode.AUTO_DECLARE,
        confirm_delivery: bool = True,
        confirm_timeout: float = 5.0,
        on_topology_conflict: str = "raise",
        max_in_flight_bytes: int | None = None,
    ) -> None:
        self._connection_config = connection_config or ConnectionConfig()
        self._security_config = security_config or SecurityConfig()
        self._pool_config = pool_config or PoolConfig()
        self._topology_mode = topology_mode
        self._topo = TopologyDispatcher(topology_mode)
        # M14: "raise" | "warn_continue" on a 406 topology-drift conflict.
        self._on_topology_conflict = on_topology_conflict
        self._confirm_delivery = confirm_delivery
        # Per-publish timeout when publisher confirms are enabled. Without this a
        # broker that never confirms would block the publish coroutine forever;
        # on timeout we release/close the channel and return PublishStatus.TIMEOUT.
        self._confirm_timeout = float(confirm_timeout)
        # G-14: PublisherConfig.max_in_flight_bytes, shared by publish() and
        # the batch publisher (None = no byte bound).
        self._byte_limiter: InFlightByteLimiter | None = (
            InFlightByteLimiter(max_in_flight_bytes) if max_in_flight_bytes is not None else None
        )
        # G-12 publisher shutdown state (see close_publishing).
        self._publish_closed = False
        self._publish_shutdown = False
        self._publishes_in_flight = 0
        self._publishes_idle: asyncio.Event | None = None

        self._conn_pool = AsyncConnectionPool(
            self._connection_config,
            self._security_config,
            self._pool_config,
            publisher_confirms=self._confirm_delivery,
            on_channel_opened=self._fire_channel_opened,
            on_channel_rebuilt=self._fire_channel_rebuilt,
            on_connection_created=self._attach_connection_callbacks,
            on_reconnect_attempt=self._fire_reconnect_attempt,
            on_connection_failed=self._fire_connection_failed,
            on_connection_state=self._fire_connection_state,
        )
        self._connected = False

        # Per-queue consumer channels: queue_name -> aio_pika channel
        self._consumer_channels: dict[str, Any] = {}
        # Per-(channel, message_id) in-flight publishes — see
        # _publish_on_channel. aiormq correlates a Basic.Return to its publish
        # by message_id and DROPS that mapping when an earlier publish of the
        # same id on the same channel confirms; re-publishing an id while its
        # previous publish is unconfirmed makes the second publish's Return
        # unhandled and the publish resolve as CONFIRMED (a false success).
        self._inflight_message_ids: dict[tuple[int, str], asyncio.Future[None]] = {}
        self._consumer_tags: dict[str, str] = {}  # queue_name -> consumer_tag
        # G-26 / CONSUMER-007: the queue object each consumer was started on
        # (cancel goes through it so robust restore forgets the consumer) and
        # detection + re-subscription of consumers the SERVER cancelled.
        self._consumer_supervisor = ConsumerSupervisor(self.queue_exists)

        # The channel currently consuming DIRECT_REPLY_TO_QUEUE (set by
        # consume(declare=False), cleared on cancel/disconnect). RabbitMQ's
        # direct reply-to requires the reply consumer and the corresponding
        # request publish to happen on the SAME channel (a publish on a
        # different channel raises "PRECONDITION_FAILED - fast reply consumer
        # does not exist") — publish() checks this to route RPC requests
        # correctly without RPCClient needing to know about channels at all.
        self._reply_to_channel: Any = None

        # Shared topology channel (consumer connection)
        self._topology_channel: Any | None = None

        # Persistent no-confirm publish channel — reused across all fire-and-forget
        # publishes when confirm_delivery=False. Eliminates per-publish pool
        # acquire/release overhead; a single channel handles concurrent writes
        # safely because aio-pika serialises AMQP frames at the connection level.
        self._fast_publish_channel: Any | None = None
        self._fast_channel_lock: asyncio.Lock = asyncio.Lock()

        # H1: mandatory=True publishes use the connection pool's mandatory
        # channels (acquire_mandatory_channel): always confirmed and
        # on_return_raises=True, independent of confirm_delivery, one publish
        # per channel so a refused publish fails only itself.
        # m3: bindings recorded for re-apply after robust reconnect (they are
        # not in RobustChannel's restoration registry — see bind_queue).
        self._recorded_bindings: list[tuple[str, str, str, str, dict[str, Any] | None]] = []
        self._binding_restore_tasks: set[Any] = set()

        # Backpressure callbacks (FlowController registers here). Each is a
        # zero-arg callable; aio-pika's blocked/unblocked frames are adapted.
        self._blocked_callbacks: list[Callable[[], None]] = []
        self._unblocked_callbacks: list[Callable[[], None]] = []

        # Connection-churn metric hook (see on_reconnect) -- adapted from
        # aio-pika's RobustConnection.reconnect_callbacks.
        self._reconnect_callbacks: list[Callable[[], None]] = []
        # G-08: failed reconnect attempts and the terminal connection failure.
        self._reconnect_attempt_callbacks: list[Callable[[ReconnectAttempt], None]] = []
        self._connection_failed_callbacks: list[Callable[[TerminalConnectionError], None]] = []
        # G-22: connection up/down per role, and publish outcomes with the
        # send-to-broker-answer latency (metrics hooks).
        self._connection_state_callbacks: list[Callable[[str, bool, BaseException | None], None]] = []
        self._publish_outcome_callbacks: list[Callable[[PublishOutcome, float | None], None]] = []

        # Channel-lifecycle signal (item 3), mirroring SyncTransport:
        # channels_opened_total counts every new channel THIS transport
        # explicitly creates (aio-pika restores the topology/consumer
        # channels it owns transparently on reconnect -- those restores are
        # invisible here by design, see the module docstring). rebuilt is the
        # subset that replaces a channel lost to a 406 topology conflict or a
        # mandatory/fast-channel recycle.
        self._channel_opened_callbacks: list[Callable[[], None]] = []
        self._channel_rebuilt_callbacks: list[Callable[[], None]] = []

        # L15: passive blocked-state tracking, independent of whether a
        # FlowController is registered above -- health.broker_health_check
        # reads this (via the is_blocked property) so a broker/disk/memory
        # alarm is visible even when the caller never opted into FlowController.
        self._blocked_state: bool = False
        # One BlockedConnectionMonitor per connection the pool creates. They
        # are the only source of blocked state on async: aio-pika has no
        # blocked callbacks (see async_/connection.py).
        self._blocked_monitors: list[Any] = []

    def on_reconnect(self, callback: Callable[[], None]) -> None:
        """Register a callback fired on every ``connect_robust`` re-connection
        (connection-churn metric hook). Reconnects were logged but never
        counted, so a flapping broker/network was invisible to metrics
        alerting."""
        self._reconnect_callbacks.append(callback)

    def on_reconnect_attempt(self, callback: Callable[[ReconnectAttempt], None]) -> None:
        """Register a callback fired after every FAILED reconnect attempt of a
        connection that had been up (G-08).

        It receives a :class:`~rabbitkit.async_.reconnect.ReconnectAttempt`:
        the attempt number within the outage, the backoff delay chosen before
        the next attempt, the time since the connection was lost, the endpoint
        and the error. A successful reconnect fires :meth:`on_reconnect`.
        """
        self._reconnect_attempt_callbacks.append(callback)

    def on_connection_failed(self, callback: Callable[[TerminalConnectionError], None]) -> None:
        """Register a callback fired once when a connection stops reconnecting
        for good after startup (G-08): a permanent failure (refused login or
        vhost, TLS verification) or a spent reconnect budget. From then on
        :meth:`is_connected` is False, :attr:`connection_failure` holds the
        error, and no connect is attempted again.
        """
        self._connection_failed_callbacks.append(callback)

    @property
    def connection_failure(self) -> TerminalConnectionError | None:
        """The terminal connection failure, if a connection gave up for good."""
        return self._conn_pool.terminal_error

    def _fire_reconnect_attempt(self, attempt: ReconnectAttempt) -> None:
        for cb in list(self._reconnect_attempt_callbacks):
            try:
                cb(attempt)
            except Exception:  # never break the reconnect path
                logger.exception("reconnect_attempt callback raised")

    def _fire_connection_failed(self, error: TerminalConnectionError) -> None:
        for cb in list(self._connection_failed_callbacks):
            try:
                cb(error)
            except Exception:  # never break the reconnect path
                logger.exception("connection_failed callback raised")

    def on_connection_state(self, callback: Callable[[str, bool, BaseException | None], None]) -> None:
        """Register ``callback(role, up, error)`` for connection state changes (G-22).

        ``role`` is ``"publisher"`` or ``"consumer"``. Fired with ``up=True``
        when a connection is created or reconnects after a drop, and with
        ``up=False`` (and the close error, if any) the moment a live
        connection drops. A connection that gives up for good fires
        :meth:`on_connection_failed` instead.
        """
        self._connection_state_callbacks.append(callback)

    def _fire_connection_state(self, role: str, up: bool, error: BaseException | None) -> None:
        for cb in list(self._connection_state_callbacks):
            try:
                cb(role, up, error)
            except Exception:  # never break the connect/reconnect path
                logger.exception("connection_state callback raised")

    def on_publish_outcome(self, callback: Callable[[PublishOutcome, float | None], None]) -> None:
        """Register ``callback(outcome, seconds)`` for every publish whose
        frame was handed to aio-pika (G-22).

        ``seconds`` is the time from handing the frame to aio-pika until the
        broker answered (confirm, nack or return), or ``None`` when no broker
        answer was observed (fire-and-forget ``SENT``, timeout, connection
        loss, refusal). It excludes admission, channel acquisition and
        serialization. Outcomes decided before anything was sent
        (``NOT_SENT``) are not reported here. Covers every transport publish:
        ``publish()``, the batch publisher, retry and dead-letter copies.
        """
        self._publish_outcome_callbacks.append(callback)

    def _report_publish(self, outcome: PublishOutcome, seconds: float | None) -> None:
        for cb in self._publish_outcome_callbacks:
            try:
                cb(outcome, seconds)
            except Exception:  # never let a metrics hook fail a publish
                logger.exception("publish_outcome callback raised")

    def connection_states(self) -> dict[str, bool]:
        """``{"publisher": up, "consumer": up}`` right now (sampled by metrics)."""
        terminal = self._conn_pool.terminal_error is not None
        states: dict[str, bool] = {}
        for role, conn in (
            ("publisher", self._conn_pool._publisher_connection),
            ("consumer", self._conn_pool._consumer_connection),
        ):
            states[role] = bool(self._connected and not terminal and conn is not None and _connection_live(conn))
        return states

    @property
    def channels_active(self) -> int:
        """Channels this transport holds open right now: the publisher and
        mandatory pools' channels, the topology and fast-publish channels and
        one per consumed queue. Bounded by ``2 * channel_pool_size + 2 +
        consumed queues``."""
        owned, _in_use = self._conn_pool.channel_counts()
        count = owned
        for channel in (self._topology_channel, self._fast_publish_channel, *self._consumer_channels.values()):
            if channel is not None and not bool(getattr(channel, "is_closed", False)):
                count += 1
        return count

    @property
    def publish_channels_in_use(self) -> int:
        """Publisher channels currently checked out of the pools."""
        return self._conn_pool.channel_counts()[1]

    def on_channel_opened(self, callback: Callable[[], None]) -> None:
        """Register a callback fired every time this transport creates a new
        channel (channels_opened_total metric hook)."""
        self._channel_opened_callbacks.append(callback)

    def on_channel_rebuilt(self, callback: Callable[[], None]) -> None:
        """Register a callback fired when a channel is created to REPLACE
        one lost to a 406 topology conflict or a mandatory/fast-channel
        recycle (channel_rebuilds_total metric hook) — a subset of
        :meth:`on_channel_opened`."""
        self._channel_rebuilt_callbacks.append(callback)

    def _fire_channel_opened(self) -> None:
        for cb in list(self._channel_opened_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover — never break the event loop
                logger.exception("channel_opened callback raised")

    def _fire_channel_rebuilt(self) -> None:
        for cb in list(self._channel_rebuilt_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover — never break the event loop
                logger.exception("channel_rebuilt callback raised")

    def _attach_connection_callbacks(self, connection: Any) -> None:
        """Register the reconnect/blocked hooks on a freshly created connection.

        Called by the pool for EVERY connection it builds. Registering only
        the initial pair (as connect() used to) meant a rebuilt or lazily
        re-created connection carried no callbacks, so ``on_reconnect`` never
        fired again — ``reconnects_total`` stopped counting and any
        ledger-invalidation wiring hung off it silently stopped running.
        ``CallbackCollection`` is set-like and bound methods compare equal, so
        re-registering the same connection is harmless.

        A connection created AFTER :meth:`connect` finished is a REPLACEMENT
        for one we were already using, so it fires the reconnect callbacks
        itself. aio-pika only fires ``reconnect_callbacks`` when a
        ``RobustConnection`` reconnects IN PLACE; when rabbitkit swaps the
        connection object instead (``_rebuild_publisher_connection``, or a
        lazy re-create after a close) the new object's first connect is not a
        "reconnect" to aio-pika — so without this, ``on_reconnect`` never
        fired for that very common path and ``reconnects_total`` undercounted
        to zero.
        """
        target = getattr(connection, "reconnect_callbacks", None)
        if target is not None:
            try:
                adder = getattr(target, "add", None) or getattr(target, "add_callback", None)
                if adder is not None:
                    adder(self._aio_reconnected)
            except Exception:  # pragma: no cover — never fail a connect on a hook
                logger.debug("Could not register reconnect_callbacks on a new connection")
        self._start_blocked_monitor(connection)
        if self._connected:
            logger.info("Replacement AMQP connection created — treating it as a reconnect")
            self._aio_reconnected()

    def _aio_reconnected(self, *_args: Any) -> None:
        for cb in list(self._reconnect_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover - never break the event loop
                logger.exception("reconnect callback raised")
        # m3: re-apply recorded bindings — RobustChannel restores queues,
        # exchanges, and consumers, but NOT bindings made through
        # get_queue/get_exchange(ensure=False) handles. Binding is idempotent
        # server-side, so re-applying on every reconnect is safe.
        if self._recorded_bindings:
            with contextlib.suppress(RuntimeError):  # no running loop at teardown
                task = asyncio.get_running_loop().create_task(self._reapply_bindings())
                self._binding_restore_tasks.add(task)
                task.add_done_callback(self._binding_restore_tasks.discard)

    def _record_binding(self, binding: tuple[str, str, str, str, dict[str, Any] | None]) -> None:
        """Remember *binding* for re-apply after a reconnect, once: repeating
        the same bind (every reconnect, every restart of a route) used to
        append it again, so the list and each re-apply pass grew without
        bound. Bindings are few and recorded off the hot path."""
        if binding not in self._recorded_bindings:
            self._recorded_bindings.append(binding)

    async def _reapply_bindings(self) -> None:
        """Re-apply all recorded bindings after a robust reconnect (m3).

        Retries each pass with backoff (verification gap 4): RobustChannel
        restoration timing is not observable from here, so a single fixed
        sleep could race it — a binding that fails to re-apply would then
        stay missing until the NEXT reconnect, silently unrouting publishes
        in the interim. Bounded so shutdown can't be held hostage.
        """
        delays = (1.0, 2.0, 4.0, 8.0)
        pending = list(self._recorded_bindings)
        for attempt, delay in enumerate(delays, start=1):
            await asyncio.sleep(delay)
            if self._topology_channel is None or self._topology_channel.is_closed:
                return  # shutting down / not reconnected yet — nothing to do
            failed: list[tuple[str, str, str, str, dict[str, Any] | None]] = []
            for kind, a, b, routing_key, arguments in pending:
                try:
                    if kind == "queue":
                        q = await self._topology_channel.get_queue(a, ensure=False)
                        ex = await self._topology_channel.get_exchange(b, ensure=False)
                        await q.bind(ex, routing_key=routing_key, arguments=arguments)
                    else:
                        dest_ex = await self._topology_channel.get_exchange(a, ensure=False)
                        src_ex = await self._topology_channel.get_exchange(b, ensure=False)
                        await dest_ex.bind(src_ex, routing_key=routing_key, arguments=arguments)
                except Exception:
                    failed.append((kind, a, b, routing_key, arguments))
            if not failed:
                return
            pending = failed
            logger.warning(
                "%d binding(s) failed to re-apply after reconnect (attempt %d/%d); retrying",
                len(pending),
                attempt,
                len(delays),
            )
        logger.error(
            "Giving up re-applying %d binding(s) after reconnect — publishes through the "
            "affected exchange(s) may be unroutable until the next reconnect: %s",
            len(pending),
            [(k, a, b) for k, a, b, _rk, _args in pending],
        )

    def on_blocked(self, callback: Callable[[], None]) -> None:
        """Register a connection-blocked callback (e.g. FlowController.on_blocked)."""
        self._blocked_callbacks.append(callback)

    def on_unblocked(self, callback: Callable[[], None]) -> None:
        """Register a connection-unblocked callback (e.g. FlowController.on_unblocked)."""
        self._unblocked_callbacks.append(callback)

    @property
    def is_blocked(self) -> bool:
        """True if RabbitMQ has sent ``connection.blocked`` (L15) -- e.g. a
        broker memory/disk alarm. Tracked passively regardless of whether
        any ``on_blocked``/``on_unblocked`` callback is registered, so
        ``health.broker_health_check`` can see it even without an opt-in
        ``FlowController``."""
        return self._blocked_state

    def _start_blocked_monitor(self, connection: Any) -> None:
        """Watch *connection* for ``connection.blocked`` (issue #37).

        Drives ``is_blocked``, the FlowController callbacks and
        ``blocked_connection_timeout``. Both the publisher and the consumer
        connection get one: the consumer connection carries the topology and
        direct-reply-to channels, so RPC request publishes stall there too.
        Only the publisher connection is force-reconnected on timeout.
        """
        from rabbitkit.async_.connection import BlockedConnectionMonitor

        try:
            asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover — the pool only connects inside a loop
            return
        monitor = BlockedConnectionMonitor(
            connection,
            blocked_timeout=self._connection_config.blocked_connection_timeout,
            on_blocked=self._aio_blocked,
            on_unblocked=self._aio_unblocked,
            # Blocked state is tracked on both connections, but only the
            # publisher one is force-reconnected: the alarm is what stalls
            # publishes, and closing the consumer connection would requeue
            # every in-flight delivery and duplicate running handlers.
            may_force_reconnect=lambda: connection is self._conn_pool._publisher_connection,
        )
        if monitor.start():
            self._blocked_monitors = [m for m in self._blocked_monitors if m._task is not None and not m._task.done()]
            self._blocked_monitors.append(monitor)

    def _aio_blocked(self, *_args: Any) -> None:
        # Fire on the transport-wide transition only: with two connections a
        # second block (or the first of two unblocks) changes nothing.
        if self._blocked_state:
            return
        self._blocked_state = True
        for cb in list(self._blocked_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover - never break the event loop
                logger.exception("blocked callback raised")

    def _aio_unblocked(self, *_args: Any) -> None:
        if any(m.is_blocked for m in self._blocked_monitors):
            return
        if not self._blocked_state:
            return
        self._blocked_state = False
        for cb in list(self._unblocked_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover
                logger.exception("unblocked callback raised")

    async def connect(self) -> None:
        """Establish publisher and consumer connections.

        All or nothing (G-10): if any step fails (or is cancelled), every
        connection, channel and monitor it created is closed before the error
        propagates, so a failed connect leaks nothing and can be retried.
        """
        if self._connected:
            return
        try:
            await self._connect_unguarded()
        except BaseException:
            await self._discard_partial_connect()
            raise

    async def _discard_partial_connect(self) -> None:
        with contextlib.suppress(Exception):
            async with asyncio.timeout(self._connection_config.socket_timeout):
                for monitor in self._blocked_monitors:
                    await monitor.stop()
                self._blocked_monitors.clear()
                self._blocked_state = False
                channel, self._topology_channel = self._topology_channel, None
                if channel is not None and not channel.is_closed:
                    await channel.close()
                await self._conn_pool.close_all()

    async def _connect_unguarded(self) -> None:
        # aio-pika 9's pamqp can't encode field-table ints from -128 to -1
        # (queue/consume/bind arguments, publish headers); fix it before any
        # frame is built. A no-op on pamqp 4.
        from rabbitkit.async_.connection import ensure_pamqp_encodes_negative_ints

        ensure_pamqp_encodes_negative_ints()

        await self._conn_pool.connect()

        # Blocked-state monitors (C-6, I-11) are started per connection by
        # _attach_connection_callbacks, which the pool calls for every
        # connection it creates, so they follow rebuilt connections too.
        pub_conn = await self._conn_pool.get_publisher_connection()

        # Connection-churn metric hook: connect_robust reconnects silently
        # (well, logged) -- count them so a flapping broker/network is
        # visible to metrics alerting. Both connections, since either can
        # flap independently.
        try:
            pub_conn.reconnect_callbacks.add(self._aio_reconnected)
            consumer_conn_for_cb = await self._conn_pool.get_consumer_connection()
            if consumer_conn_for_cb is not pub_conn:
                consumer_conn_for_cb.reconnect_callbacks.add(self._aio_reconnected)
        except Exception:  # pragma: no cover - older aio-pika may differ
            logger.debug("Could not register reconnect callbacks")

        # Open topology channel on consumer connection
        consumer_conn = await self._conn_pool.get_consumer_connection()
        self._topology_channel = await consumer_conn.channel()
        self._fire_channel_opened()

        self._connected = True
        logger.info(
            "Connected to RabbitMQ at %s:%d (async, pooled)",
            self._connection_config.host,
            self._connection_config.port,
        )

    async def __aenter__(self) -> AsyncTransportImpl:
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        # Architect review M1: this was named __exit__, so `async with
        # AsyncTransportImpl(...)` raised TypeError on entry — the protocol
        # method must be __aexit__.
        await self.disconnect()

    async def disconnect(self, timeout: float | None = None) -> None:
        """Close all channels and connections, within *timeout* seconds.

        Channels are closed first; if the broker stops answering (a
        partition) the remaining channel closes are skipped and the
        connections are closed, or abandoned, within what is left.
        """
        if not self._connected:
            return

        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout

        def left() -> float | None:
            return None if deadline is None else max(0.0, deadline - loop.time())

        try:
            try:
                async with asyncio.timeout(left()):
                    await self._close_channels()
            except TimeoutError:
                logger.warning("Closing RabbitMQ channels timed out; closing the connections")
            finally:
                self._consumer_channels.clear()
                self._consumer_tags.clear()
                self._reply_to_channel = None
                self._topology_channel = None
                self._fast_publish_channel = None
            await self._conn_pool.close_all(timeout=left())  # includes the mandatory-publish channels
        except Exception as e:
            logger.warning("Error during disconnect: %s", e)
        finally:
            self._connected = False
            logger.info("Disconnected from RabbitMQ (async)")

    async def _close_channels(self) -> None:
        """Stop the consumer supervisor and blocked monitors, then close every channel."""
        await self._consumer_supervisor.stop()
        for monitor in self._blocked_monitors:
            await monitor.stop()
        self._blocked_monitors.clear()
        self._blocked_state = False

        # Close per-queue consumer channels
        for ch in list(self._consumer_channels.values()):
            try:
                if not ch.is_closed:
                    await ch.close()
            except Exception:
                pass
        self._consumer_channels.clear()
        self._consumer_tags.clear()
        self._reply_to_channel = None

        # Close topology channel
        if self._topology_channel is not None and not self._topology_channel.is_closed:
            try:
                await self._topology_channel.close()
            except Exception:
                pass
        self._topology_channel = None

        # Close fast publish channel (no-confirm persistent path)
        if self._fast_publish_channel is not None and not self._fast_publish_channel.is_closed:
            try:
                await self._fast_publish_channel.close()
            except Exception:  # pragma: no cover — best effort close, network errors only
                pass
        self._fast_publish_channel = None

    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ, right now.

        ``RobustConnection.is_closed`` is no use on its own: it stays False
        for the whole time aio-pika is reconnecting after a broker or network
        loss, so a readiness probe built on it kept a disconnected pod in
        rotation (issue #39). The live signal is the connection's
        ``connected`` event, which aio-pika clears when the AMQP connection
        drops and sets again once the reconnect completes. Both the publisher
        and the consumer connection must be up.
        """
        if not self._connected or self._conn_pool.terminal_error is not None:
            return False
        pub = self._conn_pool._publisher_connection
        if pub is None:
            return False
        conns = [pub]
        consumer = self._conn_pool._consumer_connection
        if consumer is not None and consumer is not pub:
            conns.append(consumer)
        return all(_connection_live(c) for c in conns)

    @property
    def has_open_channels(self) -> bool:
        """True if at least one consumer channel is open (readiness contract).

        Mirrors ``SyncTransport.has_open_channels`` so ``broker_readiness`` can
        detect a dead consumer channel on async transports (I-5 async side).
        """
        if not self._consumer_channels:
            return False
        return all(not bool(getattr(ch, "is_closed", False)) for ch in self._consumer_channels.values())

    @property
    def is_reconnecting(self) -> bool:
        """Best-effort: True if the robust connection is mid-reconnect.

        aio-pika does not expose a stable public attribute, so this is a
        cheap, guarded heuristic (``reconnects`` counter / ``_reconnect_lock").
        """
        conn = self._conn_pool._publisher_connection
        if conn is None:
            return False
        try:
            # Newer aio-pika tracks pending reconnects via a lock/event.
            lock = getattr(conn, "_reconnect_lock", None)
            if lock is not None and getattr(lock, "locked", lambda: False)():
                return True
        except Exception:  # pragma: no cover
            pass
        return False

    async def _wait_for_recovery(self, timeout: float = _PUBLISH_RECOVERY_WAIT_SECONDS) -> bool:
        """Bounded wait for aio-pika's own ``connect_robust()`` to restore the
        publisher connection after a connection-class publish error (item 6).

        aio-pika owns reconnection entirely here -- this only OBSERVES the
        publisher connection's liveness (only it matters for a publish; the
        consumer connection may still be reconnecting). ``self._connected`` itself is NOT a
        usable recovery signal: it is set once at the first successful
        ``connect()`` and never flipped by connect_robust's background
        recovery, so ``_ensure_connected()`` alone can't detect a mid-flight
        reconnect the way the sync transport's ``_ensure_connected()`` does.
        Polls rather than awaiting a callback since aio-pika exposes no
        awaitable "reconnected" event.
        """
        if self._conn_pool.terminal_error is not None:
            return False  # G-08: closed for good, nothing will recover
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not self._publish_shutdown:
            if self._publisher_live():
                return True
            await asyncio.sleep(0.1)
        return self._publisher_live()

    def _publisher_live(self) -> bool:
        conn = self._conn_pool._publisher_connection
        return self._connected and conn is not None and _connection_live(conn)

    def _publisher_reconnecting(self) -> bool:
        """True while aio-pika restores an existing publisher connection."""
        conn = self._conn_pool._publisher_connection
        return conn is not None and not _connection_live(conn)

    async def _ensure_connected(self) -> None:
        """Ensure connection is established."""
        if self._connected:
            return
        await self.connect()

    async def _get_fast_channel(self) -> Any:
        """Return the persistent no-confirm publish channel, (re)opening if needed.

        Used exclusively by the fire-and-forget publish path (confirm_delivery=False).
        A single channel is reused across all concurrent publishes; aio-pika
        serialises AMQP frames at the transport level so concurrent writes are safe.
        """
        ch = self._fast_publish_channel
        if ch is not None and not ch.is_closed:
            return ch
        async with self._fast_channel_lock:
            ch = self._fast_publish_channel
            if ch is not None and not ch.is_closed:  # pragma: no cover — concurrent path
                return ch
            conn = self._conn_pool._publisher_connection
            if conn is None:
                raise RuntimeError("Publisher connection is not available")
            is_rebuild = ch is not None  # replacing a channel found closed above
            self._fast_publish_channel = await conn.channel(publisher_confirms=False)
            self._fire_channel_opened()
            if is_rebuild:
                self._fire_channel_rebuilt()
            return self._fast_publish_channel

    def _build_aio_message(self, envelope: MessageEnvelope) -> Any:
        """Build an aio_pika.Message from a MessageEnvelope.

        An empty message_id/content_type means "no such property" (DLQ
        replay of a message that had none). aiormq still stamps a random
        message_id on any publish without one.
        """
        import aio_pika

        return _exact_expiration_message_class()(
            body=envelope.body,
            expiration_ms=envelope.expiration or None,
            message_id=envelope.message_id or None,
            correlation_id=envelope.correlation_id,
            reply_to=envelope.reply_to,
            content_type=envelope.content_type or None,
            content_encoding=envelope.content_encoding,
            headers=envelope.headers or None,
            delivery_mode=aio_pika.DeliveryMode(envelope.delivery_mode),
            priority=envelope.priority,
            type=envelope.type,
            user_id=envelope.user_id,
            app_id=envelope.app_id,
            timestamp=envelope.timestamp,
        )

    def _outcome(
        self,
        envelope: MessageEnvelope,
        status: PublishStatus,
        reason: str | None = None,
        error: BaseException | None = None,
        *,
        reply_code: int | None = None,
        reply_text: str | None = None,
    ) -> PublishOutcome:
        return PublishOutcome(
            status=status,
            exchange=envelope.exchange,
            routing_key=envelope.routing_key,
            error=error,
            reason=reason,
            reply_code=reply_code,
            reply_text=reply_text,
        )

    def _not_sent(self, envelope: MessageEnvelope, reason: str, error: BaseException | None = None) -> PublishOutcome:
        return self._outcome(envelope, PublishStatus.NOT_SENT, reason, error)

    async def _publish_on_channel(
        self,
        channel: Any,
        envelope: MessageEnvelope,
        *,
        timeout: float | None = None,
        exclusive: bool = False,
    ) -> PublishOutcome:
        """Publish *envelope* on an already-acquired channel.

        Used by BatchPublisher to publish many messages on one channel and
        gather all confirms concurrently — one pool acquire/release for N
        messages instead of N separate round-trips.

        A ``mandatory=True`` publish the broker cannot route is RETURNED on
        every channel kind (G-01): a channel with ``on_return_raises=True``
        (the mandatory pool) raises ``aio_pika.exceptions.PublishError``; any
        other confirmed channel (batch workers, the direct reply-to channel)
        resolves the confirm with the returned message instead of an ack,
        which used to be reported as CONFIRMED. Both carry the broker's
        ``reply_code`` / ``reply_text``. A broker ``Basic.Nack`` raises
        ``DeliveryError`` and maps to ``NACKED``.

        ``timeout`` caps the confirm wait below ``confirm_timeout`` (the
        caller's per-call deadline); ``exclusive`` says no other publish uses
        this channel, so a channel-level refusal (404/403/406) is provably
        this publish's own. See :meth:`_classify_publish_failure`.
        """

        try:
            message = self._build_aio_message(envelope)
            exchange = (
                await channel.get_exchange(envelope.exchange, ensure=False)
                if envelope.exchange
                else channel.default_exchange
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # nothing has been sent yet
            if isinstance(e, (ValueError, TypeError)):
                return self._outcome(envelope, PublishStatus.ERROR, PublishReason.INVALID_ENVELOPE, e)
            return self._not_sent(envelope, PublishReason.CHANNEL_UNAVAILABLE, e)
        # Serialize publishes of the SAME message_id on the SAME channel.
        # aiormq keys its Basic.Return correlation by message_id
        # (``message_id_delivery_tag``) and pops that key when a publish of
        # that id confirms. The retry middleware deliberately re-publishes
        # the original message_id; if the source publish's confirm is still
        # in flight when the retry publish registers the same id (slow fsync
        # on a durable queue while the consumer already received and failed
        # the message), the late confirm deletes the RETRY's mapping, its
        # Basic.Return is logged "Unhandled message ... returning", and the
        # retry publish resolves as CONFIRMED — the source is then acked with
        # nothing on the delay queue. Waiting for the previous same-id
        # publish on this channel to settle first closes that window (its
        # aiormq done-callback pops the key before our waiter resumes).
        key = (id(channel), envelope.message_id)
        prior = self._inflight_message_ids.get(key)
        if prior is not None and not prior.done():
            wait = self._confirm_timeout if timeout is None else min(self._confirm_timeout, timeout)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.shield(prior), timeout=wait)
        mine: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._inflight_message_ids[key] = mine
        try:
            started = time.monotonic()
            outcome = await self._publish_on_channel_unguarded(
                channel, exchange, message, envelope, timeout=timeout, exclusive=exclusive
            )
            if self._publish_outcome_callbacks and outcome.status is not PublishStatus.NOT_SENT:
                answered = outcome.status in _BROKER_ANSWERS
                self._report_publish(outcome, time.monotonic() - started if answered else None)
            return outcome
        finally:
            if self._inflight_message_ids.get(key) is mine:
                del self._inflight_message_ids[key]
            if not mine.done():
                mine.set_result(None)

    async def _publish_on_channel_unguarded(
        self,
        channel: Any,
        exchange: Any,
        message: Any,
        envelope: MessageEnvelope,
        *,
        timeout: float | None = None,
        exclusive: bool = False,
    ) -> PublishOutcome:
        import aio_pika.exceptions

        wait = self._confirm_timeout
        caller_bound = timeout is not None and timeout < wait
        if timeout is not None:
            wait = min(wait, timeout)
        if wait <= 0:
            return self._not_sent(envelope, PublishReason.ADMISSION_TIMEOUT)
        try:
            async with asyncio.timeout(wait):
                result = await exchange.publish(
                    message,
                    routing_key=envelope.routing_key,
                    mandatory=envelope.mandatory,
                )
        except TimeoutError as e:
            # M17: do NOT close the channel here. This coroutine may be one of
            # several concurrent calls sharing the SAME channel (BatchPublisher's
            # _flush gathers N of these on one channel) — closing it the instant
            # OUR OWN confirm-wait times out would kill every sibling publish
            # still awaiting ITS OWN confirm on that channel, even ones that
            # would have confirmed cleanly a moment later. Callers decide
            # whether/when to close a channel that had a timeout, and do so
            # only once they know no other publish is still using it (e.g.
            # after their own single call, or after a whole gathered batch has
            # fully resolved) — see callers of _publish_on_channel.
            logger.warning("Publish confirm timed out after %.1fs", wait)
            reason = PublishReason.PUBLISH_TIMEOUT if caller_bound else PublishReason.CONFIRM_TIMEOUT
            return self._outcome(envelope, PublishStatus.TIMEOUT, reason, e)
        except aio_pika.exceptions.PublishError as e:
            code, text = _return_fields(e)
            return self._returned(envelope, code, text, e)
        except aio_pika.exceptions.DeliveryError as e:
            logger.warning(
                "Publish nacked by broker: exchange=%s routing_key=%s",
                envelope.exchange,
                envelope.routing_key,
            )
            return self._outcome(envelope, PublishStatus.NACKED, PublishReason.NACKED, e)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._classify_publish_failure(e, envelope, exclusive=exclusive)
        delivery = getattr(result, "delivery", None)
        if _is_basic_return(delivery):
            # A confirmed channel WITHOUT on_return_raises resolves an
            # unroutable mandatory publish with the returned message.
            return self._returned(
                envelope, getattr(delivery, "reply_code", None), getattr(delivery, "reply_text", None), None
            )
        return self._outcome(envelope, PublishStatus.CONFIRMED)

    def _returned(
        self, envelope: MessageEnvelope, code: int | None, text: str | None, error: BaseException | None
    ) -> PublishOutcome:
        logger.warning(
            "Publish returned as unroutable (mandatory=True, no matching binding): "
            "exchange=%s routing_key=%s reply_code=%s reply_text=%s",
            envelope.exchange,
            envelope.routing_key,
            code,
            text,
        )
        return self._outcome(
            envelope, PublishStatus.RETURNED, PublishReason.RETURNED, error, reply_code=code, reply_text=text
        )

    def _classify_publish_failure(
        self, exc: Exception, envelope: MessageEnvelope, *, exclusive: bool
    ) -> PublishOutcome:
        """Map an exception raised by ``exchange.publish`` (frames may have left).

        * A broker channel close that refused THIS publish (404 missing
          exchange, 403 access refused, 406 precondition failed) is ERROR
          with a reason and the broker's reply code/text. It is not a
          connection error, so nothing retries it (G-03/PUB-016). On a shared
          channel the refusal may belong to a sibling publish, so it counts
          as ours only if the channel is exclusive or the 404 names our
          exchange; otherwise this publish is UNKNOWN (``channel_closed``).
        * Anything connection- or channel-shaped is UNKNOWN: the frames may
          already have reached the broker. Never retried (INV-003).
        * Invalid input caught while encoding is ERROR ``invalid_envelope``;
          anything else is ERROR ``publish_error``.
        """
        import aiormq.exceptions as ape

        refusals: tuple[tuple[type[BaseException], str, int], ...] = (
            (ape.ChannelNotFoundEntity, PublishReason.EXCHANGE_NOT_FOUND, 404),
            (ape.ChannelAccessRefused, PublishReason.ACCESS_REFUSED, 403),
            (ape.ChannelPreconditionFailed, PublishReason.PRECONDITION_FAILED, 406),
        )
        for cls, reason, code in refusals:
            if isinstance(exc, cls):
                text = str(exc.args[0]) if exc.args else None
                ours = exclusive or (code == 404 and text is not None and f"no exchange '{envelope.exchange}'" in text)
                if ours:
                    logger.error(
                        "Publish refused by broker (%s): exchange=%s routing_key=%s: %s",
                        reason,
                        envelope.exchange,
                        envelope.routing_key,
                        text,
                    )
                    return self._outcome(envelope, PublishStatus.ERROR, reason, exc, reply_code=code, reply_text=text)
                return self._unknown(envelope, PublishReason.CHANNEL_CLOSED, exc)
        if isinstance(exc, (ValueError, TypeError)):
            logger.error("Async publish failed (invalid envelope): %s", exc)
            return self._outcome(envelope, PublishStatus.ERROR, PublishReason.INVALID_ENVELOPE, exc)
        if _is_connection_error(exc) or isinstance(exc, ape.ChannelInvalidStateError):
            reason = PublishReason.SHUTDOWN if self._publish_shutdown else PublishReason.CONNECTION_LOST
            return self._unknown(envelope, reason, exc)
        logger.error("Async publish failed: %s", exc)
        return self._outcome(envelope, PublishStatus.ERROR, PublishReason.PUBLISH_ERROR, exc)

    def _unknown(self, envelope: MessageEnvelope, reason: str, exc: BaseException) -> PublishOutcome:
        logger.warning(
            "Publish outcome UNKNOWN (%s): exchange=%s routing_key=%s message_id=%s: %s",
            reason,
            envelope.exchange,
            envelope.routing_key,
            envelope.message_id,
            safe_exception_repr(exc),
        )
        return self._outcome(envelope, PublishStatus.UNKNOWN, reason, exc)

    # ── Publish admission and shutdown ───────────────────────────────────

    @property
    def byte_limiter(self) -> InFlightByteLimiter | None:
        """The ``PublisherConfig.max_in_flight_bytes`` budget (None = unbounded)."""
        return self._byte_limiter

    @property
    def publishes_in_flight(self) -> int:
        """Publishes between admission and their terminal outcome."""
        return self._publishes_in_flight

    def _publish_entered(self) -> None:
        self._publishes_in_flight += 1
        if self._publishes_idle is not None:
            self._publishes_idle.clear()

    def _publish_exited(self) -> None:
        self._publishes_in_flight = max(0, self._publishes_in_flight - 1)
        if self._publishes_in_flight == 0 and self._publishes_idle is not None:
            self._publishes_idle.set()

    async def _wait_publishes_idle(self, timeout: float | None) -> bool:
        if self._publishes_in_flight == 0:
            return True
        if self._publishes_idle is None:
            self._publishes_idle = asyncio.Event()
        try:
            async with asyncio.timeout(timeout):
                await self._publishes_idle.wait()
        except TimeoutError:
            return False
        return True

    async def close_publishing(self, timeout: float | None = None) -> None:
        """Publisher shutdown (G-12), bounded by *timeout*.

        1. Close admission: every later :meth:`publish` returns ``NOT_SENT``
           (``publisher_closed``) and nothing reconnects.
        2. Wait for in-flight publishes to reach their outcome.
        3. On expiry, close the publisher channels: a publish still waiting
           for a channel or for byte budget returns ``NOT_SENT``
           (``publisher_closed``); one still waiting for its confirm returns
           ``UNKNOWN`` (``shutdown``) — its frames may have reached the broker.

        Terminal for this transport instance (the broker builds a new
        transport on every ``start()``). Connections stay open; call
        :meth:`disconnect` afterwards.
        """
        self._publish_closed = True
        if await self._wait_publishes_idle(timeout):
            return
        logger.warning(
            "Publisher shutdown: %d publish(es) still in flight after the deadline; "
            "reporting them UNKNOWN (they may have reached the broker)",
            self._publishes_in_flight,
        )
        self._publish_shutdown = True
        if self._byte_limiter is not None:
            self._byte_limiter.close()
        with contextlib.suppress(Exception):
            async with asyncio.timeout(_SHUTDOWN_UNWIND_SECONDS):
                await self._conn_pool.close_publisher_channels()
        fast = self._fast_publish_channel
        if fast is not None and not fast.is_closed:
            with contextlib.suppress(Exception):
                async with asyncio.timeout(_SHUTDOWN_UNWIND_SECONDS):
                    await fast.close()
        if not await self._wait_publishes_idle(_SHUTDOWN_UNWIND_SECONDS):
            logger.error(
                "Publisher shutdown: %d publish(es) did not unwind within %.1fs",
                self._publishes_in_flight,
                _SHUTDOWN_UNWIND_SECONDS,
            )

    def _remaining(self, deadline: float | None) -> float | None:
        if deadline is None:
            return None
        return deadline - asyncio.get_running_loop().time()

    async def publish(self, envelope: MessageEnvelope, *, timeout: float | None = None) -> PublishOutcome:
        """Publish a message.

        When confirm_delivery=False: uses a single persistent channel (no
        acquire/release overhead, no broker ACK wait) for maximum throughput.
        When confirm_delivery=True: uses the channel pool so each in-flight
        confirm is isolated to its own channel slot.

        A request with ``reply_to=DIRECT_REPLY_TO_QUEUE`` (RPCClient's direct
        reply-to requests) bypasses both paths above and is routed onto
        ``self._reply_to_channel`` — the same channel that registered the
        reply consumer — instead. RabbitMQ requires this exact channel
        affinity for direct reply-to; publishing on a different channel raises
        "PRECONDITION_FAILED - fast reply consumer does not exist".

        H1: a ``mandatory=True`` envelope (that isn't a direct reply-to
        request) always publishes on a mandatory channel of its own
        (``AsyncConnectionPool.acquire_mandatory_channel``): confirmed and
        ``on_return_raises=True`` regardless of ``confirm_delivery``. One
        publish per channel: a publish the broker refuses closes its channel
        and fails everything in flight on it.

        Outcome contract (G-03): ``NOT_SENT`` when the message definitely
        never left the client (publisher closed, byte budget or channel
        admission timed out, no connection, no channel), ``UNKNOWN`` /
        ``TIMEOUT`` when it may have, ``ERROR`` only for a refused or
        unclassified publish. Never raises for a broker or connection
        failure and never republishes on its own once frames may have left.
        ``timeout`` bounds admission, send and confirm together.
        """
        if self._publish_closed:
            return self._not_sent(envelope, PublishReason.PUBLISHER_CLOSED)
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        self._publish_entered()
        reserved = 0
        try:
            limiter = self._byte_limiter
            if limiter is not None:
                size = len(envelope.body)
                if size > limiter.limit:
                    return self._not_sent(envelope, PublishReason.EXCEEDS_BYTE_BUDGET)
                wait = self._admission_wait(deadline)
                try:
                    admitted = await limiter.acquire(size, wait)
                except ChannelPoolClosedError as e:
                    return self._not_sent(envelope, PublishReason.PUBLISHER_CLOSED, e)
                if not admitted:
                    return self._not_sent(envelope, PublishReason.ADMISSION_TIMEOUT)
                reserved = size
            return await self._publish_admitted(envelope, deadline)
        finally:
            if reserved:
                assert self._byte_limiter is not None
                self._byte_limiter.release(reserved)
            self._publish_exited()

    def _admission_wait(self, deadline: float | None) -> float:
        wait = self._pool_config.channel_acquire_timeout
        remaining = self._remaining(deadline)
        if remaining is not None:
            wait = min(wait, max(0.0, remaining))
        return wait

    async def _publish_admitted(self, envelope: MessageEnvelope, deadline: float | None) -> PublishOutcome:
        try:
            if not self._connected:
                await self._ensure_connected()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Async publish failed: not connected: %s", e)
            return self._not_sent(envelope, PublishReason.NOT_CONNECTED, e)

        if envelope.reply_to == DIRECT_REPLY_TO_QUEUE and self._reply_to_channel is not None:
            channel = self._reply_to_channel
            if not channel.is_closed:
                return await self._publish_on_channel(channel, envelope, timeout=self._remaining(deadline))

        if not envelope.mandatory and not self._confirm_delivery:
            return await self._publish_fast(envelope)
        return await self._publish_confirmed(envelope, deadline)

    async def _publish_fast(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Fire-and-forget path: persistent channel, no confirm wait, no pool."""
        try:
            message = self._build_aio_message(envelope)
            channel = await self._get_fast_channel()
            exchange = (
                await channel.get_exchange(envelope.exchange, ensure=False)
                if envelope.exchange
                else channel.default_exchange
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # nothing has been sent yet
            if isinstance(e, (ValueError, TypeError)):
                return self._outcome(envelope, PublishStatus.ERROR, PublishReason.INVALID_ENVELOPE, e)
            logger.error("Async publish failed: no publish channel: %s", e)
            return self._not_sent(envelope, PublishReason.CHANNEL_UNAVAILABLE, e)
        try:
            await exchange.publish(
                message,
                routing_key=envelope.routing_key,
                mandatory=envelope.mandatory,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            outcome = self._classify_publish_failure(e, envelope, exclusive=False)
        else:
            # M4: SENT, not CONFIRMED -- this channel has publisher_confirms=False,
            # so nothing was actually acknowledged by the broker.
            outcome = self._outcome(envelope, PublishStatus.SENT)
        if self._publish_outcome_callbacks:
            self._report_publish(outcome, None)
        return outcome

    async def _publish_confirmed(self, envelope: MessageEnvelope, deadline: float | None = None) -> PublishOutcome:
        """Confirmed (pooled-channel) publish path — one pool channel per
        publish so each confirm is isolated.

        * No hidden retry (G-03): once ``basic.publish`` may have left the
          client the outcome is reported as observed (UNKNOWN on a lost
          connection) and never republished here. Failures BEFORE the frame
          (no channel, admission timeout, connection still recovering) are
          ``NOT_SENT``; only those are retried, inside the pool's
          connection self-heal.
        * The channel is returned on every path (G-04), including
          cancellation. A channel whose confirm state is unknown (cancelled
          or failed mid-publish, timed out) is discarded — closed and never
          reused — instead of going back to the pool.
        * Pool exhaustion waits up to the admission timeout and then returns
          ``NOT_SENT(admission_timeout)``; it never rebuilds the connection
          (G-05).
        """
        pool = self._conn_pool
        if envelope.mandatory:
            acquire, release, discard = (
                pool.acquire_mandatory_channel,
                pool.release_mandatory_channel,
                pool.discard_mandatory_channel,
            )
        else:
            acquire, release, discard = (
                pool.acquire_publisher_channel,
                pool.release_publisher_channel,
                pool.discard_publisher_channel,
            )

        if self._publisher_reconnecting():
            # aio-pika is restoring the publisher connection: wait for it
            # (bounded) BEFORE sending, rather than publishing into a dead
            # channel and having to report UNKNOWN.
            wait = _PUBLISH_RECOVERY_WAIT_SECONDS
            remaining = self._remaining(deadline)
            if remaining is not None:
                wait = min(wait, max(0.0, remaining))
            if not await self._wait_for_recovery(timeout=wait):
                return self._not_sent(envelope, PublishReason.NOT_CONNECTED)

        try:
            channel = await acquire(timeout=self._admission_wait(deadline))
        except asyncio.CancelledError:
            raise
        except ChannelPoolExhaustedError as e:
            logger.warning(
                "Publish not sent: no publisher channel free within the admission timeout (exchange=%s routing_key=%s)",
                envelope.exchange,
                envelope.routing_key,
            )
            return self._not_sent(envelope, PublishReason.ADMISSION_TIMEOUT, e)
        except ChannelPoolClosedError as e:
            return self._not_sent(envelope, PublishReason.PUBLISHER_CLOSED, e)
        except Exception as e:
            logger.error("Async publish failed: no publisher channel: %s", e)
            return self._not_sent(envelope, PublishReason.CHANNEL_UNAVAILABLE, e)

        reusable = False
        try:
            try:
                outcome = await self._publish_on_channel(
                    channel, envelope, timeout=self._remaining(deadline), exclusive=True
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # defensive: _publish_on_channel classifies its own failures
                outcome = self._classify_publish_failure(e, envelope, exclusive=True)
            # M17 + G-04: only a channel whose publish reached a broker answer
            # goes back to the pool; a timed-out or broken one is discarded
            # (closed) so the pool never hands out a possibly-wedged channel.
            reusable = outcome.status in _REUSABLE_STATUSES and not channel.is_closed
            return outcome
        finally:
            if reusable:
                with contextlib.suppress(Exception):
                    await release(channel)
            else:
                discard(channel)

    async def consume(
        self,
        queue: str,
        callback: Callable[[RabbitMessage], Awaitable[None]],
        prefetch: int = 10,
        *,
        no_ack: bool = False,
        declare: bool = True,
        arguments: dict[str, Any] | None = None,
    ) -> str:
        """Start consuming from a queue.

        Each queue gets a dedicated channel so per-queue QoS settings do not
        interfere with each other.  Returns the consumer tag.

        ``no_ack=True`` starts a no-ack consumer: the broker auto-acks on
        delivery, and the built ``RabbitMessage`` is not wired with settlement
        functions (there is nothing to ack/nack/reject).

        ``declare=False`` skips the passive-declare check and instead obtains a
        bare, undeclared ``Queue`` handle (``channel.get_queue(queue,
        ensure=False)`` — constructs the wrapper locally with no AMQP frame
        sent, unlike ``declare_queue(passive=True)``). Required for AMQP
        pseudo-queues such as ``amq.rabbitmq.reply-to``: the broker rejects
        *any* Queue.Declare against that name (even passive), yet basic_consume
        against it is valid and is how RabbitMQ's direct-reply-to feature works.
        Note: an undeclared queue is not tracked in ``RobustChannel``'s
        internal registry, so unlike the ``declare=True`` path this consumer is
        NOT automatically resumed by aio-pika after a ``connect_robust``
        reconnect — acceptable for ``amq.rabbitmq.reply-to``, whose lifetime is
        scoped to the connection anyway.

        When ``declare=False`` and ``queue == DIRECT_REPLY_TO_QUEUE``, this
        consumer's channel is also remembered as ``self._reply_to_channel`` so
        :meth:`publish` can route matching requests onto the SAME channel —
        required by RabbitMQ's direct reply-to (see :meth:`publish`).

        ``arguments`` are the ``basic.consume`` arguments (e.g.
        ``x-stream-offset`` for a stream queue). The dict is passed BY
        REFERENCE: aio-pika keeps it in the queue's restore record and sends
        its current contents when ``connect_robust`` re-subscribes after a
        reconnect (and the consumer supervisor re-subscribes with it after a
        broker cancel), so the caller can keep the resume point up to date
        in place.
        """
        await self._ensure_connected()

        # Dedicated channel per consumer queue for isolated QoS
        consumer_conn = await self._conn_pool.get_consumer_connection()
        channel = await consumer_conn.channel()
        self._fire_channel_opened()
        await channel.set_qos(prefetch_count=prefetch)
        self._consumer_channels[queue] = channel

        if not declare and queue == DIRECT_REPLY_TO_QUEUE:
            self._reply_to_channel = channel

        if declare:
            # passive declare (not get_queue): RobustChannel only restores queues
            # in its _queues registry, which declare_queue populates and get_queue
            # does not. Without this, the consumer is silently NOT resumed after a
            # connect_robust reconnect (the queue — and its consumer — are
            # untracked).
            q = await channel.declare_queue(queue, passive=True)
        else:
            # No AMQP frame sent — just a local Queue wrapper for `queue`. Some
            # pseudo-queues (amq.rabbitmq.reply-to) reject any Queue.Declare.
            q = await channel.get_queue(queue, ensure=False)
        consumer_tag = f"rabbitkit.{uuid.uuid4()}"

        async def on_message(message: Any) -> None:
            if record.stopped:
                # CONSUMER-007: rabbitkit cancelled this consumer, yet a
                # delivery arrived (a reconnect restored it). Hand it back.
                await self._refuse_stray_delivery(record, message)
                return
            rabbit_msg = self._build_message(message, no_ack=no_ack)
            await callback(rabbit_msg)

        record = ConsumerRecord(
            queue=queue,
            tag=consumer_tag,
            channel=channel,
            queue_obj=q,
            callback=on_message,
            no_ack=no_ack,
            supervised=declare,
            arguments=arguments,
        )
        consume_kwargs: dict[str, Any] = {}
        if arguments is not None:
            consume_kwargs["arguments"] = arguments  # by reference: see docstring
        await q.consume(on_message, consumer_tag=consumer_tag, no_ack=no_ack, **consume_kwargs)
        self._consumer_tags[queue] = consumer_tag
        await self._consumer_supervisor.register(record)
        logger.info("Started consuming from queue '%s' with tag '%s' (async)", queue, consumer_tag)
        return consumer_tag

    async def declare_exchange(self, exchange: RabbitExchange) -> None:
        """Declare an exchange on the topology channel."""
        action = self._topo.exchange_action(exchange)
        if action is TopoAction.SKIP:
            return

        await self._ensure_connected()
        assert self._topology_channel is not None

        kwargs = exchange.to_declare_kwargs()

        import aio_pika.exceptions

        try:
            if action is TopoAction.PASSIVE:
                await self._topology_channel.get_exchange(kwargs["exchange"], ensure=True)
            else:
                await self._topology_channel.declare_exchange(
                    name=kwargs["exchange"],
                    type=kwargs.get("exchange_type", "direct"),
                    durable=kwargs.get("durable", True),
                    auto_delete=kwargs.get("auto_delete", False),
                    internal=kwargs.get("internal", False),
                    arguments=kwargs.get("arguments"),
                )
        except aio_pika.exceptions.ChannelPreconditionFailed as exc:
            await self._handle_precondition_failed("exchange", kwargs["exchange"], exc)

    async def declare_queue(self, queue: RabbitQueue) -> None:
        """Declare a queue on the topology channel."""
        action = self._topo.queue_action(queue)
        if action is TopoAction.SKIP:
            return

        await self._ensure_connected()
        assert self._topology_channel is not None

        kwargs = queue.to_declare_kwargs()

        import aio_pika.exceptions

        try:
            if action is TopoAction.PASSIVE:
                await self._topology_channel.get_queue(kwargs["queue"], ensure=True)
            else:
                await self._topology_channel.declare_queue(
                    name=kwargs["queue"],
                    durable=kwargs.get("durable", True),
                    exclusive=kwargs.get("exclusive", False),
                    auto_delete=kwargs.get("auto_delete", False),
                    arguments=kwargs.get("arguments"),
                )
        except aio_pika.exceptions.ChannelPreconditionFailed as exc:
            await self._handle_precondition_failed("queue", kwargs["queue"], exc)

    async def _handle_precondition_failed(self, kind: str, name: str, exc: BaseException) -> None:
        """M6: turn a 406 PRECONDITION_FAILED into a typed, actionable error.

        Declaring a queue/exchange with arguments that conflict with an
        existing one of the same name (e.g. an ops-created quorum queue
        where rabbitkit's config declares classic, or a different TTL/DLX)
        closes the channel with reply_code 406 --
        ``aio_pika.exceptions.ChannelPreconditionFailed`` specifically.
        Previously this aborted startup with a low-level channel-closed
        traceback giving no hint which queue/exchange or argument actually
        conflicted.

        M14: under ``SafetyConfig.on_topology_conflict="warn_continue"`` the
        406 is logged and swallowed — a 406 (unlike a 404) proves the entity
        exists, so rabbitkit continues with the EXISTING definition. The
        conflict closed the topology channel, so we reopen it first.
        """
        if self._on_topology_conflict == "warn_continue":
            consumer_conn = await self._conn_pool.get_consumer_connection()
            self._topology_channel = await consumer_conn.channel()
            self._fire_channel_opened()
            self._fire_channel_rebuilt()  # replacing a channel the broker just closed
            logger.warning(
                "Topology drift on %s %r (broker: %s); on_topology_conflict='warn_continue' "
                "— continuing with the EXISTING definition (rabbitkit's declaration was NOT "
                "applied). Reconcile the %s or fix its rabbitkit config to silence this.",
                kind,
                name,
                exc,
                kind,
            )
            return
        from rabbitkit.core.topology import describe_declare_conflict  # [retry/topology hook]

        raise ConfigurationError(describe_declare_conflict(kind, name, str(exc))) from exc

    async def bind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """Bind a queue to an exchange on the topology channel.

        ``arguments`` carries header-match criteria for HEADERS exchanges
        (``x-match`` etc.) — without them a headers binding matches every
        message (C4).
        """
        if self._topo.binding_action() is TopoAction.SKIP:
            return

        await self._ensure_connected()
        assert self._topology_channel is not None

        q = await self._topology_channel.get_queue(queue, ensure=False)
        ex = await self._topology_channel.get_exchange(exchange, ensure=False)
        await q.bind(ex, routing_key=routing_key, arguments=arguments)
        # m3: get_queue/get_exchange(ensure=False) handles are NOT in
        # RobustChannel's restoration registry, so this binding would not be
        # re-applied by connect_robust recovery — an auto-delete/exclusive
        # queue recreated after a broker restart would come back UNBOUND
        # (consumer silently receives nothing). Record it; the reconnect
        # callback re-applies all recorded bindings (bind is idempotent).
        self._record_binding(("queue", queue, exchange, routing_key, arguments))

    async def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """Bind an exchange to another exchange on the topology channel."""
        if self._topo.binding_action() is TopoAction.SKIP:
            return

        await self._ensure_connected()
        assert self._topology_channel is not None

        dest_ex = await self._topology_channel.get_exchange(destination, ensure=False)
        src_ex = await self._topology_channel.get_exchange(source, ensure=False)
        await dest_ex.bind(src_ex, routing_key=routing_key, arguments=arguments)
        # m3: recorded for post-reconnect re-apply — see bind_queue.
        self._record_binding(("exchange", destination, source, routing_key, arguments))

    async def cancel_consumer(self, consumer_tag: str, *, timeout: float | None = None) -> None:
        """Cancel a consumer by tag, waiting at most *timeout* for the broker.

        A cancel the broker does not answer in time (a partition) is handled
        like a failed one: the consumer is forgotten locally and aio-pika's
        restore record is dropped, so a later reconnect cannot resurrect it.

        CONSUMER-007: the cancel goes through the queue object the consumer
        was started on. aio-pika's robust restore re-subscribes every
        consumer recorded on that object after a reconnect; cancelling via a
        fresh ``get_queue()`` wrapper left the record in place, so a
        reconnect during ``stop()``'s drain could bring the consumer back.
        """
        if not self._connected:
            return

        for queue_name, tag in list(self._consumer_tags.items()):
            if tag == consumer_tag:
                channel = self._consumer_channels.get(queue_name)
                record = self._consumer_supervisor.unregister(queue_name)
                consumer_conn = self._conn_pool._consumer_connection
                if consumer_conn is not None and (
                    isinstance(getattr(consumer_conn, "terminal_error", None), BaseException)
                    or bool(getattr(consumer_conn, "is_closed", False))
                ):
                    # G-08: the connection is closed for good (terminal
                    # failure). The broker dropped the consumer with it, and
                    # an AMQP cancel would wait forever for a reconnect.
                    # (aio-pika 9.1 never sets is_closed on a connection that
                    # is closed without a transport, hence terminal_error.)
                    del self._consumer_tags[queue_name]
                    self._consumer_channels.pop(queue_name, None)
                    if channel is not None and channel is self._reply_to_channel:
                        self._reply_to_channel = None
                elif channel is not None:
                    q: Any = None
                    try:
                        if record is not None:
                            q = record.queue_obj
                        else:
                            q = await channel.get_queue(queue_name, ensure=False)
                        async with asyncio.timeout(timeout):
                            await q.cancel(consumer_tag)
                    except Exception as e:
                        logger.warning("Failed to cancel consumer %s: %s", consumer_tag, e)
                        # aio-pika 9.x drops its restore record only after
                        # Basic.CancelOk; a cancel lost to a dropped connection
                        # must not be restored by the reconnect either.
                        restore_records = getattr(q, "_consumers", None)
                        if isinstance(restore_records, dict):
                            restore_records.pop(consumer_tag, None)
                    finally:
                        del self._consumer_tags[queue_name]
                        del self._consumer_channels[queue_name]
                        if channel is self._reply_to_channel:
                            self._reply_to_channel = None
                break

    async def _refuse_stray_delivery(self, record: ConsumerRecord, message: Any) -> None:
        """Requeue a delivery for a consumer rabbitkit already cancelled, and cancel it again."""
        logger.warning(
            "Delivery for cancelled consumer queue=%s tag=%s (restored by a reconnect); "
            "requeueing it and cancelling the consumer",
            record.queue,
            record.tag,
        )
        if not record.no_ack:
            with contextlib.suppress(Exception):
                await message.nack(requeue=True)
        with contextlib.suppress(Exception):
            await message.channel.basic_cancel(record.tag)

    def on_consumer_cancelled(self, callback: Callable[[str], None]) -> None:
        """Register ``callback(queue)`` for a consumer the BROKER cancelled (G-26).

        Fires when RabbitMQ sends ``basic.cancel`` (queue deleted, quorum
        leader change, ...). The transport re-subscribes when the queue exists
        again and then fires :meth:`on_consumer_resumed`.
        """
        self._consumer_supervisor.on_cancelled(callback)

    def on_consumer_resumed(self, callback: Callable[[str], None]) -> None:
        """Register ``callback(queue)`` for a broker-cancelled consumer that is consuming again."""
        self._consumer_supervisor.on_resumed(callback)

    @property
    def cancelled_consumers(self) -> frozenset[str]:
        """Queues whose consumer the broker cancelled and that are not consuming again yet."""
        return self._consumer_supervisor.cancelled_queues

    # ── DLQ / inspection (DLQInspector protocol) ──────────────────────────

    @property
    def server_version(self) -> str | None:
        """RabbitMQ version the broker reported at connect (``"4.1.8"``), or None."""
        conn = self._conn_pool._consumer_connection or self._conn_pool._publisher_connection
        underlay = getattr(getattr(conn, "transport", None), "connection", None)
        props = getattr(underlay, "server_properties", None)
        version = props.get("version") if isinstance(props, dict) else None
        if isinstance(version, bytes):
            version = version.decode(errors="replace")
        return str(version) if version else None

    async def queue_exists(self, queue: str) -> bool:
        """True if *queue* exists. Probes on a throwaway channel.

        A passive declare of a missing queue is a 404 that closes the
        channel it ran on, so it must not run on the topology channel.
        """
        await self._ensure_connected()
        import aio_pika.exceptions

        consumer_conn = await self._conn_pool.get_consumer_connection()
        channel = await consumer_conn.channel()
        try:
            await channel.declare_queue(queue, passive=True)
            return True
        except aio_pika.exceptions.ChannelNotFoundEntity:
            return False
        finally:
            if not channel.is_closed:
                with contextlib.suppress(Exception):
                    await channel.close()

    @contextlib.asynccontextmanager
    async def inspection_session(self) -> AsyncIterator[AsyncInspectionSession]:
        """A channel of its own for one peek/replay operation (issue #35).

        ``basic_get`` used the shared topology channel, so concurrent
        inspections interleaved on it, and one caller's channel error (a 404
        for a mistyped queue, a 406) closed it for everyone: the broker
        requeued every caller's held messages and their later acks failed.
        Closing the session's channel also returns anything still unsettled
        to the queue, so a cancelled inspection can't strand messages.
        """
        await self._ensure_connected()
        consumer_conn = await self._conn_pool.get_consumer_connection()
        channel = await consumer_conn.channel()
        try:
            yield AsyncInspectionSession(self, channel)
        finally:
            if not channel.is_closed:
                try:
                    await channel.close()
                except Exception:  # pragma: no cover — the connection is going anyway
                    logger.debug("closing the inspection channel raised", exc_info=True)

    async def basic_get(self, queue: str) -> RabbitMessage | None:
        """Get a single message without subscribing.

        Used by DLQInspector for peek/replay. Returns None if the queue is empty.
        """
        await self._ensure_connected()
        assert self._topology_channel is not None
        q = await self._topology_channel.get_queue(queue, ensure=False)
        aio_msg = await q.get(fail=False, no_ack=False)
        if aio_msg is None:
            return None
        return self._build_message(aio_msg)

    async def purge_queue(self, queue: str) -> int:
        """Purge all messages from a queue. Returns the number of messages purged."""
        await self._ensure_connected()
        assert self._topology_channel is not None
        q = await self._topology_channel.get_queue(queue, ensure=False)
        result = await q.purge()
        return int(getattr(result, "message_count", 0))

    # ── Internal ──────────────────────────────────────────────────────────

    def _build_message(self, aio_message: Any, *, no_ack: bool = False) -> RabbitMessage:
        """Build RabbitMessage from aio-pika IncomingMessage.

        ``no_ack=True`` (delivery came from a no-ack consumer) skips wiring
        settlement functions entirely — the broker already auto-acked the
        delivery, and aio-pika's ``IncomingMessage.ack()``/``nack()``/``reject()``
        raise ``TypeError`` on a no-ack message anyway.
        """
        message = RabbitMessage(
            body=aio_message.body,
            headers=dict(aio_message.headers) if aio_message.headers else {},
            message_id=aio_message.message_id,
            correlation_id=aio_message.correlation_id,
            reply_to=aio_message.reply_to,
            content_type=aio_message.content_type,
            content_encoding=aio_message.content_encoding,
            type=aio_message.type,
            app_id=aio_message.app_id,
            priority=aio_message.priority,
            # aio-pika decodes the wire's ms-string expiration into seconds
            # (float) on IncomingMessage; re-encode to the ms-string convention
            # RabbitMessage/MessageEnvelope.expiration use everywhere else (matches
            # the raw string pika.BasicProperties.expiration carries unmodified),
            # so a retry/DLQ-replay envelope built from this message round-trips
            # correctly regardless of which transport received it. round(), not
            # int(): "1001" decodes to 1.001 s, and int(1.001 * 1000) is 1000.
            expiration=(str(round(aio_message.expiration * 1000)) if aio_message.expiration is not None else None),
            user_id=aio_message.user_id,
            delivery_mode=(int(aio_message.delivery_mode) if aio_message.delivery_mode is not None else None),
            timestamp=aio_message.timestamp,  # was never surfaced on consume
            routing_key=aio_message.routing_key,
            exchange=aio_message.exchange or "",
            delivery_tag=aio_message.delivery_tag,
            redelivered=aio_message.redelivered,
            consumer_tag=aio_message.consumer_tag,
            raw_message=aio_message,
        )

        if no_ack:
            return message

        async def ack_fn() -> None:
            await aio_message.ack()

        async def nack_fn(requeue: bool = True) -> None:
            await aio_message.nack(requeue=requeue)

        async def reject_fn(requeue: bool = False) -> None:
            await aio_message.reject(requeue=requeue)

        message._ack_async_fn = ack_fn
        message._nack_async_fn = nack_fn
        message._reject_async_fn = reject_fn

        def _channel_alive() -> bool:
            ch = getattr(aio_message, "channel", None)
            if ch is None:
                return True  # unknown — do not claim staleness we cannot see
            return not bool(getattr(ch, "is_closed", False))

        message._channel_alive = _channel_alive

        return message


class AsyncInspectionSession:
    """``basic_get`` on one dedicated channel; see ``AsyncTransportImpl.inspection_session``."""

    def __init__(self, transport: AsyncTransportImpl, channel: Any) -> None:
        self._transport = transport
        self._channel = channel

    async def basic_get(self, queue: str) -> RabbitMessage | None:
        q = await self._channel.get_queue(queue, ensure=False)
        aio_msg = await q.get(fail=False, no_ack=False)
        if aio_msg is None:
            return None
        return self._transport._build_message(aio_msg)
