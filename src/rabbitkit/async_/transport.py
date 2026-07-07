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
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.async_.pool import AsyncConnectionPool
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig
from rabbitkit.core.errors import ConfigurationError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.topology_dispatch import TopoAction, TopologyDispatcher
from rabbitkit.core.types import (
    DIRECT_REPLY_TO_QUEUE,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    TopologyMode,
)

logger = logging.getLogger(__name__)


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

        self._conn_pool = AsyncConnectionPool(
            self._connection_config,
            self._security_config,
            self._pool_config,
            publisher_confirms=self._confirm_delivery,
        )
        self._connected = False

        # Per-queue consumer channels: queue_name -> aio_pika channel
        self._consumer_channels: dict[str, Any] = {}
        self._consumer_tags: dict[str, str] = {}  # queue_name -> consumer_tag

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

        # H1: dedicated, always-confirmed channel for mandatory=True publishes,
        # independent of confirm_delivery. Detecting an unroutable Basic.Return
        # reliably requires BOTH publisher confirms AND on_return_raises=True —
        # neither the fast channel (no confirms at all) nor the regular pool
        # (confirms follow confirm_delivery, which may be False) guarantee that.
        self._mandatory_publish_channel: Any | None = None
        # M3: in-flight mandatory publishes on the shared channel + a
        # deferred-recycle flag set by a confirm timeout (closed at zero).
        self._mandatory_in_flight = 0
        self._mandatory_channel_recycle = False
        # m3: bindings recorded for re-apply after robust reconnect (they are
        # not in RobustChannel's restoration registry — see bind_queue).
        self._recorded_bindings: list[tuple[str, str, str, str, dict[str, Any] | None]] = []
        self._binding_restore_tasks: set[Any] = set()
        self._mandatory_channel_lock: asyncio.Lock = asyncio.Lock()

        # Backpressure callbacks (FlowController registers here). Each is a
        # zero-arg callable; aio-pika's blocked/unblocked frames are adapted.
        self._blocked_callbacks: list[Callable[[], None]] = []
        self._unblocked_callbacks: list[Callable[[], None]] = []

        # Connection-churn metric hook (see on_reconnect) -- adapted from
        # aio-pika's RobustConnection.reconnect_callbacks.
        self._reconnect_callbacks: list[Callable[[], None]] = []

        # L15: passive blocked-state tracking, independent of whether a
        # FlowController is registered above -- health.broker_health_check
        # reads this (via the is_blocked property) so a broker/disk/memory
        # alarm is visible even when the caller never opted into FlowController.
        self._blocked_state: bool = False

    def on_reconnect(self, callback: Callable[[], None]) -> None:
        """Register a callback fired on every ``connect_robust`` re-connection
        (connection-churn metric hook). Reconnects were logged but never
        counted, so a flapping broker/network was invisible to metrics
        alerting."""
        self._reconnect_callbacks.append(callback)

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

    def _aio_blocked(self, *_args: Any) -> None:
        self._blocked_state = True
        for cb in list(self._blocked_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover - never break the event loop
                logger.exception("blocked callback raised")

    def _aio_unblocked(self, *_args: Any) -> None:
        self._blocked_state = False
        for cb in list(self._unblocked_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover
                logger.exception("unblocked callback raised")

    async def connect(self) -> None:
        """Establish publisher and consumer connections."""
        if self._connected:
            return

        await self._conn_pool.connect()

        # Register connection blocked/unblocked callbacks (C-6) so a
        # FlowController can throttle publishes when RabbitMQ raises an alarm.
        pub_conn = await self._conn_pool.get_publisher_connection()
        try:
            pub_conn.connection_blocked.add_callback(self._aio_blocked)
            pub_conn.connection_unblocked.add_callback(self._aio_unblocked)
        except Exception:  # pragma: no cover - older aio-pika may differ
            logger.debug("Could not register blocked/unblocked callbacks")

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

        # I-11: install a blocked-connection watchdog so a broker alarm that isn't
        # cleared within blocked_connection_timeout closes the connection (forcing
        # reconnect) — aio-pika has no native knob for this.
        # m2 (architect review): the CONSUMER connection gets the same
        # blocked hooks + watchdog — it carries the topology channel and the
        # direct-reply-to channel (RPC request publishes go out on it), so a
        # block there used to stall RPC with no watchdog and is_blocked False.
        from rabbitkit.async_.connection import install_blocked_connection_watchdog

        try:
            await install_blocked_connection_watchdog(pub_conn, self._connection_config.blocked_connection_timeout)
        except Exception:  # pragma: no cover - best effort
            logger.debug("Could not install publisher blocked-connection watchdog")
        # Separate try (verification gap 4): a publisher-watchdog failure
        # must not silently skip the consumer connection's wiring too.
        try:
            consumer_conn_wd = await self._conn_pool.get_consumer_connection()
            if consumer_conn_wd is not pub_conn:
                try:
                    consumer_conn_wd.connection_blocked.add_callback(self._aio_blocked)
                    consumer_conn_wd.connection_unblocked.add_callback(self._aio_unblocked)
                except Exception:  # pragma: no cover - older aio-pika may differ
                    logger.debug("Could not register consumer-connection blocked callbacks")
                await install_blocked_connection_watchdog(
                    consumer_conn_wd, self._connection_config.blocked_connection_timeout
                )
        except Exception:  # pragma: no cover - best effort
            logger.debug("Could not install consumer blocked-connection watchdog")

        # Open topology channel on consumer connection
        consumer_conn = await self._conn_pool.get_consumer_connection()
        self._topology_channel = await consumer_conn.channel()

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

    async def disconnect(self) -> None:
        """Close all channels and connections."""
        if not self._connected:
            return

        try:
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

            # Close the dedicated mandatory-publish channel (H1)
            if self._mandatory_publish_channel is not None and not self._mandatory_publish_channel.is_closed:
                try:
                    await self._mandatory_publish_channel.close()
                except Exception:  # pragma: no cover — best effort close, network errors only
                    pass
            self._mandatory_publish_channel = None

            await self._conn_pool.close_all()
        except Exception as e:
            logger.warning("Error during disconnect: %s", e)
        finally:
            self._connected = False
            logger.info("Disconnected from RabbitMQ (async)")

    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ.

        Reflects the real underlying robust-connection state rather than a
        stale cached flag: if our cached flag is False we return False;
        otherwise we inspect the robust connection's ``is_closed`` attribute
        (guarded) so a connection that aio-pika has silently dropped is not
        reported as healthy.
        """
        if not self._connected:
            return False
        conn = self._conn_pool._publisher_connection
        if conn is None:
            return False
        try:
            # RobustConnection exposes ``is_closed``; True means fully closed.
            if bool(getattr(conn, "is_closed", False)):
                return False
        except Exception:  # pragma: no cover — defensive
            return False
        return True

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
            self._fast_publish_channel = await conn.channel(publisher_confirms=False)
            return self._fast_publish_channel

    async def _get_mandatory_channel(self) -> Any:
        """Return the dedicated always-confirmed channel for mandatory=True
        publishes, (re)opening if needed.

        H1: ``on_return_raises=True`` makes an unroutable ``Basic.Return``
        raise ``aio_pika.exceptions.PublishError`` (caught by
        :meth:`_publish_on_channel` and mapped to ``PublishStatus.RETURNED``)
        instead of silently resolving the confirmation with the returned
        message — indistinguishable from success otherwise. This channel is
        used for every ``mandatory=True`` publish regardless of the broker's
        ``confirm_delivery`` setting, since reliable return detection needs
        confirms + on_return_raises unconditionally.
        """
        ch = self._mandatory_publish_channel
        if ch is not None and not ch.is_closed:
            return ch
        async with self._mandatory_channel_lock:
            ch = self._mandatory_publish_channel
            if ch is not None and not ch.is_closed:  # pragma: no cover — concurrent path
                return ch
            conn = self._conn_pool._publisher_connection
            if conn is None:
                raise RuntimeError("Publisher connection is not available")
            self._mandatory_publish_channel = await conn.channel(
                publisher_confirms=True, on_return_raises=True
            )
            return self._mandatory_publish_channel

    def _build_aio_message(self, envelope: MessageEnvelope) -> Any:
        """Build an aio_pika.Message from a MessageEnvelope."""
        import aio_pika

        return aio_pika.Message(
            body=envelope.body,
            message_id=envelope.message_id,
            correlation_id=envelope.correlation_id,
            reply_to=envelope.reply_to,
            content_type=envelope.content_type,
            content_encoding=envelope.content_encoding,
            headers=envelope.headers or None,
            delivery_mode=aio_pika.DeliveryMode(envelope.delivery_mode),
            priority=envelope.priority,
            expiration=(int(envelope.expiration) / 1000 if envelope.expiration else None),
            type=envelope.type,
            user_id=envelope.user_id,
            app_id=envelope.app_id,
            timestamp=envelope.timestamp,
        )

    async def _publish_on_channel(self, channel: Any, envelope: MessageEnvelope) -> PublishOutcome:
        """Publish *envelope* on an already-acquired channel.

        Used by BatchPublisher to publish many messages on one channel and
        gather all confirms concurrently — one pool acquire/release for N
        messages instead of N separate round-trips.

        H1: a ``mandatory=True`` publish that the broker cannot route raises
        ``aio_pika.exceptions.PublishError`` when the channel has
        ``on_return_raises=True`` (only true for channels obtained via
        :meth:`_get_mandatory_channel` — regular pool/fast channels default to
        ``on_return_raises=False`` and would otherwise resolve the confirmation
        with the returned message instead of raising, indistinguishable from
        success). Mapped to ``PublishStatus.RETURNED`` so callers keying off
        ``outcome.ok`` correctly treat it as a failed publish. A broker-side
        ``Basic.Nack`` (not a return) raises the more generic
        ``DeliveryError`` and maps to ``PublishStatus.NACKED``.
        """
        import aio_pika.exceptions

        message = self._build_aio_message(envelope)
        exchange = (
            await channel.get_exchange(envelope.exchange, ensure=False)
            if envelope.exchange
            else channel.default_exchange
        )
        try:
            async with asyncio.timeout(self._confirm_timeout):
                await exchange.publish(
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
            logger.warning("Publish confirm timed out after %.1fs", self._confirm_timeout)
            return PublishOutcome(
                status=PublishStatus.TIMEOUT,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
                error=e,
            )
        except aio_pika.exceptions.PublishError as e:
            logger.warning(
                "Publish returned as unroutable (mandatory=True, no matching binding): "
                "exchange=%s routing_key=%s",
                envelope.exchange,
                envelope.routing_key,
            )
            return PublishOutcome(
                status=PublishStatus.RETURNED,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
                error=e,
            )
        except aio_pika.exceptions.DeliveryError as e:
            logger.warning(
                "Publish nacked by broker: exchange=%s routing_key=%s",
                envelope.exchange,
                envelope.routing_key,
            )
            return PublishOutcome(
                status=PublishStatus.NACKED,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
                error=e,
            )
        return PublishOutcome(
            status=PublishStatus.CONFIRMED,
            exchange=envelope.exchange,
            routing_key=envelope.routing_key,
        )

    async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
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
        request) always publishes via the dedicated always-confirmed channel
        from :meth:`_get_mandatory_channel`, regardless of ``confirm_delivery``
        — see that method's docstring for why neither the fast nor the regular
        pool channel can reliably report an unroutable ``Basic.Return``.
        """
        try:
            if not self._connected:
                await self._ensure_connected()

            if envelope.reply_to == DIRECT_REPLY_TO_QUEUE and self._reply_to_channel is not None:
                channel = self._reply_to_channel
                if not channel.is_closed:
                    return await self._publish_on_channel(channel, envelope)

            if envelope.mandatory:
                channel = await self._get_mandatory_channel()
                # M3 (architect review): this single persistent channel is
                # shared by ALL concurrent mandatory publishes. Closing it the
                # instant OUR publish times out cascades channel-closed errors
                # into every sibling still awaiting its own confirm (spurious
                # NACKED/ERROR → caller retries → duplicates). Ref-count
                # in-flight publishes and recycle the channel only when the
                # last one resolves; _get_mandatory_channel() lazily reopens.
                self._mandatory_in_flight += 1
                try:
                    outcome = await self._publish_on_channel(channel, envelope)
                finally:
                    self._mandatory_in_flight -= 1
                if outcome.status == PublishStatus.TIMEOUT:
                    self._mandatory_channel_recycle = True
                current = self._mandatory_publish_channel
                if (
                    self._mandatory_channel_recycle
                    and self._mandatory_in_flight == 0
                    and current is not None
                    and channel is current
                    and not current.is_closed
                ):
                    self._mandatory_channel_recycle = False
                    with contextlib.suppress(Exception):
                        await current.close()
                return outcome

            if not self._confirm_delivery:
                # Fast path: persistent channel, no confirm wait, no pool overhead
                message = self._build_aio_message(envelope)
                channel = await self._get_fast_channel()
                exchange = (
                    await channel.get_exchange(envelope.exchange, ensure=False)
                    if envelope.exchange
                    else channel.default_exchange
                )
                await exchange.publish(
                    message,
                    routing_key=envelope.routing_key,
                    mandatory=envelope.mandatory,
                )
                # M4: SENT, not CONFIRMED -- this channel has publisher_confirms=False,
                # so nothing was actually acknowledged by the broker.
                return PublishOutcome(
                    status=PublishStatus.SENT,
                    exchange=envelope.exchange,
                    routing_key=envelope.routing_key,
                )

            # Confirmed path: pool channel per publish so each confirm is isolated
            channel = await self._conn_pool.acquire_publisher_channel()
            try:
                outcome = await self._publish_on_channel(channel, envelope)
                # M17: close a timed-out channel before returning it to the pool
                # so the pool doesn't hand out a possibly-wedged channel next.
                # This channel is exclusively ours for this single publish (not
                # shared concurrently), so closing here — after our own call has
                # fully resolved — is safe.
                if outcome.status == PublishStatus.TIMEOUT and not channel.is_closed:
                    with contextlib.suppress(Exception):
                        await channel.close()
                return outcome
            finally:
                await self._conn_pool.release_publisher_channel(channel)

        except Exception as e:
            logger.error("Async publish failed: %s", e)
            return PublishOutcome(
                status=PublishStatus.ERROR,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
                error=e,
            )
    async def consume(
        self,
        queue: str,
        callback: Callable[[RabbitMessage], Awaitable[None]],
        prefetch: int = 10,
        *,
        no_ack: bool = False,
        declare: bool = True,
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
        """
        await self._ensure_connected()

        # Dedicated channel per consumer queue for isolated QoS
        consumer_conn = await self._conn_pool.get_consumer_connection()
        channel = await consumer_conn.channel()
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
            rabbit_msg = self._build_message(message, no_ack=no_ack)
            await callback(rabbit_msg)

        await q.consume(on_message, consumer_tag=consumer_tag, no_ack=no_ack)
        self._consumer_tags[queue] = consumer_tag
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
        raise ConfigurationError(
            f"Cannot declare {kind} {name!r}: it already exists with incompatible "
            f"arguments (broker said: {exc}). This usually means it was created "
            f"outside rabbitkit (e.g. ops tooling) with different arguments (e.g. "
            f"quorum vs classic queue type, a different TTL, or a different "
            f"dead-letter exchange). Either delete/reconcile the existing {kind}, "
            f"adjust its rabbitkit definition to match, or use "
            f"TopologyMode.PASSIVE_ONLY to skip declaration and just verify it exists."
        ) from exc

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
        self._recorded_bindings.append(("queue", queue, exchange, routing_key, arguments))

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
        self._recorded_bindings.append(("exchange", destination, source, routing_key, arguments))

    async def cancel_consumer(self, consumer_tag: str) -> None:
        """Cancel a consumer by tag."""
        if not self._connected:
            return

        for queue_name, tag in list(self._consumer_tags.items()):
            if tag == consumer_tag:
                channel = self._consumer_channels.get(queue_name)
                if channel is not None:
                    try:
                        q = await channel.get_queue(queue_name, ensure=False)
                        await q.cancel(consumer_tag)
                    except Exception as e:
                        logger.warning("Failed to cancel consumer %s: %s", consumer_tag, e)
                    finally:
                        del self._consumer_tags[queue_name]
                        del self._consumer_channels[queue_name]
                        if channel is self._reply_to_channel:
                            self._reply_to_channel = None
                break

    # ── DLQ / inspection (DLQInspector protocol) ──────────────────────────

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
        # correctly regardless of which transport received it.
        expiration=(str(int(aio_message.expiration * 1000)) if aio_message.expiration is not None else None),
        user_id=aio_message.user_id,
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

        return message
