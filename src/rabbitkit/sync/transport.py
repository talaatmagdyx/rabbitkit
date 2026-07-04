"""SyncTransport — pika-based transport adapter.

THREAD SAFETY (CRITICAL):
Model A — One connection per thread (used in 0.1.0):
Each thread gets its own dedicated pika connection.
No cross-thread connection sharing.

Fork safety: lazy connect (NOT in __init__) — pika sockets can't cross fork().
Reconnection: _ensure_connected() before each publish.
TopologyMode: respected in declare_exchange/declare_queue/bind_queue.
"""

from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeVar

from rabbitkit.core.config import ConnectionConfig, SecurityConfig, SocketConfig
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
from rabbitkit.sync.connection import get_connection_errors, make_pika_connection_params

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class SyncTransport:
    """Pika-based synchronous transport.

    Lazy connect: connection is established on first use, not in __init__.
    This ensures fork safety and avoids connection issues during import.

    THE INVARIANT: no pika connection is ever used from a thread other than
    the one that created it.
    """

    def __init__(
        self,
        connection_config: ConnectionConfig | None = None,
        socket_config: SocketConfig | None = None,
        security_config: SecurityConfig | None = None,
        topology_mode: TopologyMode = TopologyMode.AUTO_DECLARE,
        confirm_delivery: bool = True,
        confirm_timeout: float = 5.0,
        on_topology_conflict: str = "raise",
    ) -> None:
        self._connection_config = connection_config or ConnectionConfig()
        self._socket_config = socket_config or SocketConfig()
        self._security_config = security_config or SecurityConfig()
        self._topology_mode = topology_mode
        self._topo = TopologyDispatcher(topology_mode)
        # M14: "raise" | "warn_continue" on a 406 topology-drift conflict.
        self._on_topology_conflict = on_topology_conflict
        self._confirm_delivery = confirm_delivery
        # I-10: bound the publish+confirm wait so a missing confirm cannot stall
        # a worker forever. Brokers should pass ``config.publisher.confirm_timeout``
        # here; the default is a sane fallback.
        self._confirm_timeout = float(confirm_timeout)

        self._connection: Any = None  # pika.BlockingConnection
        self._channel: Any = None  # pika.channel.Channel (publisher/topology)
        self._connected = False
        self._consumer_tags: dict[str, str] = {}  # queue_name → consumer_tag
        self._owner_ident: int | None = None  # thread that owns the connection
        self._consuming = False  # True while the I/O loop is running
        # H2: True once start_consuming() has ever run on this connection, and
        # never reset to False until disconnect(). Unlike _consuming (which
        # goes False the instant the loop stops pumping — including during
        # SyncBroker.stop()'s worker-pool drain, while workers may still be
        # mid-handler), this stays True for the connection's whole lifetime
        # once a consume loop has run. _run_on_io_thread uses THIS (not
        # _consuming) to decide whether a cross-thread call must marshal —
        # a worker thread's ack must never run inline just because the loop
        # has momentarily stopped pumping (see _run_on_io_thread).
        self._ever_consumed = False

        # Per-queue consumer channels (H-SRE1): each queue gets its own channel
        # so per-queue basic_qos does not overwrite other consumers and fair
        # dispatch is preserved. The publisher/topology channel stays separate.
        self._consumer_channels: dict[str, Any] = {}

        # The channel currently consuming DIRECT_REPLY_TO_QUEUE (set by
        # consume(declare=False), cleared on cancel/disconnect). RabbitMQ's
        # direct reply-to requires the reply consumer and the corresponding
        # request publish to happen on the SAME channel (a publish on a
        # different channel raises "PRECONDITION_FAILED - fast reply consumer
        # does not exist") — publish() checks this to route RPC requests
        # correctly without RPCClient needing to know about channels at all.
        self._reply_to_channel: Any = None

        # H1: channels (by id) that have had confirm_delivery() enabled.
        # Detecting an unroutable Basic.Return via pika's UnroutableError
        # requires confirms — in non-confirm mode basic_publish() has no way
        # to report a return at all (see pika's own basic_publish docstring).
        # A mandatory=True publish upgrades its target channel to confirm mode
        # on demand (once, idempotently) regardless of confirm_delivery.
        self._confirmed_channel_ids: set[int] = set()

        # Backpressure callbacks (FlowController registers here). Each is a
        # zero-arg callable; pika's blocked/unblocked frames are adapted to it.
        self._blocked_callbacks: list[Callable[[], None]] = []
        self._unblocked_callbacks: list[Callable[[], None]] = []

        # L15: passive blocked-state tracking, independent of whether a
        # FlowController is registered above -- health.broker_health_check
        # reads this (via the is_blocked property) so a broker/disk/memory
        # alarm is visible even when the caller never opted into FlowController.
        self._blocked_state: bool = False

        # L14: fired once per start_consuming() loop iteration (after each
        # process_data_events() call returns), i.e. once per I/O loop tick --
        # NOT once per delivered message. The broker uses this to refresh a
        # liveness heartbeat so a healthy but message-idle consumer doesn't
        # get mistaken for a wedged one (broker_liveness previously only saw
        # a heartbeat update when a message was actually delivered).
        self._io_tick_callbacks: list[Callable[[], None]] = []

        # Reconnect bound (H-SRE4): never retry forever. Hardcoded sane default;
        # the broker may override via attribute if desired.
        self.max_reconnect_attempts: int = 0  # 0 == use the time-bounded default below
        self._reconnect_total_timeout: float = 300.0

        # Connection-churn signal: reconnects were logged but never counted,
        # so a flapping broker/network was invisible to metrics alerting.
        # Fired on every successful connect() AFTER the first (see connect()).
        self._reconnect_callbacks: list[Callable[[], None]] = []
        self._ever_connected = False

    def on_reconnect(self, callback: Callable[[], None]) -> None:
        """Register a callback fired on every re-connection after the first
        successful connect (connection-churn metric hook)."""
        self._reconnect_callbacks.append(callback)

    def _fire_reconnect(self) -> None:
        for cb in list(self._reconnect_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover — never let a cb break connect
                logger.exception("reconnect callback raised")

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

    def on_io_tick(self, callback: Callable[[], None]) -> None:
        """Register a callback fired once per ``start_consuming()`` loop
        iteration (L14) -- e.g. the broker's liveness heartbeat refresh."""
        self._io_tick_callbacks.append(callback)

    def _fire_io_tick(self) -> None:
        for cb in list(self._io_tick_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover — never let a cb break the I/O loop
                logger.exception("io_tick callback raised")

    def _pika_blocked(self, _connection: Any, *_args: Any) -> None:
        self._blocked_state = True
        for cb in list(self._blocked_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover — never let a cb break the I/O loop
                logger.exception("blocked callback raised")

    def _pika_unblocked(self, _connection: Any, *_args: Any) -> None:
        self._blocked_state = False
        for cb in list(self._unblocked_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover
                logger.exception("unblocked callback raised")

    def connect(self) -> None:
        """Establish connection to RabbitMQ."""
        if self._connected:
            return

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

        logger.info(
            "Connecting to RabbitMQ at %s:%d",
            self._connection_config.host,
            self._connection_config.port,
        )

        self._connection = pika.BlockingConnection(params)
        # Publisher/topology channel (confirm_delivery for publisher confirms).
        self._channel = self._connection.channel()
        if self._confirm_delivery:
            self._channel.confirm_delivery()
            self._confirmed_channel_ids.add(id(self._channel))

        # Register connection blocked/unblocked callbacks (C-6) so a
        # FlowController can throttle publishes when RabbitMQ raises an alarm.
        try:
            self._connection.add_on_connection_blocked_callback(self._pika_blocked)
            self._connection.add_on_connection_unblocked_callback(self._pika_unblocked)
        except Exception:  # pragma: no cover — older pika may lack these
            logger.debug("Could not register blocked/unblocked callbacks")

        self._connected = True
        self._owner_ident = threading.get_ident()
        if self._ever_connected:
            self._fire_reconnect()  # connection-churn metric hook
        self._ever_connected = True
        logger.info("Connected to RabbitMQ")

    def __enter__(self) -> SyncTransport:
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.disconnect()

    def disconnect(self) -> None:
        """Close connection to RabbitMQ."""
        if not self._connected:
            return

        try:
            # Close per-queue consumer channels first
            for ch in list(self._consumer_channels.values()):
                try:
                    if ch.is_open:
                        ch.close()
                except Exception:  # pragma: no cover — best effort
                    pass
            self._consumer_channels.clear()
            self._consumer_tags = {}

            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            logger.warning("Error during disconnect: %s", e)
        finally:
            self._connection = None
            self._channel = None
            self._reply_to_channel = None
            self._confirmed_channel_ids.clear()
            self._connected = False
            self._owner_ident = None
            self._ever_consumed = False
            logger.info("Disconnected from RabbitMQ")

    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ."""
        if not self._connected:
            return False
        try:
            return (
                self._connection is not None
                and self._connection.is_open
                and self._channel is not None
                and self._channel.is_open
            )
        except Exception:
            return False

    @property
    def has_open_channels(self) -> bool:
        """True when at least one consumer channel is open and all are open.

        Transport-contract attribute (I-5) consumed by
        :func:`rabbitkit.health._transport_consumers_alive`: when this is
        ``False``, registered ``consumer_tag``s are treated as stale and the
        health/readiness probes drop the consumer count. Backed by
        ``self._consumer_channels`` so it reflects the per-queue channels
        actually held by this transport.
        """
        channels = self._consumer_channels
        return bool(channels) and all(ch.is_open for ch in channels.values())

    def _ensure_connected(self) -> None:
        """Ensure connection is established, reconnecting if needed.

        Uses exponential backoff with full jitter (H-SRE3) to avoid the
        thundering-herd problem when many clients reconnect at once, and is
        bounded by a total-time/attempt cap (H-SRE4) so the loop can never
        retry forever — on exhaustion it raises so the broker's run() recovery
        can decide what to do.
        """
        if self.is_connected():
            return

        self._connected = False
        backoff = self._connection_config.reconnect_backoff_base
        max_backoff = self._connection_config.reconnect_backoff_max
        connection_errors = get_connection_errors()

        # Bounded reconnect: never infinite (H-SRE4). Hardcoded sane defaults.
        max_attempts = self.max_reconnect_attempts or 30
        total_deadline = time.monotonic() + self._reconnect_total_timeout
        attempts = 0

        while True:
            try:
                self.connect()
                return
            except connection_errors as e:
                attempts += 1
                # Full jitter: sleep a random fraction of the current backoff to
                # spread reconnects across clients (H-SRE3).
                sleep_for = random.uniform(0.0, backoff)  # noqa: S311
                logger.warning(
                    "Connection failed, retrying in %.2fs (attempt %d): %s",
                    sleep_for,
                    attempts,
                    e,
                )
                time.sleep(sleep_for)
                backoff = min(backoff * 2, max_backoff)
                if attempts >= max_attempts or time.monotonic() >= total_deadline:
                    logger.critical(
                        "Reconnect attempts exhausted after %d tries / %.0fs; giving up",
                        attempts,
                        self._reconnect_total_timeout,
                    )
                    raise

    def reconnect(self) -> None:
        """Force a fresh connection + channel (used by consumer recovery)."""
        self.disconnect()
        self._ensure_connected()

    def ensure_connected(self) -> None:
        """Public wrapper for :meth:`_ensure_connected` (idle-pump support).

        Unlike :meth:`reconnect`, this is a no-op if already connected —
        cheap to call on every tick of an idle-pump loop (see
        ``SyncBroker.pump_idle``). Reconnects (bounded backoff) only when
        the connection or channel is actually dead.
        """
        self._ensure_connected()

    def _run_on_io_thread(
        self,
        fn: Callable[[], _T],
        *,
        timeout: float = 30.0,
    ) -> _T:
        """Run a channel operation on the connection's I/O thread.

        pika's BlockingConnection is NOT thread-safe: every basic_* call must
        execute on the thread that owns the connection. When a worker thread
        (worker_count > 1) acks/nacks/publishes, marshal the call onto the I/O
        loop via add_callback_threadsafe and block for its result/exception.
        When already on the owner thread (single worker / publisher), or when
        no consume loop has EVER run on this connection (a pure producer with
        no consumers — nothing else can be concurrently driving the socket),
        run inline.

        H2: deliberately does NOT fall back to inline just because the I/O
        loop has momentarily stopped pumping (``not self._consuming``) once a
        consume loop has run at least once (``self._ever_consumed``) — that
        used to be true for the whole SyncBroker.stop() drain window (consumers
        already cancelled, worker pool still finishing in-flight handlers),
        so a worker thread's ack/nack/reject ran INLINE, cross-thread, on the
        pika connection — unsynchronized with, and possibly concurrent with,
        other worker threads' acks on the same consumer channel or the owner
        thread's own disconnect(). Once ``_ever_consumed`` is True we always
        marshal and rely on the owner thread pumping the I/O loop during drain
        (see ``pump()``, called from ``SyncBroker.stop()``); if nothing pumps,
        we fail fast with ``TimeoutError`` below rather than run unsafely.

        *timeout* bounds the wait for the I/O loop to drain the callback (R-3):
        on expiry we raise ``TimeoutError`` AND mark the callback cancelled so a
        late drain (after the caller has already nacked+requeued and moved on)
        becomes a no-op instead of settling an already-redelivered message.
        """
        if (
            self._owner_ident is None
            or threading.get_ident() == self._owner_ident
            or not self._ever_consumed
        ):
            return fn()

        result: list[_T] = []
        error: list[BaseException] = []
        done = threading.Event()
        # R-3: set when the caller gives up waiting, so a later _cb drain is a
        # no-op rather than settling an already-redelivered message.
        cancelled = threading.Event()

        def _cb() -> None:
            if cancelled.is_set():
                # The caller already timed out and moved on (nack+requeue).
                # Running fn() now could double-settle, so drop the late callback.
                return
            try:
                result.append(fn())
            except BaseException as exc:  # re-raised on the caller thread
                error.append(exc)
            finally:
                done.set()

        # ponytail: blocks until the I/O loop drains the callback. Bound the
        # wait so a stalled/dead I/O loop can't pin the worker thread forever —
        # on expiry we raise TimeoutError so the pipeline exception handler can
        # nack+requeue and the worker is freed. 30s is well beyond any healthy
        # round-trip (H-P7); the publish path passes a tighter bound (I-10).
        io_stall_timeout = timeout
        self._connection.add_callback_threadsafe(_cb)
        if not done.wait(timeout=io_stall_timeout):
            cancelled.set()
            raise TimeoutError(
                f"Timed out after {io_stall_timeout}s waiting for the pika I/O "
                "loop to drain a cross-thread callback (connection stalled?)"
            )
        if error:
            raise error[0]
        return result[0]

    def _publish_confirm_wait_bounded(self, fn: Callable[[], _T], timeout: float) -> _T:
        """Bound a blocking publish call that would otherwise run fully
        inline and unbounded (I-11).

        pika's ``BlockingChannel.basic_publish()`` takes no timeout
        parameter, and its confirm-wait loops via ``process_data_events``
        with no aggregate time limit -- a broker that accepts the TCP
        connection but never sends the confirm frame back (disk full,
        internally wedged) hangs this call forever, `confirm_timeout`
        notwithstanding, whenever ``_run_on_io_thread`` would otherwise run
        it inline (single-worker/pure-producer case — see
        ``_publish_on_channel``, the cross-thread marshal case is already
        bounded by ``_run_on_io_thread`` itself).

        Runs *fn* on a dedicated one-shot thread and bounds OUR wait for it
        (same R-3 shape as ``_run_on_io_thread``). On timeout, that thread
        may still be blocked inside pika, touching the connection -- never
        safe to touch that connection from any other thread afterward
        (pika's ``BlockingConnection`` supports exactly one thread at a
        time), so it is poisoned (all references dropped, never closed —
        closing would itself be a second thread touching it).
        ``_ensure_connected()`` transparently creates a fresh connection on
        the next call, the same recovery path as a genuine network failure.

        Only called when no consume loop can be sharing this connection
        (see the call site) -- if one were, resuming ``start_consuming()``
        after giving up would immediately recreate the exact concurrent-
        touch hazard this method exists to avoid.
        """
        result: list[_T] = []
        error: list[BaseException] = []
        done = threading.Event()

        def _run() -> None:
            try:
                result.append(fn())
            except BaseException as exc:
                error.append(exc)
            finally:
                done.set()

        threading.Thread(target=_run, name="rabbitkit-publish-confirm-wait", daemon=True).start()
        if not done.wait(timeout=timeout):
            self._poison_wedged_connection()
            raise TimeoutError(
                f"Timed out after {timeout}s waiting for a publish confirm; connection "
                "presumed wedged and will be re-established on the next call"
            )
        if error:
            raise error[0]
        return result[0]

    def _poison_wedged_connection(self) -> None:
        """Drop all references to a connection a timed-out background
        publish (I-11) may still be touching. Never call ``.close()`` or
        otherwise touch the pika objects here -- that would itself be a
        second thread concurrently touching a ``BlockingConnection``, which
        pika does not support. The abandoned background thread's eventual
        completion (or, rarely, permanent hang) only ever touches its own
        locally-captured references and no longer affects anything here.
        """
        self._connection = None
        self._channel = None
        self._reply_to_channel = None
        self._consumer_channels = {}
        self._consumer_tags = {}
        self._confirmed_channel_ids = set()
        self._connected = False
        self._owner_ident = None
        self._ever_consumed = False

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Publish a message to RabbitMQ.

        Returns PublishOutcome with status indicating success/failure.

        A request with ``reply_to=DIRECT_REPLY_TO_QUEUE`` (RPCClient's direct
        reply-to requests) is routed onto ``self._reply_to_channel`` — the same
        channel that registered the reply consumer — rather than the default
        publisher channel. RabbitMQ requires this exact channel affinity for
        direct reply-to; publishing on a different channel raises
        "PRECONDITION_FAILED - fast reply consumer does not exist".
        """
        self._ensure_connected()

        channel = self._channel
        if (
            envelope.reply_to == DIRECT_REPLY_TO_QUEUE
            and self._reply_to_channel is not None
            and self._reply_to_channel.is_open
        ):
            channel = self._reply_to_channel

        return self._publish_on_channel(channel, envelope)

    def _ensure_mandatory_confirms(self, channel: Any) -> None:
        """Enable publisher confirms on *channel* if not already active.

        H1: detecting an unroutable ``Basic.Return`` via pika's
        ``UnroutableError`` requires confirm mode — in non-confirm mode
        ``basic_publish()`` has no way to report a return at all. Idempotent
        and tracked per-channel (by id) so a repeat call is a no-op rather than
        pika logging a spurious "confirmation was already enabled" error.
        Marshaled like ``basic_publish`` since it drives blocking I/O.
        """
        if id(channel) in self._confirmed_channel_ids:
            return
        self._run_on_io_thread(channel.confirm_delivery)
        self._confirmed_channel_ids.add(id(channel))

    def _publish_on_channel(self, channel: Any, envelope: MessageEnvelope) -> PublishOutcome:
        """Publish *envelope* on a specific already-open channel."""
        try:
            import pika

            if envelope.mandatory:
                self._ensure_mandatory_confirms(channel)

            properties = pika.BasicProperties(
                message_id=envelope.message_id,
                correlation_id=envelope.correlation_id,
                reply_to=envelope.reply_to,
                content_type=envelope.content_type,
                content_encoding=envelope.content_encoding,
                headers=envelope.headers or None,
                delivery_mode=envelope.delivery_mode,
                priority=envelope.priority,
                expiration=envelope.expiration,
                type=envelope.type,
                user_id=envelope.user_id,
                app_id=envelope.app_id,
            )

            if envelope.timestamp:
                properties.timestamp = int(envelope.timestamp.timestamp())

            # I-10: bound the publish+confirm wait by confirm_timeout so a
            # missing confirm cannot stall the worker forever.
            publish_timeout = min(30.0, self._confirm_timeout)
            def do_publish() -> None:
                channel.basic_publish(
                    exchange=envelope.exchange,
                    routing_key=envelope.routing_key,
                    body=envelope.body,
                    properties=properties,
                    mandatory=envelope.mandatory,
                )
            if threading.get_ident() == self._owner_ident and self._ever_consumed:
                # I-11: this thread also owns dispatching further deliveries
                # via start_consuming() -- cannot safely bound this wait on a
                # separate thread (see _publish_confirm_wait_bounded's
                # docstring: resuming start_consuming() the instant we gave
                # up would immediately touch a connection our own abandoned
                # helper thread might still be using). Documented residual
                # limitation: pika's BlockingChannel has no native way to
                # bound a confirm wait from the owner thread itself. Mitigate
                # by using worker_count > 1, so a handler's publish marshals
                # through the already-bounded cross-thread path instead.
                do_publish()
            elif self._owner_ident is None or not self._ever_consumed:
                # No consume loop can be sharing this connection (pure
                # producer, or nothing has ever consumed yet) -- safe to
                # bound with a dedicated helper thread.
                self._publish_confirm_wait_bounded(do_publish, timeout=publish_timeout)
            else:
                # Cross-thread: marshal onto the owner's I/O loop, which
                # _run_on_io_thread already bounds by confirm_timeout.
                self._run_on_io_thread(do_publish, timeout=publish_timeout)

            # M4: only report CONFIRMED when the channel is actually in
            # publisher-confirm mode -- confirm_delivery=False (unless this
            # publish is `mandatory`, which always enables confirms via
            # _ensure_mandatory_confirms above) means basic_publish() is
            # fire-and-forget and nothing was broker-acknowledged.
            confirmed = self._confirm_delivery or envelope.mandatory
            return PublishOutcome(
                status=PublishStatus.CONFIRMED if confirmed else PublishStatus.SENT,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
            )

        except pika.exceptions.UnroutableError as e:
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

        except pika.exceptions.NackError as e:
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

        except TimeoutError as e:
            # I-10: basic_publish() blocks synchronously for the broker confirm
            # (in confirm mode); _run_on_io_thread bounds that wait by
            # confirm_timeout and raises TimeoutError on expiry -- exactly the
            # "no confirm arrived in time" case docs/message-safety.md documents
            # as PublishStatus.TIMEOUT (matching the async transport's
            # equivalent asyncio.timeout(confirm_timeout) branch). This used to
            # fall through to the generic ERROR branch below, so a caller
            # correctly checking `status == PublishStatus.TIMEOUT` per the
            # documented contract silently never saw it on the sync transport.
            logger.warning(
                "Publish confirm timed out: exchange=%s routing_key=%s",
                envelope.exchange,
                envelope.routing_key,
            )
            return PublishOutcome(
                status=PublishStatus.TIMEOUT,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
                error=e,
            )

        except Exception as e:
            logger.error("Publish failed: %s", e)
            return PublishOutcome(
                status=PublishStatus.ERROR,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
                error=e,
            )

    def consume(
        self,
        queue: str,
        callback: Callable[[RabbitMessage], None],
        prefetch: int = 10,
        *,
        no_ack: bool = False,
        declare: bool = True,
    ) -> str:
        """Start consuming from a queue.

        Each queue gets its OWN channel so per-queue ``basic_qos`` is isolated
        and no longer overwrites other consumers' prefetch (H-SRE1). The
        publisher/topology channel stays separate. Returns the consumer tag.

        ``no_ack=True`` starts a no-ack consumer: the broker auto-acks on
        delivery, and the built ``RabbitMessage`` is not wired with settlement
        functions (there is nothing to ack/nack/reject). The sync path never
        declares the queue here regardless of ``declare`` (declaration is the
        caller's responsibility via ``declare_queue()``); when ``declare=False``
        and ``queue == DIRECT_REPLY_TO_QUEUE``, this consumer's channel is also
        remembered as ``self._reply_to_channel`` so :meth:`publish` can route
        matching requests onto the SAME channel — required by RabbitMQ's direct
        reply-to (see :meth:`publish`).
        """
        self._ensure_connected()

        # Dedicated channel per consumer queue for isolated QoS / fair dispatch.
        consumer_channel = self._connection.channel()
        consumer_channel.basic_qos(prefetch_count=prefetch)
        self._consumer_channels[queue] = consumer_channel

        if not declare and queue == DIRECT_REPLY_TO_QUEUE:
            self._reply_to_channel = consumer_channel

        consumer_tag = f"rabbitkit.{uuid.uuid4()}"

        def on_message(ch: Any, method: Any, properties: Any, body: bytes) -> None:
            """Internal pika callback — builds RabbitMessage and calls user callback."""
            message = self._build_message(ch, method, properties, body, no_ack=no_ack)
            callback(message)

        consumer_channel.basic_consume(
            queue=queue,
            on_message_callback=on_message,
            auto_ack=no_ack,
            consumer_tag=consumer_tag,
        )

        self._consumer_tags[queue] = consumer_tag
        logger.info("Started consuming from queue '%s' with tag '%s'", queue, consumer_tag)
        return consumer_tag

    def declare_exchange(self, exchange: RabbitExchange) -> None:
        """Declare an exchange on RabbitMQ."""
        action = self._topo.exchange_action(exchange)
        if action is TopoAction.SKIP:
            return

        self._ensure_connected()

        kwargs = exchange.to_declare_kwargs()

        import pika

        try:
            if action is TopoAction.PASSIVE:
                self._channel.exchange_declare(
                    exchange=kwargs["exchange"],
                    passive=True,
                )
            else:
                self._channel.exchange_declare(**kwargs)
        except pika.exceptions.ChannelClosedByBroker as exc:
            self._raise_precondition_failed_or_reraise("exchange", kwargs["exchange"], exc)

    def declare_queue(self, queue: RabbitQueue) -> None:
        """Declare a queue on RabbitMQ."""
        action = self._topo.queue_action(queue)
        if action is TopoAction.SKIP:
            return

        self._ensure_connected()

        kwargs = queue.to_declare_kwargs()

        import pika

        try:
            if action is TopoAction.PASSIVE:
                self._channel.queue_declare(
                    queue=kwargs["queue"],
                    passive=True,
                )
            else:
                self._channel.queue_declare(**kwargs)
        except pika.exceptions.ChannelClosedByBroker as exc:
            self._raise_precondition_failed_or_reraise("queue", kwargs["queue"], exc)

    def _raise_precondition_failed_or_reraise(self, kind: str, name: str, exc: Any) -> None:
        """M6: turn a 406 PRECONDITION_FAILED into a typed, actionable error.

        Declaring a queue/exchange with arguments that conflict with an
        existing one of the same name (e.g. an ops-created quorum queue
        where rabbitkit's config declares classic, or a different TTL/DLX)
        closes the channel with reply_code 406 and an opaque
        ``ChannelClosedByBroker`` — previously this aborted startup with a
        low-level pika traceback giving no hint which queue/exchange or
        argument actually conflicted. Any other reply code is re-raised
        as-is (not this middleware's concern).

        M14: under ``SafetyConfig.on_topology_conflict="warn_continue"`` a 406
        is logged and swallowed — the entity already exists (a 406, unlike a
        404, proves existence), so rabbitkit continues with the EXISTING
        definition instead of crash-looping. The 406 closed the channel, so
        we reopen it first (connection stays open) for subsequent declares.
        """
        if exc.reply_code == 406 and self._on_topology_conflict == "warn_continue":
            # Reopen the broker-closed channel so the rest of topology
            # declaration can proceed on the existing (drifted) entity.
            self._channel = self._connection.channel()
            self._confirmed_channel_ids.discard(id(self._channel))
            logger.warning(
                "Topology drift on %s %r (broker: %s); on_topology_conflict='warn_continue' "
                "— continuing with the EXISTING definition (rabbitkit's declaration was NOT "
                "applied). Reconcile the %s or fix its rabbitkit config to silence this.",
                kind,
                name,
                exc.reply_text,
                kind,
            )
            return
        if exc.reply_code == 406:
            raise ConfigurationError(
                f"Cannot declare {kind} {name!r}: it already exists with incompatible "
                f"arguments (broker said: {exc.reply_text}). This usually means it was "
                f"created outside rabbitkit (e.g. ops tooling) with different arguments "
                f"(e.g. quorum vs classic queue type, a different TTL, or a different "
                f"dead-letter exchange). Either delete/reconcile the existing {kind}, "
                f"adjust its rabbitkit definition to match, or use "
                f"TopologyMode.PASSIVE_ONLY to skip declaration and just verify it exists."
            ) from exc
        raise exc

    def bind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """Bind a queue to an exchange.

        ``arguments`` carries header-match criteria for HEADERS exchanges
        (``x-match`` etc.) — without them a headers binding matches every
        message (C4).
        """
        if self._topo.binding_action() is TopoAction.SKIP:
            return

        self._ensure_connected()

        self._channel.queue_bind(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            arguments=arguments,
        )

    def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """Bind an exchange to another exchange (exchange-to-exchange binding)."""
        if self._topo.binding_action() is TopoAction.SKIP:
            return

        self._ensure_connected()

        self._channel.exchange_bind(
            destination=destination,
            source=source,
            routing_key=routing_key,
            arguments=arguments,
        )

    def cancel_consumer(self, consumer_tag: str) -> None:
        """Cancel a consumer by tag."""
        if not self.is_connected():
            return

        # Find the queue whose channel owns this consumer_tag (per-consumer
        # channels - H-SRE1), cancel on THAT channel, then drop it.
        queue_name: str | None = None
        for q, tag in self._consumer_tags.items():
            if tag == consumer_tag:
                queue_name = q
                break

        if queue_name is None:
            return

        channel = self._consumer_channels.get(queue_name)
        try:
            if channel is not None and channel.is_open:
                channel.basic_cancel(consumer_tag=consumer_tag)
        except Exception as e:
            logger.warning("Failed to cancel consumer %s: %s", consumer_tag, e)
        finally:
            try:
                if channel is not None and channel.is_open:
                    channel.close()
            except Exception:  # pragma: no cover - best effort
                pass
            self._consumer_tags.pop(queue_name, None)
            self._consumer_channels.pop(queue_name, None)
            if channel is self._reply_to_channel:
                self._reply_to_channel = None

    def start_consuming(self) -> None:
        """Start the pika consume loop (blocking).

        Drives the connection's I/O loop directly via ``process_data_events`` so
        consumers on ANY channel (the per-queue ``_consumer_channels`` from H-SRE1)
        are processed. pika's ``channel.start_consuming()`` only loops while *that*
        channel has consumers, which would exit immediately for the publisher
        channel under the per-consumer-channel design — so we must not use it.
        """
        self._ensure_connected()
        self._consuming = True
        self._ever_consumed = True
        self._owner_ident = threading.get_ident()
        try:
            while self._consuming:
                # process_data_events drains ALL channels' consumers + queued
                # add_callback_threadsafe callbacks (acks from worker threads).
                try:
                    self._connection.process_data_events(time_limit=1.0)
                except ValueError as exc:
                    # pika's SelectConnection ioloop raises a bare
                    # ValueError("Timeout closed before call") when the
                    # connection died between poll ticks (e.g. broker restart).
                    # Re-raise it as the connection error it really is so
                    # SyncBroker.run()'s recovery loop reconnects instead of
                    # the consumer thread dying on an unrecognized exception.
                    if self._connection.is_closed or "Timeout closed before call" in str(exc):
                        import pika.exceptions

                        raise pika.exceptions.AMQPConnectionError(
                            f"connection lost mid-poll: {exc}"
                        ) from exc
                    raise
                # L14: process_data_events returning (rather than raising a
                # connection error) is itself evidence the I/O loop is alive
                # and pumping -- fire once per tick regardless of whether any
                # message was actually delivered this iteration.
                self._fire_io_tick()
                # Safety: if no consumers are registered, exit (avoids looping
                # forever in tests/embeds that call start_consuming without a
                # consumer). Real consumers are cancelled by stop_consuming which
                # sets _consuming=False.
                if not self._consumer_channels:
                    break
        except KeyboardInterrupt:
            self._stop_all_consumers()
        finally:
            self._consuming = False

    def _stop_all_consumers(self) -> None:
        """Stop consuming on the publisher channel and every consumer channel.

        Also clears ``self._consuming`` so the ``start_consuming`` I/O loop exits.
        """
        self._consuming = False
        for ch in [self._channel, *self._consumer_channels.values()]:
            try:
                if ch is not None and ch.is_open:
                    ch.stop_consuming()
            except Exception:  # pragma: no cover - best effort during shutdown
                logger.warning("stop_consuming raised on a channel", exc_info=True)

    def stop_consuming(self) -> None:
        """Stop the pika consume loop (safe to call from any thread).

        pika's ``BlockingChannel.stop_consuming`` is not thread-safe and must run
        on the connection-owning I/O thread. Route through ``_run_on_io_thread``
        (I-17): when called cross-thread during an active consume loop (e.g. the
        SIGTERM daemon thread), marshal via ``add_callback_threadsafe``; when
        called inline (single-threaded / test / not consuming), run directly.
        On a stalled I/O loop we do NOT fall back to an inline cross-thread call
        (that would be the unsafe pika call I-17 prevents) — the broker's run()
        loop / k8s SIGKILL + redelivery backstop handles a true stall.
        """
        if not self.is_connected():
            return
        try:
            self._run_on_io_thread(self._stop_all_consumers, timeout=5.0)
        except TimeoutError:
            logger.warning(
                "stop_consuming marshal timed out (I/O loop stalled); "
                "leaving settlement to broker recovery / redelivery"
            )

    def pump(self, time_limit: float = 0.05) -> None:
        """Briefly drive the connection's I/O loop.

        H2: once ``start_consuming()``'s loop has exited (consumers cancelled,
        ``_consuming`` is False), nothing drains callbacks scheduled via
        ``add_callback_threadsafe`` anymore — including worker-thread
        acks/nacks marshaled by ``_run_on_io_thread``. ``SyncBroker.stop()``
        calls this between waits during its worker-pool/in-flight drain so
        those marshaled callbacks still get executed on the owner thread
        instead of stalling until ``_run_on_io_thread``'s timeout. MUST be
        called from the connection's owner thread — same requirement as any
        other direct pika call.
        """
        if self._connection is not None and self._connection.is_open:
            self._connection.process_data_events(time_limit=time_limit)

    # ── DLQ / inspection (DLQInspector protocol) ──────────────────────────

    def basic_get(self, queue: str) -> RabbitMessage | None:
        """Get a single message without subscribing (auto_ack=False).

        Used by DLQInspector for peek/replay. Returns None if the queue is empty.
        """
        self._ensure_connected()
        method, properties, body = self._run_on_io_thread(lambda: self._channel.basic_get(queue=queue, auto_ack=False))
        if method is None:
            return None
        return self._build_message(self._channel, method, properties, body)

    def purge_queue(self, queue: str) -> int:
        """Purge all messages from a queue. Returns the number of messages purged."""
        self._ensure_connected()
        frame = self._run_on_io_thread(lambda: self._channel.queue_purge(queue=queue))
        return int(frame.method.message_count)

    # ── Internal ──────────────────────────────────────────────────────────

    def _build_message(
        self, channel: Any, method: Any, properties: Any, body: bytes, *, no_ack: bool = False
    ) -> RabbitMessage:
        """Build RabbitMessage from a pika delivery.

        ``channel`` is the pika channel the delivery arrived on (per-consumer
        channel for consume, publisher/topology channel for basic_get); sync
        settlement (ack/nack/reject) is wired to THAT channel so it stays on the
        correct I/O thread (H-SRE1).

        ``no_ack=True`` (delivery came from a no-ack consumer) skips wiring
        settlement functions entirely — the broker already auto-acked the
        delivery, and a manual ``basic_ack``/``basic_nack``/``basic_reject`` on it
        would be a protocol violation.
        """
        # pika carries the AMQP timestamp as a Unix int (seconds); surface it as a
        # tz-aware datetime to match the publish side. Was never populated before.
        ts = properties.timestamp
        timestamp = datetime.fromtimestamp(ts, tz=UTC) if isinstance(ts, (int, float)) else None
        message = RabbitMessage(
            body=body,
            headers=dict(properties.headers) if properties.headers else {},
            message_id=properties.message_id,
            correlation_id=properties.correlation_id,
            reply_to=properties.reply_to,
            content_type=properties.content_type,
            content_encoding=properties.content_encoding,
            type=properties.type,
            app_id=properties.app_id,
            priority=properties.priority,
            expiration=properties.expiration,
            user_id=properties.user_id,
            timestamp=timestamp,
            routing_key=method.routing_key,
            exchange=method.exchange,
            delivery_tag=method.delivery_tag,
            redelivered=method.redelivered,
            consumer_tag=getattr(method, "consumer_tag", None),  # absent on basic_get (Basic.GetOk)
        )

        if no_ack:
            # Broker already auto-acked this delivery — leave settlement
            # functions unset so ack()/nack()/reject() raise (RabbitMessage's
            # existing "no settlement fn set" guard) instead of issuing an
            # invalid basic_ack/nack/reject against a no-ack delivery. Callers
            # that only read the message (e.g. RPCClient's reply handler,
            # which never settles) are unaffected.
            return message

        # Wire sync settlement functions to the channel that owns this delivery.
        def ack_fn() -> None:
            self._run_on_io_thread(lambda: channel.basic_ack(delivery_tag=method.delivery_tag))

        def nack_fn(requeue: bool = True) -> None:
            self._run_on_io_thread(lambda: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue))

        def reject_fn(requeue: bool = False) -> None:
            self._run_on_io_thread(lambda: channel.basic_reject(delivery_tag=method.delivery_tag, requeue=requeue))

        message._ack_fn = ack_fn
        message._nack_fn = nack_fn
        message._reject_fn = reject_fn

        return message
