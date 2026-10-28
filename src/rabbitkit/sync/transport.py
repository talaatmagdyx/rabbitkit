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

import contextlib
import logging
import random
import threading
import time
import uuid
from collections.abc import Callable, Iterator
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
    PublishReason,
    PublishStatus,
    TopologyMode,
)
from rabbitkit.sync.connection import get_connection_errors, make_pika_connection_params

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class _IoLoopStalledError(TimeoutError):
    """A cross-thread callback timed out before it started: it never ran and
    never will (the caller cancelled it atomically). For a publish this means
    nothing was sent."""


class _PublishNotSentError(Exception):
    """Raised before ``basic_publish`` when the channel or connection is
    already known to be closed: no frame of the message was written."""


#: Broker channel-close codes that refuse a publish -> ERROR reason.
_PUBLISH_REFUSALS: dict[int, str] = {
    404: PublishReason.EXCHANGE_NOT_FOUND,
    403: PublishReason.ACCESS_REFUSED,
    406: PublishReason.PRECONDITION_FAILED,
}


def _returned_fields(exc: Any) -> tuple[int | None, str | None]:
    """``(reply_code, reply_text)`` of the ``Basic.Return`` behind pika's
    ``UnroutableError`` (``(None, None)`` if it carries none)."""
    messages = getattr(exc, "messages", None) or []
    if not messages:
        return None, None
    method = getattr(messages[0], "method", None)
    return getattr(method, "reply_code", None), getattr(method, "reply_text", None)


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
        # Sticky variant (architect review L1/B1): once ANY consume loop has
        # ever run on this transport, cross-thread callers must marshal (or
        # fail) forever — even mid-recovery, after disconnect() reset
        # _ever_consumed/_owner_ident — and must never reconnect themselves.
        self._ever_consumed_any = False
        # Sticky owner identity: the thread that ran start_consuming(),
        # surviving disconnect() (unlike _owner_ident). Reconnection after a
        # consume loop has existed is allowed ONLY for this thread (the
        # broker's recovery loop runs on it) -- enforced in
        # _ensure_connected so every entry point (publish, declare_*,
        # bind_*, basic_get, purge_queue) is covered, closing the
        # check-then-reconnect race a publish()-only guard would leave.
        self._owner_ident_any: int | None = None
        # Channels cancelled by cancel_consumer but deliberately left OPEN
        # until disconnect() so the in-flight drain can still settle on them
        # (H1: closing at cancel time force-requeues every unacked delivery).
        self._cancelled_channels: list[Any] = []

        # Per-queue consumer channels (H-SRE1): each queue gets its own channel
        # so per-queue basic_qos does not overwrite other consumers and fair
        # dispatch is preserved. The publisher/topology channel stays separate.
        self._consumer_channels: dict[str, Any] = {}
        # Queues ever passed to consume() (item 3) — unlike _consumer_channels,
        # NOT cleared by disconnect(), so a queue seen here already means this
        # consume() call is recreating a channel after _recover_consumers()'s
        # reconnect + re-subscribe, not a first-ever open.
        self._queues_ever_consumed: set[str] = set()

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
        self._confirmed_channel_ids: set[tuple[int, int]] = set()

        # Backpressure callbacks (FlowController registers here). Each is a
        # zero-arg callable; pika's blocked/unblocked frames are adapted to it.
        self._blocked_callbacks: list[Callable[[], None]] = []
        self._unblocked_callbacks: list[Callable[[], None]] = []

        # L15: passive blocked-state tracking, independent of whether a
        # FlowController is registered above -- health.broker_health_check
        # reads this (via the is_blocked property) so a broker/disk/memory
        # alarm is visible even when the caller never opted into FlowController.
        self._blocked_state: bool = False
        self._blocked_reason: str | None = None

        # L14: fired once per start_consuming() loop iteration (after each
        # process_data_events() call returns), i.e. once per I/O loop tick --
        # NOT once per delivered message. The broker uses this to refresh a
        # liveness heartbeat so a healthy but message-idle consumer doesn't
        # get mistaken for a wedged one (broker_liveness previously only saw
        # a heartbeat update when a message was actually delivered).
        self._io_tick_callbacks: list[Callable[[], None]] = []

        # Reconnect bound (H-SRE4): never retry forever. Sane default;
        # SyncBroker overrides this from ConnectionConfig.reconnect_max_attempts
        # right after construction, so 0 (== use the fallback below) only
        # applies when this transport is built directly, bypassing the broker.
        self.max_reconnect_attempts: int = 0  # 0 == use the time-bounded default below
        self._reconnect_total_timeout: float = 300.0

        # Connection-churn signal: reconnects were logged but never counted,
        # so a flapping broker/network was invisible to metrics alerting.
        # Fired on every successful connect() AFTER the first (see connect()).
        self._reconnect_callbacks: list[Callable[[], None]] = []
        self._ever_connected = False
        # G-22 metrics hooks: connection up/down (role "shared": one pika
        # connection serves publishing and consuming), failed reconnect
        # attempts, and publish outcomes with the send-to-confirm latency.
        self._connection_state_callbacks: list[Callable[[str, bool, BaseException | None], None]] = []
        self._reconnect_attempt_callbacks: list[Callable[[int, BaseException], None]] = []
        self._publish_outcome_callbacks: list[Callable[[PublishOutcome, float | None], None]] = []
        self._loss_reported = False
        self._loss_cause: BaseException | None = None

        # Channel-lifecycle signal (item 3): channels_opened_total counts
        # every new channel this transport creates; channel_rebuilds_total
        # isolates the subset that REPLACES a channel lost to a reconnect or
        # a broker-side close, as opposed to an ordinary first-ever open.
        self._channel_opened_callbacks: list[Callable[[], None]] = []
        self._channel_rebuilt_callbacks: list[Callable[[], None]] = []

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

    def on_connection_state(self, callback: Callable[[str, bool, BaseException | None], None]) -> None:
        """Register ``callback(role, up, error)`` (G-22). ``role`` is always
        ``"shared"``: fired with ``up=True`` after every successful connect and
        with ``up=False`` once when a connection that was up is found down
        (before the reconnect starts). ``disconnect()`` does not fire it."""
        self._connection_state_callbacks.append(callback)

    def _fire_connection_state(self, up: bool, error: BaseException | None) -> None:
        for cb in list(self._connection_state_callbacks):
            try:
                cb("shared", up, error)
            except Exception:  # never let a metrics hook break a connect
                logger.exception("connection_state callback raised")

    def on_reconnect_attempt(self, callback: Callable[[int, BaseException], None]) -> None:
        """Register ``callback(attempt, error)`` fired after every FAILED
        reconnect attempt of a connection that had been up (G-22)."""
        self._reconnect_attempt_callbacks.append(callback)

    def on_publish_outcome(self, callback: Callable[[PublishOutcome, float | None], None]) -> None:
        """Register ``callback(outcome, seconds)`` for every publish handed to
        pika (G-22). ``seconds`` is the ``basic_publish`` call alone (frame
        write plus, in confirm mode, the wait for the broker's confirm, nack
        or return) when the broker answered, else ``None``."""
        self._publish_outcome_callbacks.append(callback)

    @property
    def channels_active(self) -> int:
        """Open channels: the publisher/topology channel plus one per consumed
        queue (and cancelled ones still draining)."""
        channels = (self._channel, *self._consumer_channels.values(), *self._cancelled_channels)
        return sum(1 for channel in channels if channel is not None and bool(getattr(channel, "is_open", False)))

    def on_channel_opened(self, callback: Callable[[], None]) -> None:
        """Register a callback fired every time this transport creates a new
        channel (channels_opened_total metric hook)."""
        self._channel_opened_callbacks.append(callback)

    def on_channel_rebuilt(self, callback: Callable[[], None]) -> None:
        """Register a callback fired when a channel is created to REPLACE
        one lost to a reconnect or a broker-side close (channel_rebuilds_total
        metric hook) — a subset of :meth:`on_channel_opened`."""
        self._channel_rebuilt_callbacks.append(callback)

    def _fire_channel_opened(self) -> None:
        for cb in list(self._channel_opened_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover — never let a cb break the caller
                logger.exception("channel_opened callback raised")

    def _fire_channel_rebuilt(self) -> None:
        for cb in list(self._channel_rebuilt_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover — never let a cb break the caller
                logger.exception("channel_rebuilt callback raised")

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

    @property
    def blocked_reason(self) -> str | None:
        """The broker's ``connection.blocked`` reason (``"low on disk"``,
        ``"low on memory"``) while :attr:`is_blocked`, else ``None``."""
        return self._blocked_reason if self._blocked_state else None

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

    def _pika_blocked(self, _connection: Any, method_frame: Any = None, *_args: Any) -> None:
        # pika passes the Connection.Blocked frame: method_frame.method.reason.
        reason = getattr(getattr(method_frame, "method", None), "reason", None)
        self._blocked_reason = reason if isinstance(reason, str) else None
        self._blocked_state = True
        for cb in list(self._blocked_callbacks):
            try:
                cb()
            except Exception:  # pragma: no cover — never let a cb break the I/O loop
                logger.exception("blocked callback raised")

    def _pika_unblocked(self, _connection: Any, *_args: Any) -> None:
        self._blocked_state = False
        self._blocked_reason = None
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
        self._fire_channel_opened()
        if self._ever_connected:
            # This connect() call is itself a reconnect (see _fire_reconnect
            # below) — the fresh publisher channel is replacing one lost to
            # the disconnect, not a first-ever open.
            self._fire_channel_rebuilt()
        if self._confirm_delivery:
            self._channel.confirm_delivery()
            self._confirmed_channel_ids.add(self._channel_key(self._channel))

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
        self._loss_reported = False
        self._fire_connection_state(True, None)
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
            # Close per-queue consumer channels first — including channels
            # parked by cancel_consumer (H1: cancelled but deliberately left
            # open until the in-flight drain finished).
            for ch in list(self._consumer_channels.values()) + self._cancelled_channels:
                try:
                    if ch.is_open:
                        ch.close()
                except Exception:  # pragma: no cover — best effort
                    pass
            self._consumer_channels.clear()
            self._cancelled_channels.clear()
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
            # _ever_consumed_any deliberately NOT reset (architect review L1):
            # during run()-recovery a worker thread settling between
            # disconnect() and the owner's reconnect must keep taking the
            # marshal-or-fail path, never the inline cross-thread branch.
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

        # Verification gap 2: once a consume loop has ever run, only the
        # historical owner thread may reconnect -- ANY other thread reaching
        # here (publish TOCTOU after its guard, DLQInspector.basic_get from
        # ops tooling, declare/bind helpers) must fail cleanly instead of
        # creating a second BlockingConnection cross-thread.
        if (
            self._ever_consumed_any
            and self._owner_ident_any is not None
            and threading.get_ident() != self._owner_ident_any
        ):
            raise ConnectionError(
                "Connection is down and this thread does not own it — reconnection "
                "is handled by the broker's recovery loop on the owner thread."
            )

        if self._reopen_publisher_channel():
            return

        self._connected = False
        if self._ever_connected and not self._loss_reported:
            self._loss_reported = True
            self._fire_connection_state(False, self._loss_cause)
        self._loss_cause = None
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
                if self._ever_connected:
                    for cb in list(self._reconnect_attempt_callbacks):
                        try:
                            cb(attempts, e)
                        except Exception:  # never let a metrics hook break the reconnect loop
                            logger.exception("reconnect_attempt callback raised")
                # H3 (architect review): each backoff iteration is provable
                # forward progress — refresh the liveness heartbeat so a
                # broker outage longer than wedged_timeout doesn't flip
                # liveness and restart the whole consumer fleet mid-outage
                # (docs/kubernetes.md promises exactly that it won't).
                self._fire_io_tick()
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

    def _reopen_publisher_channel(self) -> bool:
        """Replace a closed publisher channel on a connection that is still open.

        A broker refusal of a publish (404 missing exchange, 403, 406) closes
        only the publisher channel. Building a whole new connection for that
        left the old one open, with every consumer channel on it, while the
        consume loop moved on to the new one. Returns False when the
        connection itself is gone (the caller reconnects).
        """
        conn = self._connection
        if not self._connected or conn is None or not conn.is_open:
            return False
        if self._channel is not None and self._channel.is_open:
            return True  # another caller reopened it first
        try:
            channel = conn.channel()
            if self._confirm_delivery:
                channel.confirm_delivery()
        except get_connection_errors() as e:
            logger.warning("Could not reopen the publisher channel (%s); reconnecting", type(e).__name__)
            return False
        self._channel = channel
        if self._confirm_delivery:
            self._confirmed_channel_ids.add(self._channel_key(channel))
        self._fire_channel_opened()
        self._fire_channel_rebuilt()
        logger.info("Reopened the publisher channel")
        return True

    def _reopen_publisher_channel_on_io_thread(self) -> bool:
        """:meth:`_reopen_publisher_channel` for a caller that may not touch the
        connection itself (a worker thread, or a handler inside the consume
        loop): marshalled onto the I/O thread, never a reconnect. False when
        the connection is gone or the I/O loop did not run it in time."""
        conn = self._connection
        if not self._connected or conn is None or not conn.is_open:
            return False
        try:
            return self._run_on_io_thread(self._reopen_publisher_channel, timeout=self._confirm_timeout)
        except TimeoutError:
            return False

    def reconnect(self, cause: BaseException | None = None) -> None:
        """Force a fresh connection + channel (used by consumer recovery).

        *cause* is the error that showed the connection was lost; it is
        passed to :meth:`on_connection_state` hooks.
        """
        self._loss_cause = cause
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
        # L1: gate on the STICKY flag — after disconnect() during recovery
        # (_owner_ident None, _ever_consumed reset) a worker thread must keep
        # marshaling (and fail loudly if nothing pumps) rather than touch the
        # connection inline, cross-thread, while the owner is reconnecting.
        if threading.get_ident() == self._owner_ident or not (self._ever_consumed or self._ever_consumed_any):
            return fn()

        result: list[_T] = []
        error: list[BaseException] = []
        done = threading.Event()
        # R-3: set when the caller gives up waiting, so a later _cb drain is a
        # no-op rather than settling an already-redelivered message.
        cancelled = threading.Event()
        # Whether fn() began, decided atomically against the caller giving up:
        # on expiry the caller knows for certain whether fn() never ran (and
        # never will) or may still be running.
        handshake = threading.Lock()
        started: list[bool] = []

        def _cb() -> None:
            with handshake:
                if cancelled.is_set():
                    # The caller already timed out and moved on (nack+requeue).
                    # Running fn() now could double-settle, so drop the late callback.
                    return
                started.append(True)
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
            with handshake:
                cancelled.set()
                never_ran = not started
            if never_ran:
                raise _IoLoopStalledError(
                    f"Timed out after {io_stall_timeout}s waiting for the pika I/O "
                    "loop to run a cross-thread callback; it never ran and never will"
                )
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
        self._cancelled_channels = []
        self._consumer_tags = {}
        self._confirmed_channel_ids = set()
        self._connected = False
        self._owner_ident = None
        self._ever_consumed = False
        # _ever_consumed_any stays True (sticky) — see __init__.

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
        # B1 (architect review, blocker): once a consume loop has ever run,
        # a NON-owner thread must never reconnect. A worker-thread handler
        # publishing during a connection blip used to call
        # _ensure_connected(), create a NEW BlockingConnection on the worker
        # thread, and steal _owner_ident — while the real I/O thread's next
        # loop tick picked up self._connection and started pumping it: two
        # unsynchronized threads driving one pika connection (frame
        # corruption, lost acks, mass redelivery). Reconnection is owned
        # exclusively by the owner thread (SyncBroker.run()'s recovery loop);
        # here we fail fast with a clean outcome instead. The owner thread
        # itself must not reconnect while its consume loop is running either
        # (a handler publishing inline): the loop would go on pumping a fresh
        # connection that carries none of its consumers.
        if (
            (self._ever_consumed or self._ever_consumed_any)
            and (threading.get_ident() != self._owner_ident or self._consuming)
            and not self.is_connected()
            and not self._reopen_publisher_channel_on_io_thread()
        ):
            return self._outcome(
                envelope,
                PublishStatus.NOT_SENT,
                PublishReason.NOT_CONNECTED,
                ConnectionError(
                    "Connection is down and reconnection is handled by the broker's "
                    "recovery loop on the owner thread. The publish was NOT sent; "
                    "treat as transient and retry/nack."
                ),
            )

        try:
            self._ensure_connected()
            self._detect_closed_idle_connection()
        except get_connection_errors() as e:
            # Nothing was sent: the connection could not be (re)established.
            logger.warning("Publish not sent: no connection (%s)", type(e).__name__)
            return self._outcome(envelope, PublishStatus.NOT_SENT, PublishReason.NOT_CONNECTED, e)

        channel = self._channel
        if (
            envelope.reply_to == DIRECT_REPLY_TO_QUEUE
            and self._reply_to_channel is not None
            and self._reply_to_channel.is_open
        ):
            channel = self._reply_to_channel

        # Never republished here once frames may have left the client: a
        # connection lost after basic_publish is UNKNOWN (the broker may hold
        # the message), so republishing could deliver it twice and still
        # report CONFIRMED. A dead idle connection is caught before sending
        # by _detect_closed_idle_connection instead.
        return self._publish_on_channel(channel, envelope)

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

    def _detect_closed_idle_connection(self) -> None:
        """Notice, before sending, a pure-producer connection the broker closed.

        Nothing pumps a publish-only connection between publishes, so one the
        broker closed while it sat idle (missed heartbeats, an operator or a
        restart) still reports ``is_open`` until its socket is read: the next
        publish would write its frames into a dead socket and its outcome
        would be UNKNOWN. A zero-time ``process_data_events`` reads what is
        pending; if the connection turns out closed it is replaced before
        anything is sent. Only for a pure producer: no consume loop has ever
        run AND no consumer is registered. On a consuming connection the loop
        reads the socket; and once ``consume()`` registered a consumer (e.g.
        ``SyncBroker.start()`` before ``run()``), pumping would dispatch
        deliveries here, before any consume loop exists, so worker-pool
        handlers would settle them inline from their own threads (pika is
        not thread-safe).
        """
        if (
            self._ever_consumed
            or self._ever_consumed_any
            or self._consumer_channels
            or self._reply_to_channel is not None
            or self._connection is None
        ):
            return
        try:
            self._connection.process_data_events(time_limit=0)
        except get_connection_errors() as e:
            logger.warning("Publisher connection was closed while idle (%s); reconnecting before publishing", e)
            self.disconnect()
            self._ensure_connected()

    @staticmethod
    def _channel_key(channel: Any) -> tuple[int, int]:
        """Identity key for confirmed-channel tracking (architect review m5):
        ``id()`` alone can be reused after a tracked channel is GC'd, which
        would mark a fresh unconfirmed channel as confirmed and silently
        disable Basic.Return detection for mandatory publishes. Pairing it
        with the AMQP channel number makes accidental collision practically
        impossible while staying hashable and cheap."""
        return (id(channel), int(getattr(channel, "channel_number", -1)))

    def _ensure_mandatory_confirms(self, channel: Any) -> None:
        """Enable publisher confirms on *channel* if not already active.

        H1: detecting an unroutable ``Basic.Return`` via pika's
        ``UnroutableError`` requires confirm mode — in non-confirm mode
        ``basic_publish()`` has no way to report a return at all. Idempotent
        and tracked per-channel (by id) so a repeat call is a no-op rather than
        pika logging a spurious "confirmation was already enabled" error.
        Marshaled like ``basic_publish`` since it drives blocking I/O.
        """
        if self._channel_key(channel) in self._confirmed_channel_ids:
            return
        self._run_on_io_thread(channel.confirm_delivery)
        self._confirmed_channel_ids.add(self._channel_key(channel))

    def _publish_on_channel(self, channel: Any, envelope: MessageEnvelope) -> PublishOutcome:
        """Publish *envelope* on a specific already-open channel."""
        if not self._publish_outcome_callbacks:
            return self._publish_on_channel_unreported(channel, envelope, None)
        timing: list[float] = []
        outcome = self._publish_on_channel_unreported(channel, envelope, timing)
        answered = outcome.status in (PublishStatus.CONFIRMED, PublishStatus.NACKED, PublishStatus.RETURNED)
        seconds = timing[0] if answered and timing else None
        for cb in list(self._publish_outcome_callbacks):
            try:
                cb(outcome, seconds)
            except Exception:  # never let a metrics hook fail a publish
                logger.exception("publish_outcome callback raised")
        return outcome

    def _publish_on_channel_unreported(
        self, channel: Any, envelope: MessageEnvelope, timing: list[float] | None
    ) -> PublishOutcome:
        """Publish *envelope* on a specific channel and classify the result.

        The same contract as the async transport (docs/reliable-messaging.md):

        * NOT_SENT: nothing of the message left the client. The channel or
          connection was already closed before ``basic_publish``
          (``channel_unavailable`` / ``not_connected``), confirm mode could
          not be enabled (``channel_unavailable``), or a cross-thread publish
          was never picked up by the I/O loop in time (``admission_timeout``).
        * UNKNOWN: the connection (``connection_lost``) or channel
          (``channel_closed``) was lost after ``basic_publish`` wrote the
          frames; the broker may hold the message. Never republished here.
        * TIMEOUT (``confirm_timeout``): no confirm within ``confirm_timeout``.
        * ERROR: the broker refused this publish (404 ``exchange_not_found``,
          403 ``access_refused``, 406 ``precondition_failed``, with
          ``reply_code`` / ``reply_text``; the publisher channel is used by
          one publish at a time, so the refusal is this publish's own), the
          envelope could not be encoded (``invalid_envelope``), or anything
          unclassified (``publish_error``).
        """
        import pika
        import pika.exceptions

        # An empty message_id/content_type means "no such property"
        # (DLQ replay of a message that had none), not an empty string.
        try:
            properties = pika.BasicProperties(
                message_id=envelope.message_id or None,
                correlation_id=envelope.correlation_id,
                reply_to=envelope.reply_to,
                content_type=envelope.content_type or None,
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
        except (ValueError, TypeError) as e:
            logger.error("Publish failed (invalid envelope): %s", e)
            return self._outcome(envelope, PublishStatus.ERROR, PublishReason.INVALID_ENVELOPE, e)

        # The direct reply-to channel is a consumer channel, not in confirm
        # mode by default (m1); without confirms the outcome below would claim
        # CONFIRMED for a fire-and-forget publish. confirm.select is legal on
        # a channel with an active consumer. No message frame is sent here.
        needs_confirms = envelope.mandatory or (self._confirm_delivery and channel is self._reply_to_channel)
        if needs_confirms:
            try:
                self._ensure_mandatory_confirms(channel)
            except Exception as e:  # nothing of the message has been sent
                logger.warning("Publish not sent: could not enable confirms on the channel (%s)", e)
                return self._outcome(envelope, PublishStatus.NOT_SENT, PublishReason.CHANNEL_UNAVAILABLE, e)

        def do_publish() -> None:
            # Checked on the thread that publishes, right before basic_publish
            # (pika itself raises on a closed channel/connection there, before
            # writing a frame). Past this point the frames may have left.
            conn = self._connection
            if conn is None or not conn.is_open:
                raise _PublishNotSentError(PublishReason.NOT_CONNECTED)
            if not channel.is_open:
                raise _PublishNotSentError(PublishReason.CHANNEL_UNAVAILABLE)
            started = time.monotonic()
            try:
                channel.basic_publish(
                    exchange=envelope.exchange,
                    routing_key=envelope.routing_key,
                    body=envelope.body,
                    properties=properties,
                    mandatory=envelope.mandatory,
                )
            finally:
                if timing is not None:
                    timing.append(time.monotonic() - started)

        # I-10: bound the publish+confirm wait by confirm_timeout so a
        # missing confirm cannot stall the worker forever.
        # m4: honor the configured confirm_timeout as-is — the old
        # min(30, x) silently clamped an operator's deliberate 60 s to 30.
        publish_timeout = self._confirm_timeout
        try:
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
            elif not (self._ever_consumed or self._ever_consumed_any):
                # No consume loop has EVER shared this transport (pure
                # producer) -- safe to bound with a dedicated helper thread.
                # STICKY flag (verification gap 1): during the
                # _recover_consumers window _ever_consumed is reset and
                # _owner_ident may be None while the connection is already
                # alive -- a worker publish must NOT run on a raw helper
                # thread concurrently with the owner rebuilding topology on
                # the same new connection; it falls through to the marshaled
                # branch below instead.
                self._publish_confirm_wait_bounded(do_publish, timeout=publish_timeout)
            else:
                # Cross-thread: marshal onto the owner's I/O loop, which
                # _run_on_io_thread already bounds by confirm_timeout.
                self._run_on_io_thread(do_publish, timeout=publish_timeout)
        except Exception as e:
            return self._classify_publish_failure(e, envelope)

        # M4: only report CONFIRMED when the channel is actually in
        # publisher-confirm mode -- confirm_delivery=False (unless this
        # publish is `mandatory`, which always enables confirms via
        # _ensure_mandatory_confirms above) means basic_publish() is
        # fire-and-forget and nothing was broker-acknowledged.
        confirmed = self._confirm_delivery or envelope.mandatory
        return self._outcome(envelope, PublishStatus.CONFIRMED if confirmed else PublishStatus.SENT)

    def _classify_publish_failure(self, exc: Exception, envelope: MessageEnvelope) -> PublishOutcome:
        """Map an exception from the send step of :meth:`_publish_on_channel`."""
        import pika.exceptions

        if isinstance(exc, _PublishNotSentError):
            reason = str(exc)
            logger.warning(
                "Publish not sent (%s): exchange=%s routing_key=%s", reason, envelope.exchange, envelope.routing_key
            )
            return self._outcome(envelope, PublishStatus.NOT_SENT, reason, exc)
        if isinstance(exc, _IoLoopStalledError):
            logger.warning("Publish not sent: the I/O loop did not pick it up in time")
            return self._outcome(envelope, PublishStatus.NOT_SENT, PublishReason.ADMISSION_TIMEOUT, exc)
        if isinstance(exc, pika.exceptions.UnroutableError):
            code, text = _returned_fields(exc)
            logger.warning(
                "Publish returned as unroutable (mandatory=True, no matching binding): "
                "exchange=%s routing_key=%s reply_code=%s reply_text=%s",
                envelope.exchange,
                envelope.routing_key,
                code,
                text,
            )
            return self._outcome(
                envelope, PublishStatus.RETURNED, PublishReason.RETURNED, exc, reply_code=code, reply_text=text
            )
        if isinstance(exc, pika.exceptions.NackError):
            logger.warning(
                "Publish nacked by broker: exchange=%s routing_key=%s", envelope.exchange, envelope.routing_key
            )
            return self._outcome(envelope, PublishStatus.NACKED, PublishReason.NACKED, exc)
        if isinstance(exc, TimeoutError):
            # I-10: no confirm within confirm_timeout (_run_on_io_thread or
            # _publish_confirm_wait_bounded gave up): the message may be on
            # the broker. Same status as the async transport's confirm wait.
            logger.warning(
                "Publish confirm timed out: exchange=%s routing_key=%s", envelope.exchange, envelope.routing_key
            )
            return self._outcome(envelope, PublishStatus.TIMEOUT, PublishReason.CONFIRM_TIMEOUT, exc)
        if isinstance(exc, pika.exceptions.ChannelClosedByBroker):
            refusal = _PUBLISH_REFUSALS.get(exc.reply_code)
            if refusal is not None:
                logger.error(
                    "Publish refused by broker (%s): exchange=%s routing_key=%s: %s",
                    refusal,
                    envelope.exchange,
                    envelope.routing_key,
                    exc.reply_text,
                )
                return self._outcome(
                    envelope,
                    PublishStatus.ERROR,
                    refusal,
                    exc,
                    reply_code=exc.reply_code,
                    reply_text=exc.reply_text,
                )
        invalid_input = (TypeError, pika.exceptions.UnsupportedAMQPFieldException, pika.exceptions.ShortStringTooLong)
        if isinstance(exc, invalid_input) or (isinstance(exc, ValueError) and not self._connection_died(exc)):
            logger.error("Publish failed (invalid envelope): %s", exc)
            return self._outcome(envelope, PublishStatus.ERROR, PublishReason.INVALID_ENVELOPE, exc)
        if isinstance(exc, pika.exceptions.ChannelClosed | pika.exceptions.ChannelWrongStateError):
            logger.warning("Publish outcome unknown: channel closed after sending (%s)", exc)
            return self._outcome(envelope, PublishStatus.UNKNOWN, PublishReason.CHANNEL_CLOSED, exc)
        if isinstance(exc, (*get_connection_errors(), ValueError)):
            logger.warning("Publish outcome unknown: connection lost after sending (%s)", type(exc).__name__)
            return self._outcome(envelope, PublishStatus.UNKNOWN, PublishReason.CONNECTION_LOST, exc)
        logger.error("Publish failed: %s", exc)
        return self._outcome(envelope, PublishStatus.ERROR, PublishReason.PUBLISH_ERROR, exc)

    def _connection_died(self, exc: ValueError) -> bool:
        """pika's ioloop raises a bare ``ValueError("Timeout closed before
        call")`` when the connection died between poll ticks (see
        :meth:`start_consuming`); any ValueError on a closed connection is
        that, not invalid input."""
        conn = self._connection
        return "Timeout closed before call" in str(exc) or conn is None or bool(getattr(conn, "is_closed", False))

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
        # A queue seen here before means a previous channel for it is being
        # replaced — the broker's _recover_consumers() re-subscribes every
        # route's queue after a reconnect, so this is a rebuild, not a
        # first-ever open.
        is_rebuild = queue in self._queues_ever_consumed
        self._queues_ever_consumed.add(queue)
        consumer_channel = self._connection.channel()
        self._fire_channel_opened()
        if is_rebuild:
            self._fire_channel_rebuilt()
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
            self._raise_precondition_failed_or_reraise(
                "queue", kwargs["queue"], exc, declared_arguments=kwargs.get("arguments")
            )

    def _raise_precondition_failed_or_reraise(
        self,
        kind: str,
        name: str,
        exc: Any,
        *,
        declared_arguments: dict[str, Any] | None = None,
    ) -> None:
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
            # Architect review M1: if OUR declaration carried a dead-letter
            # exchange (retry/safety-injected DLX), continuing with the
            # existing DLX-less queue silently converts every terminal
            # reject(requeue=False) into a broker-side DISCARD — the DLQ
            # stays green and empty while poison messages vanish. That is
            # never an acceptable "drift"; escalate even under warn_continue.
            if declared_arguments and "x-dead-letter-exchange" in declared_arguments:
                raise ConfigurationError(
                    f"Queue {name!r} exists with arguments that conflict with the "
                    f"dead-letter configuration rabbitkit needs to declare "
                    f"(broker said: {exc.reply_text}). Continuing would mean "
                    f"terminal rejects on this queue are DISCARDED (no DLX on the "
                    f"existing queue) — refusing even under "
                    f"on_topology_conflict='warn_continue'. Reconcile the existing "
                    f"queue's x-dead-letter-exchange or declare the topology "
                    f"externally and use TopologyMode.PASSIVE_ONLY."
                ) from exc
            # Reopen the broker-closed channel so the rest of topology
            # declaration can proceed on the existing (drifted) entity.
            self._channel = self._connection.channel()
            self._fire_channel_opened()
            self._fire_channel_rebuilt()  # replacing a channel the broker just closed
            self._confirmed_channel_ids.discard(self._channel_key(self._channel))
            # Architect review M2: the 406 killed the old publisher channel —
            # the replacement must re-enter confirm mode, or every subsequent
            # publish is fire-and-forget while still being reported CONFIRMED
            # (silent loss on the reliability path).
            if self._confirm_delivery:
                self._channel.confirm_delivery()
                self._confirmed_channel_ids.add(self._channel_key(self._channel))
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
            from rabbitkit.core.topology import describe_declare_conflict  # [retry/topology hook]

            raise ConfigurationError(describe_declare_conflict(kind, name, str(exc.reply_text))) from exc
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
            # Architect review H1: do NOT close the channel here. Channel.Close
            # makes RabbitMQ instantly requeue every unacked delivery on it --
            # while worker threads are still executing those very handlers
            # (guaranteed concurrent duplicate processing on every graceful
            # shutdown), and each worker's eventual ack then dies on the
            # closed channel, so the drain could never settle anything.
            # basic_cancel alone stops new deliveries; the channel is parked
            # and closed by disconnect() AFTER the in-flight drain completes.
            self._consumer_tags.pop(queue_name, None)
            self._consumer_channels.pop(queue_name, None)
            if channel is not None and channel is not self._reply_to_channel:
                self._cancelled_channels.append(channel)
            if channel is self._reply_to_channel:
                # The direct-reply-to consumer is no-ack — no unacked
                # deliveries can exist on this channel, so closing at cancel
                # time is safe (and leaving it untracked would leak it).
                with contextlib.suppress(Exception):
                    if channel.is_open:
                        channel.close()
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
        self._ever_consumed_any = True  # sticky — never reset (L1)
        self._owner_ident = threading.get_ident()
        self._owner_ident_any = threading.get_ident()  # sticky owner identity
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

                        raise pika.exceptions.AMQPConnectionError(f"connection lost mid-poll: {exc}") from exc
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

    @property
    def server_version(self) -> str | None:
        """RabbitMQ version the broker reported at connect (``"4.1.8"``), or None."""
        impl = getattr(self._connection, "_impl", None)
        props = getattr(impl, "server_properties", None)
        version = props.get("version") if isinstance(props, dict) else None
        if isinstance(version, bytes):
            version = version.decode(errors="replace")
        return str(version) if version else None

    def queue_exists(self, queue: str) -> bool:
        """True if *queue* exists. Probes on a throwaway channel.

        A passive declare of a missing queue is a 404 that closes the
        channel it ran on, so it must not run on the topology channel.
        """
        self._ensure_connected()
        import pika

        connection = self._connection

        def probe() -> bool:
            channel = connection.channel()
            try:
                channel.queue_declare(queue=queue, passive=True)
                return True
            except pika.exceptions.ChannelClosedByBroker as exc:
                if exc.reply_code == 404:
                    return False
                raise
            finally:
                if channel.is_open:
                    channel.close()

        return self._run_on_io_thread(probe)

    @contextlib.contextmanager
    def inspection_session(self) -> Iterator[SyncInspectionSession]:
        """A channel of its own for one peek/replay operation (issue #35).

        ``basic_get`` deliveries are settled on the channel they came from.
        On the shared channel, one inspection's channel error (a 404 for a
        mistyped queue, a 406) closed it for every other caller and requeued
        everybody's held messages. Closing the session's channel also returns
        anything still unsettled to the queue, so a crash mid-inspection
        can't strand messages.
        """
        self._ensure_connected()
        connection = self._connection
        channel = self._run_on_io_thread(connection.channel)
        try:
            yield SyncInspectionSession(self, channel)
        finally:
            if channel.is_open:
                try:
                    self._run_on_io_thread(channel.close)
                except Exception:  # pragma: no cover — the connection is going anyway
                    logger.debug("closing the inspection channel raised", exc_info=True)

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
            delivery_mode=properties.delivery_mode,
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
        # Bulk settlement (ack_many/nack_many) consults this to refuse a
        # delivery tag whose channel is gone — on a rebuilt channel the same
        # tag number names a DIFFERENT message.
        message._channel_alive = lambda: bool(getattr(channel, "is_open", False))

        return message


class SyncInspectionSession:
    """``basic_get`` on one dedicated channel; see ``SyncTransport.inspection_session``."""

    def __init__(self, transport: SyncTransport, channel: Any) -> None:
        self._transport = transport
        self._channel = channel

    def basic_get(self, queue: str) -> RabbitMessage | None:
        channel = self._channel
        method, properties, body = self._transport._run_on_io_thread(
            lambda: channel.basic_get(queue=queue, auto_ack=False)
        )
        if method is None:
            return None
        return self._transport._build_message(channel, method, properties, body)
