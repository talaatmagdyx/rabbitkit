"""SyncBatchPublisher — pipelined publisher confirms for the sync transport.

pika's ``BlockingChannel.basic_publish`` blocks per-confirm, ceiling-ing
confirmed sync publish at roughly 0.9k msg/s. pika's callback-based
``SelectConnection`` CAN pipeline confirms: publish N messages back-to-back
on one channel and settle each caller as its ``Basic.Ack``/``Basic.Nack``
arrives, amortizing the confirm round-trip across the whole window.

STANDALONE-ONLY: this publisher is constructed and owned by the user — it is
NOT wired into ``SyncBroker`` (the broker's publish path keeps its
``BlockingConnection`` semantics untouched). Use it directly::

    from rabbitkit import SyncBatchPublisher
    from rabbitkit.core.config import ConnectionConfig
    from rabbitkit.core.types import MessageEnvelope

    with SyncBatchPublisher(ConnectionConfig.from_url(url)) as pub:
        outcome = pub.publish(MessageEnvelope(routing_key="q", body=b"{}"))
        assert outcome.ok

Invariants (hard-won — do not weaken):

1. ONE THREAD OWNS ONE PIKA CONNECTION. The ``SelectConnection`` lives
   entirely on the dedicated daemon thread ``rabbitkit-sync-batch-io``;
   caller threads never touch it directly — they enqueue work and wake the
   ioloop via ``ioloop.add_callback_threadsafe`` (the one documented
   thread-safe entry point).
2. EVERY CALLER'S PENDING OUTCOME IS ALWAYS SETTLED (M17) — on ack, nack,
   return, timeout, publish error, connection death, and shutdown. Never
   silently dropped.
3. CALLERS NEVER HANG: each ``publish()`` waits at most ``confirm_timeout``
   (or its per-call override) and then gets a ``PublishStatus.TIMEOUT``
   outcome; the slot is marked abandoned so a late confirm is a no-op.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections import deque
from typing import Any

from rabbitkit.core.config import ConnectionConfig, SecurityConfig, SocketConfig
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.sync.connection import make_pika_connection_params

logger = logging.getLogger(__name__)

#: Thread name of the dedicated SelectConnection I/O thread.
IO_THREAD_NAME = "rabbitkit-sync-batch-io"


class _Slot:
    """One caller's pending publish: envelope + settlement rendezvous.

    ``outcome`` transitions exactly once (first settlement wins) under the
    publisher's lock; ``event`` is set only after ``outcome`` is assigned.
    ``abandoned`` marks a slot whose caller already timed out, so a late
    broker confirm becomes a no-op instead of settling into the void.
    """

    __slots__ = ("abandoned", "envelope", "event", "outcome")

    def __init__(self, envelope: MessageEnvelope) -> None:
        self.envelope = envelope
        self.event = threading.Event()
        self.outcome: PublishOutcome | None = None
        self.abandoned = False


class SyncBatchPublisher:
    """Pipelined-confirm publisher on a dedicated ``pika.SelectConnection``.

    Thread-safe: any number of caller threads may ``publish()`` concurrently.
    Each call blocks only for ITS OWN confirm (bounded by *confirm_timeout*),
    while the I/O thread keeps the channel's confirm window full — confirms
    for many in-flight messages are serviced concurrently instead of one
    blocking round-trip per message.

    Standalone-only (see module docstring): not wired into ``SyncBroker``.
    """

    def __init__(
        self,
        connection_config: ConnectionConfig | None = None,
        socket_config: SocketConfig | None = None,
        security_config: SecurityConfig | None = None,
        confirm_timeout: float = 5.0,
    ) -> None:
        self._connection_config = connection_config or ConnectionConfig()
        self._socket_config = socket_config or SocketConfig()
        self._security_config = security_config or SecurityConfig()
        self._confirm_timeout = float(confirm_timeout)

        # RLock: settlement helpers are called both standalone and from
        # within larger locked sections (fail-all, return matching).
        self._lock = threading.RLock()
        self._queue: deque[tuple[MessageEnvelope, _Slot]] = deque()
        self._pending: dict[int, _Slot] = {}  # delivery_tag → slot (publish order)
        self._next_tag = 0  # mirrors pika's per-channel delivery-tag counter

        self._connection: Any = None  # pika.SelectConnection (I/O thread's)
        self._channel: Any = None
        self._pika: Any = None  # the imported pika module (set on the I/O thread)

        self._ready = threading.Event()  # connected + channel in confirm mode
        self._closing = threading.Event()
        self._io_dead = threading.Event()  # I/O thread has exited
        self._closed = False
        self._thread: threading.Thread | None = None
        self._start_error: BaseException | None = None
        self._connected_once = False  # current SelectConnection reached confirm mode

        # Bounded reconnect (same spirit as SyncTransport._ensure_connected:
        # bounded attempts, full jitter, exponential backoff).
        self.max_reconnect_attempts: int = 5

    # ── lifecycle ─────────────────────────────────────────────────────────

    def start(self, ready_timeout: float = 30.0) -> None:
        """Spawn the I/O thread and block until the confirm channel is ready.

        Raises ``TimeoutError`` after *ready_timeout* if the broker never
        becomes reachable, or ``RuntimeError`` if the I/O thread gave up
        (connect attempts exhausted). Idempotent while running.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("SyncBatchPublisher is closed")
            if self._thread is not None:
                return  # already started
            self._thread = threading.Thread(
                target=self._io_loop, name=IO_THREAD_NAME, daemon=True
            )
        self._thread.start()

        deadline = time.monotonic() + ready_timeout
        while not self._ready.wait(timeout=0.02):
            if self._io_dead.is_set():
                err = self._start_error
                self.close(timeout=1.0)
                raise RuntimeError(
                    "SyncBatchPublisher failed to connect (attempts exhausted)"
                ) from err
            if time.monotonic() >= deadline:
                self.close(timeout=1.0)
                raise TimeoutError(
                    f"SyncBatchPublisher not ready within {ready_timeout}s"
                )

    def close(self, timeout: float = 10.0) -> None:
        """Stop accepting publishes, drain briefly, fail stragglers, shut down.

        Waits (bounded by *timeout*) for in-flight confirms to settle, then
        fails any stragglers with ``PublishStatus.ERROR`` (M17: never silent),
        stops the ioloop and joins the I/O thread. Idempotent.
        """
        with self._lock:
            if self._closed:
                return
            self._closing.set()

        deadline = time.monotonic() + timeout
        # Bounded wait for unsettled confirms (skip if nothing is in flight).
        while time.monotonic() < deadline:
            with self._lock:
                unsettled = any(s.outcome is None for s in self._pending.values()) or any(
                    s.outcome is None for _, s in self._queue
                )
            if not unsettled or self._io_dead.is_set():
                break
            time.sleep(0.005)

        # Fail stragglers — every remaining slot gets a terminal outcome.
        self._fail_all(RuntimeError("SyncBatchPublisher closed"))

        connection = self._connection
        if connection is not None:
            try:
                connection.ioloop.add_callback_threadsafe(self._shutdown_io)
            except Exception:
                # ioloop already stopped / connection already dead — the I/O
                # thread exits via its _closing check.
                logger.debug("close(): ioloop wake-up failed (already stopped)")

        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, deadline - time.monotonic()) + 1.0)

        with self._lock:
            self._closed = True
            self._ready.clear()

    def __enter__(self) -> SyncBatchPublisher:
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── publish (caller threads) ──────────────────────────────────────────

    def publish(
        self, envelope: MessageEnvelope, timeout: float | None = None
    ) -> PublishOutcome:
        """Publish *envelope* and block until ITS confirm settles.

        Returns CONFIRMED / NACKED / RETURNED per the broker's verdict,
        TIMEOUT if no verdict arrived within *timeout* (default
        ``confirm_timeout``), or ERROR if the publisher is closed,
        disconnected, or the connection died with this message in flight.
        Never raises for transport-level failures; never hangs.
        """
        wait = self._confirm_timeout if timeout is None else float(timeout)

        with self._lock:
            if self._closed or self._closing.is_set() or not self._ready.is_set():
                return PublishOutcome(
                    status=PublishStatus.ERROR,
                    exchange=envelope.exchange,
                    routing_key=envelope.routing_key,
                    error=RuntimeError("SyncBatchPublisher is not running/connected"),
                )
            slot = _Slot(envelope)
            self._queue.append((envelope, slot))
            connection = self._connection

        try:
            # The ONLY thread-safe way to poke the SelectConnection: the
            # drain itself runs on the I/O thread (invariant 1).
            connection.ioloop.add_callback_threadsafe(self._drain)
        except Exception as exc:
            # Connection died between the ready-check and the wake-up — the
            # close callback's fail-all may already have settled the slot;
            # _settle is first-wins either way (invariant 2).
            with self._lock:
                try:
                    self._queue.remove((envelope, slot))
                except ValueError:
                    pass  # already drained/failed elsewhere
            self._settle(slot, PublishStatus.ERROR, error=exc)

        if not slot.event.wait(timeout=wait):
            with self._lock:
                if slot.outcome is None:
                    # Invariant 3: caller never hangs. Mark abandoned so the
                    # late confirm (if it ever arrives) is a no-op.
                    slot.abandoned = True
                    slot.outcome = PublishOutcome(
                        status=PublishStatus.TIMEOUT,
                        exchange=envelope.exchange,
                        routing_key=envelope.routing_key,
                        error=TimeoutError(
                            f"No publisher confirm within {wait}s"
                        ),
                    )
                    slot.event.set()

        outcome = slot.outcome
        if outcome is None:  # pragma: no cover — event is only set after outcome
            outcome = PublishOutcome(
                status=PublishStatus.ERROR,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
                error=RuntimeError("publish slot settled without an outcome"),
            )
        return outcome

    # ── settlement (any thread, lock-guarded) ─────────────────────────────

    def _settle(
        self,
        slot: _Slot,
        status: PublishStatus,
        *,
        error: BaseException | None = None,
        delivery_tag: int | None = None,
    ) -> None:
        """Settle *slot* exactly once (first settlement wins)."""
        with self._lock:
            if slot.outcome is not None or slot.abandoned:
                return  # already settled, or caller timed out (late confirm no-op)
            slot.outcome = PublishOutcome(
                status=status,
                delivery_tag=delivery_tag,
                exchange=slot.envelope.exchange,
                routing_key=slot.envelope.routing_key,
                error=error,
            )
        slot.event.set()

    def _fail_all(self, error: BaseException) -> None:
        """Fail EVERY unsettled slot — in flight and still queued (M17)."""
        with self._lock:
            pending = list(self._pending.items())
            self._pending.clear()
            queued = [slot for _, slot in self._queue]
            self._queue.clear()
        for tag, slot in pending:
            self._settle(slot, PublishStatus.ERROR, error=error, delivery_tag=tag)
        for slot in queued:
            self._settle(slot, PublishStatus.ERROR, error=error)

    # ── I/O thread ────────────────────────────────────────────────────────

    def _io_loop(self) -> None:
        """Thread body: run the SelectConnection ioloop, reconnect bounded."""
        try:
            try:
                import pika
            except ImportError as exc:
                self._start_error = ImportError(
                    "pika is required for SyncBatchPublisher. "
                    "Install it with: pip install rabbitkit[sync]"
                )
                self._start_error.__cause__ = exc
                return

            self._pika = pika
            attempts = 0
            backoff = self._connection_config.reconnect_backoff_base

            while not self._closing.is_set():
                self._connected_once = False
                try:
                    params = make_pika_connection_params(
                        self._connection_config,
                        self._socket_config,
                        self._security_config,
                    )
                    connection = pika.SelectConnection(
                        parameters=params,
                        on_open_callback=self._on_connection_open,
                        on_open_error_callback=self._on_connection_open_error,
                        on_close_callback=self._on_connection_closed,
                    )
                    self._connection = connection
                    connection.ioloop.start()  # returns after ioloop.stop()
                except Exception as exc:
                    logger.warning("SyncBatchPublisher connection attempt failed: %s", exc)
                    self._start_error = exc

                if self._closing.is_set():
                    break
                if self._connected_once:
                    # This connection reached confirm mode — reset the budget.
                    attempts = 0
                    backoff = self._connection_config.reconnect_backoff_base
                attempts += 1
                if attempts > self.max_reconnect_attempts:
                    logger.critical(
                        "SyncBatchPublisher reconnect attempts exhausted after %d tries; giving up",
                        attempts - 1,
                    )
                    break
                # Full jitter (H-SRE3 spirit): spread reconnects across clients.
                sleep_for = random.uniform(0.0, backoff)  # noqa: S311
                logger.warning(
                    "SyncBatchPublisher reconnecting in %.2fs (attempt %d/%d)",
                    sleep_for,
                    attempts,
                    self.max_reconnect_attempts,
                )
                self._closing.wait(timeout=sleep_for)  # interruptible by close()
                backoff = min(backoff * 2, self._connection_config.reconnect_backoff_max)
        finally:
            # Thread exiting for ANY reason: nothing will ever settle these
            # slots again — fail them now (invariant 2).
            self._ready.clear()
            self._fail_all(
                self._start_error
                if self._start_error is not None and not self._connected_once
                else RuntimeError("SyncBatchPublisher I/O thread exited")
            )
            self._io_dead.set()

    def _shutdown_io(self) -> None:
        """Graceful shutdown, on the I/O thread (scheduled by close())."""
        try:
            if self._channel is not None and self._channel.is_open:
                self._channel.close()
            if self._connection is not None and self._connection.is_open:
                # Triggers _on_connection_closed → ioloop.stop().
                self._connection.close()
            else:
                self._connection.ioloop.stop()
        except Exception:  # pragma: no cover — best effort during shutdown
            try:
                self._connection.ioloop.stop()
            except Exception:
                logger.debug("shutdown: ioloop.stop() failed", exc_info=True)

    # ── pika callbacks (I/O thread) ───────────────────────────────────────

    def _on_connection_open(self, connection: Any) -> None:
        connection.channel(on_open_callback=self._on_channel_open)

    def _on_connection_open_error(self, connection: Any, error: Any) -> None:
        logger.warning("SyncBatchPublisher failed to open connection: %s", error)
        self._start_error = (
            error if isinstance(error, BaseException) else RuntimeError(str(error))
        )
        connection.ioloop.stop()

    def _on_connection_closed(self, connection: Any, reason: Any) -> None:
        self._ready.clear()
        self._channel = None
        if not self._closing.is_set():
            logger.warning("SyncBatchPublisher connection closed unexpectedly: %s", reason)
        # M17: every unsettled outcome (in flight AND queued) gets ERROR —
        # the confirms for these delivery tags will never arrive.
        err = reason if isinstance(reason, BaseException) else RuntimeError(str(reason))
        self._fail_all(err)
        connection.ioloop.stop()  # _io_loop decides whether to reconnect

    def _on_channel_open(self, channel: Any) -> None:
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_closed)
        channel.add_on_return_callback(self._on_return)
        # ack_nack_callback: Basic.Ack / Basic.Nack frames (pipelined
        # confirms); callback: Confirm.SelectOk — confirm mode is active.
        channel.confirm_delivery(
            ack_nack_callback=self._on_delivery_confirmation,
            callback=self._on_confirm_select_ok,
        )

    def _on_confirm_select_ok(self, _frame: Any) -> None:
        with self._lock:
            self._next_tag = 0  # delivery tags are per-channel
        self._connected_once = True
        self._ready.set()
        logger.info("SyncBatchPublisher ready (confirm mode active)")
        self._drain()  # anything enqueued in the ready/connected race window

    def _on_channel_closed(self, channel: Any, reason: Any) -> None:
        """Channel died out from under us (connection may still be open)."""
        if self._closing.is_set() or channel is not self._channel:
            return  # shutdown path / stale channel — handled elsewhere
        logger.warning("SyncBatchPublisher channel closed unexpectedly: %s", reason)
        self._ready.clear()
        self._channel = None
        err = reason if isinstance(reason, BaseException) else RuntimeError(str(reason))
        self._fail_all(err)
        # Simplest correct recovery: recycle the whole connection (tag
        # counters and confirm mode are per-channel; a fresh connection via
        # the reconnect loop re-establishes both).
        try:
            if self._connection is not None and self._connection.is_open:
                self._connection.close()
        except Exception:  # pragma: no cover — best effort
            logger.debug("channel-closed: connection.close() failed", exc_info=True)

    def _on_delivery_confirmation(self, method_frame: Any) -> None:
        """Basic.Ack / Basic.Nack — settle one tag, or all ≤ tag if multiple."""
        method = method_frame.method
        tag = int(method.delivery_tag)
        multiple = bool(getattr(method, "multiple", False))
        acked = isinstance(method, self._pika.spec.Basic.Ack)
        status = PublishStatus.CONFIRMED if acked else PublishStatus.NACKED
        error = (
            None
            if acked
            else RuntimeError(f"Broker nacked delivery_tag={tag} (multiple={multiple})")
        )

        with self._lock:
            if multiple:
                tags = sorted(t for t in self._pending if t <= tag)
            else:
                tags = [tag] if tag in self._pending else []
            settled = [(t, self._pending.pop(t)) for t in tags]

        if not settled:
            # Late confirm for an abandoned-and-reaped tag, or unknown tag.
            logger.debug("Confirm for unknown delivery_tag=%s (late/reaped) — ignored", tag)
            return
        for t, slot in settled:
            self._settle(slot, status, error=error, delivery_tag=t)

    def _on_return(self, _channel: Any, method: Any, properties: Any, _body: bytes) -> None:
        """Basic.Return — unroutable mandatory publish bounced by the broker.

        pika delivers the Return BEFORE the corresponding Basic.Ack, so we
        settle the slot RETURNED here and first-settlement-wins makes the
        following Ack a no-op. Matched to the MOST RECENT unsettled publish
        by (exchange, routing_key) and, when the broker echoed one, message_id.
        """
        msg_id = getattr(properties, "message_id", None)
        with self._lock:
            candidate: _Slot | None = None
            candidate_tag: int | None = None
            for t, slot in self._pending.items():  # insertion order = publish order
                if slot.outcome is not None or slot.abandoned:
                    continue
                env = slot.envelope
                if env.exchange != method.exchange or env.routing_key != method.routing_key:
                    continue
                if msg_id is not None and env.message_id != msg_id:
                    continue
                candidate, candidate_tag = slot, t  # keep last match = most recent
            if candidate is None:
                logger.warning(
                    "Basic.Return with no matching unsettled publish "
                    "(exchange=%r routing_key=%r message_id=%r) — ignored",
                    method.exchange,
                    method.routing_key,
                    msg_id,
                )
                return
            # Leave the tag in _pending: the broker still sends the Ack for a
            # returned message; _on_delivery_confirmation pops it (and its
            # settle attempt no-ops — first settlement wins).
            self._settle(
                candidate,
                PublishStatus.RETURNED,
                error=RuntimeError(
                    f"Unroutable: reply_code={getattr(method, 'reply_code', None)} "
                    f"reply_text={getattr(method, 'reply_text', None)}"
                ),
                delivery_tag=candidate_tag,
            )

    # ── publishing (I/O thread) ───────────────────────────────────────────

    def _drain(self) -> None:
        """Drain the caller queue onto the channel (runs on the I/O thread)."""
        while True:
            with self._lock:
                if not self._queue:
                    return
                envelope, slot = self._queue.popleft()
                channel = self._channel

            if channel is None or not self._ready.is_set() or not channel.is_open:
                self._settle(
                    slot,
                    PublishStatus.ERROR,
                    error=RuntimeError("SyncBatchPublisher is not connected"),
                )
                continue

            try:
                properties = self._build_properties(envelope)
            except Exception as exc:
                self._settle(slot, PublishStatus.ERROR, error=exc)
                continue

            with self._lock:
                self._next_tag += 1
                tag = self._next_tag
                self._pending[tag] = slot

            try:
                channel.basic_publish(
                    exchange=envelope.exchange,
                    routing_key=envelope.routing_key,
                    body=envelope.body,
                    properties=properties,
                    mandatory=envelope.mandatory,
                )
            except Exception as exc:
                with self._lock:
                    self._pending.pop(tag, None)
                self._settle(slot, PublishStatus.ERROR, error=exc)
                # Our tag counter may now disagree with pika's — never keep
                # publishing on a desynced channel. Recycle it (the channel
                # close callback fails any siblings and triggers reconnect).
                try:
                    if channel.is_open:
                        channel.close()
                except Exception:  # pragma: no cover — best effort
                    logger.debug("drain: channel.close() failed", exc_info=True)
                return

    def _build_properties(self, envelope: MessageEnvelope) -> Any:
        """Build pika.BasicProperties exactly like SyncTransport._publish_on_channel."""
        properties = self._pika.BasicProperties(
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
        return properties
