"""Prometheus metrics middleware — tracks consume and publish operations.

Protocol-based approach: works with any Prometheus-compatible client
(prometheus_client, StatsD, custom implementations, etc.).

The full metric inventory (name, type, labels, meaning) is
:func:`metric_inventory`; ``docs/observability.md`` is generated from it and
a unit test keeps the two in sync. Every label is bounded: either an
enumerated value set (:func:`bounded_label_values`) or a declared topology
name (``queue`` / ``exchange``). Message ids, routing keys and tenant ids
are never labels.

Two emitters share one collector:

* :class:`MetricsMiddleware` — per-message consume and publish metrics on
  the routes / publish middlewares it is attached to.
* :class:`BrokerMetrics` — connection, channel, flow-control, consumer
  cancel and transport-level publish metrics. Both brokers build one from
  the first ``MetricsMiddleware`` with a collector (publish middlewares
  first, then any route), so a publish-only broker gets them too.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from rabbitkit.core.config import MetricsConfig
from rabbitkit.core.errors import classify_error
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import ErrorCategory, MessageEnvelope, PublishOutcome, PublishReason, PublishStatus
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)

# ── Metric names (backwards-compat aliases pointing at the default config) ─

MESSAGES_CONSUMED_TOTAL = MetricsConfig().consumed_total
MESSAGE_PROCESSING_SECONDS = MetricsConfig().processing_seconds
MESSAGES_PUBLISHED_TOTAL = MetricsConfig().published_total
MESSAGE_PUBLISH_SECONDS = MetricsConfig().publish_seconds


def _publish_status_label(result: Any) -> str:
    """Return the ``status`` label for a publish outcome.

    ``broker.publish()`` never raises on its own -- it returns a
    ``PublishOutcome`` so callers can branch on NACKED/TIMEOUT/RETURNED/ERROR
    themselves. Before this fix, ``publish_scope``'s "no exception raised"
    branch hardcoded the label to ``"success"`` regardless of that outcome's
    actual ``.status`` -- so a NACKED, TIMEOUT, RETURNED, or even an
    outcome-level ERROR publish (none of which raise) was counted as a
    success in Prometheus, silently hiding real failures from dashboards and
    alerts. Reads the real status when the call returned a ``PublishOutcome``
    (always true for rabbitkit's own transports); falls back to "success"
    for a custom/duck-typed publish fn that returns something else.
    """
    if isinstance(result, PublishOutcome):
        return result.status.value
    return "success"


#: ``reason`` label of ``messages_published_total`` when the outcome carries
#: no reason (a success), when the publish call raised, or when a custom
#: transport returned a reason outside :class:`PublishReason`.
REASON_NONE = "none"
REASON_EXCEPTION = "exception"
REASON_OTHER = "other"


def _publish_reason_label(result: Any) -> str:
    """Bounded ``reason`` label for a publish result (G-22)."""
    reason = getattr(result, "reason", None) if isinstance(result, PublishOutcome) else None
    if reason is None:
        return REASON_NONE
    return reason if reason in PublishReason.ALL else REASON_OTHER


#: Bounded ``reason`` values of ``connection_failures_total``.
CONNECTION_FAILURE_REASONS: frozenset[str] = frozenset(
    {
        # terminal (TerminalConnectionError.reason)
        "authentication_failed",
        "vhost_access_refused",
        "tls_verification_failed",
        "tls_handshake_rejected",
        "reconnect_budget_exhausted",
        # transient
        "connection_refused",
        "connection_reset",
        "timeout",
        "dns_failure",
        "tls_error",
        "broker_closed",
        "connection_lost",
        "other",
    }
)

# Client-library exception class names (aiormq / pika), matched by name so
# this module stays transport-free.
_BROKER_CLOSED_NAMES = frozenset({"ConnectionClosed", "ConnectionClosedByBroker"})
_LOST_NAMES = frozenset(
    {"AMQPConnectionError", "StreamLostError", "ConnectionWrongStateError", "IncompatibleProtocolError", "AMQPError"}
)
_MAX_CAUSE_DEPTH = 4


def connection_failure_reason(error: BaseException | None) -> str:
    """Bounded reason code for a connection failure (a metric label, G-22).

    A :class:`~rabbitkit.core.errors.TerminalConnectionError` keeps its own
    ``reason``. Otherwise *error* and its ``__cause__`` chain are mapped to
    one of :data:`CONNECTION_FAILURE_REASONS`; ``None`` (a drop with no
    error) is ``connection_lost``. Never returns exception text.
    """
    if error is None:
        return "connection_lost"
    from rabbitkit.core.errors import TerminalConnectionError

    if isinstance(error, TerminalConnectionError):
        return error.reason if error.reason in CONNECTION_FAILURE_REASONS else "other"
    import socket
    import ssl

    current: BaseException | None = error
    fallback = "other"
    for _ in range(_MAX_CAUSE_DEPTH):
        if current is None:
            break
        if isinstance(current, ConnectionRefusedError):
            return "connection_refused"
        if isinstance(current, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return "connection_reset"
        if isinstance(current, TimeoutError):
            return "timeout"
        if isinstance(current, socket.gaierror):
            return "dns_failure"
        if isinstance(current, ssl.SSLError):
            return "tls_error"
        names = {cls.__name__ for cls in type(current).__mro__}
        if names & _BROKER_CLOSED_NAMES:
            return "broker_closed"
        if fallback == "other" and (names & _LOST_NAMES or isinstance(current, (ConnectionError, EOFError))):
            fallback = "connection_lost"
        current = current.__cause__
    return fallback


def _queue_label(message: RabbitMessage) -> str:
    """Return the ``queue`` label value for a consumed message (M3).

    Prefers the BOUND queue name (``x-rabbitkit-original-queue``, set by
    the broker's ``on_message`` wrapper before any middleware runs) over the
    message's routing key. A topic/``Path()`` routing key that embeds an ID,
    tenant, or other per-message value is unbounded — using it directly as a
    Prometheus label creates one time series per distinct value ever seen
    (cardinality explosion), and the label was misnamed besides (it's a
    routing key, not a queue). Falls back to ``routing_key`` only when the
    header is absent — e.g. a message built directly in a test rather than
    delivered through a real broker/``TestBroker`` consume path.
    """
    original_queue = message.headers.get("x-rabbitkit-original-queue")
    if original_queue:
        return str(original_queue)
    return message.routing_key or "unknown"


# ── Protocol ─────────────────────────────────────────────────────────────


@runtime_checkable
class MetricsCollector(Protocol):
    """Protocol for metrics collection — works with Prometheus, StatsD, etc."""

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        """Increment a counter metric."""
        ...

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        """Observe a value on a histogram metric."""
        ...

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        """Set a gauge metric to an absolute value (e.g. queue depth)."""
        ...


# ── Prometheus implementation (optional import) ──────────────────────────


class PrometheusCollector:
    """Concrete MetricsCollector that wraps the ``prometheus_client`` library.

    The ``prometheus_client`` import is lazy — the library is only required
    when this class is instantiated, not when the module is imported.

    Usage::

        collector = PrometheusCollector()                      # global REGISTRY
        collector = PrometheusCollector(registry=my_registry)  # an app's own

    Args:
        registry: The ``prometheus_client.CollectorRegistry`` to register
            metrics in. ``None`` (the default) uses the global
            ``prometheus_client.REGISTRY``, as before. Pass your own to keep
            rabbitkit's metrics in an application's (e.g. obskit's) registry,
            or a fresh one per test.

    Two collectors on the same registry share the metrics: a metric another
    collector (or anything else) already registered under the same name and
    type is reused instead of raising prometheus_client's "Duplicated
    timeseries" error.
    """

    def __init__(self, registry: Any = None) -> None:
        try:
            import prometheus_client
        except ImportError as exc:
            msg = (
                "PrometheusCollector requires the 'prometheus_client' package. "
                "Install it with: pip install prometheus-client"
            )
            raise ImportError(msg) from exc

        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}
        self._gauges: dict[str, Any] = {}
        self._prometheus_client = prometheus_client
        self._registry = registry if registry is not None else prometheus_client.REGISTRY

    @property
    def registry(self) -> Any:
        """The registry this collector registers its metrics in."""
        return self._registry

    def _create(self, kind: str, name: str, label_names: tuple[str, ...]) -> Any:
        factory = getattr(self._prometheus_client, kind)
        try:
            return factory(name, f"rabbitkit {name}", label_names, registry=self._registry)
        except ValueError:
            # Already registered (a second collector on this registry): reuse
            # it if it is the same kind of metric with the same labels.
            existing = getattr(self._registry, "_names_to_collectors", {}).get(name)
            same_kind = type(existing).__name__ == kind
            if existing is None or not same_kind or tuple(getattr(existing, "_labelnames", ())) != label_names:
                raise
            return existing

    def _get_counter(self, name: str, label_names: tuple[str, ...]) -> Any:
        if name not in self._counters:
            self._counters[name] = self._create("Counter", name, label_names)
        return self._counters[name]

    def _get_histogram(self, name: str, label_names: tuple[str, ...]) -> Any:
        if name not in self._histograms:
            self._histograms[name] = self._create("Histogram", name, label_names)
        return self._histograms[name]

    def _get_gauge(self, name: str, label_names: tuple[str, ...]) -> Any:
        if name not in self._gauges:
            self._gauges[name] = self._create("Gauge", name, label_names)
        return self._gauges[name]

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        """Increment a Prometheus counter."""
        counter = self._get_counter(name, tuple(sorted(labels.keys())))
        self._child(counter, labels).inc(value)

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        """Observe a value on a Prometheus histogram."""
        histogram = self._get_histogram(name, tuple(sorted(labels.keys())))
        self._child(histogram, labels).observe(value)

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        """Set a Prometheus gauge to an absolute value."""
        gauge = self._get_gauge(name, tuple(sorted(labels.keys())))
        self._child(gauge, labels).set(value)

    @staticmethod
    def _child(metric: Any, labels: dict[str, str]) -> Any:
        """The labelled child, or the metric itself when there are no labels.

        ``prometheus_client`` raises ``ValueError: No label names were set``
        if you call ``.labels()`` on a metric constructed without label names,
        so an UNLABELLED metric must be used directly. Every unlabelled metric
        rabbitkit emits — ``reconnects_total``, ``channels_opened_total``,
        ``channel_rebuilds_total``, the broker/consumer lifecycle gauges, the
        settlement gauges and the health gauge — used to raise here, which
        took down the caller (e.g. ``broker.start()``) the first time it fired.
        """
        return metric.labels(**labels) if labels else metric


# ── Middleware ────────────────────────────────────────────────────────────


class MetricsMiddleware(BaseMiddleware):
    """Tracks consume and publish metrics via a pluggable MetricsCollector.

    If ``collector`` is None, all operations pass through without
    any overhead (no-op mode).

    Usage::

        collector = PrometheusCollector()
        middleware = MetricsMiddleware(collector)

    Or with a custom collector::

        class MyCollector:
            def inc_counter(self, name, labels, value=1.0): ...
            def observe_histogram(self, name, labels, value): ...

        middleware = MetricsMiddleware(MyCollector())

    Args:
        collector: Any object satisfying the MetricsCollector protocol.
            None for no-op (passthrough) mode.
    """

    def __init__(
        self,
        collector: MetricsCollector | None = None,
        config: MetricsConfig | None = None,
    ) -> None:
        self._collector = collector
        self._cfg = config or MetricsConfig()
        # Plan §11: the declared ``in_flight_messages`` gauge was never
        # emitted. Track handlers currently inside consume_scope (per queue)
        # and publish the gauge on every enter/exit.
        self._in_flight: dict[str, int] = {}
        self._in_flight_lock = threading.Lock()
        # G-22: set by the broker when its transport reports the real
        # send-to-confirm latency (BrokerMetrics); this middleware then stops
        # timing the whole publish call as "confirm latency".
        self._confirm_latency_from_transport = False

    @property
    def collector(self) -> MetricsCollector | None:
        """The configured collector (None in no-op mode). Read by
        ``HandlerPipeline``/``RetryMiddleware`` to emit settlement/retry
        metrics they observe but this middleware itself cannot (M2) --
        settlement happens in the pipeline's own ack-orchestration code,
        outside this middleware's ``consume_scope``."""
        return self._collector

    @property
    def config(self) -> MetricsConfig:
        return self._cfg

    def _in_flight_delta(self, queue: str, delta: int) -> None:
        if self._collector is None:
            return
        with self._in_flight_lock:
            value = max(0, self._in_flight.get(queue, 0) + delta)
            self._in_flight[queue] = value
        set_gauge = getattr(self._collector, "set_gauge", None)
        if set_gauge is not None:
            set_gauge(self._cfg.in_flight_messages, {"queue": queue}, float(value))

    def _observe_publish(self, exchange: str, result: Any, elapsed: float) -> None:
        """Publish-side counters + histograms shared by both scopes.

        ``messages_published_total`` carries the outcome's ``status`` and a
        bounded ``reason`` (G-22). ``publish_confirm_latency_seconds`` is
        observed here only when no transport reports it (TestBroker, a
        custom publish fn): then it is the whole call for a CONFIRMED
        outcome. Under ``SyncBroker`` / ``AsyncBroker`` the transport times
        send-to-confirm itself (see :class:`BrokerMetrics`).
        """
        assert self._collector is not None
        self._collector.inc_counter(
            self._cfg.published_total,
            {"exchange": exchange, "status": _publish_status_label(result), "reason": _publish_reason_label(result)},
        )
        self._collector.observe_histogram(self._cfg.publish_seconds, {"exchange": exchange}, elapsed)
        status = getattr(result, "status", None)
        if status is PublishStatus.CONFIRMED and not self._confirm_latency_from_transport:
            self._collector.observe_histogram(
                self._cfg.publish_confirm_latency_seconds, {"exchange": exchange}, elapsed
            )

    def record_settlement(self, message: RabbitMessage, disposition: str) -> None:
        """Emit the ack/nack/reject counter for a settled message (M2).

        ``consume_scope``/``consume_scope_async`` only wrap handler
        execution -- final settlement (ack/nack/reject per AckPolicy) is
        decided by the pipeline's own ack-orchestration code, which runs
        AFTER this middleware's wrapped call returns. ``HandlerPipeline``
        calls this directly once a route's message is settled, if a
        ``MetricsMiddleware`` is present on that route.
        """
        if self._collector is None:
            return
        name = {
            "acked": self._cfg.messages_acked_total,
            "nacked": self._cfg.messages_nacked_total,
            "rejected": self._cfg.messages_rejected_total,
        }.get(disposition)
        if name is None:
            return  # pragma: no cover - defensive, disposition is always one of the three
        self._collector.inc_counter(name, {"queue": _queue_label(message)})

    def record_settlement_outcome_unknown(self, message: RabbitMessage) -> None:
        """Count an ack that raised after the handler succeeded: the broker
        may or may not have received it (labelled by ``queue``)."""
        if self._collector is None:
            return
        self._collector.inc_counter(self._cfg.settlement_outcome_unknown_total, {"queue": _queue_label(message)})

    def _record_failure(self, queue: str, exc: BaseException) -> None:
        """``messages_failed_total{queue, category}``: the handler failure's
        category (one of the six ``ErrorCategory`` values), classified the
        same way as the pipeline's "Handler failed" log line."""
        assert self._collector is not None
        category = classify_error(exc).category.value
        self._collector.inc_counter(self._cfg.messages_failed_total, {"queue": queue, "category": category})

    # ── Consume-side ──────────────────────────────────────────────────

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        """Wrap handler execution with metrics tracking (sync)."""
        if self._collector is None:
            return call_next(message)

        queue = _queue_label(message)
        if message.redelivered:
            # Broker-redelivery rate: a sustained rise means handlers are
            # dying/timing out before acking, which the success/error
            # consume counters alone can't distinguish from normal traffic.
            self._collector.inc_counter(
                self._cfg.messages_redelivered_total,
                {"queue": queue},
            )
        start = time.monotonic()
        self._in_flight_delta(queue, +1)
        try:
            result = call_next(message)
        except BaseException as exc:
            self._collector.inc_counter(
                self._cfg.consumed_total,
                {"queue": queue, "status": "error"},
            )
            self._record_failure(queue, exc)
            self._collector.observe_histogram(
                self._cfg.processing_seconds,
                {"queue": queue},
                time.monotonic() - start,
            )
            raise
        else:
            self._collector.inc_counter(
                self._cfg.consumed_total,
                {"queue": queue, "status": "success"},
            )
            self._collector.observe_histogram(
                self._cfg.processing_seconds,
                {"queue": queue},
                time.monotonic() - start,
            )
            return result
        finally:
            self._in_flight_delta(queue, -1)

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Wrap handler execution with metrics tracking (async)."""
        if self._collector is None:
            return await call_next(message)

        queue = _queue_label(message)
        if message.redelivered:
            # Broker-redelivery rate — see consume_scope.
            self._collector.inc_counter(
                self._cfg.messages_redelivered_total,
                {"queue": queue},
            )
        start = time.monotonic()
        self._in_flight_delta(queue, +1)
        try:
            result = await call_next(message)
        except BaseException as exc:
            self._collector.inc_counter(
                self._cfg.consumed_total,
                {"queue": queue, "status": "error"},
            )
            self._record_failure(queue, exc)
            self._collector.observe_histogram(
                self._cfg.processing_seconds,
                {"queue": queue},
                time.monotonic() - start,
            )
            raise
        else:
            self._collector.inc_counter(
                self._cfg.consumed_total,
                {"queue": queue, "status": "success"},
            )
            self._collector.observe_histogram(
                self._cfg.processing_seconds,
                {"queue": queue},
                time.monotonic() - start,
            )
            return result
        finally:
            self._in_flight_delta(queue, -1)

    # ── Publish-side ──────────────────────────────────────────────────

    def publish_scope(
        self,
        call_next: Callable[[MessageEnvelope], Any],
        envelope: MessageEnvelope,
    ) -> Any:
        """Wrap outgoing publish with metrics tracking (sync)."""
        if self._collector is None:
            return call_next(envelope)

        exchange = envelope.exchange or "default"
        start = time.monotonic()
        try:
            result = call_next(envelope)
        except BaseException:
            self._collector.inc_counter(
                self._cfg.published_total,
                {"exchange": exchange, "status": "error", "reason": REASON_EXCEPTION},
            )
            self._collector.observe_histogram(
                self._cfg.publish_seconds,
                {"exchange": exchange},
                time.monotonic() - start,
            )
            raise
        else:
            self._observe_publish(exchange, result, time.monotonic() - start)
            return result

    async def publish_scope_async(
        self,
        call_next: Callable[[MessageEnvelope], Awaitable[Any]],
        envelope: MessageEnvelope,
    ) -> Any:
        """Wrap outgoing publish with metrics tracking (async)."""
        if self._collector is None:
            return await call_next(envelope)

        exchange = envelope.exchange or "default"
        start = time.monotonic()
        try:
            result = await call_next(envelope)
        except BaseException:
            self._collector.inc_counter(
                self._cfg.published_total,
                {"exchange": exchange, "status": "error", "reason": REASON_EXCEPTION},
            )
            self._collector.observe_histogram(
                self._cfg.publish_seconds,
                {"exchange": exchange},
                time.monotonic() - start,
            )
            raise
        else:
            self._observe_publish(exchange, result, time.monotonic() - start)
            return result


# ── Broker/transport metrics (G-22) ───────────────────────────────────────

#: Connection roles: the async broker's two connections, the sync broker's
#: one shared connection, and a terminal failure with no role.
CONNECTION_ROLES: frozenset[str] = frozenset({"publisher", "consumer", "shared", "connection"})
#: ``reason`` values of ``publish_routing_failures_total``.
ROUTING_FAILURE_REASONS: frozenset[str] = frozenset({PublishReason.RETURNED, PublishReason.EXCHANGE_NOT_FOUND})
_EXCHANGE_NOT_FOUND_CODE = 404


def _routing_failure_reason(outcome: PublishOutcome) -> str | None:
    if outcome.status is PublishStatus.RETURNED:
        return PublishReason.RETURNED
    if outcome.status is PublishStatus.ERROR:
        if outcome.reason == PublishReason.EXCHANGE_NOT_FOUND:
            return PublishReason.EXCHANGE_NOT_FOUND
        # The sync transport attaches no reason: pika's ChannelClosedByBroker
        # carries the 404 as reply_code.
        if outcome.reason is None and getattr(outcome.error, "reply_code", None) == _EXCHANGE_NOT_FOUND_CODE:
            return PublishReason.EXCHANGE_NOT_FOUND
    return None


class BrokerMetrics:
    """Connection, channel, flow-control and transport-publish metrics (G-22).

    Transport-free: the brokers feed it from their transport's hooks
    (``on_connection_state``, ``on_reconnect_attempt``,
    ``on_connection_failed``, ``on_reconnect``, ``on_channel_opened`` /
    ``on_channel_rebuilt``, ``on_blocked`` / ``on_unblocked``,
    ``on_consumer_cancelled`` / ``on_consumer_resumed``,
    ``on_publish_outcome``) and sample the gauges that change on every
    publish (in flight, channels) periodically instead of on the hot path.

    Args:
        collector: Where to emit (the first ``MetricsMiddleware``'s collector).
        config: Metric names.
        roles: The connection roles this broker has (``("publisher",
            "consumer")`` for async, ``("shared",)`` for sync).
    """

    def __init__(self, collector: MetricsCollector, config: MetricsConfig, roles: tuple[str, ...]) -> None:
        self._collector = collector
        self._cfg = config
        self._roles = roles
        self._set_gauge: Callable[[str, dict[str, str], float], None] | None = getattr(collector, "set_gauge", None)
        self._up: dict[str, bool] = dict.fromkeys(roles, False)
        self._lock = threading.Lock()

    @property
    def config(self) -> MetricsConfig:
        return self._cfg

    def _gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        if self._set_gauge is not None:
            self._set_gauge(name, labels, value)

    # ── connections ───────────────────────────────────────────────────

    def connection_state(self, role: str, up: bool, error: BaseException | None = None) -> None:
        """A connection came up or dropped. A drop counts one
        ``connection_failures_total`` with the classified reason."""
        if role not in CONNECTION_ROLES:
            role = "connection"
        with self._lock:
            self._up[role] = up
            states = dict(self._up)
        if not up:
            self._collector.inc_counter(
                self._cfg.connection_failures_total, {"role": role, "reason": connection_failure_reason(error)}
            )
        self._publish_states(states)

    def set_states(self, states: dict[str, bool]) -> None:
        """Overwrite the per-role state from a sample (no failure counted)."""
        with self._lock:
            for role, up in states.items():
                if role in self._up:
                    self._up[role] = up
            current = dict(self._up)
        self._publish_states(current)

    def _publish_states(self, states: dict[str, bool]) -> None:
        for role, up in states.items():
            self._gauge(self._cfg.connection_up, {"role": role}, 1.0 if up else 0.0)
        self._gauge(self._cfg.connections_active, {}, float(sum(states.values())))
        self._gauge(self._cfg.broker_connected, {}, 1.0 if states and all(states.values()) else 0.0)

    def reconnected(self) -> None:
        """A connection reconnected (``reconnects_total``)."""
        self._collector.inc_counter(self._cfg.reconnects_total, {})

    def reconnect_failed(self, role: str, error: BaseException | None) -> None:
        """A reconnect attempt failed."""
        if role not in CONNECTION_ROLES:
            role = "connection"
        self._collector.inc_counter(self._cfg.reconnect_attempts_total, {"role": role})
        self._collector.inc_counter(
            self._cfg.connection_failures_total, {"role": role, "reason": connection_failure_reason(error)}
        )

    def connection_failed(self, error: BaseException) -> None:
        """A connection stopped reconnecting for good (terminal)."""
        role = getattr(error, "role", "") or "connection"
        if role not in CONNECTION_ROLES:
            role = "connection"
        self._collector.inc_counter(
            self._cfg.connection_failures_total, {"role": role, "reason": connection_failure_reason(error)}
        )
        with self._lock:
            if role in self._up:
                self._up[role] = False
            states = dict(self._up)
        self._publish_states(states)

    def stopped(self, routes: Iterable[str] = ()) -> None:
        """The broker stopped: every sampled gauge goes to zero."""
        with self._lock:
            self._up = dict.fromkeys(self._roles, False)
            states = dict(self._up)
        self._publish_states(states)
        for name in (self._cfg.channels_active, self._cfg.publish_in_flight, self._cfg.connection_blocked):
            self._gauge(name, {}, 0.0)
        for queue in routes:
            self._gauge(self._cfg.route_consuming, {"queue": queue}, 0.0)

    # ── channels / flow control ───────────────────────────────────────

    def channel_opened(self) -> None:
        self._collector.inc_counter(self._cfg.channels_opened_total, {})

    def channel_rebuilt(self) -> None:
        self._collector.inc_counter(self._cfg.channel_rebuilds_total, {})

    def blocked(self, is_blocked: bool) -> None:
        """``connection.blocked`` / ``connection.unblocked`` from RabbitMQ."""
        self._gauge(self._cfg.connection_blocked, {}, 1.0 if is_blocked else 0.0)

    # ── consumers ─────────────────────────────────────────────────────

    def consumer_cancelled(self, queue: str) -> None:
        """The broker cancelled the consumer of *queue* (``basic.cancel``)."""
        self._collector.inc_counter(self._cfg.consumer_cancelled_total, {"queue": queue})
        self._gauge(self._cfg.route_consuming, {"queue": queue}, 0.0)

    def consumer_resumed(self, queue: str) -> None:
        self._gauge(self._cfg.route_consuming, {"queue": queue}, 1.0)

    # ── publishing ────────────────────────────────────────────────────

    def publish_outcome(self, outcome: PublishOutcome, seconds: float | None) -> None:
        """A transport publish finished. *seconds* is send-to-broker-answer."""
        exchange = outcome.exchange or "default"
        if seconds is not None and outcome.status is PublishStatus.CONFIRMED:
            self._collector.observe_histogram(
                self._cfg.publish_confirm_latency_seconds, {"exchange": exchange}, seconds
            )
        reason = _routing_failure_reason(outcome)
        if reason is not None:
            self._collector.inc_counter(
                self._cfg.publish_routing_failures_total, {"exchange": exchange, "reason": reason}
            )

    def sample(
        self,
        *,
        channels_active: int | None = None,
        publish_in_flight: int | None = None,
        publish_in_flight_bytes: int | None = None,
        publish_channels_in_use: int | None = None,
        connection_blocked: bool | None = None,
        consuming: dict[str, bool] | None = None,
    ) -> None:
        """Set the sampled gauges (``None`` = not available, not emitted)."""
        for name, value in (
            (self._cfg.channels_active, channels_active),
            (self._cfg.publish_in_flight, publish_in_flight),
            (self._cfg.publish_in_flight_bytes, publish_in_flight_bytes),
            (self._cfg.publish_channels_in_use, publish_channels_in_use),
        ):
            if value is not None:
                self._gauge(name, {}, float(value))
        if connection_blocked is not None:
            self.blocked(connection_blocked)
        for queue, active in (consuming or {}).items():
            self._gauge(self._cfg.route_consuming, {"queue": queue}, 1.0 if active else 0.0)

    def sample_worker_pool(self, pending: int) -> None:
        """Set ``worker_pool_pending``."""
        self._gauge(self._cfg.worker_pool_pending, {}, float(pending))

    def take_over_confirm_latency(self, middlewares: Iterable[object]) -> None:
        """The transport reports send-to-confirm latency: every
        ``MetricsMiddleware`` in *middlewares* stops timing the whole call."""
        for mw in middlewares:
            if isinstance(mw, MetricsMiddleware):
                mw._confirm_latency_from_transport = True


# ── Inventory (code truth for docs/observability.md) ─────────────────────


@dataclass(frozen=True, slots=True)
class MetricSpec:
    """One metric rabbitkit emits.

    Attributes:
        name: Full metric name (namespace applied).
        kind: ``counter``, ``gauge`` or ``histogram``.
        labels: The exact label names every sample carries.
        emitter: Who emits it.
        help: What it means.
    """

    name: str
    kind: str
    labels: tuple[str, ...]
    emitter: str
    help: str


_MW = "MetricsMiddleware"
_BROKER = "broker (BrokerMetrics)"
_RETRY = "RetryMiddleware"
_BULK = "broker bulk API"
_ACKER = "coalescing acker"
_POLLER = "QueueMetricsPoller"
_OUTBOX = "producer outbox"

# (MetricsConfig property, kind, labels, emitter, help)
_SPECS: tuple[tuple[str, str, tuple[str, ...], str, str], ...] = (
    # consume
    ("consumed_total", "counter", ("queue", "status"), _MW, "Handler executions; status success or error."),
    ("messages_failed_total", "counter", ("queue", "category"), _MW, "Handler failures by ErrorCategory."),
    ("processing_seconds", "histogram", ("queue",), _MW, "Handler duration."),
    ("in_flight_messages", "gauge", ("queue",), _MW, "Handlers running now."),
    ("messages_redelivered_total", "counter", ("queue",), _MW, "Deliveries the broker flagged redelivered."),
    ("messages_acked_total", "counter", ("queue",), _MW, "Messages acked by the pipeline."),
    ("messages_nacked_total", "counter", ("queue",), _MW, "Messages nacked by the pipeline."),
    ("messages_rejected_total", "counter", ("queue",), _MW, "Messages rejected by the pipeline."),
    (
        "settlement_outcome_unknown_total",
        "counter",
        ("queue",),
        _MW,
        "Settlements whose frame may not have reached the broker (redelivery possible).",
    ),
    ("messages_retried_total", "counter", ("queue", "category"), _RETRY, "Failures handed to a retry delay queue."),
    ("messages_dead_lettered_total", "counter", ("queue", "category"), _RETRY, "Terminal failures sent to the DLQ."),
    ("retry_handoff_failures_total", "counter", ("queue",), _RETRY, "Retry/DLQ copy publishes that failed."),
    ("retry_handoff_paused", "gauge", ("queue",), _RETRY, "1 while a route's retry handoff is backing off."),
    (
        "dedup_fallback_total",
        "counter",
        ("queue",),
        "DeduplicationMiddleware",
        "Messages processed despite a Redis error.",
    ),
    (
        "rate_limit_dropped_total",
        "counter",
        ("reason",),
        "RateLimitMiddleware",
        "Messages settled without running the handler.",
    ),
    ("consumer_cancelled_total", "counter", ("queue",), _BROKER, "Consumers the broker cancelled (basic.cancel)."),
    ("route_consuming", "gauge", ("queue",), _BROKER, "1 while the route's consumer is subscribed."),
    ("consumer_active", "gauge", (), _BROKER, "Routes registered while started."),
    ("worker_pool_pending", "gauge", (), _BROKER, "Deliveries waiting for a worker."),
    # publish
    (
        "published_total",
        "counter",
        ("exchange", "status", "reason"),
        _MW,
        "broker.publish() outcomes by PublishStatus and PublishReason.",
    ),
    ("publish_seconds", "histogram", ("exchange",), _MW, "Whole publish call, including admission."),
    (
        "publish_confirm_latency_seconds",
        "histogram",
        ("exchange",),
        _BROKER,
        "Send to broker confirm for CONFIRMED publishes (transport-timed).",
    ),
    (
        "publish_routing_failures_total",
        "counter",
        ("exchange", "reason"),
        _BROKER,
        "Unroutable publishes: returned or exchange_not_found (all transport publishes).",
    ),
    ("publish_in_flight", "gauge", (), _BROKER, "Async publishes between admission and outcome (sampled)."),
    ("publish_in_flight_bytes", "gauge", (), _BROKER, "Bytes held by max_in_flight_bytes (sampled; only when set)."),
    ("publish_channels_in_use", "gauge", (), _BROKER, "Async publisher channels checked out (sampled)."),
    ("bulk_publish_items_total", "counter", ("status", "reason"), _BULK, "publish_many/iter_publish item outcomes."),
    ("bulk_publish_batch_size", "histogram", (), _BULK, "publish_many input sizes."),
    ("settlement_items_total", "counter", ("action", "status"), _BULK, "ack_many/nack_many item outcomes."),
    # connection
    ("broker_connected", "gauge", (), _BROKER, "1 while every connection is up; follows reconnects."),
    ("connection_up", "gauge", ("role",), _BROKER, "1 while the connection of this role is up."),
    ("connections_active", "gauge", (), _BROKER, "Connections up."),
    ("channels_active", "gauge", (), _BROKER, "Channels held open (sampled)."),
    ("channels_opened_total", "counter", (), _BROKER, "Channels created."),
    ("channel_rebuilds_total", "counter", (), _BROKER, "Channels created to replace a lost one."),
    ("reconnects_total", "counter", (), _BROKER, "Successful reconnects (one per connection)."),
    ("reconnect_attempts_total", "counter", ("role",), _BROKER, "Failed reconnect attempts."),
    (
        "connection_failures_total",
        "counter",
        ("role", "reason"),
        _BROKER,
        "Connections lost, reconnect attempts failed, terminal give-ups.",
    ),
    ("connection_blocked", "gauge", (), _BROKER, "1 while RabbitMQ blocks the connection (memory/disk alarm)."),
    # settlement coordinator / coalescing acker
    ("settlement_pending", "gauge", (), _ACKER, "Registered deliveries not yet settled."),
    ("settlement_ack_ready", "gauge", (), _ACKER, "Deliveries approved for ack, not yet sent."),
    ("settlement_frontier", "gauge", (), _ACKER, "Highest tag a cumulative ack could cover."),
    ("settlement_gap_count", "gauge", (), _ACKER, "Blocking deliveries below an ack-ready one."),
    ("settlement_oldest_pending_age_seconds", "gauge", (), _ACKER, "Age of the oldest unsettled delivery."),
    ("settlement_coalescing_ratio", "gauge", (), _ACKER, "Deliveries settled per frame."),
    ("settlement_coalesced_total", "counter", (), _ACKER, "Tags settled through a cumulative ack."),
    # broker-side (management API poller)
    ("queue_messages_ready", "gauge", ("queue",), _POLLER, "Messages ready (backlog)."),
    ("queue_messages_unacked", "gauge", ("queue",), _POLLER, "Messages delivered, not yet acked."),
    ("queue_messages_total", "gauge", ("queue",), _POLLER, "Ready plus unacked."),
    ("queue_consumers", "gauge", ("queue",), _POLLER, "Consumers attached to the queue."),
    # producer outbox (rabbitkit.outbox)
    ("outbox_pending", "gauge", (), _OUTBOX, "Entries waiting to be published and confirmed."),
    ("outbox_failed_entries", "gauge", (), _OUTBOX, "Entries in the failed state (kept until replayed or purged)."),
    ("outbox_bytes", "gauge", (), _OUTBOX, "Bytes held by the outbox (pending and failed)."),
    ("outbox_enqueued_total", "counter", (), _OUTBOX, "Entries durably written."),
    ("outbox_relayed_total", "counter", (), _OUTBOX, "Entries confirmed by the broker and deleted."),
    (
        "outbox_publish_retries_total",
        "counter",
        ("status", "reason"),
        _OUTBOX,
        "Relay publishes not confirmed; the entry stays pending.",
    ),
    ("outbox_failed_total", "counter", ("status", "reason"), _OUTBOX, "Entries moved to the failed state."),
    ("outbox_full_total", "counter", ("reason",), _OUTBOX, "Enqueues refused at max_entries or max_bytes."),
)

#: ``HealthWatcher``'s default gauge (its name is a constructor argument).
HEALTH_STATE_GAUGE = "rabbitkit_health_state"


def metric_inventory(config: MetricsConfig | None = None) -> tuple[MetricSpec, ...]:
    """Every metric rabbitkit can emit, with its exact label names.

    Declared ``bulk_publish_admission_wait_seconds`` is not emitted and not
    listed. ``HealthWatcher``'s gauge is listed under its default name.
    """
    cfg = config or MetricsConfig()
    specs = [
        MetricSpec(getattr(cfg, attr), kind, labels, emitter, text) for attr, kind, labels, emitter, text in _SPECS
    ]
    specs.append(
        MetricSpec(
            HEALTH_STATE_GAUGE, "gauge", (), "HealthWatcher", "Debounced health: 0 healthy, 1 degraded, 2 unhealthy."
        )
    )
    return tuple(specs)


def bounded_label_values() -> dict[str, frozenset[str] | None]:
    """Allowed values per label name. ``None`` marks a declared topology name
    (``queue`` / ``exchange``): bounded by the application's topology, never
    by message content."""
    from rabbitkit.core.bulk import ALL_REASONS
    from rabbitkit.core.types import BulkPublishStatus, SettlementAction, SettlementItemStatus

    statuses = (
        {s.value for s in PublishStatus}
        | {s.value for s in BulkPublishStatus}
        | {s.value for s in SettlementItemStatus}
        | {"success", "error"}
    )
    return {
        "queue": None,
        "exchange": None,
        "status": frozenset(statuses),
        "reason": frozenset(
            PublishReason.ALL
            | ALL_REASONS
            | CONNECTION_FAILURE_REASONS
            | {REASON_NONE, REASON_EXCEPTION, REASON_OTHER, "nack", "drop", "wait_deadline_exceeded"}
            | {"max_entries", "max_bytes"}
        ),
        "category": frozenset(c.value for c in ErrorCategory),
        "role": CONNECTION_ROLES,
        "action": frozenset(a.value for a in SettlementAction),
    }


# ── Prometheus exposition (M-SRE1) ────────────────────────────────────────


def metrics_app() -> Callable[[Any, Any, Any], Awaitable[None]]:
    """Return a minimal ASGI app that exposes ``/metrics`` in Prometheus text format.

    Requires ``prometheus_client`` (lazy-imported on first request). Mount it
    behind your existing ASGI server (uvicorn, hypercorn) or the dashboard::

        from rabbitkit.middleware.metrics import metrics_app
        app = metrics_app()
        # uvicorn rabbitkit.middleware.metrics:metrics_app  (after binding the factory)

    For a stdlib one-liner without an ASGI server, see :func:`start_metrics_server`.
    """
    try:
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "metrics_app() requires the 'prometheus_client' package. Install it with: pip install prometheus-client"
        ) from exc

    async def app(scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or scope.get("path") != "/metrics":
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b"not found", "more_body": False})
            return
        body = generate_latest()
        headers = [[b"content-type", CONTENT_TYPE_LATEST.encode()]]
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": body, "more_body": False})

    return app


def start_metrics_server(port: int = 8000, host: str = "127.0.0.1") -> None:
    """Start a background HTTP server exposing ``/metrics`` on ``port``.

    Thin wrapper around ``prometheus_client.start_http_server``. Call once at
    process startup (e.g. in a ``RabbitApp.on_startup`` hook). For k8s, scrape
    this port with a ``ServiceMonitor`` / ``PodMonitor``.

    The default ``host`` is ``127.0.0.1`` (loopback only) so the metrics
    endpoint is not exposed to the network by default. For k8s / multi-host
    scrapers pass ``host="0.0.0.0"`` explicitly and restrict access with a
    NetworkPolicy (the metrics endpoint is unauthenticated and exposes broker
    topology/throughput — never expose it publicly without authn in front).

    Requires ``prometheus_client``.
    """
    try:
        from prometheus_client import start_http_server
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "start_metrics_server() requires the 'prometheus_client' package. "
            "Install it with: pip install prometheus-client"
        ) from exc
    start_http_server(port, host)
    logger.info("Prometheus metrics server on http://%s:%d/metrics", host, port)
