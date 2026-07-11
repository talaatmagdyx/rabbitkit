"""Prometheus metrics middleware — tracks consume and publish operations.

Protocol-based approach: works with any Prometheus-compatible client
(prometheus_client, StatsD, custom implementations, etc.).

Metric names:
- rabbitkit_messages_consumed_total   Counter(queue, status)
                                      status: success | error
- rabbitkit_message_processing_seconds Histogram(queue)
- rabbitkit_messages_published_total  Counter(exchange, status)
                                      status: confirmed | sent | nacked |
                                      timeout | returned | error (the real
                                      PublishOutcome.status value -- a
                                      raised exception escaping the publish
                                      call itself is also labeled "error")
- rabbitkit_message_publish_seconds   Histogram(exchange)
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from rabbitkit.core.config import MetricsConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope, PublishOutcome
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

        collector = PrometheusCollector()
        middleware = MetricsMiddleware(collector)
    """

    def __init__(self) -> None:
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

    def _get_counter(self, name: str, label_names: tuple[str, ...]) -> Any:
        if name not in self._counters:
            self._counters[name] = self._prometheus_client.Counter(
                name,
                f"rabbitkit {name}",
                label_names,
            )
        return self._counters[name]

    def _get_histogram(self, name: str, label_names: tuple[str, ...]) -> Any:
        if name not in self._histograms:
            self._histograms[name] = self._prometheus_client.Histogram(
                name,
                f"rabbitkit {name}",
                label_names,
            )
        return self._histograms[name]

    def _get_gauge(self, name: str, label_names: tuple[str, ...]) -> Any:
        if name not in self._gauges:
            self._gauges[name] = self._prometheus_client.Gauge(
                name,
                f"rabbitkit {name}",
                label_names,
            )
        return self._gauges[name]

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        """Increment a Prometheus counter."""
        label_names = tuple(sorted(labels.keys()))
        counter = self._get_counter(name, label_names)
        counter.labels(**labels).inc(value)

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        """Observe a value on a Prometheus histogram."""
        label_names = tuple(sorted(labels.keys()))
        histogram = self._get_histogram(name, label_names)
        histogram.labels(**labels).observe(value)

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        """Set a Prometheus gauge to an absolute value."""
        label_names = tuple(sorted(labels.keys()))
        gauge = self._get_gauge(name, label_names)
        gauge.labels(**labels).set(value)


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
        try:
            result = call_next(message)
        except BaseException:
            self._collector.inc_counter(
                self._cfg.consumed_total,
                {"queue": queue, "status": "error"},
            )
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
        try:
            result = await call_next(message)
        except BaseException:
            self._collector.inc_counter(
                self._cfg.consumed_total,
                {"queue": queue, "status": "error"},
            )
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
                {"exchange": exchange, "status": "error"},
            )
            self._collector.observe_histogram(
                self._cfg.publish_seconds,
                {"exchange": exchange},
                time.monotonic() - start,
            )
            raise
        else:
            self._collector.inc_counter(
                self._cfg.published_total,
                {"exchange": exchange, "status": _publish_status_label(result)},
            )
            self._collector.observe_histogram(
                self._cfg.publish_seconds,
                {"exchange": exchange},
                time.monotonic() - start,
            )
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
                {"exchange": exchange, "status": "error"},
            )
            self._collector.observe_histogram(
                self._cfg.publish_seconds,
                {"exchange": exchange},
                time.monotonic() - start,
            )
            raise
        else:
            self._collector.inc_counter(
                self._cfg.published_total,
                {"exchange": exchange, "status": _publish_status_label(result)},
            )
            self._collector.observe_histogram(
                self._cfg.publish_seconds,
                {"exchange": exchange},
                time.monotonic() - start,
            )
            return result


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
