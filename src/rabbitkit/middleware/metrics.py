"""Prometheus metrics middleware — tracks consume and publish operations.

Protocol-based approach: works with any Prometheus-compatible client
(prometheus_client, StatsD, custom implementations, etc.).

Metric names:
- rabbitkit_messages_consumed_total   Counter(queue, status)
- rabbitkit_message_processing_seconds Histogram(queue)
- rabbitkit_messages_published_total  Counter(exchange, status)
- rabbitkit_message_publish_seconds   Histogram(exchange)
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from rabbitkit.core.config import MetricsConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)

# ── Metric names (backwards-compat aliases pointing at the default config) ─

MESSAGES_CONSUMED_TOTAL = MetricsConfig().consumed_total
MESSAGE_PROCESSING_SECONDS = MetricsConfig().processing_seconds
MESSAGES_PUBLISHED_TOTAL = MetricsConfig().published_total
MESSAGE_PUBLISH_SECONDS = MetricsConfig().publish_seconds


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

    # ── Consume-side ──────────────────────────────────────────────────

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        """Wrap handler execution with metrics tracking (sync)."""
        if self._collector is None:
            return call_next(message)

        queue = message.routing_key or "unknown"
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

        queue = message.routing_key or "unknown"
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
                {"exchange": exchange, "status": "success"},
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
                {"exchange": exchange, "status": "success"},
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
