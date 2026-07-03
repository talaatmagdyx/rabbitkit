"""Tests for middleware/metrics.py — MetricsMiddleware."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.metrics import (
    MESSAGE_PROCESSING_SECONDS,
    MESSAGE_PUBLISH_SECONDS,
    MESSAGES_CONSUMED_TOTAL,
    MESSAGES_PUBLISHED_TOTAL,
    MetricsMiddleware,
)

# ── Helpers ──────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b'{"id": 1}',
        "routing_key": "test.queue",
        "headers": {},
        "path": {},
    }
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


def _make_envelope(**kwargs: object) -> MessageEnvelope:
    defaults: dict[str, object] = {"routing_key": "test.key", "body": b"test", "exchange": "test.exchange"}
    defaults.update(kwargs)
    return MessageEnvelope(**defaults)  # type: ignore[arg-type]


@dataclass
class _CounterCall:
    name: str
    labels: dict[str, str]
    value: float


@dataclass
class _HistogramCall:
    name: str
    labels: dict[str, str]
    value: float


class FakeCollector:
    """In-memory MetricsCollector for testing — stores all calls."""

    def __init__(self) -> None:
        self.counter_calls: list[_CounterCall] = []
        self.histogram_calls: list[_HistogramCall] = []

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        self.counter_calls.append(_CounterCall(name=name, labels=labels, value=value))

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        self.histogram_calls.append(_HistogramCall(name=name, labels=labels, value=value))


# ── Consume scope (sync) ─────────────────────────────────────────────────


class TestConsumeScope:
    def test_consume_success_increments_counter(self) -> None:
        """Successful consume increments counter with status=success."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()
        call_next = MagicMock(return_value="ok")

        result = mw.consume_scope(call_next, msg)

        assert result == "ok"
        call_next.assert_called_once_with(msg)

        # Counter: one success call
        assert len(collector.counter_calls) == 1
        cc = collector.counter_calls[0]
        assert cc.name == MESSAGES_CONSUMED_TOTAL
        assert cc.labels == {"queue": "test.queue", "status": "success"}
        assert cc.value == 1.0

    def test_consume_error_increments_error_counter(self) -> None:
        """Failed consume increments counter with status=error and re-raises."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()
        call_next = MagicMock(side_effect=ValueError("boom"))

        with pytest.raises(ValueError, match="boom"):
            mw.consume_scope(call_next, msg)

        assert len(collector.counter_calls) == 1
        cc = collector.counter_calls[0]
        assert cc.name == MESSAGES_CONSUMED_TOTAL
        assert cc.labels == {"queue": "test.queue", "status": "error"}

    def test_redelivered_message_increments_redelivered_counter(self) -> None:
        """Broker-redelivery rate: a redelivered=True consume increments the
        dedicated counter -- the signal that handlers are dying/timing out
        before acking, which success/error counts alone can't distinguish
        from ordinary traffic. Previously the redelivered flag was never
        counted at all."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message(redelivered=True)

        mw.consume_scope(MagicMock(return_value="ok"), msg)

        redelivered_calls = [
            c for c in collector.counter_calls if c.name.endswith("_messages_redelivered_total")
        ]
        assert len(redelivered_calls) == 1
        assert redelivered_calls[0].labels == {"queue": "test.queue"}

    def test_fresh_delivery_does_not_increment_redelivered_counter(self) -> None:
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message(redelivered=False)

        mw.consume_scope(MagicMock(return_value="ok"), msg)

        assert not any(
            c.name.endswith("_messages_redelivered_total") for c in collector.counter_calls
        )

    @pytest.mark.asyncio
    async def test_redelivered_counter_async(self) -> None:
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message(redelivered=True)

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(handler, msg)

        redelivered_calls = [
            c for c in collector.counter_calls if c.name.endswith("_messages_redelivered_total")
        ]
        assert len(redelivered_calls) == 1
        assert redelivered_calls[0].labels == {"queue": "test.queue"}

    def test_consume_records_histogram(self) -> None:
        """Consume records processing duration in histogram."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()
        call_next = MagicMock(return_value="ok")

        mw.consume_scope(call_next, msg)

        assert len(collector.histogram_calls) == 1
        hc = collector.histogram_calls[0]
        assert hc.name == MESSAGE_PROCESSING_SECONDS
        assert hc.labels == {"queue": "test.queue"}
        assert hc.value >= 0.0  # duration is non-negative

    def test_consume_error_records_histogram(self) -> None:
        """Consume records histogram even on error."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()
        call_next = MagicMock(side_effect=RuntimeError("fail"))

        with pytest.raises(RuntimeError, match="fail"):
            mw.consume_scope(call_next, msg)

        assert len(collector.histogram_calls) == 1
        hc = collector.histogram_calls[0]
        assert hc.name == MESSAGE_PROCESSING_SECONDS
        assert hc.value >= 0.0

    def test_consume_falls_back_to_routing_key_without_original_queue_header(self) -> None:
        """M3: without x-rabbitkit-original-queue (e.g. a message built
        directly rather than delivered through a broker), falls back to
        routing_key -- the pre-M3 behavior, only as a fallback now."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message(routing_key="orders.created")
        call_next = MagicMock(return_value="ok")

        mw.consume_scope(call_next, msg)

        cc = collector.counter_calls[0]
        assert cc.labels["queue"] == "orders.created"

    def test_consume_empty_routing_key_defaults_to_unknown(self) -> None:
        """Empty routing_key falls back to 'unknown'."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message(routing_key="")
        call_next = MagicMock(return_value="ok")

        mw.consume_scope(call_next, msg)

        cc = collector.counter_calls[0]
        assert cc.labels["queue"] == "unknown"

    def test_consume_prefers_bound_queue_name_over_routing_key(self) -> None:
        """M3: with x-rabbitkit-original-queue set (as the broker's
        on_message wrapper always sets it before middlewares run), the
        BOUND queue name is used, not the routing key -- prevents
        cardinality explosion from a topic/Path() routing key that embeds
        an unbounded value (tenant id, order id, etc.)."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message(
            routing_key="orders.tenant-42.created",
            headers={"x-rabbitkit-original-queue": "orders-processor"},
        )
        call_next = MagicMock(return_value="ok")

        mw.consume_scope(call_next, msg)

        cc = collector.counter_calls[0]
        assert cc.labels["queue"] == "orders-processor"

    @pytest.mark.asyncio
    async def test_consume_async_prefers_bound_queue_name_over_routing_key(self) -> None:
        """M3, async variant."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message(
            routing_key="orders.tenant-42.created",
            headers={"x-rabbitkit-original-queue": "orders-processor"},
        )

        async def call_next(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(call_next, msg)

        cc = collector.counter_calls[0]
        assert cc.labels["queue"] == "orders-processor"


# ── record_settlement (M2) ───────────────────────────────────────────────


class TestRecordSettlement:
    def test_acked_emits_messages_acked_total(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()

        mw.record_settlement(msg, "acked")

        assert len(collector.counter_calls) == 1
        cc = collector.counter_calls[0]
        assert cc.name == MetricsConfig().messages_acked_total
        assert cc.labels == {"queue": "test.queue"}

    def test_nacked_emits_messages_nacked_total(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()

        mw.record_settlement(msg, "nacked")

        cc = collector.counter_calls[0]
        assert cc.name == MetricsConfig().messages_nacked_total

    def test_rejected_emits_messages_rejected_total(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()

        mw.record_settlement(msg, "rejected")

        cc = collector.counter_calls[0]
        assert cc.name == MetricsConfig().messages_rejected_total

    def test_no_collector_is_noop(self) -> None:
        mw = MetricsMiddleware(None)
        msg = _make_message()

        mw.record_settlement(msg, "acked")  # must not raise

    def test_uses_bound_queue_name_label(self) -> None:
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message(
            routing_key="orders.tenant-42.created",
            headers={"x-rabbitkit-original-queue": "orders-processor"},
        )

        mw.record_settlement(msg, "acked")

        assert collector.counter_calls[0].labels["queue"] == "orders-processor"

    def test_collector_and_config_properties(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        collector = FakeCollector()
        cfg = MetricsConfig(namespace="custom")
        mw = MetricsMiddleware(collector, cfg)

        assert mw.collector is collector
        assert mw.config is cfg


# ── Consume scope (async) ────────────────────────────────────────────────


class TestConsumeScopeAsync:
    async def test_consume_async_success(self) -> None:
        """Async successful consume increments counter with status=success."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "async-ok"

        result = await mw.consume_scope_async(call_next, msg)

        assert result == "async-ok"
        assert len(collector.counter_calls) == 1
        cc = collector.counter_calls[0]
        assert cc.name == MESSAGES_CONSUMED_TOTAL
        assert cc.labels == {"queue": "test.queue", "status": "success"}

    async def test_consume_async_error(self) -> None:
        """Async failed consume increments counter with status=error and re-raises."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            raise ValueError("async-boom")

        with pytest.raises(ValueError, match="async-boom"):
            await mw.consume_scope_async(call_next, msg)

        assert len(collector.counter_calls) == 1
        cc = collector.counter_calls[0]
        assert cc.labels["status"] == "error"

    async def test_consume_async_records_histogram(self) -> None:
        """Async consume records processing duration in histogram."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(call_next, msg)

        assert len(collector.histogram_calls) == 1
        hc = collector.histogram_calls[0]
        assert hc.name == MESSAGE_PROCESSING_SECONDS
        assert hc.value >= 0.0


# ── Publish scope (sync) ─────────────────────────────────────────────────


class TestPublishScope:
    def test_publish_success(self) -> None:
        """Successful publish increments counter with status=success."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        envelope = _make_envelope()
        call_next = MagicMock(return_value="published")

        result = mw.publish_scope(call_next, envelope)

        assert result == "published"
        call_next.assert_called_once_with(envelope)

        assert len(collector.counter_calls) == 1
        cc = collector.counter_calls[0]
        assert cc.name == MESSAGES_PUBLISHED_TOTAL
        assert cc.labels == {"exchange": "test.exchange", "status": "success"}

    def test_publish_error(self) -> None:
        """Failed publish increments counter with status=error and re-raises."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        envelope = _make_envelope()
        call_next = MagicMock(side_effect=ConnectionError("broker down"))

        with pytest.raises(ConnectionError, match="broker down"):
            mw.publish_scope(call_next, envelope)

        assert len(collector.counter_calls) == 1
        cc = collector.counter_calls[0]
        assert cc.labels == {"exchange": "test.exchange", "status": "error"}

    def test_publish_records_histogram(self) -> None:
        """Publish records duration in histogram."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        envelope = _make_envelope()
        call_next = MagicMock(return_value="published")

        mw.publish_scope(call_next, envelope)

        assert len(collector.histogram_calls) == 1
        hc = collector.histogram_calls[0]
        assert hc.name == MESSAGE_PUBLISH_SECONDS
        assert hc.labels == {"exchange": "test.exchange"}
        assert hc.value >= 0.0

    def test_publish_uses_exchange_label(self) -> None:
        """Exchange label comes from envelope.exchange."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        envelope = _make_envelope(exchange="events.fanout")
        call_next = MagicMock(return_value="ok")

        mw.publish_scope(call_next, envelope)

        cc = collector.counter_calls[0]
        assert cc.labels["exchange"] == "events.fanout"

    def test_publish_empty_exchange_defaults(self) -> None:
        """Empty exchange falls back to 'default'."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        envelope = _make_envelope(exchange="")
        call_next = MagicMock(return_value="ok")

        mw.publish_scope(call_next, envelope)

        cc = collector.counter_calls[0]
        assert cc.labels["exchange"] == "default"


# ── Publish scope (async) ────────────────────────────────────────────────


class TestPublishScopeAsync:
    async def test_publish_async_success(self) -> None:
        """Async successful publish increments counter with status=success."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        envelope = _make_envelope()

        async def call_next(e: MessageEnvelope) -> str:
            return "async-published"

        result = await mw.publish_scope_async(call_next, envelope)

        assert result == "async-published"
        assert len(collector.counter_calls) == 1
        cc = collector.counter_calls[0]
        assert cc.name == MESSAGES_PUBLISHED_TOTAL
        assert cc.labels == {"exchange": "test.exchange", "status": "success"}

    async def test_publish_async_error(self) -> None:
        """Async failed publish increments counter with status=error and re-raises."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        envelope = _make_envelope()

        async def call_next(e: MessageEnvelope) -> str:
            raise ConnectionError("async-broker-down")

        with pytest.raises(ConnectionError, match="async-broker-down"):
            await mw.publish_scope_async(call_next, envelope)

        assert len(collector.counter_calls) == 1
        cc = collector.counter_calls[0]
        assert cc.labels["status"] == "error"

    async def test_publish_async_records_histogram(self) -> None:
        """Async publish records duration in histogram."""
        collector = FakeCollector()
        mw = MetricsMiddleware(collector)
        envelope = _make_envelope()

        async def call_next(e: MessageEnvelope) -> str:
            return "ok"

        await mw.publish_scope_async(call_next, envelope)

        assert len(collector.histogram_calls) == 1
        hc = collector.histogram_calls[0]
        assert hc.name == MESSAGE_PUBLISH_SECONDS
        assert hc.value >= 0.0


# ── No-op mode ───────────────────────────────────────────────────────────


class TestNoopWithoutCollector:
    def test_noop_consume_without_collector(self) -> None:
        """No collector -> consume passthrough, no metrics recorded."""
        mw = MetricsMiddleware()
        msg = _make_message()
        call_next = MagicMock(return_value="pass")

        result = mw.consume_scope(call_next, msg)

        assert result == "pass"
        call_next.assert_called_once_with(msg)

    async def test_noop_consume_async_without_collector(self) -> None:
        """No collector -> async consume passthrough."""
        mw = MetricsMiddleware()
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "async-pass"

        result = await mw.consume_scope_async(call_next, msg)
        assert result == "async-pass"

    def test_noop_publish_without_collector(self) -> None:
        """No collector -> publish passthrough, no metrics recorded."""
        mw = MetricsMiddleware()
        envelope = _make_envelope()
        call_next = MagicMock(return_value="pass")

        result = mw.publish_scope(call_next, envelope)

        assert result == "pass"
        call_next.assert_called_once_with(envelope)

    async def test_noop_publish_async_without_collector(self) -> None:
        """No collector -> async publish passthrough."""
        mw = MetricsMiddleware()
        envelope = _make_envelope()

        async def call_next(e: MessageEnvelope) -> str:
            return "async-pass"

        result = await mw.publish_scope_async(call_next, envelope)
        assert result == "async-pass"


# ── MetricsConfig ────────────────────────────────────────────────────────


class TestMetricsConfig:
    def test_default_namespace_names(self) -> None:
        """Default MetricsConfig uses 'rabbitkit' namespace."""
        from rabbitkit.core.config import MetricsConfig

        cfg = MetricsConfig()
        assert cfg.consumed_total == "rabbitkit_messages_consumed_total"
        assert cfg.processing_seconds == "rabbitkit_message_processing_seconds"
        assert cfg.published_total == "rabbitkit_messages_published_total"
        assert cfg.publish_seconds == "rabbitkit_message_publish_seconds"

    def test_custom_namespace(self) -> None:
        """MetricsConfig(namespace='myapp') prefixes all metric names."""
        from rabbitkit.core.config import MetricsConfig

        cfg = MetricsConfig(namespace="myapp")
        assert cfg.consumed_total == "myapp_messages_consumed_total"
        assert cfg.processing_seconds == "myapp_message_processing_seconds"
        assert cfg.published_total == "myapp_messages_published_total"
        assert cfg.publish_seconds == "myapp_message_publish_seconds"

    def test_full_name_override(self) -> None:
        """Individual name overrides take precedence over namespace."""
        from rabbitkit.core.config import MetricsConfig

        cfg = MetricsConfig(namespace="ns", consumed_counter="custom_consumed")
        assert cfg.consumed_total == "custom_consumed"
        assert cfg.processing_seconds == "ns_message_processing_seconds"

    def test_middleware_uses_custom_config(self) -> None:
        """MetricsMiddleware records metrics under the custom names."""
        from unittest.mock import MagicMock

        from rabbitkit.core.config import MetricsConfig
        from rabbitkit.core.message import RabbitMessage
        from rabbitkit.middleware.metrics import MetricsMiddleware

        cfg = MetricsConfig(namespace="eng")
        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector, config=cfg)

        msg = MagicMock(spec=RabbitMessage)
        msg.routing_key = "orders"
        mw.consume_scope(lambda m: None, msg)

        recorded_names = [call.args[0] for call in collector.inc_counter.call_args_list]
        assert "eng_messages_consumed_total" in recorded_names

    def test_backward_compat_module_constants(self) -> None:
        """Module-level constants still equal the default config names."""
        from rabbitkit.middleware.metrics import (
            MESSAGE_PROCESSING_SECONDS,
            MESSAGE_PUBLISH_SECONDS,
            MESSAGES_CONSUMED_TOTAL,
            MESSAGES_PUBLISHED_TOTAL,
        )

        assert MESSAGES_CONSUMED_TOTAL == "rabbitkit_messages_consumed_total"
        assert MESSAGE_PROCESSING_SECONDS == "rabbitkit_message_processing_seconds"
        assert MESSAGES_PUBLISHED_TOTAL == "rabbitkit_messages_published_total"
        assert MESSAGE_PUBLISH_SECONDS == "rabbitkit_message_publish_seconds"


# ── PrometheusCollector ──────────────────────────────────────────────────


class TestPrometheusCollector:
    def test_init_succeeds_when_prometheus_available(self) -> None:
        """PrometheusCollector initialises when prometheus_client is installed."""
        from rabbitkit.middleware.metrics import PrometheusCollector

        collector = PrometheusCollector()
        assert collector._prometheus_client is not None
        assert isinstance(collector._counters, dict)
        assert isinstance(collector._histograms, dict)

    def test_inc_counter_creates_and_increments(self) -> None:
        """inc_counter creates a Counter and calls .inc() without raising."""
        from unittest.mock import MagicMock

        from rabbitkit.middleware.metrics import PrometheusCollector

        collector = PrometheusCollector()
        mock_pc = MagicMock()
        collector._prometheus_client = mock_pc

        collector.inc_counter("my_requests_total", {"queue": "orders"}, 1.0)

        mock_pc.Counter.assert_called_once()
        mock_pc.Counter.return_value.labels.assert_called_once_with(queue="orders")
        mock_pc.Counter.return_value.labels.return_value.inc.assert_called_once_with(1.0)

    def test_inc_counter_caches_metric(self) -> None:
        """Calling inc_counter twice with the same name reuses the cached Counter."""
        from unittest.mock import MagicMock

        from rabbitkit.middleware.metrics import PrometheusCollector

        collector = PrometheusCollector()
        mock_pc = MagicMock()
        collector._prometheus_client = mock_pc

        collector.inc_counter("cached_counter", {"queue": "a"}, 1.0)
        collector.inc_counter("cached_counter", {"queue": "b"}, 2.0)

        # Counter constructor called only once despite two inc_counter calls
        assert mock_pc.Counter.call_count == 1

    def test_observe_histogram_creates_and_observes(self) -> None:
        """observe_histogram creates a Histogram and calls .observe() without raising."""
        from unittest.mock import MagicMock

        from rabbitkit.middleware.metrics import PrometheusCollector

        collector = PrometheusCollector()
        mock_pc = MagicMock()
        collector._prometheus_client = mock_pc

        collector.observe_histogram("my_latency_seconds", {"queue": "orders"}, 0.123)

        mock_pc.Histogram.assert_called_once()
        mock_pc.Histogram.return_value.labels.assert_called_once_with(queue="orders")
        mock_pc.Histogram.return_value.labels.return_value.observe.assert_called_once_with(0.123)

    def test_observe_histogram_caches_metric(self) -> None:
        """Calling observe_histogram twice with the same name reuses the cached Histogram."""
        from unittest.mock import MagicMock

        from rabbitkit.middleware.metrics import PrometheusCollector

        collector = PrometheusCollector()
        mock_pc = MagicMock()
        collector._prometheus_client = mock_pc

        collector.observe_histogram("cached_histogram", {"queue": "x"}, 0.1)
        collector.observe_histogram("cached_histogram", {"queue": "y"}, 0.2)

        assert mock_pc.Histogram.call_count == 1

    def test_init_fails_without_prometheus(self) -> None:
        """PrometheusCollector raises ImportError when prometheus_client is missing."""
        import builtins
        from unittest.mock import patch

        from rabbitkit.middleware.metrics import PrometheusCollector

        real_import = builtins.__import__

        def mock_import(name: str, *args: object, **kwargs: object) -> object:
            if name == "prometheus_client":
                raise ImportError("No module named 'prometheus_client'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="PrometheusCollector requires"):
                PrometheusCollector()


# ── metrics_app exposition (M-SRE1) ───────────────────────────────────────


class TestMetricsApp:
    def test_metrics_app_exposes_prometheus_text(self) -> None:
        """metrics_app() returns an ASGI app serving /metrics in Prometheus format."""
        pytest.importorskip("prometheus_client")
        from rabbitkit.middleware.metrics import metrics_app

        app = metrics_app()

        captured: dict[str, object] = {}

        async def receive():  # pragma: no cover - not used for http.response
            return {"type": "http.request"}

        async def send(message):
            if message["type"] == "http.response.start":
                captured["status"] = message["status"]
                captured["headers"] = message["headers"]
            elif message["type"] == "http.response.body":
                captured["body"] = message["body"]

        import asyncio

        scope = {"type": "http", "path": "/metrics"}
        asyncio.run(app(scope, receive, send))

        assert captured["status"] == 200
        body = captured["body"]
        assert isinstance(body, (bytes, bytearray))
        # Prometheus text format always starts with a '# HELP' or '# TYPE' or a metric line
        text = body.decode()
        assert "=" in text or text.startswith("#")

    def test_metrics_app_404_for_non_metrics_path(self) -> None:
        pytest.importorskip("prometheus_client")
        from rabbitkit.middleware.metrics import metrics_app

        app = metrics_app()
        captured: dict[str, object] = {}

        async def receive():  # pragma: no cover
            return {"type": "http.request"}

        async def send(message):
            if message["type"] == "http.response.start":
                captured["status"] = message["status"]

        import asyncio

        scope = {"type": "http", "path": "/other"}
        asyncio.run(app(scope, receive, send))
        assert captured["status"] == 404


class TestStartMetricsServerDefaultHost:
    def test_default_host_is_loopback(self) -> None:
        """M-2: start_metrics_server binds 127.0.0.1 by default (not 0.0.0.0)."""
        import inspect

        from rabbitkit.middleware.metrics import start_metrics_server

        sig = inspect.signature(start_metrics_server)
        host_default = sig.parameters["host"].default
        assert host_default == "127.0.0.1"

    def test_explicit_host_is_used(self) -> None:
        """An explicit host=0.0.0.0 is forwarded to prometheus_client."""
        pytest.importorskip("prometheus_client")
        from unittest.mock import patch

        from rabbitkit.middleware.metrics import start_metrics_server

        with patch("prometheus_client.start_http_server") as mock_start:
            start_metrics_server(port=9090, host="0.0.0.0")  # noqa: S104  # explicit all-ifaces test
        mock_start.assert_called_once_with(9090, "0.0.0.0")  # noqa: S104
