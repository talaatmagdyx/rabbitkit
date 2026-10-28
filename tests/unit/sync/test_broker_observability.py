"""SyncBroker metrics wiring: BrokerMetrics on the transport hooks
(one "shared" connection), publish-only brokers included, sampled gauges."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from rabbitkit.core.config import MetricsConfig
from rabbitkit.core.types import PublishOutcome, PublishStatus
from rabbitkit.middleware.metrics import MetricsMiddleware
from rabbitkit.sync.broker import SyncBroker
from tests.unit.middleware.test_metrics_inventory import RecordingCollector

_CFG = MetricsConfig()


def _transport() -> MagicMock:
    transport = MagicMock()
    transport.is_connected.return_value = True
    transport.channels_active = 3
    transport.is_blocked = False
    transport.blocked_reason = None
    return transport


def _hook(transport: MagicMock, name: str) -> Any:
    getattr(transport, name).assert_called_once()
    return getattr(transport, name).call_args.args[0]


def _broker(collector: RecordingCollector) -> SyncBroker:
    broker = SyncBroker(middlewares=[MetricsMiddleware(collector)])
    broker._transport = _transport()
    broker._wire_reconnect_metric()
    return broker


class TestWiring:
    def test_a_publish_only_broker_gets_connection_metrics(self) -> None:
        collector = RecordingCollector()
        broker = _broker(collector)
        transport = broker._transport
        assert isinstance(transport, MagicMock)
        _hook(transport, "on_connection_state")("shared", True, None)
        assert collector.gauge(_CFG.broker_connected) == 1.0
        _hook(transport, "on_connection_state")("shared", False, ConnectionResetError())
        assert collector.gauge(_CFG.broker_connected) == 0.0
        assert collector.count(_CFG.connection_failures_total, role="shared", reason="connection_reset") == 1

    def test_every_transport_hook_is_wired(self) -> None:
        collector = RecordingCollector()
        broker = _broker(collector)
        transport = broker._transport
        assert isinstance(transport, MagicMock)
        _hook(transport, "on_reconnect")()
        _hook(transport, "on_channel_opened")()
        _hook(transport, "on_channel_rebuilt")()
        _hook(transport, "on_reconnect_attempt")(2, ConnectionRefusedError())
        transport.blocked_reason = "low on disk"
        _hook(transport, "on_blocked")()
        assert collector.gauge(_CFG.connection_blocked) == 1.0
        assert collector.gauge(_CFG.connection_blocked_reason, reason="disk") == 1.0
        assert collector.gauge(_CFG.connection_blocked_reason, reason="memory") == 0.0
        _hook(transport, "on_unblocked")()
        _hook(transport, "on_publish_outcome")(PublishOutcome(status=PublishStatus.CONFIRMED, exchange="x"), 0.002)
        assert collector.count(_CFG.reconnects_total) == 1
        assert collector.count(_CFG.reconnect_attempts_total, role="shared") == 1
        assert collector.count(_CFG.connection_failures_total, role="shared", reason="connection_refused") == 1
        assert collector.histograms[-1] == (_CFG.publish_confirm_latency_seconds, {"exchange": "x"}, 0.002)

    def test_no_metrics_without_a_collector(self) -> None:
        broker = SyncBroker()
        broker._transport = _transport()
        broker._wire_reconnect_metric()
        assert broker._broker_metrics is None
        broker._sample_metrics_throttled()  # no-op
        broker._sample_metrics()  # no-op


class TestSampling:
    def test_lifecycle_samples_then_zeroes(self) -> None:
        collector = RecordingCollector()
        broker = SyncBroker(middlewares=[MetricsMiddleware(collector)])

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker._transport = _transport()
        broker._wire_reconnect_metric()
        broker._registry.routes[0].runtime_state.consumer_tag = "ctag"
        broker._worker_pool = MagicMock(pending_count=1)
        broker._set_lifecycle_gauges(connected=True)
        assert collector.gauge(_CFG.connection_up, role="shared") == 1.0
        assert collector.gauge(_CFG.channels_active) == 3.0
        assert collector.gauge(_CFG.route_consuming, queue="orders") == 1.0
        assert collector.gauge(_CFG.worker_pool_pending) == 1.0
        broker._set_lifecycle_gauges(connected=False)
        assert collector.gauge(_CFG.connection_up, role="shared") == 0.0
        assert collector.gauge(_CFG.route_consuming, queue="orders") == 0.0

    def test_io_tick_sampling_is_throttled(self) -> None:
        collector = RecordingCollector()
        broker = _broker(collector)
        transport = broker._transport
        assert isinstance(transport, MagicMock)
        with patch("rabbitkit.sync.broker.time.monotonic", side_effect=[100.0, 101.0, 106.0]):
            broker._sample_metrics_throttled()
            broker._sample_metrics_throttled()
            broker._sample_metrics_throttled()
        assert transport.is_connected.call_count == 2

    def test_a_failing_sample_is_logged_not_raised(self) -> None:
        broker = _broker(RecordingCollector())
        transport = broker._transport
        assert isinstance(transport, MagicMock)
        transport.is_connected.side_effect = RuntimeError("torn down")
        with patch("rabbitkit.sync.broker.logger") as log:
            broker._sample_metrics()
        assert log.warning.call_args.args[0] == "Sampling broker metrics failed"


class TestRecovery:
    def test_the_loss_cause_reaches_the_transport(self) -> None:
        broker = SyncBroker()
        transport = MagicMock()
        broker._transport = transport
        cause = ConnectionResetError("reset")
        broker._recover_consumers(cause)
        transport.reconnect.assert_called_once_with(cause)


def test_lifecycle_gauges_before_the_metrics_are_wired() -> None:
    collector = RecordingCollector()
    broker = SyncBroker(middlewares=[MetricsMiddleware(collector)])
    broker._set_lifecycle_gauges(connected=False)
    assert collector.gauge(_CFG.broker_connected) == 0.0
    assert collector.gauge(_CFG.connection_up, role="shared") is None
