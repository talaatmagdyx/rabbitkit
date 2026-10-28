"""AsyncBroker metrics wiring: BrokerMetrics on every transport hook,
publish-only brokers included, sampled gauges, consumer cancels, stop."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.async_.reconnect import ReconnectAttempt
from rabbitkit.core.config import MetricsConfig
from rabbitkit.core.errors import AuthenticationFailedError
from rabbitkit.core.types import PublishOutcome, PublishStatus
from rabbitkit.middleware.metrics import BrokerMetrics, MetricsMiddleware
from tests.unit.middleware.test_metrics_inventory import RecordingCollector

_CFG = MetricsConfig()


def _transport() -> MagicMock:
    transport = MagicMock()
    transport.connection_states.return_value = {"publisher": True, "consumer": True}
    transport.channels_active = 4
    transport.publishes_in_flight = 2
    transport.publish_channels_in_use = 1
    transport.is_blocked = False
    transport.blocked_reason = None
    transport.byte_limiter = None
    return transport


def _hook(transport: MagicMock, name: str) -> Any:
    getattr(transport, name).assert_called_once()
    return getattr(transport, name).call_args.args[0]


def _publish_only_broker(collector: RecordingCollector) -> AsyncBroker:
    broker = AsyncBroker(middlewares=[MetricsMiddleware(collector)])
    broker._transport = _transport()
    broker._wire_reconnect_metric()
    return broker


class TestWiring:
    def test_a_publish_only_broker_gets_connection_metrics(self) -> None:
        collector = RecordingCollector()
        broker = _publish_only_broker(collector)
        assert isinstance(broker._broker_metrics, BrokerMetrics)
        transport = broker._transport
        assert isinstance(transport, MagicMock)
        _hook(transport, "on_connection_state")("publisher", False, ConnectionResetError())
        assert collector.gauge(_CFG.connection_up, role="publisher") == 0.0
        assert collector.count(_CFG.connection_failures_total, role="publisher", reason="connection_reset") == 1

    def test_every_transport_hook_is_wired(self) -> None:
        collector = RecordingCollector()
        broker = _publish_only_broker(collector)
        transport = broker._transport
        assert isinstance(transport, MagicMock)
        _hook(transport, "on_reconnect")()
        _hook(transport, "on_channel_opened")()
        _hook(transport, "on_channel_rebuilt")()
        attempt = ReconnectAttempt(
            role="consumer", attempt=1, delay=0.5, elapsed=1.0, endpoint="h:5672", error=TimeoutError()
        )
        _hook(transport, "on_reconnect_attempt")(attempt)
        failed = transport.on_connection_failed.call_args_list[-1].args[0]
        failed(AuthenticationFailedError("no", reason="authentication_failed", role="publisher"))
        transport.blocked_reason = "low on disk"
        _hook(transport, "on_blocked")()
        assert collector.gauge(_CFG.connection_blocked) == 1.0
        assert collector.gauge(_CFG.connection_blocked_reason, reason="disk") == 1.0
        assert collector.gauge(_CFG.connection_blocked_reason, reason="memory") == 0.0
        _hook(transport, "on_unblocked")()
        assert collector.gauge(_CFG.connection_blocked) == 0.0
        _hook(transport, "on_publish_outcome")(PublishOutcome(status=PublishStatus.RETURNED, exchange="x"), 0.01)
        assert collector.count(_CFG.reconnects_total) == 1
        assert collector.count(_CFG.channels_opened_total) == 1
        assert collector.count(_CFG.channel_rebuilds_total) == 1
        assert collector.count(_CFG.reconnect_attempts_total, role="consumer") == 1
        assert collector.count(_CFG.connection_failures_total, role="consumer", reason="timeout") == 1
        assert collector.count(_CFG.connection_failures_total, reason="authentication_failed") == 1
        assert collector.count(_CFG.publish_routing_failures_total, exchange="x", reason="returned") == 1

    def test_the_middleware_hands_confirm_latency_to_the_transport(self) -> None:
        mw = MetricsMiddleware(RecordingCollector())
        broker = AsyncBroker(middlewares=[mw])
        broker._transport = _transport()
        broker._wire_reconnect_metric()
        assert mw._confirm_latency_from_transport is True

    def test_no_metrics_without_a_collector(self) -> None:
        broker = AsyncBroker(middlewares=[MetricsMiddleware(None)])
        broker._transport = _transport()
        broker._wire_reconnect_metric()
        assert broker._broker_metrics is None
        broker._sample_metrics()  # no-op


class TestSampling:
    def test_samples_every_gauge(self) -> None:
        collector = RecordingCollector()
        broker = AsyncBroker(middlewares=[MetricsMiddleware(collector)])

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        transport = _transport()
        limiter = MagicMock()
        limiter.used = 512
        transport.byte_limiter = limiter
        broker._transport = transport
        broker._wire_reconnect_metric()
        route = broker._registry.routes[0]
        route.runtime_state.consumer_tag = "ctag"
        broker._worker_pool = MagicMock(pending_count=3)

        broker._set_lifecycle_gauges(connected=True)

        assert collector.gauge(_CFG.broker_connected) == 1.0
        assert collector.gauge(_CFG.connections_active) == 2.0
        assert collector.gauge(_CFG.channels_active) == 4.0
        assert collector.gauge(_CFG.publish_in_flight) == 2.0
        assert collector.gauge(_CFG.publish_in_flight_bytes) == 512.0
        assert collector.gauge(_CFG.publish_channels_in_use) == 1.0
        assert collector.gauge(_CFG.connection_blocked) == 0.0
        assert collector.gauge(_CFG.route_consuming, queue="orders") == 1.0
        assert collector.gauge(_CFG.worker_pool_pending) == 3.0

        route.runtime_state.consumer_cancelled = True
        broker._sample_metrics()
        assert collector.gauge(_CFG.route_consuming, queue="orders") == 0.0

    def test_stop_zeroes_the_gauges(self) -> None:
        collector = RecordingCollector()
        broker = _publish_only_broker(collector)
        broker._set_lifecycle_gauges(connected=True)
        broker._set_lifecycle_gauges(connected=False)
        assert collector.gauge(_CFG.broker_connected) == 0.0
        assert collector.gauge(_CFG.connection_up, role="publisher") == 0.0
        assert collector.gauge(_CFG.channels_active) == 0.0

    def test_a_failing_sample_is_logged_not_raised(self) -> None:
        collector = RecordingCollector()
        broker = _publish_only_broker(collector)
        transport = broker._transport
        assert isinstance(transport, MagicMock)
        transport.connection_states.side_effect = RuntimeError("torn down")
        with patch("rabbitkit.async_.broker.logger") as log:
            broker._sample_metrics()
        assert log.warning.call_args.args[0] == "Sampling broker metrics failed"

    async def test_the_heartbeat_loop_samples(self, monkeypatch: Any) -> None:
        import asyncio

        collector = RecordingCollector()
        broker = _publish_only_broker(collector)
        monkeypatch.setattr(AsyncBroker, "_HEARTBEAT_INTERVAL", 0.0)
        task = asyncio.ensure_future(broker._heartbeat_loop())
        for _ in range(5):
            await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert collector.gauge(_CFG.channels_active) == 4.0


class TestConsumerCancels:
    def test_broker_cancelled_consumer_is_counted(self) -> None:
        collector = RecordingCollector()
        broker = _publish_only_broker(collector)
        broker._consumer_state_changed("orders", cancelled=True)
        broker._consumer_state_changed("orders", cancelled=False)
        assert collector.count(_CFG.consumer_cancelled_total, queue="orders") == 1
        assert collector.gauge(_CFG.route_consuming, queue="orders") == 1.0

    def test_without_metrics_only_the_route_state_changes(self) -> None:
        broker = AsyncBroker()
        broker._consumer_state_changed("orders", cancelled=True)  # must not raise


def test_lifecycle_gauges_before_the_metrics_are_wired() -> None:
    """A start() that fails before wiring still sets the lifecycle gauges."""
    collector = RecordingCollector()
    broker = AsyncBroker(middlewares=[MetricsMiddleware(collector)])
    broker._set_lifecycle_gauges(connected=False)
    assert collector.gauge(_CFG.broker_connected) == 0.0
    assert collector.gauge(_CFG.connection_up, role="publisher") is None
