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


class TestRetryHandoffPaused:
    """``retry_handoff_paused`` follows the route's handoff failure streak
    (0.20.4): the I/O-tick sample clears an idle route whose streak ended,
    and stop() sets it to 0."""

    @staticmethod
    def _route(broker: SyncBroker, collector: RecordingCollector) -> tuple[Any, list[float]]:
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.core.retry_handoff import RetryHandoffTracker
        from rabbitkit.middleware.retry import RetryMiddleware

        now = [0.0]
        retry = RetryConfig(max_retries=1, delays=(1,))
        mw = RetryMiddleware(
            retry,
            publish_fn=MagicMock(return_value=PublishOutcome(status=PublishStatus.NACKED)),
            metrics_collector=collector,
            metrics_config=_CFG,
            handoff_tracker=RetryHandoffTracker(retry.handoff, clock=lambda: now[0]),
        )

        @broker.subscriber(queue="orders", retry=retry, middlewares=[mw, MetricsMiddleware(collector)])
        def handle(body: bytes) -> None:
            pass

        return mw, now

    @staticmethod
    def _fail_a_handoff(mw: Any) -> None:
        from rabbitkit.core.message import RabbitMessage

        def fail(message: RabbitMessage) -> None:
            raise TimeoutError("redis down")

        message = RabbitMessage(body=b"{}", routing_key="orders", headers={"x-rabbitkit-original-queue": "orders"})
        message._nack_fn = MagicMock()
        mw.consume_scope(fail, message)

    def test_the_io_tick_sample_clears_an_idle_route_after_the_quiet_period(self) -> None:
        collector = RecordingCollector()
        broker = _broker(collector)
        mw, now = self._route(broker, collector)
        self._fail_a_handoff(mw)
        now[0] += 330.0
        with patch("rabbitkit.sync.broker.time.monotonic", side_effect=[100.0, 101.0]):
            broker._sample_metrics_throttled()
            assert collector.gauge(_CFG.retry_handoff_paused, queue="orders") == 0.0
            self._fail_a_handoff(mw)
            broker._sample_metrics_throttled()  # throttled, and the route is failing again anyway
        assert collector.gauge(_CFG.retry_handoff_paused, queue="orders") == 1.0

    def test_the_io_tick_clears_an_idle_route_without_a_metrics_middleware(self) -> None:
        """A RetryMiddleware with its own collector and no MetricsMiddleware anywhere: the broker
        has no broker metrics, yet the I/O tick still clears the idle route (async parity)."""
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.core.retry_handoff import RetryHandoffTracker
        from rabbitkit.middleware.retry import RetryMiddleware

        collector = RecordingCollector()
        broker = SyncBroker()
        broker._transport = _transport()
        assert broker._broker_metrics is None
        now = [0.0]
        retry = RetryConfig(max_retries=1, delays=(1,))
        mw = RetryMiddleware(
            retry,
            publish_fn=MagicMock(return_value=PublishOutcome(status=PublishStatus.NACKED)),
            metrics_collector=collector,
            metrics_config=_CFG,
            handoff_tracker=RetryHandoffTracker(retry.handoff, clock=lambda: now[0]),
        )

        @broker.subscriber(queue="orders", retry=retry, middlewares=[mw])
        def handle(body: bytes) -> None:
            pass

        self._fail_a_handoff(mw)
        assert collector.gauge(_CFG.retry_handoff_paused, queue="orders") == 1.0
        now[0] += 330.0
        with patch("rabbitkit.sync.broker.time.monotonic", return_value=100.0):
            broker._sample_metrics_throttled()
        assert collector.gauge(_CFG.retry_handoff_paused, queue="orders") == 0.0

    def test_the_io_tick_refresh_without_broker_metrics_is_throttled(self) -> None:
        broker = SyncBroker()
        broker._transport = _transport()
        with (
            patch("rabbitkit.sync.broker.refresh_retry_handoff_paused") as refresh,
            patch("rabbitkit.sync.broker.time.monotonic", side_effect=[100.0, 101.0, 106.0]),
        ):
            broker._sample_metrics_throttled()
            broker._sample_metrics_throttled()
            broker._sample_metrics_throttled()
        assert refresh.call_count == 2  # 100 and 106; 101 is inside the 5 s interval

    def test_stop_sets_the_gauge_to_0(self) -> None:
        collector = RecordingCollector()
        broker = SyncBroker(middlewares=[MetricsMiddleware(collector)])
        mw, _ = self._route(broker, collector)
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as connection:
                channel = MagicMock()
                channel.is_open = True
                connection.return_value.channel.return_value = channel
                connection.return_value.is_open = True
                broker.start()
        self._fail_a_handoff(mw)
        assert collector.gauge(_CFG.retry_handoff_paused, queue="orders") == 1.0
        broker.stop()
        assert collector.gauge(_CFG.retry_handoff_paused, queue="orders") == 0.0
