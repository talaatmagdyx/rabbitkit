"""SyncBroker parity with AsyncBroker (wave 4): ``processing_timeout``,
broker-initiated consumer cancels (G-26) and the sampled publish gauge."""

from __future__ import annotations

import threading
import time
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.core.config import (
    ConsumerConfig,
    MetricsConfig,
    PoolConfig,
    PublisherConfig,
    RabbitConfig,
    WorkerConfig,
)
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import AckPolicy, MessageEnvelope, PublishReason, PublishStatus
from rabbitkit.middleware.metrics import MetricsMiddleware
from rabbitkit.sync.broker import SyncBroker
from rabbitkit.sync.deadlines import LateSettlementError
from rabbitkit.sync.pool import InFlightByteLimiter
from tests.unit.middleware.test_metrics_foundation import RecordingCollector

_CFG = MetricsConfig()


def _transport() -> MagicMock:
    transport = MagicMock()
    transport.consume.return_value = "ctag"
    transport.is_connected.return_value = True
    transport.channels_active = 2
    transport.publishes_in_flight = 3
    transport.byte_limiter = None
    transport.is_blocked = False
    transport.poll_interval = 1.0
    return transport


@contextmanager
def _started(broker: SyncBroker, worker_config: WorkerConfig | None = None) -> Iterator[MagicMock]:
    transport = _transport()
    with patch("rabbitkit.sync.broker.SyncTransport", return_value=transport):
        broker.start(worker_config=worker_config)
    try:
        yield transport
    finally:
        broker.stop(timeout=2)


def _delivery(*, redelivered: bool = False) -> tuple[RabbitMessage, list[tuple[str, Any]]]:
    sent: list[tuple[str, Any]] = []
    lock = threading.Lock()

    def record(kind: str, arg: Any = None) -> None:
        with lock:
            sent.append((kind, arg))

    msg = RabbitMessage(body=b"{}", message_id="m-1", delivery_tag=1, redelivered=redelivered)
    msg._ack_fn = lambda: record("ack")
    msg._nack_fn = lambda requeue=True: record("nack", requeue)
    msg._reject_fn = lambda requeue=False: record("reject", requeue)
    msg._channel_alive = lambda: True
    return msg, sent


def _consumer_callback(transport: MagicMock, queue: str = "orders") -> Any:
    for call in transport.consume.call_args_list:
        if call.kwargs["queue"] == queue:
            return call.kwargs["callback"]
    raise AssertionError(f"no consumer for {queue}")


def _wait(predicate: Any, what: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.005)


def _timeout_broker(seconds: float = 0.05, **consumer: Any) -> SyncBroker:
    return SyncBroker(
        RabbitConfig(consumer=ConsumerConfig(processing_timeout=seconds, **consumer)),
    )


# ── processing_timeout: wiring ───────────────────────────────────────────


class TestProcessingTimeoutWiring:
    def test_no_deadlines_and_no_warning_by_default(self) -> None:
        with patch("rabbitkit.sync.broker.logger") as log:
            broker = SyncBroker()
        assert broker._deadlines is None
        log.warning.assert_not_called()

    def test_set_timeout_is_enforced_not_ignored(self) -> None:
        with patch("rabbitkit.sync.broker.logger") as log:
            broker = _timeout_broker(30.0)
        assert broker._deadlines is not None and broker._deadlines.seconds == 30.0
        assert not any("not enforced" in str(c) for c in log.warning.call_args_list)

    def test_single_worker_runs_handlers_on_one_dedicated_thread(self) -> None:
        broker = _timeout_broker(2.0)

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)  # no inline-heartbeat warning
            with _started(broker) as transport:
                pool = broker.worker_pool
                assert pool is not None and pool.worker_count == 1
                assert pool._executor is not None  # a thread, not inline
                assert broker._deadlines is not None
                transport.on_io_tick.assert_any_call(broker._deadlines.check)
                assert transport.poll_interval == pytest.approx(0.5)

    def test_a_worker_pool_is_used_as_is(self) -> None:
        broker = _timeout_broker(2.0)

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with _started(broker, WorkerConfig(worker_count=3)):
            assert broker.worker_pool is not None and broker.worker_pool.worker_count == 3

    def test_a_publish_only_broker_starts_no_handler_thread(self) -> None:
        broker = _timeout_broker(2.0)
        with _started(broker):
            assert broker.worker_pool is None


# ── processing_timeout: behaviour ────────────────────────────────────────


class TestProcessingTimeout:
    def test_expired_handler_is_requeued_once_and_its_late_ack_is_dropped(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        broker = _timeout_broker(0.05)
        release = threading.Event()
        started = threading.Event()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            started.set()
            release.wait(5)

        with _started(broker) as transport:
            msg, sent = _delivery()
            _consumer_callback(transport)(msg)
            assert started.wait(5)
            assert broker._deadlines is not None
            time.sleep(0.06)
            with caplog.at_level("WARNING"):
                broker._deadlines.check()  # the owner's I/O tick
                assert sent == [("nack", True)]  # transient UNKNOWN_OUTCOME, first delivery
                release.set()
                _wait(lambda: broker._deadlines is not None and broker._deadlines.running == 0, "the late return")
            assert sent == [("nack", True)]  # the late success did not ack
            assert "so its result is discarded" in caplog.text

    def test_expired_redelivery_is_dead_lettered(self) -> None:
        broker = _timeout_broker(0.05)
        release = threading.Event()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            release.wait(5)

        with _started(broker) as transport:
            msg, sent = _delivery(redelivered=True)
            _consumer_callback(transport)(msg)
            assert broker._deadlines is not None
            _wait(lambda: broker._deadlines is not None and broker._deadlines.running == 1, "the handler start")
            time.sleep(0.06)
            broker._pump(0.0)  # the drain pump checks deadlines too
            transport.pump.assert_called_with(0.0)
            release.set()
        assert sent == [("reject", False)]

    def test_a_late_handler_result_publish_is_never_sent(self) -> None:
        broker = _timeout_broker(0.05)
        release = threading.Event()
        returned = threading.Event()

        @broker.subscriber(queue="orders")
        @broker.publisher(routing_key="results")
        def handle(body: bytes) -> bytes:
            release.wait(5)
            returned.set()
            return b"result"

        outcomes: list[Any] = []
        original = broker._flow_controlled_internal_publish

        def spy(env: MessageEnvelope) -> Any:
            outcome = original(env)
            outcomes.append(outcome)
            return outcome

        broker._flow_controlled_internal_publish = spy  # type: ignore[method-assign]
        with _started(broker) as transport:
            msg, sent = _delivery()
            _consumer_callback(transport)(msg)
            _wait(lambda: broker._deadlines is not None and broker._deadlines.running == 1, "the handler start")
            time.sleep(0.06)
            assert broker._deadlines is not None
            broker._deadlines.check()
            release.set()
            _wait(lambda: broker._deadlines is not None and broker._deadlines.running == 0, "the late return")
            transport.publish.assert_not_called()
        assert [(o.status, o.reason) for o in outcomes] == [(PublishStatus.NOT_SENT, PublishReason.CANCELLED)]
        assert sent == [("nack", True)]

    def test_a_handler_in_time_publishes_and_acks(self) -> None:
        broker = _timeout_broker(5.0)

        @broker.subscriber(queue="orders")
        @broker.publisher(routing_key="results")
        def handle(body: bytes) -> bytes:
            return b"result"

        with _started(broker) as transport:
            transport.publish.return_value = MagicMock(ok=True)
            msg, sent = _delivery()
            _consumer_callback(transport)(msg)
            _wait(lambda: sent == [("ack", None)], "the ack")
            transport.publish.assert_called_once()

    def test_late_settlement_raised_into_the_handler_run_is_contained(self) -> None:
        broker = _timeout_broker(5.0)

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(body: bytes) -> None:
            raise LateSettlementError

        with _started(broker) as transport:
            msg, _sent = _delivery()
            _consumer_callback(transport)(msg)
            _wait(lambda: broker._deadlines is not None and broker._deadlines.running == 0, "the handler run")
            assert broker._in_flight == 0


# ── G-26: broker-initiated consumer cancel ───────────────────────────────


class TestConsumerCancelledByBroker:
    def _broker(self, collector: RecordingCollector | None = None) -> SyncBroker:
        broker = SyncBroker(middlewares=[MetricsMiddleware(collector)] if collector is not None else None)

        @broker.subscriber(queue="orders")
        def orders(body: bytes) -> None:
            pass

        @broker.subscriber(queue="audit")
        def audit(body: bytes) -> None:
            pass

        return broker

    def test_cancel_and_resume_update_the_route_metrics_and_hooks(self) -> None:
        from rabbitkit.health import broker_health_check

        collector = RecordingCollector()
        broker = self._broker(collector)
        cancelled: list[str] = []
        resumed: list[str] = []
        broker.on_consumer_cancelled = cancelled.append
        broker.on_consumer_resumed = resumed.append
        with _started(broker) as transport:
            transport.has_open_channels = True
            transport.on_consumer_cancelled.assert_called_once()  # once per transport, not per route
            on_cancel = transport.on_consumer_cancelled.call_args.args[0]
            on_resume = transport.on_consumer_resumed.call_args.args[0]

            on_cancel("orders")
            by_queue = {r.queue.name: r for r in broker.routes}
            assert by_queue["orders"].runtime_state.consumer_cancelled is True
            assert by_queue["audit"].runtime_state.consumer_cancelled is False
            assert cancelled == ["orders"]
            assert broker_health_check(broker).consumer_count == 1
            assert collector.count(_CFG.consumer_cancelled_total, queue="orders") == 1
            broker._sample_metrics()
            assert collector.gauge(_CFG.route_consuming, queue="orders") == 0.0
            assert collector.gauge(_CFG.route_consuming, queue="audit") == 1.0

            on_resume("orders")
            assert by_queue["orders"].runtime_state.consumer_cancelled is False
            assert resumed == ["orders"]
            assert broker_health_check(broker).consumer_count == 2
            assert collector.gauge(_CFG.route_consuming, queue="orders") == 1.0

    def test_hooks_are_optional_and_a_raising_hook_is_logged(self) -> None:
        broker = self._broker()
        with _started(broker) as transport:
            on_cancel = transport.on_consumer_cancelled.call_args.args[0]
            on_cancel("orders")  # no hook, no metrics

            def boom(queue: str) -> None:
                raise RuntimeError("hook bug")

            broker.on_consumer_resumed = boom
            on_resume = transport.on_consumer_resumed.call_args.args[0]
            with patch("rabbitkit.sync.broker.logger") as log:
                on_resume("orders")
            assert log.warning.call_args.args[0] == "on_consumer_%s hook raised (queue=%s)"
            assert all(not r.runtime_state.consumer_cancelled for r in broker.routes)

    def test_a_reconnect_resubscribes_a_cancelled_route_and_reports_it_resumed(self) -> None:
        broker = self._broker()
        resumed: list[str] = []
        broker.on_consumer_resumed = resumed.append
        with _started(broker) as transport:
            transport.on_consumer_cancelled.call_args.args[0]("orders")
            broker._recover_consumers(ConnectionResetError())
            assert resumed == ["orders"]
            assert all(not r.runtime_state.consumer_cancelled for r in broker.routes)
            # A new transport object would be wired again; the same one is not.
            transport.on_consumer_cancelled.assert_called_once()


# ── G-22: sampled publish gauge ──────────────────────────────────────────


class TestPublishInFlightGauge:
    def test_sampled_from_the_transport(self) -> None:
        collector = RecordingCollector()
        broker = SyncBroker(middlewares=[MetricsMiddleware(collector)])
        broker._transport = _transport()
        broker._wire_reconnect_metric()
        broker._sample_metrics()
        assert collector.gauge(_CFG.publish_in_flight) == 3.0
        broker._set_lifecycle_gauges(connected=False)
        assert collector.gauge(_CFG.publish_in_flight) == 0.0

    def test_pump_idle_samples_a_publish_only_broker(self) -> None:
        collector = RecordingCollector()
        broker = SyncBroker(middlewares=[MetricsMiddleware(collector)])
        transport = _transport()
        broker._transport = transport
        broker._wire_reconnect_metric()
        broker.pump_idle(0.0)
        transport.pump.assert_called_once_with(0.0)
        assert collector.gauge(_CFG.publish_in_flight) == 3.0


# ── max_in_flight_bytes on the sync broker ───────────────────────────────


class TestByteBudget:
    def test_start_passes_the_budget_and_the_admission_timeout(self) -> None:
        config = RabbitConfig(
            publisher=PublisherConfig(max_message_bytes=1000, max_in_flight_bytes=4096),
            pool=PoolConfig(channel_acquire_timeout=2.5),
        )
        broker = SyncBroker(config)
        with patch("rabbitkit.sync.broker.SyncTransport", return_value=_transport()) as cls:
            broker.start()
        try:
            kwargs = cls.call_args.kwargs
            assert (kwargs["max_in_flight_bytes"], kwargs["admission_timeout"]) == (4096, 2.5)
        finally:
            broker.stop(timeout=2)

    def test_no_budget_by_default(self) -> None:
        broker = SyncBroker()
        with patch("rabbitkit.sync.broker.SyncTransport", return_value=_transport()) as cls:
            broker.start()
        try:
            assert cls.call_args.kwargs["max_in_flight_bytes"] is None
        finally:
            broker.stop(timeout=2)

    def test_bytes_in_flight_are_sampled_when_a_budget_is_set(self) -> None:
        collector = RecordingCollector()
        broker = SyncBroker(middlewares=[MetricsMiddleware(collector)])
        transport = _transport()
        limiter = InFlightByteLimiter(1024)
        assert limiter.acquire(512, timeout=0.1)
        transport.byte_limiter = limiter
        broker._transport = transport
        broker._wire_reconnect_metric()
        broker._sample_metrics()
        assert collector.gauge(_CFG.publish_in_flight_bytes) == 512.0

    def test_bytes_in_flight_are_not_emitted_without_a_budget(self) -> None:
        collector = RecordingCollector()
        broker = SyncBroker(middlewares=[MetricsMiddleware(collector)])
        broker._transport = _transport()
        broker._wire_reconnect_metric()
        broker._sample_metrics()
        assert collector.gauge(_CFG.publish_in_flight_bytes) is None
