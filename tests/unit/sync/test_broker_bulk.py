"""SyncBroker bulk operations — publish_many / iter_publish / ack_many / nack_many / preflight."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.bulk import REASON_BODY_TOO_LARGE, REASON_CONFIRMED, REASON_RETURNED, BulkPublishOptions
from rabbitkit.core.config import PublisherConfig, RabbitConfig
from rabbitkit.core.errors import BackpressureError, BrokerNotStartedError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import (
    BulkPublishStatus,
    MessageEnvelope,
    PreflightStatus,
    PublishOutcome,
    PublishStatus,
    ReliabilityProfile,
    SettlementItemStatus,
)
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.middleware.metrics import MetricsMiddleware
from rabbitkit.sync.broker import SyncBroker


@pytest.fixture(autouse=True)
def _check_pika() -> None:
    pytest.importorskip("pika")


def _env(i: int, size: int = 1) -> MessageEnvelope:
    return MessageEnvelope(routing_key=f"q{i}", body=b"x" * size, message_id=f"id-{i}")


def _started(broker: SyncBroker, publish: Any) -> MagicMock:
    transport = MagicMock()
    transport.publish = publish
    broker._transport = transport
    broker._started = True
    return transport


def _msg(tag: int, alive: bool = True) -> RabbitMessage:
    m = RabbitMessage(body=b"x", delivery_tag=tag, message_id=f"m{tag}")
    calls: list[tuple[str, Any]] = []
    m.raw_message = calls
    m._ack_fn = lambda: calls.append(("ack", None))
    m._nack_fn = lambda rq: calls.append(("nack", rq))
    m._reject_fn = lambda rq: calls.append(("reject", rq))
    m._channel_alive = lambda: alive
    return m


class _RecordingMiddleware(BaseMiddleware):
    def __init__(self) -> None:
        self.seen: list[str] = []

    def publish_scope(self, call_next: Any, envelope: MessageEnvelope) -> Any:
        self.seen.append(envelope.message_id)
        return call_next(envelope)


class TestPublishMany:
    def test_requires_started(self) -> None:
        broker = SyncBroker()
        with pytest.raises(BrokerNotStartedError):
            broker.publish_many([_env(0)])
        with pytest.raises(BrokerNotStartedError):
            list(broker.iter_publish([_env(0)]))

    def test_all_confirmed_in_input_order(self) -> None:
        broker = SyncBroker()
        transport = _started(broker, MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        result = broker.publish_many([_env(i) for i in range(5)])
        assert result.all_confirmed and len(result) == 5
        assert [it.index for it in result.items] == [0, 1, 2, 3, 4]
        assert [it.reason for it in result.items] == [REASON_CONFIRMED] * 5
        assert transport.publish.call_count == 5
        assert result.raise_for_status() is result

    def test_mixed_outcomes(self) -> None:
        broker = SyncBroker()
        outcomes = iter(
            [
                PublishOutcome(status=PublishStatus.CONFIRMED),
                PublishOutcome(status=PublishStatus.RETURNED),
                PublishOutcome(status=PublishStatus.TIMEOUT),
            ]
        )
        _started(broker, MagicMock(side_effect=lambda e: next(outcomes)))
        result = broker.publish_many([_env(i) for i in range(3)])
        assert [it.status for it in result.items] == [
            BulkPublishStatus.CONFIRMED,
            BulkPublishStatus.UNROUTABLE,
            BulkPublishStatus.UNKNOWN,
        ]
        assert result.items[1].reason == REASON_RETURNED
        assert len(result.unknown) == 1

    def test_oversized_item_rejected_before_transport(self) -> None:
        broker = SyncBroker(RabbitConfig(publisher=PublisherConfig(max_message_bytes=10)))
        transport = _started(broker, MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        result = broker.publish_many([_env(0), _env(1, size=50)])
        assert result.items[1].status is BulkPublishStatus.INVALID
        assert result.items[1].reason == REASON_BODY_TOO_LARGE
        assert transport.publish.call_count == 1

    def test_shares_publish_middlewares(self) -> None:
        mw = _RecordingMiddleware()
        broker = SyncBroker(middlewares=[mw])
        _started(broker, MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        broker.publish_many([_env(0), _env(1)])
        assert mw.seen == ["id-0", "id-1"]  # exactly once per attempt

    def test_shares_flow_controller(self) -> None:
        broker = SyncBroker()
        transport = _started(broker, MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        fc = MagicMock()
        fc.acquire.return_value = False
        broker.flow_controller = fc
        result = broker.publish_many([_env(0)])
        assert result.items[0].status is BulkPublishStatus.NOT_SENT
        assert result.items[0].reason == "backpressure_dropped"
        assert isinstance(result.items[0].error, BackpressureError)
        transport.publish.assert_not_called()

    def test_max_items_guard_publishes_nothing(self) -> None:
        broker = SyncBroker()
        transport = _started(broker, MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        with pytest.raises(ValueError, match="max_items"):
            broker.publish_many([_env(i) for i in range(3)], BulkPublishOptions(max_items=2))
        transport.publish.assert_not_called()

    def test_iter_publish_streams(self) -> None:
        broker = SyncBroker()
        _started(broker, MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))

        def gen() -> Any:
            for i in range(3):
                yield _env(i)

        items = list(broker.iter_publish(gen()))
        assert [it.index for it in items] == [0, 1, 2] and all(it.ok for it in items)

    def test_bulk_metrics_recorded(self) -> None:
        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector)
        broker = SyncBroker(middlewares=[mw])
        _started(broker, MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        broker.publish_many([_env(0), _env(1)])
        collector.inc_counter.assert_any_call(
            "rabbitkit_bulk_publish_items_total", {"status": "confirmed", "reason": "confirmed"}
        )
        collector.observe_histogram.assert_any_call("rabbitkit_bulk_publish_batch_size", {}, 2.0)


class TestAckMany:
    def test_ack_many_individual(self) -> None:
        broker = SyncBroker()
        msgs = [_msg(1), _msg(3)]
        report = broker.ack_many(msgs)
        assert report.all_dispatched
        for m in msgs:
            assert m.raw_message == [("ack", None)] and m.disposition == "acked"

    def test_nack_many_requeue_false(self) -> None:
        broker = SyncBroker()
        m = _msg(1)
        report = broker.nack_many([m], requeue=False)
        assert report.all_dispatched and m.raw_message == [("nack", False)]

    def test_stale_handle_reported_not_acked(self) -> None:
        broker = SyncBroker()
        stale, fresh = _msg(1, alive=False), _msg(2)
        report = broker.ack_many([stale, fresh])
        assert report.items[0].status is SettlementItemStatus.STALE
        assert report.items[1].status is SettlementItemStatus.NOT_ATTEMPTED  # fail_fast default
        assert not fresh.is_settled

    def test_fail_fast_false_settles_valid_ones(self) -> None:
        broker = SyncBroker()
        stale, fresh = _msg(1, alive=False), _msg(2)
        report = broker.ack_many([stale, fresh], fail_fast=False)
        assert report.items[1].status is SettlementItemStatus.DISPATCHED
        assert fresh.is_settled and not stale.is_settled

    def test_settlement_metrics(self) -> None:
        collector = MagicMock()
        broker = SyncBroker(middlewares=[MetricsMiddleware(collector=collector)])
        broker.ack_many([_msg(1)])
        collector.inc_counter.assert_any_call(
            "rabbitkit_settlement_items_total", {"action": "ack", "status": "dispatched"}
        )


class TestPreflight:
    def test_preflight_default_standard(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        report = broker.preflight()
        assert report.profile is ReliabilityProfile.STANDARD
        assert report.ok

    def test_preflight_critical_unverified_without_management(self) -> None:
        from rabbitkit.core.profiles import critical_config
        from rabbitkit.core.topology import RabbitQueue
        from rabbitkit.core.types import QueueType

        broker = SyncBroker(critical_config())

        @broker.subscriber(queue=RabbitQueue(name="orders", queue_type=QueueType.QUORUM))
        def handle(body: bytes) -> None:
            pass

        report = broker.preflight("critical")
        assert report.ok and not report.fully_verified
        assert report.unverified[0].status is PreflightStatus.UNVERIFIED


class TestLifecycleGauges:
    def test_gauges_set_on_start_and_stop(self) -> None:
        from unittest.mock import patch

        collector = MagicMock()
        broker = SyncBroker(middlewares=[MetricsMiddleware(collector=collector)])

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"), patch("pika.BlockingConnection") as conn:
            ch = MagicMock()
            ch.is_open = True
            conn.return_value.channel.return_value = ch
            conn.return_value.is_open = True
            broker.start()
            collector.set_gauge.assert_any_call("rabbitkit_broker_connected", {}, 1.0)
            collector.set_gauge.assert_any_call("rabbitkit_consumer_active", {}, 1.0)
            collector.set_gauge.reset_mock()
            broker.stop()
        collector.set_gauge.assert_any_call("rabbitkit_broker_connected", {}, 0.0)
        collector.set_gauge.assert_any_call("rabbitkit_consumer_active", {}, 0.0)
