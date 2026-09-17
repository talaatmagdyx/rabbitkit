"""AsyncBroker bulk operations — publish_many / iter_publish / ack_many / nack_many / preflight."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.bulk import REASON_CONFIRM_TIMEOUT, BulkPublishOptions
from rabbitkit.core.config import BatchPublishConfig, PublisherConfig, RabbitConfig
from rabbitkit.core.errors import BrokerNotStartedError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import (
    BulkPublishStatus,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    ReliabilityProfile,
    SettlementItemStatus,
)
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.middleware.metrics import MetricsMiddleware


def _env(i: int, size: int = 1) -> MessageEnvelope:
    return MessageEnvelope(routing_key=f"q{i}", body=b"x" * size, message_id=f"id-{i}")


def _started(broker: AsyncBroker, publish: Any) -> Any:
    transport = AsyncMock()
    transport.publish = publish
    broker._transport = transport
    broker._started = True
    return transport


def _msg(tag: int, alive: bool = True) -> RabbitMessage:
    m = RabbitMessage(body=b"x", delivery_tag=tag, message_id=f"m{tag}")
    calls: list[tuple[str, Any]] = []
    m.raw_message = calls

    async def _a() -> None:
        calls.append(("ack", None))

    async def _n(rq: bool) -> None:
        calls.append(("nack", rq))

    m._ack_async_fn = _a
    m._nack_async_fn = _n
    m._channel_alive = lambda: alive
    return m


class _RecordingMiddleware(BaseMiddleware):
    def __init__(self) -> None:
        self.seen: list[str] = []

    async def publish_scope_async(self, call_next: Any, envelope: MessageEnvelope) -> Any:
        self.seen.append(envelope.message_id)
        return await call_next(envelope)


class TestPublishMany:
    async def test_requires_started(self) -> None:
        broker = AsyncBroker()
        with pytest.raises(BrokerNotStartedError):
            await broker.publish_many([_env(0)])

    async def test_all_confirmed_in_input_order(self) -> None:
        broker = AsyncBroker()
        transport = _started(broker, AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        result = await broker.publish_many([_env(i) for i in range(20)], BulkPublishOptions(max_in_flight=4))
        assert result.all_confirmed and len(result) == 20
        assert [it.index for it in result.items] == list(range(20))
        assert transport.publish.await_count == 20

    async def test_concurrency_bounded(self) -> None:
        broker = AsyncBroker()
        active = 0
        peak = 0

        async def pub(e: MessageEnvelope) -> PublishOutcome:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.005)
            active -= 1
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        _started(broker, pub)
        await broker.publish_many([_env(i) for i in range(30)], BulkPublishOptions(max_in_flight=3))
        assert peak <= 3

    async def test_mixed_and_unknown(self) -> None:
        broker = AsyncBroker()

        async def pub(e: MessageEnvelope) -> PublishOutcome:
            if e.routing_key == "q1":
                return PublishOutcome(status=PublishStatus.NACKED)
            if e.routing_key == "q2":
                raise ConnectionResetError("dead")
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        _started(broker, pub)
        result = await broker.publish_many([_env(i) for i in range(3)])
        assert [it.status for it in result.items] == [
            BulkPublishStatus.CONFIRMED,
            BulkPublishStatus.NACKED,
            BulkPublishStatus.UNKNOWN,
        ]

    async def test_confirm_timeout_override(self) -> None:
        broker = AsyncBroker()

        async def slow(e: MessageEnvelope) -> PublishOutcome:
            await asyncio.sleep(1)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        _started(broker, slow)
        result = await broker.publish_many([_env(0)], BulkPublishOptions(confirm_timeout=0.02))
        assert result.items[0].status is BulkPublishStatus.UNKNOWN
        assert result.items[0].reason == REASON_CONFIRM_TIMEOUT

    async def test_oversized_rejected_before_transport(self) -> None:
        broker = AsyncBroker(RabbitConfig(publisher=PublisherConfig(max_message_bytes=8)))
        transport = _started(broker, AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        result = await broker.publish_many([_env(0, size=100)])
        assert result.items[0].status is BulkPublishStatus.INVALID
        transport.publish.assert_not_awaited()

    async def test_middleware_once_per_item(self) -> None:
        mw = _RecordingMiddleware()
        broker = AsyncBroker(middlewares=[mw])
        _started(broker, AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        await broker.publish_many([_env(0), _env(1)])
        assert sorted(mw.seen) == ["id-0", "id-1"]

    async def test_uses_batch_publisher_when_configured(self) -> None:
        broker = AsyncBroker(batch_config=BatchPublishConfig(batch_size=2))
        _started(broker, AsyncMock(return_value=PublishOutcome(status=PublishStatus.ERROR)))
        batch = MagicMock()
        batch.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        broker._batch_publisher = batch
        result = await broker.publish_many([_env(0), _env(1)])
        assert result.all_confirmed and batch.publish.await_count == 2

    async def test_max_items_guard(self) -> None:
        broker = AsyncBroker()
        transport = _started(broker, AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        with pytest.raises(ValueError, match="max_items"):
            await broker.publish_many([_env(i) for i in range(3)], BulkPublishOptions(max_items=2))
        transport.publish.assert_not_awaited()

    async def test_iter_publish_async_input(self) -> None:
        broker = AsyncBroker()
        _started(broker, AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))

        async def agen() -> Any:
            for i in range(3):
                yield _env(i)

        items = [it async for it in broker.iter_publish(agen())]
        assert sorted(it.index for it in items) == [0, 1, 2]

    async def test_bulk_metrics_recorded(self) -> None:
        collector = MagicMock()
        broker = AsyncBroker(middlewares=[MetricsMiddleware(collector=collector)])
        _started(broker, AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED)))
        await broker.publish_many([_env(0)])
        collector.inc_counter.assert_any_call(
            "rabbitkit_bulk_publish_items_total", {"status": "confirmed", "reason": "confirmed"}
        )


class TestAckMany:
    async def test_ack_many(self) -> None:
        broker = AsyncBroker()
        msgs = [_msg(1), _msg(3)]
        report = await broker.ack_many(msgs)
        assert report.all_dispatched
        for m in msgs:
            assert m.raw_message == [("ack", None)]

    async def test_nack_many(self) -> None:
        broker = AsyncBroker()
        m = _msg(1)
        report = await broker.nack_many([m], requeue=False)
        assert report.all_dispatched and m.raw_message == [("nack", False)]

    async def test_stale_and_fail_fast(self) -> None:
        broker = AsyncBroker()
        stale, fresh = _msg(1, alive=False), _msg(2)
        report = await broker.ack_many([stale, fresh])
        assert report.items[0].status is SettlementItemStatus.STALE
        assert report.items[1].status is SettlementItemStatus.NOT_ATTEMPTED
        report = await broker.ack_many([stale, fresh], fail_fast=False)
        assert report.items[1].status is SettlementItemStatus.DISPATCHED


class TestPreflight:
    async def test_preflight(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        report = broker.preflight(ReliabilityProfile.STANDARD)
        assert report.ok


class TestLifecycleGauges:
    async def test_gauges_on_start_stop(self) -> None:
        collector = MagicMock()
        broker = AsyncBroker(middlewares=[MetricsMiddleware(collector=collector)])

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        from unittest.mock import patch

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        mock_transport.on_reconnect = MagicMock()
        mock_transport.on_channel_opened = MagicMock()
        mock_transport.on_channel_rebuilt = MagicMock()
        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()
            collector.set_gauge.assert_any_call("rabbitkit_broker_connected", {}, 1.0)
            collector.set_gauge.assert_any_call("rabbitkit_consumer_active", {}, 1.0)
            collector.set_gauge.reset_mock()
            await broker.stop()
        collector.set_gauge.assert_any_call("rabbitkit_broker_connected", {}, 0.0)
