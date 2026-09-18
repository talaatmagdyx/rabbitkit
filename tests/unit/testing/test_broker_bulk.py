"""TestBroker.ack_many / nack_many — selected settlement in user tests."""

from __future__ import annotations

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import AckPolicy, SettlementItemStatus
from rabbitkit.testing.broker import TestAsyncBroker, TestBroker


class TestTestBrokerSelectedSettlement:
    def test_ack_many_after_manual_handlers(self) -> None:
        broker = TestBroker()
        held: list[RabbitMessage] = []

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(body: bytes, msg: RabbitMessage) -> None:
            held.append(msg)  # defer settlement to a batch commit

        broker.start()
        for i in range(3):
            broker.publish("orders", f"o{i}".encode())
        assert all(not m.is_settled for m in held)

        report = broker.ack_many([held[0], held[2]])
        assert report.all_dispatched
        broker.assert_acked(held[0])
        broker.assert_acked(held[2])
        assert not held[1].is_settled  # unselected sibling untouched

    def test_nack_many_and_report_problems(self) -> None:
        broker = TestBroker()
        held: list[RabbitMessage] = []

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(body: bytes, msg: RabbitMessage) -> None:
            held.append(msg)

        broker.start()
        broker.publish("orders", b"a")
        broker.publish("orders", b"b")
        held[0].ack()
        report = broker.nack_many(held, requeue=False, fail_fast=False)
        assert report.items[0].status is SettlementItemStatus.ALREADY_SETTLED
        assert report.items[1].status is SettlementItemStatus.DISPATCHED
        broker.assert_nacked(held[1], requeue=False)

    def test_duplicate_and_invalid_inputs(self) -> None:
        broker = TestBroker()
        held: list[RabbitMessage] = []

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(body: bytes, msg: RabbitMessage) -> None:
            held.append(msg)

        broker.start()
        broker.publish("orders", b"a")
        report = broker.ack_many([held[0], held[0], "junk"])  # type: ignore[list-item]
        assert not report.all_dispatched
        statuses = [it.status for it in report.items]
        assert statuses == [
            SettlementItemStatus.NOT_ATTEMPTED,
            SettlementItemStatus.DUPLICATE,
            SettlementItemStatus.INVALID,
        ]
        assert not held[0].is_settled

    async def test_async_variants(self) -> None:
        broker = TestAsyncBroker()
        held: list[RabbitMessage] = []

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        async def handle(body: bytes, msg: RabbitMessage) -> None:
            held.append(msg)

        broker.start()
        await broker.publish_async("orders", b"a")
        await broker.publish_async("orders", b"b")
        report = await broker.ack_many_async([held[0]])
        assert report.all_dispatched
        broker.assert_acked(held[0])
        report = await broker.nack_many_async([held[1]])
        assert report.all_dispatched
        broker.assert_nacked(held[1], requeue=True)
