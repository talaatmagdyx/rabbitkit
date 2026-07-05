"""Pytest tests using TestBroker — no RabbitMQ required.

Run:
    pytest examples/testbroker_pytest/test_handler.py -v

Requirements:
    pip install "rabbitkit" pytest
"""

import json

import pytest

from rabbitkit.testing import TestBroker

# ── Application code under test ───────────────────────────────────────────────

def make_broker() -> tuple["TestBroker", object]:
    broker = TestBroker()

    @broker.subscriber(queue="orders")
    def handle_order(body: bytes) -> None:
        data = json.loads(body)
        if data.get("qty", 0) <= 0:
            raise ValueError(f"invalid qty: {data['qty']}")
        print(f"[handler] order {data['id']} confirmed")

    return broker, handle_order


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_handler_is_called() -> None:
    broker, handle_order = make_broker()
    broker.start()

    broker.publish("orders", json.dumps({"id": 1, "qty": 2}).encode())

    handle_order.mock.assert_called_once()  # type: ignore[union-attr]


def test_message_is_acked_on_success() -> None:
    broker, _ = make_broker()
    broker.start()

    broker.publish("orders", json.dumps({"id": 2, "qty": 1}).encode())

    msg = broker.consumed_messages[0]
    broker.assert_acked(msg)


def test_message_is_nacked_on_error() -> None:
    broker, _ = make_broker()
    broker.start()

    broker.publish("orders", json.dumps({"id": 3, "qty": 0}).encode())

    msg = broker.consumed_messages[0]
    # AUTO policy: permanent error (ValueError) → nack(requeue=False)
    assert msg._disposition in ("nacked", "rejected")


def test_multiple_messages() -> None:
    broker, handle_order = make_broker()
    broker.start()

    for i in range(5):
        broker.publish("orders", json.dumps({"id": i, "qty": 1}).encode())

    assert handle_order.mock.call_count == 5  # type: ignore[union-attr]
    assert len(broker.consumed_messages) == 5


def test_settlement_record() -> None:
    broker, _ = make_broker()
    broker.start()

    broker.publish("orders", json.dumps({"id": 10, "qty": 3}).encode())

    msg = broker.consumed_messages[0]
    records = broker.settlements_for(msg)
    assert records, "expected at least one settlement record"
    assert records[-1].kind == "ack"
