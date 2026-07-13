"""production_pipeline/test_pipeline.py — the pipeline's contract, no broker.

TestBroker runs the REAL pipeline (serialization -> validation -> handler ->
result publish -> settlement) in memory, so the business contract is testable
in CI without RabbitMQ. This mirrors app.py's processor route 1:1 — if you
change the handler there, change it here (the nightly smoke test runs both).

Run:
    pytest examples/production_pipeline/test_pipeline.py -v
"""

import json

from pydantic import BaseModel, Field

from rabbitkit.serialization import JSONSerializer
from rabbitkit.testing import TestBroker

# NOTE: no `from __future__ import annotations` — Pydantic body validation
# needs real (non-stringized) annotations, exactly as in app.py.


class OrderCreated(BaseModel):
    order_id: str
    customer_id: str
    amount_cents: int = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    simulate: str | None = None


def make_pipeline() -> tuple[TestBroker, object]:
    """TestBroker mirror of app.py's processor route (same contract)."""
    broker = TestBroker(serializer=JSONSerializer())

    @broker.subscriber(queue="pp.orders.incoming")
    @broker.publisher(exchange="pp.orders", routing_key="order.processed")
    def process_order(order: OrderCreated) -> dict:
        if order.simulate == "transient":
            raise ConnectionError(f"downstream unavailable for {order.order_id}")
        if order.simulate == "permanent":
            raise ValueError(f"unprocessable order {order.order_id}")
        fee = max(30, order.amount_cents * 3 // 100)
        return {
            "order_id": order.order_id,
            "customer_id": order.customer_id,
            "amount_cents": order.amount_cents,
            "fee_cents": fee,
            "currency": order.currency,
            "status": "processed",
        }

    return broker, process_order


def order_body(order_id: str = "ord-1", amount: int = 10_000, **extra: object) -> bytes:
    return json.dumps(
        {"order_id": order_id, "customer_id": "cus-1", "amount_cents": amount, "currency": "USD", **extra}
    ).encode()


def test_valid_order_is_processed_acked_and_republished() -> None:
    broker, handler = make_pipeline()
    broker.start()

    broker.publish("pp.orders.incoming", order_body(amount=10_000))

    handler.mock.assert_called_once()  # type: ignore[union-attr]
    broker.assert_acked(broker.consumed_messages[0])

    # The consume side is only half the contract — the result must publish.
    assert len(broker.published_messages) == 1
    result = broker.published_messages[0]
    assert result.exchange == "pp.orders"
    assert result.routing_key == "order.processed"
    payload = json.loads(result.body)
    assert payload["status"] == "processed"
    assert payload["fee_cents"] == 300  # 3% of 10_000


def test_fee_has_a_floor() -> None:
    broker, _ = make_pipeline()
    broker.start()

    broker.publish("pp.orders.incoming", order_body(amount=100))

    assert json.loads(broker.published_messages[0].body)["fee_cents"] == 30


def test_invalid_payload_is_permanent_and_publishes_nothing() -> None:
    """amount_cents <= 0 fails Pydantic validation BEFORE the handler runs:
    a malformed message is a PERMANENT error — settled, never retried, and
    no downstream event escapes. (`.mock` records DELIVERIES with the raw
    body, not handler executions — so the proof the handler never produced
    a result is the empty publish log, not the mock.)"""
    broker, _ = make_pipeline()
    broker.start()

    broker.publish("pp.orders.incoming", order_body(amount=0))

    msg = broker.consumed_messages[0]
    assert msg._disposition in ("nacked", "rejected")
    assert broker.published_messages == []


def test_permanent_business_error_publishes_nothing() -> None:
    broker, _ = make_pipeline()
    broker.start()

    broker.publish("pp.orders.incoming", order_body(simulate="permanent"))

    msg = broker.consumed_messages[0]
    assert msg._disposition in ("nacked", "rejected")
    assert broker.published_messages == []


def test_transient_error_is_not_acked() -> None:
    """A transient failure must NEVER ack the source message — it stays
    eligible for redelivery (in production the retry ladder owns it)."""
    broker, _ = make_pipeline()
    broker.start()

    broker.publish("pp.orders.incoming", order_body(simulate="transient"))

    msg = broker.consumed_messages[0]
    assert msg._disposition != "acked"


def test_handler_is_idempotent_for_the_same_order() -> None:
    """The idempotency contract (docs/production/idempotency.md): the same
    logical event delivered twice must yield the same downstream result."""
    broker, handler = make_pipeline()
    broker.start()

    body = order_body(order_id="ord-dup", amount=5_000)
    broker.publish("pp.orders.incoming", body)
    broker.publish("pp.orders.incoming", body)

    assert handler.mock.call_count == 2  # type: ignore[union-attr]
    first, second = (json.loads(m.body) for m in broker.published_messages)
    assert first == second  # rerun-safe: identical result, no drift
