"""Handler behaviour via TestBroker (docs §25). NACK_ON_ERROR routes terminal
failures to nack(requeue=False) → DLQ. Async settlement is asserted via the
message's _disposition and the nack_spy fixture (which captures requeue)."""

from __future__ import annotations

from collections.abc import Callable

from examples.order_service.services import get_order_service
from rabbitkit.testing import TestBroker

from .conftest import order_body


async def test_success_acks(make_broker: Callable[..., TestBroker]) -> None:
    broker = make_broker()
    await broker.publish_async("orders.queue", order_body(), routing_key="orders.created")

    assert broker.consumed_messages[-1]._disposition == "acked"
    assert get_order_service().already_processed("o1", 1)


async def test_invalid_tenant_is_permanent(
    make_broker: Callable[..., TestBroker], nack_spy: list[bool]
) -> None:
    broker = make_broker()
    await broker.publish_async(
        "orders.queue", order_body(tenant_id="unknown"), routing_key="orders.created"
    )

    # PermanentError (⊂ ValueError) → terminal → nack(requeue=False), never acked.
    assert broker.consumed_messages[-1]._disposition == "nacked"
    assert nack_spy == [False]


async def test_validation_error_no_retry(
    make_broker: Callable[..., TestBroker], nack_spy: list[bool]
) -> None:
    broker = make_broker()
    # pydantic ValidationError (⊂ ValueError) → PERMANENT → DLQ, never retried.
    await broker.publish_async("orders.queue", b'{"bad":"data"}', routing_key="orders.created")

    assert broker.consumed_messages[-1]._disposition == "nacked"
    assert nack_spy == [False]


async def test_idempotent_second_delivery_is_noop(make_broker: Callable[..., TestBroker]) -> None:
    broker = make_broker()
    await broker.publish_async("orders.queue", order_body(), routing_key="orders.created")
    await broker.publish_async("orders.queue", order_body(), routing_key="orders.created")

    # Both deliveries acked; the second is a safe no-op (idempotency).
    assert [m._disposition for m in broker.consumed_messages] == ["acked", "acked"]
    assert len(get_order_service()._orders) == 1
