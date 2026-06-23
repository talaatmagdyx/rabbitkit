"""Retry middleware behaviour (docs §0/§3/§7).

Subscriber middlewares ARE executed by the pipeline (RetryMiddleware/Dedup/etc.
run via @subscriber(middlewares=[...])). These tests prove the correct retry
wiring: RetryMiddleware + a real publish_async_fn + AckPolicy.NACK_ON_ERROR.
"""

from __future__ import annotations

from collections.abc import Callable

from examples.order_service.config import ORDERS_RETRY
from examples.order_service.error_mapping import map_http_status
from examples.order_service.errors import DownstreamUnavailable, PermanentError
from examples.order_service.services import get_order_service
from rabbitkit import PublishOutcome, PublishStatus
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.retry import RetryMiddleware
from rabbitkit.testing import TestBroker

from .conftest import order_body


def _capturing_retry() -> tuple[RetryMiddleware, list[MessageEnvelope]]:
    captured: list[MessageEnvelope] = []

    async def publish(env: MessageEnvelope) -> PublishOutcome:
        captured.append(env)
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    return RetryMiddleware(ORDERS_RETRY, publish_async_fn=publish), captured


async def test_transient_routes_to_delay_queue(make_broker: Callable[..., TestBroker]) -> None:
    """Transient failure → RetryMiddleware publishes to a delay queue + acks source."""
    retry_mw, captured = _capturing_retry()
    broker = make_broker(middlewares=[retry_mw])
    get_order_service().downstream_up = False  # force a transient failure

    await broker.publish_async("orders.queue", order_body(), routing_key="orders.created")

    assert len(captured) == 1
    assert ".retry." in captured[0].routing_key
    assert broker.consumed_messages[-1]._disposition == "acked"


async def test_exhausted_transient_goes_to_dlq(
    make_broker: Callable[..., TestBroker], nack_spy: list[bool]
) -> None:
    """At the retry ceiling → terminal → nack(requeue=False) → DLQ (no hot loop)."""
    retry_mw, captured = _capturing_retry()
    broker = make_broker(middlewares=[retry_mw])
    get_order_service().downstream_up = False

    await broker.publish_async(
        "orders.queue",
        order_body(),
        routing_key="orders.created",
        headers={"x-rabbitkit-retry-count": str(ORDERS_RETRY.max_retries)},
    )

    assert captured == []  # no further delay-queue publish
    assert broker.consumed_messages[-1]._disposition == "nacked"
    assert nack_spy == [False]  # nack(requeue=False) → DLQ, NOT a requeue=True hot loop


def test_http_error_mapping_by_type() -> None:
    assert isinstance(map_http_status(503), DownstreamUnavailable)  # ⊂ OSError → transient
    assert isinstance(map_http_status(429), DownstreamUnavailable)
    assert isinstance(map_http_status(400), PermanentError)         # ⊂ ValueError → permanent
    assert isinstance(map_http_status(404), PermanentError)
