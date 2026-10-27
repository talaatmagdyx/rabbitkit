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
    make_broker: Callable[..., TestBroker], reject_spy: list[bool]
) -> None:
    """At the retry ceiling, RetryMiddleware publishes a confirmed,
    metadata-stamped copy to ``<queue>.dlq`` and only then acks the source
    (no hot loop, no requeue). If that DLQ publish failed it would fall back
    to the pipeline's reject(requeue=False) -> source-queue DLX -> DLQ.
    """
    retry_mw, captured = _capturing_retry()
    broker = make_broker(middlewares=[retry_mw])
    get_order_service().downstream_up = False

    await broker.publish_async(
        "orders.queue",
        order_body(),
        routing_key="orders.created",
        headers={"x-rabbitkit-retry-count": str(ORDERS_RETRY.max_retries)},
    )

    # no further delay-queue publish: the only publish is the DLQ copy
    assert [e.routing_key for e in captured] == ["orders.queue.dlq"]
    assert captured[0].headers["x-rabbitkit-failure-category"] == "exhausted"
    assert broker.consumed_messages[-1]._disposition == "acked"
    assert reject_spy == []  # never a requeue=True hot loop


def test_http_error_mapping_by_type() -> None:
    assert isinstance(map_http_status(503), DownstreamUnavailable)  # ⊂ OSError → transient
    assert isinstance(map_http_status(429), DownstreamUnavailable)
    assert isinstance(map_http_status(400), PermanentError)         # ⊂ ValueError → permanent
    assert isinstance(map_http_status(404), PermanentError)
