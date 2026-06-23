"""Regression guard: @subscriber(middlewares=[...]) is executed by the pipeline.

The pipeline composes route.route_middlewares outer→inner around the handler and
calls consume_scope / consume_scope_async. Before this was wired, middlewares were
stored on the route but never run.
"""

from __future__ import annotations

from typing import Any

from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.testing import TestBroker


def _probe(order: list[str], name: str) -> BaseMiddleware:
    class _M(BaseMiddleware):
        def consume_scope(self, call_next: Any, message: Any) -> Any:
            order.append(f"{name}:before")
            result = call_next(message)
            order.append(f"{name}:after")
            return result

        async def consume_scope_async(self, call_next: Any, message: Any) -> Any:
            order.append(f"{name}:before")
            result = await call_next(message)
            order.append(f"{name}:after")
            return result

    return _M()


def test_sync_middlewares_execute_outer_to_inner() -> None:
    order: list[str] = []
    broker = TestBroker()

    @broker.subscriber(queue="q", middlewares=[_probe(order, "A"), _probe(order, "B")])
    def handler(body: bytes) -> None:
        order.append("handler")

    broker.start()
    broker.publish("q", b"{}")

    # First middleware in the list is the OUTERMOST wrapper.
    assert order == ["A:before", "B:before", "handler", "B:after", "A:after"]


async def test_async_middlewares_execute_outer_to_inner() -> None:
    order: list[str] = []
    broker = TestBroker()

    @broker.subscriber(queue="q", middlewares=[_probe(order, "A"), _probe(order, "B")])
    async def handler(body: bytes) -> None:
        order.append("handler")

    broker.start()
    await broker.publish_async("q", b"{}")

    assert order == ["A:before", "B:before", "handler", "B:after", "A:after"]


def test_middleware_can_short_circuit_the_handler() -> None:
    order: list[str] = []

    class Skip(BaseMiddleware):
        def consume_scope(self, call_next: Any, message: Any) -> Any:
            order.append("skip")  # deliberately does NOT call call_next

    broker = TestBroker()

    @broker.subscriber(queue="q", middlewares=[Skip()])
    def handler(body: bytes) -> None:
        order.append("handler")

    broker.start()
    broker.publish("q", b"{}")

    assert order == ["skip"]  # handler never ran


# ── C4: route publish_scope middlewares apply to result publishing ───────────


class _Route:
    """Minimal stand-in: _compose_publish_* only reads route_middlewares."""

    def __init__(self, middlewares: list[Any]) -> None:
        self.route_middlewares = middlewares


def test_compose_publish_sync_applies_route_publish_scope() -> None:
    order: list[str] = []

    class PubProbe(BaseMiddleware):
        def publish_scope(self, call_next: Any, envelope: Any) -> Any:
            order.append("pub:before")
            result = call_next(envelope)
            order.append("pub:after")
            return result

    def publish_fn(env: MessageEnvelope) -> PublishOutcome:
        order.append("publish")
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    pipeline = HandlerPipeline()
    chain = pipeline._compose_publish_sync(_Route([PubProbe()]), publish_fn)
    outcome = chain(MessageEnvelope(routing_key="x", body=b"{}"))

    assert order == ["pub:before", "publish", "pub:after"]
    assert outcome.ok


async def test_compose_publish_async_applies_route_publish_scope() -> None:
    order: list[str] = []

    class PubProbe(BaseMiddleware):
        async def publish_scope_async(self, call_next: Any, envelope: Any) -> Any:
            order.append("pub:before")
            result = await call_next(envelope)
            order.append("pub:after")
            return result

    async def publish_fn(env: MessageEnvelope) -> PublishOutcome:
        order.append("publish")
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    pipeline = HandlerPipeline()
    chain = pipeline._compose_publish_async(_Route([PubProbe()]), publish_fn)
    outcome = await chain(MessageEnvelope(routing_key="x", body=b"{}"))

    assert order == ["pub:before", "publish", "pub:after"]
    assert outcome.ok
