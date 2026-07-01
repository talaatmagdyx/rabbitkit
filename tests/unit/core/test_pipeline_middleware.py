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
    outcome = chain(MessageEnvelope(routing_key="x", body=b"{}"), publish_fn)

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
    outcome = await chain(MessageEnvelope(routing_key="x", body=b"{}"), publish_fn)

    assert order == ["pub:before", "publish", "pub:after"]
    assert outcome.ok


def test_compression_middleware_compresses_handler_result_publish() -> None:
    """C4: CompressionMiddleware (not a generic probe) actually compresses a
    handler's @publisher result via the real publish_scope chain end-to-end,
    through TestBroker. Before wiring publish_scope, this middleware's
    transform_envelope() had no caller anywhere in the pipeline."""
    import gzip

    from rabbitkit.core.config import CompressionConfig
    from rabbitkit.middleware.compression import CompressionMiddleware

    compression_mw = CompressionMiddleware(CompressionConfig(algorithm="gzip", threshold=0))
    broker = TestBroker()

    large_body = b"order-payload " * 200

    @broker.subscriber(queue="source-q", middlewares=[compression_mw])
    @broker.publisher(exchange="", routing_key="target-q")
    def handle(body: bytes) -> bytes:
        return large_body

    broker.start()
    broker.publish("source-q", b"trigger")

    assert len(broker.published_messages) == 1
    published = broker.published_messages[0]
    assert published.content_encoding == "gzip"
    assert published.body != large_body
    assert gzip.decompress(published.body) == large_body


# ── M-P1: middleware chain cached per route ───────────────────────────────


class TestChainCaching:
    def test_consume_chain_cached_per_route(self) -> None:
        """The composed middleware chain is built once per route, not per message."""
        from rabbitkit.core.message import RabbitMessage
        from rabbitkit.core.registry import SubscriberRegistry
        from rabbitkit.core.types import AckPolicy

        calls: list[str] = []

        class TaggingMiddleware(BaseMiddleware):
            def consume_scope(self, call_next, message):  # type: ignore[no-untyped-def]
                calls.append("mw")
                return call_next(message)

        pipeline = HandlerPipeline()
        reg = SubscriberRegistry()

        @reg.subscriber(queue="q1", middlewares=[TaggingMiddleware()], ack_policy=AckPolicy.MANUAL)
        def handle(msg: RabbitMessage) -> None:
            pass

        route = reg.routes[0]

        def _make_msg() -> RabbitMessage:
            m = RabbitMessage(body=b"{}", routing_key="q1")
            m._ack_fn = lambda: None  # wire a no-op sync ack so the pipeline can settle
            return m

        pipeline.process_sync(route, _make_msg())
        assert id(route) in pipeline._consume_chain_cache
        pipeline.process_sync(route, _make_msg())
        # Cache hit: the chain is reused (mw still called per message, but the
        # closure object is the same — verify the cache entry is stable).
        assert calls.count("mw") == 2
        assert len(pipeline._consume_chain_cache) == 1
