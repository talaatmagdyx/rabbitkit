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


# ── H7: on_receive ordering and exception-interception semantics ─────────


def _on_receive_probe(order: list[str], name: str) -> BaseMiddleware:
    class _M(BaseMiddleware):
        def on_receive(self, message: Any) -> None:
            order.append(name)

        async def on_receive_async(self, message: Any) -> None:
            order.append(name)

    return _M()


def _publish_through_middlewares(middlewares: list[Any], envelope: MessageEnvelope) -> MessageEnvelope:
    """Apply middlewares' publish_scope OUTER→INNER (mirrors the pipeline's
    own composition — see HandlerPipeline._compose_publish_sync): the first
    item in the list transforms first, its result is what the next
    middleware transforms, and so on. Returns the final transformed envelope."""

    def call_next(env: MessageEnvelope) -> MessageEnvelope:
        return env  # innermost "publish step" — just hand back the envelope

    for mw in reversed(middlewares):
        nxt = call_next

        def wrapped(env: MessageEnvelope, _mw: Any = mw, _nxt: Any = nxt) -> MessageEnvelope:
            return _mw.publish_scope(_nxt, env)  # type: ignore[no-any-return]

        call_next = wrapped

    return call_next(envelope)


def test_on_receive_runs_in_reverse_registration_order() -> None:
    """H7: on_receive hooks run in the REVERSE of middlewares=[...]'s
    registration order — the mirror of publish_scope's outer→inner
    composition — unlike consume_scope, which runs OUTER→INNER (forward),
    already covered by test_sync_middlewares_execute_outer_to_inner above."""
    order: list[str] = []
    broker = TestBroker()

    @broker.subscriber(queue="q", middlewares=[_on_receive_probe(order, "A"), _on_receive_probe(order, "B")])
    def handler(body: bytes) -> None:
        pass

    broker.start()
    broker.publish("q", b"{}")

    assert order == ["B", "A"]


async def test_on_receive_async_runs_in_reverse_registration_order() -> None:
    order: list[str] = []
    broker = TestBroker()

    @broker.subscriber(queue="q", middlewares=[_on_receive_probe(order, "A"), _on_receive_probe(order, "B")])
    async def handler(body: bytes) -> None:
        pass

    broker.start()
    await broker.publish_async("q", b"{}")

    assert order == ["B", "A"]


class TestSigningCompressionComposition:
    """H7: the canonical, working order is CompressionMiddleware OUTER (listed
    first), SigningMiddleware INNER (listed second) —
    ``middlewares=[compression_mw, signing_mw]``. This is NOT arbitrary:
    SigningMiddleware's signature covers ``content_encoding`` (H3), which
    CompressionMiddleware's publish_scope is what actually SETS — if signing
    runs first (outer), it signs ``content_encoding=None`` (unset at that
    point) while compression sets it to e.g. "gzip" afterward, so the
    delivered message's content_encoding never matches what was signed and
    verification always fails, regardless of the on_receive ordering fix
    below. Compression outer / signing inner is the only order where signing
    sees the FINAL content_encoding it needs to sign correctly.

    Making on_receive run in REVERSE registration order (this fix's other
    half) is what makes even the correct order work at all: without it,
    on_receive ran in the same (forward) order as publish_scope's apply
    order, so decompression would run BEFORE verification instead of after —
    verifying against the wrong (already-decompressed) bytes. Both halves of
    the fix are required together for the canonical order to actually work.
    """

    SECRET = "h7-test-secret"

    def _make_middlewares(self) -> tuple[Any, Any]:
        from rabbitkit.core.config import CompressionConfig
        from rabbitkit.middleware.compression import CompressionMiddleware
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        signing_mw = SigningMiddleware(SigningConfig(secret_key=self.SECRET, require_freshness=True))
        compression_mw = CompressionMiddleware(CompressionConfig(algorithm="gzip", threshold=0))
        return signing_mw, compression_mw

    def _round_trip(self, middlewares: list[Any]) -> tuple[list[bytes], Any]:
        """Publish `original` through *middlewares* (outer→inner), then feed
        the resulting wire envelope through the SAME middlewares (as route
        middlewares) on the consume side. Returns (bodies the handler saw,
        the settled RabbitMessage) — process_sync catches on_receive/handler
        exceptions internally and settles the message rather than
        re-raising, so callers check disposition, not a raised exception."""
        from rabbitkit.core.message import RabbitMessage
        from rabbitkit.core.registry import SubscriberRegistry
        from rabbitkit.core.types import AckPolicy

        original = MessageEnvelope(routing_key="q", body=b"hello world " * 50, exchange="")
        wire = _publish_through_middlewares(middlewares, original)

        received_bodies: list[bytes] = []
        reg = SubscriberRegistry()

        @reg.subscriber(queue="q", middlewares=middlewares, ack_policy=AckPolicy.AUTO)
        def handle(body: bytes) -> None:
            received_bodies.append(body)

        route = reg.routes[0]
        pipeline = HandlerPipeline()

        message = RabbitMessage(
            body=wire.body,
            headers=dict(wire.headers),
            routing_key=wire.routing_key,
            exchange=wire.exchange,
            content_encoding=wire.content_encoding,
            reply_to=wire.reply_to,
        )
        message._ack_fn = lambda: None
        message._nack_fn = lambda requeue=True: None
        message._reject_fn = lambda requeue=False: None

        pipeline.process_sync(route, message)
        return received_bodies, message

    def test_compress_outer_sign_inner_composes_correctly(self) -> None:
        """The canonical, documented order: compression outer, signing inner."""
        signing_mw, compression_mw = self._make_middlewares()
        received, message = self._round_trip([compression_mw, signing_mw])
        assert received == [b"hello world " * 50]
        assert message._disposition == "acked"

    def test_sign_outer_compress_inner_fails_verification(self) -> None:
        """The WRONG (undocumented) order fails predictably, not silently —
        signing captures content_encoding=None (compression hasn't run yet),
        but the delivered message has content_encoding="gzip" (compression
        already ran on publish), so verification always fails. Pinning the
        canonical order (see the class docstring) means this failure mode is
        documented and avoidable rather than a mystery. process_sync catches
        the InvalidSignatureError internally (see
        test_on_receive_exception_bypasses_retry_middleware) rather than
        raising it, so the handler simply never runs and the message rejects."""
        signing_mw, compression_mw = self._make_middlewares()
        received, message = self._round_trip([signing_mw, compression_mw])
        assert received == []
        assert message._disposition == "rejected"


def test_on_receive_exception_bypasses_retry_middleware() -> None:
    """H7's exact regression spec: Route [retry_mw, failing_signing_mw] — a
    signing verification failure must NOT be routed through
    RetryMiddleware's delay-queue mechanism. on_receive hooks run in a flat
    pre-pass entirely before consume_scope is entered, so RetryMiddleware's
    consume_scope (which wraps call_next in a try/except) never sees this
    exception — it settles per the route's AckPolicy directly instead."""
    from unittest.mock import MagicMock

    from rabbitkit.core.config import RetryConfig
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.registry import SubscriberRegistry
    from rabbitkit.core.types import AckPolicy
    from rabbitkit.middleware.retry import RetryMiddleware
    from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

    retry_published: list[MessageEnvelope] = []
    retry_mw = RetryMiddleware(
        RetryConfig(max_retries=3, delays=(5, 30, 120)),
        publish_fn=lambda env: retry_published.append(env),
    )
    # No signature header on the incoming message + reject_unsigned=True ->
    # on_receive raises InvalidSignatureError.
    signing_mw = SigningMiddleware(SigningConfig(secret_key="s3cr3t", reject_unsigned=True))

    reg = SubscriberRegistry()

    @reg.subscriber(queue="q", middlewares=[retry_mw, signing_mw], ack_policy=AckPolicy.AUTO)
    def handle(body: bytes) -> None:
        raise AssertionError("handler must never run when on_receive rejects the message")

    route = reg.routes[0]
    pipeline = HandlerPipeline()

    message = RabbitMessage(body=b"unsigned-payload", routing_key="q", headers={})
    message._ack_fn = MagicMock()
    message._nack_fn = MagicMock()
    message._reject_fn = MagicMock()

    pipeline.process_sync(route, message)

    # Explicitly NOT routed through retry: RetryMiddleware's delay-queue
    # publish was never invoked, since its consume_scope never ran.
    assert retry_published == []
    # Settled per AckPolicy's default classifier instead (InvalidSignatureError
    # falls to unknown_policy=PERMANENT) -> reject(requeue=False), not acked.
    assert message._disposition == "rejected"
    message._reject_fn.assert_called_once_with(False)
    message._ack_fn.assert_not_called()
