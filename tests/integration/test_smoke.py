"""Integration smoke tests — full wiring through TestBroker.

No RabbitMQ required. These tests exercise the complete rabbitkit stack
(registration → topology → pipeline → handler → ack) using the in-memory
TestBroker and TestApp.
"""

from __future__ import annotations

from typing import Annotated

import pytest

from rabbitkit.core.app import AppState, RabbitApp
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.router import RabbitRouter
from rabbitkit.core.topology import RabbitExchange
from rabbitkit.core.types import AckPolicy, ExchangeType
from rabbitkit.di.context import Context, ContextRepo, Header
from rabbitkit.di.depends import Depends
from rabbitkit.di.resolver import DIResolver
from rabbitkit.testing.app import TestApp
from rabbitkit.testing.broker import TestBroker

# ── Sync smoke tests ──────────────────────────────────────────────────────


class TestSyncSmoke:
    """End-to-end sync wiring through the in-memory TestBroker."""

    def test_publish_and_consume_roundtrip(self) -> None:
        """Register a handler, publish a message, verify handler is called."""
        broker = TestBroker()
        received: list[bytes] = []

        @broker.subscriber(queue="orders")
        def handle_order(body: bytes) -> None:
            received.append(body)

        broker.start()
        broker.publish("orders", b'{"id": 1}')

        assert len(received) == 1
        assert received[0] == b'{"id": 1}'
        handle_order.mock.assert_called_once()

    def test_multiple_routes(self) -> None:
        """Two handlers on different queues receive only their messages."""
        broker = TestBroker()
        orders: list[bytes] = []
        payments: list[bytes] = []

        @broker.subscriber(queue="orders")
        def handle_orders(body: bytes) -> None:
            orders.append(body)

        @broker.subscriber(queue="payments")
        def handle_payments(body: bytes) -> None:
            payments.append(body)

        broker.start()
        broker.publish("orders", b"order-1")
        broker.publish("payments", b"payment-1")

        assert orders == [b"order-1"]
        assert payments == [b"payment-1"]

    def test_handler_with_exchange(self) -> None:
        """Handler with explicit exchange is registered correctly."""
        broker = TestBroker()
        received: list[bytes] = []
        exchange = RabbitExchange(name="events", type=ExchangeType.TOPIC)

        @broker.subscriber(queue="events-q", exchange=exchange, routing_key="order.*")
        def handle(body: bytes) -> None:
            received.append(body)

        broker.start()

        assert "events" in broker.declared_exchanges
        assert broker.declared_exchanges["events"].type == ExchangeType.TOPIC
        assert "events-q" in broker.declared_queues

        broker.publish("events-q", b"topic-msg")
        assert received == [b"topic-msg"]

    def test_include_router(self) -> None:
        """Routes from a RabbitRouter are included correctly."""
        broker = TestBroker()
        router = RabbitRouter(prefix="v1")
        received: list[bytes] = []

        @router.subscriber(queue="v1-queue", routing_key="created")
        def handle(body: bytes) -> None:
            received.append(body)

        broker.include_router(router)
        broker.start()

        assert len(broker.routes) == 1
        broker.publish("v1-queue", b"routed")
        assert received == [b"routed"]

    def test_consumed_messages_tracked(self) -> None:
        """consumed_messages property captures processed messages."""
        broker = TestBroker()

        @broker.subscriber(queue="track-q")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("track-q", b"msg-1")
        broker.publish("track-q", b"msg-2")

        assert len(broker.consumed_messages) == 2
        assert broker.consumed_messages[0].body == b"msg-1"
        assert broker.consumed_messages[1].body == b"msg-2"

    def test_reset_clears_state(self) -> None:
        """reset() clears published/consumed messages and mock state."""
        broker = TestBroker()

        @broker.subscriber(queue="reset-q")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("reset-q", b"before-reset")

        assert len(broker.consumed_messages) == 1
        handle.mock.assert_called_once()

        broker.reset()

        assert len(broker.consumed_messages) == 0
        handle.mock.assert_not_called()

    def test_publish_to_unknown_queue_raises(self) -> None:
        """Publishing to a non-existent queue raises ValueError."""
        broker = TestBroker()
        broker.start()

        with pytest.raises(ValueError, match="No subscriber registered"):
            broker.publish("nonexistent", b"data")


# ── Async smoke tests ─────────────────────────────────────────────────────


class TestAsyncSmoke:
    """End-to-end async wiring through the in-memory TestBroker."""

    async def test_async_publish_and_consume(self) -> None:
        """Async handler receives messages via publish_async."""
        broker = TestBroker()
        received: list[bytes] = []

        @broker.subscriber(queue="async-orders")
        async def handle_order(body: bytes) -> None:
            received.append(body)

        broker.start()
        await broker.publish_async("async-orders", b'{"async": true}')

        assert len(received) == 1
        assert received[0] == b'{"async": true}'

    async def test_async_multiple_routes(self) -> None:
        """Multiple async handlers each receive their own messages."""
        broker = TestBroker()
        queue_a: list[bytes] = []
        queue_b: list[bytes] = []

        @broker.subscriber(queue="async-a")
        async def handle_a(body: bytes) -> None:
            queue_a.append(body)

        @broker.subscriber(queue="async-b")
        async def handle_b(body: bytes) -> None:
            queue_b.append(body)

        broker.start()
        await broker.publish_async("async-a", b"a-msg")
        await broker.publish_async("async-b", b"b-msg")

        assert queue_a == [b"a-msg"]
        assert queue_b == [b"b-msg"]


# ── App lifecycle smoke tests ─────────────────────────────────────────────


class TestAppLifecycle:
    """Test RabbitApp + TestBroker lifecycle integration."""

    def test_sync_lifecycle(self) -> None:
        """TestApp start/stop transitions app through expected states."""
        app = RabbitApp(title="smoke-app")
        broker = TestBroker()

        @broker.subscriber(queue="lifecycle-q")
        def handle(body: bytes) -> None:
            pass

        ta = TestApp(app, broker)

        assert ta.state == AppState.IDLE
        ta.start()
        assert ta.state == AppState.RUNNING
        ta.stop()
        assert ta.state == AppState.STOPPED

    def test_context_manager(self) -> None:
        """TestApp works as a context manager."""
        app = RabbitApp(title="ctx-app")
        broker = TestBroker()

        @broker.subscriber(queue="ctx-q")
        def handle(body: bytes) -> None:
            pass

        with TestApp(app, broker):
            broker.publish("ctx-q", b"in-context")
            handle.mock.assert_called_once()

    async def test_async_lifecycle(self) -> None:
        """TestApp async context manager works correctly."""
        app = RabbitApp(title="async-app")
        broker = TestBroker()

        @broker.subscriber(queue="async-lifecycle-q")
        async def handle(body: bytes) -> None:
            pass

        async with TestApp(app, broker):
            await broker.publish_async("async-lifecycle-q", b"async-ctx")
            handle.mock.assert_called_once()

    def test_startup_hook_runs(self) -> None:
        """on_startup hooks run during TestApp.start()."""
        app = RabbitApp(title="hook-app")
        broker = TestBroker()
        hook_called = False

        @app.on_startup
        def my_startup_hook() -> None:
            nonlocal hook_called
            hook_called = True

        ta = TestApp(app, broker)
        ta.start()

        assert hook_called

    def test_shutdown_hook_runs(self) -> None:
        """on_shutdown hooks run during TestApp.stop()."""
        app = RabbitApp(title="shutdown-app")
        broker = TestBroker()
        hook_called = False

        @app.on_shutdown
        def my_shutdown_hook() -> None:
            nonlocal hook_called
            hook_called = True

        ta = TestApp(app, broker)
        ta.start()
        ta.stop()

        assert hook_called


# ── Message property smoke tests ──────────────────────────────────────────


class TestMessageProperties:
    """Verify message properties flow correctly through the pipeline."""

    def test_message_headers(self) -> None:
        """Custom headers are preserved on the consumed message."""
        broker = TestBroker()

        @broker.subscriber(queue="headers-q")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("headers-q", b"data", headers={"x-tenant": "acme"})

        msg = broker.consumed_messages[0]
        assert msg.headers["x-tenant"] == "acme"

    def test_message_correlation_id(self) -> None:
        """correlation_id is preserved on the consumed message."""
        broker = TestBroker()

        @broker.subscriber(queue="corr-q")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("corr-q", b"data", correlation_id="req-123")

        msg = broker.consumed_messages[0]
        assert msg.correlation_id == "req-123"

    def test_message_reply_to(self) -> None:
        """reply_to is preserved on the consumed message."""
        broker = TestBroker()

        @broker.subscriber(queue="reply-q")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("reply-q", b"data", reply_to="callback-q")

        msg = broker.consumed_messages[0]
        assert msg.reply_to == "callback-q"


# ── DI test helpers (module-level for type-hint resolution) ───────────────
# ``from __future__ import annotations`` turns annotations into strings.
# typing.get_type_hints() resolves them against the module's globals,
# so dependency factories referenced in Annotated[] must live here.


def _di_get_service() -> str:
    return "injected-service"


_di_gen_cleanup_log: list[str] = []


def _di_get_resource():
    _di_gen_cleanup_log.append("opened")
    yield "resource"
    _di_gen_cleanup_log.append("closed")


def _di_get_async_service() -> str:
    return "async-service"


_di_async_gen_cleanup_log: list[str] = []


async def _di_get_async_resource():
    _di_async_gen_cleanup_log.append("opened")
    yield "async-resource"
    _di_async_gen_cleanup_log.append("closed")


# ── Middleware integration tests ──────────────────────────────────────────


class TestMiddlewareIntegration:
    """Test middleware wiring through the full TestBroker stack."""

    def test_handler_with_di_resolution(self) -> None:
        """Handler with Depends() parameter receives injected value."""
        broker = TestBroker(di_resolver=DIResolver())
        received: list[tuple[bytes, str]] = []

        @broker.subscriber(queue="di-test")
        def handle(
            body: bytes,
            svc: Annotated[str, Depends(_di_get_service)],
        ) -> None:
            received.append((body, svc))

        broker.start()
        broker.publish("di-test", b"hello")

        assert len(received) == 1
        assert received[0] == (b"hello", "injected-service")

    def test_handler_with_generator_depends(self) -> None:
        """Generator dependency yields value and cleanup runs after handler."""
        _di_gen_cleanup_log.clear()

        broker = TestBroker(di_resolver=DIResolver())
        received: list[str] = []

        @broker.subscriber(queue="gen-di-test")
        def handle(
            body: bytes,
            res: Annotated[str, Depends(_di_get_resource)],
        ) -> None:
            received.append(res)

        broker.start()
        broker.publish("gen-di-test", b"data")

        assert received == ["resource"]
        assert _di_gen_cleanup_log == ["opened", "closed"]

    def test_handler_with_header_injection(self) -> None:
        """Header() extracts values from message headers."""
        broker = TestBroker(di_resolver=DIResolver())
        received: list[str] = []

        @broker.subscriber(queue="header-test")
        def handle(
            body: bytes,
            tenant: Annotated[str, Header("x-tenant")],
        ) -> None:
            received.append(tenant)

        broker.start()
        broker.publish("header-test", b"data", headers={"x-tenant": "acme"})

        assert received == ["acme"]

    def test_optional_header_with_default_runs_with_default_when_missing(self) -> None:
        """H10: Annotated[str | None, Header(...)] = None -- a message
        missing the header must run the handler with the default, and the
        message must be acked (processed normally), not rejected."""
        broker = TestBroker(di_resolver=DIResolver())
        received: list[str | None] = []

        @broker.subscriber(queue="optional-header-test")
        def handle(
            body: bytes,
            tenant: Annotated[str | None, Header("x-tenant")] = None,
        ) -> None:
            received.append(tenant)

        broker.start()
        broker.publish("optional-header-test", b"data")  # no headers at all

        assert received == [None]
        assert broker.consumed_messages[-1]._disposition == "acked"

    def test_optional_header_marker_default_runs_with_default_when_missing(self) -> None:
        """H10: Header(name, default=...) as an alternative to a function
        default -- same outcome, marker owns the default this time."""
        broker = TestBroker(di_resolver=DIResolver())
        received: list[str] = []

        @broker.subscriber(queue="optional-header-marker-default-test")
        def handle(
            body: bytes,
            tenant: Annotated[str, Header("x-tenant", default="anonymous")],
        ) -> None:
            received.append(tenant)

        broker.start()
        broker.publish("optional-header-marker-default-test", b"data")

        assert received == ["anonymous"]
        assert broker.consumed_messages[-1]._disposition == "acked"

    def test_required_header_missing_rejects_with_typed_error(self) -> None:
        """H10: required (no default anywhere) + missing -> the message is
        rejected via the pipeline's normal exception handling (classified
        PERMANENT, same as before) -- but now driven by a typed
        MissingDependencyError naming the parameter, not a bare KeyError."""
        broker = TestBroker(di_resolver=DIResolver())
        handler_called = False

        @broker.subscriber(queue="required-header-test")
        def handle(
            body: bytes,
            tenant: Annotated[str, Header("x-tenant")],
        ) -> None:
            nonlocal handler_called
            handler_called = True

        broker.start()
        broker.publish("required-header-test", b"data")  # no x-tenant header

        assert not handler_called
        assert broker.consumed_messages[-1]._disposition == "rejected"

    def test_handler_with_context_injection(self) -> None:
        """Context() extracts values from ContextRepo."""
        context_repo = ContextRepo()
        broker = TestBroker(di_resolver=DIResolver(), context_repo=context_repo)
        received: list[str] = []

        @broker.subscriber(queue="ctx-test")
        def handle(
            body: bytes,
            app: Annotated[str, Context("app_name")],
        ) -> None:
            received.append(app)

        broker.start()
        # Set context before publishing
        context_repo.set_global("app_name", "my-app")
        broker.publish("ctx-test", b"data")

        assert received == ["my-app"]


# ── Async middleware integration tests ────────────────────────────────────


class TestAsyncMiddlewareIntegration:
    """Test async middleware wiring through TestBroker."""

    async def test_async_handler_with_di(self) -> None:
        """Async handler with Depends() receives injected value."""
        broker = TestBroker(di_resolver=DIResolver())
        received: list[str] = []

        @broker.subscriber(queue="async-di-test")
        async def handle(
            body: bytes,
            svc: Annotated[str, Depends(_di_get_async_service)],
        ) -> None:
            received.append(svc)

        broker.start()
        await broker.publish_async("async-di-test", b"hello")

        assert received == ["async-service"]

    async def test_async_handler_with_async_generator_depends(self) -> None:
        """Async generator dependency yields value with cleanup."""
        _di_async_gen_cleanup_log.clear()

        broker = TestBroker(di_resolver=DIResolver())
        received: list[str] = []

        @broker.subscriber(queue="async-gen-di-test")
        async def handle(
            body: bytes,
            res: Annotated[str, Depends(_di_get_async_resource)],
        ) -> None:
            received.append(res)

        broker.start()
        await broker.publish_async("async-gen-di-test", b"data")

        assert received == ["async-resource"]
        assert _di_async_gen_cleanup_log == ["opened", "closed"]


# ── Result publishing integration tests ───────────────────────────────────


class TestResultPublishing:
    """Test result publishing through the full stack."""

    def test_publisher_decorator_publishes_result(self) -> None:
        """Handler with @publisher publishes return value."""
        broker = TestBroker()

        @broker.subscriber(queue="source-q")
        @broker.publisher(exchange="", routing_key="target-q")
        def handle(body: bytes) -> bytes:
            return b"processed-" + body

        @broker.subscriber(queue="target-q")
        def receive(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("source-q", b"data")

        # The result was published as an envelope, check published_messages
        assert len(broker.published_messages) == 1
        assert broker.published_messages[0].body == b"processed-data"
        assert broker.published_messages[0].routing_key == "target-q"

    def test_handler_returning_none_no_publish(self) -> None:
        """Handler returning None does not publish anything."""
        broker = TestBroker()

        @broker.subscriber(queue="no-publish-q")
        @broker.publisher(exchange="", routing_key="nowhere")
        def handle(body: bytes) -> None:
            pass  # returns None

        broker.start()
        broker.publish("no-publish-q", b"data")

        # No result published
        assert len(broker.published_messages) == 0
        handle.mock.assert_called_once()


# ── Error handling integration tests ──────────────────────────────────────


class TestErrorHandling:
    """Test error handling through the full pipeline."""

    def test_transient_error_nacks_with_requeue(self) -> None:
        """Transient error in AUTO mode nacks with requeue."""
        broker = TestBroker()

        @broker.subscriber(queue="transient-err-q")
        def handle(body: bytes) -> None:
            raise ConnectionResetError("transient!")

        broker.start()
        broker.publish("transient-err-q", b"data")

        msg = broker.consumed_messages[0]
        assert msg._disposition == "nacked"

    def test_permanent_error_rejects(self) -> None:
        """Permanent error in AUTO mode rejects without requeue."""
        broker = TestBroker()

        @broker.subscriber(queue="permanent-err-q")
        def handle(body: bytes) -> None:
            raise ValueError("bad data!")

        broker.start()
        broker.publish("permanent-err-q", b"data")

        msg = broker.consumed_messages[0]
        assert msg._disposition == "rejected"


# ── Worker pool integration tests ──────────────────────────────────────


class TestWorkerPoolIntegration:
    """Integration tests for WorkerPool with TestBroker."""

    def test_sync_worker_pool_processes_messages(self) -> None:
        """SyncWorkerPool processes messages through callback."""
        from rabbitkit.concurrency import SyncWorkerPool
        from rabbitkit.core.config import WorkerConfig
        from rabbitkit.core.message import RabbitMessage

        received: list[bytes] = []

        def callback(msg: RabbitMessage) -> None:
            received.append(msg.body)

        pool = SyncWorkerPool(config=WorkerConfig(worker_count=2))
        pool.start()

        for i in range(3):
            msg = RabbitMessage(body=f"msg-{i}".encode(), routing_key="test")
            pool.submit(callback, msg)

        pool.stop()

        assert len(received) == 3
        assert set(received) == {b"msg-0", b"msg-1", b"msg-2"}

    async def test_async_worker_pool_processes_messages(self) -> None:
        """AsyncWorkerPool processes messages through async callback."""
        from rabbitkit.concurrency import AsyncWorkerPool
        from rabbitkit.core.config import WorkerConfig
        from rabbitkit.core.message import RabbitMessage

        received: list[bytes] = []

        async def callback(msg: RabbitMessage) -> None:
            received.append(msg.body)

        pool = AsyncWorkerPool(config=WorkerConfig(worker_count=2))
        pool.start()

        for i in range(3):
            msg = RabbitMessage(body=f"async-msg-{i}".encode(), routing_key="test")
            await pool.submit(callback, msg)

        await pool.stop()

        assert len(received) == 3
        assert set(received) == {b"async-msg-0", b"async-msg-1", b"async-msg-2"}


# ── Compression middleware integration tests ───────────────────────────


class TestMultipleMiddleware:
    """Integration tests combining compression middleware."""

    def test_compression_roundtrip(self) -> None:
        """Message compressed on publish, decompressed on consume."""
        from rabbitkit.core.config import CompressionConfig
        from rabbitkit.middleware.compression import CompressionMiddleware

        mw = CompressionMiddleware(CompressionConfig(threshold=0))

        original = b"hello world -- this is a test of the compression middleware"
        compressed, encoding = mw.compress(original)

        assert encoding == "gzip"
        assert compressed != original

        decompressed = mw.decompress(compressed, encoding)
        assert decompressed == original

    async def test_async_compression_roundtrip(self) -> None:
        """Async compression roundtrip using on_receive_async."""
        from rabbitkit.core.config import CompressionConfig
        from rabbitkit.core.message import RabbitMessage
        from rabbitkit.middleware.compression import CompressionMiddleware

        mw = CompressionMiddleware(CompressionConfig(threshold=0))

        original = b"async test data for compression middleware roundtrip"
        compressed, encoding = mw.compress(original)

        assert encoding is not None

        # Simulate incoming compressed message
        msg = RabbitMessage(
            body=compressed,
            routing_key="test",
            content_encoding=encoding,
        )

        await mw.on_receive_async(msg)
        assert msg.body == original


# ── Ack policy integration tests ───────────────────────────────────────


class TestAckPolicies:
    """Integration tests for different ack policies."""

    def test_manual_ack_handler_acks(self) -> None:
        """MANUAL policy: handler calls msg.ack() explicitly and it takes
        effect. (M11: the pipeline itself never auto-acks on success under
        MANUAL -- the handler owns settlement entirely.)"""
        broker = TestBroker(di_resolver=DIResolver())
        ack_recorded: list[bool] = []

        @broker.subscriber(queue="manual-ack-q", ack_policy=AckPolicy.MANUAL)
        def handle(body: bytes, msg: RabbitMessage) -> None:
            msg.ack()
            ack_recorded.append(True)

        broker.start()
        broker.publish("manual-ack-q", b"data")

        assert ack_recorded == [True]
        consumed = broker.consumed_messages[0]
        assert consumed._disposition == "acked"

    def test_manual_ack_handler_defers_settlement_stays_pending(self) -> None:
        """M11: a MANUAL handler that returns WITHOUT settling must be left
        unsettled -- previously the pipeline auto-acked here, which is a
        real loss risk for a handler that intentionally defers settlement
        (e.g. hands the message to another task/thread to ack later) if the
        process crashes before that deferred ack actually runs."""
        broker = TestBroker(di_resolver=DIResolver())

        @broker.subscriber(queue="manual-defer-q", ack_policy=AckPolicy.MANUAL)
        def handle(body: bytes, msg: RabbitMessage) -> None:
            pass  # intentionally defers settlement -- does not ack here

        broker.start()
        broker.publish("manual-defer-q", b"data")

        consumed = broker.consumed_messages[0]
        assert consumed._disposition == "pending"

    def test_nack_on_error_success(self) -> None:
        """NACK_ON_ERROR: success -> ack."""
        broker = TestBroker()

        @broker.subscriber(queue="noe-success-q", ack_policy=AckPolicy.NACK_ON_ERROR)
        def handle(body: bytes) -> None:
            pass  # success path

        broker.start()
        broker.publish("noe-success-q", b"data")

        msg = broker.consumed_messages[0]
        assert msg._disposition == "acked"

    def test_nack_on_error_failure(self) -> None:
        """NACK_ON_ERROR: exception -> nack(requeue=False)."""
        broker = TestBroker()

        @broker.subscriber(queue="noe-fail-q", ack_policy=AckPolicy.NACK_ON_ERROR)
        def handle(body: bytes) -> None:
            raise ValueError("handler failed")

        broker.start()
        broker.publish("noe-fail-q", b"data")

        msg = broker.consumed_messages[0]
        assert msg._disposition == "nacked"

    def test_ack_first_policy(self) -> None:
        """ACK_FIRST: message acked before handler runs."""
        broker = TestBroker()

        @broker.subscriber(queue="ack-first-q", ack_policy=AckPolicy.ACK_FIRST)
        def handle(body: bytes) -> None:
            # The message should already be acked by the time the handler runs.
            # We can't inspect the message from here without DI, but we can
            # verify after the fact that the final disposition is "acked".
            pass

        broker.start()
        broker.publish("ack-first-q", b"data")

        msg = broker.consumed_messages[0]
        assert msg._disposition == "acked"


# ── Router integration tests ──────────────────────────────────────────


class TestRouterIntegration:
    """Integration tests for RabbitRouter with TestBroker."""

    def test_router_prefix(self) -> None:
        """Router with prefix correctly sets routing key on routes."""
        broker = TestBroker()
        router = RabbitRouter(prefix="orders")
        received: list[bytes] = []

        @router.subscriber(queue="orders-prefix-q", routing_key="created")
        def handle(body: bytes) -> None:
            received.append(body)

        broker.include_router(router)
        broker.start()

        # Verify the route's effective routing key includes the prefix
        route = broker.routes[0]
        assert route.queue.routing_key == "orders.created"

        broker.publish("orders-prefix-q", b"order-data")
        assert received == [b"order-data"]

    def test_include_router(self) -> None:
        """include_router includes routes from sub-router."""
        broker = TestBroker()
        sub_router = RabbitRouter()
        received_a: list[bytes] = []
        received_b: list[bytes] = []

        @sub_router.subscriber(queue="sub-a-q")
        def handle_a(body: bytes) -> None:
            received_a.append(body)

        @sub_router.subscriber(queue="sub-b-q")
        def handle_b(body: bytes) -> None:
            received_b.append(body)

        broker.include_router(sub_router)
        broker.start()

        assert len(broker.routes) == 2

        broker.publish("sub-a-q", b"a-msg")
        broker.publish("sub-b-q", b"b-msg")

        assert received_a == [b"a-msg"]
        assert received_b == [b"b-msg"]


# ── M2: settlement/retry/dead-letter metrics, end-to-end through a broker ──


class TestMetricsEndToEnd:
    """M2: messages_acked/nacked/rejected/retried/dead_lettered_total must
    actually be emitted through a full broker start()/publish() cycle, not
    just when calling pipeline internals directly."""

    def test_ack_emits_through_full_broker_cycle(self) -> None:
        from unittest.mock import MagicMock

        from rabbitkit.middleware.metrics import MetricsMiddleware

        collector = MagicMock()
        broker = TestBroker()

        @broker.subscriber(queue="metrics-ack-q", middlewares=[MetricsMiddleware(collector)])
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("metrics-ack-q", b"data")

        collector.inc_counter.assert_any_call(
            "rabbitkit_messages_acked_total", {"queue": "metrics-ack-q"}
        )

    def test_retry_then_dead_letter_emits_through_full_broker_cycle(self) -> None:
        from unittest.mock import MagicMock

        from rabbitkit.core.config import RetryConfig
        from rabbitkit.middleware.metrics import MetricsMiddleware

        collector = MagicMock()
        broker = TestBroker()
        retry_cfg = RetryConfig(max_retries=1, delays=(5,))

        @broker.subscriber(
            queue="metrics-retry-q",
            retry=retry_cfg,
            middlewares=[MetricsMiddleware(collector)],
        )
        def handle(body: bytes) -> None:
            raise ConnectionResetError("transient outage")  # always fails

        broker.start()

        # First delivery (retry_count=0 < max_retries=1): routed to the delay queue.
        broker.publish("metrics-retry-q", b"data")
        collector.inc_counter.assert_any_call(
            "rabbitkit_messages_retried_total", {"queue": "metrics-retry-q"}
        )

        # A redelivery already at retry_count=1 (== max_retries): exhausted -> dead-lettered.
        broker.publish("metrics-retry-q", b"data", headers={"x-rabbitkit-retry-count": 1})
        collector.inc_counter.assert_any_call(
            "rabbitkit_messages_dead_lettered_total", {"queue": "metrics-retry-q"}
        )
