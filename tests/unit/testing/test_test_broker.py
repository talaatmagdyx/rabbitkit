"""Tests for testing/broker.py — TestBroker."""

from __future__ import annotations

import pytest

from rabbitkit.core.topology import RabbitExchange
from rabbitkit.testing.broker import TestBroker

# ── Registration ─────────────────────────────────────────────────────────


class TestRegistration:
    def test_subscriber_registers_route(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        assert len(broker.routes) == 1
        assert broker.routes[0].queue.name == "orders"

    def test_subscriber_attaches_mock(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        assert hasattr(handle, "mock")

    def test_multiple_subscribers(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle_orders(body: bytes) -> None:
            pass

        @broker.subscriber(queue="payments")
        def handle_payments(body: bytes) -> None:
            pass

        assert len(broker.routes) == 2

    def test_subscriber_with_exchange(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders", exchange="events")
        def handle(body: bytes) -> None:
            pass

        route = broker.routes[0]
        assert route.exchange is not None
        assert route.exchange.name == "events"

    def test_subscriber_with_exchange_object(self) -> None:
        broker = TestBroker()
        ex = RabbitExchange(name="events")

        @broker.subscriber(queue="orders", exchange=ex)
        def handle(body: bytes) -> None:
            pass

        route = broker.routes[0]
        assert route.exchange is ex

    def test_publisher_decorator(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        def handle(body: bytes) -> str:
            return "ok"

        route = broker.routes[0]
        assert route.result_publisher is not None
        assert route.result_publisher.routing_key == "done"

    def test_duplicate_queue_raises(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle1(body: bytes) -> None:
            pass

        with pytest.raises(Exception, match="already has a registered handler"):

            @broker.subscriber(queue="orders")
            def handle2(body: bytes) -> None:
                pass


# ── Lifecycle ────────────────────────────────────────────────────────────


class TestLifecycle:
    def test_start_records_topology(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders", exchange="events")
        def handle(body: bytes) -> None:
            pass

        broker.start()

        assert "orders" in broker.declared_queues
        assert "events" in broker.declared_exchanges

    def test_stop(self) -> None:
        broker = TestBroker()
        broker.start()
        broker.stop()
        # No error

    def test_reset_clears_state(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("orders", b"hello")

        assert len(broker.consumed_messages) == 1

        broker.reset()

        assert len(broker.consumed_messages) == 0
        assert len(broker.published_messages) == 0
        handle.mock.assert_not_called()


# ── Publish & handler execution ──────────────────────────────────────────


class TestPublish:
    def test_publish_calls_handler(self) -> None:
        broker = TestBroker()
        called = False

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            nonlocal called
            called = True

        broker.start()
        broker.publish("orders", b'{"id": 1}')

        assert called

    def test_publish_records_mock(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("orders", b"hello")

        handle.mock.assert_called_once_with(b"hello")

    def test_publish_captures_consumed_message(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("orders", b"hello")

        assert len(broker.consumed_messages) == 1
        assert broker.consumed_messages[0].body == b"hello"

    def test_publish_with_headers(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("orders", b"hello", headers={"x-tenant": "acme"})

        assert broker.consumed_messages[0].headers["x-tenant"] == "acme"

    def test_publish_unknown_queue_raises(self) -> None:
        broker = TestBroker()
        broker.start()

        with pytest.raises(ValueError, match="No subscriber registered"):
            broker.publish("unknown-queue", b"hello")

    def test_multiple_publishes(self) -> None:
        broker = TestBroker()
        count = 0

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            nonlocal count
            count += 1

        broker.start()
        broker.publish("orders", b"msg1")
        broker.publish("orders", b"msg2")
        broker.publish("orders", b"msg3")

        assert count == 3
        assert handle.mock.call_count == 3


# ── Result publishing ────────────────────────────────────────────────────


class TestResultPublishing:
    def test_handler_return_publishes_result(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        def handle(body: bytes) -> str:
            return "processed"

        broker.start()
        broker.publish("orders", b"hello")

        assert len(broker.published_messages) == 1
        result_msg = broker.published_messages[0]
        assert result_msg.routing_key == "done"

    def test_handler_none_return_no_publish(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("orders", b"hello")

        assert len(broker.published_messages) == 0


# ── Async ────────────────────────────────────────────────────────────────


class TestAsyncPublish:
    @pytest.mark.asyncio
    async def test_async_publish(self) -> None:
        broker = TestBroker()
        called = False

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            nonlocal called
            called = True

        broker.start()
        await broker.publish_async("orders", b"hello")

        assert called

    @pytest.mark.asyncio
    async def test_async_publish_records_mock(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        broker.start()
        await broker.publish_async("orders", b"hello")

        handle.mock.assert_called_once_with(b"hello")

    @pytest.mark.asyncio
    async def test_async_publish_unknown_queue_raises(self) -> None:
        broker = TestBroker()
        broker.start()

        with pytest.raises(ValueError, match="No subscriber registered"):
            await broker.publish_async("unknown", b"hello")


# ── Include router ───────────────────────────────────────────────────────


class TestIncludeRouter:
    def test_include_router(self) -> None:
        from rabbitkit.core.router import RabbitRouter

        router = RabbitRouter(prefix="orders")

        @router.subscriber(queue="orders-queue", routing_key="created")
        def handle(body: bytes) -> None:
            pass

        broker = TestBroker()
        broker.include_router(router)

        assert len(broker.routes) == 1
        assert broker.routes[0].queue.name == "orders-queue"


# ── start() attaches mock to handler that lacks one ──────────────────────


class TestStartAttachesMock:
    def test_start_attaches_mock_when_handler_has_none(self) -> None:
        """start() attaches .mock to handlers that don't already have one.

        This covers the branch at line 151 where the handler registered
        via the registry directly has no .mock attribute yet.
        """

        # Build a broker but bypass the subscriber() wrapper so the handler
        # has NO .mock attribute at start time.
        broker = TestBroker()

        # Register directly through the underlying registry (no mock attached)
        @broker._registry.subscriber(queue="raw-queue")
        def raw_handler(body: bytes) -> None:
            pass

        assert not hasattr(raw_handler, "mock")

        broker.start()

        # After start(), the handler should have a mock
        assert hasattr(raw_handler, "mock")


# ── publish_async inner async functions coverage ─────────────────────────


class TestPublishAsyncInternals:
    async def test_async_nack_and_reject_are_callable(self) -> None:
        """Exercise async_nack and async_reject inner functions in publish_async.

        Normally these are never called directly but they need to be defined.
        We access them by inspecting the message after publish_async sets them.
        """
        broker = TestBroker()

        @broker.subscriber(queue="q-async-inner")
        async def handle(body: bytes) -> None:
            pass

        broker.start()
        await broker.publish_async("q-async-inner", b"test")

        # The consumed message should have the async fn references set
        assert len(broker.consumed_messages) == 1
        msg = broker.consumed_messages[0]
        # Call the async ack/nack/reject to ensure they are covered
        await msg._ack_async_fn()
        await msg._nack_async_fn(requeue=True)
        await msg._reject_async_fn(requeue=False)

    async def test_async_publish_records_published(self) -> None:
        """publish_async's test_publish_fn stores results in _published."""
        broker = TestBroker()

        @broker.subscriber(queue="pub-async-q")
        @broker.publisher(exchange="out", routing_key="done")
        async def handle(body: bytes) -> str:
            return "result"

        broker.start()
        await broker.publish_async("pub-async-q", b"data")

        assert len(broker.published_messages) == 1


class TestFixturesImport:
    def test_fixtures_module_is_importable(self) -> None:
        """Covers testing/fixtures.py lines 7-15 — module-level imports."""
        import rabbitkit.testing.fixtures as fixtures_module

        assert hasattr(fixtures_module, "test_broker")
        assert hasattr(fixtures_module, "test_app")


# ── Real settlement + injectable publish outcome (M-C4 / Core-M6) ────────


class TestRealSettlement:
    """Settlement now goes through the real RabbitMessage methods, so
    ``is_settled`` / ``_disposition`` reflect reality and the transport-level
    ack/nack/reject calls are recorded (with requeue) for the assert helpers.
    """

    def test_auto_success_acks_and_records(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("orders", b"hello")

        msg = broker.consumed_messages[0]
        assert msg.is_settled is True
        broker.assert_acked(msg)
        records = broker.settlements_for(msg)
        assert any(r.kind == "ack" for r in records)

    def test_manual_nack_recorded_with_requeue(self) -> None:
        from rabbitkit.core.message import NackMessage
        from rabbitkit.core.types import AckPolicy

        broker = TestBroker()

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(body: bytes) -> None:
            raise NackMessage(requeue=True)

        broker.start()
        broker.publish("orders", b"hello")

        msg = broker.consumed_messages[0]
        broker.assert_nacked(msg, requeue=True)
        records = broker.settlements_for(msg)
        assert any(r.kind == "nack" and r.requeue for r in records)

    def test_manual_reject_recorded_no_requeue(self) -> None:
        from rabbitkit.core.message import RejectMessage
        from rabbitkit.core.types import AckPolicy

        broker = TestBroker()

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(body: bytes) -> None:
            raise RejectMessage(requeue=False)

        broker.start()
        broker.publish("orders", b"hello")

        msg = broker.consumed_messages[0]
        broker.assert_rejected(msg, requeue=False)

    def test_nack_requeue_false_assert_helper_fails_on_mismatch(self) -> None:
        from rabbitkit.core.message import NackMessage
        from rabbitkit.core.types import AckPolicy

        broker = TestBroker()

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(body: bytes) -> None:
            raise NackMessage(requeue=False)

        broker.start()
        broker.publish("orders", b"hello")

        msg = broker.consumed_messages[0]
        # disposition is nacked, requeue is False — assert_nacked(default True) must fail
        with pytest.raises(AssertionError, match="requeue"):
            broker.assert_nacked(msg, requeue=True)
        broker.assert_nacked(msg, requeue=False)


class TestPublishOutcomeInjection:
    """The failed-publish → nack(requeue=True) pipeline branch is now reachable."""

    def test_fail_next_publish_triggers_nack_requeue(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        def handle(body: bytes) -> str:
            return "processed"

        broker.start()
        broker.fail_next_publish()
        broker.publish("orders", b"hello")

        msg = broker.consumed_messages[0]
        # Result publish failed → pipeline nacks with requeue=True for redelivery
        broker.assert_nacked(msg, requeue=True)
        # The publish was still captured (the broker records the attempt)
        assert len(broker.published_messages) == 1

    def test_fail_next_publish_is_one_shot(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        def handle(body: bytes) -> str:
            return "processed"

        broker.start()
        broker.fail_next_publish()
        broker.publish("orders", b"msg1")  # fails → nacked
        broker.publish("orders", b"msg2")  # succeeds → acked

        broker.assert_nacked(broker.consumed_messages[0], requeue=True)
        broker.assert_acked(broker.consumed_messages[1])

    def test_persistent_publish_outcome(self) -> None:
        from rabbitkit.core.types import PublishOutcome, PublishStatus

        broker = TestBroker()
        broker.publish_outcome = PublishOutcome(status=PublishStatus.NACKED)

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        def handle(body: bytes) -> str:
            return "processed"

        broker.start()
        broker.publish("orders", b"msg1")
        broker.publish("orders", b"msg2")

        # Both fail until the override is cleared
        broker.assert_nacked(broker.consumed_messages[0], requeue=True)
        broker.assert_nacked(broker.consumed_messages[1], requeue=True)

        broker.publish_outcome = None
        broker.publish("orders", b"msg3")
        broker.assert_acked(broker.consumed_messages[2])

    def test_publish_outcome_property_default_none(self) -> None:
        broker = TestBroker()
        assert broker.publish_outcome is None

    def test_reset_clears_one_shot_failure(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        def handle(body: bytes) -> str:
            return "processed"

        broker.start()
        broker.fail_next_publish()
        broker.reset()
        broker.publish("orders", b"hello")

        broker.assert_acked(broker.consumed_messages[0])

    @pytest.mark.asyncio
    async def test_async_fail_next_publish_triggers_nack_requeue(self) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        async def handle(body: bytes) -> str:
            return "processed"

        broker.start()
        broker.fail_next_publish()
        await broker.publish_async("orders", b"hello")

        msg = broker.consumed_messages[0]
        broker.assert_nacked(msg, requeue=True)


class TestTestAsyncBrokerExport:
    def test_async_broker_subclass_shares_behavior(self) -> None:
        from rabbitkit.testing import TestAsyncBroker

        broker = TestAsyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("orders", b"hello")
        broker.assert_acked(broker.consumed_messages[0])
        assert isinstance(broker, TestBroker)


# ── filter_fn on TestBroker.subscriber (Low) ─────────────────────────────


class TestSubscriberFilterFn:
    """Low: ``TestBroker.subscriber`` forwards ``filter_fn`` to the registry so
    the pre-deserialization reject path works through the test broker.
    """

    def test_filtered_out_message_is_nacked_not_handled(self) -> None:
        broker = TestBroker()
        body_ran = False

        @broker.subscriber(
            queue="orders",
            filter_fn=lambda msg: msg.headers.get("x-tenant") == "acme",
        )
        def handle(body: bytes) -> None:
            nonlocal body_ran
            body_ran = True

        broker.start()
        # No x-tenant header → filter rejects before the handler body runs.
        broker.publish("orders", b"payload", headers={})

        msg = broker.consumed_messages[0]
        broker.assert_nacked(msg, requeue=False)
        # The handler BODY did not run (the filter rejected pre-deserialization).
        # NOTE: TestBroker.records handler.mock(body) unconditionally after
        # processing, so we assert on a body-side flag rather than the mock.
        assert body_ran is False

    def test_filter_passes_message_is_handled(self) -> None:
        broker = TestBroker()

        @broker.subscriber(
            queue="orders",
            filter_fn=lambda msg: msg.headers.get("x-tenant") == "acme",
        )
        def handle(body: bytes) -> None:
            pass

        broker.start()
        broker.publish("orders", b"payload", headers={"x-tenant": "acme"})

        msg = broker.consumed_messages[0]
        broker.assert_acked(msg)
        handle.mock.assert_called_once_with(b"payload")

    def test_filter_fn_forwarded_to_registry(self) -> None:
        """White-box: filter_fn reaches the registered RouteDefinition."""
        broker = TestBroker()
        sentinel = lambda m: True

        @broker.subscriber(queue="orders", filter_fn=sentinel)
        def handle(body: bytes) -> None:
            pass

        routes = broker.routes
        assert len(routes) == 1
        assert routes[0].filter_fn is sentinel
