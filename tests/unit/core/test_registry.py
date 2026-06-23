"""Tests for core/registry.py — SubscriberRegistry, decorator registration."""

from __future__ import annotations

import pytest

from rabbitkit.core.config import RETRY_DISABLED, RetryConfig
from rabbitkit.core.registry import DuplicateRouteError, SubscriberRegistry
from rabbitkit.core.route import ConfigurationError
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy, ExchangeType

# ── helpers ───────────────────────────────────────────────────────────────


def _make_registry(**kwargs: object) -> SubscriberRegistry:
    return SubscriberRegistry(**kwargs)  # type: ignore[arg-type]


# ── basic registration ──────────────────────────────────────────────────


class TestBasicRegistration:
    def test_subscriber_with_string_queue(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert len(reg.routes) == 1
        route = reg.routes[0]
        assert route.queue.name == "orders"
        assert route.queue.durable is True
        assert route.handler is handle

    def test_subscriber_with_queue_object(self) -> None:
        reg = _make_registry()
        q = RabbitQueue(name="events")

        @reg.subscriber(queue=q)
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].queue is q

    def test_subscriber_with_string_exchange(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders", exchange="events")
        def handle(msg: object) -> None:
            pass

        ex = reg.routes[0].exchange
        assert ex is not None
        assert ex.name == "events"
        assert ex.type == ExchangeType.DIRECT

    def test_subscriber_with_exchange_object(self) -> None:
        reg = _make_registry()
        ex = RabbitExchange(name="events", type=ExchangeType.TOPIC)

        @reg.subscriber(queue="orders", exchange=ex)
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].exchange is ex

    def test_subscriber_no_exchange(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].exchange is None

    def test_subscriber_with_routing_key(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders", exchange="events", routing_key="orders.created")
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].queue.routing_key == "orders.created"

    def test_auto_generated_name(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        route = reg.routes[0]
        assert "orders" in route.name
        assert "handle" in route.name

    def test_explicit_name(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders", name="my-order-handler")
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].name == "my-order-handler"

    def test_ack_policy(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].ack_policy == AckPolicy.MANUAL

    def test_tags(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders", tags={"billing", "v2"})
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].tags == frozenset({"billing", "v2"})

    def test_description(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders", description="Process new orders")
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].description == "Process new orders"

    def test_multiple_routes(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle_orders(msg: object) -> None:
            pass

        @reg.subscriber(queue="payments")
        def handle_payments(msg: object) -> None:
            pass

        assert len(reg.routes) == 2


# ── duplicate detection ──────────────────────────────────────────────────


class TestDuplicateDetection:
    def test_duplicate_queue_raises(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle1(msg: object) -> None:
            pass

        with pytest.raises(DuplicateRouteError, match="orders"):

            @reg.subscriber(queue="orders")
            def handle2(msg: object) -> None:
                pass

    def test_duplicate_queue_object_raises(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue=RabbitQueue(name="events"))
        def handle1(msg: object) -> None:
            pass

        with pytest.raises(DuplicateRouteError, match="events"):

            @reg.subscriber(queue=RabbitQueue(name="events"))
            def handle2(msg: object) -> None:
                pass

    def test_different_queues_ok(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle1(msg: object) -> None:
            pass

        @reg.subscriber(queue="payments")
        def handle2(msg: object) -> None:
            pass

        assert len(reg.routes) == 2


# ── @publisher decorator ────────────────────────────────────────────────


class TestPublisherDecorator:
    def test_publisher_before_subscriber(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        @reg.publisher(exchange="results", routing_key="orders.result")
        def handle(msg: object) -> None:
            pass

        route = reg.routes[0]
        assert route.result_publisher is not None
        assert route.result_publisher.routing_key == "orders.result"
        assert route.result_publisher.resolve_exchange_name() == "results"

    def test_publisher_with_exchange_object(self) -> None:
        reg = _make_registry()
        ex = RabbitExchange(name="out", type=ExchangeType.FANOUT)

        @reg.subscriber(queue="orders")
        @reg.publisher(exchange=ex, routing_key="")
        def handle(msg: object) -> None:
            pass

        route = reg.routes[0]
        rp = route.result_publisher
        assert rp is not None
        assert rp.exchange is ex

    def test_subscriber_without_publisher(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].result_publisher is None

    def test_publisher_pending_without_subscriber(self) -> None:
        """Publisher stored as pending — will be picked up by later subscriber."""
        reg = _make_registry()

        @reg.publisher(exchange="results", routing_key="rk")
        def handle(msg: object) -> None:
            pass

        # Not yet registered as a route
        assert len(reg.routes) == 0

        # Pending publisher stored
        assert id(handle) in reg._pending_publishers


# ── retry configuration ─────────────────────────────────────────────────


class TestRetryConfiguration:
    def test_per_route_retry(self) -> None:
        reg = _make_registry()
        retry = RetryConfig(max_retries=2)

        @reg.subscriber(queue="orders", retry=retry)
        def handle(msg: object) -> None:
            pass

        route = reg.routes[0]
        assert route.retry_override is retry

    def test_retry_disabled_per_route(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders", retry=RETRY_DISABLED)
        def handle(msg: object) -> None:
            pass

        route = reg.routes[0]
        assert route.retry_override is RETRY_DISABLED

    def test_broker_retry_default(self) -> None:
        broker_retry = RetryConfig()
        reg = _make_registry(broker_retry=broker_retry)

        @reg.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        route = reg.routes[0]
        assert route.has_retry_enabled(broker_retry) is True

    def test_retry_manual_ack_fails_at_registration(self) -> None:
        reg = _make_registry()

        with pytest.raises(ConfigurationError, match="MANUAL"):

            @reg.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL, retry=RetryConfig())
            def handle(msg: object) -> None:
                pass

    def test_retry_ack_first_fails_at_registration(self) -> None:
        reg = _make_registry()

        with pytest.raises(ConfigurationError, match="ACK_FIRST"):

            @reg.subscriber(queue="orders", ack_policy=AckPolicy.ACK_FIRST, retry=RetryConfig())
            def handle(msg: object) -> None:
                pass

    def test_retry_dlx_conflict_fails_at_registration(self) -> None:
        reg = _make_registry()

        with pytest.raises(ConfigurationError, match="dead_letter_exchange"):

            @reg.subscriber(
                queue=RabbitQueue(name="orders", dead_letter_exchange="custom-dlx"),
                retry=RetryConfig(),
            )
            def handle(msg: object) -> None:
                pass

    def test_broker_retry_manual_ack_fails_at_registration(self) -> None:
        broker_retry = RetryConfig()
        reg = _make_registry(broker_retry=broker_retry)

        with pytest.raises(ConfigurationError, match="MANUAL"):

            @reg.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
            def handle(msg: object) -> None:
                pass

    def test_retry_disabled_manual_ack_ok(self) -> None:
        broker_retry = RetryConfig()
        reg = _make_registry(broker_retry=broker_retry)

        @reg.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL, retry=RETRY_DISABLED)
        def handle(msg: object) -> None:
            pass

        assert len(reg.routes) == 1


# ── set_broker_retry ─────────────────────────────────────────────────────


class TestSetBrokerRetry:
    def test_set_broker_retry_validates_existing(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(msg: object) -> None:
            pass

        # Setting broker retry should fail validation for MANUAL route
        with pytest.raises(ConfigurationError, match="MANUAL"):
            reg.set_broker_retry(RetryConfig())

    def test_set_broker_retry_ok(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders", ack_policy=AckPolicy.AUTO)
        def handle(msg: object) -> None:
            pass

        reg.set_broker_retry(RetryConfig())  # no exception


# ── route list ───────────────────────────────────────────────────────────


class TestRouteList:
    def test_routes_returns_copy(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        routes = reg.routes
        routes.clear()
        assert len(reg.routes) == 1  # original unchanged

    def test_empty_routes(self) -> None:
        reg = _make_registry()
        assert reg.routes == []


# ── serializer override ─────────────────────────────────────────────────


class TestSerializerOverride:
    def test_custom_serializer(self) -> None:
        reg = _make_registry()
        fake_serializer = object()

        @reg.subscriber(queue="orders", serializer=fake_serializer)
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].serializer_override is fake_serializer

    def test_no_serializer_default(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].serializer_override is None


# ── middlewares ──────────────────────────────────────────────────────────


class TestMiddlewares:
    def test_route_middlewares(self) -> None:
        reg = _make_registry()
        mw1 = object()
        mw2 = object()

        @reg.subscriber(queue="orders", middlewares=[mw1, mw2])
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].route_middlewares == [mw1, mw2]

    def test_no_middlewares_default(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert reg.routes[0].route_middlewares == []


# ── include_router edge cases ───────────────────────────────────────────


class TestIncludeRouterEdgeCases:
    def test_include_router_with_non_router_raises_type_error(self) -> None:
        """include_router raises TypeError for non-router objects (line 174)."""
        reg = _make_registry()

        with pytest.raises(TypeError, match="Expected a RabbitRouter"):
            reg.include_router("not-a-router")

    def test_include_router_applies_prefix_to_routing_key(self) -> None:
        """Prefix is prepended to existing routing_key (lines 179-180)."""
        from rabbitkit.core.router import RabbitRouter

        router = RabbitRouter()

        @router.subscriber(queue="events-queue", routing_key="created")
        def handle(msg: object) -> None:
            pass

        reg = _make_registry()
        reg.include_router(router, prefix="orders")

        # The routing key should now be "orders.created"
        assert reg.routes[0].queue.routing_key == "orders.created"

    def test_include_router_prefix_with_no_routing_key(self) -> None:
        """Prefix alone is applied when route has no routing_key (lines 179-180)."""
        from rabbitkit.core.router import RabbitRouter

        router = RabbitRouter()

        @router.subscriber(queue="events-queue2")
        def handle(msg: object) -> None:
            pass

        reg = _make_registry()
        reg.include_router(router, prefix="myprefix")

        # Routing key should be the prefix itself
        assert reg.routes[0].queue.routing_key == "myprefix"

    def test_include_router_duplicate_raises(self) -> None:
        """Duplicate queue from included router raises DuplicateRouteError (line 184)."""
        from rabbitkit.core.router import RabbitRouter

        router = RabbitRouter()

        @router.subscriber(queue="shared-queue")
        def handle(msg: object) -> None:
            pass

        reg = _make_registry()

        @reg.subscriber(queue="shared-queue")
        def existing_handle(msg: object) -> None:
            pass

        with pytest.raises(DuplicateRouteError, match="shared-queue"):
            reg.include_router(router)
