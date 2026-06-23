"""Tests for core/router.py — RabbitRouter with prefix and defaults."""

from __future__ import annotations

from rabbitkit.core.router import RabbitRouter
from rabbitkit.core.topology import RabbitExchange
from rabbitkit.core.types import ExchangeType

# ── basic routing ────────────────────────────────────────────────────────


class TestBasicRouting:
    def test_register_subscriber(self) -> None:
        router = RabbitRouter()

        @router.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert len(router.routes) == 1
        assert router.routes[0].queue.name == "orders"

    def test_prefix_applied_to_routing_key(self) -> None:
        router = RabbitRouter(prefix="orders")

        @router.subscriber(queue="orders-queue", routing_key="created")
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].queue.routing_key == "orders.created"

    def test_prefix_only_no_routing_key(self) -> None:
        router = RabbitRouter(prefix="orders")

        @router.subscriber(queue="orders-queue")
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].queue.routing_key == "orders"

    def test_no_prefix_no_routing_key(self) -> None:
        router = RabbitRouter()

        @router.subscriber(queue="orders-queue")
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].queue.routing_key == ""


# ── default exchange ─────────────────────────────────────────────────────


class TestDefaultExchange:
    def test_router_default_exchange_string(self) -> None:
        router = RabbitRouter(exchange="events")

        @router.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        route = router.routes[0]
        assert route.exchange is not None
        assert route.exchange.name == "events"

    def test_router_default_exchange_object(self) -> None:
        ex = RabbitExchange(name="events", type=ExchangeType.TOPIC)
        router = RabbitRouter(exchange=ex)

        @router.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].exchange is ex

    def test_route_exchange_overrides_router(self) -> None:
        router = RabbitRouter(exchange="default-exchange")

        @router.subscriber(queue="orders", exchange="specific-exchange")
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].exchange is not None
        assert router.routes[0].exchange.name == "specific-exchange"

    def test_no_exchange(self) -> None:
        router = RabbitRouter()

        @router.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].exchange is None


# ── middleware merging ───────────────────────────────────────────────────


class TestMiddlewareMerging:
    def test_router_middlewares_applied(self) -> None:
        mw1 = object()
        router = RabbitRouter(middlewares=[mw1])

        @router.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].route_middlewares == [mw1]

    def test_route_middlewares_appended(self) -> None:
        mw1 = object()
        mw2 = object()
        router = RabbitRouter(middlewares=[mw1])

        @router.subscriber(queue="orders", middlewares=[mw2])
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].route_middlewares == [mw1, mw2]


# ── serializer fallback ────────────────────────────────────────────────


class TestSerializerFallback:
    def test_router_serializer_used(self) -> None:
        fake_serializer = object()
        router = RabbitRouter(serializer=fake_serializer)

        @router.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].serializer_override is fake_serializer

    def test_route_serializer_overrides_router(self) -> None:
        router_ser = object()
        route_ser = object()
        router = RabbitRouter(serializer=router_ser)

        @router.subscriber(queue="orders", serializer=route_ser)
        def handle(msg: object) -> None:
            pass

        assert router.routes[0].serializer_override is route_ser


# ── tag merging ──────────────────────────────────────────────────────────


class TestTagMerging:
    def test_router_tags_applied(self) -> None:
        router = RabbitRouter(tags={"v2"})

        @router.subscriber(queue="orders")
        def handle(msg: object) -> None:
            pass

        assert "v2" in router.routes[0].tags

    def test_route_tags_merged(self) -> None:
        router = RabbitRouter(tags={"v2"})

        @router.subscriber(queue="orders", tags={"billing"})
        def handle(msg: object) -> None:
            pass

        tags = router.routes[0].tags
        assert "v2" in tags
        assert "billing" in tags


# ── publisher ────────────────────────────────────────────────────────────


class TestRouterPublisher:
    def test_publisher_decorator(self) -> None:
        router = RabbitRouter()

        @router.subscriber(queue="orders")
        @router.publisher(exchange="out", routing_key="result")
        def handle(msg: object) -> None:
            pass

        route = router.routes[0]
        assert route.result_publisher is not None
        assert route.result_publisher.routing_key == "result"


# ── router properties ───────────────────────────────────────────────────


class TestRouterProperties:
    def test_prefix_property(self) -> None:
        router = RabbitRouter(prefix="orders")
        assert router.prefix == "orders"

    def test_empty_prefix(self) -> None:
        router = RabbitRouter()
        assert router.prefix == ""
