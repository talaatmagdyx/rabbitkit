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


# ── DLX cycle detection (M-C6) ────────────────────────────────────────────


class TestDLXCycleDetection:
    def test_two_queue_dlx_cycle_raises(self) -> None:
        """A 2-queue DLX loop (A→B→A) is rejected at registration time."""
        reg = _make_registry()

        @reg.subscriber(
            queue=RabbitQueue(name="qa", dead_letter_exchange="xb"),
            exchange=RabbitExchange(name="xa"),
        )
        def handle_a(msg: object) -> None:
            pass

        with pytest.raises(ConfigurationError, match="Dead-letter-exchange cycle"):

            @reg.subscriber(
                queue=RabbitQueue(name="qb", dead_letter_exchange="xa"),
                exchange=RabbitExchange(name="xb"),
            )
            def handle_b(msg: object) -> None:
                pass

    def test_no_cycle_when_dlx_is_sink(self) -> None:
        """A DLX pointing at an exchange with no bound queue (sink) is fine."""
        reg = _make_registry()

        @reg.subscriber(
            queue=RabbitQueue(name="qa", dead_letter_exchange="external-dlx"),
            exchange=RabbitExchange(name="xa"),
        )
        def handle_a(msg: object) -> None:
            pass

        # external-dlx has no route bound to it → sink, no cycle
        assert len(reg.routes) == 1

    def test_self_loop_raises(self) -> None:
        """A queue that dead-letters back to its own bound exchange loops."""
        reg = _make_registry()

        with pytest.raises(ConfigurationError, match="Dead-letter-exchange cycle"):

            @reg.subscriber(
                queue=RabbitQueue(name="qa", dead_letter_exchange="xa"),
                exchange=RabbitExchange(name="xa"),
            )
            def handle_a(msg: object) -> None:
                pass


class TestPrefetchOverride:
    @pytest.mark.parametrize("value", [0, -5])
    def test_invalid_prefetch_fails_at_registration(self, value: int) -> None:
        reg = _make_registry()
        with pytest.raises(ConfigurationError, match="prefetch_count"):

            @reg.subscriber(queue="orders", prefetch_count=value)
            def handle(msg: object) -> None:
                pass

        assert reg.routes == []


# ── stream routes ────────────────────────────────────────────────────────


def _stream_queue(name: str = "events") -> RabbitQueue:
    from rabbitkit.core.types import QueueType

    return RabbitQueue(name=name, queue_type=QueueType.STREAM)


class TestStreamRoutes:
    def test_a_stream_queue_gets_a_default_stream_config(self) -> None:
        from rabbitkit.core.streams import StreamConsumerConfig

        reg = _make_registry()

        @reg.subscriber(queue=_stream_queue())
        def handle(body: bytes) -> None: ...

        assert reg.routes[0].stream_config == StreamConsumerConfig()
        assert reg.routes[0].is_stream

    def test_a_classic_queue_has_no_stream_config(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue="orders")
        def handle(body: bytes) -> None: ...

        assert reg.routes[0].stream_config is None
        assert not reg.routes[0].is_stream

    def test_stream_offset_shorthand(self) -> None:
        from rabbitkit.core.streams import StreamOffset

        reg = _make_registry()

        @reg.subscriber(queue=_stream_queue(), stream_offset=42)
        def handle(body: bytes) -> None: ...

        config = reg.routes[0].stream_config
        assert config is not None and config.offset == StreamOffset.offset(42)

    def test_stream_config_is_kept(self) -> None:
        from rabbitkit.core.streams import StreamConsumerConfig

        reg = _make_registry()
        config = StreamConsumerConfig(offset="first", commit_every=10)

        @reg.subscriber(queue=_stream_queue(), stream=config)
        def handle(body: bytes) -> None: ...

        assert reg.routes[0].stream_config is config

    def test_both_stream_and_stream_offset_is_refused(self) -> None:
        from rabbitkit.core.streams import StreamConsumerConfig

        reg = _make_registry()
        with pytest.raises(ConfigurationError, match="not both"):

            @reg.subscriber(queue=_stream_queue(), stream=StreamConsumerConfig(), stream_offset="first")
            def handle(body: bytes) -> None: ...

    def test_stream_must_be_a_stream_consumer_config(self) -> None:
        reg = _make_registry()
        with pytest.raises(ConfigurationError, match="must be a StreamConsumerConfig"):

            @reg.subscriber(queue=_stream_queue(), stream="first")  # type: ignore[arg-type]
            def handle(body: bytes) -> None: ...

    @pytest.mark.parametrize("bad", ["begin", -1, 1.5])
    def test_invalid_stream_offset_is_a_configuration_error(self, bad: object) -> None:
        reg = _make_registry()
        with pytest.raises(ConfigurationError, match="invalid stream_offset"):

            @reg.subscriber(queue=_stream_queue(), stream_offset=bad)  # type: ignore[arg-type]
            def handle(body: bytes) -> None: ...

    def test_stream_options_on_a_classic_queue_are_refused(self) -> None:
        reg = _make_registry()
        with pytest.raises(ConfigurationError, match="need a stream queue; queue 'orders' is classic"):

            @reg.subscriber(queue="orders", stream_offset="first")
            def handle(body: bytes) -> None: ...

    def test_retry_on_a_stream_route_is_refused(self) -> None:
        reg = _make_registry()
        with pytest.raises(ConfigurationError, match=r"retry/DLQ is not supported .*this route.s retry="):

            @reg.subscriber(queue=_stream_queue(), retry=RetryConfig(max_retries=3))
            def handle(body: bytes) -> None: ...

    def test_inherited_broker_retry_on_a_stream_route_is_refused(self) -> None:
        reg = _make_registry(broker_retry=RetryConfig(max_retries=3))
        with pytest.raises(ConfigurationError, match=r"the broker.s default RetryConfig.*RETRY_DISABLED"):

            @reg.subscriber(queue=_stream_queue())
            def handle(body: bytes) -> None: ...

    def test_retry_disabled_on_a_stream_route_is_accepted_under_a_broker_retry(self) -> None:
        reg = _make_registry(broker_retry=RetryConfig(max_retries=3))

        @reg.subscriber(queue=_stream_queue(), retry=RETRY_DISABLED)
        def handle(body: bytes) -> None: ...

        assert reg.routes[0].is_stream

    def test_setting_a_broker_retry_later_revalidates_stream_routes(self) -> None:
        reg = _make_registry()

        @reg.subscriber(queue=_stream_queue())
        def handle(body: bytes) -> None: ...

        with pytest.raises(ConfigurationError, match="retry/DLQ is not supported"):
            reg.set_broker_retry(RetryConfig(max_retries=3))

    def test_ack_first_on_a_stream_route_is_refused(self) -> None:
        reg = _make_registry()
        with pytest.raises(ConfigurationError, match="ACK_FIRST is not supported on a stream queue"):

            @reg.subscriber(queue=_stream_queue(), ack_policy=AckPolicy.ACK_FIRST)
            def handle(body: bytes) -> None: ...

    @pytest.mark.parametrize("policy", [AckPolicy.AUTO, AckPolicy.NACK_ON_ERROR, AckPolicy.MANUAL])
    def test_other_ack_policies_are_accepted(self, policy: AckPolicy) -> None:
        reg = _make_registry()

        @reg.subscriber(queue=_stream_queue(), ack_policy=policy)
        def handle(body: bytes) -> None: ...

        assert reg.routes[0].ack_policy is policy

    def test_router_routes_keep_their_stream_config(self) -> None:
        from rabbitkit.core.router import RabbitRouter

        router = RabbitRouter()

        @router.subscriber(_stream_queue(), stream_offset="first")
        def handle(body: bytes) -> None: ...

        reg = _make_registry()
        reg.include_router(router)
        config = reg.routes[0].stream_config
        assert config is not None and config.offset.type.value == "first"
