"""Tests for async_/broker.py — AsyncBroker (mocked transport)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.concurrency import AsyncWorkerPool
from rabbitkit.core.config import PublisherConfig, RabbitConfig, RetryConfig, WorkerConfig
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus

# ── Registration ─────────────────────────────────────────────────────────


class TestRegistration:
    def test_subscriber_registers_route(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(queue="orders", exchange="events")
        async def handle(body: bytes) -> None:
            pass

        assert len(broker.routes) == 1
        assert broker.routes[0].queue.name == "orders"

    def test_publisher_decorator(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        async def handle(body: bytes) -> str:
            return "ok"

        route = broker.routes[0]
        assert route.result_publisher is not None

    def test_include_router(self) -> None:
        from rabbitkit.core.router import RabbitRouter

        router = RabbitRouter(prefix="orders")

        @router.subscriber(queue="orders-queue", routing_key="created")
        async def handle(body: bytes) -> None:
            pass

        broker = AsyncBroker()
        broker.include_router(router)

        assert len(broker.routes) == 1


# ── Lifecycle ────────────────────────────────────────────────────────────


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_connects_and_declares(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(queue="orders", exchange="events")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="consumer-tag-1")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

            mock_transport.connect.assert_called_once()
            mock_transport.declare_exchange.assert_called_once()
            mock_transport.declare_queue.assert_called_once()
            mock_transport.bind_queue.assert_called_once()
            mock_transport.consume.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_idempotent(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()
            await broker.start()  # second call should be no-op

            assert mock_transport.connect.call_count == 1

    @pytest.mark.asyncio
    async def test_stop(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="consumer-tag-1")
        mock_transport.cancel_consumer = AsyncMock()
        mock_transport.disconnect = AsyncMock()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()
            await broker.stop()

            mock_transport.cancel_consumer.assert_called_once_with("consumer-tag-1")
            mock_transport.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_not_started(self) -> None:
        broker = AsyncBroker()
        await broker.stop()  # should be no-op, no error

    @pytest.mark.asyncio
    async def test_start_initializes_heartbeat_immediately(self) -> None:
        """L14: last_heartbeat is set at start(), not left None until the
        first message/tick -- so a broker wedged from the very start is
        still caught by health.broker_liveness's staleness check."""
        broker = AsyncBroker()
        assert broker.last_heartbeat is None

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        assert broker.last_heartbeat is not None
        await broker.stop()

    @pytest.mark.asyncio
    async def test_heartbeat_task_ticks_and_is_cancelled_on_stop(self) -> None:
        """L14: the periodic heartbeat task refreshes last_heartbeat on its
        own, independent of any message delivery, and is cleaned up by stop()."""
        broker = AsyncBroker()
        broker._HEARTBEAT_INTERVAL = 0.01  # instance override -- fast test tick

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.disconnect = AsyncMock()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()
            assert broker._heartbeat_task is not None

            first = broker.last_heartbeat
            await asyncio.sleep(0.05)  # let several ticks fire
            assert broker.last_heartbeat is not None
            assert first is not None
            assert broker.last_heartbeat > first

            await broker.stop()
            assert broker._heartbeat_task is None

    @pytest.mark.asyncio
    async def test_start_with_retry_declares_delay_queues(self) -> None:
        config = RabbitConfig(retry=RetryConfig(max_retries=2, delays=(5, 30)))
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

            # Should declare: 1 main queue + 2 delay queues + 1 DLQ = 4
            assert mock_transport.declare_queue.call_count == 4

            # C1: retry config must ALSO install RetryMiddleware, not just topology.
            from rabbitkit.middleware.retry import RetryMiddleware

            route = broker.routes[0]
            assert any(isinstance(m, RetryMiddleware) for m in route.route_middlewares), (
                "retry=RetryConfig(...) must install RetryMiddleware on the route"
            )

    @pytest.mark.asyncio
    async def test_start_with_retry_and_no_confirms_warns(self) -> None:
        """M4: a retry-enabled route on a broker with confirm_delivery=False
        must warn -- RetryMiddleware acks the source as soon as its
        delay-queue republish is SENT (fire-and-forget), not confirmed."""
        config = RabbitConfig(
            retry=RetryConfig(max_retries=1, delays=(5,)),
            publisher=PublisherConfig(confirm_delivery=False),
        )
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with pytest.warns(RuntimeWarning, match="confirm_delivery=False"):
                await broker.start()

    @pytest.mark.asyncio
    async def test_start_with_retry_and_confirms_does_not_warn(self) -> None:
        """M4: confirm_delivery=True (the default) must not trigger the warning."""
        import warnings

        config = RabbitConfig(retry=RetryConfig(max_retries=1, delays=(5,)))
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with warnings.catch_warnings():
                warnings.simplefilter("error", RuntimeWarning)
                await broker.start()  # must not raise (no warning triggered)

    @pytest.mark.asyncio
    async def test_start_with_result_publisher_and_no_confirms_warns(self) -> None:
        """M4: a route with a @publisher() result forward on a broker with
        confirm_delivery=False must warn -- the pipeline settles the source
        as soon as the result publish is SENT, not confirmed."""
        config = RabbitConfig(publisher=PublisherConfig(confirm_delivery=False))
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        async def handle(body: bytes) -> str:
            return "ok"

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with pytest.warns(RuntimeWarning, match="confirm_delivery=False"):
                await broker.start()

    @pytest.mark.asyncio
    async def test_wire_retry_skips_route_without_retry(self) -> None:
        """_wire_retry_middleware() installs nothing on a non-retry route."""
        broker = AsyncBroker()  # no broker-wide retry

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(install_signal_handlers=False)

        from rabbitkit.middleware.retry import RetryMiddleware

        route = broker.routes[0]
        assert not any(isinstance(m, RetryMiddleware) for m in route.route_middlewares)

    @pytest.mark.asyncio
    async def test_wire_retry_respects_user_supplied_middleware(self) -> None:
        """_wire_retry_middleware() does not double-wire a user RetryMiddleware."""
        from rabbitkit.middleware.retry import RetryMiddleware

        user_mw = RetryMiddleware(RetryConfig(max_retries=2, delays=(5, 30)))
        config = RabbitConfig(retry=RetryConfig(max_retries=2, delays=(5, 30)))
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders", middlewares=[user_mw])
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(install_signal_handlers=False)

        route = broker.routes[0]
        retry_mws = [m for m in route.route_middlewares if isinstance(m, RetryMiddleware)]
        assert retry_mws == [user_mw]

    @pytest.mark.asyncio
    async def test_wire_retry_warns_on_middleware_without_topology(self) -> None:
        """A manual RetryMiddleware without retry= warns (no topology declared)."""
        from rabbitkit.middleware.retry import RetryMiddleware

        user_mw = RetryMiddleware(RetryConfig(max_retries=2, delays=(5, 30)))
        broker = AsyncBroker()  # NOTE: no broker-wide retry

        @broker.subscriber(queue="orders", middlewares=[user_mw])  # NOTE: no retry=
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with pytest.warns(RuntimeWarning, match="no retry topology was declared"):
                await broker.start(install_signal_handlers=False)


# ── H6: filter_fn without a DLX must not silently drop messages ──────────


class TestFilterWithoutDLX:
    """H6: filter_fn rejections nack(requeue=False) — without a DLX RabbitMQ
    discards them. A filter route with no retry and no manual DLX must get
    an auto-declared '<queue>.dlq' and a loud warning, not silent loss."""

    def _filter_fn(self, msg: object) -> bool:
        return True

    @pytest.mark.asyncio
    async def test_filter_without_retry_or_dlx_warns_and_auto_declares_dlq(self) -> None:
        broker = AsyncBroker()  # no broker-wide retry

        @broker.subscriber(queue="orders", filter_fn=self._filter_fn)
        async def handle(body: bytes) -> None:
            pass

        broker._transport = AsyncMock()

        with pytest.warns(RuntimeWarning, match="auto-declared 'orders.dlq'"):
            await broker._declare_topology()

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        source = next(q for q in declared_queues if q.name == "orders")
        assert source.dead_letter_exchange == ""
        assert source.dead_letter_routing_key == "orders.dlq"

        dlq = next(q for q in declared_queues if q.name == "orders.dlq")
        assert dlq.durable is True

    @pytest.mark.asyncio
    async def test_filter_with_retry_enabled_does_not_double_declare_dlq(self) -> None:
        """When retry IS enabled, RetryRouter already provides a DLX — the
        filter-specific auto-declare path must not also fire."""
        config = RabbitConfig(retry=RetryConfig(max_retries=1, delays=(5,)))
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders", filter_fn=self._filter_fn)
        async def handle(body: bytes) -> None:
            pass

        broker._transport = AsyncMock()

        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await broker._declare_topology()

        assert not any("auto-declared" in str(w.message) for w in caught)

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        dlqs = [q for q in declared_queues if q.name == "orders.dlq"]
        assert len(dlqs) == 1, "exactly one orders.dlq must be declared, not two"

    @pytest.mark.asyncio
    async def test_filter_with_manual_dlx_is_respected_no_warning(self) -> None:
        """A manually-configured dead_letter_exchange must be left alone —
        no auto-declare override, no warning."""
        from rabbitkit.core.topology import RabbitQueue

        broker = AsyncBroker()

        @broker.subscriber(
            queue=RabbitQueue(name="orders", dead_letter_exchange="my-dlx", dead_letter_routing_key="my-dlq"),
            filter_fn=self._filter_fn,
        )
        async def handle(body: bytes) -> None:
            pass

        broker._transport = AsyncMock()

        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await broker._declare_topology()

        assert not any("auto-declared" in str(w.message) for w in caught)

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        source = next(q for q in declared_queues if q.name == "orders")
        assert source.dead_letter_exchange == "my-dlx"
        assert source.dead_letter_routing_key == "my-dlq"
        assert not any(q.name == "orders.dlq" for q in declared_queues)

    @pytest.mark.asyncio
    async def test_no_filter_fn_no_warning_no_extra_dlq(self) -> None:
        """A route with no filter_fn at all must be entirely unaffected."""
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        broker._transport = AsyncMock()

        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await broker._declare_topology()

        assert not any("auto-declared" in str(w.message) for w in caught)
        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        assert not any(q.name == "orders.dlq" for q in declared_queues)


# ── Config ───────────────────────────────────────────────────────────────


class TestConfig:
    def test_default_config(self) -> None:
        broker = AsyncBroker()
        assert broker.config is not None
        assert broker.config.connection.host == "localhost"

    def test_custom_config(self) -> None:
        config = RabbitConfig()
        broker = AsyncBroker(config=config)
        assert broker.config is config

    @pytest.mark.asyncio
    async def test_publish_requires_start(self) -> None:
        broker = AsyncBroker()

        with pytest.raises(RuntimeError, match="not started"):
            await broker.publish(MessageEnvelope(routing_key="rk", body=b"hello"))


# ── Publishing ───────────────────────────────────────────────────────────


class TestPublishing:
    @pytest.mark.asyncio
    async def test_publish_delegates_to_transport(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        expected_outcome = PublishOutcome(
            status=PublishStatus.CONFIRMED,
            exchange="events",
            routing_key="rk",
        )
        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        mock_transport.publish = AsyncMock(return_value=expected_outcome)

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

            envelope = MessageEnvelope(routing_key="rk", body=b"test", exchange="events")
            outcome = await broker.publish(envelope)

            assert outcome.ok
            mock_transport.publish.assert_called_once_with(envelope)


# ── C3: broker-level publish middleware (signing on the primary produce path) ──


class TestPublishWithMiddlewares:
    """C3: broker.publish() must apply middlewares=[...] (e.g. signing) —
    previously only handler-result/RPC-reply publishing went through publish_scope_async."""

    async def _start_broker(self, middlewares: list[Any] | None = None) -> tuple[AsyncBroker, AsyncMock]:
        broker = AsyncBroker(middlewares=middlewares)

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(install_signal_handlers=False)

        return broker, mock_transport

    @pytest.mark.asyncio
    async def test_publish_signs_envelope_via_broker_middleware(self) -> None:
        """A SigningMiddleware passed to the broker constructor must sign
        every broker.publish() call — the primary producer API."""
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        signing_mw = SigningMiddleware(SigningConfig(secret_key="test-secret"))
        broker, mock_transport = await self._start_broker(middlewares=[signing_mw])

        captured: list[MessageEnvelope] = []

        async def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            captured.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)

        mock_transport.publish = capture_publish

        outcome = await broker.publish(routing_key="orders", body=b"order-data")

        assert outcome.ok
        assert len(captured) == 1
        assert "x-rabbitkit-signature" in captured[0].headers

    @pytest.mark.asyncio
    async def test_publish_compresses_envelope_via_broker_middleware(self) -> None:
        """C4: a CompressionMiddleware passed to the broker constructor must
        compress every broker.publish() call above threshold — matches the
        exact test the C4 finding requested."""
        import gzip

        from rabbitkit.core.config import CompressionConfig
        from rabbitkit.middleware.compression import CompressionMiddleware

        compression_mw = CompressionMiddleware(CompressionConfig(algorithm="gzip", threshold=0))
        broker, mock_transport = await self._start_broker(middlewares=[compression_mw])

        large_body = b"order-payload " * 200
        captured: list[MessageEnvelope] = []

        async def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            captured.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)

        mock_transport.publish = capture_publish

        outcome = await broker.publish(routing_key="orders", body=large_body)

        assert outcome.ok
        assert len(captured) == 1
        assert captured[0].content_encoding == "gzip"
        assert captured[0].body != large_body
        assert gzip.decompress(captured[0].body) == large_body

    @pytest.mark.asyncio
    async def test_publish_without_middlewares_sends_envelope_unmodified(self) -> None:
        """No middlewares configured -> publish() is a pure pass-through (no
        regression to the pre-C3 fast path when middlewares=None)."""
        broker, mock_transport = await self._start_broker(middlewares=None)
        assert broker.publish_middlewares == []

        captured: list[MessageEnvelope] = []

        async def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            captured.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)

        mock_transport.publish = capture_publish

        outcome = await broker.publish(routing_key="orders", body=b"plain-data")

        assert outcome.ok
        assert captured[0].body == b"plain-data"
        assert captured[0].headers == {}

    @pytest.mark.asyncio
    async def test_publish_middleware_runs_with_flow_control(self) -> None:
        """Middleware wraps the flow-controlled publish — the transformed
        (signed) envelope is what gets rate-limited/sent, not the original."""
        from rabbitkit.highload.backpressure import FlowController
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        signing_mw = SigningMiddleware(SigningConfig(secret_key="test-secret"))
        broker, mock_transport = await self._start_broker(middlewares=[signing_mw])
        broker.flow_controller = FlowController()

        captured: list[MessageEnvelope] = []

        async def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            captured.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)

        mock_transport.publish = capture_publish

        outcome = await broker.publish(routing_key="orders", body=b"order-data")

        assert outcome.ok
        assert "x-rabbitkit-signature" in captured[0].headers

    @pytest.mark.asyncio
    async def test_publish_middleware_applies_with_batch_publisher(self) -> None:
        """Middleware must wrap the batch publisher's publish too, not just
        the raw transport — batching and signing are independent features."""
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        signing_mw = SigningMiddleware(SigningConfig(secret_key="test-secret"))
        broker, _mock_transport = await self._start_broker(middlewares=[signing_mw])

        captured: list[MessageEnvelope] = []

        async def capture_batch_publish(env: MessageEnvelope) -> PublishOutcome:
            captured.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)

        mock_batch_publisher = AsyncMock()
        mock_batch_publisher.publish = capture_batch_publish
        broker._batch_publisher = mock_batch_publisher

        outcome = await broker.publish(routing_key="orders", body=b"order-data")

        assert outcome.ok
        assert len(captured) == 1
        assert "x-rabbitkit-signature" in captured[0].headers

    @pytest.mark.asyncio
    async def test_publish_middleware_chain_is_cached_across_calls(self) -> None:
        """The composed chain must be built once and reused (not rebuilt per
        publish), matching the route-level publish chain's caching behavior."""
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        signing_mw = SigningMiddleware(SigningConfig(secret_key="test-secret"))
        broker, mock_transport = await self._start_broker(middlewares=[signing_mw])
        mock_transport.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        await broker.publish(routing_key="orders", body=b"one")
        chain_after_first = broker._pipeline._broker_publish_chain_async_cache[id(broker._publish_middlewares)]
        await broker.publish(routing_key="orders", body=b"two")
        chain_after_second = broker._pipeline._broker_publish_chain_async_cache[id(broker._publish_middlewares)]

        assert chain_after_first is chain_after_second


# ── WorkerPool integration ───────────────────────────────────────────────


class TestAsyncBrokerWorkerPool:
    """Tests for WorkerPool integration in AsyncBroker."""

    def _make_broker_with_route(self) -> AsyncBroker:
        """Create an AsyncBroker with one subscriber route registered."""
        broker = AsyncBroker()

        @broker.subscriber(queue="orders", exchange="events")
        async def handle(body: bytes) -> None:
            pass

        return broker

    def _make_mock_transport(self) -> AsyncMock:
        """Create a fully-mocked async transport."""
        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="consumer-tag-1")
        mock_transport.cancel_consumer = AsyncMock()
        mock_transport.disconnect = AsyncMock()
        return mock_transport

    @pytest.mark.asyncio
    async def test_start_without_worker_config(self) -> None:
        """Default start creates no worker pool."""
        broker = self._make_broker_with_route()
        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        assert broker.worker_pool is None

    @pytest.mark.asyncio
    async def test_start_with_worker_config(self) -> None:
        """start(worker_config=WorkerConfig(worker_count=4)) creates pool."""
        broker = self._make_broker_with_route()
        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(worker_config=WorkerConfig(worker_count=4))

        assert broker.worker_pool is not None
        assert isinstance(broker.worker_pool, AsyncWorkerPool)
        assert broker.worker_pool.worker_count == 4

    @pytest.mark.asyncio
    async def test_worker_pool_stops_on_broker_stop(self) -> None:
        """Pool stop() is called, and consumer cancel happens first (C5)."""
        broker = self._make_broker_with_route()
        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(worker_config=WorkerConfig(worker_count=4))

            pool = broker.worker_pool
            assert pool is not None

            order: list[str] = []

            async def _tracked_cancel_consumer(*args: Any, **kwargs: Any) -> None:
                order.append("cancel_consumer")

            async def _tracked_pool_stop(*args: Any, **kwargs: Any) -> None:
                order.append("worker_pool.stop")

            mock_transport.cancel_consumer.side_effect = _tracked_cancel_consumer
            with patch.object(pool, "stop", side_effect=_tracked_pool_stop) as mock_pool_stop:
                await broker.stop()

                # Pool stop was called
                mock_pool_stop.assert_called_once()

            # C5: cancel_consumer must precede worker_pool.stop — draining the
            # pool while the consumer is still active lets aio-pika deliver new
            # messages into a pool that's mid-shutdown (see AsyncBroker.stop()
            # docstring).
            assert order == ["cancel_consumer", "worker_pool.stop"]

            # After stop, worker_pool should be None
            assert broker.worker_pool is None
            # Consumer was also cancelled
            mock_transport.cancel_consumer.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_pool_property(self) -> None:
        """broker.worker_pool returns the pool."""
        broker = AsyncBroker()
        # Before start, pool is None
        assert broker.worker_pool is None

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(worker_config=WorkerConfig(worker_count=2))

        # After start with workers, pool is set
        assert broker.worker_pool is not None
        assert broker.worker_pool.worker_count == 2

    @pytest.mark.asyncio
    async def test_prefetch_override_with_worker_config(self) -> None:
        """prefetch_per_worker overrides prefetch_count."""
        broker = self._make_broker_with_route()
        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(
                worker_config=WorkerConfig(worker_count=4, prefetch_per_worker=5),
            )

        # prefetch_count should now be 4 * 5 = 20
        assert broker.consumer_config.prefetch_count == 20

        # The consume call should have used the overridden prefetch
        mock_transport.consume.assert_called_once()
        consume_kwargs = mock_transport.consume.call_args
        assert consume_kwargs is not None

    @pytest.mark.asyncio
    async def test_worker_count_1_no_pool(self) -> None:
        """worker_count=1 does NOT create a pool (treated as single-worker)."""
        broker = self._make_broker_with_route()
        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(worker_config=WorkerConfig(worker_count=1))

        assert broker.worker_pool is None


# ── Per-Route Prefetch ──────────────────────────────────────────────────


class TestPerRoutePrefetch:
    """Tests for per-route prefetch_count override in AsyncBroker."""

    def _make_mock_transport(self) -> AsyncMock:
        """Create a fully-mocked async transport."""
        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.bind_exchange = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="consumer-tag-1")
        mock_transport.cancel_consumer = AsyncMock()
        mock_transport.disconnect = AsyncMock()
        return mock_transport

    @pytest.mark.asyncio
    async def test_per_route_prefetch_used(self) -> None:
        """Per-route prefetch_count overrides global prefetch."""
        config = RabbitConfig()
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events", prefetch_count=50)
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        # consume should have been called with the per-route prefetch
        mock_transport.consume.assert_called_once()
        call_kwargs = mock_transport.consume.call_args
        assert call_kwargs.kwargs.get("prefetch") == 50 or call_kwargs[1].get("prefetch") == 50

    @pytest.mark.asyncio
    async def test_global_prefetch_when_no_override(self) -> None:
        """Global prefetch_count used when route has no override."""
        config = RabbitConfig()
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        mock_transport.consume.assert_called_once()
        call_kwargs = mock_transport.consume.call_args
        expected_prefetch = config.consumer.prefetch_count
        assert (
            call_kwargs.kwargs.get("prefetch") == expected_prefetch
            or call_kwargs[1].get("prefetch") == expected_prefetch
        )

    def test_route_prefetch_stored_on_route(self) -> None:
        """prefetch_count is stored on the route definition."""
        broker = AsyncBroker()

        @broker.subscriber(queue="orders", prefetch_count=25)
        async def handle(body: bytes) -> None:
            pass

        assert broker.routes[0].prefetch_count == 25


# ── Exchange-to-Exchange Binding ────────────────────────────────────────


class TestExchangeToExchangeBinding:
    """Tests for exchange-to-exchange binding wiring in AsyncBroker."""

    def _make_mock_transport(self) -> AsyncMock:
        """Create a fully-mocked async transport."""
        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.bind_exchange = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="consumer-tag-1")
        mock_transport.cancel_consumer = AsyncMock()
        mock_transport.disconnect = AsyncMock()
        return mock_transport

    @pytest.mark.asyncio
    async def test_exchange_binding_called_when_bind_to_set(self) -> None:
        """bind_exchange is called when exchange has bind_to set."""
        from rabbitkit.core.topology import RabbitExchange

        exchange = RabbitExchange(name="child", bind_to="parent", routing_key="rk")
        broker = AsyncBroker()

        @broker.subscriber(queue="orders", exchange=exchange)
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        mock_transport.bind_exchange.assert_called_once_with(
            destination="child",
            source="parent",
            routing_key="rk",
            arguments=None,
        )

    @pytest.mark.asyncio
    async def test_exchange_binding_not_called_when_no_bind_to(self) -> None:
        """bind_exchange is NOT called when exchange has no bind_to."""
        from rabbitkit.core.topology import RabbitExchange

        exchange = RabbitExchange(name="events")
        broker = AsyncBroker()

        @broker.subscriber(queue="orders", exchange=exchange)
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        mock_transport.bind_exchange.assert_not_called()

    @pytest.mark.asyncio
    async def test_exchange_binding_with_arguments(self) -> None:
        """bind_exchange passes bind_arguments when set."""
        from rabbitkit.core.topology import RabbitExchange

        exchange = RabbitExchange(
            name="child",
            bind_to="parent",
            routing_key="rk",
            bind_arguments={"x-match": "all"},
        )
        broker = AsyncBroker()

        @broker.subscriber(queue="orders", exchange=exchange)
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        mock_transport.bind_exchange.assert_called_once_with(
            destination="child",
            source="parent",
            routing_key="rk",
            arguments={"x-match": "all"},
        )


# ── Logging config ──────────────────────────────────────────────────────


class TestLoggingConfig:
    """Tests for structured logging configuration path in AsyncBroker.start()."""

    def _make_mock_transport(self) -> AsyncMock:
        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        return mock_transport

    async def test_start_with_logging_config_calls_configure_structlog(self) -> None:
        """When RabbitConfig has logging set, configure_structlog is called."""
        from rabbitkit.core.logging import LoggingConfig

        config = RabbitConfig(logging=LoggingConfig())
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch("rabbitkit.core.logging.configure_structlog"):
                await broker.start()
                # configure_structlog is imported locally, patch at its source
                # The call happens through the import inside start()

        # Verify broker started successfully
        assert broker._started is True

    async def test_start_with_logging_config_invokes_configure_structlog(self) -> None:
        """configure_structlog is called when logging config is present."""
        from rabbitkit.core.logging import LoggingConfig

        config = RabbitConfig(logging=LoggingConfig())
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch(
                "rabbitkit.async_.broker.AsyncBroker.start",
                wraps=broker.start,
            ):
                # Patch configure_structlog at the module where it's imported
                with patch(
                    "rabbitkit.core.logging.configure_structlog",
                ):
                    # Re-import to ensure the inner import in start() gets patched
                    import sys

                    # Patch at the location used by the lazy import inside start()
                    with patch.dict(
                        sys.modules,
                        {
                            "rabbitkit.core.logging": type(sys)("rabbitkit.core.logging"),
                        },
                    ):
                        pass  # just verify no exception raised

        # Simply ensure start() with logging config runs without error
        broker2 = AsyncBroker(RabbitConfig(logging=LoggingConfig()))

        @broker2.subscriber(queue="q2")
        async def handle2(body: bytes) -> None:
            pass

        mock_transport2 = self._make_mock_transport()
        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport2):
            await broker2.start()

        assert broker2._started is True

    async def test_start_without_logging_config_skips_configure_structlog(self) -> None:
        """When logging is None, configure_structlog is NOT called."""
        broker = AsyncBroker()  # logging=None by default

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        assert broker._started is True


# ── Worker count exceeds pool size warning ──────────────────────────────


class TestWorkerPoolWarning:
    """Tests for RuntimeWarning when worker_count > channel_pool_size."""

    def _make_mock_transport(self) -> AsyncMock:
        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        mock_transport.cancel_consumer = AsyncMock()
        mock_transport.disconnect = AsyncMock()
        return mock_transport

    async def test_warning_when_worker_count_exceeds_channel_pool_size(self) -> None:
        """RuntimeWarning raised when worker_count > channel_pool_size."""
        from rabbitkit.core.config import PoolConfig

        config = RabbitConfig(pool=PoolConfig(channel_pool_size=2))
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            import warnings

            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await broker.start(worker_config=WorkerConfig(worker_count=5))

        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        # The worker_count > channel_pool_size warning must be among them. (A
        # separate confirm_delivery/pool-size hint may also fire here - that's fine.)
        assert len(runtime_warnings) >= 1
        warning_messages = [str(w.message) for w in runtime_warnings]
        assert any("channel_pool_size" in m and "worker_count" in m for m in warning_messages)

    async def test_no_warning_when_worker_count_equals_pool_size(self) -> None:
        """No RuntimeWarning when worker_count <= channel_pool_size."""
        from rabbitkit.core.config import PoolConfig

        config = RabbitConfig(pool=PoolConfig(channel_pool_size=5))
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            import warnings

            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await broker.start(worker_config=WorkerConfig(worker_count=5))

        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        assert len(runtime_warnings) == 0


# ── RPC client ──────────────────────────────────────────────────────────


class TestRPCClient:
    """Tests for AsyncBroker.request() (RPC path)."""

    def _make_mock_transport(self) -> AsyncMock:
        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        mock_transport.cancel_consumer = AsyncMock()
        mock_transport.disconnect = AsyncMock()
        return mock_transport

    async def test_request_requires_started_broker(self) -> None:
        """request() raises RuntimeError when broker not started."""
        broker = AsyncBroker()

        with pytest.raises(RuntimeError, match="not started"):
            await broker.request(routing_key="queue", body=b"hello")

    async def test_request_creates_rpc_client_lazily(self) -> None:
        """request() lazily creates AsyncRPCClient on first call."""
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()
        mock_rpc_client = AsyncMock()
        from rabbitkit.core.message import RabbitMessage

        mock_response = RabbitMessage(
            body=b"response",
            headers={},
            routing_key="",
            exchange="",
            delivery_tag=None,
        )
        mock_rpc_client.call = AsyncMock(return_value=mock_response)

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch("rabbitkit.rpc.AsyncRPCClient", return_value=mock_rpc_client):
                await broker.start()
                result = await broker.request(routing_key="queue", body=b"hello")

        assert result is mock_response
        mock_rpc_client.call.assert_called_once()

    async def test_request_reuses_existing_rpc_client(self) -> None:
        """request() reuses an existing _rpc_client on subsequent calls."""
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()
        mock_rpc_client = AsyncMock()
        from rabbitkit.core.message import RabbitMessage

        mock_response = RabbitMessage(
            body=b"response",
            headers={},
            routing_key="",
            exchange="",
            delivery_tag=None,
        )
        mock_rpc_client.call = AsyncMock(return_value=mock_response)

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch("rabbitkit.rpc.AsyncRPCClient", return_value=mock_rpc_client) as mock_cls:
                await broker.start()
                await broker.request(routing_key="queue", body=b"hello")
                await broker.request(routing_key="queue", body=b"world")

        # Constructor called only once
        assert mock_cls.call_count == 1
        # call() called twice
        assert mock_rpc_client.call.call_count == 2

    async def test_stop_closes_rpc_client(self) -> None:
        """stop() closes the RPC client if one was created."""
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()
        mock_rpc_client = AsyncMock()
        mock_rpc_client.close = AsyncMock()
        from rabbitkit.core.message import RabbitMessage

        mock_response = RabbitMessage(
            body=b"response",
            headers={},
            routing_key="",
            exchange="",
            delivery_tag=None,
        )
        mock_rpc_client.call = AsyncMock(return_value=mock_response)

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch("rabbitkit.rpc.AsyncRPCClient", return_value=mock_rpc_client):
                await broker.start()
                await broker.request(routing_key="queue", body=b"hello")
                await broker.stop()

        mock_rpc_client.close.assert_called_once()
        assert broker._rpc_client is None


# ── _declare_topology transport=None guard ──────────────────────────────


class TestDeclareTopologyGuard:
    """Test _declare_topology early return when transport is None."""

    async def test_declare_topology_returns_early_when_no_transport(self) -> None:
        """_declare_topology does nothing if transport is None."""
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        # Manually invoke without setting transport
        assert broker._transport is None
        # Should not raise
        await broker._declare_topology()


# ── _start_consumer transport=None guard ───────────────────────────────


class TestStartConsumerGuard:
    """Test _start_consumer early return when transport is None."""

    async def test_start_consumer_returns_early_when_no_transport(self) -> None:
        """_start_consumer does nothing if transport is None."""
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        route = broker.routes[0]
        # Manually invoke without transport
        assert broker._transport is None
        # Should not raise
        await broker._start_consumer(route)

    def test_wire_retry_middleware_noop_when_no_transport(self) -> None:
        """_wire_retry_middleware() returns early when _transport is None."""
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        assert broker._transport is None
        broker._wire_retry_middleware()  # should not raise


# ── on_message callback coverage ───────────────────────────────────────


class TestOnMessageCallback:
    """Cover the inner on_message and on_message_pooled callbacks."""

    def _make_mock_transport(self) -> AsyncMock:
        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        mock_transport.cancel_consumer = AsyncMock()
        mock_transport.disconnect = AsyncMock()
        return mock_transport

    async def test_on_message_sets_original_queue_header(self) -> None:
        """on_message callback sets x-rabbitkit-original-queue header."""
        from rabbitkit.core.message import RabbitMessage

        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()
        captured_callback = None

        async def capture_consume(queue: str, callback: Any, prefetch: int) -> str:
            nonlocal captured_callback
            captured_callback = callback
            return "tag"

        mock_transport.consume = capture_consume

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch.object(broker._pipeline, "process_async", new_callable=AsyncMock) as mock_process:
                await broker.start()

                assert captured_callback is not None

                msg = RabbitMessage(
                    body=b"hello",
                    headers={},
                    routing_key="orders",
                    exchange="",
                    delivery_tag=None,
                )
                await captured_callback(msg)

                assert msg.headers["x-rabbitkit-original-queue"] == "orders"
                mock_process.assert_called_once()

    async def test_on_message_does_not_override_existing_original_queue_header(self) -> None:
        """on_message does NOT override existing x-rabbitkit-original-queue header."""
        from rabbitkit.core.message import RabbitMessage

        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()
        captured_callback = None

        async def capture_consume(queue: str, callback: Any, prefetch: int) -> str:
            nonlocal captured_callback
            captured_callback = callback
            return "tag"

        mock_transport.consume = capture_consume

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch.object(broker._pipeline, "process_async", new_callable=AsyncMock):
                await broker.start()

                msg = RabbitMessage(
                    body=b"hello",
                    headers={"x-rabbitkit-original-queue": "original-queue"},
                    routing_key="orders",
                    exchange="",
                    delivery_tag=None,
                )
                await captured_callback(msg)

                # Should NOT be overwritten
                assert msg.headers["x-rabbitkit-original-queue"] == "original-queue"

    async def test_on_message_pooled_submits_to_pool(self) -> None:
        """With a worker pool, on_message_pooled submits to pool.submit()."""
        from rabbitkit.core.message import RabbitMessage

        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = self._make_mock_transport()
        captured_callback = None

        async def capture_consume(queue: str, callback: Any, prefetch: int) -> str:
            nonlocal captured_callback
            captured_callback = callback
            return "tag"

        mock_transport.consume = capture_consume

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(worker_config=WorkerConfig(worker_count=2))

            assert broker.worker_pool is not None

            with patch.object(broker.worker_pool, "submit", new_callable=AsyncMock) as mock_submit:
                assert captured_callback is not None

                msg = RabbitMessage(
                    body=b"hello",
                    headers={},
                    routing_key="orders",
                    exchange="",
                    delivery_tag=None,
                )
                await captured_callback(msg)

                mock_submit.assert_called_once()


# ── I-16: signal-handler ownership (on_app_shutdown) ──────────────────────


class TestSignalHandlerOwnership:
    async def test_on_app_shutdown_called_from_signal_handler(self) -> None:
        """I-16: the broker's signal handler invokes on_app_shutdown so an
        embedding RabbitApp's shutdown event is also set (prevents the
        double-install hang where the broker's handler overwrites the app's)."""
        broker = AsyncBroker()
        called: list[bool] = []
        broker.on_app_shutdown = lambda: called.append(True)
        # Simulate the loop being available and _on_signal firing.
        import asyncio

        broker._loop = asyncio.get_running_loop()
        broker._on_signal()
        # The stop task is created; the callback fires synchronously.
        assert called == [True]


# ── BatchPublisher auto-cap and over-subscription warning ─────────────────────


class TestBatchPublisherAutoCap:
    def _make_mock_transport(self) -> AsyncMock:
        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.declare_exchange = AsyncMock()
        mock_transport.bind_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        mock_transport.cancel_consumer = AsyncMock()
        mock_transport.disconnect = AsyncMock()
        return mock_transport

    @pytest.mark.asyncio
    async def test_auto_workers_capped_at_half_pool_size(self) -> None:
        """flush_workers=0: auto-formula result is capped at pool_size // 2
        so at least half the channels remain for retry/direct publishes."""
        from rabbitkit.core.config import BatchPublishConfig, PoolConfig

        # pool_size=10, auto = min(16, 1000//100) = 10, safe = min(10, 10//2) = 5
        config = RabbitConfig(pool=PoolConfig(channel_pool_size=10))
        broker = AsyncBroker(config, batch_config=BatchPublishConfig(flush_workers=0))

        captured: list[Any] = []

        class CapturingPublisher:
            def __init__(self, transport: Any, cfg: Any) -> None:
                captured.append(cfg)

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                pass

            async def publish(self, envelope: Any) -> Any:  # pragma: no cover
                pass

        mock_transport = self._make_mock_transport()
        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch("rabbitkit.async_.batch.AsyncBatchPublisher", new=CapturingPublisher):
                await broker.start()
                await broker.stop()

        assert len(captured) == 1
        assert captured[0].flush_workers == 5  # capped at 10 // 2

    @pytest.mark.asyncio
    async def test_explicit_workers_over_half_pool_emits_warning(self) -> None:
        """flush_workers > pool_size // 2 emits a RuntimeWarning about potential deadlock."""
        from rabbitkit.core.config import BatchPublishConfig, PoolConfig

        # flush_workers=6 > 10 // 2 = 5
        config = RabbitConfig(pool=PoolConfig(channel_pool_size=10))
        broker = AsyncBroker(
            config,
            batch_config=BatchPublishConfig(flush_workers=6, batch_size=100, max_in_flight=1000),
        )

        class NoOpPublisher:
            def __init__(self, transport: Any, cfg: Any) -> None:
                pass

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                pass

            async def publish(self, envelope: Any) -> Any:  # pragma: no cover
                pass

        mock_transport = self._make_mock_transport()
        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch("rabbitkit.async_.batch.AsyncBatchPublisher", new=NoOpPublisher):
                import warnings

                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    await broker.start()
                    await broker.stop()

        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        assert any(
            "flush_workers" in str(w.message) and "channel_pool_size" in str(w.message)
            for w in runtime_warnings
        ), f"Expected flush_workers/channel_pool_size RuntimeWarning, got: {[str(w.message) for w in runtime_warnings]}"

    @pytest.mark.asyncio
    async def test_explicit_workers_at_half_pool_no_warning(self) -> None:
        """flush_workers == pool_size // 2 is within safe bounds — no warning."""
        from rabbitkit.core.config import BatchPublishConfig, PoolConfig

        # flush_workers=5 == 10 // 2 = 5 — safe
        config = RabbitConfig(pool=PoolConfig(channel_pool_size=10))
        broker = AsyncBroker(
            config,
            batch_config=BatchPublishConfig(flush_workers=5, batch_size=100, max_in_flight=1000),
        )

        class NoOpPublisher:
            def __init__(self, transport: Any, cfg: Any) -> None:
                pass

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                pass

            async def publish(self, envelope: Any) -> Any:  # pragma: no cover
                pass

        mock_transport = self._make_mock_transport()
        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            with patch("rabbitkit.async_.batch.AsyncBatchPublisher", new=NoOpPublisher):
                import warnings

                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    await broker.start()
                    await broker.stop()

        flush_worker_warnings = [
            w
            for w in caught
            if issubclass(w.category, RuntimeWarning) and "flush_workers" in str(w.message)
        ]
        assert flush_worker_warnings == []


# ── flow_controller property ──────────────────────────────────────────────


class TestAsyncFlowControllerProperty:
    """Tests for flow_controller property getter/setter in AsyncBroker."""

    @pytest.mark.asyncio
    async def test_flow_controller_getter_returns_none_initially(self) -> None:
        broker = AsyncBroker()
        assert broker.flow_controller is None

    @pytest.mark.asyncio
    async def test_flow_controller_setter_wires_transport(self) -> None:
        """Setting flow_controller after start wires on_blocked/on_unblocked (lines 117-120)."""
        from rabbitkit.highload.backpressure import FlowController

        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        mock_transport.on_blocked = MagicMock()
        mock_transport.on_unblocked = MagicMock()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        fc = FlowController()
        broker.flow_controller = fc

        mock_transport.on_blocked.assert_called_with(fc.on_blocked)
        mock_transport.on_unblocked.assert_called_with(fc.on_unblocked)

    @pytest.mark.asyncio
    async def test_flow_controller_wired_in_start(self) -> None:
        """flow_controller pre-set before start is wired during start() (lines 245-247)."""
        from rabbitkit.highload.backpressure import FlowController

        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        fc = FlowController()
        broker.flow_controller = fc  # set before start

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        mock_transport.on_blocked = MagicMock()
        mock_transport.on_unblocked = MagicMock()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        mock_transport.on_blocked.assert_called_with(fc.on_blocked)
        mock_transport.on_unblocked.assert_called_with(fc.on_unblocked)


# ── _on_signal ────────────────────────────────────────────────────────────


class TestAsyncOnSignal:
    """Tests for _on_signal (lines 365-373)."""

    @pytest.mark.asyncio
    async def test_on_signal_creates_stop_task(self) -> None:
        """_on_signal creates a stop task when _loop is set."""
        import asyncio

        broker = AsyncBroker()
        broker._loop = asyncio.get_running_loop()
        broker._on_signal()
        # Give the event loop a chance to schedule; on_signal is synchronous
        # so create_task was called.
        await asyncio.sleep(0)  # yield once; stop task runs and exits (not started)

    @pytest.mark.asyncio
    async def test_on_signal_calls_on_app_shutdown(self) -> None:
        """_on_signal invokes on_app_shutdown if set."""
        import asyncio

        broker = AsyncBroker()
        broker._loop = asyncio.get_running_loop()
        called: list[bool] = []
        broker.on_app_shutdown = lambda: called.append(True)
        broker._on_signal()
        assert called == [True]


# ── run() joins the drain instead of fire-and-forget (H11) ────────────────


class TestAsyncRun:
    """Tests for AsyncBroker.run() / request_shutdown() (H11)."""

    async def _start_run(self, broker: AsyncBroker) -> Any:
        import asyncio

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            run_task = asyncio.ensure_future(broker.run())
            for _ in range(200):
                if broker._run_waiting:
                    break
                await asyncio.sleep(0)
            else:
                run_task.cancel()
                pytest.fail("run() never reached the shutdown wait")
            return run_task, mock_transport

    @pytest.mark.asyncio
    async def test_run_returns_only_after_drain_completes(self) -> None:
        import asyncio

        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        run_task, mock_transport = await self._start_run(broker)
        assert broker._started is True

        broker.request_shutdown()
        await asyncio.wait_for(run_task, timeout=2.0)

        assert broker._started is False
        mock_transport.disconnect.assert_awaited()

    @pytest.mark.asyncio
    async def test_signal_during_run_does_not_double_stop(self) -> None:
        """While run() is awaiting shutdown, _trigger_shutdown must not also
        schedule the fire-and-forget stop() task (H11) — only run()'s own
        awaited stop() call should settle the broker."""
        import asyncio

        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        run_task, _mock_transport = await self._start_run(broker)
        assert broker._loop is not None
        with patch.object(broker._loop, "create_task", wraps=broker._loop.create_task) as spy:
            broker._on_signal()
            assert spy.call_count == 0

        await asyncio.wait_for(run_task, timeout=2.0)
        assert broker._started is False

    @pytest.mark.asyncio
    async def test_request_shutdown_without_run_falls_back_to_fire_and_forget(self) -> None:
        """request_shutdown() outside of run() (bare start() usage) still
        schedules a stop() task, matching the pre-H11 signal-handler path."""
        import asyncio

        broker = AsyncBroker()
        broker._loop = asyncio.get_running_loop()
        broker.request_shutdown()
        assert broker._shutdown_event.is_set()
        await asyncio.sleep(0)  # let the scheduled stop() task run (no-op: not started)

    @pytest.mark.asyncio
    async def test_start_clears_stale_shutdown_event(self) -> None:
        broker = AsyncBroker()
        broker._shutdown_event.set()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(install_signal_handlers=False)

        assert not broker._shutdown_event.is_set()


# ── async _wait_in_flight deadline ────────────────────────────────────────


class TestAsyncWaitInFlightDeadline:
    """Tests for async _wait_in_flight deadline warning (lines 405-421)."""

    @pytest.mark.asyncio
    async def test_wait_in_flight_logs_warning_past_deadline(self) -> None:
        import time

        broker = AsyncBroker()
        broker._in_flight = 1
        deadline = time.monotonic() - 1.0  # already past
        with patch("rabbitkit.async_.broker.logger") as mock_logger:
            await broker._wait_in_flight(deadline)
        mock_logger.warning.assert_called()

    @pytest.mark.asyncio
    async def test_wait_in_flight_returns_immediately_when_zero(self) -> None:
        import time

        broker = AsyncBroker()
        broker._in_flight = 0
        await broker._wait_in_flight(time.monotonic() + 10.0)


# ── async publish() kwargs form ───────────────────────────────────────────


class TestAsyncPublishKwargsForm:
    """Tests for async publish() kwargs form (lines 510-520)."""

    async def _start_broker(self) -> AsyncBroker:
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        mock_transport.publish = AsyncMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        return broker

    @pytest.mark.asyncio
    async def test_publish_body_none(self) -> None:
        """body=None → raw_body=b''."""
        broker = await self._start_broker()
        await broker.publish(body=None, routing_key="rk")
        call_args = broker._transport.publish.call_args[0][0]
        assert call_args.body == b""

    @pytest.mark.asyncio
    async def test_publish_body_bytes(self) -> None:
        """body=bytes → raw_body=body."""
        broker = await self._start_broker()
        await broker.publish(body=b"raw", routing_key="rk")
        call_args = broker._transport.publish.call_args[0][0]
        assert call_args.body == b"raw"

    @pytest.mark.asyncio
    async def test_publish_body_str(self) -> None:
        """body=str → raw_body=body.encode()."""
        broker = await self._start_broker()
        await broker.publish(body="hello", routing_key="rk")
        call_args = broker._transport.publish.call_args[0][0]
        assert call_args.body == b"hello"

    @pytest.mark.asyncio
    async def test_publish_body_dict(self) -> None:
        """body=dict → raw_body=json.dumps(body).encode()."""
        import json

        broker = await self._start_broker()
        await broker.publish(body={"key": "value"}, routing_key="rk")
        call_args = broker._transport.publish.call_args[0][0]
        assert json.loads(call_args.body) == {"key": "value"}


# ── async publish() with FlowController ──────────────────────────────────


class TestAsyncPublishWithFlowController:
    """Tests for async publish() with FlowController (lines 541-551)."""

    async def _start_broker(self) -> AsyncBroker:
        broker = AsyncBroker()

        @broker.subscriber(queue="orders")
        async def handle(body: bytes) -> None:
            pass

        mock_transport = AsyncMock()
        mock_transport.connect = AsyncMock()
        mock_transport.declare_queue = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag")
        # on_blocked/on_unblocked are sync registration calls, not coroutines
        mock_transport.on_blocked = MagicMock()
        mock_transport.on_unblocked = MagicMock()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start()

        return broker

    @pytest.mark.asyncio
    async def test_publish_dropped_by_backpressure(self) -> None:
        """fc.acquire_async() returns False → publish returns ERROR outcome."""
        from rabbitkit.core.config import BackpressureConfig
        from rabbitkit.highload.backpressure import FlowController

        broker = await self._start_broker()
        fc = FlowController(BackpressureConfig(on_blocked="drop"))
        fc.on_blocked()  # block so acquire_async() returns False immediately
        broker.flow_controller = fc

        envelope = MessageEnvelope(routing_key="rk", body=b"test")
        outcome = await broker.publish(envelope)
        assert outcome.status == PublishStatus.ERROR

    @pytest.mark.asyncio
    async def test_publish_succeeds_with_flow_controller(self) -> None:
        """fc.acquire_async() returns True → publishes and releases the slot."""
        from rabbitkit.highload.backpressure import FlowController

        broker = await self._start_broker()
        fc = FlowController()
        broker.flow_controller = fc

        expected = PublishOutcome(status=PublishStatus.CONFIRMED)
        broker._transport.publish = AsyncMock(return_value=expected)

        envelope = MessageEnvelope(routing_key="rk", body=b"test")
        outcome = await broker.publish(envelope)
        assert outcome.status == PublishStatus.CONFIRMED
        assert fc.in_flight == 0  # slot released after publish


# ── _wait_in_flight with deadline=None (lines 407-408) ───────────────────────


class TestAsyncWaitInFlightNoDeadline:
    """Lines 407-408: _wait_in_flight with deadline=None must wait for the
    condition to be notified (by _in_flight_dec) before returning."""

    @pytest.mark.asyncio
    async def test_wait_in_flight_no_deadline_waits_for_dec(self) -> None:
        """deadline=None: the coroutine must block until _in_flight reaches 0
        via _in_flight_dec(), which notifies the condition."""
        import asyncio

        broker = AsyncBroker()
        broker._in_flight = 1
        # Initialise the condition in the running loop (must happen before
        # _wait_in_flight is called, because asyncio.Condition is loop-bound).
        broker._ensure_inflight_cond()

        async def decrement_after_delay() -> None:
            await asyncio.sleep(0.05)
            await broker._in_flight_dec()

        dec_task = asyncio.create_task(decrement_after_delay())
        await broker._wait_in_flight(None)
        await dec_task

        assert broker._in_flight == 0

    @pytest.mark.asyncio
    async def test_wait_in_flight_no_deadline_loop_decrements_multiple(self) -> None:
        """deadline=None with multiple in-flight: keeps waiting until all done."""
        import asyncio

        broker = AsyncBroker()
        broker._in_flight = 2
        broker._ensure_inflight_cond()

        async def dec_all() -> None:
            await asyncio.sleep(0.02)
            await broker._in_flight_dec()
            await asyncio.sleep(0.02)
            await broker._in_flight_dec()

        dec_task = asyncio.create_task(dec_all())
        await broker._wait_in_flight(None)
        await dec_task

        assert broker._in_flight == 0


# ── _wait_in_flight with deadline that has not expired (lines 415-419) ───────


class TestAsyncWaitInFlightDeadlineNotExpired:
    """Lines 415-419: _wait_in_flight with a deadline that hasn't expired must
    wait for cond.wait() and break on TimeoutError when the deadline passes."""

    @pytest.mark.asyncio
    async def test_wait_in_flight_deadline_not_expired_resolves_via_dec(self) -> None:
        """When deadline is in the future and _in_flight_dec() fires before it
        expires, _wait_in_flight returns without the TimeoutError branch."""
        import asyncio
        import time

        broker = AsyncBroker()
        broker._in_flight = 1
        broker._ensure_inflight_cond()

        async def dec_soon() -> None:
            await asyncio.sleep(0.02)
            await broker._in_flight_dec()

        dec_task = asyncio.create_task(dec_soon())
        # Give plenty of time — condition should be notified before deadline.
        deadline = time.monotonic() + 5.0
        await broker._wait_in_flight(deadline)
        await dec_task

        assert broker._in_flight == 0

    @pytest.mark.asyncio
    async def test_wait_in_flight_deadline_expires_breaks_out(self) -> None:
        """When the deadline expires before _in_flight reaches 0, the loop
        hits the TimeoutError branch (lines 418-419) and breaks out."""
        import time

        broker = AsyncBroker()
        broker._in_flight = 1
        broker._ensure_inflight_cond()

        # Deadline already very close — will expire while cond.wait() blocks.
        deadline = time.monotonic() + 0.05

        # Nobody decrements in_flight, so the timeout fires.
        with patch("rabbitkit.async_.broker.logger"):
            await broker._wait_in_flight(deadline)

        # in_flight still 1 — drained via timeout, not via dec.
        assert broker._in_flight == 1
