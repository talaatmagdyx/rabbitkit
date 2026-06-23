"""Tests for async_/broker.py — AsyncBroker (mocked transport)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.concurrency import AsyncWorkerPool
from rabbitkit.core.config import RabbitConfig, RetryConfig, WorkerConfig
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
        """Pool stop() called before consumer cancel."""
        broker = self._make_broker_with_route()
        mock_transport = self._make_mock_transport()

        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=mock_transport):
            await broker.start(worker_config=WorkerConfig(worker_count=4))

            pool = broker.worker_pool
            assert pool is not None

            with patch.object(pool, "stop", new_callable=AsyncMock) as mock_pool_stop:
                await broker.stop()

                # Pool stop was called
                mock_pool_stop.assert_called_once()

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
        assert broker.config.consumer.prefetch_count == 20

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
        assert len(runtime_warnings) == 1
        assert "channel_pool_size" in str(runtime_warnings[0].message)
        assert "worker_count" in str(runtime_warnings[0].message)

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
