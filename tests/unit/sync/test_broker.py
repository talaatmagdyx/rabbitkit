"""Tests for sync/broker.py — SyncBroker (mocked transport)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.concurrency import SyncWorkerPool
from rabbitkit.core.config import RabbitConfig, WorkerConfig
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.sync.broker import SyncBroker

# ── helpers ───────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _check_pika() -> None:
    pytest.importorskip("pika")


# ── Registration ─────────────────────────────────────────────────────────


class TestRegistration:
    def test_subscriber_registers_route(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders", exchange="events")
        def handle(body: bytes) -> None:
            pass

        assert len(broker.routes) == 1
        assert broker.routes[0].queue.name == "orders"

    def test_publisher_decorator(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        def handle(body: bytes) -> str:
            return "ok"

        route = broker.routes[0]
        assert route.result_publisher is not None

    def test_include_router(self) -> None:
        from rabbitkit.core.router import RabbitRouter

        router = RabbitRouter(prefix="orders")

        @router.subscriber(queue="orders-queue", routing_key="created")
        def handle(body: bytes) -> None:
            pass

        broker = SyncBroker()
        broker.include_router(router)

        assert len(broker.routes) == 1


# ── Lifecycle ────────────────────────────────────────────────────────────


class TestLifecycle:
    def test_start_connects_and_declares(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders", exchange="events")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                broker.start()

                # Should declare exchange and queue
                assert mock_channel.exchange_declare.called
                assert mock_channel.queue_declare.called
                assert mock_channel.queue_bind.called
                assert mock_channel.basic_consume.called

    def test_start_idempotent(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = MagicMock()

                broker.start()
                broker.start()  # second call should be no-op

                assert mock_conn.call_count == 1

    def test_stop(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                broker.start()
                broker.stop()

                # Should cancel consumer and close
                mock_channel.basic_cancel.assert_called_once()


# ── Config ───────────────────────────────────────────────────────────────


class TestConfig:
    def test_default_config(self) -> None:
        broker = SyncBroker()
        assert broker.config is not None
        assert broker.config.connection.host == "localhost"

    def test_custom_config(self) -> None:
        config = RabbitConfig()
        broker = SyncBroker(config=config)
        assert broker.config is config

    def test_publish_requires_start(self) -> None:
        broker = SyncBroker()

        with pytest.raises(RuntimeError, match="not started"):
            broker.publish(MessageEnvelope(routing_key="rk", body=b"hello"))


# ── WorkerPool integration ───────────────────────────────────────────────


class TestSyncBrokerWorkerPool:
    """Tests for WorkerPool integration in SyncBroker."""

    def _make_broker_with_route(self) -> SyncBroker:
        """Create a SyncBroker with one subscriber route registered."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders", exchange="events")
        def handle(body: bytes) -> None:
            pass

        return broker

    def _start_broker(
        self,
        broker: SyncBroker,
        worker_config: WorkerConfig | None = None,
    ) -> MagicMock:
        """Start the broker with mocked pika, return the mock channel."""
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                broker.start(worker_config=worker_config)
                return mock_channel

    def test_start_without_worker_config(self) -> None:
        """Default start creates no worker pool."""
        broker = self._make_broker_with_route()
        self._start_broker(broker)

        assert broker.worker_pool is None

    def test_start_with_worker_config(self) -> None:
        """start(worker_config=WorkerConfig(worker_count=4)) creates pool."""
        broker = self._make_broker_with_route()
        self._start_broker(broker, worker_config=WorkerConfig(worker_count=4))

        assert broker.worker_pool is not None
        assert isinstance(broker.worker_pool, SyncWorkerPool)
        assert broker.worker_pool.worker_count == 4

    def test_worker_pool_stops_on_broker_stop(self) -> None:
        """Pool stop() called before consumer cancel."""
        broker = self._make_broker_with_route()
        mock_channel = self._start_broker(
            broker, worker_config=WorkerConfig(worker_count=4)
        )

        pool = broker.worker_pool
        assert pool is not None

        with patch.object(pool, "stop", wraps=pool.stop) as mock_pool_stop:
            broker.stop()

            # Pool stop was called
            mock_pool_stop.assert_called_once()

        # After stop, worker_pool should be None
        assert broker.worker_pool is None
        # Consumer was also cancelled
        mock_channel.basic_cancel.assert_called_once()

    def test_worker_pool_property(self) -> None:
        """broker.worker_pool returns the pool."""
        broker = SyncBroker()
        # Before start, pool is None
        assert broker.worker_pool is None

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        self._start_broker(broker, worker_config=WorkerConfig(worker_count=2))

        # After start with workers, pool is set
        assert broker.worker_pool is not None
        assert broker.worker_pool.worker_count == 2

    def test_prefetch_override_with_worker_config(self) -> None:
        """prefetch_per_worker overrides prefetch_count."""
        broker = self._make_broker_with_route()
        mock_channel = self._start_broker(
            broker,
            worker_config=WorkerConfig(worker_count=4, prefetch_per_worker=5),
        )

        # prefetch_count should now be 4 * 5 = 20
        assert broker.config.consumer.prefetch_count == 20

        # The consume call should have used the overridden prefetch
        consume_call = mock_channel.basic_consume.call_args
        assert consume_call is not None

    def test_worker_count_1_no_pool(self) -> None:
        """worker_count=1 does NOT create a pool (treated as single-worker)."""
        broker = self._make_broker_with_route()
        self._start_broker(broker, worker_config=WorkerConfig(worker_count=1))

        assert broker.worker_pool is None


# ── Per-Route Prefetch ──────────────────────────────────────────────────


class TestPerRoutePrefetch:
    """Tests for per-route prefetch_count override in SyncBroker."""

    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def _start_broker(self, broker: SyncBroker) -> MagicMock:
        """Start the broker with mocked pika, return the mock channel."""
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                broker.start()
                return mock_channel

    def test_per_route_prefetch_used(self) -> None:
        """Per-route prefetch_count overrides global prefetch."""
        config = RabbitConfig()
        broker = SyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events", prefetch_count=50)
        def handle(body: bytes) -> None:
            pass

        mock_channel = self._start_broker(broker)

        # basic_qos should have been called with the per-route prefetch
        mock_channel.basic_qos.assert_called_with(prefetch_count=50)

    def test_global_prefetch_when_no_override(self) -> None:
        """Global prefetch_count used when route has no override."""
        config = RabbitConfig()
        broker = SyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events")
        def handle(body: bytes) -> None:
            pass

        mock_channel = self._start_broker(broker)

        # basic_qos should use the global prefetch_count (default is 10)
        mock_channel.basic_qos.assert_called_with(
            prefetch_count=config.consumer.prefetch_count,
        )

    def test_route_prefetch_stored_on_route(self) -> None:
        """prefetch_count is stored on the route definition."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders", prefetch_count=25)
        def handle(body: bytes) -> None:
            pass

        assert broker.routes[0].prefetch_count == 25


# ── Exchange-to-Exchange Binding ────────────────────────────────────────


class TestExchangeToExchangeBinding:
    """Tests for exchange-to-exchange binding wiring in SyncBroker."""

    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def _start_broker(self, broker: SyncBroker) -> MagicMock:
        """Start the broker with mocked pika, return the mock channel."""
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                broker.start()
                return mock_channel

    def test_exchange_binding_called_when_bind_to_set(self) -> None:
        """exchange_bind is called when exchange has bind_to set."""
        from rabbitkit.core.topology import RabbitExchange

        exchange = RabbitExchange(name="child", bind_to="parent", routing_key="rk")
        broker = SyncBroker()

        @broker.subscriber(queue="orders", exchange=exchange)
        def handle(body: bytes) -> None:
            pass

        mock_channel = self._start_broker(broker)

        mock_channel.exchange_bind.assert_called_once_with(
            destination="child",
            source="parent",
            routing_key="rk",
            arguments=None,
        )

    def test_exchange_binding_not_called_when_no_bind_to(self) -> None:
        """exchange_bind is NOT called when exchange has no bind_to."""
        from rabbitkit.core.topology import RabbitExchange

        exchange = RabbitExchange(name="events")
        broker = SyncBroker()

        @broker.subscriber(queue="orders", exchange=exchange)
        def handle(body: bytes) -> None:
            pass

        mock_channel = self._start_broker(broker)

        mock_channel.exchange_bind.assert_not_called()

    def test_exchange_binding_with_arguments(self) -> None:
        """exchange_bind passes bind_arguments when set."""
        from rabbitkit.core.topology import RabbitExchange

        exchange = RabbitExchange(
            name="child",
            bind_to="parent",
            routing_key="rk",
            bind_arguments={"x-match": "all"},
        )
        broker = SyncBroker()

        @broker.subscriber(queue="orders", exchange=exchange)
        def handle(body: bytes) -> None:
            pass

        mock_channel = self._start_broker(broker)

        mock_channel.exchange_bind.assert_called_once_with(
            destination="child",
            source="parent",
            routing_key="rk",
            arguments={"x-match": "all"},
        )


# ── Logging config ──────────────────────────────────────────────────────────


class TestLoggingConfig:
    """Tests for configure_structlog branch on broker.start()."""

    def _start_broker_with_logging(self, broker: SyncBroker) -> None:
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

    def test_start_calls_configure_structlog_when_logging_set(self) -> None:
        """start() calls configure_structlog when config.logging is set."""
        from rabbitkit.core.logging import LoggingConfig

        config = RabbitConfig()
        config.logging = LoggingConfig(render_json=False)
        broker = SyncBroker(config)

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.core.logging.configure_structlog") as mock_cfg:
            self._start_broker_with_logging(broker)
            mock_cfg.assert_called_once_with(config.logging)

    def test_start_no_configure_structlog_when_logging_none(self) -> None:
        """start() does not call configure_structlog when config.logging is None."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.core.logging.configure_structlog") as mock_cfg:
            self._start_broker_with_logging(broker)
            mock_cfg.assert_not_called()


# ── Stop edge cases ────────────────────────────────────────────────────────


class TestStopEdgeCases:
    """Tests for stop() edge cases."""

    def test_stop_when_not_started_is_noop(self) -> None:
        """stop() is a no-op when broker has not been started."""
        broker = SyncBroker()
        # Should not raise
        broker.stop()
        assert broker._started is False

    def test_stop_closes_rpc_client(self) -> None:
        """stop() calls close() on the RPC client if one was set."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        # Inject a mock RPC client
        mock_rpc = MagicMock()
        broker._rpc_client = mock_rpc

        broker.stop()

        mock_rpc.close.assert_called_once()
        assert broker._rpc_client is None


# ── run() method ───────────────────────────────────────────────────────────


class TestRunMethod:
    """Tests for broker.run() lifecycle."""

    def _patch_pika(self) -> tuple:
        return (
            patch("rabbitkit.sync.transport.make_pika_connection_params"),
            patch("pika.BlockingConnection"),
        )

    def _make_run_broker(self) -> tuple:
        """Helper to create a started broker with mocked transport for run() tests."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        return broker, mock_channel

    def test_run_calls_start_consuming_and_stop(self) -> None:
        """run() calls start_consuming() then stop()."""
        broker, _ = self._make_run_broker()

        assert broker._transport is not None
        # Patch start_consuming to return immediately
        broker._transport.start_consuming = MagicMock()
        # broker is already started; run() → start() (no-op) → start_consuming → stop
        broker.run()

        broker._transport.start_consuming.assert_called_once()
        assert broker._started is False  # stop() was called

    def test_run_handles_keyboard_interrupt(self) -> None:
        """run() handles KeyboardInterrupt gracefully."""
        broker, _ = self._make_run_broker()

        assert broker._transport is not None
        broker._transport.start_consuming = MagicMock(side_effect=KeyboardInterrupt)

        broker.run()  # should not raise

        assert broker._started is False  # stop() was called

    def test_run_no_transport_skips_consuming(self) -> None:
        """run() skips start_consuming if transport is None."""
        broker, _ = self._make_run_broker()

        # Broker is started. Replace transport with None to test the branch.
        broker._transport = None

        # stop() asserts transport is not None, so patch it to avoid that
        with patch.object(broker, "stop") as mock_stop:
            broker.run()
            mock_stop.assert_called_once()


# ── publish / request edge cases ──────────────────────────────────────────


class TestPublishRequestEdgeCases:
    """Tests for publish() and request() edge cases."""

    def test_publish_success(self) -> None:
        """publish() calls transport.publish() when broker is started."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        envelope = MessageEnvelope(routing_key="rk", body=b"hello")
        transport = broker._transport
        assert transport is not None
        transport.publish = MagicMock(return_value=MagicMock())

        broker.publish(envelope)
        transport.publish.assert_called_once_with(envelope)

    def test_request_raises_when_not_started(self) -> None:
        """request() raises RuntimeError when broker is not started."""
        broker = SyncBroker()
        with pytest.raises(RuntimeError, match="not started"):
            broker.request("rk", b"body")

    def test_request_lazy_init_rpc_client(self) -> None:
        """request() lazily creates RPCClient on first call."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        assert broker._rpc_client is None

        # Patch RPCClient so we don't do real network
        mock_rpc_class = MagicMock()
        mock_rpc_instance = MagicMock()
        mock_rpc_instance.call.return_value = MagicMock()
        mock_rpc_class.return_value = mock_rpc_instance

        with patch("rabbitkit.rpc.RPCClient", mock_rpc_class):
            broker.request("rk", b"body", timeout=1.0, exchange="ex")

        # RPCClient was created and call() invoked
        mock_rpc_class.assert_called_once_with(broker._transport)
        mock_rpc_instance.call.assert_called_once_with(
            "rk", b"body", timeout=1.0, exchange="ex", headers=None
        )
        # Client is cached
        assert broker._rpc_client is mock_rpc_instance

    def test_request_reuses_existing_rpc_client(self) -> None:
        """request() reuses existing _rpc_client on subsequent calls."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        # Pre-set a mock RPC client
        mock_rpc = MagicMock()
        mock_rpc.call.return_value = MagicMock()
        broker._rpc_client = mock_rpc

        with patch("rabbitkit.rpc.RPCClient") as mock_rpc_class:
            broker.request("rk", b"data")

        # Should NOT have created a new RPCClient
        mock_rpc_class.assert_not_called()
        mock_rpc.call.assert_called_once()


# ── _declare_topology edge cases ──────────────────────────────────────────


class TestDeclareTopologyEdgeCases:
    """Tests for _declare_topology() edge cases."""

    def test_declare_topology_noop_when_no_transport(self) -> None:
        """_declare_topology() returns early when _transport is None."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        # Transport not set
        assert broker._transport is None
        broker._declare_topology()  # should not raise

    def test_declare_topology_with_retry_config(self) -> None:
        """_declare_topology() declares DLQ topology when retry is configured."""
        from rabbitkit.core.config import RetryConfig

        config = RabbitConfig()
        config.retry = RetryConfig(max_retries=2, delays=(5, 30))
        broker = SyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        # queue_declare called multiple times: source queue + 2 delay queues + DLQ
        # (at minimum 4 declare calls)
        assert mock_channel.queue_declare.call_count >= 3

    def test_declare_topology_retry_no_exchange(self) -> None:
        """_declare_topology() handles retry config without exchange."""
        from rabbitkit.core.config import RetryConfig

        config = RabbitConfig()
        config.retry = RetryConfig(max_retries=1, delays=(5,))
        broker = SyncBroker(config)

        @broker.subscriber(queue="no-exchange-queue")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        # Should not raise — delay queue declared with empty exchange_name
        assert mock_channel.queue_declare.call_count >= 2


# ── _start_consumer edge cases ────────────────────────────────────────────


class TestStartConsumerEdgeCases:
    """Tests for _start_consumer() edge cases."""

    def test_start_consumer_noop_when_no_transport(self) -> None:
        """_start_consumer() returns early when _transport is None."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        assert broker._transport is None
        route = broker.routes[0]
        broker._start_consumer(route)  # should not raise, returns immediately

    def test_on_message_sets_original_queue_header(self) -> None:
        """on_message sets x-rabbitkit-original-queue header if absent.

        We capture the callback passed to transport.consume() by patching
        the SyncTransport.consume method, then invoke it directly.
        """
        from rabbitkit.core.message import RabbitMessage

        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        captured_callback = {}

        def fake_consume(queue, callback, prefetch=10):
            captured_callback["fn"] = callback
            return "consumer-tag-test"

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        assert broker._transport is not None
        broker._transport.consume = fake_consume

        # Reset so _start_consumer runs again with our fake
        broker._started = False
        route = broker.routes[0]
        route.consumer_tag = None

        # Patch pipeline to capture the message
        pipeline_calls = []

        def mock_process_sync(r, msg, publish_fn=None):
            pipeline_calls.append(msg)

        broker._pipeline.process_sync = mock_process_sync

        # Re-start the consumer registration
        broker._start_consumer(route)

        assert "fn" in captured_callback, "consume() was never called"
        callback = captured_callback["fn"]

        # Invoke the callback with a message lacking the header
        msg = RabbitMessage(body=b"hello", routing_key="orders", headers={})
        callback(msg)

        assert msg.headers["x-rabbitkit-original-queue"] == "orders"
        assert pipeline_calls[0] is msg

    def test_on_message_skips_header_if_already_set(self) -> None:
        """on_message does NOT overwrite existing x-rabbitkit-original-queue."""
        from rabbitkit.core.message import RabbitMessage

        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        captured_callback = {}

        def fake_consume(queue, callback, prefetch=10):
            captured_callback["fn"] = callback
            return "consumer-tag-test"

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        assert broker._transport is not None
        broker._transport.consume = fake_consume

        broker._started = False
        route = broker.routes[0]
        route.consumer_tag = None

        pipeline_calls = []

        def mock_process_sync(r, msg, publish_fn=None):
            pipeline_calls.append(msg)

        broker._pipeline.process_sync = mock_process_sync
        broker._start_consumer(route)

        callback = captured_callback["fn"]

        msg = RabbitMessage(
            body=b"hello",
            routing_key="orders",
            headers={"x-rabbitkit-original-queue": "already-set"},
        )
        callback(msg)
        assert msg.headers["x-rabbitkit-original-queue"] == "already-set"

    def test_pooled_callback_submits_to_pool(self) -> None:
        """When pool is set, on_message_pooled submits work to the pool."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        captured_callback = {}

        def fake_consume(queue, callback, prefetch=10):
            captured_callback["fn"] = callback
            return "consumer-tag-pooled"

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start(worker_config=WorkerConfig(worker_count=4))

        assert broker.worker_pool is not None
        assert broker._transport is not None

        broker._transport.consume = fake_consume

        # Reset so _start_consumer re-registers
        route = broker.routes[0]
        route.consumer_tag = None
        broker._start_consumer(route)

        callback = captured_callback["fn"]

        from rabbitkit.core.message import RabbitMessage

        msg = RabbitMessage(body=b"data", routing_key="orders", headers={})

        with patch.object(broker.worker_pool, "submit") as mock_submit:
            callback(msg)

        mock_submit.assert_called_once()


# ── worker_count warning ───────────────────────────────────────────────────


class TestWorkerCountWarning:
    """Test that a warning is emitted when worker_count > channel_pool_size."""

    def test_warns_when_worker_count_exceeds_pool_size(self) -> None:
        """RuntimeWarning emitted when worker_count > channel_pool_size."""
        from rabbitkit.core.config import PoolConfig

        config = RabbitConfig()
        config.pool = PoolConfig(channel_pool_size=2)
        broker = SyncBroker(config)

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                import warnings

                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    broker.start(worker_config=WorkerConfig(worker_count=5))

        assert any(issubclass(warning.category, RuntimeWarning) for warning in w)
        warning_messages = [str(warning.message) for warning in w]
        assert any("worker_count" in msg and "channel_pool_size" in msg for msg in warning_messages)
