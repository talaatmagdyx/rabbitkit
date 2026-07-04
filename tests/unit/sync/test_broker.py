"""Tests for sync/broker.py — SyncBroker (mocked transport)."""

from __future__ import annotations

import warnings
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.concurrency import SyncWorkerPool
from rabbitkit.core.config import (
    RETRY_DISABLED,
    ConnectionConfig,
    PublisherConfig,
    RabbitConfig,
    WorkerConfig,
)
from rabbitkit.core.errors import BackpressureError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
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

    def test_start_initializes_heartbeat_immediately(self) -> None:
        """L14: last_heartbeat is set at start(), not left None until the
        first message/tick -- so a broker wedged from the very start is
        still caught by health.broker_liveness's staleness check."""
        broker = SyncBroker()
        assert broker.last_heartbeat is None

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = MagicMock()
                broker.start()

        assert broker.last_heartbeat is not None

    def test_transport_io_tick_refreshes_broker_heartbeat(self) -> None:
        """L14: the transport's per-tick callback is wired to the broker's
        heartbeat, independent of message delivery."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = MagicMock()
                broker.start()

        assert broker._transport is not None
        first = broker.last_heartbeat
        assert first is not None

        with patch("rabbitkit.sync.broker.time") as mock_time:
            mock_time.monotonic.return_value = first + 100.0
            broker._transport._fire_io_tick()

        assert broker.last_heartbeat == first + 100.0


class TestPumpIdle:
    """pump_idle(): idle keep-alive for a publish-only (no active consume
    loop) broker -- reconnects if dead, services pending I/O, refreshes
    the liveness heartbeat."""

    def _start_broker(self) -> SyncBroker:
        broker = SyncBroker()
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()
        return broker

    def test_pump_idle_before_start_is_noop(self) -> None:
        broker = SyncBroker()
        broker.pump_idle()  # must not raise

    def test_pump_idle_calls_ensure_connected_and_pump(self) -> None:
        broker = self._start_broker()
        assert broker._transport is not None

        with (
            patch.object(broker._transport, "ensure_connected") as mock_ensure,
            patch.object(broker._transport, "pump") as mock_pump,
        ):
            broker.pump_idle(time_limit=0.02)

        mock_ensure.assert_called_once()
        mock_pump.assert_called_once_with(0.02)

    def test_pump_idle_refreshes_liveness_heartbeat(self) -> None:
        """A publish-only broker with no consume loop must still see its
        liveness heartbeat advance when pump_idle() is called."""
        broker = self._start_broker()
        first = broker.last_heartbeat
        assert first is not None

        with patch("rabbitkit.sync.broker.time") as mock_time:
            mock_time.monotonic.return_value = first + 100.0
            broker.pump_idle()

        assert broker.last_heartbeat == first + 100.0

    def test_pump_idle_reconnects_dead_connection(self) -> None:
        """ensure_connected() is called before pump() -- a dead connection
        is reconnected proactively rather than only on the next publish."""
        broker = self._start_broker()
        assert broker._transport is not None

        order: list[str] = []
        with (
            patch.object(broker._transport, "ensure_connected", side_effect=lambda: order.append("ensure")),
            patch.object(broker._transport, "pump", side_effect=lambda t: order.append("pump")),
        ):
            broker.pump_idle()

        assert order == ["ensure", "pump"]


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
        """Pool stop() is called, and consumer cancel happens first (C5)."""
        broker = self._make_broker_with_route()
        mock_channel = self._start_broker(broker, worker_config=WorkerConfig(worker_count=4))

        pool = broker.worker_pool
        assert pool is not None

        order: list[str] = []
        real_pool_stop = pool.stop

        def _tracked_basic_cancel(*args: Any, **kwargs: Any) -> None:
            order.append("cancel_consumer")

        def _tracked_pool_stop(*args: Any, **kwargs: Any) -> Any:
            order.append("worker_pool.stop")
            return real_pool_stop(*args, **kwargs)

        mock_channel.basic_cancel.side_effect = _tracked_basic_cancel
        with patch.object(pool, "stop", side_effect=_tracked_pool_stop) as mock_pool_stop:
            broker.stop()

            # Pool stop was called
            mock_pool_stop.assert_called_once()

        # C5: cancel_consumer must precede worker_pool.stop — draining the pool
        # while the consumer is still active lets RabbitMQ deliver new messages
        # into a pool that's mid-shutdown (see SyncBroker.stop() docstring).
        assert order == ["cancel_consumer", "worker_pool.stop"]

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
        assert broker.consumer_config.prefetch_count == 20

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

        config = RabbitConfig(logging=LoggingConfig(render_json=False))
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
        mock_rpc_instance.call.assert_called_once_with("rk", b"body", timeout=1.0, exchange="ex", headers=None)
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

    def test_wire_retry_middleware_noop_when_no_transport(self) -> None:
        """_wire_retry_middleware() returns early when _transport is None."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        assert broker._transport is None
        broker._wire_retry_middleware()  # should not raise

    def test_declare_topology_with_retry_config(self) -> None:
        """_declare_topology() declares DLQ topology when retry is configured."""
        from rabbitkit.core.config import RetryConfig

        config = RabbitConfig(retry=RetryConfig(max_retries=2, delays=(5, 30)))
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

        # C1: retry config must ALSO install RetryMiddleware, not just topology.
        from rabbitkit.middleware.retry import RetryMiddleware

        route = broker.routes[0]
        assert any(isinstance(m, RetryMiddleware) for m in route.route_middlewares), (
            "retry=RetryConfig(...) must install RetryMiddleware on the route"
        )

    def test_declare_topology_retry_no_exchange(self) -> None:
        """_declare_topology() handles retry config without exchange."""
        from rabbitkit.core.config import RetryConfig

        config = RabbitConfig(retry=RetryConfig(max_retries=1, delays=(5,)))
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

    def test_start_with_retry_and_no_confirms_warns(self) -> None:
        """M4: a retry-enabled route on a broker with confirm_delivery=False
        must warn -- RetryMiddleware acks the source as soon as its
        delay-queue republish is SENT (fire-and-forget), not confirmed."""
        from rabbitkit.core.config import PublisherConfig, RetryConfig

        config = RabbitConfig(
            retry=RetryConfig(max_retries=1, delays=(5,)),
            publisher=PublisherConfig(confirm_delivery=False),
        )
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
                with pytest.warns(RuntimeWarning, match="confirm_delivery=False"):
                    broker.start()

    def test_start_with_retry_and_confirms_does_not_warn(self) -> None:
        """M4: confirm_delivery=True (the default) must not trigger the warning."""
        import warnings

        from rabbitkit.core.config import RetryConfig

        config = RabbitConfig(retry=RetryConfig(max_retries=1, delays=(5,)))
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
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    broker.start()
                # M4: no confirm-related warning. (A separate H2 single-worker
                # heartbeat warning is expected here and is not what M4 checks.)
                assert not any("confirm" in str(w.message).lower() for w in caught)

    def test_start_with_result_publisher_and_no_confirms_warns(self) -> None:
        """M4: a route with a @publisher() result forward on a broker with
        confirm_delivery=False must warn -- the pipeline settles the source
        as soon as the result publish is SENT, not confirmed."""
        from rabbitkit.core.config import PublisherConfig

        config = RabbitConfig(publisher=PublisherConfig(confirm_delivery=False))
        broker = SyncBroker(config)

        @broker.subscriber(queue="orders")
        @broker.publisher(exchange="results", routing_key="done")
        def handle(body: bytes) -> str:
            return "ok"

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                with pytest.warns(RuntimeWarning, match="confirm_delivery=False"):
                    broker.start()

    def test_wire_retry_skips_route_without_retry(self) -> None:
        """_wire_retry_middleware() installs nothing on a non-retry route."""
        broker = SyncBroker()  # no broker-wide retry

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

        from rabbitkit.middleware.retry import RetryMiddleware

        route = broker.routes[0]
        assert not any(isinstance(m, RetryMiddleware) for m in route.route_middlewares)

    def test_wire_retry_respects_user_supplied_middleware(self) -> None:
        """_wire_retry_middleware() does not double-wire a user RetryMiddleware."""
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.middleware.retry import RetryMiddleware

        user_mw = RetryMiddleware(RetryConfig(max_retries=2, delays=(5, 30)))
        config = RabbitConfig(retry=RetryConfig(max_retries=2, delays=(5, 30)))
        broker = SyncBroker(config)

        @broker.subscriber(queue="orders", middlewares=[user_mw])
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start()

        route = broker.routes[0]
        retry_mws = [m for m in route.route_middlewares if isinstance(m, RetryMiddleware)]
        assert retry_mws == [user_mw]

    def test_wire_retry_warns_on_middleware_without_topology(self) -> None:
        """A manual RetryMiddleware without retry= warns (no topology declared)."""
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.middleware.retry import RetryMiddleware

        user_mw = RetryMiddleware(RetryConfig(max_retries=2, delays=(5, 30)))
        broker = SyncBroker()  # NOTE: no broker-wide retry

        @broker.subscriber(queue="orders", middlewares=[user_mw])  # NOTE: no retry=
        def handle(body: bytes) -> None:
            pass

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                with pytest.warns(RuntimeWarning, match="no retry topology was declared"):
                    broker.start()


# ── H6: filter_fn without a DLX must not silently drop messages ──────────


class TestFilterWithoutDLX:
    """H6 (now generalized by C3): filter_fn rejections nack(requeue=False) —
    without a DLX RabbitMQ discards them. Under the default
    reject_without_dlx='auto_provision' policy, every rejecting route (filter
    or not) gets an auto-declared '<queue>.dlq', not silent loss."""

    def _filter_fn(self, msg: object) -> bool:
        return True

    def test_filter_without_retry_or_dlx_auto_declares_dlq(self) -> None:
        broker = SyncBroker()  # no broker-wide retry

        @broker.subscriber(queue="orders", filter_fn=self._filter_fn)
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._declare_topology()

        # Source queue re-declared with the auto-DLX pointing at orders.dlq.
        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        source = next(q for q in declared_queues if q.name == "orders")
        assert source.dead_letter_exchange == ""
        assert source.dead_letter_routing_key == "orders.dlq"

        # The DLQ itself was declared as a plain durable queue.
        dlq = next(q for q in declared_queues if q.name == "orders.dlq")
        assert dlq.durable is True

    def test_filter_with_retry_enabled_does_not_double_declare_dlq(self) -> None:
        """When retry IS enabled, RetryRouter already provides a DLX — the
        filter-specific auto-declare path must not also fire."""
        from rabbitkit.core.config import RetryConfig

        config = RabbitConfig(retry=RetryConfig(max_retries=1, delays=(5,)))
        broker = SyncBroker(config)

        @broker.subscriber(queue="orders", filter_fn=self._filter_fn)
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()

        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            broker._declare_topology()

        assert not any("auto-declared" in str(w.message) for w in caught)

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        dlqs = [q for q in declared_queues if q.name == "orders.dlq"]
        assert len(dlqs) == 1, "exactly one orders.dlq must be declared, not two"

    def test_filter_with_manual_dlx_is_respected_no_warning(self) -> None:
        """A manually-configured dead_letter_exchange must be left alone —
        no auto-declare override, no warning."""
        from rabbitkit.core.topology import RabbitQueue

        broker = SyncBroker()

        @broker.subscriber(
            queue=RabbitQueue(name="orders", dead_letter_exchange="my-dlx", dead_letter_routing_key="my-dlq"),
            filter_fn=self._filter_fn,
        )
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()

        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            broker._declare_topology()

        assert not any("auto-declared" in str(w.message) for w in caught)

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        source = next(q for q in declared_queues if q.name == "orders")
        assert source.dead_letter_exchange == "my-dlx"
        assert source.dead_letter_routing_key == "my-dlq"
        assert not any(q.name == "orders.dlq" for q in declared_queues)

    def test_plain_route_also_gets_auto_dlq(self) -> None:
        """C3: a plain route (no filter_fn) can still reject poison messages
        (permanent errors) — under the default auto_provision policy it gets
        a DLQ too, so nothing is silently discarded."""
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._declare_topology()

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        source = next(q for q in declared_queues if q.name == "orders")
        assert source.dead_letter_exchange == ""
        assert source.dead_letter_routing_key == "orders.dlq"
        assert any(q.name == "orders.dlq" for q in declared_queues)


class TestRejectWithoutDLXPolicy:
    """C3: SafetyConfig.reject_without_dlx policy behaviors."""

    def test_error_policy_fails_startup(self) -> None:
        from rabbitkit.core.config import SafetyConfig
        from rabbitkit.core.errors import UnsafeTopologyError

        broker = SyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="error")))

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()

        with pytest.raises(UnsafeTopologyError, match="orders"):
            broker._declare_topology()

    def test_discard_policy_warns_and_declares_no_dlq(self) -> None:
        from rabbitkit.core.config import SafetyConfig

        broker = SyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard")))

        @broker.subscriber(queue="telemetry")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()

        with pytest.warns(RuntimeWarning, match="permanently discarded"):
            broker._declare_topology()

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        assert not any(q.name == "telemetry.dlq" for q in declared_queues)
        source = next(q for q in declared_queues if q.name == "telemetry")
        assert source.dead_letter_exchange is None  # untouched

    def test_per_route_override_beats_broker_default(self) -> None:
        broker = SyncBroker()  # broker default: auto_provision

        @broker.subscriber(queue="telemetry", reject_without_dlx="discard")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()

        with pytest.warns(RuntimeWarning, match="permanently discarded"):
            broker._declare_topology()

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        assert not any(q.name == "telemetry.dlq" for q in declared_queues)

    def test_ack_first_route_without_filter_gets_no_dlq(self) -> None:
        """ACK_FIRST acks before the handler — it can never reject, so no
        DLQ is provisioned and the error policy does not trip."""
        from rabbitkit.core.config import SafetyConfig
        from rabbitkit.core.types import AckPolicy

        broker = SyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="error")))

        @broker.subscriber(queue="fire-and-forget", ack_policy=AckPolicy.ACK_FIRST)
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._declare_topology()  # must NOT raise

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        assert not any(q.name == "fire-and-forget.dlq" for q in declared_queues)

    def test_passive_only_mode_skips_policy(self) -> None:
        """In PASSIVE_ONLY mode rabbitkit does not own queue arguments and
        cannot know whether an external DLX exists — the policy is skipped."""
        from rabbitkit.core.config import SafetyConfig
        from rabbitkit.core.types import TopologyMode

        broker = SyncBroker(
            RabbitConfig(
                topology_mode=TopologyMode.PASSIVE_ONLY,
                safety=SafetyConfig(reject_without_dlx="error"),
            )
        )

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._declare_topology()  # must NOT raise

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        assert not any(q.name == "orders.dlq" for q in declared_queues)

    def test_custom_dlq_suffix(self) -> None:
        from rabbitkit.core.config import SafetyConfig

        broker = SyncBroker(RabbitConfig(safety=SafetyConfig(dlq_suffix=".dead")))

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._declare_topology()

        declared_queues = [call.args[0] for call in broker._transport.declare_queue.call_args_list]
        assert any(q.name == "orders.dead" for q in declared_queues)


class TestBindArguments:
    """C4: queue bind_arguments must reach the transport bind call."""

    def test_bind_arguments_passed_to_transport(self) -> None:
        from rabbitkit.core.topology import RabbitExchange, RabbitQueue
        from rabbitkit.core.types import ExchangeType

        broker = SyncBroker()

        @broker.subscriber(
            queue=RabbitQueue(
                name="orders.headers",
                bind_arguments={"x-match": "all", "type": "order"},
            ),
            exchange=RabbitExchange(name="events.headers", type=ExchangeType.HEADERS),
        )
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._declare_topology()

        broker._transport.bind_queue.assert_called_once_with(
            queue="orders.headers",
            exchange="events.headers",
            routing_key="",
            arguments={"x-match": "all", "type": "order"},
        )

    def test_non_headers_binding_passes_none_arguments(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders", exchange="events", routing_key="orders.created")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._declare_topology()

        kwargs = broker._transport.bind_queue.call_args.kwargs
        assert kwargs["arguments"] is None


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
        route.runtime_state.consumer_tag = None

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
        route.runtime_state.consumer_tag = None

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
        route.runtime_state.consumer_tag = None
        broker._start_consumer(route)

        callback = captured_callback["fn"]

        from rabbitkit.core.message import RabbitMessage

        msg = RabbitMessage(body=b"data", routing_key="orders", headers={})

        with patch.object(broker.worker_pool, "submit") as mock_submit:
            callback(msg)

        mock_submit.assert_called_once()


# ── worker_count warning ───────────────────────────────────────────────────


class TestWorkerCountWarning:
    """M2: the old worker_count > channel_pool_size deadlock warning was
    removed — the default publish path does not use the channel pool, so it
    could never cause the described deadlock. It must NOT warn about that."""

    def test_no_misleading_pool_size_warning(self) -> None:
        from rabbitkit.core.config import PoolConfig

        config = RabbitConfig(pool=PoolConfig(channel_pool_size=2))
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

        warning_messages = [str(warning.message) for warning in w]
        assert not any("channel_pool_size" in msg for msg in warning_messages)


# ── flow_controller property ──────────────────────────────────────────────


class TestFlowControllerProperty:
    """Tests for flow_controller property getter/setter."""

    def test_flow_controller_getter_returns_none_initially(self) -> None:
        """flow_controller property returns None before any assignment."""
        broker = SyncBroker()
        assert broker.flow_controller is None

    def test_flow_controller_setter_no_transport(self) -> None:
        """Setting flow_controller before start stores it; no wiring yet."""
        from rabbitkit.highload.backpressure import FlowController

        broker = SyncBroker()
        fc = FlowController()
        broker.flow_controller = fc
        assert broker.flow_controller is fc

    def test_flow_controller_setter_wires_transport(self) -> None:
        """Setting flow_controller after start wires on_blocked/on_unblocked."""
        from rabbitkit.highload.backpressure import FlowController

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

        # Replace transport with a mock that tracks on_blocked/on_unblocked calls
        mock_transport = MagicMock()
        broker._transport = mock_transport

        fc = FlowController()
        broker.flow_controller = fc

        mock_transport.on_blocked.assert_called_once_with(fc.on_blocked)
        mock_transport.on_unblocked.assert_called_once_with(fc.on_unblocked)


# ── flow_controller wired on start() ─────────────────────────────────────


class TestFlowControllerWiredOnStart:
    """flow_controller wired in start() when pre-set (lines 217-219)."""

    def test_flow_controller_wired_after_start(self) -> None:
        from rabbitkit.highload.backpressure import FlowController

        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        fc = FlowController()
        broker.flow_controller = fc  # set before start

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                with patch.object(broker, "_start_consumer"):
                    broker.start()

        # flow_controller survives start and transport was wired
        assert broker.flow_controller is fc


# ── _wait_in_flight deadline ──────────────────────────────────────────────


class TestWaitInFlightDeadline:
    """Tests for _wait_in_flight deadline warning path (lines 294-303)."""

    def test_wait_in_flight_logs_warning_when_still_in_flight(self) -> None:
        import time

        broker = SyncBroker()
        # Set in_flight > 0 so the while loop runs
        broker._in_flight = 1
        # Use a past deadline so the wait immediately times out
        deadline = time.monotonic() - 1.0
        with patch("rabbitkit.sync.broker.logger") as mock_logger:
            broker._wait_in_flight(deadline)
        mock_logger.warning.assert_called()

    def test_wait_in_flight_returns_immediately_when_zero(self) -> None:
        import time

        broker = SyncBroker()
        broker._in_flight = 0
        # Should return immediately with a far future deadline
        broker._wait_in_flight(time.monotonic() + 10.0)


# ── _on_sigterm handler ───────────────────────────────────────────────────


class TestOnSigterm:
    """Tests for _on_sigterm handler."""

    def test_on_sigterm_with_no_transport_returns_early(self) -> None:
        """_on_sigterm returns immediately when _transport is None."""
        broker = SyncBroker()
        broker._transport = None
        # Should not raise and should not create a thread
        broker._on_sigterm(15, None)
        assert broker._sigterm_thread is None

    def test_on_sigterm_starts_drain_thread(self) -> None:
        """_on_sigterm starts a daemon thread that calls stop_consuming."""
        broker = SyncBroker()
        mock_transport = MagicMock()
        broker._transport = mock_transport

        broker._on_sigterm(15, None)

        assert broker._sigterm_thread is not None
        broker._sigterm_thread.join(timeout=2.0)

        mock_transport.stop_consuming.assert_called_once()


# ── run() connection error recovery ──────────────────────────────────────


class TestRunConnectionRecovery:
    """Tests for run() connection error recovery (lines 411-413)."""

    def test_run_recovers_on_connection_error(self) -> None:
        """run() calls _recover_consumers on pika connection error."""
        import pika.exceptions

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

        assert broker._transport is not None

        call_count: list[int] = [0]

        def start_consuming_side_effect() -> None:
            call_count[0] += 1
            if call_count[0] == 1:
                raise pika.exceptions.AMQPConnectionError("connection lost")
            # Second call: clean exit (stop_consuming was called elsewhere)

        broker._transport.start_consuming = MagicMock(side_effect=start_consuming_side_effect)
        with patch.object(broker, "_recover_consumers") as mock_recover:
            broker.run()

        mock_recover.assert_called_once()


# ── publish() kwargs form ─────────────────────────────────────────────────


class TestPublishKwargsForm:
    """Tests for publish() kwargs form (lines 455-465)."""

    def _start_broker(self) -> SyncBroker:
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

        from rabbitkit.core.types import PublishOutcome, PublishStatus

        broker._transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        return broker

    def test_publish_body_none(self) -> None:
        """body=None → raw_body=b''."""
        broker = self._start_broker()
        broker.publish(body=None, routing_key="rk")
        call_args = broker._transport.publish.call_args[0][0]
        assert call_args.body == b""

    def test_publish_body_bytes(self) -> None:
        """body=bytes → raw_body=body."""
        broker = self._start_broker()
        broker.publish(body=b"raw", routing_key="rk")
        call_args = broker._transport.publish.call_args[0][0]
        assert call_args.body == b"raw"

    def test_publish_body_str(self) -> None:
        """body=str → raw_body=body.encode()."""
        broker = self._start_broker()
        broker.publish(body="hello", routing_key="rk")
        call_args = broker._transport.publish.call_args[0][0]
        assert call_args.body == b"hello"

    def test_publish_body_dict(self) -> None:
        """body=dict → raw_body=json.dumps(body).encode()."""
        import json

        broker = self._start_broker()
        broker.publish(body={"key": "value"}, routing_key="rk")
        call_args = broker._transport.publish.call_args[0][0]
        assert json.loads(call_args.body) == {"key": "value"}


# ── publish() with FlowController ────────────────────────────────────────


class TestPublishWithFlowController:
    """Tests for publish() with FlowController (lines 479-490)."""

    def _start_broker(self) -> SyncBroker:
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

        return broker

    def test_publish_dropped_by_backpressure(self) -> None:
        """fc.acquire() returns False → publish returns ERROR outcome."""
        from rabbitkit.core.config import BackpressureConfig
        from rabbitkit.core.types import PublishStatus
        from rabbitkit.highload.backpressure import FlowController

        broker = self._start_broker()
        fc = FlowController(BackpressureConfig(on_blocked="drop"))
        fc.on_blocked()  # block so acquire() returns False immediately
        broker.flow_controller = fc

        envelope = MessageEnvelope(routing_key="rk", body=b"test")
        outcome = broker.publish(envelope)
        assert outcome.status == PublishStatus.ERROR

    def test_publish_succeeds_with_flow_controller(self) -> None:
        """fc.acquire() returns True → publishes and releases the slot."""
        from rabbitkit.core.types import PublishOutcome, PublishStatus
        from rabbitkit.highload.backpressure import FlowController

        broker = self._start_broker()
        fc = FlowController()
        broker.flow_controller = fc

        expected = PublishOutcome(status=PublishStatus.CONFIRMED)
        broker._transport.publish = MagicMock(return_value=expected)

        envelope = MessageEnvelope(routing_key="rk", body=b"test")
        outcome = broker.publish(envelope)
        assert outcome.status == PublishStatus.CONFIRMED
        assert fc.in_flight == 0  # slot released after publish


# ── C3: broker-level publish middleware (signing on the primary produce path) ──


class TestPublishWithMiddlewares:
    """C3: broker.publish() must apply middlewares=[...] (e.g. signing) —
    previously only handler-result/RPC-reply publishing went through publish_scope."""

    def _start_broker(self, middlewares: list[Any] | None = None) -> SyncBroker:
        broker = SyncBroker(middlewares=middlewares)

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

        return broker

    def test_publish_signs_envelope_via_broker_middleware(self) -> None:
        """A SigningMiddleware passed to the broker constructor must sign
        every broker.publish() call — the primary producer API."""
        from rabbitkit.core.types import PublishOutcome, PublishStatus
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        signing_mw = SigningMiddleware(SigningConfig(secret_key="test-secret"))
        broker = self._start_broker(middlewares=[signing_mw])

        captured: list[MessageEnvelope] = []

        def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            captured.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)

        broker._transport.publish = capture_publish  # type: ignore[union-attr]

        outcome = broker.publish(routing_key="orders", body=b"order-data")

        assert outcome.ok
        assert len(captured) == 1
        assert "x-rabbitkit-signature" in captured[0].headers

    def test_publish_compresses_envelope_via_broker_middleware(self) -> None:
        """C4: a CompressionMiddleware passed to the broker constructor must
        compress every broker.publish() call above threshold — matches the
        exact test the C4 finding requested."""
        import gzip

        from rabbitkit.core.config import CompressionConfig
        from rabbitkit.core.types import PublishOutcome, PublishStatus
        from rabbitkit.middleware.compression import CompressionMiddleware

        compression_mw = CompressionMiddleware(CompressionConfig(algorithm="gzip", threshold=0))
        broker = self._start_broker(middlewares=[compression_mw])

        large_body = b"order-payload " * 200
        captured: list[MessageEnvelope] = []

        def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            captured.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)

        broker._transport.publish = capture_publish  # type: ignore[union-attr]

        outcome = broker.publish(routing_key="orders", body=large_body)

        assert outcome.ok
        assert len(captured) == 1
        assert captured[0].content_encoding == "gzip"
        assert captured[0].body != large_body
        assert gzip.decompress(captured[0].body) == large_body

    def test_publish_without_middlewares_sends_envelope_unmodified(self) -> None:
        """No middlewares configured -> publish() is a pure pass-through (no
        regression to the pre-C3 fast path when middlewares=None)."""
        from rabbitkit.core.types import PublishOutcome, PublishStatus

        broker = self._start_broker(middlewares=None)
        assert broker.publish_middlewares == []

        captured: list[MessageEnvelope] = []

        def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            captured.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)

        broker._transport.publish = capture_publish  # type: ignore[union-attr]

        outcome = broker.publish(routing_key="orders", body=b"plain-data")

        assert outcome.ok
        assert captured[0].body == b"plain-data"
        assert captured[0].headers == {}

    def test_publish_middleware_runs_outside_flow_control(self) -> None:
        """Middleware wraps the flow-controlled publish — the transformed
        (signed) envelope is what gets rate-limited/sent, not the original."""
        from rabbitkit.core.types import PublishOutcome, PublishStatus
        from rabbitkit.highload.backpressure import FlowController
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        signing_mw = SigningMiddleware(SigningConfig(secret_key="test-secret"))
        broker = self._start_broker(middlewares=[signing_mw])
        broker.flow_controller = FlowController()

        captured: list[MessageEnvelope] = []

        def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            captured.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)

        broker._transport.publish = capture_publish  # type: ignore[union-attr]

        outcome = broker.publish(routing_key="orders", body=b"order-data")

        assert outcome.ok
        assert "x-rabbitkit-signature" in captured[0].headers

    def test_publish_middleware_chain_is_cached_across_calls(self) -> None:
        """The composed chain must be built once and reused (not rebuilt per
        publish), matching the route-level publish chain's caching behavior."""
        from rabbitkit.core.types import PublishOutcome, PublishStatus
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        signing_mw = SigningMiddleware(SigningConfig(secret_key="test-secret"))
        broker = self._start_broker(middlewares=[signing_mw])
        broker._transport.publish = MagicMock(  # type: ignore[union-attr]
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )

        broker.publish(routing_key="orders", body=b"one")
        chain_after_first = broker._pipeline._broker_publish_chain_cache[id(broker._publish_middlewares)]
        broker.publish(routing_key="orders", body=b"two")
        chain_after_second = broker._pipeline._broker_publish_chain_cache[id(broker._publish_middlewares)]

        assert chain_after_first is chain_after_second


# ── _recover_consumers ────────────────────────────────────────────────────


class TestRecoverConsumers:
    """Tests for _recover_consumers (lines 519-524)."""

    def test_recover_consumers_reconnects_and_redeclares(self) -> None:
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

        with patch.object(broker._transport, "reconnect") as mock_reconnect:
            with patch.object(broker, "_declare_topology") as mock_declare:
                with patch.object(broker, "_start_consumer") as mock_start:
                    broker._recover_consumers()

        mock_reconnect.assert_called_once()
        mock_declare.assert_called_once()
        mock_start.assert_called_once()

    def test_recover_consumers_noop_when_no_transport(self) -> None:
        broker = SyncBroker()
        broker._transport = None
        # Should not raise
        broker._recover_consumers()


# ── _wait_in_flight lines 296-297, 301 ───────────────────────────────────


class TestWaitInFlightUncoveredLines:
    """Tests for _wait_in_flight() lines 296-297 (deadline=None) and 301 (deadline with wait)."""

    def test_wait_in_flight_deadline_none_waits_for_notify(self) -> None:
        """Lines 296-297: deadline=None path — wait() blocks until in_flight reaches 0.

        We set in_flight=1, then a background thread calls _in_flight_dec()
        which notifies the condition. _wait_in_flight(deadline=None) must
        return without logging a warning.
        """
        import threading
        import time

        broker = SyncBroker()
        broker._in_flight = 1

        def decrement_after_delay() -> None:
            time.sleep(0.05)
            broker._in_flight_dec()

        t = threading.Thread(target=decrement_after_delay, daemon=True)
        t.start()

        with patch("rabbitkit.sync.broker.logger") as mock_logger:
            broker._wait_in_flight(deadline=None)

        t.join(timeout=2.0)

        # No warning should have been logged — in_flight reached 0
        mock_logger.warning.assert_not_called()
        assert broker._in_flight == 0

    def test_wait_in_flight_with_future_deadline_waits_for_notify(self) -> None:
        """Line 301: deadline set in the future — wait(timeout=remaining) is called.

        We set in_flight=1, set a deadline 5s in the future, and have a
        background thread call _in_flight_dec() quickly. The method must
        return (having hit the while-loop's notify path) without warning.
        """
        import threading
        import time

        broker = SyncBroker()
        broker._in_flight = 1
        deadline = time.monotonic() + 5.0  # far future so we don't time out

        def decrement_after_delay() -> None:
            time.sleep(0.05)
            broker._in_flight_dec()

        t = threading.Thread(target=decrement_after_delay, daemon=True)
        t.start()

        with patch("rabbitkit.sync.broker.logger") as mock_logger:
            broker._wait_in_flight(deadline=deadline)

        t.join(timeout=2.0)

        # in_flight is 0, so no warning
        mock_logger.warning.assert_not_called()
        assert broker._in_flight == 0


# ── H2: _wait_in_flight pumps the transport while waiting ─────────────────


class TestWaitInFlightPumpsTransport:
    """H2: _wait_in_flight() must pump the transport's I/O loop between
    condvar waits so a worker thread's ack — marshaled onto the transport's
    owner thread once a consume loop has run — actually gets drained instead
    of stalling for the whole drain window."""

    def test_wait_in_flight_calls_transport_pump_while_waiting(self) -> None:
        import threading
        import time

        broker = SyncBroker()
        mock_transport = MagicMock()
        broker._transport = mock_transport
        broker._in_flight = 1

        def decrement_after_delay() -> None:
            time.sleep(0.15)
            broker._in_flight_dec()

        t = threading.Thread(target=decrement_after_delay, daemon=True)
        t.start()

        broker._wait_in_flight(deadline=time.monotonic() + 5.0)

        t.join(timeout=2.0)

        assert broker._in_flight == 0
        assert mock_transport.pump.call_count >= 1
        mock_transport.pump.assert_called_with(0.05)

    def test_wait_in_flight_skips_pump_when_no_transport(self) -> None:
        """broker._transport is None (never started) -- no AttributeError."""
        broker = SyncBroker()
        broker._transport = None
        broker._in_flight = 1
        broker._in_flight_dec()

        broker._wait_in_flight(deadline=None)

        assert broker._in_flight == 0


class TestPublisherConfigDefaults:
    """M2: kwargs-form publish honors PublisherConfig.mandatory/.persistent
    (previously dead config)."""

    def _publish_and_capture(self, broker: SyncBroker) -> MessageEnvelope:
        captured: list[MessageEnvelope] = []
        transport = MagicMock()
        transport.publish.side_effect = lambda env: captured.append(env) or MagicMock(ok=True)
        broker._transport = transport
        broker._started = True
        broker.publish(routing_key="orders", body=b"x")
        return captured[0]

    def test_persistent_true_sets_delivery_mode_2(self) -> None:
        broker = SyncBroker(RabbitConfig(publisher=PublisherConfig(persistent=True)))
        env = self._publish_and_capture(broker)
        assert env.delivery_mode == 2

    def test_persistent_false_sets_delivery_mode_1(self) -> None:
        broker = SyncBroker(RabbitConfig(publisher=PublisherConfig(persistent=False)))
        env = self._publish_and_capture(broker)
        assert env.delivery_mode == 1

    def test_mandatory_flag_honored(self) -> None:
        broker = SyncBroker(RabbitConfig(publisher=PublisherConfig(mandatory=True)))
        env = self._publish_and_capture(broker)
        assert env.mandatory is True


class TestRejectTransientOnRedeliveryWiring:
    """M6: ConsumerConfig.reject_transient_on_redelivery reaches the pipeline."""

    def test_flag_threaded_to_pipeline(self) -> None:
        from rabbitkit.core.config import ConsumerConfig

        broker = SyncBroker(RabbitConfig(consumer=ConsumerConfig(reject_transient_on_redelivery=True)))
        assert broker._pipeline._reject_transient_on_redelivery is True

    def test_default_off(self) -> None:
        assert SyncBroker()._pipeline._reject_transient_on_redelivery is False


class TestPublishSizeGuard:
    """M10: reject oversized bodies at publish time when a cap is set."""

    def _broker(self, max_bytes: int) -> SyncBroker:
        broker = SyncBroker(RabbitConfig(publisher=PublisherConfig(max_message_bytes=max_bytes)))
        broker._transport = MagicMock()
        broker._transport.publish.return_value = MagicMock(ok=True)
        broker._started = True
        return broker

    def test_oversized_body_raises(self) -> None:
        broker = self._broker(max_bytes=10)
        with pytest.raises(ValueError, match="exceeds"):
            broker.publish(routing_key="orders", body=b"way-too-long-body")

    def test_within_limit_publishes(self) -> None:
        broker = self._broker(max_bytes=1000)
        broker.publish(routing_key="orders", body=b"small")
        broker._transport.publish.assert_called_once()

    def test_disabled_by_default_allows_large(self) -> None:
        broker = self._broker(max_bytes=0)  # disabled
        broker.publish(routing_key="orders", body=b"x" * 100_000)
        broker._transport.publish.assert_called_once()


class TestQuorumDeliveryLimitBackstop:
    """M5: a quorum source queue with delivery_limit keeps both the quorum
    type AND the delivery limit after retry re-declares it with DLX args — so
    crash-loop redeliveries (which bypass the header retry count) are
    eventually dead-lettered by the broker's x-delivery-limit."""

    def test_quorum_and_delivery_limit_survive_retry_redeclare(self) -> None:
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.core.topology import RabbitQueue
        from rabbitkit.core.types import QueueType

        broker = SyncBroker(RabbitConfig(retry=RetryConfig(max_retries=1, delays=(5,))))

        @broker.subscriber(
            queue=RabbitQueue(name="orders", queue_type=QueueType.QUORUM, delivery_limit=10)
        )
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._declare_topology()

        declared = [c.args[0] for c in broker._transport.declare_queue.call_args_list]
        source = next(q for q in declared if q.name == "orders")
        assert source.queue_type == QueueType.QUORUM
        assert source.delivery_limit == 10  # backstop preserved
        # And retry still wired its DLX routing onto the same queue.
        assert source.dead_letter_routing_key == "orders.dlq"


class TestManualHandlerExceptionContainment:
    """M12: a MANUAL-policy handler that raises without settling must NOT kill
    the whole consumer loop — it is contained and nack-requeued."""

    def test_handler_exception_does_not_escape_callback(self) -> None:
        from rabbitkit.core.types import AckPolicy

        broker = SyncBroker()

        @broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
        def handle(message: RabbitMessage) -> None:
            raise ValueError("boom")  # MANUAL: handler owns settlement, but raised

        captured: dict[str, Any] = {}
        transport = MagicMock()

        def _consume(queue: str, callback: Any, prefetch: int = 10) -> str:
            captured["callback"] = callback
            return "tag"

        transport.consume.side_effect = _consume
        broker._transport = transport
        broker._start_consumer(broker.routes[0])

        msg = RabbitMessage(body=b"x", routing_key="orders", headers={})
        msg._nack_fn = MagicMock()

        # The delivery callback must swallow the handler exception (not raise).
        captured["callback"](msg)

        msg._nack_fn.assert_called_once_with(True)  # nacked for redelivery


class TestWireReconnectMetric:
    """Connection-churn counter: _wire_reconnect_metric registers a
    transport.on_reconnect callback that increments reconnects_total via
    the first route MetricsMiddleware's collector. Reconnects were logged
    but never counted -- a flapping broker/network was invisible to
    metrics-based alerting."""

    def test_registers_callback_when_metrics_middleware_present(self) -> None:
        from rabbitkit.middleware.metrics import MetricsMiddleware

        collector = MagicMock()
        broker = SyncBroker()

        @broker.subscriber(queue="orders", middlewares=[MetricsMiddleware(collector)])
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._wire_reconnect_metric()

        broker._transport.on_reconnect.assert_called_once()
        # Fire the registered callback -> counter increments.
        registered_cb = broker._transport.on_reconnect.call_args.args[0]
        registered_cb()
        collector.inc_counter.assert_called_once()
        name = collector.inc_counter.call_args.args[0]
        assert name.endswith("_reconnects_total")

    def test_noop_without_metrics_middleware(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._wire_reconnect_metric()

        broker._transport.on_reconnect.assert_not_called()

    def test_noop_without_transport(self) -> None:
        broker = SyncBroker()
        broker._transport = None
        broker._wire_reconnect_metric()  # must not raise


class TestFlowControlledInternalPublish:
    """M18: RetryMiddleware's delay-queue republish and the pipeline's
    result-publish previously bypassed FlowController entirely -- a broker
    configured to protect itself under load did not protect these paths.
    _flow_controlled_internal_publish closes that gap, with one deliberate
    safety property: it must NEVER let BackpressureError escape as an
    exception (both RetryMiddleware and the pipeline's result-publish only
    understand a returned PublishOutcome, checked via .ok — an escaped
    exception would be misclassified PERMANENT and destroy the message
    instead of the existing safe nack+requeue-on-failure behavior)."""

    def test_no_flow_controller_passes_through_directly(self) -> None:
        broker = SyncBroker()
        broker._transport = MagicMock()
        broker._transport.publish.return_value = PublishOutcome(status=PublishStatus.CONFIRMED)
        env = MessageEnvelope(routing_key="q", body=b"x")

        outcome = broker._flow_controlled_internal_publish(env)

        assert outcome.status == PublishStatus.CONFIRMED
        broker._transport.publish.assert_called_once_with(env)

    def test_flow_controller_acquires_and_releases_on_success(self) -> None:
        from rabbitkit.highload.backpressure import FlowController

        broker = SyncBroker()
        broker._transport = MagicMock()
        broker._transport.publish.return_value = PublishOutcome(status=PublishStatus.CONFIRMED)
        broker.flow_controller = FlowController()
        env = MessageEnvelope(routing_key="q", body=b"x")

        outcome = broker._flow_controlled_internal_publish(env)

        assert outcome.status == PublishStatus.CONFIRMED
        assert broker.flow_controller.in_flight == 0  # released after publish

    def test_drop_policy_returns_failed_outcome_without_publishing(self) -> None:
        from rabbitkit.core.config import BackpressureConfig
        from rabbitkit.highload.backpressure import FlowController

        broker = SyncBroker()
        broker._transport = MagicMock()
        broker.flow_controller = FlowController(BackpressureConfig(max_in_flight=1, on_blocked="drop"))
        broker.flow_controller.acquire()  # saturate the single slot

        env = MessageEnvelope(routing_key="q", body=b"x")
        outcome = broker._flow_controlled_internal_publish(env)

        assert not outcome.ok
        assert isinstance(outcome.error, BackpressureError)
        broker._transport.publish.assert_not_called()  # never sent, not lost silently

    def test_raise_policy_never_escapes_as_exception(self) -> None:
        """The critical safety property: even with on_blocked='raise'
        configured, this method must return a failed outcome, not raise --
        otherwise RetryMiddleware/result-publish would see an uncaught
        BackpressureError, classify it PERMANENT, and destroy the message."""
        from rabbitkit.core.config import BackpressureConfig
        from rabbitkit.highload.backpressure import FlowController

        broker = SyncBroker()
        broker._transport = MagicMock()
        broker.flow_controller = FlowController(BackpressureConfig(max_in_flight=1, on_blocked="raise"))
        broker.flow_controller.acquire()  # saturate the single slot

        env = MessageEnvelope(routing_key="q", body=b"x")
        outcome = broker._flow_controlled_internal_publish(env)  # must NOT raise

        assert not outcome.ok
        assert isinstance(outcome.error, BackpressureError)
        broker._transport.publish.assert_not_called()

    def test_retry_middleware_wired_with_flow_controlled_publish_not_raw(self) -> None:
        """The actual gap being closed: RetryMiddleware must receive the
        flow-controlled wrapper, not raw transport.publish."""
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.middleware.retry import RetryMiddleware

        broker = SyncBroker(RabbitConfig(retry=RetryConfig(max_retries=1, delays=(5,))))

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._wire_retry_middleware()

        retry_mw = next(
            mw for mw in broker.routes[0].route_middlewares if isinstance(mw, RetryMiddleware)
        )
        assert retry_mw._publish_fn == broker._flow_controlled_internal_publish

    def test_result_publish_uses_flow_controlled_publish(self) -> None:
        """The result-publish path (Contract 5 / RPC replies) must also go
        through the flow-controlled wrapper, not raw transport.publish."""
        broker = SyncBroker()
        captured: dict[str, Any] = {}

        def _consume(queue: str, callback: Any, prefetch: int = 10) -> str:
            return "tag"

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._transport.consume.side_effect = _consume

        def spy_process_sync(route: Any, message: Any, publish_fn: Any = None) -> None:
            captured["publish_fn"] = publish_fn

        broker._pipeline.process_sync = spy_process_sync  # type: ignore[method-assign]
        broker._start_consumer(broker.routes[0])

        msg = RabbitMessage(body=b"x", routing_key="orders", headers={})
        # Invoke the delivery callback captured by consume().
        callback = broker._transport.consume.call_args.kwargs["callback"]
        callback(msg)

        assert captured["publish_fn"] == broker._flow_controlled_internal_publish


class TestSigningRetryConflict:
    """H1: signing + retry on the same route destroys every retried message —
    fail fast at wiring time."""

    def test_signing_plus_retry_raises(self) -> None:
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.core.errors import ConfigurationError
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        broker = SyncBroker()
        signing = SigningMiddleware(SigningConfig(secret_key="s3cr3t"))

        @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=1, delays=(5,)), middlewares=[signing])
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        with pytest.raises(ConfigurationError, match="SigningMiddleware is incompatible with retry"):
            broker._wire_retry_middleware()

    def test_signing_without_retry_is_fine(self) -> None:
        from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

        broker = SyncBroker()
        signing = SigningMiddleware(SigningConfig(secret_key="s3cr3t"))

        @broker.subscriber(queue="orders", retry=RETRY_DISABLED, middlewares=[signing])
        def handle(body: bytes) -> None:
            pass

        broker._transport = MagicMock()
        broker._wire_retry_middleware()  # must NOT raise


class TestSingleWorkerHeartbeatWarning:
    """H2: single-worker sync consumers run handlers inline on the I/O thread —
    long handlers starve heartbeats. Warn at start()."""

    def _start_mocked(self, broker: SyncBroker, **start_kwargs: object) -> None:
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                broker.start(**start_kwargs)  # type: ignore[arg-type]

    def test_single_worker_consumer_warns(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with pytest.warns(RuntimeWarning, match="starve heartbeats"):
            self._start_mocked(broker)

    def test_multi_worker_does_not_warn(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            # worker_count>1 runs handlers off the I/O thread — no starvation.
            self._start_mocked(broker, worker_config=WorkerConfig(worker_count=4))

    def test_publish_only_broker_does_not_warn(self) -> None:
        """No consumer routes → no inline handlers → no heartbeat starvation."""
        broker = SyncBroker()
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            self._start_mocked(broker)

    def test_heartbeat_disabled_does_not_warn(self) -> None:
        broker = SyncBroker(RabbitConfig(connection=ConnectionConfig(heartbeat=0)))

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            self._start_mocked(broker)


class TestDlqConsumerRouteNotAutoChained:
    """CI real-broker regression: a route consuming ANOTHER route's DLQ must
    not get a safety DLX auto-provisioned onto it — that re-declares the DLQ
    with different args than the retry topology declared it with (406)."""

    def test_safety_dlx_skipped_for_dlq_consumer_route(self) -> None:
        from rabbitkit.core.config import RetryConfig

        broker = SyncBroker(RabbitConfig(retry=RetryConfig(max_retries=1, delays=(5,))))

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None: ...

        @broker.subscriber(queue="orders.dlq", retry=None)
        def handle_dead(body: bytes) -> None: ...

        broker._transport = MagicMock()
        broker._declare_topology()

        declared = {c.args[0].name: c.args[0] for c in broker._transport.declare_queue.call_args_list}
        dlq = declared["orders.dlq"]
        # Terminal: no x-dead-letter args injected onto the DLQ itself.
        assert "x-dead-letter-exchange" not in (dlq.arguments or {})
        assert dlq.dead_letter_exchange is None


class TestStartedProperty:
    def test_started_reflects_lifecycle(self) -> None:
        broker = SyncBroker(RabbitConfig())
        assert broker.started is False
        broker._started = True
        assert broker.started is True

    def test_health_check_uses_property_without_deprecation(self) -> None:
        import warnings

        from rabbitkit.health import _get_started

        broker = SyncBroker(RabbitConfig())
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            assert _get_started(broker) is False
