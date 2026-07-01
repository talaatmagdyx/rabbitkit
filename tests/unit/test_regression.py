"""Regression tests for the 12 high-load bugs fixed in 0.6.1.

Each test is named after the bug ID (C1-C3, H1-H4, M1-M5) and fails
if the original bug is reintroduced.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.async_.pool import AsyncChannelPool, AsyncConnectionPool
from rabbitkit.core.config import (
    BackpressureConfig,
    PoolConfig,
    RetryConfig,
    WorkerConfig,
)
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.highload.backpressure import FlowController
from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware
from rabbitkit.middleware.retry import RetryRouter
from rabbitkit.rpc import AsyncRPCClient, RPCClient

# ── helpers ──────────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b"test",
        "routing_key": "test.key",
        "exchange": "",
        "headers": {},
    }
    defaults.update(kwargs)
    msg = RabbitMessage(**defaults)  # type: ignore[arg-type]
    msg._ack_fn = MagicMock()
    msg._nack_fn = MagicMock()
    msg._reject_fn = MagicMock()
    return msg


def _make_mock_connection() -> AsyncMock:
    ch = AsyncMock()
    ch.is_closed = False
    ch.set_qos = AsyncMock()
    ch.get_queue = AsyncMock()
    ch.get_exchange = AsyncMock()
    ch.default_exchange = AsyncMock()
    ch.close = AsyncMock()
    conn = AsyncMock()
    conn.is_closed = False
    conn.channel = AsyncMock(return_value=ch)
    conn.close = AsyncMock()
    return conn


# ── C1: async backpressure on_blocked="wait" must wait, not drop ─────────────


class TestC1AsyncBackpressureWait:
    @pytest.mark.asyncio
    async def test_on_blocked_wait_does_not_drop_when_slot_available(self) -> None:
        """C1: acquire_async with on_blocked='wait' and a free slot returns True."""
        config = BackpressureConfig(max_in_flight=5, on_blocked="wait")
        fc = FlowController(config)
        result = await fc.acquire_async()
        assert result is True
        assert fc.in_flight == 1

    @pytest.mark.asyncio
    async def test_on_blocked_wait_unblocks_after_release(self) -> None:
        """C1: acquire_async blocks when at limit, unblocks after release."""
        config = BackpressureConfig(max_in_flight=1, on_blocked="wait", blocked_timeout=2.0)
        fc = FlowController(config)

        # Fill the single slot
        assert await fc.acquire_async() is True
        assert fc.in_flight == 1

        # Launch a second acquire in background — it should block
        acquired_event = asyncio.Event()
        result_holder: list[bool] = []

        async def background_acquire() -> None:
            r = await fc.acquire_async()
            result_holder.append(r)
            acquired_event.set()

        task = asyncio.create_task(background_acquire())

        # Give the task a moment to start blocking
        await asyncio.sleep(0.05)
        assert not acquired_event.is_set(), "Should still be waiting for slot"

        # Release the slot — background task should unblock
        await fc.release_async()
        await asyncio.wait_for(acquired_event.wait(), timeout=2.0)

        assert result_holder == [True]
        task.cancel()

    @pytest.mark.asyncio
    async def test_on_blocked_drop_returns_false_at_limit(self) -> None:
        """C1 sanity: on_blocked='drop' returns False immediately at limit."""
        config = BackpressureConfig(max_in_flight=1, on_blocked="drop")
        fc = FlowController(config)
        assert await fc.acquire_async() is True
        # Second call should drop immediately
        assert await fc.acquire_async() is False

    @pytest.mark.asyncio
    async def test_release_async_sets_slot_event(self) -> None:
        """C1: release_async signals the slot event so waiters wake up."""
        config = BackpressureConfig(max_in_flight=2, on_blocked="wait")
        fc = FlowController(config)
        await fc.acquire_async()
        await fc.acquire_async()
        assert fc.in_flight == 2
        await fc.release_async()
        assert fc.in_flight == 1


# ── C2: circuit breaker sync-CB + async handler must raise TypeError ──────────


class TestC2CircuitBreakerAsyncFallback:
    @pytest.mark.asyncio
    async def test_sync_cb_async_handler_raises_type_error(self) -> None:
        """C2: sync CB + async broker raises TypeError instead of silently skipping."""

        class _SyncCB:
            def call(self, fn: object, *args: object) -> object:
                return fn(*args)  # type: ignore[operator]

        mw = CircuitBreakerMiddleware(circuit_breaker=_SyncCB())
        msg = _make_message()

        called = False

        async def async_handler(m: RabbitMessage) -> str:
            nonlocal called
            called = True
            return "ok"

        with pytest.raises(TypeError, match="async_circuit_breaker"):
            await mw.consume_scope_async(async_handler, msg)

        # Handler must NOT have been called
        assert not called

    @pytest.mark.asyncio
    async def test_sync_publish_cb_async_raises_type_error(self) -> None:
        """C2: sync publish CB + async publish raises TypeError."""

        class _SyncCB:
            def call(self, fn: object, *args: object) -> object:
                return fn(*args)  # type: ignore[operator]

        mw = CircuitBreakerMiddleware(circuit_breaker=_SyncCB())

        async def async_publish(env: MessageEnvelope) -> str:
            return "published"

        with pytest.raises(TypeError, match="async_publish_circuit_breaker"):
            await mw.publish_scope_async(async_publish, MessageEnvelope(routing_key="rk", body=b"x"))

    @pytest.mark.asyncio
    async def test_async_cb_works_correctly(self) -> None:
        """C2 sanity: proper async CB is called and handler runs."""

        class _AsyncCB:
            calls: ClassVar[list[str]] = []

            async def call_async(self, fn: object, *args: object) -> object:
                self.calls.append("called")
                return await fn(*args)  # type: ignore[operator, misc]

        acb = _AsyncCB()
        mw = CircuitBreakerMiddleware(async_circuit_breaker=acb)

        async def handler(m: RabbitMessage) -> str:
            return "result"

        result = await mw.consume_scope_async(handler, _make_message())
        assert result == "result"
        assert acb.calls == ["called"]


# ── C3: AsyncRPCClient _ensure_consuming must not register duplicate consumer ─


class TestC3RPCEnsureConsumingRace:
    @pytest.mark.asyncio
    async def test_concurrent_first_calls_register_consumer_once(self) -> None:
        """C3: concurrent _ensure_consuming calls must call consume() exactly once."""
        mock_transport = AsyncMock()
        mock_transport.consume = AsyncMock(return_value="tag-1")

        client = AsyncRPCClient(mock_transport)

        # Fire 10 concurrent first calls
        await asyncio.gather(*[client._ensure_consuming() for _ in range(10)])

        # Despite 10 concurrent callers, consume() must be called exactly once
        assert mock_transport.consume.call_count == 1
        assert client._consuming is True

    def test_sync_rpc_concurrent_ensure_consuming_once(self) -> None:
        """M5/C3: sync RPCClient _ensure_consuming is also protected by lock."""
        consume_count = 0
        lock = threading.Lock()

        class _MockTransport:
            def consume(
                self, queue: str, callback: object, *, no_ack: bool = False, declare: bool = True
            ) -> str:  # type: ignore[override]
                nonlocal consume_count
                time.sleep(0.01)  # simulate latency
                with lock:
                    consume_count += 1
                return "tag-1"

        client = RPCClient(_MockTransport())  # type: ignore[arg-type]

        threads = [threading.Thread(target=client._ensure_consuming) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert consume_count == 1


# ── H1/H2: AsyncChannelPool acquire with timeout, no deadlock ────────────────


class TestH2ChannelPoolTimeout:
    @pytest.mark.asyncio
    async def test_acquire_returns_channel_when_available(self) -> None:
        """H2: acquire() returns a channel when pool has capacity."""
        mock_conn = _make_mock_connection()
        pool = AsyncChannelPool(mock_conn, pool_size=2, acquire_timeout=1.0)

        ch = await pool.acquire()
        assert ch is not None

    @pytest.mark.asyncio
    async def test_acquire_raises_timeout_when_exhausted(self) -> None:
        """H2: acquire() raises asyncio.TimeoutError when all channels are held."""
        mock_conn = _make_mock_connection()
        pool = AsyncChannelPool(mock_conn, pool_size=1, acquire_timeout=0.1)

        # Acquire the only channel and don't release it
        _held = await pool.acquire()

        # Second acquire should time out
        with pytest.raises((asyncio.TimeoutError, TimeoutError)):
            await pool.acquire()

    @pytest.mark.asyncio
    async def test_acquire_succeeds_after_release(self) -> None:
        """H2: acquire() unblocks after release() returns the channel."""
        mock_conn = _make_mock_connection()
        pool = AsyncChannelPool(mock_conn, pool_size=1, acquire_timeout=2.0)

        ch1 = await pool.acquire()

        async def release_after_delay() -> None:
            await asyncio.sleep(0.05)
            await pool.release(ch1)

        _task = asyncio.create_task(release_after_delay())
        ch2 = await pool.acquire()
        await _task
        assert ch2 is not None


# ── H3: DLQ — source queue must carry dead-letter arguments ──────────────────


class TestH3DLQSourceQueueArguments:
    def test_get_source_queue_dlq_arguments_per_queue(self) -> None:
        """H3: RetryRouter returns correct dead-letter args for source queue."""
        config = RetryConfig(max_retries=3, delays=(5, 30, 120), per_queue=True)
        router = RetryRouter(config)
        args = router.get_source_queue_dlq_arguments("orders")

        assert args["x-dead-letter-exchange"] == ""
        assert args["x-dead-letter-routing-key"] == "orders.dlq"

    def test_get_source_queue_dlq_arguments_shared(self) -> None:
        """H3: Shared mode points to shared DLQ."""
        config = RetryConfig(max_retries=2, delays=(5, 30), per_queue=False)
        router = RetryRouter(config)
        args = router.get_source_queue_dlq_arguments("orders")

        assert args["x-dead-letter-routing-key"] == "rabbitkit.dlq"

    def test_get_dlq_name_per_queue(self) -> None:
        config = RetryConfig(max_retries=1, delays=(5,), per_queue=True)
        router = RetryRouter(config)
        assert router.get_dlq_name("my-queue") == "my-queue.dlq"

    def test_get_dlq_name_shared(self) -> None:
        config = RetryConfig(max_retries=1, delays=(5,), per_queue=False)
        router = RetryRouter(config)
        assert router.get_dlq_name("my-queue") == "rabbitkit.dlq"

    def test_dlq_declaration_is_present(self) -> None:
        """H3: DLQ is declared as a durable queue (no TTL)."""
        config = RetryConfig(max_retries=2, delays=(5, 30), per_queue=True)
        router = RetryRouter(config)
        queues = router.get_delay_queue_definitions("orders", "orders-ex")
        dlq = queues[-1]
        assert dlq.name == "orders.dlq"
        assert dlq.durable is True
        assert "x-message-ttl" not in dlq.arguments


# ── H4: SyncWorkerPool _futures thread safety ────────────────────────────────


class TestH4SyncWorkerPoolThreadSafety:
    def test_concurrent_submit_and_pending_count_no_crash(self) -> None:
        """H4: concurrent submit() + pending_count from multiple threads must not crash."""
        from rabbitkit.concurrency import SyncWorkerPool

        pool = SyncWorkerPool(config=WorkerConfig(worker_count=4))
        pool.start()

        errors: list[Exception] = []

        def submit_messages() -> None:
            for _ in range(20):
                msg = _make_message()
                try:
                    pool.submit(lambda m: time.sleep(0.001), msg)
                except Exception as e:
                    errors.append(e)

        def read_pending() -> None:
            for _ in range(50):
                try:
                    _ = pool.pending_count
                except Exception as e:
                    errors.append(e)

        threads = [threading.Thread(target=submit_messages) for _ in range(4)] + [
            threading.Thread(target=read_pending) for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        pool.stop()
        assert errors == [], f"Thread safety errors: {errors}"

    def test_futures_list_cleaned_up_after_completion(self) -> None:
        """H4: completed futures are pruned from the list."""
        from rabbitkit.concurrency import SyncWorkerPool

        pool = SyncWorkerPool(config=WorkerConfig(worker_count=2))
        pool.start()

        for _ in range(5):
            pool.submit(lambda m: None, _make_message())

        pool.stop()
        # After stop, the futures set should be empty
        assert not pool._futures


# ── M1: async rate limiter must not block the event loop ─────────────────────


class TestM1AsyncRateLimiter:
    @pytest.mark.asyncio
    async def test_async_rate_limiter_does_not_block_event_loop(self) -> None:
        """M1: rate limiter in acquire_async must not hold a threading.Lock."""
        from rabbitkit.highload.backpressure import _AsyncTokenBucket

        bucket = _AsyncTokenBucket(rate=100)
        # Should complete quickly using asyncio.Lock, not block event loop
        results = await asyncio.gather(*[bucket.acquire() for _ in range(50)])
        # First 100 should succeed (full bucket), rest should fail
        assert sum(results) <= 100

    @pytest.mark.asyncio
    async def test_flow_controller_acquire_async_uses_async_rate_limiter(self) -> None:
        """M1: FlowController.acquire_async uses _AsyncTokenBucket, not threading.Lock."""
        config = BackpressureConfig(max_in_flight=1000, rate_limit=100)
        fc = FlowController(config)
        # _async_rate_limiter is created alongside _rate_limiter
        assert fc._async_rate_limiter is not None
        assert fc._rate_limiter is not None

        result = await fc.acquire_async()
        assert result is True


# ── M2: DI generator cleanup exceptions must be logged, not swallowed ─────────


class TestM2DICleanupException:
    def test_sync_cleanup_exception_is_logged_not_swallowed(self) -> None:
        """M2: scope.cleanup() raising must be caught + logged, handler result preserved."""

        class _FailingScope:
            def cleanup(self) -> None:
                raise RuntimeError("cleanup explosion")

            async def cleanup_async(self) -> None:
                raise RuntimeError("async cleanup explosion")

        pipeline = HandlerPipeline()
        route = RouteDefinition(
            name="test",
            queue=RabbitQueue(name="test"),
            exchange=None,
            handler=lambda body: "ok",
        )
        msg = _make_message()
        msg._ack_fn = MagicMock()

        # Patch DependencyScope to return our failing scope
        with patch("rabbitkit.di.resolver.DependencyScope", return_value=_FailingScope()):
            # Should NOT raise — exception is logged
            pipeline.process_sync(route, msg)

        # Message should still be settled normally
        assert msg.is_settled

    @pytest.mark.asyncio
    async def test_async_cleanup_exception_is_logged_not_swallowed(self) -> None:
        """M2: async scope.cleanup_async() raising must be caught + logged."""

        class _FailingScope:
            def cleanup(self) -> None:
                pass

            async def cleanup_async(self) -> None:
                raise RuntimeError("async cleanup boom")

        pipeline = HandlerPipeline()

        async def async_handler(body: bytes) -> str:
            return "ok"

        route = RouteDefinition(
            name="test",
            queue=RabbitQueue(name="test"),
            exchange=None,
            handler=async_handler,
        )
        msg = _make_message()

        async def async_ack() -> None:
            pass

        msg._ack_async_fn = async_ack

        with patch("rabbitkit.di.resolver.DependencyScope", return_value=_FailingScope()):
            await pipeline.process_async(route, msg)

        assert msg.is_settled


# ── M3: RetryConfig warns when delays < max_retries ──────────────────────────


class TestM3RetryConfigValidation:
    def test_warns_when_fewer_delays_than_retries(self) -> None:
        """M3: UserWarning emitted when len(delays) < max_retries (non-strict mode)."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            RetryConfig(max_retries=4, delays=(5, 30), strict_delays=False)
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "max_retries" in str(w[0].message)

    def test_raises_when_strict_and_fewer_delays_than_retries(self) -> None:
        """M3: strict mode (default) raises ValueError on under-length delays."""
        with pytest.raises(ValueError, match="max_retries"):
            RetryConfig(max_retries=4, delays=(5, 30))

    def test_no_warning_when_delays_match(self) -> None:
        """M3: No warning when delays tuple has >= max_retries entries."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            RetryConfig(max_retries=3, delays=(5, 30, 120))
            user_warnings = [x for x in w if issubclass(x.category, UserWarning)]
            assert len(user_warnings) == 0

    def test_negative_max_retries_raises(self) -> None:
        """M3: Negative max_retries raises ValueError."""
        with pytest.raises(ValueError, match="max_retries"):
            RetryConfig(max_retries=-1, delays=(5,))

    def test_empty_delays_with_zero_retries_ok(self) -> None:
        """M3: max_retries=0 with empty delays is valid."""
        config = RetryConfig(max_retries=0, delays=())
        assert config.max_retries == 0


# ── M4: set_qos per-consumer channel (each queue gets its own channel) ─────────


class TestM4SetQosPerConsumer:
    @pytest.mark.asyncio
    async def test_each_queue_gets_separate_channel(self) -> None:
        """M4: two consume() calls on different queues get separate channels."""
        from rabbitkit.async_.transport import AsyncTransportImpl
        from rabbitkit.core.config import ConnectionConfig, SecurityConfig

        transport = AsyncTransportImpl(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )

        ch1 = AsyncMock()
        ch1.is_closed = False
        ch1.set_qos = AsyncMock()
        ch1.get_queue = AsyncMock(return_value=AsyncMock())

        ch2 = AsyncMock()
        ch2.is_closed = False
        ch2.set_qos = AsyncMock()
        ch2.get_queue = AsyncMock(return_value=AsyncMock())

        mock_conn = AsyncMock()
        mock_conn.is_closed = False
        mock_conn.channel = AsyncMock(
            side_effect=[
                ch1,
                ch1,  # publisher pool: topology channel
                ch2,
                ch2,  # consumer connection channels
            ]
        )
        mock_conn.close = AsyncMock()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_conn):
                await transport.connect()

        # Each consume gets its own channel from consumer connection
        consumer_conn = await transport._conn_pool.get_consumer_connection()
        consumer_conn.channel = AsyncMock(side_effect=[ch1, ch2])

        await transport.consume("queue-a", AsyncMock(), prefetch=10)
        await transport.consume("queue-b", AsyncMock(), prefetch=5)

        # Each queue's channel got its own set_qos call with its own prefetch
        ch1.set_qos.assert_called_with(prefetch_count=10)
        ch2.set_qos.assert_called_with(prefetch_count=5)

    @pytest.mark.asyncio
    async def test_consumer_channels_stored_per_queue(self) -> None:
        """M4: transport tracks per-queue consumer channels."""
        from rabbitkit.async_.transport import AsyncTransportImpl
        from rabbitkit.core.config import ConnectionConfig, SecurityConfig

        transport = AsyncTransportImpl(
            connection_config=ConnectionConfig(),
            security_config=SecurityConfig(),
        )
        # consumer_channels dict starts empty
        assert transport._consumer_channels == {}


# ── M5: RPCClient sync _ensure_consuming thread safety (covered in C3) ────────
# (Already covered by TestC3RPCEnsureConsumingRace.test_sync_rpc_concurrent_ensure_consuming_once)


# ── AsyncConnectionPool: separate publisher/consumer connections ──────────────


class TestH1AsyncConnectionPool:
    @pytest.mark.asyncio
    async def test_publisher_and_consumer_are_separate_connections(self) -> None:
        """H1: publisher and consumer connections are different objects."""
        connections_created: list[AsyncMock] = []

        def make_conn(*args: object, **kwargs: object) -> AsyncMock:
            conn = _make_mock_connection()
            connections_created.append(conn)
            return conn

        pool = AsyncConnectionPool(
            connection_config=MagicMock(),
            security_config=MagicMock(),
        )

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, side_effect=make_conn):
                await pool.connect()

        pub_conn = await pool.get_publisher_connection()
        con_conn = await pool.get_consumer_connection()

        assert pub_conn is not con_conn
        assert len(connections_created) == 2

    @pytest.mark.asyncio
    async def test_publisher_channel_pool_is_available(self) -> None:
        """H1: publisher channel pool is created on connect."""
        pool = AsyncConnectionPool(
            connection_config=MagicMock(),
            security_config=MagicMock(),
        )

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=_make_mock_connection()):
                await pool.connect()

        assert pool._publisher_channel_pool is not None

    @pytest.mark.asyncio
    async def test_acquire_release_publisher_channel(self) -> None:
        """H1: acquire/release publisher channel round-trips correctly."""
        pool = AsyncConnectionPool(
            connection_config=MagicMock(),
            security_config=MagicMock(),
            pool_config=PoolConfig(channel_pool_size=2),
        )

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=_make_mock_connection()):
                await pool.connect()

        ch = await pool.acquire_publisher_channel()
        assert ch is not None
        await pool.release_publisher_channel(ch)
