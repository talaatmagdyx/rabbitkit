"""AsyncBroker — wires core registry + pipeline + AsyncTransportImpl.

The AsyncBroker is the high-level entry point for async applications.
It combines the registry (for handler registration), the pipeline
(for message processing), and the transport (for RabbitMQ I/O).

Graceful shutdown:
1. Cancel all consumers (cancel per consumer_tag)
2. Wait for in-flight messages
3. Close transport connection
"""

from __future__ import annotations

import asyncio
import signal
import time
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import structlog

from rabbitkit.async_.transport import AsyncTransportImpl
from rabbitkit.concurrency import AsyncWorkerPool
from rabbitkit.core.config import (
    BatchPublishConfig,
    ConsumerConfig,
    RabbitConfig,
    RetryConfig,
    RetryDisabled,
    WorkerConfig,
)
from rabbitkit.core.errors import BackpressureError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.path import extract_path, to_binding_key
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.registry import SubscriberRegistry
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy, MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.middleware.retry import RetryRouter

logger = structlog.stdlib.get_logger(__name__)


class AsyncBroker:
    """Async broker — wires registry + pipeline + AsyncTransportImpl.

    Usage::

        config = RabbitConfig(connection=ConnectionConfig(host="localhost"))
        broker = AsyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events")
        async def handle_order(body: bytes) -> None:
            ...

        await broker.start()
    """

    def __init__(
        self,
        config: RabbitConfig | None = None,
        *,
        serializer: Any | None = None,
        di_resolver: Any | None = None,
        context_repo: Any | None = None,
        batch_config: BatchPublishConfig | None = None,
    ) -> None:
        self._config = config or RabbitConfig()
        # Private mutable view of consumer config — brokers may apply a
        # prefetch override derived from WorkerConfig.prefetch_per_worker.
        # Stored separately so the caller's frozen RabbitConfig is never mutated.
        self._consumer_config = self._config.consumer

        self._registry = SubscriberRegistry(broker_retry=self._config.retry)
        self._pipeline = HandlerPipeline(
            serializer=serializer,
            di_resolver=di_resolver,
            context_repo=context_repo,
        )

        self._batch_config = batch_config
        self._batch_publisher: Any | None = None  # BatchPublisher, started lazily in start()
        self._transport: Any | None = None  # AsyncTransportImpl
        self._worker_pool: AsyncWorkerPool | None = None
        self._started = False
        self._rpc_client: Any | None = None
        # I-16: optional callback invoked from the signal handler so an embedding
        # RabbitApp's shutdown event is also set (prevents the double-install hang
        # where the broker's handler overwrites the app's). Wire
        # ``broker.on_app_shutdown = app.request_shutdown`` before ``app.run_async()``.
        self.on_app_shutdown: Callable[[], None] | None = None

        # Bounded graceful drain (C-2): inline in-flight counter guarded by an
        # asyncio.Condition (R-Condition). ``_in_flight`` stays a plain int so
        # health checks that read it directly keep working (backward compat).
        self._in_flight = 0
        self._in_flight_cond: asyncio.Condition | None = None  # lazily created in loop

        # Optional publish-side flow control (C-6).
        self._flow_controller: Any | None = None

        # Signal-handler bookkeeping (H-SRE5).
        self._original_handlers: dict[signal.Signals, Any] = {}
        self._installed_loop_handlers = False
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def flow_controller(self) -> Any | None:
        """Optional FlowController used to throttle the publish path."""
        return self._flow_controller

    @flow_controller.setter
    def flow_controller(self, value: Any | None) -> None:
        self._flow_controller = value
        if self._transport is not None and value is not None:
            self._transport.on_blocked(value.on_blocked)
            self._transport.on_unblocked(value.on_unblocked)

    def _ensure_inflight_cond(self) -> asyncio.Condition:
        if self._in_flight_cond is None:
            self._in_flight_cond = asyncio.Condition()
        return self._in_flight_cond

    async def _in_flight_inc(self) -> None:
        async with self._ensure_inflight_cond():
            self._in_flight += 1

    async def _in_flight_dec(self) -> None:
        cond = self._ensure_inflight_cond()
        async with cond:
            if self._in_flight > 0:
                self._in_flight -= 1
            if self._in_flight == 0:
                cond.notify_all()

    @property
    def config(self) -> RabbitConfig:
        return self._config

    @property
    def routes(self) -> list[RouteDefinition]:
        return self._registry.routes

    @property
    def worker_pool(self) -> AsyncWorkerPool | None:
        """Return the worker pool (if configured)."""
        return self._worker_pool

    @property
    def consumer_config(self) -> ConsumerConfig:
        """Effective consumer config (may reflect WorkerConfig.prefetch override)."""
        return self._consumer_config

    # ── Registration (decorator API) ──────────────────────────────────────

    def subscriber(
        self,
        queue: RabbitQueue | str,
        exchange: RabbitExchange | str | None = None,
        routing_key: str = "",
        ack_policy: AckPolicy = AckPolicy.AUTO,
        middlewares: list[Any] | None = None,
        serializer: Any | None = None,
        retry: RetryConfig | RetryDisabled | None = None,
        tags: frozenset[str] | set[str] | None = None,
        description: str = "",
        name: str | None = None,
        prefetch_count: int | None = None,
        filter_fn: Any | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a subscriber handler."""
        return self._registry.subscriber(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            ack_policy=ack_policy,
            middlewares=middlewares,
            serializer=serializer,
            retry=retry,
            tags=tags,
            description=description,
            name=name,
            prefetch_count=prefetch_count,
            filter_fn=filter_fn,
        )

    def publisher(
        self,
        exchange: RabbitExchange | str | None = None,
        routing_key: str = "",
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a result publisher."""
        return self._registry.publisher(exchange=exchange, routing_key=routing_key)

    def include_router(self, router: Any, prefix: str = "") -> None:
        """Include routes from a RabbitRouter."""
        self._registry.include_router(router, prefix=prefix)

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(
        self,
        worker_config: WorkerConfig | None = None,
        *,
        install_signal_handlers: bool = True,
    ) -> None:
        """Start the broker - connect, declare topology, start consuming.

        1. Connect to RabbitMQ
        2. Declare exchanges, queues, bindings per TopologyMode
        3. Declare retry topology (delay queues, DLQ)
        4. Optionally create a worker pool for concurrent processing
        5. Start consuming from all registered queues

        When ``install_signal_handlers`` is True (default), SIGINT/SIGTERM are
        trapped so the common ``await broker.start()`` pattern drains gracefully
        instead of hard-dying (H-SRE5). Pass ``False`` when an outer lifecycle
        manager (e.g. ``RabbitApp``) owns signal handling.
        """
        if self._started:
            return

        # Configure structured logging if enabled
        if self._config.logging is not None:
            from rabbitkit.core.logging import configure_structlog

            configure_structlog(self._config.logging)

        # Create transport
        self._transport = AsyncTransportImpl(
            connection_config=self._config.connection,
            security_config=self._config.security,
            topology_mode=self._config.topology_mode,
            confirm_delivery=self._config.publisher.confirm_delivery,
            confirm_timeout=self._config.publisher.confirm_timeout,
        )

        await self._transport.connect()

        # Wire an opt-in FlowController's blocked/unblocked callbacks to the
        # transport now that it exists (C-6).
        if self._flow_controller is not None:
            self._transport.on_blocked(self._flow_controller.on_blocked)
            self._transport.on_unblocked(self._flow_controller.on_unblocked)

        # M-P5: warn when confirms are on and the publisher channel pool is
        # small relative to expected concurrency (default unchanged).
        if self._config.publisher.confirm_delivery:
            pool_size = self._config.pool.channel_pool_size
            # Publisher concurrency ~ worker_count (handlers that publish) or 1.
            expected = worker_config.worker_count if worker_config and worker_config.worker_count > 1 else 1
            if pool_size < max(4, expected):
                import warnings

                warnings.warn(
                    f"confirm_delivery=True with channel_pool_size={pool_size} "
                    f"(expected publisher concurrency ~{expected}). Concurrent confirms "
                    "are capped by the pool size; increase PoolConfig.channel_pool_size "
                    "if publish throughput matters.",
                    RuntimeWarning,
                    stacklevel=2,
                )

        # Start batch publisher if configured (must come before topology so
        # the flush task is alive when the first publish arrives).
        if self._batch_config is not None:
            import dataclasses
            import warnings

            from rabbitkit.async_.batch import AsyncBatchPublisher

            batch_cfg = self._batch_config
            pool_size = self._config.pool.channel_pool_size
            if batch_cfg.flush_workers == 0:
                # Auto-compute workers but cap at half the pool so at least
                # half the channels remain available for retry/direct publishes.
                # Batch workers hold their channels permanently; exhausting the
                # pool deadlocks any non-batch transport.publish() call (e.g. retry).
                auto = min(16, max(1, batch_cfg.max_in_flight // batch_cfg.batch_size))
                safe = min(auto, max(1, pool_size // 2))
                batch_cfg = dataclasses.replace(batch_cfg, flush_workers=safe)
            elif batch_cfg.flush_workers > pool_size // 2:
                warnings.warn(
                    f"BatchPublishConfig.flush_workers={batch_cfg.flush_workers} > "
                    f"channel_pool_size({pool_size}) // 2. Batch workers hold pool "
                    "channels permanently; retry/direct publish calls may exhaust "
                    "the remaining slots and deadlock. "
                    "Increase PoolConfig.channel_pool_size to at least flush_workers * 2.",
                    RuntimeWarning,
                    stacklevel=2,
                )
            self._batch_publisher = AsyncBatchPublisher(self._transport, batch_cfg)
            await self._batch_publisher.start()

        # Declare topology
        await self._declare_topology()

        # Install RetryMiddleware on retry-enabled routes (topology alone does
        # not retry — the middleware routes failures into the delay queues).
        self._wire_retry_middleware()

        # Create worker pool if requested
        if worker_config is not None and worker_config.worker_count > 1:
            # Warn if worker_count exceeds channel_pool_size — all workers
            # publishing simultaneously will exhaust the channel pool and
            # block until acquire_timeout, risking deadlock under load.
            if worker_config.worker_count > self._config.pool.channel_pool_size:
                import warnings

                warnings.warn(
                    f"worker_count={worker_config.worker_count} exceeds "
                    f"channel_pool_size={self._config.pool.channel_pool_size}. "
                    "Concurrent publishes may exhaust the channel pool and deadlock. "
                    "Increase PoolConfig.channel_pool_size to at least worker_count.",
                    RuntimeWarning,
                    stacklevel=2,
                )
            # Override prefetch_count if prefetch_per_worker is set
            if worker_config.prefetch_per_worker is not None:
                self._consumer_config = replace(
                    self._config.consumer,
                    prefetch_count=worker_config.worker_count * worker_config.prefetch_per_worker,
                )
            self._worker_pool = AsyncWorkerPool(config=worker_config)
            self._worker_pool.start()

        # Start consuming
        for route in self._registry.routes:
            await self._start_consumer(route)

        self._started = True
        logger.info(
            "AsyncBroker started with %d routes",
            len(self._registry.routes),
        )

        if install_signal_handlers:
            self._install_signal_handlers()

    def _install_signal_handlers(self) -> None:
        """Install portable SIGINT/SIGTERM handlers that drain via stop() (H-SRE5).

        Prefers ``loop.add_signal_handler``; falls back to ``signal.signal`` on
        platforms/threads where the loop API is unavailable. Idempotent.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover
            return  # no running loop - nothing to install
        self._loop = loop
        installed = False
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._on_signal)
            installed = True
        except (NotImplementedError, RuntimeError, ValueError):  # pragma: no cover
            pass  # pragma: no cover
        self._installed_loop_handlers = installed
        if not installed:  # pragma: no cover
            for sig in (signal.SIGINT, signal.SIGTERM):  # pragma: no cover
                try:  # pragma: no cover
                    self._original_handlers[sig] = signal.signal(sig, self._on_signal_sync)  # pragma: no cover
                except (ValueError, OSError):  # pragma: no cover - not main thread
                    pass

    def _on_signal(self) -> None:
        logger.info("Received shutdown signal; initiating graceful drain")
        if self._loop is not None:
            self._loop.create_task(self.stop())
        if self.on_app_shutdown is not None:
            try:
                self.on_app_shutdown()
            except Exception:  # pragma: no cover - never block shutdown on the callback
                logger.warning("on_app_shutdown callback raised", exc_info=True)

    def _on_signal_sync(self, signum: int, frame: Any) -> None:  # pragma: no cover
        logger.info("Received shutdown signal %d", signum)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(lambda: self._loop.create_task(self.stop()))  # type: ignore[union-attr]
        if self.on_app_shutdown is not None:
            try:
                self.on_app_shutdown()
            except Exception:  # pragma: no cover
                logger.warning("on_app_shutdown callback raised", exc_info=True)

    def _restore_signal_handlers(self) -> None:
        if self._loop is not None and self._installed_loop_handlers:
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    self._loop.remove_signal_handler(sig)
                except (NotImplementedError, RuntimeError, ValueError):  # pragma: no cover
                    pass
            self._installed_loop_handlers = False
        for sig, prev in self._original_handlers.items():  # pragma: no cover
            try:  # pragma: no cover
                signal.signal(sig, prev)  # pragma: no cover
            except (ValueError, OSError):  # pragma: no cover
                pass  # pragma: no cover
        self._original_handlers.clear()

    async def _wait_in_flight(self, deadline: float | None) -> None:
        cond = self._ensure_inflight_cond()
        async with cond:
            if self._in_flight == 0:
                return
            while self._in_flight > 0:
                if deadline is None:
                    await cond.wait()
                    continue
                remaining = max(0.0, deadline - time.monotonic())
                if remaining <= 0:
                    break
                # R-timeout: use asyncio.timeout instead of asyncio.wait_for to
                # avoid the wrapper-task overhead and let the wait be cancelled
                # cleanly when the deadline expires.
                try:
                    async with asyncio.timeout(remaining):
                        await cond.wait()
                except TimeoutError:
                    break
            if self._in_flight > 0:
                logger.warning(
                    "AsyncBroker.stop: %d in-flight handler(s) still running after "
                    "graceful drain deadline; disconnecting anyway",
                    self._in_flight,
                )

    async def stop(self, timeout: float | None = None) -> None:
        """Stop the broker - stop pool, cancel consumers, drain, disconnect.

        ``timeout`` defaults to ``ConsumerConfig.graceful_timeout`` (C-2). The
        whole sequence is bounded by an overall deadline.
        """
        if not self._started:
            return

        self._restore_signal_handlers()

        effective = timeout if timeout is not None else self._consumer_config.graceful_timeout
        deadline = None if effective is None else time.monotonic() + effective

        # Stop worker pool first (let in-flight tasks finish), bounded by deadline.
        if self._worker_pool is not None:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            await self._worker_pool.stop(timeout=remaining)
            self._worker_pool = None

        # Cancel all consumers (stop new deliveries before draining).
        assert self._transport is not None
        for route in self._registry.routes:
            if route.consumer_tag:
                await self._transport.cancel_consumer(route.consumer_tag)

        # Drain inline in-flight handlers (C-2).
        await self._wait_in_flight(deadline)

        # Stop batch publisher (drain remaining messages before disconnecting)
        if self._batch_publisher is not None:
            await self._batch_publisher.stop()
            self._batch_publisher = None

        # Close RPC client if used
        if self._rpc_client is not None:
            await self._rpc_client.close()
            self._rpc_client = None

        # Disconnect
        if self._transport:
            await self._transport.disconnect()

        self._started = False
        logger.info("AsyncBroker stopped")

    # ── Publishing ────────────────────────────────────────────────────────

    async def publish(
        self,
        envelope: MessageEnvelope | None = None,
        *,
        exchange: str = "",
        routing_key: str = "",
        body: bytes | str | dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        content_type: str = "application/json",
        correlation_id: str | None = None,
        reply_to: str | None = None,
    ) -> PublishOutcome:
        """Publish a message.

        Accepts either a pre-built ``MessageEnvelope`` or individual kwargs::

            # Envelope form (full control):
            await broker.publish(MessageEnvelope(routing_key="orders.created", body=b"..."))

            # Kwargs form (convenient):
            await broker.publish(
                exchange="orders",
                routing_key="orders.created",
                body={"order_id": 123},
                headers={"x-tenant": "acme"},
            )

        When ``body`` is a dict or str it is JSON-encoded automatically.

        When an opt-in ``FlowController`` is configured (``broker.flow_controller
        = fc``), a publish slot is acquired/released around the transport publish
        so backpressure/rate-limiting applies to the hot path. Without a
        controller this is a plain pass-through.
        """
        if envelope is None:
            import json as _json

            if body is None:
                raw_body = b""
            elif isinstance(body, bytes):
                raw_body = body
            elif isinstance(body, str):
                raw_body = body.encode()
            else:
                raw_body = _json.dumps(body).encode()
            envelope = MessageEnvelope(
                routing_key=routing_key,
                body=raw_body,
                exchange=exchange,
                headers=headers or {},
                content_type=content_type,
                correlation_id=correlation_id,
                reply_to=reply_to,
            )

        if self._transport is None:
            raise RuntimeError("Broker not started. Call start() first.")

        publish_fn = (
            self._batch_publisher.publish
            if self._batch_publisher is not None
            else self._transport.publish
        )

        fc = self._flow_controller
        if fc is not None:
            if not await fc.acquire_async():
                return PublishOutcome(
                    status=PublishStatus.ERROR,
                    exchange=envelope.exchange,
                    routing_key=envelope.routing_key,
                    error=BackpressureError("publish dropped by backpressure policy"),
                )
            try:
                return await publish_fn(envelope)  # type: ignore[no-any-return]
            finally:
                await fc.release_async()
        return await publish_fn(envelope)  # type: ignore[no-any-return]

    async def request(
        self,
        routing_key: str,
        body: bytes,
        *,
        timeout: float = 5.0,
        exchange: str = "",
        headers: dict[str, Any] | None = None,
    ) -> RabbitMessage:
        """Send an RPC request and wait for a response.

        Lazily initializes an AsyncRPCClient on first call.
        The client is shared across calls and closed in stop().

        Args:
            routing_key: Target queue/routing key.
            body: Request body as bytes.
            timeout: Max seconds to wait for response.
            exchange: Exchange to publish to (default "").
            headers: Optional AMQP headers.

        Returns:
            RabbitMessage containing the response.

        Raises:
            RuntimeError: If broker is not started.
            RPCTimeoutError: If no response within timeout.
        """
        if self._transport is None:
            raise RuntimeError("Broker not started. Call start() first.")
        if self._rpc_client is None:
            from rabbitkit.rpc import AsyncRPCClient

            self._rpc_client = AsyncRPCClient(self._transport)
        return await self._rpc_client.call(routing_key, body, timeout=timeout, exchange=exchange, headers=headers)

    # ── Internal ──────────────────────────────────────────────────────────

    def _wire_retry_middleware(self) -> None:
        """Install ``RetryMiddleware`` on routes whose retry is enabled.

        ``_declare_topology`` declares the retry/DLQ *topology* (delay queues +
        source-queue DLX), but the ``RetryMiddleware`` that actually routes a
        failed message into the delay queues must also run in the route's
        middleware chain. Without it, ``retry=RetryConfig(...)`` would build the
        topology while transient failures ``nack(requeue=True)`` in a hot loop —
        the delay queues would never receive anything and ``max_retries`` would
        never be enforced. This wires both halves from the single retry switch
        (``RabbitConfig.retry`` / ``@subscriber(retry=...)``).

        Placed outer of ordinary user middlewares (e.g. ``TimeoutMiddleware``) so
        retry can classify and re-queue exceptions they raise, but inner of any
        ``ExceptionMiddleware`` (the documented true-outermost layer) — see
        :func:`rabbitkit.middleware.retry.retry_middleware_insertion_index`.

        Idempotent: routes that already carry a ``RetryMiddleware`` — supplied
        explicitly via ``middlewares=[...]`` or auto-wired on a previous start —
        are left untouched (no double-retry, no stacking on reconnect).
        """
        if self._transport is None:
            return

        from rabbitkit.middleware.retry import (
            RetryMiddleware,
            retry_middleware_insertion_index,
            warn_retry_middleware_without_topology,
        )

        wired = False
        for route in self._registry.routes:
            retry_config = route.effective_retry_config(self._config.retry)
            has_retry_mw = any(isinstance(mw, RetryMiddleware) for mw in route.route_middlewares)
            if retry_config is None:
                if has_retry_mw:
                    # Half-configured: a RetryMiddleware runs but no retry topology
                    # was declared, so its delay-queue publishes target non-existent
                    # queues and are silently dropped. Surface it loudly.
                    warn_retry_middleware_without_topology(route.name)
                continue
            if has_retry_mw:
                continue
            index = retry_middleware_insertion_index(route.route_middlewares)
            route.route_middlewares.insert(
                index, RetryMiddleware(retry_config, publish_async_fn=self._transport.publish)
            )
            wired = True

        if wired:
            # Drop any middleware chains cached before the retry mw was installed.
            self._pipeline.clear_caches()

    async def _declare_topology(self) -> None:
        """Declare exchanges, queues, and bindings for all routes."""
        if self._transport is None:
            return

        for route in self._registry.routes:
            # Declare exchange
            if route.exchange is not None:
                await self._transport.declare_exchange(route.exchange)

                # Exchange-to-exchange binding
                bind_kwargs = route.exchange.to_bind_kwargs()
                if bind_kwargs is not None:
                    await self._transport.bind_exchange(
                        destination=bind_kwargs["destination"],
                        source=bind_kwargs["source"],
                        routing_key=bind_kwargs["routing_key"],
                        arguments=bind_kwargs["arguments"],
                    )

            # Determine retry config early so source queue can include DLQ routing
            retry_config = route.effective_retry_config(self._config.retry)
            if retry_config is not None:
                retry_router = RetryRouter(retry_config)
                dlq_name = retry_router.get_dlq_name(route.queue.name)
                # Re-declare source queue with x-dead-letter fields so RabbitMQ
                # automatically routes nacked/rejected messages to the DLQ.
                import dataclasses

                source_queue = dataclasses.replace(
                    route.queue,
                    dead_letter_exchange="",
                    dead_letter_routing_key=dlq_name,
                )
            else:
                source_queue = route.queue

            # Declare queue (with DLQ routing arguments if retry enabled)
            await self._transport.declare_queue(source_queue)

            # Bind queue to exchange
            if route.exchange is not None:
                await self._transport.bind_queue(
                    queue=route.queue.name,
                    exchange=route.exchange.name,
                    routing_key=to_binding_key(route.queue.routing_key),
                )

            # Declare retry/DLQ topology if retry is enabled
            if retry_config is not None:
                exchange_name = route.exchange.name if route.exchange else ""
                delay_queues = retry_router.get_delay_queue_definitions(route.queue.name, exchange_name)
                for delay_queue in delay_queues:
                    await self._transport.declare_queue(delay_queue)

    async def _start_consumer(self, route: RouteDefinition) -> None:
        """Start consuming for a single route."""
        if self._transport is None:
            return

        transport = self._transport
        pool = self._worker_pool

        async def on_message(message: RabbitMessage) -> None:
            """Process incoming message through the pipeline."""
            # Track inline in-flight so stop() can drain gracefully (C-2).
            await self._in_flight_inc()
            # Heartbeat for liveness wedge detection (I-4): updated on every
            # delivery so broker_liveness can fail when the I/O loop stalls.
            self.last_heartbeat = time.monotonic()
            try:
                # Set the original queue in headers for retry routing
                if "x-rabbitkit-original-queue" not in message.headers:
                    message.headers["x-rabbitkit-original-queue"] = route.queue.name

                # Populate named routing-key segments for Path() DI
                message.path = extract_path(message.routing_key, route.queue.routing_key)

                await self._pipeline.process_async(
                    route,
                    message,
                    publish_fn=transport.publish,
                )
            finally:
                await self._in_flight_dec()

        if pool is not None:

            async def on_message_pooled(message: RabbitMessage) -> None:
                await pool.submit(on_message, message)

            callback = on_message_pooled
        else:
            callback = on_message

        effective_prefetch = route.prefetch_count or self._consumer_config.prefetch_count
        consumer_tag = await self._transport.consume(
            queue=route.queue.name,
            callback=callback,
            prefetch=effective_prefetch,
        )
        route.runtime_state.consumer_tag = consumer_tag
