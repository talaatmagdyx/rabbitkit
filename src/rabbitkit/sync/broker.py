"""SyncBroker — wires core registry + pipeline + SyncTransport.

The SyncBroker is the high-level entry point for sync applications.
It combines the registry (for handler registration), the pipeline
(for message processing), and the transport (for RabbitMQ I/O).

Graceful shutdown:
1. Cancel all consumers (basic_cancel per consumer_tag)
2. Wait for in-flight messages (up to graceful_timeout)
3. Close transport connection
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any

import structlog

from rabbitkit.concurrency import SyncWorkerPool
from rabbitkit.core.config import (
    RabbitConfig,
    RetryConfig,
    RetryDisabled,
    WorkerConfig,
)
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.registry import SubscriberRegistry
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy, MessageEnvelope, PublishOutcome
from rabbitkit.middleware.retry import RetryRouter
from rabbitkit.sync.connection import get_connection_errors
from rabbitkit.sync.transport import SyncTransport

logger = structlog.stdlib.get_logger(__name__)


class SyncBroker:
    """Sync broker — wires registry + pipeline + SyncTransport.

    Usage::

        config = RabbitConfig(connection=ConnectionConfig(host="localhost"))
        broker = SyncBroker(config)

        @broker.subscriber(queue="orders", exchange="events")
        def handle_order(body: bytes) -> None:
            ...

        broker.start()
    """

    def __init__(
        self,
        config: RabbitConfig | None = None,
        *,
        serializer: Any | None = None,
        di_resolver: Any | None = None,
        context_repo: Any | None = None,
    ) -> None:
        self._config = config or RabbitConfig()

        self._registry = SubscriberRegistry(broker_retry=self._config.retry)
        self._pipeline = HandlerPipeline(
            serializer=serializer,
            di_resolver=di_resolver,
            context_repo=context_repo,
        )

        self._transport: SyncTransport | None = None
        self._worker_pool: SyncWorkerPool | None = None
        self._started = False
        self._rpc_client: Any | None = None

    @property
    def config(self) -> RabbitConfig:
        return self._config

    @property
    def routes(self) -> list[RouteDefinition]:
        return self._registry.routes

    @property
    def worker_pool(self) -> SyncWorkerPool | None:
        """Return the worker pool (if configured)."""
        return self._worker_pool

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

    def start(self, worker_config: WorkerConfig | None = None) -> None:
        """Start the broker — connect, declare topology, start consuming.

        1. Connect to RabbitMQ
        2. Declare exchanges, queues, bindings per TopologyMode
        3. Declare retry topology (delay queues, DLQ)
        4. Optionally create a worker pool for concurrent processing
        5. Start consuming from all registered queues
        """
        if self._started:
            return

        # Configure structured logging if enabled
        if self._config.logging is not None:
            from rabbitkit.core.logging import configure_structlog

            configure_structlog(self._config.logging)

        # Create transport
        self._transport = SyncTransport(
            connection_config=self._config.connection,
            socket_config=self._config.socket,
            security_config=self._config.security,
            topology_mode=self._config.topology_mode,
            confirm_delivery=self._config.publisher.confirm_delivery,
        )

        self._transport.connect()

        # Declare topology
        self._declare_topology()

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
                self._config.consumer = replace(
                    self._config.consumer,
                    prefetch_count=worker_config.worker_count
                    * worker_config.prefetch_per_worker,
                )
            self._worker_pool = SyncWorkerPool(config=worker_config)
            self._worker_pool.start()

        # Start consuming
        for route in self._registry.routes:
            self._start_consumer(route)

        self._started = True
        logger.info(
            "SyncBroker started with %d routes",
            len(self._registry.routes),
        )

    def stop(self) -> None:
        """Stop the broker — stop pool, cancel consumers, disconnect."""
        if not self._started:
            return

        # Stop worker pool first (let in-flight tasks finish)
        if self._worker_pool is not None:
            self._worker_pool.stop()
            self._worker_pool = None

        # Cancel all consumers
        assert self._transport is not None
        for route in self._registry.routes:
            if route.consumer_tag:
                self._transport.cancel_consumer(route.consumer_tag)

        # Close RPC client if used
        if self._rpc_client is not None:
            self._rpc_client.close()
            self._rpc_client = None

        # Disconnect
        if self._transport:
            self._transport.disconnect()

        self._started = False
        logger.info("SyncBroker stopped")

    def run(self, worker_config: WorkerConfig | None = None) -> None:
        """Start and run the blocking consume loop.

        Blocks until KeyboardInterrupt or stop() is called. Recovers from
        connection drops by reconnecting, re-declaring topology, and
        re-subscribing all consumers — pika's BlockingConnection has no
        built-in recovery, so without this a single blip kills the consumer.

        ``worker_config`` is forwarded to :meth:`start`, so a multi-worker
        consumer (``worker_count > 1``) also gets the recovery loop.
        """
        self.start(worker_config=worker_config)
        connection_errors = get_connection_errors()
        try:
            while True:
                try:
                    if self._transport is None:
                        break
                    self._transport.start_consuming()
                    break  # clean stop_consuming() → exit
                except KeyboardInterrupt:
                    break
                except connection_errors as exc:
                    logger.warning("consumer connection lost; recovering", error=str(exc))
                    self._recover_consumers()
        finally:
            self.stop()

    # ── Publishing ────────────────────────────────────────────────────────

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Publish a message envelope."""
        if self._transport is None:
            raise RuntimeError("Broker not started. Call start() first.")
        return self._transport.publish(envelope)

    def request(
        self,
        routing_key: str,
        body: bytes,
        *,
        timeout: float = 5.0,
        exchange: str = "",
        headers: dict[str, Any] | None = None,
    ) -> RabbitMessage:
        """Send an RPC request and wait for a response (sync).

        Lazily initializes an RPCClient on first call.
        The client is shared across calls and closed in stop().
        """
        if self._transport is None:
            raise RuntimeError("Broker not started. Call start() first.")
        if self._rpc_client is None:
            from rabbitkit.rpc import RPCClient

            self._rpc_client = RPCClient(self._transport)
        return self._rpc_client.call(
            routing_key, body, timeout=timeout, exchange=exchange, headers=headers
        )

    # ── Internal ──────────────────────────────────────────────────────────

    def _recover_consumers(self) -> None:
        """Reconnect and re-establish topology + subscriptions after a drop."""
        if self._transport is None:
            return
        self._transport.reconnect()
        self._declare_topology()
        for route in self._registry.routes:
            self._start_consumer(route)

    def _declare_topology(self) -> None:
        """Declare exchanges, queues, and bindings for all routes."""
        if self._transport is None:
            return

        for route in self._registry.routes:
            # Declare exchange
            if route.exchange is not None:
                self._transport.declare_exchange(route.exchange)

                # Exchange-to-exchange binding
                bind_kwargs = route.exchange.to_bind_kwargs()
                if bind_kwargs is not None:
                    self._transport.bind_exchange(
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
            self._transport.declare_queue(source_queue)

            # Bind queue to exchange
            if route.exchange is not None:
                self._transport.bind_queue(
                    queue=route.queue.name,
                    exchange=route.exchange.name,
                    routing_key=route.queue.routing_key,
                )

            # Declare retry/DLQ topology if retry is enabled
            if retry_config is not None:
                exchange_name = route.exchange.name if route.exchange else ""
                delay_queues = retry_router.get_delay_queue_definitions(
                    route.queue.name, exchange_name
                )
                for delay_queue in delay_queues:
                    self._transport.declare_queue(delay_queue)

    def _start_consumer(self, route: RouteDefinition) -> None:
        """Start consuming for a single route."""
        if self._transport is None:
            return

        transport = self._transport
        pool = self._worker_pool

        def on_message(message: RabbitMessage) -> None:
            """Process incoming message through the pipeline."""
            # Set the original queue in headers for retry routing
            if "x-rabbitkit-original-queue" not in message.headers:
                message.headers["x-rabbitkit-original-queue"] = route.queue.name

            self._pipeline.process_sync(
                route,
                message,
                publish_fn=transport.publish,
            )

        if pool is not None:

            def on_message_pooled(message: RabbitMessage) -> None:
                pool.submit(on_message, message)

            callback = on_message_pooled
        else:
            callback = on_message

        effective_prefetch = route.prefetch_count or self._config.consumer.prefetch_count
        consumer_tag = self._transport.consume(
            queue=route.queue.name,
            callback=callback,
            prefetch=effective_prefetch,
        )
        route.consumer_tag = consumer_tag
