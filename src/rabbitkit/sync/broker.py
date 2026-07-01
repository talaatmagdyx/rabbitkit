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

import signal
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import structlog

from rabbitkit.concurrency import SyncWorkerPool
from rabbitkit.core.config import (
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
from rabbitkit.core.route import RouteDefinition, warn_filter_without_dlx
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy, MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.middleware.retry import RetryRouter
from rabbitkit.serialization.base import Serializer
from rabbitkit.sync.connection import get_connection_errors
from rabbitkit.sync.transport import SyncTransport

if TYPE_CHECKING:
    from rabbitkit.core.router import RabbitRouter

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
        serializer: Serializer[Any] | None = None,
        di_resolver: Any | None = None,
        context_repo: Any | None = None,
        middlewares: list[BaseMiddleware] | None = None,
    ) -> None:
        self._config = config or RabbitConfig()
        # C3: middlewares applied to every broker.publish() call — the primary
        # producer API. Distinct from @subscriber(middlewares=[...]), which
        # only wraps a route's HANDLER-RESULT publishes (Contract 5); without
        # this, e.g. SigningMiddleware never signed anything published via
        # broker.publish() directly. The composed chain is cached by this
        # list's identity (see HandlerPipeline.compose_broker_publish_sync), so
        # set the full list via this constructor param — mutating it in place
        # after the first publish() call would silently reuse the stale
        # pre-mutation chain.
        self._publish_middlewares: list[BaseMiddleware] = middlewares or []
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

        self._transport: SyncTransport | None = None
        self._worker_pool: SyncWorkerPool | None = None
        self._started = False
        self._rpc_client: Any | None = None

        # L14: liveness heartbeat (see health.broker_liveness). Read by
        # health.py via duck-typed attribute access (no formal HealthProvider
        # method for it). None until start() -- see the start() docstring for
        # why it's set there rather than only on delivery/tick.
        self.last_heartbeat: float | None = None

        # Bounded graceful drain (C-2): track inline in-flight handlers so
        # stop() can wait for them to finish (up to graceful_timeout) instead
        # of disconnecting mid-handler. R-Condition: a threading.Condition
        # replaces the prior Event+int+manual set/clear; ``_in_flight`` stays
        # a plain int for backward-compat reads (health checks).
        self._in_flight = 0
        self._in_flight_cond = threading.Condition()

        # Optional publish-side flow control (C-6). Opt-in via the
        # flow_controller setter; when set, publish() acquires/releases a
        # slot around transport.publish().
        self._flow_controller: Any | None = None

        # Signal-handler bookkeeping (C-1).
        self._original_handlers: dict[int, Any] = {}
        self._sigterm_thread: threading.Thread | None = None

    @property
    def flow_controller(self) -> Any | None:
        """Optional FlowController used to throttle the publish path."""
        return self._flow_controller

    @flow_controller.setter
    def flow_controller(self, value: Any | None) -> None:
        self._flow_controller = value
        # Wire the controller's blocked/unblocked callbacks to the transport if
        # it is already up (registration before start() is also fine: start()
        # wires them after constructing the transport).
        if self._transport is not None and value is not None:
            self._transport.on_blocked(value.on_blocked)
            self._transport.on_unblocked(value.on_unblocked)

    @property
    def config(self) -> RabbitConfig:
        return self._config

    @property
    def publish_middlewares(self) -> list[BaseMiddleware]:
        """Middlewares applied to every ``broker.publish()`` call (e.g. signing).

        Set via the constructor's ``middlewares=`` param. See the comment on
        ``self._publish_middlewares`` for why reassigning (not mutating) is
        required to change this after construction.
        """
        return self._publish_middlewares

    @property
    def routes(self) -> list[RouteDefinition]:
        return self._registry.routes

    @property
    def worker_pool(self) -> SyncWorkerPool | None:
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
        middlewares: list[BaseMiddleware] | None = None,
        serializer: Serializer[Any] | None = None,
        retry: RetryConfig | RetryDisabled | None = None,
        tags: frozenset[str] | set[str] | None = None,
        description: str = "",
        name: str | None = None,
        prefetch_count: int | None = None,
        filter_fn: Callable[[RabbitMessage], bool] | None = None,
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

    def include_router(self, router: RabbitRouter, prefix: str = "") -> None:
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

        L14: ``last_heartbeat`` is initialized here (not left ``None`` until
        the first message/tick) so a broker that is wedged from the very
        start -- before it ever processes a message or a
        ``start_consuming()`` loop iteration -- is still caught by
        :func:`health.broker_liveness`'s staleness check, instead of
        bypassing it entirely (a ``None`` heartbeat is treated as "no
        signal available" there, which previously meant "always alive").
        """
        if self._started:
            return

        self.last_heartbeat = time.monotonic()

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
            confirm_timeout=self._config.publisher.confirm_timeout,
        )

        self._transport.connect()
        self._transport.on_io_tick(self._mark_heartbeat)

        # Wire an opt-in FlowController's blocked/unblocked callbacks to the
        # transport now that it exists (C-6).
        if self._flow_controller is not None:
            self._transport.on_blocked(self._flow_controller.on_blocked)
            self._transport.on_unblocked(self._flow_controller.on_unblocked)

        # M-P5: a small publisher channel pool caps concurrent confirms. Warn
        # when confirms are on and the pool is small relative to expected
        # concurrency (kept as a non-fatal hint; default unchanged).
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

        # Declare topology
        self._declare_topology()

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

    def _in_flight_inc(self) -> None:
        with self._in_flight_cond:
            self._in_flight += 1

    def _in_flight_dec(self) -> None:
        with self._in_flight_cond:
            if self._in_flight > 0:
                self._in_flight -= 1
            if self._in_flight == 0:
                self._in_flight_cond.notify_all()

    def _wait_in_flight(self, deadline: float | None) -> None:
        """Wait for in-flight handlers to finish (bounded by deadline).

        H2: polls in short slices and pumps the transport's I/O loop between
        them (rather than one long condvar wait) so a worker thread's
        ack/nack/reject — marshaled onto the transport's owner thread via
        ``_run_on_io_thread`` once a consume loop has run — actually gets
        drained instead of stalling for the whole wait. Safe only because
        ``stop()`` (and therefore this method) runs on the transport's owner
        thread, matching ``SyncBroker.run()``'s call pattern.
        """
        transport = self._transport
        poll = 0.05
        with self._in_flight_cond:
            if self._in_flight == 0:
                return
            while self._in_flight > 0:
                if transport is not None:
                    transport.pump(poll)
                if deadline is None:
                    self._in_flight_cond.wait(timeout=poll)
                    continue
                remaining = max(0.0, deadline - time.monotonic())
                if remaining <= 0:
                    break
                self._in_flight_cond.wait(timeout=min(poll, remaining))
            if self._in_flight > 0:
                logger.warning(
                    "SyncBroker.stop: %d in-flight handler(s) still running after "
                    "graceful drain deadline; disconnecting anyway",
                    self._in_flight,
                )

    def stop(self, timeout: float | None = None) -> None:
        """Stop the broker - cancel consumers, drain pool, drain in-flight, disconnect.

        ``timeout`` defaults to ``ConsumerConfig.graceful_timeout`` (C-2). The
        whole stop sequence is bounded by an overall deadline.

        C5: consumers are cancelled FIRST, before the worker pool is drained.
        Draining the pool before cancelling left the consumer active for the
        entire (potentially graceful_timeout-long) drain wait — a message
        delivered in that window was submitted to a pool already mid-shutdown:
        ``SyncWorkerPool.submit()`` either raises ``RuntimeError`` (uncaught,
        propagating into pika's callback machinery) or, once ``.stop()`` has
        fully returned, silently runs the handler *inline* on the pika I/O
        thread — either way the message is never cleanly settled before
        ``disconnect()``, so it is redelivered (duplicate-processing risk) on
        the next connection. Cancelling first stops new deliveries outright,
        so the pool only ever drains work that was already in flight.
        """
        if not self._started:
            return

        effective = timeout if timeout is not None else self._consumer_config.graceful_timeout
        deadline = None if effective is None else time.monotonic() + effective

        # Cancel all consumers FIRST — stop new deliveries before draining
        # anything, so nothing new arrives while the pool/in-flight drain runs.
        assert self._transport is not None
        for route in self._registry.routes:
            if route.consumer_tag:
                self._transport.cancel_consumer(route.consumer_tag)

        # Drain the worker pool (let in-flight pooled tasks finish), bounded by
        # the outer deadline. H2: pass the transport's pump so a worker
        # thread's marshaled ack/nack/reject is actually drained during the
        # wait, instead of the transport falling back to an unsafe inline
        # cross-thread call once the consume loop has stopped.
        if self._worker_pool is not None:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            self._worker_pool.stop(timeout=remaining, pump=self._transport.pump)
            self._worker_pool = None

        # Drain inline in-flight handlers (C-2).
        self._wait_in_flight(deadline)

        # Close RPC client if used
        if self._rpc_client is not None:
            self._rpc_client.close()
            self._rpc_client = None

        # Disconnect
        if self._transport:
            self._transport.disconnect()

        self._started = False
        logger.info("SyncBroker stopped")

    def _install_sigterm_handler(self) -> None:
        """Install a SIGTERM handler that breaks the pika consume loop (C-1).

        pika's BlockingConnection is not signal-safe, so the handler spawns a
        short-lived daemon thread that calls ``transport.stop_consuming()``.
        Only installed when running in the main thread; failures are ignored.
        """
        try:
            self._original_handlers[signal.SIGTERM] = signal.signal(signal.SIGTERM, self._on_sigterm)
        except (ValueError, OSError):  # pragma: no cover - not in main thread - best effort
            logger.debug("SIGTERM handler not installed (not in main thread)")

    def _restore_signal_handlers(self) -> None:
        for sig, prev in self._original_handlers.items():
            try:
                signal.signal(sig, prev)
            except (ValueError, OSError):  # pragma: no cover
                pass
        self._original_handlers.clear()

    def _on_sigterm(self, signum: int, frame: Any) -> None:
        logger.info("Received SIGTERM; initiating graceful drain")
        # pika BlockingConnection is not signal-safe: do the stop on a thread.
        transport = self._transport
        if transport is None:
            return

        def _drain() -> None:
            try:
                transport.stop_consuming()
            except Exception:  # pragma: no cover - best effort
                logger.warning("stop_consuming raised during SIGTERM drain", exc_info=True)

        self._sigterm_thread = threading.Thread(target=_drain, name="rabbitkit-sigterm", daemon=True)
        self._sigterm_thread.start()

    def run(self, worker_config: WorkerConfig | None = None) -> None:
        """Start and run the blocking consume loop.

        Blocks until SIGINT/SIGTERM or stop() is called. Installs a SIGTERM
        handler (C-1) so k8s pod termination drains instead of hard-killing.
        Recovers from connection drops by reconnecting, re-declaring topology,
        and re-subscribing all consumers - pika's BlockingConnection has no
        built-in recovery, so without this a single blip kills the consumer.

        ``worker_config`` is forwarded to :meth:`start`, so a multi-worker
        consumer (``worker_count > 1``) also gets the recovery loop.
        """
        self.start(worker_config=worker_config)
        self._install_sigterm_handler()
        connection_errors = get_connection_errors()
        try:
            while True:
                try:
                    if self._transport is None:
                        break
                    self._transport.start_consuming()
                    break  # clean stop_consuming() -> exit
                except KeyboardInterrupt:
                    break
                except connection_errors as exc:
                    logger.warning("consumer connection lost; recovering", error=str(exc))
                    self._recover_consumers()
        finally:
            self._restore_signal_handlers()
            self.stop()

    # ── Publishing ────────────────────────────────────────────────────────

    def publish(
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
            broker.publish(MessageEnvelope(routing_key="orders.created", body=b"..."))

            # Kwargs form (convenient):
            broker.publish(
                exchange="orders",
                routing_key="orders.created",
                body={"order_id": 123},
                headers={"x-tenant": "acme"},
            )

        When ``body`` is a dict or str it is JSON-encoded automatically.

        When an opt-in ``FlowController`` is configured (``broker.flow_controller
        = fc``), a publish slot is acquired before and released after the
        transport publish so backpressure/rate-limiting applies to the hot path.
        Without a controller this is a plain pass-through.

        When ``middlewares=[...]`` was passed to the constructor, each
        middleware's ``publish_scope`` wraps this call (e.g. ``SigningMiddleware``
        signs the envelope) — see ``publish_middlewares``. Middleware wraps
        OUTSIDE the flow-control gate, so a middleware-transformed envelope is
        what gets rate-limited/blocked, and what the transport actually sends.
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
        transport = self._transport  # narrowed local capture for the closure below

        def do_transport_publish(env: MessageEnvelope) -> PublishOutcome:
            fc = self._flow_controller
            if fc is not None:
                if not fc.acquire():
                    # Dropped by backpressure policy (drop/timeout). Do not publish.
                    return PublishOutcome(
                        status=PublishStatus.ERROR,
                        exchange=env.exchange,
                        routing_key=env.routing_key,
                        error=BackpressureError("publish dropped by backpressure policy"),
                    )
                try:
                    return transport.publish(env)
                finally:
                    fc.release()
            return transport.publish(env)

        if self._publish_middlewares:
            chain = self._pipeline.compose_broker_publish_sync(self._publish_middlewares)
            outcome: PublishOutcome = chain(envelope, do_transport_publish)
            return outcome
        return do_transport_publish(envelope)

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
        return self._rpc_client.call(routing_key, body, timeout=timeout, exchange=exchange, headers=headers)

    # ── Internal ──────────────────────────────────────────────────────────

    def _recover_consumers(self) -> None:
        """Reconnect and re-establish topology + subscriptions after a drop."""
        if self._transport is None:
            return
        self._transport.reconnect()
        self._declare_topology()
        for route in self._registry.routes:
            self._start_consumer(route)

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

        from rabbitkit.middleware.metrics import MetricsMiddleware
        from rabbitkit.middleware.retry import (
            RetryMiddleware,
            retry_middleware_insertion_index,
            warn_retry_middleware_without_topology,
            warn_retry_without_confirms,
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
            if not self._config.publisher.confirm_delivery:
                warn_retry_without_confirms(route.name)  # M4
            index = retry_middleware_insertion_index(route.route_middlewares)
            # M2: wire in an existing route MetricsMiddleware (if any) so
            # messages_retried_total/dead_lettered_total are observable.
            metrics_mw = next(
                (mw for mw in route.route_middlewares if isinstance(mw, MetricsMiddleware)), None
            )
            route.route_middlewares.insert(
                index,
                RetryMiddleware(
                    retry_config,
                    publish_fn=self._transport.publish,
                    metrics_collector=metrics_mw.collector if metrics_mw else None,
                    metrics_config=metrics_mw.config if metrics_mw else None,
                ),
            )
            wired = True

        if not self._config.publisher.confirm_delivery:
            for route in self._registry.routes:
                if route.result_publisher is not None:
                    warn_retry_without_confirms(route.name, context="result")  # M4

        if wired:
            # Drop any middleware chains cached before the retry mw was installed.
            self._pipeline.clear_caches()

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
            # H6: a filter_fn rejection nack(requeue=False)'s the message; without a
            # DLX RabbitMQ just discards it. Retry-enabled routes already get one
            # below; a manually-configured dead_letter_exchange is respected as-is.
            # Otherwise auto-declare a "<queue>.dlq" DLQ so filter rejections are
            # preserved even with no retry configured.
            filter_dlq_name: str | None = None
            if (
                retry_config is None
                and route.filter_fn is not None
                and route.queue.dead_letter_exchange is None
            ):
                filter_dlq_name = f"{route.queue.name}.dlq"
                warn_filter_without_dlx(route.name, route.queue.name, filter_dlq_name)

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
            elif filter_dlq_name is not None:
                import dataclasses

                source_queue = dataclasses.replace(
                    route.queue,
                    dead_letter_exchange="",
                    dead_letter_routing_key=filter_dlq_name,
                )
            else:
                source_queue = route.queue

            # Declare queue (with DLQ routing arguments if retry/filter-DLX applies)
            self._transport.declare_queue(source_queue)

            # Bind queue to exchange
            if route.exchange is not None:
                self._transport.bind_queue(
                    queue=route.queue.name,
                    exchange=route.exchange.name,
                    routing_key=to_binding_key(route.queue.routing_key),
                )

            # Declare retry/DLQ topology if retry is enabled
            if retry_config is not None:
                exchange_name = route.exchange.name if route.exchange else ""
                delay_queues = retry_router.get_delay_queue_definitions(route.queue.name, exchange_name)
                for delay_queue in delay_queues:
                    self._transport.declare_queue(delay_queue)
            elif filter_dlq_name is not None:
                self._transport.declare_queue(RabbitQueue(name=filter_dlq_name, durable=True))

    def _mark_heartbeat(self) -> None:
        """Refresh the liveness heartbeat (I-4/L14).

        Called both per delivered message (``on_message`` below) and once
        per ``start_consuming()`` I/O loop tick (wired via
        ``transport.on_io_tick`` in :meth:`start`) -- the latter is what
        keeps a healthy but message-idle consumer from being mistaken for a
        wedged one by :func:`health.broker_liveness`.
        """
        self.last_heartbeat = time.monotonic()

    def _start_consumer(self, route: RouteDefinition) -> None:
        """Start consuming for a single route."""
        if self._transport is None:
            return

        transport = self._transport
        pool = self._worker_pool

        def on_message(message: RabbitMessage) -> None:
            """Process incoming message through the pipeline."""
            # Track inline in-flight so stop() can drain gracefully (C-2).
            self._in_flight_inc()
            self._mark_heartbeat()
            try:
                # Set the original queue in headers for retry routing
                if "x-rabbitkit-original-queue" not in message.headers:
                    message.headers["x-rabbitkit-original-queue"] = route.queue.name

                # Populate named routing-key segments for Path() DI
                message.path = extract_path(message.routing_key, route.queue.routing_key)

                self._pipeline.process_sync(
                    route,
                    message,
                    publish_fn=transport.publish,
                )
            finally:
                self._in_flight_dec()

        if pool is not None:

            def on_message_pooled(message: RabbitMessage) -> None:
                pool.submit(on_message, message)

            callback = on_message_pooled
        else:
            callback = on_message

        effective_prefetch = route.prefetch_count or self._consumer_config.prefetch_count
        consumer_tag = self._transport.consume(
            queue=route.queue.name,
            callback=callback,
            prefetch=effective_prefetch,
        )
        route.runtime_state.consumer_tag = consumer_tag
