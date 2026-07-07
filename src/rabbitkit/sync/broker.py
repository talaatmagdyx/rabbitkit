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
from rabbitkit.core.errors import BackpressureError, BrokerNotStartedError, MessageTooLargeError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.path import extract_path, to_binding_key
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.registry import SubscriberRegistry
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import (
    AckPolicy,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    QueueType,
    TopologyMode,
)
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
            reject_transient_on_redelivery=self._config.consumer.reject_transient_on_redelivery,
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
    def started(self) -> bool:
        """True between a successful ``start()`` and ``stop()``.

        Public counterpart of ``_started`` — health checks
        (:func:`rabbitkit.health.broker_health_check`) read this instead of
        falling back to the private attribute (which emits a
        DeprecationWarning).
        """
        return self._started

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
        reject_without_dlx: str | None = None,
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
            reject_without_dlx=reject_without_dlx,
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
            on_topology_conflict=self._config.safety.on_topology_conflict,
        )

        self._transport.connect()
        self._transport.on_io_tick(self._mark_heartbeat)

        # Wire an opt-in FlowController's blocked/unblocked callbacks to the
        # transport now that it exists (C-6).
        if self._flow_controller is not None:
            self._transport.on_blocked(self._flow_controller.on_blocked)
            self._transport.on_unblocked(self._flow_controller.on_unblocked)

        # M2: the old M-P5 "channel_pool_size caps concurrent confirms"
        # warning was removed. The default SyncTransport publishes on a single
        # dedicated channel (not SyncChannelPool — see sync/pool.py), so
        # channel_pool_size does not gate publish concurrency or confirms;
        # tuning it changed nothing on this path. The real sync
        # confirmed-publish ceiling (RTT-bound, ~0.9k msg/s, H6) is documented
        # on SyncBroker.publish and in the README.

        # Declare topology
        self._declare_topology()

        # Install RetryMiddleware on retry-enabled routes (topology alone does
        # not retry — the middleware routes failures into the delay queues).
        self._wire_retry_middleware()

        # Connection-churn counter: reconnects were logged but never counted.
        self._wire_reconnect_metric()

        # H2: single-worker sync consumers run the handler INLINE on the pika
        # I/O thread, so nothing services heartbeat frames while a handler
        # runs. A handler that runs longer than ~2x the heartbeat interval
        # gets its connection killed broker-side mid-handler → the ack fails,
        # the message is redelivered, and side effects can repeat (possibly in
        # a loop). worker_count>1 is immune (handlers run on pool threads while
        # the I/O thread keeps pumping). Warn so the failure mode is visible.
        single_worker = worker_config is None or worker_config.worker_count <= 1
        if single_worker and self._registry.routes and self._config.connection.heartbeat > 0:
            import warnings

            warnings.warn(
                f"Starting a single-worker sync consumer (worker_count=1) with "
                f"heartbeat={self._config.connection.heartbeat}s. Handlers run inline on "
                "the I/O thread, so any handler taking longer than ~"
                f"{self._config.connection.heartbeat * 2}s will starve heartbeats and the "
                "broker will drop the connection mid-handler (→ redelivery + duplicate "
                "side effects). For slow handlers, pass "
                "start(worker_config=WorkerConfig(worker_count=N)) so handlers run off the "
                "I/O thread, or raise ConnectionConfig.heartbeat well above your worst-case "
                "handler duration.",
                RuntimeWarning,
                stacklevel=2,
            )

        # Create worker pool if requested
        if worker_config is not None and worker_config.worker_count > 1:
            # M2: the old channel_pool_size deadlock warning was removed — the
            # default SyncTransport publishes on a single dedicated publisher
            # channel, NOT through SyncChannelPool (see sync/pool.py), so
            # worker_count vs channel_pool_size cannot cause the described
            # publish deadlock. Tuning channel_pool_size changed nothing on
            # this path; the warning only misled operators.
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

    def pump_idle(self, time_limit: float = 0.05) -> None:
        """Service the connection's I/O loop without consuming (idle keep-alive).

        ``run()``/``start_consuming()`` pumps ``process_data_events()``
        continuously while consumers are registered, which incidentally
        keeps the (single, shared) connection's heartbeats serviced too —
        see ``sync/transport.py``'s module docstring on the one-connection
        model. A **publish-only** broker (no registered routes, or one that
        never calls ``run()``) has nothing driving that pump: the connection
        is only touched when ``publish()`` actually runs, so a long idle gap
        can get it heartbeat-timed-out broker-side, and a dead connection is
        only discovered (and reconnected) on the *next* publish attempt.

        Call this periodically — from the SAME thread that called
        ``start()``, same invariant as every other transport call — from
        your own idle loop (e.g. between polling for work) to reconnect
        proactively if the connection died, service pending heartbeat
        frames, and refresh the liveness heartbeat (see
        ``health.broker_liveness``) even though no message was delivered.
        A no-op if the broker is not started.
        """
        if self._transport is None:
            return
        self._transport.ensure_connected()
        self._transport.pump(time_limit)
        self._mark_heartbeat()

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

        Throughput note (H6): with publisher confirms on (the default), this
        waits for a broker confirm per message on a single channel, so it is
        RTT-bound at ~0.9k msg/s and does NOT scale with worker_count — pika's
        BlockingConnection serializes confirms, it cannot pipeline them. To
        drain a large backlog fast, use AsyncBroker + AsyncBatchPublisher
        (pipelined confirms, ~6.1k msg/s) or more processes.
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
            # M2: honor PublisherConfig defaults for the kwargs form (they were
            # previously dead config). Envelope-form callers keep full control.
            envelope = MessageEnvelope(
                routing_key=routing_key,
                body=raw_body,
                exchange=exchange,
                headers=headers or {},
                content_type=content_type,
                correlation_id=correlation_id,
                reply_to=reply_to,
                mandatory=self._config.publisher.mandatory,
                delivery_mode=2 if self._config.publisher.persistent else 1,
            )

        # M10: reject oversized bodies at publish time (input validation at
        # the trust boundary — a too-large message is a programming/policy
        # error, caught before it hits the wire).
        max_bytes = self._config.publisher.max_message_bytes
        if max_bytes and len(envelope.body) > max_bytes:
            raise MessageTooLargeError(
                f"Message body ({len(envelope.body)} bytes) exceeds "
                f"PublisherConfig.max_message_bytes ({max_bytes}). Large messages are a "
                "RabbitMQ anti-pattern — store the payload externally and publish a "
                "reference, or raise the limit if this is intentional."
            )

        if self._transport is None:
            raise BrokerNotStartedError("Broker not started. Call start() first.")
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

    def _flow_controlled_internal_publish(self, env: MessageEnvelope) -> PublishOutcome:
        """M18: apply the broker's ``FlowController`` (if configured) to an
        INTERNAL republish — ``RetryMiddleware``'s delay-queue publish, or a
        handler's result/RPC-reply publish — used as their ``publish_fn``
        instead of the raw, unthrottled ``transport.publish``.

        Deliberately diverges from ``do_transport_publish`` above: a
        configured ``on_blocked="raise"`` must NEVER raise ``BackpressureError``
        out of here. ``RetryMiddleware`` and the pipeline's result-publish path
        only understand a returned ``PublishOutcome`` (checked via ``.ok``),
        not exceptions — letting one escape would propagate as an unclassified
        error, default to PERMANENT, and reject/destroy the message instead of
        the existing safe nack+requeue-on-publish-failure behavior both paths
        already implement. So regardless of the configured policy, a
        blocked/dropped slot here always resolves as a failed
        ``PublishOutcome`` (status=ERROR), never an exception — the same
        outcome shape a real transport failure already produces.
        """
        if self._transport is None:  # pragma: no cover — defensive; callers only run while consuming
            raise BrokerNotStartedError("Broker not started. Call start() first.")
        transport = self._transport
        fc = self._flow_controller
        if fc is None:
            return transport.publish(env)
        try:
            acquired = fc.acquire()
        except BackpressureError as exc:
            return PublishOutcome(
                status=PublishStatus.ERROR, exchange=env.exchange, routing_key=env.routing_key, error=exc
            )
        if not acquired:
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
            raise BrokerNotStartedError("Broker not started. Call start() first.")
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
        from rabbitkit.middleware.signing import check_signing_retry_conflict

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
            # H1: signing + retry destroys every retried message — fail fast.
            check_signing_retry_conflict(route.name, route.route_middlewares)
            if has_retry_mw:
                # A user-constructed RetryMiddleware: inject the broker's
                # confirmed publish fn if none was passed, so it routes to the
                # delay queues instead of nack-hot-looping (or, historically,
                # ack-dropping) every transient failure.
                for mw in route.route_middlewares:
                    if isinstance(mw, RetryMiddleware):
                        mw.ensure_publish_fns(publish_fn=self._flow_controlled_internal_publish)
                continue
            # (No confirms warning for the RETRY context: retry envelopes are
            # published mandatory=True, which forces per-publish confirm mode
            # on both transports even when confirm_delivery=False — the
            # ack-after-confirmed-outcome invariant holds regardless.)
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
                    publish_fn=self._flow_controlled_internal_publish,  # M18: honor FlowController
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

    def _wire_reconnect_metric(self) -> None:
        """Count transport reconnects (connection churn) via the first route
        ``MetricsMiddleware``'s collector, if any. Reconnects were logged but
        never counted, so a flapping broker/network was invisible to
        metrics-based alerting. No-op when no route carries metrics."""
        if self._transport is None:
            return
        from rabbitkit.middleware.metrics import MetricsMiddleware

        metrics_mw = next(
            (
                mw
                for route in self._registry.routes
                for mw in route.route_middlewares
                if isinstance(mw, MetricsMiddleware) and mw.collector is not None
            ),
            None,
        )
        if metrics_mw is None or metrics_mw.collector is None:
            return
        collector = metrics_mw.collector
        metric_name = metrics_mw.config.reconnects_total
        self._transport.on_reconnect(lambda: collector.inc_counter(metric_name, {}))

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
            # C3: a route with no dead-letter path can reject(requeue=False)
            # (permanent errors, filter_fn, RejectMessage) and RabbitMQ would
            # DISCARD the message. Apply SafetyConfig.reject_without_dlx:
            # auto-provision "<queue>.dlq" (default), fail startup, or warn
            # and allow discard. Only under AUTO_DECLARE — in passive/manual
            # modes rabbitkit does not own the queue's arguments.
            safety_dlq_name: str | None = None
            if self._config.topology_mode is TopologyMode.AUTO_DECLARE:
                safety_dlq_name = route.resolve_safety_dlq(self._config.safety, self._config.retry)
            # A queue that IS another route's DLQ is terminal — consuming your
            # own DLQ is a legitimate pattern (inspect/replay consumers), and
            # auto-chaining more topology onto it (safety DLX injection, or
            # BROKER-DEFAULT retry inherited by the DLQ-consumer route) would
            # re-declare the DLQ with different arguments than the retry/
            # safety topology already declared it with — a 406 inequivalent-
            # arg startup failure, caught by the real-broker CI suite. An
            # EXPLICIT per-route retry= on a DLQ consumer still wins.
            is_anothers_dlq = any(
                other is not route and route.queue.name == f"{other.queue.name}.dlq"
                for other in self._registry.routes
            )
            if is_anothers_dlq:
                safety_dlq_name = None
                if route.retry_override is None:
                    retry_config = None  # don't inherit broker-default retry

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
            elif safety_dlq_name is not None:
                import dataclasses

                logger.info(
                    "Auto-provisioned DLQ %r for queue %r (reject_without_dlx=auto_provision)",
                    safety_dlq_name,
                    route.queue.name,
                )
                source_queue = dataclasses.replace(
                    route.queue,
                    dead_letter_exchange="",
                    dead_letter_routing_key=safety_dlq_name,
                )
            else:
                source_queue = route.queue

            # Declare queue (with DLQ routing arguments if retry/safety DLX applies)
            self._transport.declare_queue(source_queue)

            # Bind queue to exchange (C4: bind_arguments matter for headers exchanges)
            if route.exchange is not None:
                self._transport.bind_queue(
                    queue=route.queue.name,
                    exchange=route.exchange.name,
                    routing_key=to_binding_key(route.queue.routing_key),
                    arguments=route.queue.bind_arguments or None,
                )

            # Declare retry/DLQ topology if retry is enabled
            if retry_config is not None:
                exchange_name = route.exchange.name if route.exchange else ""
                delay_queues = retry_router.get_delay_queue_definitions(
                    route.queue.name, exchange_name, source_queue_type=route.queue.queue_type
                )
                for delay_queue in delay_queues:
                    self._transport.declare_queue(delay_queue)
            elif safety_dlq_name is not None:
                self._transport.declare_queue(
                    RabbitQueue(
                        name=safety_dlq_name,
                        durable=True,
                        # Inherit quorum from a quorum source (see RetryRouter
                        # DLQ note): the DLQ stores failures indefinitely.
                        queue_type=(
                            QueueType.QUORUM
                            if route.queue.queue_type == QueueType.QUORUM
                            else QueueType.CLASSIC
                        ),
                    )
                )

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

        pool = self._worker_pool

        def on_message(message: RabbitMessage) -> None:
            """Process incoming message through the pipeline."""
            # Track inline in-flight so stop() can drain gracefully (C-2).
            self._in_flight_inc()
            self._mark_heartbeat()
            try:
                # Set the original queue in headers for retry routing.
                # H2 (spoofing): ALWAYS overwrite — a legitimate retry round-trip
                # returns to this same queue, so the overwrite is always
                # correct, while honoring a producer-set value would let a
                # malicious/buggy publisher steer retries into another
                # route's delay ladder (cross-queue injection) or a
                # nonexistent queue (requeue hot loop).
                message.headers["x-rabbitkit-original-queue"] = route.queue.name

                # Populate named routing-key segments for Path() DI
                message.path = extract_path(message.routing_key, route.queue.routing_key)

                try:
                    self._pipeline.process_sync(
                        route,
                        message,
                        publish_fn=self._flow_controlled_internal_publish,  # M18
                    )
                except Exception:
                    # M12: AUTO/NACK_ON_ERROR settle inside the pipeline and
                    # never reach here — but a MANUAL-policy handler that
                    # raises without settling used to propagate out of the
                    # delivery callback and STOP the entire run loop (one bad
                    # handler took down the broker). Contain it: log and
                    # nack-requeue if still unsettled, so the failure degrades
                    # to a redelivery instead of a broker-wide halt. Matches
                    # the pooled/async paths, which already isolate handler
                    # exceptions.
                    logger.error(
                        "Handler raised through the pipeline; nacking for redelivery",
                        queue=route.queue.name,
                        message_id=message.message_id,
                        exc_info=True,
                    )
                    if not message.is_settled:
                        message.nack(requeue=True)
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
