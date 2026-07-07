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
import contextlib
import signal
import time
from collections.abc import Callable
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import structlog

from rabbitkit.async_.transport import AsyncTransportImpl
from rabbitkit.concurrency import AsyncWorkerPool
from rabbitkit.core.config import (
    BatchPublishConfig,
    ConsumerConfig,
    RabbitConfig,
    RetryConfig,
    RetryDisabled,
    SocketConfig,
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

if TYPE_CHECKING:
    from rabbitkit.core.router import RabbitRouter

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

    # L14: liveness heartbeat tick interval -- well under any reasonable
    # health.broker_liveness(wedged_timeout=...) so idle-but-healthy periods
    # never spuriously trip liveness.
    _HEARTBEAT_INTERVAL: float = 5.0

    def __init__(
        self,
        config: RabbitConfig | None = None,
        *,
        serializer: Serializer[Any] | None = None,
        di_resolver: Any | None = None,
        context_repo: Any | None = None,
        batch_config: BatchPublishConfig | None = None,
        middlewares: list[BaseMiddleware] | None = None,
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
            reject_transient_on_redelivery=self._config.consumer.reject_transient_on_redelivery,
        )

        # C3: middlewares applied to every broker.publish() call — the primary
        # producer API. Distinct from @subscriber(middlewares=[...]), which
        # only wraps a route's HANDLER-RESULT publishes (Contract 5); without
        # this, e.g. SigningMiddleware never signed anything published via
        # broker.publish() directly. The composed chain is cached by this
        # list's identity (see HandlerPipeline.compose_broker_publish_async), so
        # set the full list via this constructor param — mutating it in place
        # after the first publish() call would silently reuse the stale
        # pre-mutation chain.
        self._publish_middlewares: list[BaseMiddleware] = middlewares or []

        self._batch_config = batch_config
        self._batch_publisher: Any | None = None  # BatchPublisher, started lazily in start()
        self._transport: Any | None = None  # AsyncTransportImpl
        self._worker_pool: AsyncWorkerPool | None = None
        self._started = False
        self._rpc_client: Any | None = None

        # L14: liveness heartbeat (see health.broker_liveness). None until
        # start() -- see the start() docstring for why it's set there rather
        # than only on delivery/tick.
        self.last_heartbeat: float | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
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
        # Task/message pairs for in-flight INLINE consumption (no worker pool
        # configured -- the default). Mirrors AsyncWorkerPool._task_messages so
        # a drain-deadline timeout can cancel + nack the still-running ones
        # with delivery-tag logging, the same as the pooled path already does,
        # instead of silently abandoning them unacked.
        self._inflight_tasks: dict[asyncio.Task[None], RabbitMessage] = {}

        # Optional publish-side flow control (C-6).
        self._flow_controller: Any | None = None

        # Signal-handler bookkeeping (H-SRE5).
        self._original_handlers: dict[signal.Signals, Any] = {}
        self._installed_loop_handlers = False
        self._loop: asyncio.AbstractEventLoop | None = None

        # H11: shutdown event awaited by run() so the drain triggered by a
        # signal (or request_shutdown()) is joined instead of fire-and-forget.
        # _run_waiting is True only while run() is actually awaiting the
        # event, so a signal received under bare start() usage still falls
        # back to the pre-H11 fire-and-forget stop() task.
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._run_waiting = False

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

    def _mark_heartbeat(self) -> None:
        """Refresh the liveness heartbeat (I-4/L14).

        Called both per delivered message (``on_message`` in
        :meth:`_start_consumer`) and periodically by ``_heartbeat_loop`` --
        the latter is what keeps a healthy but message-idle consumer from
        being mistaken for a wedged one by :func:`health.broker_liveness`.
        """
        self.last_heartbeat = time.monotonic()

    async def _heartbeat_loop(self) -> None:
        """L14: periodic liveness heartbeat -- the async analogue of the sync
        broker's per-``start_consuming()``-tick heartbeat.

        aio-pika has no exposed manual I/O-loop-tick to hook (unlike pika's
        ``process_data_events``); this task ticking on its own interval is
        itself the liveness signal instead -- if the event loop were
        genuinely wedged (blocked, not just disconnected), this task would
        not get scheduled and the heartbeat would correctly go stale. A
        transient disconnect during reconnect is intentionally NOT
        distinguished from "healthy but idle" here: ``broker_liveness``
        documents that a transient disconnect is not itself a liveness
        failure, and a reconnect attempt completes well within
        ``wedged_timeout`` in practice -- only a reconnect loop stuck for the
        full timeout window would (correctly) trip liveness.
        """
        try:
            while True:
                await asyncio.sleep(self._HEARTBEAT_INTERVAL)
                self._mark_heartbeat()
        except asyncio.CancelledError:
            pass

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

        L14: ``last_heartbeat`` is initialized here (not left ``None`` until
        the first message/tick) so a broker that is wedged from the very
        start -- before it ever processes a message or a periodic heartbeat
        tick -- is still caught by :func:`health.broker_liveness`'s
        staleness check, instead of bypassing it entirely (a ``None``
        heartbeat is treated as "no signal available" there, which
        previously meant "always alive").
        """
        if self._started:
            return

        self.last_heartbeat = time.monotonic()

        # Configure structured logging if enabled
        if self._config.logging is not None:
            from rabbitkit.core.logging import configure_structlog

            configure_structlog(self._config.logging)

        # SocketConfig is sync-only: pika accepts tcp_options, but
        # aio-pika/aiormq exposes no socket-tuning knobs, and applying
        # setsockopt to the live socket wouldn't survive connect_robust's
        # automatic reconnects (each reconnect is a fresh, untuned socket).
        # Warn instead of silently ignoring a config the user set.
        if self._config.socket != SocketConfig():
            import warnings

            warnings.warn(
                "RabbitConfig.socket (SocketConfig) is not applied by AsyncBroker: "
                "aio-pika manages its own sockets and provides no TCP-tuning "
                "options, and per-socket tuning would be silently lost on every "
                "automatic reconnect. SocketConfig only affects SyncBroker; tune "
                "the async side via ConnectionConfig (heartbeat, timeouts) or at "
                "the OS level.",
                RuntimeWarning,
                stacklevel=2,
            )

        # Create transport
        self._transport = AsyncTransportImpl(
            connection_config=self._config.connection,
            security_config=self._config.security,
            pool_config=self._config.pool,
            topology_mode=self._config.topology_mode,
            confirm_delivery=self._config.publisher.confirm_delivery,
            confirm_timeout=self._config.publisher.confirm_timeout,
            on_topology_conflict=self._config.safety.on_topology_conflict,
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

        # Connection-churn counter: reconnects were logged but never counted.
        self._wire_reconnect_metric()

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

        # L14: periodic liveness heartbeat -- keeps a healthy, message-idle
        # consumer from going stale between deliveries. See _heartbeat_loop.
        self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())

        self._started = True
        logger.info(
            "AsyncBroker started with %d routes",
            len(self._registry.routes),
        )

        # H11: clear any shutdown request left over from a previous
        # start()/stop() cycle so run() doesn't return immediately.
        self._shutdown_event.clear()

        if install_signal_handlers:
            self._install_signal_handlers()

    async def run(self, worker_config: WorkerConfig | None = None) -> None:
        """Start, wait for a shutdown signal, then stop (H11).

        ``await broker.start()`` alone installs signal handlers whose drain is
        fire-and-forget — nothing joins the ``stop()`` task they create, so
        whether in-flight messages actually finish draining depends on
        incidental event-loop lifetime (e.g. it can be cut short by
        ``asyncio.run()`` cancelling outstanding tasks once the awaited
        coroutine returns). ``run()`` is the direct-use equivalent of
        ``RabbitApp.run_async()``: it does not return until the drain
        triggered by SIGINT/SIGTERM (or :meth:`request_shutdown`) has fully
        completed, so awaiting it end-to-end guarantees in-flight messages are
        settled before the process exits::

            broker = AsyncBroker(config)

            @broker.subscriber(queue="orders")
            async def handle_order(body: bytes) -> None: ...

            asyncio.run(broker.run())

        Use plain ``start()``/``stop()`` instead when an outer lifecycle
        manager (``RabbitApp``) owns the run loop.
        """
        await self.start(worker_config=worker_config)
        self._run_waiting = True
        try:
            await self._shutdown_event.wait()
        finally:
            self._run_waiting = False
            await self.stop()

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

    def _trigger_shutdown(self) -> None:
        """Set the shutdown event so an in-progress ``run()`` joins the drain
        (H11). If nothing is awaiting it via ``run()``, falls back to the
        pre-H11 fire-and-forget ``stop()`` task so bare ``await
        broker.start()`` usage still drains on signal.
        """
        self._shutdown_event.set()
        if not self._run_waiting and self._loop is not None:
            self._loop.create_task(self.stop())

    def _on_signal(self) -> None:
        logger.info("Received shutdown signal; initiating graceful drain")
        self._trigger_shutdown()
        if self.on_app_shutdown is not None:
            try:
                self.on_app_shutdown()
            except Exception:  # pragma: no cover - never block shutdown on the callback
                logger.warning("on_app_shutdown callback raised", exc_info=True)

    def _on_signal_sync(self, signum: int, frame: Any) -> None:  # pragma: no cover
        logger.info("Received shutdown signal %d", signum)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._trigger_shutdown)
        if self.on_app_shutdown is not None:
            try:
                self.on_app_shutdown()
            except Exception:  # pragma: no cover
                logger.warning("on_app_shutdown callback raised", exc_info=True)

    def request_shutdown(self) -> None:
        """Request a graceful shutdown from any context — e.g. a failing
        health check or a management command (H11). Equivalent to receiving
        SIGINT/SIGTERM: if ``run()`` is awaiting shutdown it performs the
        drain; otherwise this schedules a fire-and-forget ``stop()``.
        """
        self._trigger_shutdown()

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
        # Deadline elapsed with handlers still running: cancel + nack them
        # explicitly (delivery-tag logged) instead of silently abandoning
        # them unacked -- matches AsyncWorkerPool.stop()'s behavior for the
        # pooled path. Outside the `async with cond:` block since we're no
        # longer touching `_in_flight`/the condition itself here, and
        # cancelling a task can re-enter this broker (e.g. its `finally`
        # decrementing `_in_flight`), which would deadlock re-acquiring cond.
        if self._in_flight > 0:
            logger.warning(
                "AsyncBroker.stop: %d in-flight handler(s) still running after "
                "graceful drain deadline; disconnecting anyway",
                self._in_flight,
            )
            tasks = dict(self._inflight_tasks)
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            for message in tasks.values():
                if message.is_settled:
                    continue  # handler reached its own ack/nack before being cut off
                logger.warning(
                    "AsyncBroker.stop: handler for delivery_tag=%s message_id=%s did not "
                    "complete within graceful_timeout; abandoning (task cancelled) and "
                    "nacking for redelivery — ensure handlers are idempotent under "
                    "at-least-once delivery",
                    message.delivery_tag,
                    message.message_id,
                )
                try:
                    await message.nack_async(requeue=True)
                except Exception:
                    logger.warning("nack on abandoned handler's message raised", exc_info=True)

    async def stop(self, timeout: float | None = None) -> None:
        """Stop the broker - cancel consumers, drain pool, drain in-flight, disconnect.

        ``timeout`` defaults to ``ConsumerConfig.graceful_timeout`` (C-2). The
        whole sequence is bounded by an overall deadline.

        C5: consumers are cancelled FIRST, before the worker pool is drained.
        Draining the pool before cancelling left the consumer active for the
        entire (potentially graceful_timeout-long) drain wait — a message
        delivered in that window was submitted via ``AsyncWorkerPool.submit()``,
        which creates a task unconditionally (it never checks ``_running``) and
        adds it to ``_tasks``. If that submit happens after ``.stop()`` already
        cleared ``_tasks``, the new task is never awaited by anything — an
        orphaned background task racing the event loop's shutdown, with the
        message never cleanly settled before ``disconnect()``, so it is
        redelivered (duplicate-processing risk) on the next connection.
        Cancelling first stops new deliveries outright, so the pool only ever
        drains work that was already in flight.
        """
        if not self._started:
            return

        self._restore_signal_handlers()

        effective = timeout if timeout is not None else self._consumer_config.graceful_timeout
        deadline = None if effective is None else time.monotonic() + effective

        # L14: stop the periodic heartbeat first, alongside cancelling consumers.
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

        # Cancel all consumers FIRST — stop new deliveries before draining
        # anything, so nothing new arrives while the pool/in-flight drain runs.
        if self._transport is None:  # pragma: no cover — defensive (assert was stripped under -O)
            return
        for route in self._registry.routes:
            if route.consumer_tag:
                await self._transport.cancel_consumer(route.consumer_tag)

        # Drain the worker pool (let in-flight pooled tasks finish), bounded by
        # the outer deadline.
        if self._worker_pool is not None:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            await self._worker_pool.stop(timeout=remaining)
            self._worker_pool = None

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

        When ``middlewares=[...]`` was passed to the constructor, each
        middleware's ``publish_scope_async`` wraps this call (e.g.
        ``SigningMiddleware`` signs the envelope) — see ``publish_middlewares``.
        Middleware wraps OUTSIDE both flow control and batching, so a
        middleware-transformed envelope is what gets rate-limited/batched, and
        what actually reaches the wire.
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

        # M10: reject oversized bodies at publish time (see sync broker).
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

        publish_fn = (
            self._batch_publisher.publish
            if self._batch_publisher is not None
            else self._transport.publish
        )

        async def do_publish(env: MessageEnvelope) -> PublishOutcome:
            fc = self._flow_controller
            if fc is not None:
                if not await fc.acquire_async():
                    return PublishOutcome(
                        status=PublishStatus.ERROR,
                        exchange=env.exchange,
                        routing_key=env.routing_key,
                        error=BackpressureError("publish dropped by backpressure policy"),
                    )
                try:
                    return await publish_fn(env)  # type: ignore[no-any-return]
                finally:
                    await fc.release_async()
            return await publish_fn(env)  # type: ignore[no-any-return]

        if self._publish_middlewares:
            chain = self._pipeline.compose_broker_publish_async(self._publish_middlewares)
            outcome: PublishOutcome = await chain(envelope, do_publish)
            return outcome
        return await do_publish(envelope)

    async def _flow_controlled_internal_publish(self, env: MessageEnvelope) -> PublishOutcome:
        """M18: async mirror of ``SyncBroker._flow_controlled_internal_publish``
        — see its docstring. Used as the ``publish_fn`` for ``RetryMiddleware``'s
        delay-queue republish and the pipeline's result/RPC-reply publish so
        the broker's ``FlowController`` (if configured) applies to these
        internal publishes too. Never lets ``BackpressureError`` escape as an
        exception — a blocked/dropped slot always resolves as a failed
        ``PublishOutcome`` so existing nack+requeue handling applies
        regardless of the configured ``on_blocked`` policy.
        """
        if self._transport is None:  # pragma: no cover — defensive; callers only run while consuming
            raise BrokerNotStartedError("Broker not started. Call start() first.")
        transport = self._transport
        fc = self._flow_controller
        if fc is None:
            return await transport.publish(env)  # type: ignore[no-any-return]
        try:
            acquired = await fc.acquire_async()
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
            return await transport.publish(env)  # type: ignore[no-any-return]
        finally:
            await fc.release_async()

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
            raise BrokerNotStartedError("Broker not started. Call start() first.")
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
                # User-constructed RetryMiddleware: inject the broker's
                # confirmed publish fn if none was passed (see sync broker).
                for mw in route.route_middlewares:
                    if isinstance(mw, RetryMiddleware):
                        mw.ensure_publish_fns(publish_async_fn=self._flow_controlled_internal_publish)
                continue
            # (No confirms warning for the RETRY context: retry envelopes are
            # published mandatory=True, which forces per-publish confirm mode
            # on both transports even when confirm_delivery=False — the
            # ack-after-confirmed-outcome invariant holds regardless.)
            index = retry_middleware_insertion_index(route.route_middlewares)
            # M2: if a MetricsMiddleware is already on this route, wire it into
            # RetryMiddleware too so messages_retried_total/dead_lettered_total
            # are observable (RetryMiddleware settles messages the pipeline
            # itself never sees settle, so it must record these itself).
            metrics_mw = next(
                (mw for mw in route.route_middlewares if isinstance(mw, MetricsMiddleware)), None
            )
            route.route_middlewares.insert(
                index,
                RetryMiddleware(
                    retry_config,
                    publish_async_fn=self._flow_controlled_internal_publish,  # M18
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
        """Async mirror of ``SyncBroker._wire_reconnect_metric`` — count
        transport reconnects (connection churn) via the first route
        ``MetricsMiddleware``'s collector, if any. No-op without metrics."""
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
            await self._transport.declare_queue(source_queue)

            # Bind queue to exchange (C4: bind_arguments matter for headers exchanges)
            if route.exchange is not None:
                await self._transport.bind_queue(
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
                    await self._transport.declare_queue(delay_queue)
            elif safety_dlq_name is not None:
                await self._transport.declare_queue(
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

    async def _start_consumer(self, route: RouteDefinition) -> None:
        """Start consuming for a single route."""
        if self._transport is None:
            return

        pool = self._worker_pool

        async def on_message(message: RabbitMessage) -> None:
            """Process incoming message through the pipeline."""
            # Track inline in-flight so stop() can drain gracefully (C-2).
            # Also register this task/message pair so a drain-deadline timeout
            # can cancel + nack it explicitly (with delivery-tag logging),
            # matching AsyncWorkerPool.stop()'s behavior for the pooled path,
            # instead of silently abandoning it unacked.
            await self._in_flight_inc()
            task = asyncio.current_task()
            if task is not None:
                self._inflight_tasks[task] = message
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

                await self._pipeline.process_async(
                    route,
                    message,
                    publish_fn=self._flow_controlled_internal_publish,  # M18
                )
            finally:
                if task is not None:
                    self._inflight_tasks.pop(task, None)
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
