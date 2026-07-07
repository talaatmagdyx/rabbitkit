"""TestBroker — in-memory broker for unit testing.

No RabbitMQ required. Routes messages between subscribers using
exchange type matching. Captures published messages for assertions.

Implements the Transport protocol so it can be used anywhere a
transport is expected.

Settlement is *real*: ``ack``/``nack``/``reject`` go through the actual
``RabbitMessage`` methods, so ``msg.is_settled`` and ``msg._disposition``
reflect what happened (no no-op mocks). The transport-level settlement
functions record their invocations (including the ``requeue`` argument)
so :meth:`TestBroker.assert_acked` / :meth:`assert_nacked` /
:meth:`assert_rejected` can assert on both disposition and requeue.

Publish outcomes are injectable via :attr:`publish_outcome` (persistent
override) or :meth:`fail_next_publish` (one-shot), so the pipeline's
failed-publish → ``nack(requeue=True)`` branch is reachable in tests.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from rabbitkit.core.config import RetryConfig, RetryDisabled
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.path import extract_path
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.registry import SubscriberRegistry
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.serialization.base import Serializer

if TYPE_CHECKING:
    from rabbitkit.core.router import RabbitRouter
from rabbitkit.core.types import (
    AckPolicy,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SettlementRecord:
    """A single transport-level settlement call recorded by TestBroker.

    ``requeue`` is ``None`` for ``ack`` (which has no requeue argument).
    """

    kind: str  # "ack" | "nack" | "reject"
    requeue: bool | None


class TestBroker:
    """In-memory broker — no RabbitMQ needed.

    Features:
    - Routes messages between subscribers using exchange type matching
    - .mock attribute on every handler for assertions
    - Captures published messages for assertion
    - *Real* settlement: ack/nack/reject update ``msg._disposition`` and are
      recorded (including ``requeue``) for :meth:`assert_acked` /
      :meth:`assert_nacked` / :meth:`assert_rejected`
    - Injectable publish outcome (``publish_outcome`` / ``fail_next_publish()``)
      so the failed-publish → nack(requeue=True) branch is reachable
    - Implements basic Transport-like interface

    Usage:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle_order(body: bytes) -> None:
            ...

        broker.start()
        broker.publish("orders", b'{"id": 1}')

        handle_order.mock.assert_called_once()
        broker.assert_acked(broker.consumed_messages[0])
    """

    __test__ = False  # Prevent pytest from collecting as test class

    def __init__(
        self,
        *,
        serializer: Serializer[Any] | None = None,
        di_resolver: Any | None = None,
        context_repo: Any | None = None,
        publish_outcome: PublishOutcome | None = None,
    ) -> None:
        self._registry = SubscriberRegistry()
        self._pipeline = HandlerPipeline(
            serializer=serializer,
            di_resolver=di_resolver,
            context_repo=context_repo,
        )
        self._published: list[MessageEnvelope] = []
        self._consumed: list[RabbitMessage] = []
        self._exchanges: dict[str, RabbitExchange] = {}
        self._queues: dict[str, RabbitQueue] = {}
        self._bindings: list[tuple[str, str, str]] = []  # (queue, exchange, routing_key)
        self._started = False
        # Per-message settlement log, keyed by id(message). Messages are retained
        # in ``_consumed`` for the test's lifetime, so ids stay stable.
        self._settlements: dict[int, list[SettlementRecord]] = {}
        # Injectable publish outcome. When set, every publish returns it. When
        # None, publishes return CONFIRMED (unless ``_fail_next`` is set).
        self._publish_outcome: PublishOutcome | None = publish_outcome
        # One-shot: the next publish returns a NACKED outcome, then clears.
        self._fail_next: bool = False

    # ── Registration (mirrors real broker API) ────────────────────────────

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
        """Register a subscriber — same API as real broker."""
        decorator = self._registry.subscriber(
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

        def wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
            # Apply the subscriber decorator
            result = decorator(func)
            # Attach a mock for assertions
            if not hasattr(result, "mock"):
                result.mock = MagicMock()  # type: ignore[attr-defined]
            return result

        return wrapper

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

    def start(self) -> None:
        """Start the test broker. Records topology declarations."""
        for route in self._registry.routes:
            # Record exchange
            if route.exchange is not None:
                self._exchanges[route.exchange.name] = route.exchange

            # Record queue
            self._queues[route.queue.name] = route.queue

            # Record binding
            exchange_name = route.exchange.name if route.exchange else ""
            self._bindings.append((route.queue.name, exchange_name, route.queue.routing_key))

            # Attach mock to handler
            if not hasattr(route.handler, "mock"):
                route.handler.mock = MagicMock()  # type: ignore[attr-defined]

        # Mirror the real broker: install RetryMiddleware on retry-enabled routes
        # so retry=RetryConfig(...) actually routes failures to the delay queues.
        # Retry publishes are captured in ``_published`` (routing_key
        # ``<queue>.retry.<n>``) so tests can assert on them.
        self._wire_retry_middleware()

        self._started = True

    def _wire_retry_middleware(self) -> None:
        """Install ``RetryMiddleware`` on retry-enabled routes (mirrors the real broker).

        Idempotent — see ``SyncBroker._wire_retry_middleware`` for the insertion
        position rationale (outer of ordinary middlewares, inner of any
        ``ExceptionMiddleware``).
        """
        from rabbitkit.middleware.metrics import MetricsMiddleware
        from rabbitkit.middleware.retry import (
            RetryMiddleware,
            retry_middleware_insertion_index,
            warn_retry_middleware_without_topology,
        )

        for route in self._registry.routes:
            retry_config = route.effective_retry_config()
            has_retry_mw = any(isinstance(mw, RetryMiddleware) for mw in route.route_middlewares)
            if retry_config is None:
                if has_retry_mw:
                    warn_retry_middleware_without_topology(route.name)
                continue
            if has_retry_mw:
                continue
            index = retry_middleware_insertion_index(route.route_middlewares)
            # M2: mirror the real brokers -- wire in a route MetricsMiddleware
            # (if any) so messages_retried_total/dead_lettered_total are
            # observable through TestBroker too.
            metrics_mw = next(
                (mw for mw in route.route_middlewares if isinstance(mw, MetricsMiddleware)), None
            )
            route.route_middlewares.insert(
                index,
                RetryMiddleware(
                    retry_config,
                    publish_fn=self._retry_publish_sync,
                    publish_async_fn=self._retry_publish_async,
                    metrics_collector=metrics_mw.collector if metrics_mw else None,
                    metrics_config=metrics_mw.config if metrics_mw else None,
                ),
            )
        self._pipeline.clear_caches()

    def _retry_publish_sync(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Capture a retry (delay-queue) publish; honors injected outcomes."""
        self._published.append(envelope)
        return self._next_publish_outcome(envelope)

    async def _retry_publish_async(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Async variant of :meth:`_retry_publish_sync`."""
        self._published.append(envelope)
        return self._next_publish_outcome(envelope)

    def stop(self) -> None:
        """Stop the test broker."""
        self._started = False

    def reset(self) -> None:
        """Reset all captured state (published messages, settlements, mocks)."""
        self._published.clear()
        self._consumed.clear()
        self._settlements.clear()
        # One-shot failure state is reset too, so a fresh publish succeeds.
        self._fail_next = False

        for route in self._registry.routes:
            if hasattr(route.handler, "mock"):
                route.handler.mock.reset_mock()

    # ── Publish outcome injection ─────────────────────────────────────────

    @property
    def publish_outcome(self) -> PublishOutcome | None:
        """The persistent publish outcome override (None = CONFIRMED)."""
        return self._publish_outcome

    @publish_outcome.setter
    def publish_outcome(self, outcome: PublishOutcome | None) -> None:
        self._publish_outcome = outcome

    def fail_next_publish(self) -> None:
        """Make the next handler-result publish return a NACKED outcome.

        One-shot: only the next publish is affected; subsequent publishes
        return to CONFIRMED (or the persistent ``publish_outcome``).
        """
        self._fail_next = True

    def _next_publish_outcome(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Compute the outcome for a publish, honoring injection state."""
        if self._fail_next:
            self._fail_next = False
            return PublishOutcome(
                status=PublishStatus.NACKED,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
            )
        if self._publish_outcome is not None:
            return self._publish_outcome
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    # ── Settlement wiring (real — records transport-level calls) ──────────

    def _wire_settlement(self, message: RabbitMessage) -> None:
        """Attach *real* settlement functions to ``message``.

        These are the transport-level stubs that ``RabbitMessage.ack()`` /
        ``nack()`` / ``reject()`` (and the async variants) call internally.
        They record the call (including ``requeue``) so the assert helpers
        can verify both disposition and requeue. They succeed by default —
        for ack-failure propagation tests, build a ``RabbitMessage`` with a
        raising ``_ack_fn`` directly (see ``tests/unit/core/test_message.py``).
        """
        records: list[SettlementRecord] = []
        self._settlements[id(message)] = records

        def ack_fn() -> None:
            records.append(SettlementRecord(kind="ack", requeue=None))

        def nack_fn(requeue: bool = True) -> None:
            records.append(SettlementRecord(kind="nack", requeue=requeue))

        def reject_fn(requeue: bool = False) -> None:
            records.append(SettlementRecord(kind="reject", requeue=requeue))

        async def async_ack() -> None:
            records.append(SettlementRecord(kind="ack", requeue=None))

        async def async_nack(requeue: bool = True) -> None:
            records.append(SettlementRecord(kind="nack", requeue=requeue))

        async def async_reject(requeue: bool = False) -> None:
            records.append(SettlementRecord(kind="reject", requeue=requeue))

        message._ack_fn = ack_fn
        message._nack_fn = nack_fn
        message._reject_fn = reject_fn
        message._ack_async_fn = async_ack
        message._nack_async_fn = async_nack
        message._reject_async_fn = async_reject

    # ── Publish (test helper) ─────────────────────────────────────────────

    def publish(
        self,
        queue: str,
        body: bytes,
        *,
        headers: dict[str, Any] | None = None,
        routing_key: str = "",
        exchange: str = "",
        message_id: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> None:
        """Publish a message to a queue for processing.

        Finds the matching route and processes the message through the pipeline.
        This is the primary test helper — call this to trigger handler execution.

        Args:
            queue: Target queue name (must match a registered subscriber).
            body: Raw message body.
            headers: Message headers.
            routing_key: Routing key (defaults to "").
            exchange: Exchange name (defaults to "").
            message_id: Message ID.
            correlation_id: Correlation ID.
            reply_to: Reply-to queue.
            content_type: Content type.
            content_encoding: Content encoding.
        """
        route = self._find_route_by_queue(queue)
        if route is None:
            raise ValueError(f"No subscriber registered for queue '{queue}'")

        # Build RabbitMessage
        message = RabbitMessage(
            body=body,
            headers=headers or {},
            routing_key=routing_key or route.queue.routing_key,
            exchange=exchange or (route.exchange.name if route.exchange else ""),
            message_id=message_id,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type=content_type,
            content_encoding=content_encoding,
        )

        # Wire real settlement (records ack/nack/reject, updates disposition)
        self._wire_settlement(message)

        # Mirror the real broker: stamp the source queue for retry routing.
        # H2: ALWAYS overwrite (never trust a producer-set value) — see the
        # real brokers for the spoofing rationale; TestBroker mirrors it so
        # tests exercise the same semantics.
        message.headers["x-rabbitkit-original-queue"] = route.queue.name

        message.path = extract_path(message.routing_key, route.queue.routing_key)
        self._consumed.append(message)

        # Process through pipeline — publish outcome is injectable so the
        # failed-publish → nack(requeue=True) branch is reachable.
        def test_publish_fn(envelope: MessageEnvelope) -> PublishOutcome:
            self._published.append(envelope)
            return self._next_publish_outcome(envelope)

        self._pipeline.process_sync(route, message, publish_fn=test_publish_fn)

        # Record mock call
        if hasattr(route.handler, "mock"):
            route.handler.mock(body)

    async def publish_async(
        self,
        queue: str,
        body: bytes,
        *,
        headers: dict[str, Any] | None = None,
        routing_key: str = "",
        exchange: str = "",
        message_id: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> None:
        """Async variant of publish."""
        route = self._find_route_by_queue(queue)
        if route is None:
            raise ValueError(f"No subscriber registered for queue '{queue}'")

        message = RabbitMessage(
            body=body,
            headers=headers or {},
            routing_key=routing_key or route.queue.routing_key,
            exchange=exchange or (route.exchange.name if route.exchange else ""),
            message_id=message_id,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type=content_type,
            content_encoding=content_encoding,
        )

        # Wire real settlement (async + sync variants)
        self._wire_settlement(message)

        # Mirror the real broker: stamp the source queue for retry routing.
        # H2: ALWAYS overwrite (never trust a producer-set value) — see the
        # real brokers for the spoofing rationale; TestBroker mirrors it so
        # tests exercise the same semantics.
        message.headers["x-rabbitkit-original-queue"] = route.queue.name

        message.path = extract_path(message.routing_key, route.queue.routing_key)
        self._consumed.append(message)

        async def test_publish_fn(envelope: MessageEnvelope) -> PublishOutcome:
            self._published.append(envelope)
            return self._next_publish_outcome(envelope)

        await self._pipeline.process_async(route, message, publish_fn=test_publish_fn)

        if hasattr(route.handler, "mock"):
            route.handler.mock(body)

    # ── Assertions ────────────────────────────────────────────────────────

    @property
    def published_messages(self) -> list[MessageEnvelope]:
        """Return all messages published by handlers (result publishing)."""
        return list(self._published)

    @property
    def consumed_messages(self) -> list[RabbitMessage]:
        """Return all messages consumed during tests."""
        return list(self._consumed)

    @property
    def routes(self) -> list[RouteDefinition]:
        """Return all registered routes."""
        return self._registry.routes

    @property
    def declared_exchanges(self) -> dict[str, RabbitExchange]:
        """Return all declared exchanges."""
        return dict(self._exchanges)

    @property
    def declared_queues(self) -> dict[str, RabbitQueue]:
        """Return all declared queues."""
        return dict(self._queues)

    def settlements_for(self, message: RabbitMessage) -> list[SettlementRecord]:
        """Return the recorded transport-level settlements for ``message``."""
        return list(self._settlements.get(id(message), []))

    def assert_acked(self, message: RabbitMessage) -> None:
        """Assert ``message`` was acked (disposition + transport record)."""
        assert message._disposition == "acked", f"expected message acked, got disposition={message._disposition!r}"
        records = self._settlements.get(id(message), [])
        assert any(r.kind == "ack" for r in records), "no transport ack recorded"

    def assert_nacked(self, message: RabbitMessage, *, requeue: bool = True) -> None:
        """Assert ``message`` was nacked with the given ``requeue`` flag."""
        assert message._disposition == "nacked", f"expected message nacked, got disposition={message._disposition!r}"
        nack_records = [r for r in self._settlements.get(id(message), []) if r.kind == "nack"]
        assert nack_records, "no transport nack recorded"
        last = nack_records[-1]
        assert last.requeue == requeue, f"expected nack requeue={requeue}, got {last.requeue}"

    def assert_rejected(self, message: RabbitMessage, *, requeue: bool = False) -> None:
        """Assert ``message`` was rejected with the given ``requeue`` flag."""
        assert message._disposition == "rejected", (
            f"expected message rejected, got disposition={message._disposition!r}"
        )
        reject_records = [r for r in self._settlements.get(id(message), []) if r.kind == "reject"]
        assert reject_records, "no transport reject recorded"
        last = reject_records[-1]
        assert last.requeue == requeue, f"expected reject requeue={requeue}, got {last.requeue}"

    # ── Internal ──────────────────────────────────────────────────────────

    def _find_route_by_queue(self, queue_name: str) -> RouteDefinition | None:
        """Find the route registered for a given queue name."""
        for route in self._registry.routes:
            if route.queue.name == queue_name:
                return route
        return None


class TestAsyncBroker(TestBroker):
    """Async-flavored alias for :class:`TestBroker`.

    ``TestBroker`` already supports both sync (``publish``) and async
    (``publish_async``) flows; this subclass exists so tests that want an
    explicitly async-named fixture read naturally and share the same
    injectable-publish-outcome / real-settlement behavior.
    """

    __test__ = False
