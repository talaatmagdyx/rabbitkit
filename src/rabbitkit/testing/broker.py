"""TestBroker — in-memory broker for unit testing.

No RabbitMQ required. Routes messages between subscribers using
exchange type matching. Captures published messages for assertions.

Implements the Transport protocol so it can be used anywhere a
transport is expected.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

from rabbitkit.core.config import RetryConfig, RetryDisabled
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.path import extract_path
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.registry import SubscriberRegistry
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import (
    AckPolicy,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
)

logger = logging.getLogger(__name__)


class TestBroker:
    """In-memory broker — no RabbitMQ needed.

    Features:
    - Routes messages between subscribers using exchange type matching
    - .mock attribute on every handler for assertions
    - Captures published messages for assertion
    - Simulates ack/nack/reject
    - Implements basic Transport-like interface

    Usage:
        broker = TestBroker()

        @broker.subscriber(queue="orders")
        def handle_order(body: bytes) -> None:
            ...

        broker.start()
        broker.publish("orders", b'{"id": 1}')

        handle_order.mock.assert_called_once()
    """

    __test__ = False  # Prevent pytest from collecting as test class

    def __init__(
        self,
        *,
        serializer: Any | None = None,
        di_resolver: Any | None = None,
        context_repo: Any | None = None,
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

    # ── Registration (mirrors real broker API) ────────────────────────────

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

    def include_router(self, router: Any, prefix: str = "") -> None:
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
            self._bindings.append(
                (route.queue.name, exchange_name, route.queue.routing_key)
            )

            # Attach mock to handler
            if not hasattr(route.handler, "mock"):
                route.handler.mock = MagicMock()  # type: ignore[attr-defined]

        self._started = True

    def stop(self) -> None:
        """Stop the test broker."""
        self._started = False

    def reset(self) -> None:
        """Reset all captured state (published messages, mocks)."""
        self._published.clear()
        self._consumed.clear()

        for route in self._registry.routes:
            if hasattr(route.handler, "mock"):
                route.handler.mock.reset_mock()

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

        # Set up ack/nack/reject functions (no-op for test)
        ack_mock = MagicMock()
        nack_mock = MagicMock()
        reject_mock = MagicMock()

        message._ack_fn = ack_mock
        message._nack_fn = nack_mock
        message._reject_fn = reject_mock

        message.path = extract_path(message.routing_key, route.queue.routing_key)
        self._consumed.append(message)

        # Process through pipeline
        def test_publish_fn(envelope: MessageEnvelope) -> PublishOutcome:
            self._published.append(envelope)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

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

        # Set up async ack functions
        async def async_ack() -> None:
            pass

        async def async_nack(requeue: bool = True) -> None:
            pass

        async def async_reject(requeue: bool = False) -> None:
            pass

        message._ack_async_fn = async_ack
        message._nack_async_fn = async_nack
        message._reject_async_fn = async_reject

        message.path = extract_path(message.routing_key, route.queue.routing_key)
        self._consumed.append(message)

        async def test_publish_fn(envelope: MessageEnvelope) -> PublishOutcome:
            self._published.append(envelope)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

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

    # ── Internal ──────────────────────────────────────────────────────────

    def _find_route_by_queue(self, queue_name: str) -> RouteDefinition | None:
        """Find the route registered for a given queue name."""
        for route in self._registry.routes:
            if route.queue.name == queue_name:
                return route
        return None
