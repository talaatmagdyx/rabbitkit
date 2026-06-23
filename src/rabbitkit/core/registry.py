"""Subscriber/Publisher registry — stores all @subscriber/@publisher registrations.

Semantic rules:
- One handler per queue (duplicate → DuplicateRouteError at registration time)
- str queue → auto-creates RabbitQueue(name=str, durable=True)
- str exchange → auto-creates RabbitExchange(name=str, type=DIRECT)
- @publisher without @subscriber → raises ConfigurationError
- @publisher BEFORE @subscriber on same handler → sets result_publisher

Registration-time retry conflict checks (fail fast):
When a route is registered, the registry resolves the effective retry policy
and validates retry + ack policy + DLX config compatibility.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from rabbitkit.core.config import RetryConfig, RetryDisabled
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.route import ResultPublisher, RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy


class DuplicateRouteError(Exception):
    """Raised when two routes target the same queue."""


class SubscriberRegistry:
    """Stores all @subscriber/@publisher registrations.

    Used by RabbitApp and RabbitRouter to collect route definitions
    which are later wired by the broker.
    """

    def __init__(self, broker_retry: RetryConfig | None = None) -> None:
        self._routes: list[RouteDefinition] = []
        self._queue_names: set[str] = set()
        self._pending_publishers: dict[int, ResultPublisher] = {}  # handler id → ResultPublisher
        self._broker_retry = broker_retry

    @property
    def routes(self) -> list[RouteDefinition]:
        """Return all registered routes."""
        return list(self._routes)

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
        filter_fn: Callable[[RabbitMessage], bool] | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator to register a message handler for a queue.

        Args:
            queue: Queue to consume from (str auto-creates RabbitQueue).
            exchange: Exchange to bind to (str auto-creates RabbitExchange).
            routing_key: Routing key for binding.
            ack_policy: Acknowledgment policy for this route.
            middlewares: Route-specific middleware list.
            serializer: Route-specific serializer override.
            retry: Per-route retry config (None=inherit, RETRY_DISABLED=opt-out).
            tags: Route tags for filtering/grouping.
            description: Human-readable route description.
            name: Explicit route name (auto-generated if None).
            prefetch_count: Per-route prefetch override (None=use global).
        """
        # Normalize queue
        if isinstance(queue, str):
            queue = RabbitQueue(name=queue)

        # Normalize exchange
        if isinstance(exchange, str):
            exchange = RabbitExchange(name=exchange)

        # Normalize tags
        if tags is not None and not isinstance(tags, frozenset):
            tags = frozenset(tags)
        elif tags is None:
            tags = frozenset()

        # Apply routing_key to queue if not already set
        if routing_key and not queue.routing_key:
            queue.routing_key = routing_key

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            # Check duplicate queue
            if queue.name in self._queue_names:
                raise DuplicateRouteError(
                    f"Queue '{queue.name}' already has a registered handler. "
                    "rabbitkit enforces one handler per queue. "
                    "Use multiple routing keys on the same queue for fan-in."
                )

            # Auto-generate name
            route_name = name or f"{queue.name}:{func.__qualname__}"

            # Check for pending @publisher
            result_publisher = self._pending_publishers.pop(id(func), None)

            # Create route
            route = RouteDefinition(
                name=route_name,
                queue=queue,
                exchange=exchange,
                handler=func,
                ack_policy=ack_policy,
                route_middlewares=middlewares or [],
                result_publisher=result_publisher,
                serializer_override=serializer,
                retry_override=retry,
                prefetch_count=prefetch_count,
                tags=tags,
                description=description,
                filter_fn=filter_fn,
            )

            # Validate at registration time (fail fast)
            route.validate(self._broker_retry)

            self._routes.append(route)
            self._queue_names.add(queue.name)

            return func

        return decorator

    def publisher(
        self,
        exchange: RabbitExchange | str | None = None,
        routing_key: str = "",
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator to configure result publishing for a handler.

        Must be applied BEFORE @subscriber on the same handler.
        If applied without @subscriber, the publisher info is stored
        and applied when @subscriber is later applied.

        Args:
            exchange: Target exchange for result publishing.
            routing_key: Routing key for result publishing.
        """
        # Normalize exchange
        if isinstance(exchange, str):
            exchange = RabbitExchange(name=exchange)

        result_pub = ResultPublisher(exchange=exchange, routing_key=routing_key)

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            # Store pending publisher for this handler
            self._pending_publishers[id(func)] = result_pub
            return func

        return decorator

    def include_router(self, router: Any, prefix: str = "") -> None:
        """Include routes from a RabbitRouter.

        Applies the router's prefix to routing keys and merges
        router-level defaults (exchange, middlewares, serializer, tags).
        """
        if not hasattr(router, "_registry"):
            raise TypeError(f"Expected a RabbitRouter, got {type(router).__name__}")

        for route in router._registry.routes:
            # Apply prefix to routing key
            if prefix:
                effective_rk = f"{prefix}.{route.queue.routing_key}" if route.queue.routing_key else prefix
                route.queue.routing_key = effective_rk

            # Check duplicate
            if route.queue.name in self._queue_names:
                raise DuplicateRouteError(
                    f"Queue '{route.queue.name}' already has a registered handler. "
                    "Duplicate from included router."
                )

            # Validate with broker retry context
            route.validate(self._broker_retry)

            self._routes.append(route)
            self._queue_names.add(route.queue.name)

    def set_broker_retry(self, retry: RetryConfig | None) -> None:
        """Update broker retry default. Re-validates all existing routes."""
        self._broker_retry = retry
        for route in self._routes:
            route.validate(self._broker_retry)
