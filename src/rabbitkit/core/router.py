"""Modular router with prefix support.

Features:
- routing_key prefix (prepended to routing keys, NOT queue names)
- Default exchange (used by all routes unless overridden)
- Middleware stack (applied to all routes in this router)
- Serializer override (applied to all routes in this router)
- Tags (applied to all routes in this router)
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from rabbitkit.core.config import RetryConfig, RetryDisabled
from rabbitkit.core.registry import SubscriberRegistry
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy
from rabbitkit.serialization.base import Serializer

if TYPE_CHECKING:
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.middleware.base import BaseMiddleware


class RabbitRouter:
    """Modular router — groups related routes with shared defaults.

    Use routers to organize handlers by domain:
    ```python
    orders_router = RabbitRouter(prefix="orders", exchange="orders-exchange")

    @orders_router.subscriber(queue="orders-queue", routing_key="created")
    def handle_order(order: Order) -> None:
        ...
    ```

    Include in broker/app:
    ```python
    broker.include_router(orders_router)
    ```
    """

    def __init__(
        self,
        prefix: str = "",
        exchange: RabbitExchange | str | None = None,
        middlewares: list[BaseMiddleware] | None = None,
        serializer: Serializer[Any] | None = None,
        tags: frozenset[str] | set[str] | None = None,
    ) -> None:
        self._prefix = prefix
        self._default_exchange = self._normalize_exchange(exchange)
        self._middlewares = middlewares or []
        self._serializer = serializer
        self._tags = frozenset(tags) if tags else frozenset()
        self._registry = SubscriberRegistry()

    @property
    def prefix(self) -> str:
        return self._prefix

    @property
    def routes(self) -> list[RouteDefinition]:
        return self._registry.routes

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
        """Register a subscriber on this router.

        Applies router-level defaults:
        - prefix is prepended to routing_key
        - exchange falls back to router's default exchange
        - middlewares are merged (router-level + route-level)
        - serializer falls back to router's serializer
        - tags are merged (router-level + route-level)
        """
        # Apply prefix to routing key
        if self._prefix and routing_key:
            effective_rk = f"{self._prefix}.{routing_key}"
        else:
            effective_rk = routing_key or self._prefix

        # Fall back to router's default exchange
        effective_exchange = exchange if exchange is not None else self._default_exchange

        # Merge middlewares (router-level first, then route-level)
        route_mw = middlewares or []
        effective_mw = self._middlewares + route_mw

        # Fall back to router's serializer
        effective_serializer = serializer if serializer is not None else self._serializer

        # Merge tags
        route_tags = frozenset(tags) if tags else frozenset()
        effective_tags = self._tags | route_tags

        return self._registry.subscriber(
            queue=queue,
            exchange=effective_exchange,
            routing_key=effective_rk,
            ack_policy=ack_policy,
            middlewares=effective_mw,
            serializer=effective_serializer,
            retry=retry,
            tags=effective_tags,
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
        """Register a result publisher on this router."""
        return self._registry.publisher(exchange=exchange, routing_key=routing_key)

    def _normalize_exchange(self, exchange: RabbitExchange | str | None) -> RabbitExchange | None:
        """Normalize exchange to RabbitExchange or None."""
        if isinstance(exchange, str):
            return RabbitExchange(name=exchange)
        return exchange
