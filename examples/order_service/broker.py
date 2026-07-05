"""Broker assembly (docs §6/§15).

``build_broker`` constructs the AsyncBroker, the Pydantic serializer, the DI
resolver, and the middleware pipeline in the correct order. It is a FUNCTION, not
import-time work, so importing this module never needs a live broker/Redis.

The retry middleware is wired with ``publish_async_fn=broker.publish`` — this is
what makes retries actually publish AND what makes the "failed publish → nack,
not ack" safety net work (it needs a real PublishOutcome). See docs §0.

The pipeline composes ``route.route_middlewares`` outer→inner around the handler,
so the middlewares below run on every message (verified in
tests/examples/order_service/test_retry.py).
"""

from __future__ import annotations

from typing import Any

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import RabbitConfig
from rabbitkit.di.resolver import DIResolver
from rabbitkit.middleware.exception import ExceptionMiddleware
from rabbitkit.middleware.otel import OTelTracingMiddleware
from rabbitkit.middleware.retry import RetryMiddleware
from rabbitkit.middleware.timeout import TimeoutConfig, TimeoutMiddleware
from rabbitkit.serialization.pipeline import JsonParser, PydanticDecoder, SerializationPipeline

from .config import ORDERS_RETRY
from .handlers import register_order_handlers


def build_broker(config: RabbitConfig) -> AsyncBroker:
    """Build a fully-wired AsyncBroker with the order handler registered."""
    broker = AsyncBroker(
        config,
        serializer=SerializationPipeline(JsonParser(), PydanticDecoder()),
        di_resolver=DIResolver(),
    )

    # Middleware order is OUTER → INNER (docs §15). Tracing outermost so the span
    # covers retries; timeout innermost so a timeout counts as one retryable attempt.
    middlewares: list[Any] = [
        OTelTracingMiddleware(service_name="order-service"),
        ExceptionMiddleware(swallow_permanent=False),  # let terminal errors reach the pipeline → DLQ
        RetryMiddleware(ORDERS_RETRY, publish_async_fn=broker.publish),  # ← must be wired
        TimeoutMiddleware(TimeoutConfig(timeout_seconds=15.0)),
    ]

    register_order_handlers(broker, middlewares=middlewares)
    return broker
