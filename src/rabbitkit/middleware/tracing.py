"""TracedConsumerMiddleware — obskit tracing integration.

Lazy/no-op: if obskit is not installed, all operations are no-ops.
Availability is checked at ``__init__`` time, NOT at module level.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)


def _get_obskit_tracing() -> Any:
    """Lazy import of obskit.tracing — returns module or ``None``."""
    try:
        from obskit import tracing

        return tracing
    except ImportError:
        return None


class TracedConsumerMiddleware(BaseMiddleware):
    """Wraps handler execution and publish in obskit trace spans.

    If obskit is not installed or tracing is not configured,
    all methods are passthrough no-ops.

    Usage::

        mw = TracedConsumerMiddleware(service_name="order-service")
        broker = SyncBroker(config)
        # Add as outermost middleware so the span wraps retry, dedup, etc.
    """

    def __init__(self, service_name: str = "rabbitkit") -> None:
        self._service_name = service_name
        self._tracing = _get_obskit_tracing()
        self._available = (
            self._tracing is not None and self._tracing.is_tracing_available()
        )
        if not self._available:
            # A caller who explicitly adds this middleware is opting into
            # tracing -- silently no-oping every span forever (the previous
            # behavior) means trace propagation "goes dark" with zero
            # signal, easily mistaken for "tracing is working, just nothing
            # to show yet" until someone notices spans never appear. Loud
            # once, at construction, not per-message.
            reason = "obskit is not installed" if self._tracing is None else "obskit tracing is not configured"
            logger.warning(
                "TracedConsumerMiddleware(service_name=%r) added but %s -- "
                "every consume/publish span will be a silent no-op. Install "
                "obskit and configure tracing, or remove this middleware.",
                self._service_name,
                reason,
            )

    @property
    def is_available(self) -> bool:
        """Return True if obskit tracing is installed and configured."""
        return self._available

    # ── Internal helpers ──────────────────────────────────────────────────

    def _build_consume_attributes(self, message: RabbitMessage) -> dict[str, str]:
        """Build OpenTelemetry semantic attributes from a consumed message."""
        attrs: dict[str, str] = {
            "messaging.system": "rabbitmq",
            "messaging.operation": "receive",
        }
        if message.routing_key:
            attrs["messaging.rabbitmq.routing_key"] = message.routing_key
        if message.exchange:
            attrs["messaging.destination"] = message.exchange
        if message.message_id:
            attrs["messaging.message_id"] = message.message_id
        if message.correlation_id:
            attrs["messaging.correlation_id"] = message.correlation_id
        queue = message.headers.get("x-rabbitkit-original-queue", "")
        if queue:
            attrs["messaging.destination.name"] = queue
        retry_count = message.headers.get("x-rabbitkit-retry-count")
        if retry_count is not None:
            attrs["messaging.rabbitmq.retry_count"] = str(retry_count)
        return attrs

    def _build_publish_attributes(self, envelope: MessageEnvelope) -> dict[str, str]:
        """Build OpenTelemetry semantic attributes for an outgoing publish."""
        attrs: dict[str, str] = {
            "messaging.system": "rabbitmq",
            "messaging.operation": "send",
        }
        if envelope.routing_key:
            attrs["messaging.rabbitmq.routing_key"] = envelope.routing_key
        if envelope.exchange:
            attrs["messaging.destination"] = envelope.exchange
        if envelope.message_id:
            attrs["messaging.message_id"] = envelope.message_id
        if envelope.correlation_id:
            attrs["messaging.correlation_id"] = envelope.correlation_id
        return attrs

    def _envelope_with_trace_headers(self, envelope: MessageEnvelope) -> MessageEnvelope:
        """Return a copy of *envelope* with trace-propagation headers injected."""
        trace_headers = self._tracing.inject_trace_context()
        if not trace_headers:
            return envelope
        merged = {**envelope.headers, **trace_headers}
        return replace(envelope, headers=merged)

    # ── Consume-side hooks ────────────────────────────────────────────────

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        """Sync: wrap handler in ``trace_span``."""
        if not self._available:
            return call_next(message)

        # Extract incoming trace context from message headers
        carrier = {k: str(v) for k, v in message.headers.items() if isinstance(v, str)}
        self._tracing.extract_trace_context(carrier)

        attrs = self._build_consume_attributes(message)
        span_name = f"rabbitkit.consume {message.routing_key or 'unknown'}"

        with self._tracing.trace_span(
            span_name,
            component=self._service_name,
            operation="consume",
            attributes=attrs,
        ):
            return call_next(message)

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Async: wrap handler in ``async_trace_span``."""
        if not self._available:
            return await call_next(message)

        carrier = {k: str(v) for k, v in message.headers.items() if isinstance(v, str)}
        self._tracing.extract_trace_context(carrier)

        attrs = self._build_consume_attributes(message)
        span_name = f"rabbitkit.consume {message.routing_key or 'unknown'}"

        async with self._tracing.async_trace_span(
            span_name,
            component=self._service_name,
            operation="consume",
            attributes=attrs,
        ):
            return await call_next(message)

    # ── Publish-side hooks ────────────────────────────────────────────────

    def publish_scope(
        self,
        call_next: Callable[[MessageEnvelope], Any],
        envelope: MessageEnvelope,
    ) -> Any:
        """Sync: inject trace context into outgoing headers and create span."""
        if not self._available:
            return call_next(envelope)

        envelope = self._envelope_with_trace_headers(envelope)
        attrs = self._build_publish_attributes(envelope)
        span_name = f"rabbitkit.publish {envelope.routing_key or 'unknown'}"

        with self._tracing.trace_span(
            span_name,
            component=self._service_name,
            operation="publish",
            attributes=attrs,
        ):
            return call_next(envelope)

    async def publish_scope_async(
        self,
        call_next: Callable[[MessageEnvelope], Awaitable[Any]],
        envelope: MessageEnvelope,
    ) -> Any:
        """Async: inject trace context into outgoing headers and create span."""
        if not self._available:
            return await call_next(envelope)

        envelope = self._envelope_with_trace_headers(envelope)
        attrs = self._build_publish_attributes(envelope)
        span_name = f"rabbitkit.publish {envelope.routing_key or 'unknown'}"

        async with self._tracing.async_trace_span(
            span_name,
            component=self._service_name,
            operation="publish",
            attributes=attrs,
        ):
            return await call_next(envelope)
