"""Circuit breaker middleware — wraps handler execution and publish operations.

Uses obskit's CircuitBreaker or any compatible implementation that satisfies
CircuitBreakerProtocol / AsyncCircuitBreakerProtocol.

When the circuit is open, operations fail fast with CircuitBreakerOpenError
without hitting the broker, preventing cascade failures.

Lazy/no-op pattern: if no circuit breaker is provided, middleware is a passthrough.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open and rejects an operation."""


class CircuitBreakerMiddleware(BaseMiddleware):
    """Wraps handler execution and publish operations with circuit breaker.

    If circuit breaker is not provided (None), all operations pass through
    without any wrapping (no-op mode).

    Usage::

        from obskit.resilience import CircuitBreaker

        cb = CircuitBreaker(name="rabbitmq", fail_max=5, reset_timeout=60)
        middleware = CircuitBreakerMiddleware(circuit_breaker=cb)

        # Or with separate publish CB:
        middleware = CircuitBreakerMiddleware(
            circuit_breaker=consume_cb,
            publish_circuit_breaker=publish_cb,
        )

    Args:
        circuit_breaker: Circuit breaker for consume operations (handler execution).
            Must satisfy CircuitBreakerProtocol. None for no-op.
        publish_circuit_breaker: Circuit breaker for publish operations.
            If None, uses circuit_breaker for both. If circuit_breaker is also
            None, publish operations pass through.
        async_circuit_breaker: Async circuit breaker for consume operations.
            Must satisfy AsyncCircuitBreakerProtocol. If None, falls back to
            wrapping the sync circuit_breaker in async context.
        async_publish_circuit_breaker: Async circuit breaker for publish operations.
    """

    def __init__(
        self,
        circuit_breaker: Any | None = None,
        publish_circuit_breaker: Any | None = None,
        *,
        async_circuit_breaker: Any | None = None,
        async_publish_circuit_breaker: Any | None = None,
    ) -> None:
        self._cb = circuit_breaker
        self._publish_cb = publish_circuit_breaker or circuit_breaker
        self._async_cb = async_circuit_breaker
        self._async_publish_cb = async_publish_circuit_breaker or async_circuit_breaker

    # ── Consume-side ──────────────────────────────────────────────────────

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        """Wrap handler execution with circuit breaker (sync)."""
        if self._cb is None:
            return call_next(message)
        return self._cb.call(call_next, message)

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Wrap handler execution with circuit breaker (async)."""
        if self._async_cb is not None:
            return await self._async_cb.call_async(call_next, message)
        if self._cb is not None:
            # Sync CB cannot safely wrap an async handler — the sync call()
            # would receive a coroutine object, not the result. Raise at call
            # time so the misconfiguration surfaces immediately rather than
            # silently skipping every handler invocation.
            raise TypeError(
                "CircuitBreakerMiddleware: async handler requires "
                "async_circuit_breaker=. Providing only a sync circuit_breaker "
                "with an async broker is not supported (the handler would never "
                "run). Pass async_circuit_breaker= instead."
            )
        return await call_next(message)

    # ── Publish-side ──────────────────────────────────────────────────────

    def publish_scope(
        self,
        call_next: Callable[[MessageEnvelope], Any],
        envelope: MessageEnvelope,
    ) -> Any:
        """Wrap publish with circuit breaker (sync)."""
        if self._publish_cb is None:
            return call_next(envelope)
        return self._publish_cb.call(call_next, envelope)

    async def publish_scope_async(
        self,
        call_next: Callable[[MessageEnvelope], Awaitable[Any]],
        envelope: MessageEnvelope,
    ) -> Any:
        """Wrap publish with circuit breaker (async)."""
        if self._async_publish_cb is not None:
            return await self._async_publish_cb.call_async(call_next, envelope)
        if self._publish_cb is not None:
            raise TypeError(
                "CircuitBreakerMiddleware: async publish requires "
                "async_publish_circuit_breaker=. Providing only a sync "
                "publish_circuit_breaker with an async broker is not supported."
            )
        return await call_next(envelope)
