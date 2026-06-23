"""Tests for middleware/circuit_breaker.py — CircuitBreakerMiddleware."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.circuit_breaker import (
    CircuitBreakerMiddleware,
    CircuitBreakerOpenError,
)


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b'{"id": 1}',
        "routing_key": "test.key",
        "headers": {},
        "path": {},
    }
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


def _make_envelope(**kwargs: object) -> MessageEnvelope:
    defaults: dict[str, object] = {"routing_key": "test.key", "body": b"test"}
    defaults.update(kwargs)
    return MessageEnvelope(**defaults)  # type: ignore[arg-type]


class _FakeCircuitBreaker:
    """Fake sync circuit breaker that satisfies CircuitBreakerProtocol."""

    def __init__(self, *, is_open: bool = False) -> None:
        self._open = is_open
        self.calls: list[tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]] = []

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        self.calls.append((func, args, kwargs))
        if self._open:
            raise CircuitBreakerOpenError("Circuit is open")
        return func(*args, **kwargs)


class _FakeAsyncCircuitBreaker:
    """Fake async circuit breaker that satisfies AsyncCircuitBreakerProtocol."""

    def __init__(self, *, is_open: bool = False) -> None:
        self._open = is_open
        self.calls: list[tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]] = []

    async def call_async(
        self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> Any:
        self.calls.append((func, args, kwargs))
        if self._open:
            raise CircuitBreakerOpenError("Circuit is open")
        return await func(*args, **kwargs)


# ── Consume scope (sync) ─────────────────────────────────────────────────


class TestConsumeScope:
    def test_noop_when_no_cb(self) -> None:
        """No circuit breaker -> passthrough."""
        mw = CircuitBreakerMiddleware()
        msg = _make_message()
        call_next = MagicMock(return_value="result")
        result = mw.consume_scope(call_next, msg)
        assert result == "result"
        call_next.assert_called_once_with(msg)

    def test_calls_through_cb(self) -> None:
        """Handler is invoked through the circuit breaker."""
        cb = _FakeCircuitBreaker()
        mw = CircuitBreakerMiddleware(circuit_breaker=cb)
        msg = _make_message()
        call_next = MagicMock(return_value="result")
        result = mw.consume_scope(call_next, msg)
        assert result == "result"
        assert len(cb.calls) == 1

    def test_cb_open_raises(self) -> None:
        """Open circuit breaker raises CircuitBreakerOpenError."""
        cb = _FakeCircuitBreaker(is_open=True)
        mw = CircuitBreakerMiddleware(circuit_breaker=cb)
        msg = _make_message()
        call_next = MagicMock()
        with pytest.raises(CircuitBreakerOpenError):
            mw.consume_scope(call_next, msg)


# ── Consume scope (async) ────────────────────────────────────────────────


class TestConsumeScopeAsync:
    async def test_noop_when_no_cb(self) -> None:
        """No circuit breaker -> async passthrough."""
        mw = CircuitBreakerMiddleware()
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "async-result"

        result = await mw.consume_scope_async(call_next, msg)
        assert result == "async-result"

    async def test_calls_through_async_cb(self) -> None:
        """Handler is invoked through the async circuit breaker."""
        acb = _FakeAsyncCircuitBreaker()
        mw = CircuitBreakerMiddleware(async_circuit_breaker=acb)
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "async-result"

        result = await mw.consume_scope_async(call_next, msg)
        assert result == "async-result"
        assert len(acb.calls) == 1

    async def test_fallback_to_sync_cb(self) -> None:
        """When no async CB, falls back to sync CB wrapping."""
        cb = _FakeCircuitBreaker()
        mw = CircuitBreakerMiddleware(circuit_breaker=cb)
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "result"

        # Sync CB wrapping: call_next returns a coroutine (not awaited by sync CB)
        result = mw.consume_scope(MagicMock(return_value="result"), msg)
        assert result == "result"
        assert len(cb.calls) == 1

    async def test_async_cb_open_raises(self) -> None:
        """Open async circuit breaker raises CircuitBreakerOpenError."""
        acb = _FakeAsyncCircuitBreaker(is_open=True)
        mw = CircuitBreakerMiddleware(async_circuit_breaker=acb)
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "result"

        with pytest.raises(CircuitBreakerOpenError):
            await mw.consume_scope_async(call_next, msg)


# ── Publish scope (sync) ─────────────────────────────────────────────────


class TestPublishScope:
    def test_noop_when_no_cb(self) -> None:
        """No circuit breaker -> passthrough."""
        mw = CircuitBreakerMiddleware()
        envelope = _make_envelope()
        call_next = MagicMock(return_value="published")
        result = mw.publish_scope(call_next, envelope)
        assert result == "published"
        call_next.assert_called_once_with(envelope)

    def test_uses_publish_cb(self) -> None:
        """Separate publish CB is used for publish operations."""
        consume_cb = _FakeCircuitBreaker()
        publish_cb = _FakeCircuitBreaker()
        mw = CircuitBreakerMiddleware(
            circuit_breaker=consume_cb, publish_circuit_breaker=publish_cb
        )
        envelope = _make_envelope()
        call_next = MagicMock(return_value="published")
        mw.publish_scope(call_next, envelope)
        assert len(publish_cb.calls) == 1
        assert len(consume_cb.calls) == 0  # not used for publish

    def test_falls_back_to_consume_cb(self) -> None:
        """When no publish CB, falls back to consume CB."""
        cb = _FakeCircuitBreaker()
        mw = CircuitBreakerMiddleware(circuit_breaker=cb)
        envelope = _make_envelope()
        call_next = MagicMock(return_value="published")
        mw.publish_scope(call_next, envelope)
        assert len(cb.calls) == 1

    def test_publish_cb_open_raises(self) -> None:
        """Open circuit breaker on publish raises CircuitBreakerOpenError."""
        cb = _FakeCircuitBreaker(is_open=True)
        mw = CircuitBreakerMiddleware(circuit_breaker=cb)
        envelope = _make_envelope()
        call_next = MagicMock()
        with pytest.raises(CircuitBreakerOpenError):
            mw.publish_scope(call_next, envelope)


# ── Publish scope (async) ────────────────────────────────────────────────


class TestPublishScopeAsync:
    async def test_noop_when_no_cb(self) -> None:
        """No circuit breaker -> async passthrough."""
        mw = CircuitBreakerMiddleware()
        envelope = _make_envelope()

        async def call_next(e: MessageEnvelope) -> str:
            return "async-published"

        result = await mw.publish_scope_async(call_next, envelope)
        assert result == "async-published"

    async def test_uses_async_publish_cb(self) -> None:
        """Async publish CB is used for async publish operations."""
        acb = _FakeAsyncCircuitBreaker()
        mw = CircuitBreakerMiddleware(async_publish_circuit_breaker=acb)
        envelope = _make_envelope()

        async def call_next(e: MessageEnvelope) -> str:
            return "published"

        await mw.publish_scope_async(call_next, envelope)
        assert len(acb.calls) == 1

    async def test_sync_publish_cb_with_async_raises(self) -> None:
        """Providing only a sync publish CB to an async path raises TypeError.

        Sync CB cannot safely wrap async call_next — it would receive a
        coroutine object and never await it, silently skipping publishes.
        This is now an explicit error so misconfiguration surfaces immediately.
        """
        cb = _FakeCircuitBreaker()
        mw = CircuitBreakerMiddleware(circuit_breaker=cb)
        envelope = _make_envelope()

        async def call_next(e: MessageEnvelope) -> str:
            return "published"

        with pytest.raises(TypeError, match="async_publish_circuit_breaker"):
            await mw.publish_scope_async(call_next, envelope)

    async def test_async_publish_cb_open_raises(self) -> None:
        """Open async publish CB raises CircuitBreakerOpenError."""
        acb = _FakeAsyncCircuitBreaker(is_open=True)
        mw = CircuitBreakerMiddleware(async_publish_circuit_breaker=acb)
        envelope = _make_envelope()

        async def call_next(e: MessageEnvelope) -> str:
            return "published"

        with pytest.raises(CircuitBreakerOpenError):
            await mw.publish_scope_async(call_next, envelope)
