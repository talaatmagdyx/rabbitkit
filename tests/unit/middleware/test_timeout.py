"""Tests for handler timeout middleware (F7)."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.timeout import HandlerTimeoutError, TimeoutConfig, TimeoutMiddleware


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b"test",
        "routing_key": "test",
        "exchange": "",
        "headers": {},
    }
    defaults.update(kwargs)
    msg = RabbitMessage(**defaults)  # type: ignore[arg-type]
    msg._ack_fn = MagicMock()
    msg._nack_fn = MagicMock()
    msg._reject_fn = MagicMock()
    msg._ack_async_fn = AsyncMock()
    msg._nack_async_fn = AsyncMock()
    msg._reject_async_fn = AsyncMock()
    return msg


class TestTimeoutConfig:
    def test_defaults(self) -> None:
        config = TimeoutConfig()
        assert config.timeout_seconds == 30.0

    def test_custom_timeout(self) -> None:
        config = TimeoutConfig(timeout_seconds=5.0)
        assert config.timeout_seconds == 5.0

    def test_invalid_timeout(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            TimeoutConfig(timeout_seconds=0)

    def test_negative_timeout(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            TimeoutConfig(timeout_seconds=-1)

    def test_frozen(self) -> None:
        config = TimeoutConfig()
        with pytest.raises(AttributeError):
            config.timeout_seconds = 10  # type: ignore[misc]


class TestTimeoutMiddlewareSync:
    def test_fast_handler_passes(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=1.0))
        msg = _make_message()
        call_next = MagicMock(return_value="result")

        result = mw.consume_scope(call_next, msg)

        call_next.assert_called_once_with(msg)
        assert result == "result"

    def test_slow_handler_raises_timeout(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()

        def slow_handler(m: RabbitMessage) -> str:
            time.sleep(1.0)
            return "late"

        with pytest.raises(HandlerTimeoutError, match=r"0\.1s"):
            mw.consume_scope(slow_handler, msg)

    def test_handler_exception_propagates(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=1.0))
        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            mw.consume_scope(failing_handler, msg)


class TestTimeoutMiddlewareAsync:
    @pytest.mark.asyncio
    async def test_fast_handler_passes_async(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=1.0))
        msg = _make_message()
        call_next = AsyncMock(return_value="result")

        result = await mw.consume_scope_async(call_next, msg)

        call_next.assert_called_once_with(msg)
        assert result == "result"

    @pytest.mark.asyncio
    async def test_slow_handler_raises_timeout_async(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()

        async def slow_handler(m: RabbitMessage) -> str:
            await asyncio.sleep(1.0)
            return "late"

        with pytest.raises(HandlerTimeoutError, match=r"0\.1s"):
            await mw.consume_scope_async(slow_handler, msg)

    @pytest.mark.asyncio
    async def test_handler_exception_propagates_async(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=1.0))
        msg = _make_message()

        async def failing_handler(m: RabbitMessage) -> None:
            raise ValueError("async boom")

        with pytest.raises(ValueError, match="async boom"):
            await mw.consume_scope_async(failing_handler, msg)

    @pytest.mark.asyncio
    async def test_timeout_error_type(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()

        async def slow(m: RabbitMessage) -> None:
            await asyncio.sleep(1.0)

        with pytest.raises(HandlerTimeoutError) as exc_info:
            await mw.consume_scope_async(slow, msg)
        assert exc_info.value.timeout_seconds == 0.1
