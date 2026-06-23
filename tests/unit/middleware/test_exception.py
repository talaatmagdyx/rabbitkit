"""Tests for middleware/exception.py — ExceptionMiddleware."""

from __future__ import annotations

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.exception import ExceptionMiddleware


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {"body": b"hello", "routing_key": "rk"}
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


class TestExceptionMiddleware:
    def test_no_exception_passes_through(self) -> None:
        mw = ExceptionMiddleware()
        msg = _make_message()
        result = mw.consume_scope(lambda m: "ok", msg)
        assert result == "ok"

    def test_unhandled_exception_re_raises(self) -> None:
        mw = ExceptionMiddleware()
        msg = _make_message()

        def fail(m: RabbitMessage) -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            mw.consume_scope(fail, msg)

    def test_handled_exception_returns_fallback(self) -> None:
        mw = ExceptionMiddleware()
        mw.add_handler(ValueError, lambda e: "fallback")
        msg = _make_message()

        def fail(m: RabbitMessage) -> None:
            raise ValueError("bad data")

        result = mw.consume_scope(fail, msg)
        assert result == "fallback"

    def test_handler_matches_subclass(self) -> None:
        mw = ExceptionMiddleware()
        mw.add_handler(Exception, lambda e: "caught")
        msg = _make_message()

        def fail(m: RabbitMessage) -> None:
            raise ValueError("subclass of Exception")

        result = mw.consume_scope(fail, msg)
        assert result == "caught"

    def test_terminal_exception_re_raises_by_default(self) -> None:
        mw = ExceptionMiddleware()
        msg = _make_message()

        def fail(m: RabbitMessage) -> None:
            exc = RuntimeError("terminal")
            exc._rabbitkit_terminal = True  # type: ignore[attr-defined]
            raise exc

        with pytest.raises(RuntimeError, match="terminal"):
            mw.consume_scope(fail, msg)

    def test_terminal_exception_swallowed_when_opted_in(self) -> None:
        mw = ExceptionMiddleware(swallow_permanent=True)
        msg = _make_message()

        def fail(m: RabbitMessage) -> None:
            exc = RuntimeError("terminal")
            exc._rabbitkit_terminal = True  # type: ignore[attr-defined]
            raise exc

        result = mw.consume_scope(fail, msg)
        assert result is None

    def test_terminal_with_handler_and_swallow(self) -> None:
        mw = ExceptionMiddleware(swallow_permanent=True)
        mw.add_handler(RuntimeError, lambda e: "recovered")
        msg = _make_message()

        def fail(m: RabbitMessage) -> None:
            exc = RuntimeError("terminal with handler")
            exc._rabbitkit_terminal = True  # type: ignore[attr-defined]
            raise exc

        result = mw.consume_scope(fail, msg)
        assert result == "recovered"


class TestExceptionMiddlewareAsync:
    @pytest.mark.asyncio
    async def test_no_exception_passes_through(self) -> None:
        mw = ExceptionMiddleware()
        msg = _make_message()

        async def ok(m: RabbitMessage) -> str:
            return "ok"

        result = await mw.consume_scope_async(ok, msg)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_unhandled_exception_re_raises(self) -> None:
        mw = ExceptionMiddleware()
        msg = _make_message()

        async def fail(m: RabbitMessage) -> None:
            raise RuntimeError("async boom")

        with pytest.raises(RuntimeError, match="async boom"):
            await mw.consume_scope_async(fail, msg)

    @pytest.mark.asyncio
    async def test_handled_exception_returns_fallback(self) -> None:
        mw = ExceptionMiddleware()
        mw.add_handler(ValueError, lambda e: "async-fallback")
        msg = _make_message()

        async def fail(m: RabbitMessage) -> None:
            raise ValueError("async bad")

        result = await mw.consume_scope_async(fail, msg)
        assert result == "async-fallback"

    @pytest.mark.asyncio
    async def test_terminal_re_raises_by_default(self) -> None:
        mw = ExceptionMiddleware()
        msg = _make_message()

        async def fail(m: RabbitMessage) -> None:
            exc = RuntimeError("terminal")
            exc._rabbitkit_terminal = True  # type: ignore[attr-defined]
            raise exc

        with pytest.raises(RuntimeError, match="terminal"):
            await mw.consume_scope_async(fail, msg)
