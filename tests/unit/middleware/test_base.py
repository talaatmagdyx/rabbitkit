"""Tests for middleware/base.py — BaseMiddleware."""

from __future__ import annotations

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {"body": b"hello", "routing_key": "rk"}
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


class TestBaseMiddleware:
    def test_consume_scope_passthrough(self) -> None:
        mw = BaseMiddleware()
        msg = _make_message()
        result = mw.consume_scope(lambda m: "result", msg)
        assert result == "result"

    @pytest.mark.asyncio
    async def test_consume_scope_async_passthrough(self) -> None:
        mw = BaseMiddleware()
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "async-result"

        result = await mw.consume_scope_async(call_next, msg)
        assert result == "async-result"

    def test_on_receive_noop(self) -> None:
        mw = BaseMiddleware()
        msg = _make_message()
        mw.on_receive(msg)  # no exception

    @pytest.mark.asyncio
    async def test_on_receive_async_noop(self) -> None:
        mw = BaseMiddleware()
        msg = _make_message()
        await mw.on_receive_async(msg)  # no exception

    def test_after_processed_noop(self) -> None:
        mw = BaseMiddleware()
        msg = _make_message()
        mw.after_processed(msg)  # no exception
        mw.after_processed(msg, exc=RuntimeError("test"))  # no exception

    @pytest.mark.asyncio
    async def test_after_processed_async_noop(self) -> None:
        mw = BaseMiddleware()
        msg = _make_message()
        await mw.after_processed_async(msg)

    def test_publish_scope_passthrough(self) -> None:
        mw = BaseMiddleware()
        env = MessageEnvelope(routing_key="rk", body=b"hello")
        result = mw.publish_scope(lambda e: "published", env)
        assert result == "published"

    @pytest.mark.asyncio
    async def test_publish_scope_async_passthrough(self) -> None:
        mw = BaseMiddleware()
        env = MessageEnvelope(routing_key="rk", body=b"hello")

        async def call_next(e: MessageEnvelope) -> str:
            return "async-published"

        result = await mw.publish_scope_async(call_next, env)
        assert result == "async-published"


class TestCustomMiddleware:
    def test_custom_consume_scope(self) -> None:
        class LoggingMiddleware(BaseMiddleware):
            def __init__(self) -> None:
                self.log: list[str] = []

            def consume_scope(self, call_next, message):
                self.log.append("before")
                result = call_next(message)
                self.log.append("after")
                return result

        mw = LoggingMiddleware()
        msg = _make_message()
        mw.consume_scope(lambda m: None, msg)
        assert mw.log == ["before", "after"]

    def test_custom_on_receive(self) -> None:
        class TrackingMiddleware(BaseMiddleware):
            def __init__(self) -> None:
                self.received: list[RabbitMessage] = []

            def on_receive(self, message: RabbitMessage) -> None:
                self.received.append(message)

        mw = TrackingMiddleware()
        msg = _make_message()
        mw.on_receive(msg)
        assert len(mw.received) == 1
        assert mw.received[0] is msg


class TestNoOpMiddleware:
    def test_consume_scope_passthrough(self) -> None:
        from rabbitkit.middleware.base import NoOpMiddleware
        mw = NoOpMiddleware()
        msg = _make_message()
        assert mw.consume_scope(lambda m: "ok", msg) == "ok"

    @pytest.mark.asyncio
    async def test_consume_scope_async_passthrough(self) -> None:
        from rabbitkit.middleware.base import NoOpMiddleware
        mw = NoOpMiddleware()
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "async-ok"

        assert await mw.consume_scope_async(call_next, msg) == "async-ok"

    def test_publish_scope_passthrough(self) -> None:
        from rabbitkit.middleware.base import NoOpMiddleware
        mw = NoOpMiddleware()
        env = MessageEnvelope(routing_key="q", body=b"x")
        assert mw.publish_scope(lambda e: "pub-ok", env) == "pub-ok"

    @pytest.mark.asyncio
    async def test_publish_scope_async_passthrough(self) -> None:
        from rabbitkit.middleware.base import NoOpMiddleware
        mw = NoOpMiddleware()
        env = MessageEnvelope(routing_key="q", body=b"x")

        async def call_next(e: MessageEnvelope) -> str:
            return "pub-async-ok"

        assert await mw.publish_scope_async(call_next, env) == "pub-async-ok"
