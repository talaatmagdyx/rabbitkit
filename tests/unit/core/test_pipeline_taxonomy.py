"""HandlerPipeline and the failure taxonomy: body-decode and
on_receive failures are INVALID_MESSAGE, and the failure log names the
category."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.errors import ERROR_CATEGORY_ATTR
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.pipeline import HandlerPipeline, _WindowedLog
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import ErrorCategory
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.serialization.json import JSONSerializer


def _msg(body: bytes = b"{}") -> RabbitMessage:
    msg = RabbitMessage(body=body, message_id="m-1", delivery_tag=1)
    msg._ack_fn, msg._nack_fn, msg._reject_fn = MagicMock(), MagicMock(), MagicMock()
    msg._ack_async_fn, msg._nack_async_fn, msg._reject_async_fn = AsyncMock(), AsyncMock(), AsyncMock()
    return msg


def _route(handler: Any, middlewares: list[Any] | None = None) -> RouteDefinition:
    return RouteDefinition(
        name="r",
        queue=RabbitQueue(name="orders"),
        exchange=None,
        handler=handler,
        route_middlewares=middlewares or [],
    )


class _Unreadable(BaseMiddleware):
    def on_receive(self, message: RabbitMessage) -> None:
        raise ValueError("corrupt gzip stream")


class _Capture(BaseMiddleware):
    """Outer middleware that records what reached it, then re-raises."""

    def __init__(self) -> None:
        self.seen: list[BaseException] = []

    def consume_scope(self, call_next: Any, message: RabbitMessage) -> Any:
        try:
            return call_next(message)
        except Exception as exc:
            self.seen.append(exc)
            raise

    async def consume_scope_async(self, call_next: Any, message: RabbitMessage) -> Any:
        try:
            return await call_next(message)
        except Exception as exc:
            self.seen.append(exc)
            raise


def _captured_log(pipeline: HandlerPipeline) -> MagicMock:
    log = MagicMock()
    pipeline._failure_log = _WindowedLog(clock=lambda: 0.0, log=log)
    return log


def _rendered(log: MagicMock) -> str:
    call = log.warning.call_args
    return str(call.args[0] % call.args[1:])


def _sync_dict_handler(body: dict[str, Any]) -> None:
    raise AssertionError("never reached")


async def _async_dict_handler(body: dict[str, Any]) -> None:
    raise AssertionError("never reached")


class TestDecodeFailureIsInvalidMessage:
    def test_sync_decode_failure_is_tagged_and_dead_lettered(self) -> None:
        capture = _Capture()
        pipeline = HandlerPipeline(serializer=JSONSerializer())
        log = _captured_log(pipeline)
        msg = _msg(b"{not json")
        pipeline.process_sync(_route(_sync_dict_handler, [capture]), msg)
        (exc,) = capture.seen
        assert getattr(exc, ERROR_CATEGORY_ATTR) is ErrorCategory.INVALID_MESSAGE
        msg._reject_fn.assert_called_once_with(False)  # type: ignore[union-attr]
        assert "category=invalid_message" in _rendered(log)

    async def test_async_decode_failure_is_tagged(self) -> None:
        capture = _Capture()
        pipeline = HandlerPipeline(serializer=JSONSerializer())
        msg = _msg(b"{not json")
        await pipeline.process_async(_route(_async_dict_handler, [capture]), msg)
        (exc,) = capture.seen
        assert getattr(exc, ERROR_CATEGORY_ATTR) is ErrorCategory.INVALID_MESSAGE

    async def test_handler_errors_are_not_tagged(self) -> None:
        capture = _Capture()

        async def handler(body: bytes) -> None:
            raise ValueError("business rule")

        await HandlerPipeline().process_async(_route(handler, [capture]), _msg())
        (exc,) = capture.seen
        assert not hasattr(exc, ERROR_CATEGORY_ATTR)


class TestOnReceiveFailureIsInvalidMessage:
    def test_sync(self) -> None:
        pipeline = HandlerPipeline()
        log = _captured_log(pipeline)
        msg = _msg()
        pipeline.process_sync(_route(lambda body: None, [_Unreadable()]), msg)
        msg._reject_fn.assert_called_once_with(False)  # type: ignore[union-attr]
        assert "category=invalid_message" in _rendered(log)

    async def test_async(self) -> None:
        pipeline = HandlerPipeline()
        log = _captured_log(pipeline)
        msg = _msg()

        async def handler(body: bytes) -> None:
            return None

        await pipeline.process_async(_route(handler, [_Unreadable()]), msg)
        msg._reject_async_fn.assert_awaited_once_with(False)  # type: ignore[union-attr]
        assert "category=invalid_message" in _rendered(log)


@pytest.mark.parametrize(
    ("exc", "category"),
    [(TimeoutError("db"), "retryable"), (RuntimeError("bug"), "permanent")],
)
async def test_failure_log_names_the_category(exc: Exception, category: str) -> None:
    pipeline = HandlerPipeline()
    log = _captured_log(pipeline)

    async def handler(body: bytes) -> None:
        raise exc

    await pipeline.process_async(_route(handler), _msg())
    assert f"category={category}" in _rendered(log)
