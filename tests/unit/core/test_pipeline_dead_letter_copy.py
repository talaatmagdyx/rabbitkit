"""HandlerPipeline: the G-17 DLQ copy for failures outside RetryMiddleware,
and the completed-dead-letter hook RetryMiddleware attaches."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

from rabbitkit.core.message import RabbitMessage, RejectMessage
from rabbitkit.core.pipeline import HandlerPipeline, _note_dead_lettered
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import AckPolicy
from rabbitkit.middleware.base import BaseMiddleware

if TYPE_CHECKING:
    import pytest


def _route(handler: Any, **kw: Any) -> RouteDefinition:
    return RouteDefinition(name="r", queue=RabbitQueue(name="orders"), exchange=None, handler=handler, **kw)


def _msg(**kw: Any) -> RabbitMessage:
    msg = RabbitMessage(body=b"{}", message_id="m-1", delivery_tag=1, **kw)
    msg._ack_async_fn, msg._nack_async_fn, msg._reject_async_fn = AsyncMock(), AsyncMock(), AsyncMock()
    return msg


def _raiser(exc: Exception) -> Any:
    async def handler(body: bytes) -> None:
        raise exc

    return handler


class _BadReceive(BaseMiddleware):
    async def on_receive_async(self, message: RabbitMessage) -> None:
        raise OSError("not a gzipped file")


class TestDeadLetterCopy:
    async def test_permanent_failure_is_copied_then_acked(self) -> None:
        copy = AsyncMock(return_value=True)
        check = AsyncMock(return_value=True)
        msg = _msg()
        error = ValueError("bad")

        await HandlerPipeline(dead_letter_copy=copy, dead_letter_check=check).process_async(_route(_raiser(error)), msg)

        copy.assert_awaited_once()
        assert copy.await_args is not None
        assert copy.await_args.args[2] is error and copy.await_args.args[3] == "permanent"
        msg._ack_async_fn.assert_awaited_once()  # type: ignore[union-attr]
        msg._reject_async_fn.assert_not_awaited()  # type: ignore[union-attr]
        check.assert_not_awaited()

    async def test_on_receive_failure_is_copied(self) -> None:
        copy = AsyncMock(return_value=True)
        msg = _msg()
        route = _route(_raiser(AssertionError("handler must not run")), route_middlewares=[_BadReceive()])

        await HandlerPipeline(dead_letter_copy=copy).process_async(route, msg)

        assert copy.await_args is not None
        assert isinstance(copy.await_args.args[2], OSError)
        msg._ack_async_fn.assert_awaited_once()  # type: ignore[union-attr]

    async def test_oversized_body_is_copied(self) -> None:
        copy = AsyncMock(return_value=True)
        msg = _msg()

        await HandlerPipeline(dead_letter_copy=copy, max_body_bytes=1).process_async(_route(_raiser(ValueError())), msg)

        assert copy.await_args is not None and copy.await_args.args[3] == "body_too_large"
        msg._ack_async_fn.assert_awaited_once()  # type: ignore[union-attr]

    async def test_unconfirmed_copy_falls_back_to_the_checked_reject(self) -> None:
        copy = AsyncMock(return_value=False)
        check = AsyncMock(return_value=True)
        msg = _msg()

        await HandlerPipeline(dead_letter_copy=copy, dead_letter_check=check).process_async(
            _route(_raiser(ValueError())), msg
        )

        check.assert_awaited_once()
        msg._reject_async_fn.assert_awaited_once_with(False)  # type: ignore[union-attr]
        msg._ack_async_fn.assert_not_awaited()  # type: ignore[union-attr]

    async def test_unconfirmed_copy_with_a_missing_dlq_requeues(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import rabbitkit.core.pipeline as pipeline_mod

        monkeypatch.setattr(pipeline_mod.asyncio, "sleep", AsyncMock())
        msg = _msg()

        await HandlerPipeline(
            dead_letter_copy=AsyncMock(return_value=False), dead_letter_check=AsyncMock(return_value=False)
        ).process_async(_route(_raiser(ValueError())), msg)

        msg._nack_async_fn.assert_awaited_once_with(True)  # type: ignore[union-attr]
        msg._ack_async_fn.assert_not_awaited()  # type: ignore[union-attr]

    async def test_transient_failure_requeued_is_not_copied(self) -> None:
        copy = AsyncMock(return_value=True)
        msg = _msg()

        await HandlerPipeline(dead_letter_copy=copy).process_async(_route(_raiser(TimeoutError())), msg)

        copy.assert_not_awaited()
        msg._nack_async_fn.assert_awaited_once_with(True)  # type: ignore[union-attr]

    async def test_handler_requested_reject_is_not_copied(self) -> None:
        copy = AsyncMock(return_value=True)
        msg = _msg()

        await HandlerPipeline(dead_letter_copy=copy).process_async(_route(_raiser(RejectMessage())), msg)

        copy.assert_not_awaited()
        msg._reject_async_fn.assert_awaited_once_with(False)  # type: ignore[union-attr]

    async def test_retry_terminal_hand_back_is_not_copied_again(self) -> None:
        copy = AsyncMock(return_value=True)
        error = ValueError("copy already attempted by RetryMiddleware")
        error._rabbitkit_terminal = True  # type: ignore[attr-defined]
        msg = _msg()

        await HandlerPipeline(dead_letter_copy=copy).process_async(_route(_raiser(error)), msg)

        copy.assert_not_awaited()
        msg._reject_async_fn.assert_awaited_once_with(False)  # type: ignore[union-attr]

    async def test_ack_first_route_is_never_copied(self) -> None:
        copy = AsyncMock(return_value=True)
        msg = _msg()

        await HandlerPipeline(dead_letter_copy=copy).process_async(
            _route(_raiser(ValueError()), ack_policy=AckPolicy.ACK_FIRST), msg
        )

        copy.assert_not_awaited()


class TestCompletedDeadLetterHook:
    async def test_async_reject_runs_the_hook_once(self) -> None:
        hook = MagicMock()
        error = ValueError()
        error._rabbitkit_terminal = True  # type: ignore[attr-defined]
        error._rabbitkit_on_dead_lettered = hook  # type: ignore[attr-defined]

        await HandlerPipeline(dead_letter_check=AsyncMock(return_value=True)).process_async(
            _route(_raiser(error)), _msg()
        )

        hook.assert_called_once_with()
        assert not hasattr(error, "_rabbitkit_on_dead_lettered")

    async def test_requeue_for_a_missing_dlq_does_not_run_the_hook(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import rabbitkit.core.pipeline as pipeline_mod

        monkeypatch.setattr(pipeline_mod.asyncio, "sleep", AsyncMock())
        hook = MagicMock()
        error = ValueError()
        error._rabbitkit_terminal = True  # type: ignore[attr-defined]
        error._rabbitkit_on_dead_lettered = hook  # type: ignore[attr-defined]

        await HandlerPipeline(dead_letter_check=AsyncMock(return_value=False)).process_async(
            _route(_raiser(error)), _msg()
        )

        hook.assert_not_called()

    def test_sync_reject_runs_the_hook(self) -> None:
        hook = MagicMock()
        error = ValueError()
        error._rabbitkit_terminal = True  # type: ignore[attr-defined]
        error._rabbitkit_on_dead_lettered = hook  # type: ignore[attr-defined]

        def handler(body: bytes) -> None:
            raise error

        msg = RabbitMessage(body=b"{}", message_id="m-1", delivery_tag=1)
        msg._reject_fn = MagicMock()
        HandlerPipeline().process_sync(_route(handler), msg)

        msg._reject_fn.assert_called_once_with(False)
        hook.assert_called_once_with()

    def test_a_raising_hook_is_logged_not_raised(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import rabbitkit.core.pipeline as pipeline_mod

        logged = MagicMock()
        monkeypatch.setattr(pipeline_mod.logger, "exception", logged)
        error = ValueError()
        error._rabbitkit_on_dead_lettered = MagicMock(side_effect=RuntimeError("down"))  # type: ignore[attr-defined]

        _note_dead_lettered(error)

        logged.assert_called_once_with("dead-letter hook raised")

    def test_no_hook_is_a_noop(self) -> None:
        _note_dead_lettered(ValueError())
