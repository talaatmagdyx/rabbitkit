"""HandlerPipeline raw-body cap (§15.6, ``ConsumerConfig.max_body_bytes``):
an oversized delivery is INVALID_MESSAGE and dead-lettered without running
the filter, any middleware or the handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.errors import InboundMessageTooLargeError, InvalidMessageError, classify_error
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.pipeline import HandlerPipeline, _WindowedLog
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import AckPolicy, ErrorCategory
from rabbitkit.middleware.base import BaseMiddleware


class _Recorder(BaseMiddleware):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def on_receive(self, message: RabbitMessage) -> None:
        self.calls.append("on_receive")

    def consume_scope(self, call_next: Any, message: RabbitMessage) -> Any:
        self.calls.append("consume_scope")
        return call_next(message)

    async def consume_scope_async(self, call_next: Any, message: RabbitMessage) -> Any:
        self.calls.append("consume_scope")
        return await call_next(message)


def _msg(body: bytes) -> RabbitMessage:
    msg = RabbitMessage(body=body, message_id="m-1", delivery_tag=1)
    msg._ack_fn, msg._nack_fn, msg._reject_fn = MagicMock(), MagicMock(), MagicMock()
    msg._ack_async_fn, msg._nack_async_fn, msg._reject_async_fn = AsyncMock(), AsyncMock(), AsyncMock()
    return msg


def _route(handler: Any, *, ack_policy: AckPolicy = AckPolicy.AUTO, **kw: Any) -> RouteDefinition:
    return RouteDefinition(
        name="r", queue=RabbitQueue(name="orders"), exchange=None, handler=handler, ack_policy=ack_policy, **kw
    )


def _log(pipeline: HandlerPipeline) -> MagicMock:
    log = MagicMock()
    pipeline._failure_log = _WindowedLog(clock=lambda: 0.0, log=log)
    return log


def test_error_is_an_invalid_message() -> None:
    exc = InboundMessageTooLargeError(11, 10)
    assert isinstance(exc, InvalidMessageError)
    assert classify_error(exc).category is ErrorCategory.INVALID_MESSAGE
    assert (exc.size, exc.limit) == (11, 10)
    assert "max_body_bytes (10)" in str(exc)


class TestSync:
    @pytest.mark.parametrize("policy", list(AckPolicy))
    def test_oversized_is_rejected_without_requeue_under_every_policy(self, policy: AckPolicy) -> None:
        handler = MagicMock()
        mw = _Recorder()
        flt = MagicMock(return_value=True)
        pipeline = HandlerPipeline(max_body_bytes=10, manual_settle_on_error=False)
        log = _log(pipeline)
        msg = _msg(b"x" * 11)
        pipeline.process_sync(_route(handler, ack_policy=policy, route_middlewares=[mw], filter_fn=flt), msg)
        handler.assert_not_called()
        flt.assert_not_called()
        assert mw.calls == []
        msg._reject_fn.assert_called_once_with(False)
        msg._ack_fn.assert_not_called()
        assert msg.disposition == "rejected"
        rendered = str(log.warning.call_args.args[0] % log.warning.call_args.args[1:])
        assert "category=invalid_message" in rendered and "reason=body_too_large" in rendered

    def test_body_at_the_limit_is_processed(self) -> None:
        handler = MagicMock(return_value=None)
        pipeline = HandlerPipeline(max_body_bytes=10)
        msg = _msg(b"x" * 10)
        pipeline.process_sync(_route(lambda body: handler(body)), msg)
        handler.assert_called_once()
        assert msg.disposition == "acked"

    def test_no_cap_by_default(self) -> None:
        pipeline = HandlerPipeline()
        msg = _msg(b"x" * 100_000)
        pipeline.process_sync(_route(lambda body: None), msg)
        assert msg.disposition == "acked"

    def test_the_wire_body_is_measured(self) -> None:
        pipeline = HandlerPipeline(max_body_bytes=10)
        msg = _msg(b"x" * 11)
        msg.body = b"small"  # an on_receive hook may rewrite body; the cap is on what arrived
        pipeline.process_sync(_route(MagicMock()), msg)
        assert msg.disposition == "rejected"

    def test_already_settled_is_left_alone(self) -> None:
        pipeline = HandlerPipeline(max_body_bytes=1)
        msg = _msg(b"xx")
        msg._disposition = "acked"
        pipeline.process_sync(_route(MagicMock()), msg)
        msg._reject_fn.assert_not_called()

    def test_dead_channel_is_a_warning_not_a_raise(self) -> None:
        class ChannelClosed(Exception):
            pass

        pipeline = HandlerPipeline(max_body_bytes=1)
        warner = MagicMock()
        pipeline._settlement_loss_warner = warner
        msg = _msg(b"xx")
        msg._reject_fn.side_effect = ChannelClosed()
        pipeline.process_sync(_route(MagicMock()), msg)
        warner.warn.assert_called_once_with("ChannelClosed")

    def test_other_settlement_errors_propagate(self) -> None:
        pipeline = HandlerPipeline(max_body_bytes=1)
        msg = _msg(b"xx")
        msg._reject_fn.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            pipeline.process_sync(_route(MagicMock()), msg)


class TestAsync:
    @pytest.mark.parametrize("policy", list(AckPolicy))
    async def test_oversized_is_rejected_without_requeue_under_every_policy(self, policy: AckPolicy) -> None:
        handler = AsyncMock()
        mw = _Recorder()
        pipeline = HandlerPipeline(max_body_bytes=10, manual_settle_on_error=False)
        msg = _msg(b"x" * 11)
        await pipeline.process_async(_route(handler, ack_policy=policy, route_middlewares=[mw]), msg)
        handler.assert_not_awaited()
        assert mw.calls == []
        msg._reject_async_fn.assert_awaited_once_with(False)
        assert msg.disposition == "rejected"

    async def test_missing_dlq_requeues_instead_of_dropping(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import rabbitkit.core.pipeline as pipeline_mod

        monkeypatch.setattr(pipeline_mod, "_DLQ_MISSING_BACKOFF_INITIAL", 0.0)
        check = AsyncMock(return_value=False)
        pipeline = HandlerPipeline(max_body_bytes=1, dead_letter_check=check)
        msg = _msg(b"xx")
        await pipeline.process_async(_route(AsyncMock()), msg)
        check.assert_awaited_once()
        msg._nack_async_fn.assert_awaited_once_with(True)
        msg._reject_async_fn.assert_not_awaited()

    async def test_dead_channel_is_a_warning_not_a_raise(self) -> None:
        class ChannelInvalidStateError(Exception):
            pass

        pipeline = HandlerPipeline(max_body_bytes=1)
        warner = MagicMock()
        pipeline._settlement_loss_warner = warner
        msg = _msg(b"xx")
        msg._reject_async_fn.side_effect = ChannelInvalidStateError()
        await pipeline.process_async(_route(AsyncMock()), msg)
        warner.warn.assert_called_once_with("ChannelInvalidStateError")

    async def test_other_settlement_errors_propagate(self) -> None:
        pipeline = HandlerPipeline(max_body_bytes=1)
        msg = _msg(b"xx")
        msg._reject_async_fn.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            await pipeline.process_async(_route(AsyncMock()), msg)

    async def test_already_settled_is_left_alone(self) -> None:
        pipeline = HandlerPipeline(max_body_bytes=1)
        msg = _msg(b"xx")
        msg._disposition = "acked"
        await pipeline.process_async(_route(AsyncMock()), msg)
        msg._reject_async_fn.assert_not_awaited()

    async def test_body_within_the_limit_is_processed(self) -> None:
        seen: list[bytes] = []

        async def handler(body: bytes) -> None:
            seen.append(body)

        pipeline = HandlerPipeline(max_body_bytes=10)
        msg = _msg(b"abc")
        await pipeline.process_async(_route(handler), msg)
        assert seen == [b"abc"] and msg.disposition == "acked"
