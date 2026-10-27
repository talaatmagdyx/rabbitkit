"""HandlerPipeline settlement paths not covered elsewhere: handler-raised
RejectMessage(requeue=True), a wiring-bug ack, and the unbounded reply_to
warning."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage, RejectMessage, SettlementError
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus


def _route(handler: Any, **kw: Any) -> RouteDefinition:
    return RouteDefinition(name="r", queue=RabbitQueue(name="orders"), exchange=None, handler=handler, **kw)


def _async_msg(**kw: Any) -> RabbitMessage:
    msg = RabbitMessage(body=b"{}", message_id="m-1", delivery_tag=1, **kw)
    msg._ack_async_fn, msg._nack_async_fn, msg._reject_async_fn = AsyncMock(), AsyncMock(), AsyncMock()
    return msg


async def test_async_reject_with_requeue_requeues_without_dead_letter_check() -> None:
    check = AsyncMock(return_value=False)

    async def handler(body: bytes) -> None:
        raise RejectMessage(requeue=True)

    msg = _async_msg()
    await HandlerPipeline(dead_letter_check=check).process_async(_route(handler), msg)
    msg._reject_async_fn.assert_awaited_once_with(True)  # type: ignore[union-attr]
    check.assert_not_awaited()


def test_sync_ack_on_an_async_only_message_is_a_wiring_error_not_an_unknown_outcome() -> None:
    pipeline = HandlerPipeline()
    msg = RabbitMessage(body=b"{}", message_id="m-1", delivery_tag=1)  # no sync ack fn wired
    with pytest.raises(SettlementError):
        pipeline._ack_after_success_sync(_route(lambda body: None), msg, None)
    assert pipeline.ack_outcome_unknown_total == 0


async def test_unbounded_reply_to_is_published_and_warned_once(caplog: pytest.LogCaptureFixture) -> None:
    published: list[MessageEnvelope] = []

    async def publish(env: MessageEnvelope) -> PublishOutcome:
        published.append(env)
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    async def handler(body: bytes) -> bytes:
        return b"result"

    pipeline = HandlerPipeline()
    route = _route(handler)
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            await pipeline.process_async(route, _async_msg(reply_to="callers-queue"), publish_fn=publish)
    assert [e.routing_key for e in published] == ["callers-queue", "callers-queue"]
    assert sum("reply_to_allow" in r.getMessage() for r in caplog.records) == 1


def test_sync_result_to_unbounded_reply_to_is_published() -> None:
    publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
    msg = RabbitMessage(body=b"{}", message_id="m-1", delivery_tag=1, reply_to="callers-queue")
    msg._ack_fn = MagicMock()
    HandlerPipeline().process_sync(_route(lambda body: b"ok"), msg, publish_fn=publish)
    assert publish.call_args.args[0].routing_key == "callers-queue"
    msg._ack_fn.assert_called_once()
