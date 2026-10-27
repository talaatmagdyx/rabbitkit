"""Tests for middleware/event_metadata.py — opt-in event metadata validation,
and preservation of the metadata through retry, DLQ and replay copies."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.config import RetryConfig
from rabbitkit.core.errors import EventMetadataError
from rabbitkit.core.event_metadata import HEADER_EVENT_VERSION, HEADER_TENANT_ID, EventMetadata
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.dlq import DLQInspector
from rabbitkit.middleware.event_metadata import EventMetadataMiddleware
from rabbitkit.middleware.retry import RetryMiddleware

OCCURRED = datetime(2026, 10, 27, 12, 0, tzinfo=UTC)


def _meta(**kw: Any) -> EventMetadata:
    fields: dict[str, Any] = {
        "event_type": "order.created",
        "event_version": 1,
        "source": "checkout",
        "idempotency_key": "order:42",
        "occurred_at": OCCURRED,
        "message_id": "m-1",
        "correlation_id": "c-1",
        "tenant_id": "t-1",
    }
    fields.update(kw)
    return EventMetadata(**fields)


def _message(meta: EventMetadata | None = None, *, queue: str = "orders", **header_overrides: Any) -> RabbitMessage:
    env = (meta or _meta()).to_envelope(routing_key=queue, body=b'{"id": 42}')
    headers = {**env.headers, "x-rabbitkit-original-queue": queue, **header_overrides}
    msg = RabbitMessage(
        body=env.body,
        headers=headers,
        message_id=env.message_id,
        correlation_id=env.correlation_id,
        type=env.type,
        app_id=env.app_id,
        timestamp=env.timestamp,
        routing_key=queue,
        delivery_mode=2,
    )
    msg._ack_fn, msg._nack_fn, msg._reject_fn = MagicMock(), MagicMock(), MagicMock()
    msg._ack_async_fn, msg._nack_async_fn, msg._reject_async_fn = AsyncMock(), AsyncMock(), AsyncMock()
    return msg


def _assert_metadata_preserved(env: MessageEnvelope, meta: EventMetadata) -> None:
    assert EventMetadata.from_envelope(env) == meta
    assert env.message_id == meta.message_id
    assert env.timestamp is not None


class TestConsume:
    def test_valid_message_reaches_the_handler(self) -> None:
        mw = EventMetadataMiddleware(supported_versions={"order.created": [1, 2]}, require_tenant=True)
        call_next = MagicMock(return_value="done")
        assert mw.consume_scope(call_next, _message()) == "done"
        call_next.assert_called_once()

    async def test_valid_message_reaches_the_async_handler(self) -> None:
        mw = EventMetadataMiddleware(supported_versions=[1])
        call_next = AsyncMock(return_value="done")
        assert await mw.consume_scope_async(call_next, _message()) == "done"

    def test_missing_metadata_never_reaches_the_handler(self) -> None:
        call_next = MagicMock()
        msg = RabbitMessage(body=b"{}", message_id="m-1")
        with pytest.raises(EventMetadataError):
            EventMetadataMiddleware().consume_scope(call_next, msg)
        call_next.assert_not_called()

    async def test_unknown_version_never_reaches_the_async_handler(self) -> None:
        call_next = AsyncMock()
        mw = EventMetadataMiddleware(supported_versions={"order.created": {1}})
        with pytest.raises(EventMetadataError) as info:
            await mw.consume_scope_async(call_next, _message(_meta(event_version=2)))
        assert info.value.fields == ("event_version",)
        call_next.assert_not_awaited()

    def test_require_tenant(self) -> None:
        mw = EventMetadataMiddleware(require_tenant=True)
        with pytest.raises(EventMetadataError):
            mw.validate_message(_message(_meta(tenant_id=None)))
        assert mw.validate_message(_message()).tenant_id == "t-1"


class TestPublish:
    def test_sets_the_timestamp_when_missing(self) -> None:
        meta = _meta()
        env_no_ts = MessageEnvelope(
            routing_key="q",
            body=b"",
            headers=meta.headers(),
            message_id=meta.message_id,
            correlation_id=meta.correlation_id,
            type=meta.event_type,
            app_id=meta.source,
        )
        call_next = MagicMock(return_value="outcome")
        assert EventMetadataMiddleware().publish_scope(call_next, env_no_ts) == "outcome"
        sent = call_next.call_args.args[0]
        assert sent.timestamp is not None and sent.timestamp.tzinfo is UTC

    async def test_keeps_an_existing_timestamp(self) -> None:
        env = _meta().to_envelope(routing_key="q", body=b"")
        call_next = AsyncMock(return_value="outcome")
        assert await EventMetadataMiddleware().publish_scope_async(call_next, env) == "outcome"
        assert call_next.await_args.args[0] is env

    def test_invalid_envelope_is_never_sent(self) -> None:
        call_next = MagicMock()
        with pytest.raises(EventMetadataError):
            EventMetadataMiddleware().publish_scope(call_next, MessageEnvelope(routing_key="q", body=b""))
        call_next.assert_not_called()

    async def test_unsupported_version_is_never_sent(self) -> None:
        call_next = AsyncMock()
        env = _meta(event_version=3).to_envelope(routing_key="q", body=b"")
        with pytest.raises(EventMetadataError):
            await EventMetadataMiddleware(supported_versions={1, 2}).publish_scope_async(call_next, env)
        call_next.assert_not_awaited()

    async def test_validate_publish_false_passes_everything_through(self) -> None:
        mw = EventMetadataMiddleware(validate_publish=False)
        env = MessageEnvelope(routing_key="q", body=b"")
        sync_next = MagicMock(return_value=1)
        async_next = AsyncMock(return_value=2)
        assert mw.publish_scope(sync_next, env) == 1
        assert await mw.publish_scope_async(async_next, env) == 2
        assert sync_next.call_args.args[0] is env and async_next.await_args.args[0] is env


# ── Retry, DLQ and replay copies keep the metadata ──────────────────────────


def _ok(published: list[MessageEnvelope]) -> Any:
    def publish(env: MessageEnvelope) -> PublishOutcome:
        published.append(env)
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    return publish


def _ok_async(published: list[MessageEnvelope]) -> Any:
    async def publish(env: MessageEnvelope) -> PublishOutcome:
        published.append(env)
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    return publish


class TestPreservedThroughRetryAndDLQ:
    def test_retry_copy_keeps_the_metadata(self) -> None:
        published: list[MessageEnvelope] = []
        meta = _meta()
        retry = RetryMiddleware(RetryConfig(max_retries=2, delays=(1, 2)), publish_fn=_ok(published))

        def handler(msg: RabbitMessage) -> None:
            raise TimeoutError("flaky")

        msg = _message(meta)
        retry.consume_scope(handler, msg)
        (env,) = published
        assert env.routing_key == "orders.retry.1"
        _assert_metadata_preserved(env, meta)
        assert env.timestamp == msg.timestamp

    async def test_invalid_metadata_is_dead_lettered_with_failure_metadata(self) -> None:
        """RetryMiddleware is auto-inserted OUTSIDE route middlewares, so an
        EventMetadataError reaches it: terminal, confirmed DLQ copy with
        category invalid_message, the original metadata still on it."""
        published: list[MessageEnvelope] = []
        meta = _meta(event_version=7)
        retry = RetryMiddleware(RetryConfig(max_retries=3, delays=(1, 2, 3)), publish_async_fn=_ok_async(published))
        validator = EventMetadataMiddleware(supported_versions={"order.created": {1}})
        handler = AsyncMock()
        msg = _message(meta)

        async def chain(m: RabbitMessage) -> Any:
            return await validator.consume_scope_async(handler, m)

        with pytest.raises(EventMetadataError) as info:
            await retry.consume_scope_async(chain, msg)
        handler.assert_not_awaited()
        assert getattr(info.value, "_rabbitkit_dead_lettered", False) is True
        (env,) = published
        assert env.routing_key == "orders.dlq"
        assert env.headers["x-rabbitkit-error-category"] == "invalid_message"
        assert env.headers["x-rabbitkit-failure-category"] == "permanent"
        _assert_metadata_preserved(env, meta)
        msg._ack_async_fn.assert_awaited_once()

    def test_exhausted_retries_dlq_copy_keeps_the_metadata(self) -> None:
        published: list[MessageEnvelope] = []
        meta = _meta()
        retry = RetryMiddleware(RetryConfig(max_retries=1, delays=(1,)), publish_fn=_ok(published))

        def handler(msg: RabbitMessage) -> None:
            raise TimeoutError("still flaky")

        with pytest.raises(TimeoutError):
            retry.consume_scope(handler, _message(meta, **{"x-rabbitkit-retry-count": 1}))
        (env,) = published
        assert env.routing_key == "orders.dlq"
        _assert_metadata_preserved(env, meta)

    def test_replay_copy_keeps_the_metadata(self) -> None:
        meta = _meta()
        dlq_msg = _message(meta, **{"x-rabbitkit-retry-count": 2})
        env = DLQInspector._build_replay_envelope(dlq_msg, None, None, True, actor="ops")
        assert env.routing_key == "orders"
        _assert_metadata_preserved(env, meta)
        assert env.headers[HEADER_EVENT_VERSION] == 1 and env.headers[HEADER_TENANT_ID] == "t-1"
