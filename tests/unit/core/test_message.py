"""Tests for core/message.py — RabbitMessage, ack/nack/reject, exceptions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {"body": b'{"id": 1}', "routing_key": "orders.created"}
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


def _wire_sync(msg: RabbitMessage) -> tuple[MagicMock, MagicMock, MagicMock]:
    ack = MagicMock()
    nack = MagicMock()
    reject = MagicMock()
    msg._ack_fn = ack
    msg._nack_fn = nack
    msg._reject_fn = reject
    return ack, nack, reject


# ── construction ──────────────────────────────────────────────────────────


class TestMessageConstruction:
    def test_required_fields(self) -> None:
        msg = RabbitMessage(body=b"hello", routing_key="rk")
        assert msg.body == b"hello"
        assert msg.routing_key == "rk"

    def test_defaults(self) -> None:
        msg = _make_message()
        assert msg.headers == {}
        assert msg.message_id is None
        assert msg.correlation_id is None
        assert msg.reply_to is None
        assert msg.content_type is None
        assert msg.content_encoding is None
        assert msg.timestamp is None
        assert msg.type is None
        assert msg.app_id is None
        assert msg.exchange == ""
        assert msg.delivery_tag is None
        assert msg.redelivered is False
        assert msg.consumer_tag is None
        assert msg.path == {}
        assert msg.raw_message is None

    def test_is_not_settled_initially(self) -> None:
        msg = _make_message()
        assert msg.is_settled is False
        assert msg._disposition == "pending"


# ── sync ack ──────────────────────────────────────────────────────────────


class TestSyncAck:
    def test_ack_calls_fn(self) -> None:
        msg = _make_message()
        ack_fn, _, _ = _wire_sync(msg)
        msg.ack()
        ack_fn.assert_called_once()
        assert msg._disposition == "acked"
        assert msg.is_settled is True

    def test_ack_idempotent(self) -> None:
        msg = _make_message()
        ack_fn, _, _ = _wire_sync(msg)
        msg.ack()
        msg.ack()  # second call is no-op
        ack_fn.assert_called_once()

    def test_ack_without_fn_raises(self) -> None:
        msg = _make_message()
        with pytest.raises(RuntimeError, match="sync-ack"):
            msg.ack()

    def test_nack_calls_fn(self) -> None:
        msg = _make_message()
        _, nack_fn, _ = _wire_sync(msg)
        msg.nack(requeue=True)
        nack_fn.assert_called_once_with(True)
        assert msg._disposition == "nacked"

    def test_nack_default_requeue_true(self) -> None:
        msg = _make_message()
        _, nack_fn, _ = _wire_sync(msg)
        msg.nack()
        nack_fn.assert_called_once_with(True)

    def test_nack_requeue_false(self) -> None:
        msg = _make_message()
        _, nack_fn, _ = _wire_sync(msg)
        msg.nack(requeue=False)
        nack_fn.assert_called_once_with(False)

    def test_nack_without_fn_raises(self) -> None:
        msg = _make_message()
        with pytest.raises(RuntimeError, match="sync-nack"):
            msg.nack()

    def test_reject_calls_fn(self) -> None:
        msg = _make_message()
        _, _, reject_fn = _wire_sync(msg)
        msg.reject(requeue=False)
        reject_fn.assert_called_once_with(False)
        assert msg._disposition == "rejected"

    def test_reject_default_requeue_false(self) -> None:
        msg = _make_message()
        _, _, reject_fn = _wire_sync(msg)
        msg.reject()
        reject_fn.assert_called_once_with(False)

    def test_reject_without_fn_raises(self) -> None:
        msg = _make_message()
        with pytest.raises(RuntimeError, match="sync-reject"):
            msg.reject()

    def test_nack_after_ack_is_noop(self) -> None:
        msg = _make_message()
        _, nack_fn, _ = _wire_sync(msg)
        msg.ack()
        msg.nack()  # no-op — already settled
        nack_fn.assert_not_called()

    def test_reject_after_nack_is_noop(self) -> None:
        msg = _make_message()
        _, _nack_fn, reject_fn = _wire_sync(msg)
        msg.nack()
        msg.reject()  # no-op
        reject_fn.assert_not_called()


# ── async ack ─────────────────────────────────────────────────────────────


class TestAsyncAck:
    @pytest.mark.asyncio
    async def test_async_ack_with_async_fn(self) -> None:
        msg = _make_message()
        called = False

        async def mock_ack() -> None:
            nonlocal called
            called = True

        msg._ack_async_fn = mock_ack
        await msg.ack_async()
        assert called
        assert msg._disposition == "acked"

    @pytest.mark.asyncio
    async def test_async_ack_falls_back_to_sync(self) -> None:
        msg = _make_message()
        sync_ack = MagicMock()
        msg._ack_fn = sync_ack
        await msg.ack_async()
        sync_ack.assert_called_once()
        assert msg._disposition == "acked"

    @pytest.mark.asyncio
    async def test_async_ack_idempotent(self) -> None:
        msg = _make_message()
        sync_ack = MagicMock()
        msg._ack_fn = sync_ack
        await msg.ack_async()
        await msg.ack_async()  # second call no-op
        sync_ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_nack(self) -> None:
        msg = _make_message()
        sync_nack = MagicMock()
        msg._nack_fn = sync_nack
        await msg.nack_async(requeue=False)
        sync_nack.assert_called_once_with(False)
        assert msg._disposition == "nacked"

    @pytest.mark.asyncio
    async def test_async_nack_with_async_fn(self) -> None:
        msg = _make_message()
        received_requeue = None

        async def mock_nack(requeue: bool) -> None:
            nonlocal received_requeue
            received_requeue = requeue

        msg._nack_async_fn = mock_nack
        await msg.nack_async(requeue=True)
        assert received_requeue is True
        assert msg._disposition == "nacked"

    @pytest.mark.asyncio
    async def test_async_reject(self) -> None:
        msg = _make_message()
        sync_reject = MagicMock()
        msg._reject_fn = sync_reject
        await msg.reject_async(requeue=True)
        sync_reject.assert_called_once_with(True)
        assert msg._disposition == "rejected"

    @pytest.mark.asyncio
    async def test_async_reject_with_async_fn(self) -> None:
        msg = _make_message()
        received_requeue = None

        async def mock_reject(requeue: bool) -> None:
            nonlocal received_requeue
            received_requeue = requeue

        msg._reject_async_fn = mock_reject
        await msg.reject_async(requeue=False)
        assert received_requeue is False
        assert msg._disposition == "rejected"


# ── exception-based ack ──────────────────────────────────────────────────


class TestAckExceptions:
    def test_ack_message(self) -> None:
        exc = AckMessage()
        assert isinstance(exc, Exception)

    def test_nack_message_default(self) -> None:
        exc = NackMessage()
        assert exc.requeue is True

    def test_nack_message_no_requeue(self) -> None:
        exc = NackMessage(requeue=False)
        assert exc.requeue is False

    def test_reject_message_default(self) -> None:
        exc = RejectMessage()
        assert exc.requeue is False

    def test_reject_message_with_requeue(self) -> None:
        exc = RejectMessage(requeue=True)
        assert exc.requeue is True


class TestAsyncFunctions:
    async def test_nack_async_with_async_fn(self) -> None:
        """Line 155: nack_async calls _nack_async_fn when set."""
        from unittest.mock import AsyncMock

        msg = _make_message()
        async_nack = AsyncMock()
        msg._nack_async_fn = async_nack

        await msg.nack_async(requeue=False)

        async_nack.assert_called_once_with(False)
        assert msg.is_settled

    async def test_reject_async_with_async_fn(self) -> None:
        """Line 165: reject_async calls _reject_async_fn when set."""
        from unittest.mock import AsyncMock

        msg = _make_message()
        async_reject = AsyncMock()
        msg._reject_async_fn = async_reject

        await msg.reject_async(requeue=True)

        async_reject.assert_called_once_with(True)
        assert msg.is_settled


class TestAsyncAlreadySettled:
    async def test_nack_async_no_op_when_settled(self) -> None:
        """Line 155: nack_async returns early if already settled."""
        from unittest.mock import AsyncMock

        msg = _make_message()
        # Settle the message first
        msg._ack_fn = MagicMock()
        msg.ack()
        assert msg.is_settled

        nack_fn = AsyncMock()
        msg._nack_async_fn = nack_fn
        await msg.nack_async()  # should be a no-op

        nack_fn.assert_not_called()

    async def test_reject_async_no_op_when_settled(self) -> None:
        """Line 165: reject_async returns early if already settled."""
        from unittest.mock import AsyncMock

        msg = _make_message()
        # Settle the message first
        msg._ack_fn = MagicMock()
        msg.ack()
        assert msg.is_settled

        reject_fn = AsyncMock()
        msg._reject_async_fn = reject_fn
        await msg.reject_async()  # should be a no-op

        reject_fn.assert_not_called()


# ── ack-failure propagation (Core-H4) ─────────────────────────────────────
# A failed settlement must RAISE and leave the message UNSETTLED, so the
# recovery loop can redeliver instead of silently swallowing the failure.


class TestAckFailurePropagation:
    def test_sync_ack_failure_raises_and_leaves_unsettled(self) -> None:
        msg = _make_message()

        def boom() -> None:
            raise RuntimeError("channel closed")

        msg._ack_fn = boom

        with pytest.raises(RuntimeError, match="channel closed"):
            msg.ack()

        # disposition stayed pending — recovery loop can redeliver
        assert msg.is_settled is False
        assert msg._disposition == "pending"

    def test_sync_nack_failure_raises_and_leaves_unsettled(self) -> None:
        msg = _make_message()

        def boom(_requeue: bool) -> None:
            raise RuntimeError("channel closed")

        msg._nack_fn = boom

        with pytest.raises(RuntimeError, match="channel closed"):
            msg.nack(requeue=True)

        assert msg.is_settled is False
        assert msg._disposition == "pending"

    def test_sync_reject_failure_raises_and_leaves_unsettled(self) -> None:
        msg = _make_message()

        def boom(_requeue: bool) -> None:
            raise RuntimeError("channel closed")

        msg._reject_fn = boom

        with pytest.raises(RuntimeError, match="channel closed"):
            msg.reject(requeue=False)

        assert msg.is_settled is False
        assert msg._disposition == "pending"

    def test_failed_ack_then_retry_succeeds(self) -> None:
        """After a failed ack leaves the message unsettled, a subsequent ack
        (e.g. on redelivery) can succeed and settle it."""
        msg = _make_message()
        calls: list[str] = []

        def flaky() -> None:
            calls.append("called")
            if len(calls) == 1:
                raise RuntimeError("transient")

        msg._ack_fn = flaky
        with pytest.raises(RuntimeError, match="transient"):
            msg.ack()
        assert msg.is_settled is False

        # Second attempt succeeds → disposition set
        msg.ack()
        assert msg.is_settled is True
        assert msg._disposition == "acked"

    @pytest.mark.asyncio
    async def test_async_ack_failure_raises_and_leaves_unsettled(self) -> None:
        msg = _make_message()

        async def boom() -> None:
            raise RuntimeError("channel closed")

        msg._ack_async_fn = boom

        with pytest.raises(RuntimeError, match="channel closed"):
            await msg.ack_async()

        assert msg.is_settled is False
        assert msg._disposition == "pending"

    @pytest.mark.asyncio
    async def test_async_nack_failure_raises_and_leaves_unsettled(self) -> None:
        msg = _make_message()

        async def boom(_requeue: bool) -> None:
            raise RuntimeError("channel closed")

        msg._nack_async_fn = boom

        with pytest.raises(RuntimeError, match="channel closed"):
            await msg.nack_async(requeue=True)

        assert msg.is_settled is False
        assert msg._disposition == "pending"

    @pytest.mark.asyncio
    async def test_async_reject_failure_raises_and_leaves_unsettled(self) -> None:
        msg = _make_message()

        async def boom(_requeue: bool) -> None:
            raise RuntimeError("channel closed")

        msg._reject_async_fn = boom

        with pytest.raises(RuntimeError, match="channel closed"):
            await msg.reject_async(requeue=False)

        assert msg.is_settled is False
        assert msg._disposition == "pending"


class TestAsyncNoSettlementFn:
    @pytest.mark.asyncio
    async def test_ack_async_no_fn_raises(self) -> None:
        """ack_async raises RuntimeError when no async or sync fn is set."""
        msg = RabbitMessage(body=b"x", routing_key="q")
        with pytest.raises(RuntimeError, match="Cannot async-ack"):
            await msg.ack_async()

    @pytest.mark.asyncio
    async def test_nack_async_no_fn_raises(self) -> None:
        """nack_async raises RuntimeError when no async or sync fn is set."""
        msg = RabbitMessage(body=b"x", routing_key="q")
        with pytest.raises(RuntimeError, match="Cannot async-nack"):
            await msg.nack_async()

    @pytest.mark.asyncio
    async def test_reject_async_no_fn_raises(self) -> None:
        """reject_async raises RuntimeError when no async or sync fn is set."""
        msg = RabbitMessage(body=b"x", routing_key="q")
        with pytest.raises(RuntimeError, match="Cannot async-reject"):
            await msg.reject_async()
