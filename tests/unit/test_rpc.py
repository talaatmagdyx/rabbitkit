"""Tests for rpc.py — RPCClient and AsyncRPCClient."""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import PublishOutcome, PublishStatus
from rabbitkit.rpc import AsyncRPCClient, RPCClient, RPCTimeoutError

# ── helpers ───────────────────────────────────────────────────────────────


def _make_response(correlation_id: str, body: bytes = b"response") -> RabbitMessage:
    """Create a mock response RabbitMessage."""
    return RabbitMessage(
        body=body,
        headers={},
        message_id="msg-resp",
        correlation_id=correlation_id,
        reply_to=None,
        content_type="application/json",
        content_encoding=None,
        type=None,
        app_id=None,
        routing_key="amq.rabbitmq.reply-to",
        exchange="",
        delivery_tag=1,
        redelivered=False,
        consumer_tag="tag",
    )


# ── RPCClient (sync) ────────────────────────────────────────────────────


class TestRPCClient:
    def test_construction(self) -> None:
        transport = MagicMock()
        client = RPCClient(transport)
        assert client._max_pending == 100
        assert not client._consuming

    def test_call_publishes_and_receives(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )

        # Capture the consume callback
        consume_callback = None

        def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume

        client = RPCClient(transport)

        # Run the call in a thread, and simulate the response
        def run_call():
            return client.call("rpc.queue", b'{"id": 1}', timeout=5.0)

        result_holder = [None]
        exc_holder = [None]

        def thread_fn():
            try:
                result_holder[0] = run_call()
            except Exception as e:
                exc_holder[0] = e

        t = threading.Thread(target=thread_fn)
        t.start()

        # Give the thread time to start and publish
        import time
        time.sleep(0.1)

        # Get the correlation_id from the published message
        publish_call = transport.publish.call_args
        envelope = publish_call[0][0]
        cid = envelope.correlation_id
        assert cid is not None

        # Simulate response
        response = _make_response(cid, body=b'{"result": "ok"}')
        consume_callback(response)

        t.join(timeout=5.0)
        assert exc_holder[0] is None
        assert result_holder[0] is not None
        assert result_holder[0].body == b'{"result": "ok"}'

    def test_call_timeout(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport)

        with pytest.raises(RPCTimeoutError) as exc_info:
            client.call("rpc.queue", b"test", timeout=0.1)

        assert exc_info.value.timeout == 0.1
        assert exc_info.value.correlation_id is not None

    def test_call_sets_reply_to(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport)

        # Will timeout, but we can check the published message
        try:
            client.call("rpc.queue", b"test", timeout=0.05)
        except RPCTimeoutError:
            pass

        call_args = transport.publish.call_args
        envelope = call_args[0][0]
        assert envelope.reply_to == "amq.rabbitmq.reply-to"
        assert envelope.correlation_id is not None
        assert envelope.routing_key == "rpc.queue"
        assert envelope.body == b"test"

    def test_call_with_custom_exchange(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport)

        try:
            client.call("rpc.queue", b"test", timeout=0.05, exchange="my-exchange")
        except RPCTimeoutError:
            pass

        envelope = transport.publish.call_args[0][0]
        assert envelope.exchange == "my-exchange"

    def test_call_with_headers(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport)

        try:
            client.call("rpc.queue", b"test", timeout=0.05, headers={"x-tenant": "abc"})
        except RPCTimeoutError:
            pass

        envelope = transport.publish.call_args[0][0]
        assert envelope.headers == {"x-tenant": "abc"}

    def test_max_pending_exceeded(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport, max_pending=1)

        # Fill up pending
        # We need to add a pending call manually
        from rabbitkit.rpc import _PendingCall
        client._pending["existing"] = _PendingCall()
        client._consuming = True

        with pytest.raises(RuntimeError, match="Max pending"):
            client.call("rpc.queue", b"test", timeout=0.05)

    def test_close(self) -> None:
        transport = MagicMock()
        transport.consume = MagicMock(return_value="reply-tag")
        transport.cancel_consumer = MagicMock()

        client = RPCClient(transport)
        client._consuming = True
        client._consumer_tag = "reply-tag"

        client.close()

        transport.cancel_consumer.assert_called_once_with("reply-tag")
        assert not client._consuming
        assert client._consumer_tag is None

    def test_close_clears_pending(self) -> None:
        transport = MagicMock()
        client = RPCClient(transport)

        from rabbitkit.rpc import _PendingCall
        pending = _PendingCall()
        client._pending["cid-1"] = pending

        client.close()

        assert len(client._pending) == 0

    def test_late_reply_discarded(self) -> None:
        transport = MagicMock()
        transport.consume = MagicMock(return_value="reply-tag")
        transport.publish = MagicMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )

        consume_callback = None

        def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume

        client = RPCClient(transport)
        client._ensure_consuming()

        # Send a reply with unknown correlation_id — should be discarded (no error)
        response = _make_response("unknown-cid")
        consume_callback(response)  # should not raise

    def test_reply_without_correlation_id_discarded(self) -> None:
        transport = MagicMock()

        consume_callback = None

        def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume

        client = RPCClient(transport)
        client._ensure_consuming()

        # Send a reply without correlation_id
        response = RabbitMessage(
            body=b"test",
            headers={},
            message_id=None,
            correlation_id=None,  # no correlation_id
            reply_to=None,
            content_type=None,
            content_encoding=None,
            type=None,
            app_id=None,
            routing_key="rk",
            exchange="",
            delivery_tag=1,
            redelivered=False,
            consumer_tag="tag",
        )
        consume_callback(response)  # should not raise


# ── AsyncRPCClient ──────────────────────────────────────────────────────


class TestAsyncRPCClient:
    @pytest.mark.asyncio
    async def test_construction(self) -> None:
        transport = AsyncMock()
        client = AsyncRPCClient(transport)
        assert client._max_pending == 100
        assert not client._consuming

    @pytest.mark.asyncio
    async def test_call_publishes_and_receives(self) -> None:
        transport = AsyncMock()
        transport.publish = AsyncMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )

        consume_callback = None

        async def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume

        client = AsyncRPCClient(transport)

        async def simulate_response():
            """Wait for publish, then send response."""
            await asyncio.sleep(0.05)
            # Get correlation_id from published message
            call_args = transport.publish.call_args
            envelope = call_args[0][0]
            cid = envelope.correlation_id
            response = _make_response(cid, body=b'{"result": "ok"}')
            await consume_callback(response)

        # Run call and response simulation concurrently
        task = asyncio.create_task(simulate_response())
        result = await client.call("rpc.queue", b'{"id": 1}', timeout=5.0)
        await task

        assert result.body == b'{"result": "ok"}'

    @pytest.mark.asyncio
    async def test_call_timeout(self) -> None:
        transport = AsyncMock()
        transport.publish = AsyncMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )
        transport.consume = AsyncMock(return_value="reply-tag")

        client = AsyncRPCClient(transport)

        with pytest.raises(RPCTimeoutError) as exc_info:
            await client.call("rpc.queue", b"test", timeout=0.1)

        assert exc_info.value.timeout == 0.1

    @pytest.mark.asyncio
    async def test_call_sets_reply_to(self) -> None:
        transport = AsyncMock()
        transport.publish = AsyncMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )
        transport.consume = AsyncMock(return_value="reply-tag")

        client = AsyncRPCClient(transport)

        try:
            await client.call("rpc.queue", b"test", timeout=0.05)
        except RPCTimeoutError:
            pass

        envelope = transport.publish.call_args[0][0]
        assert envelope.reply_to == "amq.rabbitmq.reply-to"
        assert envelope.correlation_id is not None
        assert envelope.routing_key == "rpc.queue"

    @pytest.mark.asyncio
    async def test_call_with_exchange_and_headers(self) -> None:
        transport = AsyncMock()
        transport.publish = AsyncMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )
        transport.consume = AsyncMock(return_value="reply-tag")

        client = AsyncRPCClient(transport)

        try:
            await client.call(
                "rpc.queue",
                b"test",
                timeout=0.05,
                exchange="my-ex",
                headers={"x-key": "val"},
            )
        except RPCTimeoutError:
            pass

        envelope = transport.publish.call_args[0][0]
        assert envelope.exchange == "my-ex"
        assert envelope.headers == {"x-key": "val"}

    @pytest.mark.asyncio
    async def test_max_pending_exceeded(self) -> None:
        transport = AsyncMock()
        transport.publish = AsyncMock(
            return_value=PublishOutcome(status=PublishStatus.CONFIRMED)
        )
        transport.consume = AsyncMock(return_value="reply-tag")

        client = AsyncRPCClient(transport, max_pending=1)

        # Fill up pending manually
        loop = asyncio.get_running_loop()
        future: asyncio.Future[RabbitMessage] = loop.create_future()
        client._pending["existing"] = future
        client._consuming = True

        with pytest.raises(RuntimeError, match="Max pending"):
            await client.call("rpc.queue", b"test", timeout=0.05)

    @pytest.mark.asyncio
    async def test_close(self) -> None:
        transport = AsyncMock()
        transport.cancel_consumer = AsyncMock()

        client = AsyncRPCClient(transport)
        client._consuming = True
        client._consumer_tag = "reply-tag"

        await client.close()

        transport.cancel_consumer.assert_called_once_with("reply-tag")
        assert not client._consuming
        assert client._consumer_tag is None

    @pytest.mark.asyncio
    async def test_close_cancels_pending_futures(self) -> None:
        transport = AsyncMock()
        client = AsyncRPCClient(transport)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[RabbitMessage] = loop.create_future()
        client._pending["cid-1"] = future

        await client.close()

        assert len(client._pending) == 0
        assert future.cancelled()

    @pytest.mark.asyncio
    async def test_late_reply_discarded(self) -> None:
        transport = AsyncMock()

        consume_callback = None

        async def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume

        client = AsyncRPCClient(transport)
        await client._ensure_consuming()

        # Send a reply with unknown correlation_id
        response = _make_response("unknown-cid")
        await consume_callback(response)  # should not raise

    @pytest.mark.asyncio
    async def test_reply_without_correlation_id_discarded(self) -> None:
        transport = AsyncMock()

        consume_callback = None

        async def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume

        client = AsyncRPCClient(transport)
        await client._ensure_consuming()

        response = RabbitMessage(
            body=b"test",
            headers={},
            message_id=None,
            correlation_id=None,
            reply_to=None,
            content_type=None,
            content_encoding=None,
            type=None,
            app_id=None,
            routing_key="rk",
            exchange="",
            delivery_tag=1,
            redelivered=False,
            consumer_tag="tag",
        )
        await consume_callback(response)  # should not raise


# ── RPCTimeoutError ─────────────────────────────────────────────────────


class TestRPCTimeoutError:
    def test_attributes(self) -> None:
        err = RPCTimeoutError("cid-123", 5.0)
        assert err.correlation_id == "cid-123"
        assert err.timeout == 5.0
        assert "cid-123" in str(err)
        assert "5.0" in str(err)

    def test_is_timeout_error(self) -> None:
        err = RPCTimeoutError("cid", 1.0)
        assert isinstance(err, TimeoutError)


class TestRPCClientCancelException:
    def test_close_swallows_cancel_consumer_exception(self) -> None:
        """Lines 141-142: cancel_consumer exception is logged, not raised."""
        from unittest.mock import patch

        from rabbitkit.rpc import RPCClient

        transport = MagicMock()
        transport.cancel_consumer.side_effect = RuntimeError("channel gone")
        client = RPCClient(transport)
        client._consumer_tag = "tag-1"

        with patch("rabbitkit.rpc.logger") as mock_logger:
            client.close()  # should not raise

        mock_logger.warning.assert_called_once()
        assert "Failed to cancel" in mock_logger.warning.call_args[0][0]


class TestAsyncRPCClientCancelException:
    async def test_close_swallows_cancel_consumer_exception(self) -> None:
        """Lines 288-289: async cancel_consumer exception is logged, not raised."""
        from unittest.mock import AsyncMock, patch

        from rabbitkit.rpc import AsyncRPCClient

        transport = MagicMock()
        transport.cancel_consumer = AsyncMock(side_effect=RuntimeError("channel gone"))
        client = AsyncRPCClient(transport)
        client._consumer_tag = "tag-1"

        with patch("rabbitkit.rpc.logger") as mock_logger:
            await client.close()  # should not raise

        mock_logger.warning.assert_called_once()
        assert "Failed to cancel" in mock_logger.warning.call_args[0][0]
