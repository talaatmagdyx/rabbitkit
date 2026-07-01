"""Tests for rpc.py — RPCClient and AsyncRPCClient."""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import Future as _CFuture
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import PublishOutcome, PublishStatus
from rabbitkit.rpc import (
    AsyncRPCClient,
    RPCClient,
    RPCClientClosed,
    RPCTimeoutError,
    _AsyncFutureSink,
    _FutureSink,
)

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
        assert client._router.max_pending == 100
        assert not client._consuming

    def test_call_publishes_and_receives(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

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
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport)

        with pytest.raises(RPCTimeoutError) as exc_info:
            client.call("rpc.queue", b"test", timeout=0.1)

        assert exc_info.value.timeout == 0.1
        assert exc_info.value.correlation_id is not None

    def test_call_sets_reply_to(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
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
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
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
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
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
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport, max_pending=1)

        # Fill up pending
        # We need to add a pending call manually
        client._router._pending["existing"] = _FutureSink(_CFuture())
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

        client._router._pending["cid-1"] = _FutureSink(_CFuture())

        client.close()

        assert client._router.pending_count() == 0

    def test_late_reply_discarded(self) -> None:
        transport = MagicMock()
        transport.consume = MagicMock(return_value="reply-tag")
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

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


# ── C2: reply-consumer must use no_ack + no-declare (amq.rabbitmq.reply-to) ──


class TestRPCClientReplyConsumerContract:
    """C2: amq.rabbitmq.reply-to rejects Queue.Declare and requires a no-ack
    consumer. RPCClient must request both via transport.consume()."""

    def test_ensure_consuming_passes_no_ack_and_declare_false(self) -> None:
        transport = MagicMock()
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport)
        client._ensure_consuming()

        transport.consume.assert_called_once()
        call = transport.consume.call_args
        assert call.kwargs["queue"] == "amq.rabbitmq.reply-to"
        assert call.kwargs["no_ack"] is True
        assert call.kwargs["declare"] is False


class TestAsyncRPCClientReplyConsumerContract:
    """Async counterpart of :class:`TestRPCClientReplyConsumerContract`."""

    @pytest.mark.asyncio
    async def test_ensure_consuming_passes_no_ack_and_declare_false(self) -> None:
        transport = AsyncMock()
        transport.consume = AsyncMock(return_value="reply-tag")

        client = AsyncRPCClient(transport)
        await client._ensure_consuming()

        transport.consume.assert_called_once()
        call = transport.consume.call_args
        assert call.kwargs["queue"] == "amq.rabbitmq.reply-to"
        assert call.kwargs["no_ack"] is True
        assert call.kwargs["declare"] is False


# ── AsyncRPCClient ──────────────────────────────────────────────────────


class TestAsyncRPCClient:
    @pytest.mark.asyncio
    async def test_construction(self) -> None:
        transport = AsyncMock()
        client = AsyncRPCClient(transport)
        assert client._router.max_pending == 100
        assert not client._consuming

    @pytest.mark.asyncio
    async def test_call_publishes_and_receives(self) -> None:
        transport = AsyncMock()
        transport.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

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
        transport.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        transport.consume = AsyncMock(return_value="reply-tag")

        client = AsyncRPCClient(transport)

        with pytest.raises(RPCTimeoutError) as exc_info:
            await client.call("rpc.queue", b"test", timeout=0.1)

        assert exc_info.value.timeout == 0.1

    @pytest.mark.asyncio
    async def test_call_sets_reply_to(self) -> None:
        transport = AsyncMock()
        transport.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
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
        transport.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
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
        transport.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        transport.consume = AsyncMock(return_value="reply-tag")

        client = AsyncRPCClient(transport, max_pending=1)

        # Fill up pending manually
        loop = asyncio.get_running_loop()
        future: asyncio.Future[RabbitMessage] = loop.create_future()
        client._router._pending["existing"] = _AsyncFutureSink(future)
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
        client._router._pending["cid-1"] = _AsyncFutureSink(future)

        await client.close()

        assert client._router.pending_count() == 0
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


# ── dedicated connection + close error (C-7, M-P6) ──────────────────────


class TestRPCClientDedicatedConnection:
    def test_call_pumps_dedicated_connection_and_receives_reply(self) -> None:
        """C-7: a dedicated reply connection is pumped while waiting for a reply."""
        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        consume_callbacks: list = []

        def capture_consume(queue, callback, **kwargs):
            consume_callbacks.append(callback)
            return "reply-tag"

        transport.consume = capture_consume

        connection = MagicMock()
        pending_replies: dict[str, RabbitMessage] = {}

        def process_data(time_limit: float = 0.0) -> None:
            # Deliver any queued reply to the registered consume callback.
            if not consume_callbacks or not pending_replies:
                return
            cb = consume_callbacks[0]
            for cid, resp in list(pending_replies.items()):
                cb(resp)
                pending_replies.pop(cid, None)

        connection.process_data_events = MagicMock(side_effect=process_data)

        client = RPCClient(transport, reply_connection=connection)

        result: dict = {}

        def run() -> None:
            try:
                r = client.call("rpc.queue", b"request", timeout=5.0)
                result["resp"] = r
            except BaseException as exc:
                result["exc"] = exc

        t = threading.Thread(target=run)
        t.start()
        # Let the call publish and start pumping.
        time.sleep(0.1)
        envelope = transport.publish.call_args[0][0]
        cid = envelope.correlation_id
        assert cid is not None
        pending_replies[cid] = _make_response(cid, body=b"reply-body")

        t.join(timeout=5.0)
        assert "exc" not in result, result
        assert result["resp"].body == b"reply-body"
        assert connection.process_data_events.called


class TestRPCClientClosed:
    def test_call_raises_rpc_client_closed_when_closed_midflight(self) -> None:
        """M-P6: a call closed while in-flight raises RPCClientClosed, not AttributeError."""
        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport)
        client._consuming = True  # avoid re-registering the consumer

        result: dict = {}

        def run() -> None:
            try:
                client.call("rpc.queue", b"request", timeout=5.0)
            except RPCClientClosed as exc:
                result["exc"] = exc
            except BaseException as exc:
                result["unexpected"] = exc

        t = threading.Thread(target=run)
        t.start()
        time.sleep(0.1)  # let it publish and block on the event
        client.close()  # resolves the waiter with RPCClientClosed
        t.join(timeout=5.0)

        assert isinstance(result.get("exc"), RPCClientClosed)
        assert "unexpected" not in result, result

    def test_close_sets_error_on_pending(self) -> None:
        """close() resolves pending waiters with RPCClientClosed."""
        transport = MagicMock()
        client = RPCClient(transport)
        fut: _CFuture = _CFuture()
        client._router._pending["cid-1"] = _FutureSink(fut)

        client.close()

        assert fut.done()
        assert isinstance(fut.exception(), RPCClientClosed)
        assert client._router.pending_count() == 0


# -- Low: close() closes the dedicated reply connection when owned ---------


class TestRPCClientCloseReplyConnection:
    """Low: RPCClient.close() closes the dedicated reply connection only when
    close_reply_connection=True (ownership transferred); otherwise the caller
    keeps ownership and the connection is NOT closed by close().
    """

    def test_default_does_not_close_reply_connection(self) -> None:
        transport = MagicMock()
        connection = MagicMock()
        client = RPCClient(transport, reply_connection=connection)
        assert client._close_reply_connection is False

        client.close()

        connection.close.assert_not_called()  # caller retains ownership

    def test_close_reply_connection_true_closes_it(self) -> None:
        transport = MagicMock()
        connection = MagicMock()
        client = RPCClient(transport, reply_connection=connection, close_reply_connection=True)
        assert client._close_reply_connection is True

        client.close()

        connection.close.assert_called_once_with()  # client owned it

    def test_close_reply_connection_true_without_connection_is_noop(self) -> None:
        transport = MagicMock()
        client = RPCClient(transport, reply_connection=None, close_reply_connection=True)
        # No reply_connection supplied so close() must not raise.
        client.close()

    def test_close_reply_connection_swallows_close_exception(self) -> None:
        transport = MagicMock()
        connection = MagicMock()
        connection.close.side_effect = RuntimeError("already closed")
        client = RPCClient(transport, reply_connection=connection, close_reply_connection=True)

        # Must not raise even though the dedicated connection close() fails.
        client.close()
        connection.close.assert_called_once_with()


# -- L-8: RPC reply body size cap -----------------------------------------


class TestRPCReplySizeCap:
    def test_sync_oversized_reply_raises_reply_too_large(self) -> None:
        from rabbitkit.rpc import ReplyTooLargeError

        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        consume_callback = None

        def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume
        client = RPCClient(transport, max_reply_bytes=8)

        import threading
        import time

        result_holder = [None]
        exc_holder = [None]

        def thread_fn() -> None:
            try:
                result_holder[0] = client.call("rpc.q", b"req", timeout=5.0)
            except Exception as e:
                exc_holder[0] = e

        t = threading.Thread(target=thread_fn)
        t.start()
        time.sleep(0.1)
        cid = transport.publish.call_args[0][0].correlation_id
        # Oversized reply body.
        consume_callback(_make_response(cid, body=b"x" * 100))
        t.join(timeout=5.0)

        assert result_holder[0] is None
        assert isinstance(exc_holder[0], ReplyTooLargeError)
        assert exc_holder[0].size == 100
        assert exc_holder[0].limit == 8

    def test_sync_within_cap_returns_result(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        consume_callback = None

        def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume
        client = RPCClient(transport, max_reply_bytes=1024)

        import threading
        import time

        result_holder = [None]
        exc_holder = [None]

        def thread_fn() -> None:
            try:
                result_holder[0] = client.call("rpc.q", b"req", timeout=5.0)
            except Exception as e:
                exc_holder[0] = e

        t = threading.Thread(target=thread_fn)
        t.start()
        time.sleep(0.1)
        cid = transport.publish.call_args[0][0].correlation_id
        consume_callback(_make_response(cid, body=b"ok"))
        t.join(timeout=5.0)

        assert exc_holder[0] is None
        assert result_holder[0] is not None
        assert result_holder[0].body == b"ok"

    @pytest.mark.asyncio
    async def test_async_oversized_reply_raises_reply_too_large(self) -> None:
        from rabbitkit.rpc import ReplyTooLargeError

        transport = AsyncMock()
        transport.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        consume_callback = None

        async def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume
        client = AsyncRPCClient(transport, max_reply_bytes=8)

        async def simulate_response() -> None:
            await asyncio.sleep(0.05)
            cid = transport.publish.call_args[0][0].correlation_id
            await consume_callback(_make_response(cid, body=b"x" * 100))

        task = asyncio.create_task(simulate_response())
        with pytest.raises(ReplyTooLargeError, match="exceeds"):
            await client.call("rpc.q", b"req", timeout=5.0)
        await task

    @pytest.mark.asyncio
    async def test_async_within_cap_returns_result(self) -> None:
        transport = AsyncMock()
        transport.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        consume_callback = None

        async def capture_consume(queue, callback, **kwargs):
            nonlocal consume_callback
            consume_callback = callback
            return "reply-tag"

        transport.consume = capture_consume
        client = AsyncRPCClient(transport, max_reply_bytes=1024)

        async def simulate_response() -> None:
            await asyncio.sleep(0.05)
            cid = transport.publish.call_args[0][0].correlation_id
            await consume_callback(_make_response(cid, body=b"ok"))

        task = asyncio.create_task(simulate_response())
        result = await client.call("rpc.q", b"req", timeout=5.0)
        await task
        assert result.body == b"ok"


# ── L-4: call() must clean up _pending when process_data_events raises ──────


class TestRPCClientPumpExceptionCleanup:
    """L-4: if ``process_data_events`` raises while pumping the dedicated reply
    connection, the ``_pending`` entry for the in-flight call is popped so it
    does not leak and eventually exhaust ``max_pending``.
    """

    def test_process_data_events_raising_cleans_pending(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        transport.consume = MagicMock(return_value="reply-tag")

        connection = MagicMock()

        def boom(time_limit: float = 0.0) -> None:
            raise RuntimeError("io pump exploded")

        connection.process_data_events = MagicMock(side_effect=boom)

        client = RPCClient(transport, reply_connection=connection, max_pending=5)

        with pytest.raises(RuntimeError, match="io pump exploded"):
            client.call("rpc.queue", b"request", timeout=5.0)

        # L-4: the pending entry must be cleaned up, not left to leak.
        assert client._router.pending_count() == 0

    def test_repeated_failing_pump_does_not_exhaust_max_pending(self) -> None:
        """A leak would fill _pending up to max_pending and then raise
        'Max pending' instead of the pump error — the L-4 finally prevents that."""
        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        transport.consume = MagicMock(return_value="reply-tag")

        connection = MagicMock()

        def boom(time_limit: float = 0.0) -> None:
            raise RuntimeError("io pump exploded")

        connection.process_data_events = MagicMock(side_effect=boom)

        client = RPCClient(transport, reply_connection=connection, max_pending=2)

        for _ in range(5):
            with pytest.raises(RuntimeError, match="io pump exploded"):
                client.call("rpc.queue", b"request", timeout=5.0)

        # None leaked — every call cleaned up its own _pending entry.
        assert client._router.pending_count() == 0
        transport.consume.assert_called_once()  # consumer registered only once


# ── L-5: RPCClient._closed guard ------------------------------------------------


class TestRPCClientClosedGuard:
    """L-5: after ``close()``, ``call()`` raises ``RPCClientClosed`` instead of
    silently re-registering a consumer on the torn-down client.
    """

    def test_call_after_close_raises_rpc_client_closed(self) -> None:
        transport = MagicMock()
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport)
        client.close()

        with pytest.raises(RPCClientClosed):
            client.call("rpc.queue", b"request", timeout=1.0)

        # The consumer was NOT re-registered on the closed client.
        transport.consume.assert_not_called()

    def test_ensure_consuming_after_close_raises(self) -> None:
        transport = MagicMock()
        transport.consume = MagicMock(return_value="reply-tag")

        client = RPCClient(transport)
        client.close()

        with pytest.raises(RPCClientClosed):
            client._ensure_consuming()
        transport.consume.assert_not_called()

# ── R4: _ReplyRouter / sink-based routing ----------------------------------

class TestReplyRouter:
    """R4: shared reply routing logic lives in _ReplyRouter."""

    def test_register_and_is_full(self) -> None:
        from rabbitkit.rpc import _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=None, max_pending=2)
        assert not router.is_full()
        router.register("a", _FutureSink(_CFuture()))
        router.register("b", _FutureSink(_CFuture()))
        assert router.is_full()

    def test_pop_removes_entry(self) -> None:
        from rabbitkit.rpc import _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=None, max_pending=10)
        router.register("cid", _FutureSink(_CFuture()))
        assert router.pop("cid") is not None
        assert router.pending_count() == 0

    def test_resolve_sets_result(self) -> None:
        from rabbitkit.rpc import _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=None, max_pending=10)
        fut: _CFuture = _CFuture()
        router.register("cid-1", _FutureSink(fut))
        router.resolve(_make_response("cid-1", body=b"ok"))
        assert fut.done()
        assert fut.result().body == b"ok"

    def test_resolve_unknown_cid_discarded(self) -> None:
        from rabbitkit.rpc import _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=None, max_pending=10)
        # Should not raise on unknown correlation_id.
        router.resolve(_make_response("nope"))
        assert router.pending_count() == 0

    def test_resolve_without_correlation_id_discarded(self) -> None:
        from rabbitkit.rpc import _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=None, max_pending=10)
        router.register("cid-1", _FutureSink(_CFuture()))
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
        router.resolve(response)
        # Entry still present — reply was discarded, not resolved.
        assert router.pending_count() == 1

    def test_resolve_oversized_sets_exception(self) -> None:
        from rabbitkit.rpc import ReplyTooLargeError, _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=8, max_pending=10)
        fut: _CFuture = _CFuture()
        router.register("cid-1", _FutureSink(fut))
        router.resolve(_make_response("cid-1", body=b"x" * 100))
        assert fut.done()
        assert isinstance(fut.exception(), ReplyTooLargeError)

    def test_resolve_within_cap_sets_result(self) -> None:
        from rabbitkit.rpc import _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=1024, max_pending=10)
        fut: _CFuture = _CFuture()
        router.register("cid-1", _FutureSink(fut))
        router.resolve(_make_response("cid-1", body=b"ok"))
        assert fut.result().body == b"ok"

    def test_resolve_already_done_is_noop(self) -> None:
        from rabbitkit.rpc import _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=None, max_pending=10)
        fut: _CFuture = _CFuture()
        fut.set_result(_make_response("cid-1", body=b"first"))
        router.register("cid-1", _FutureSink(fut))
        # Second resolve should be a no-op — result stays "first".
        router.resolve(_make_response("cid-1", body=b"second"))
        assert fut.result().body == b"first"

    def test_close_all_sets_exception(self) -> None:
        from rabbitkit.rpc import _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=None, max_pending=10)
        fut: _CFuture = _CFuture()
        router.register("cid-1", _FutureSink(fut))
        router.close_all(RPCClientClosed("closed"))
        assert fut.done()
        assert isinstance(fut.exception(), RPCClientClosed)
        assert router.pending_count() == 0

    def test_cancel_all_cancels_sinks(self) -> None:
        from rabbitkit.rpc import _ReplyRouter

        router = _ReplyRouter(max_reply_bytes=None, max_pending=10)
        fut: _CFuture = _CFuture()
        router.register("cid-1", _FutureSink(fut))
        router.cancel_all()
        assert fut.cancelled() or fut.done()
        assert router.pending_count() == 0

class TestFutureSink:
    """R4: _FutureSink wraps concurrent.futures.Future."""

    def test_set_result_and_done(self) -> None:
        fut: _CFuture = _CFuture()
        sink = _FutureSink(fut)
        assert not sink.done()
        sink.set_result(_make_response("cid"))
        assert sink.done()
        assert fut.result().body == b"response"

    def test_set_exception(self) -> None:
        fut: _CFuture = _CFuture()
        sink = _FutureSink(fut)
        sink.set_exception(ValueError("boom"))
        assert isinstance(fut.exception(), ValueError)

    def test_cancel(self) -> None:
        fut: _CFuture = _CFuture()
        sink = _FutureSink(fut)
        assert sink.cancel()
        assert fut.cancelled()

class TestAsyncFutureSink:
    """R4: _AsyncFutureSink wraps asyncio.Future."""

    @pytest.mark.asyncio
    async def test_set_result_and_done(self) -> None:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[RabbitMessage] = loop.create_future()
        sink = _AsyncFutureSink(fut)
        assert not sink.done()
        sink.set_result(_make_response("cid"))
        assert sink.done()
        assert fut.result().body == b"response"

    @pytest.mark.asyncio
    async def test_cancel(self) -> None:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[RabbitMessage] = loop.create_future()
        sink = _AsyncFutureSink(fut)
        assert sink.cancel()
        assert fut.cancelled()

class TestRPCClientUsesConcurrentFuture:
    """R4: sync client uses concurrent.futures.Future, not _PendingCall."""

    def test_pending_call_class_removed(self) -> None:
        import rabbitkit.rpc as rpc_mod

        assert not hasattr(rpc_mod, "_PendingCall")

    def test_sync_client_uses_future_sink(self) -> None:
        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        transport.consume = MagicMock(return_value="reply-tag")
        client = RPCClient(transport, max_reply_bytes=1024)
        # The router holds _Sink entries, not _PendingCall.
        assert client._router.max_reply_bytes == 1024


# ── Abstract base classes — NotImplementedError coverage ──────────────────


class TestReplyConnectionAbstract:
    """Line 74: _ReplyConnection.process_data_events raises NotImplementedError."""

    def test_process_data_events_raises_not_implemented(self) -> None:
        from rabbitkit.rpc import _ReplyConnection

        conn = _ReplyConnection()
        with pytest.raises(NotImplementedError):
            conn.process_data_events()


class TestSinkAbstractMethods:
    """Lines 93, 96, 99, 102: _Sink abstract methods each raise NotImplementedError."""

    def test_set_result_raises_not_implemented(self) -> None:
        from rabbitkit.rpc import _Sink

        sink = _Sink()
        with pytest.raises(NotImplementedError):
            sink.set_result(_make_response("cid"))

    def test_set_exception_raises_not_implemented(self) -> None:
        from rabbitkit.rpc import _Sink

        sink = _Sink()
        with pytest.raises(NotImplementedError):
            sink.set_exception(ValueError("boom"))

    def test_done_raises_not_implemented(self) -> None:
        from rabbitkit.rpc import _Sink

        sink = _Sink()
        with pytest.raises(NotImplementedError):
            sink.done()

    def test_cancel_raises_not_implemented(self) -> None:
        from rabbitkit.rpc import _Sink

        sink = _Sink()
        with pytest.raises(NotImplementedError):
            sink.cancel()


# ── Line 360: deadline exceeded in dedicated-connection pump loop ──────────


class TestRPCClientDeadlineExceeded:
    """Line 360: ``if remaining <= 0: break`` fires when the deadline has already
    passed before the first ``process_data_events`` call.
    """

    def test_deadline_already_passed_breaks_immediately(self) -> None:
        """Pass a tiny timeout so that by the time the pump loop checks
        ``remaining = deadline - time.monotonic()`` it is already <= 0,
        triggering the ``break`` on line 360."""
        transport = MagicMock()
        transport.publish = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        transport.consume = MagicMock(return_value="reply-tag")

        connection = MagicMock()

        call_count = [0]

        def slow_process(time_limit: float = 0.0) -> None:
            # Simulate a small delay so the deadline expires after the first call.
            call_count[0] += 1
            time.sleep(0.05)

        connection.process_data_events = MagicMock(side_effect=slow_process)

        client = RPCClient(transport, reply_connection=connection)

        # Use a very short timeout — deadline will pass during or right after
        # the first process_data_events call, so the loop's ``remaining <= 0``
        # branch triggers and the loop breaks without a reply.
        with pytest.raises(RPCTimeoutError):
            client.call("rpc.queue", b"request", timeout=0.001)

        # The pending entry must be cleaned up after timeout.
        assert client._router.pending_count() == 0


# ── Lines 445-448: _ensure_consuming transport.consume raises ──────────────


class TestEnsureConsumingTransportError:
    """Lines 445-448: when ``transport.consume`` raises, ``_starting`` is reset
    to ``False`` and the exception propagates.
    """

    def test_consume_exception_resets_starting_flag(self) -> None:
        transport = MagicMock()
        transport.consume.side_effect = RuntimeError("channel closed")

        client = RPCClient(transport)

        with pytest.raises(RuntimeError, match="channel closed"):
            client._ensure_consuming()

        # _starting must have been reset to False so a subsequent call can retry.
        assert client._starting is False
        # _consuming must NOT have been set (consumer never registered).
        assert client._consuming is False

    def test_consume_exception_allows_retry(self) -> None:
        """After a failed consume, the client can attempt again on the next call."""
        transport = MagicMock()
        transport.consume.side_effect = [
            RuntimeError("first failure"),
            "reply-tag",  # second call succeeds
        ]

        client = RPCClient(transport)

        # First attempt raises.
        with pytest.raises(RuntimeError, match="first failure"):
            client._ensure_consuming()

        assert client._starting is False
        assert client._consuming is False

        # Second attempt succeeds.
        client._ensure_consuming()
        assert client._consuming is True
        assert client._consumer_tag == "reply-tag"
