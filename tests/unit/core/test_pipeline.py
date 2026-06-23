"""Tests for core/pipeline.py — HandlerPipeline, ack behavior, result publishing."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.route import ResultPublisher, RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy, MessageEnvelope, PublishOutcome, PublishStatus

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


def _make_route(handler=None, ack_policy=AckPolicy.AUTO, result_publisher=None, **kwargs):
    if handler is None:

        def handler(body: bytes) -> None:
            pass

    defaults = {
        "name": "test-route",
        "queue": RabbitQueue(name="test-queue"),
        "exchange": RabbitExchange(name="test-exchange"),
        "handler": handler,
        "ack_policy": ack_policy,
        "result_publisher": result_publisher,
    }
    defaults.update(kwargs)
    return RouteDefinition(**defaults)


# ── AUTO ack policy ──────────────────────────────────────────────────────


class TestAutoAckPolicy:
    def test_success_acks(self) -> None:
        msg = _make_message()
        ack_fn, _, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        ack_fn.assert_called_once()
        assert msg._disposition == "acked"

    def test_transient_error_nacks_with_requeue(self) -> None:
        msg = _make_message()
        _, nack_fn, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise ConnectionResetError("lost connection")

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        nack_fn.assert_called_once_with(True)
        assert msg._disposition == "nacked"

    def test_permanent_error_rejects(self) -> None:
        msg = _make_message()
        _, _, reject_fn = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise ValueError("bad data")

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        reject_fn.assert_called_once_with(False)
        assert msg._disposition == "rejected"

    def test_unknown_error_rejects(self) -> None:
        """Unknown errors default to PERMANENT → reject."""
        msg = _make_message()
        _, _, reject_fn = _wire_sync(msg)

        class CustomError(Exception):
            pass

        def handler(body: bytes) -> None:
            raise CustomError("unknown")

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        reject_fn.assert_called_once_with(False)


# ── MANUAL ack policy ───────────────────────────────────────────────────


class TestManualAckPolicy:
    def test_handler_acks_manually(self) -> None:
        msg = _make_message()
        _, _, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler, ack_policy=AckPolicy.MANUAL)
        pipeline = HandlerPipeline()

        # MANUAL: pipeline does not auto-ack after success
        # Verify pipeline runs without error for MANUAL mode
        pipeline.process_sync(route, msg, publish_fn=None)

    def test_exception_in_manual_re_raises(self) -> None:
        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise RuntimeError("handler bug")

        route = _make_route(handler=handler, ack_policy=AckPolicy.MANUAL)
        pipeline = HandlerPipeline()

        with pytest.raises(RuntimeError, match="handler bug"):
            pipeline.process_sync(route, msg)

        # Message should NOT be settled (MANUAL mode)
        assert msg._disposition == "pending"


# ── NACK_ON_ERROR ack policy ────────────────────────────────────────────


class TestNackOnErrorPolicy:
    def test_success_acks(self) -> None:
        msg = _make_message()
        ack_fn, _, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler, ack_policy=AckPolicy.NACK_ON_ERROR)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        ack_fn.assert_called_once()

    def test_error_nacks_no_requeue(self) -> None:
        msg = _make_message()
        _, nack_fn, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise RuntimeError("fail")

        route = _make_route(handler=handler, ack_policy=AckPolicy.NACK_ON_ERROR)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        nack_fn.assert_called_once_with(False)


# ── ACK_FIRST policy ────────────────────────────────────────────────────


class TestAckFirstPolicy:
    def test_acks_before_handler(self) -> None:
        msg = _make_message()
        ack_fn, _, _ = _wire_sync(msg)
        ack_order: list[str] = []

        def handler(body: bytes) -> None:
            ack_order.append(f"handler:settled={msg.is_settled}")

        route = _make_route(handler=handler, ack_policy=AckPolicy.ACK_FIRST)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        # Ack was called before handler
        ack_fn.assert_called_once()
        assert ack_order == ["handler:settled=True"]

    def test_exception_after_ack_first(self) -> None:
        """Exception after ACK_FIRST — message already acked, no additional settlement."""
        msg = _make_message()
        ack_fn, nack_fn, reject_fn = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise RuntimeError("post-ack failure")

        route = _make_route(handler=handler, ack_policy=AckPolicy.ACK_FIRST)
        pipeline = HandlerPipeline()
        # Should not raise — ACK_FIRST already settled
        pipeline.process_sync(route, msg)

        ack_fn.assert_called_once()
        nack_fn.assert_not_called()
        reject_fn.assert_not_called()


# ── Exception-based ack control ─────────────────────────────────────────


class TestExceptionAckControl:
    def test_ack_message_exception(self) -> None:
        msg = _make_message()
        ack_fn, _, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise AckMessage()

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        ack_fn.assert_called_once()

    def test_nack_message_exception(self) -> None:
        msg = _make_message()
        _, nack_fn, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise NackMessage(requeue=True)

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        nack_fn.assert_called_once_with(True)

    def test_reject_message_exception(self) -> None:
        msg = _make_message()
        _, _, reject_fn = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise RejectMessage(requeue=False)

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        reject_fn.assert_called_once_with(False)


# ── Result publishing (Contract 5) ──────────────────────────────────────


class TestResultPublishing:
    def test_none_return_no_publish(self) -> None:
        msg = _make_message()
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> None:
            return None

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg, publish_fn=publish_fn)

        publish_fn.assert_not_called()

    def test_result_published_to_result_publisher(self) -> None:
        msg = _make_message()
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> bytes:
            return b"result"

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg, publish_fn=publish_fn)

        publish_fn.assert_called_once()
        envelope = publish_fn.call_args[0][0]
        assert envelope.body == b"result"
        assert envelope.routing_key == "result"
        assert envelope.exchange == "out"

    def test_reply_to_takes_precedence(self) -> None:
        msg = _make_message(reply_to="amq.rabbitmq.reply-to", correlation_id="corr-123")
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> bytes:
            return b"response"

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg, publish_fn=publish_fn)

        publish_fn.assert_called_once()
        envelope = publish_fn.call_args[0][0]
        assert envelope.routing_key == "amq.rabbitmq.reply-to"
        assert envelope.correlation_id == "corr-123"
        assert envelope.exchange == ""

    def test_no_publisher_no_reply_no_publish(self) -> None:
        msg = _make_message()
        _wire_sync(msg)
        publish_fn = MagicMock()

        def handler(body: bytes) -> bytes:
            return b"result"

        route = _make_route(handler=handler, result_publisher=None)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg, publish_fn=publish_fn)

        publish_fn.assert_not_called()

    def test_string_result_encoded(self) -> None:
        msg = _make_message()
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> str:
            return "hello"

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg, publish_fn=publish_fn)

        envelope = publish_fn.call_args[0][0]
        assert envelope.body == b"hello"

    def test_dict_result_json_encoded(self) -> None:
        msg = _make_message()
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> dict:
            return {"status": "ok"}

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg, publish_fn=publish_fn)

        envelope = publish_fn.call_args[0][0]
        assert json.loads(envelope.body) == {"status": "ok"}


# ── Async pipeline ───────────────────────────────────────────────────────


class TestAsyncPipeline:
    @pytest.mark.asyncio
    async def test_async_success_acks(self) -> None:
        msg = _make_message()
        ack_fn = MagicMock()
        msg._ack_fn = ack_fn

        async def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        ack_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_transient_error_nacks(self) -> None:
        msg = _make_message()
        nack_fn = MagicMock()
        msg._ack_fn = MagicMock()
        msg._nack_fn = nack_fn

        async def handler(body: bytes) -> None:
            raise ConnectionResetError("lost")

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        nack_fn.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_async_permanent_error_rejects(self) -> None:
        msg = _make_message()
        reject_fn = MagicMock()
        msg._ack_fn = MagicMock()
        msg._reject_fn = reject_fn

        async def handler(body: bytes) -> None:
            raise ValueError("bad")

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        reject_fn.assert_called_once_with(False)

    @pytest.mark.asyncio
    async def test_async_ack_first(self) -> None:
        msg = _make_message()
        ack_fn = MagicMock()
        msg._ack_fn = ack_fn
        settled_before_handler = None

        async def handler(body: bytes) -> None:
            nonlocal settled_before_handler
            settled_before_handler = msg.is_settled

        route = _make_route(handler=handler, ack_policy=AckPolicy.ACK_FIRST)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        assert settled_before_handler is True
        ack_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_result_published(self) -> None:
        msg = _make_message()
        msg._ack_fn = MagicMock()
        published: list[MessageEnvelope] = []

        async def mock_publish(envelope: MessageEnvelope) -> PublishOutcome:
            published.append(envelope)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        async def handler(body: bytes) -> bytes:
            return b"async-result"

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg, publish_fn=mock_publish)

        assert len(published) == 1
        assert published[0].body == b"async-result"

    @pytest.mark.asyncio
    async def test_async_exception_ack_control(self) -> None:
        msg = _make_message()
        nack_fn = MagicMock()
        msg._ack_fn = MagicMock()
        msg._nack_fn = nack_fn

        async def handler(body: bytes) -> None:
            raise NackMessage(requeue=True)

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        nack_fn.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_async_manual_exception_re_raises(self) -> None:
        msg = _make_message()
        msg._ack_fn = MagicMock()
        msg._nack_fn = MagicMock()

        async def handler(body: bytes) -> None:
            raise RuntimeError("async handler bug")

        route = _make_route(handler=handler, ack_policy=AckPolicy.MANUAL)
        pipeline = HandlerPipeline()

        with pytest.raises(RuntimeError, match="async handler bug"):
            await pipeline.process_async(route, msg)

        assert msg._disposition == "pending"


# ── Helpers for async settlement ─────────────────────────────────────────


def _wire_async(msg: RabbitMessage) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
    """Wire async settlement functions onto msg."""
    ack = AsyncMock()
    nack = AsyncMock()
    reject = AsyncMock()
    msg._ack_async_fn = ack
    msg._nack_async_fn = nack
    msg._reject_async_fn = reject
    return ack, nack, reject


# ── Async exception-based ack control (lines 156-166) ───────────────────


class TestAsyncExceptionAckControl:
    async def test_async_ack_message_exception(self) -> None:
        """Lines 157-158: except AckMessage in process_async."""
        msg = _make_message()
        ack, nack, reject = _wire_async(msg)

        def handler(body: bytes) -> None:
            raise AckMessage()

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        ack.assert_called_once()
        nack.assert_not_called()
        reject.assert_not_called()

    async def test_async_reject_message_exception(self) -> None:
        """Lines 165-166: except RejectMessage in process_async."""
        msg = _make_message()
        ack, nack, reject = _wire_async(msg)

        def handler(body: bytes) -> None:
            raise RejectMessage(requeue=False)

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        reject.assert_called_once_with(False)
        ack.assert_not_called()
        nack.assert_not_called()

    async def test_async_nack_message_exception(self) -> None:
        """NackMessage in process_async (existing line 162, complements the above)."""
        msg = _make_message()
        _ack, nack, _reject = _wire_async(msg)

        def handler(body: bytes) -> None:
            raise NackMessage(requeue=True)

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        nack.assert_called_once_with(True)


# ── Async exception handling policies (lines 471-481) ────────────────────


class TestAsyncExceptionHandling:
    async def test_async_already_settled_logs_warning(self) -> None:
        """Lines 472-473: settled message — warning logged, no re-raise."""
        msg = _make_message()
        _ack, _nack, _reject = _wire_async(msg)

        # Pre-settle the message before raising
        async def handler(body: bytes) -> None:
            await msg.ack_async()
            raise RuntimeError("error after settlement")

        route = _make_route(handler=handler, ack_policy=AckPolicy.AUTO)
        pipeline = HandlerPipeline()
        # Should NOT raise — message already settled
        await pipeline.process_async(route, msg)

        assert msg._disposition == "acked"

    async def test_async_manual_policy_reraises(self) -> None:
        """Lines 475-477: MANUAL policy — re-raises exception."""
        msg = _make_message()
        _ack, _nack, _reject = _wire_async(msg)

        def handler(body: bytes) -> None:
            raise ValueError("manual handler error")

        route = _make_route(handler=handler, ack_policy=AckPolicy.MANUAL)
        pipeline = HandlerPipeline()

        with pytest.raises(ValueError, match="manual handler error"):
            await pipeline.process_async(route, msg)

        assert msg._disposition == "pending"

    async def test_async_nack_on_error_policy(self) -> None:
        """Lines 479-481: NACK_ON_ERROR policy — nacks without requeue."""
        msg = _make_message()
        ack, nack, reject = _wire_async(msg)

        def handler(body: bytes) -> None:
            raise RuntimeError("handler failure")

        route = _make_route(handler=handler, ack_policy=AckPolicy.NACK_ON_ERROR)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        nack.assert_called_once_with(False)
        ack.assert_not_called()
        reject.assert_not_called()


# ── Async filter rejection (line 214) ────────────────────────────────────


class TestAsyncFilterRejection:
    async def test_async_filter_fn_rejects_message(self) -> None:
        """Line 214: filter_fn returns False in process_async → nack_async."""
        msg = _make_message()
        ack, nack, _reject = _wire_async(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler, filter_fn=lambda m: False)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        nack.assert_called_once_with(False)
        ack.assert_not_called()

    async def test_async_filter_fn_passes_message(self) -> None:
        """Filter_fn returns True → message is processed normally."""
        msg = _make_message()
        ack, _nack, _reject = _wire_async(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler, filter_fn=lambda m: True)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        ack.assert_called_once()


# ── DI cleanup exception paths (lines 196-199, 226-229) ──────────────────


class TestDICleanupExceptions:
    def test_sync_di_cleanup_exception_logged(self) -> None:
        """Lines 196-199: DI scope.cleanup() raises — error is logged, not re-raised."""
        from rabbitkit.di.resolver import DependencyScope

        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = {}

        pipeline = HandlerPipeline(di_resolver=mock_resolver)
        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            return None

        route = _make_route(handler=handler)

        error_calls: list[tuple] = []

        with patch.object(DependencyScope, "cleanup", side_effect=RuntimeError("leak!")):
            with patch("rabbitkit.core.pipeline.logger") as mock_logger:
                mock_logger.error = MagicMock(side_effect=lambda *a, **kw: error_calls.append(a))
                # Should NOT raise — exception is swallowed inside the finally block
                pipeline.process_sync(route, msg)

        assert any("DI generator cleanup raised" in str(call) for call in error_calls)

    async def test_async_di_cleanup_exception_logged(self) -> None:
        """Lines 226-229: DI scope.cleanup_async() raises — error is logged, not re-raised."""
        from rabbitkit.di.resolver import DependencyScope

        mock_resolver = MagicMock()
        mock_resolver.resolve_async = AsyncMock(return_value={})

        pipeline = HandlerPipeline(di_resolver=mock_resolver)
        msg = _make_message()
        _wire_async(msg)

        def handler(body: bytes) -> None:
            return None

        route = _make_route(handler=handler)

        error_calls: list[tuple] = []

        with patch.object(
            DependencyScope,
            "cleanup_async",
            new_callable=AsyncMock,
            side_effect=RuntimeError("async leak!"),
        ):
            with patch("rabbitkit.core.pipeline.logger") as mock_logger:
                mock_logger.error = MagicMock(side_effect=lambda *a, **kw: error_calls.append(a))
                # Should NOT raise — exception is swallowed inside the finally block
                await pipeline.process_async(route, msg)

        assert any("DI generator cleanup raised" in str(call) for call in error_calls)


# ── _resolve_params fallback paths (lines 264-274) ───────────────────────


class TestResolveParamsFallback:
    def test_resolve_params_body_then_default_skipped(self) -> None:
        """Lines 267: param with default is skipped after body is injected."""
        pipeline = HandlerPipeline()
        msg = _make_message()
        _wire_sync(msg)

        results: list[object] = []

        def handler(body: bytes, flag: bool = True) -> None:
            results.append((body, flag))

        route = _make_route(handler=handler)
        pipeline.process_sync(route, msg)

        # body is injected, flag uses its default
        assert len(results) == 1
        assert results[0][0] == b'{"id": 1}'
        assert results[0][1] is True  # default used

    def test_resolve_params_fallback_to_message_for_extra_param(self) -> None:
        """Lines 271: extra param without default falls back to message injection."""
        pipeline = HandlerPipeline()
        msg = _make_message()
        _wire_sync(msg)

        captured: list[object] = []

        def handler(body: bytes, extra) -> None:  # type: ignore[no-untyped-def]
            captured.append(extra)

        # Need to bypass the DI validator — create route directly without DI resolver
        route = _make_route(handler=handler)
        pipeline.process_sync(route, msg)

        # extra param should receive the message as fallback
        assert len(captured) == 1
        assert captured[0] is msg


# ── Result publishing failure paths (lines 289, 301, 306, 324, 347, 369) ─


class TestResultPublishingFailure:
    def test_sync_publish_failure_logs_warning(self) -> None:
        """Lines 346-352: publish_fn returns non-ok outcome — warning logged."""
        msg = _make_message()
        _wire_sync(msg)

        failed_outcome = PublishOutcome(status=PublishStatus.ERROR)
        publish_fn = MagicMock(return_value=failed_outcome)

        def handler(body: bytes) -> bytes:
            return b"result"

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        warning_calls: list[tuple] = []
        with patch("rabbitkit.core.pipeline.logger") as mock_logger:
            mock_logger.warning = MagicMock(side_effect=lambda *a, **kw: warning_calls.append(a))
            pipeline.process_sync(route, msg, publish_fn=publish_fn)

        assert any("Result publish failed" in str(call) for call in warning_calls)

    async def test_async_publish_failure_logs_warning(self) -> None:
        """Lines 367-374: async publish_fn returns non-ok outcome — warning logged."""
        msg = _make_message()
        _wire_async(msg)

        failed_outcome = PublishOutcome(status=PublishStatus.ERROR)

        async def publish_fn(envelope: MessageEnvelope) -> PublishOutcome:
            return failed_outcome

        def handler(body: bytes) -> bytes:
            return b"result"

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        warning_calls: list[tuple] = []
        with patch("rabbitkit.core.pipeline.logger") as mock_logger:
            mock_logger.warning = MagicMock(side_effect=lambda *a, **kw: warning_calls.append(a))
            await pipeline.process_async(route, msg, publish_fn=publish_fn)

        assert any("Result publish failed" in str(call) for call in warning_calls)


# ── _build_result_envelope with result_publisher (line 391) ──────────────


class TestBuildResultEnvelope:
    def test_build_result_envelope_with_result_publisher(self) -> None:
        """Line 406-412: result_publisher path in _build_result_envelope."""
        from rabbitkit.core.topology import RabbitExchange

        exchange = RabbitExchange(name="results")
        rp = ResultPublisher(exchange=exchange, routing_key="output.key")
        route = _make_route(result_publisher=rp)

        pipeline = HandlerPipeline()
        msg = _make_message()

        envelope = pipeline._build_result_envelope(route, msg, b"result-body")
        assert envelope is not None
        assert envelope.routing_key == "output.key"
        assert envelope.exchange == "results"
        assert envelope.body == b"result-body"

    def test_build_result_envelope_no_publisher_no_reply_to(self) -> None:
        """Lines 413-414: no result_publisher and no reply_to → None."""
        route = _make_route(result_publisher=None)
        pipeline = HandlerPipeline()
        msg = _make_message()

        envelope = pipeline._build_result_envelope(route, msg, b"some-body")
        assert envelope is None


# ── _serialize_result paths (lines 422-432) ──────────────────────────────


class TestSerializeResult:
    def test_serialize_result_message_envelope(self) -> None:
        """Line 423: result is a MessageEnvelope — return envelope.body."""
        pipeline = HandlerPipeline()
        route = _make_route()
        envelope = MessageEnvelope(routing_key="out", body=b"envelope-body", exchange="")
        serialized = pipeline._serialize_result(route, envelope)
        assert serialized == b"envelope-body"

    def test_serialize_result_json_fallback(self) -> None:
        """Line 427-432: no serializer, non-bytes/str/envelope → JSON fallback."""
        pipeline = HandlerPipeline()  # no serializer
        route = _make_route()
        result = {"key": "value"}
        serialized = pipeline._serialize_result(route, result)
        assert json.loads(serialized) == {"key": "value"}

    def test_serialize_result_with_serializer(self) -> None:
        """Lines 425-427: serializer path — calls serializer.encode()."""
        mock_serializer = MagicMock()
        mock_serializer.encode.return_value = b"serialized"
        pipeline = HandlerPipeline(serializer=mock_serializer)
        route = _make_route()
        serialized = pipeline._serialize_result(route, {"data": 1})
        assert serialized == b"serialized"
        mock_serializer.encode.assert_called_once()


# ── _get_body_type edge cases (lines 263-274) ────────────────────────────
# NOTE: Handlers are imported from _pipeline_handlers.py which does NOT have
# `from __future__ import annotations`, so inspect.signature() returns real
# type objects (not strings), allowing ann-identity comparisons to work.


class TestGetBodyType:
    def test_empty_annotation_skipped(self) -> None:
        """Line 264: parameter with no annotation (Parameter.empty) is skipped."""
        from tests.unit.core._pipeline_handlers import handler_no_annotation

        pipeline = HandlerPipeline()
        route = _make_route(handler=handler_no_annotation)
        result = pipeline._get_body_type(route)
        assert result is None

    def test_rabbit_message_annotation_skipped(self) -> None:
        """Line 267: parameter annotated RabbitMessage is skipped."""
        from tests.unit.core._pipeline_handlers import handler_rabbit_message

        pipeline = HandlerPipeline()
        route = _make_route(handler=handler_rabbit_message)
        result = pipeline._get_body_type(route)
        assert result is None

    def test_annotated_type_metadata_skipped(self) -> None:
        """Line 271: parameter with __metadata__ (Annotated type) is skipped."""
        from tests.unit.core._pipeline_handlers import handler_annotated_param

        pipeline = HandlerPipeline()
        route = _make_route(handler=handler_annotated_param)
        result = pipeline._get_body_type(route)
        # Annotated[str, ...] has __metadata__ → skipped → returns None
        assert result is None

    def test_no_parameters_returns_none(self) -> None:
        """Line 274: handler with no parameters → returns None."""
        pipeline = HandlerPipeline()

        def handler() -> None:
            pass

        route = _make_route(handler=handler)
        result = pipeline._get_body_type(route)
        assert result is None


# ── _resolve_params with RabbitMessage annotation (line 301) ─────────────
# NOTE: Uses _pipeline_handlers module (no future annotations) so
# ann is RabbitMessage comparison works correctly.


class TestResolveParamsRabbitMessageAnnotation:
    def test_rabbit_message_param_injected(self) -> None:
        """Line 301: parameter annotated as RabbitMessage receives the message object."""
        from tests.unit.core._pipeline_handlers import handler_rabbit_message_body

        pipeline = HandlerPipeline()
        msg = _make_message()
        _wire_sync(msg)

        route = _make_route(handler=handler_rabbit_message_body)
        # The handler has (msg: RabbitMessage, body: bytes)
        # msg should be injected (line 301), body injected second
        # Handler just runs without error
        pipeline.process_sync(route, msg)
        # Message should be acked successfully
        assert msg._disposition == "acked"


# ── _build_result_envelope with None result (line 391) ───────────────────


class TestBuildResultEnvelopeNoneResult:
    def test_none_result_returns_none(self) -> None:
        """Line 391: _build_result_envelope returns None when result is None."""
        pipeline = HandlerPipeline()
        route = _make_route()
        msg = _make_message()

        result = pipeline._build_result_envelope(route, msg, None)
        assert result is None


# ── _deserialize_body coverage (lines 240-250) ───────────────────────────


class TestDeserializeBody:
    def test_deserializer_with_non_bytes_type(self) -> None:
        """Lines 241-250: serializer decodes body to target type (non-bytes)."""
        mock_serializer = MagicMock()
        mock_serializer.decode.return_value = {"id": 1}

        pipeline = HandlerPipeline(serializer=mock_serializer)
        msg = _make_message(body=b'{"id": 1}')
        _wire_sync(msg)

        received: list[object] = []

        def handler(body: dict) -> None:
            received.append(body)

        route = _make_route(handler=handler)
        pipeline.process_sync(route, msg)

        mock_serializer.decode.assert_called_once()
        assert received[0] == {"id": 1}

    def test_deserializer_pydantic_model_validation(self) -> None:
        """Lines 244-249: auto-validates Pydantic models when decoded is dict.

        The handler must be defined in a module WITHOUT from __future__ import annotations
        so that the body type annotation is a real class (not a string), allowing
        _get_body_type() to return it and _deserialize_body() to call model_validate().
        """
        from tests.unit.core._pipeline_handlers_pydantic import handler_fake_pydantic

        mock_serializer = MagicMock()
        mock_serializer.decode.return_value = {"id": 42}

        pipeline = HandlerPipeline(serializer=mock_serializer)
        msg = _make_message(body=b'{"id": 42}')
        _wire_sync(msg)

        route = _make_route(handler=handler_fake_pydantic)
        pipeline.process_sync(route, msg)

        # The FakePydanticModel.model_validate should have been called
        assert msg._disposition == "acked"

    def test_deserializer_skipped_for_bytes_type(self) -> None:
        """Lines 241: bytes type → serializer.decode NOT called (bytes target is excluded)."""
        mock_serializer = MagicMock()
        mock_serializer.decode.return_value = b"raw-bytes"

        pipeline = HandlerPipeline(serializer=mock_serializer)
        msg = _make_message(body=b"raw-bytes")
        _wire_sync(msg)

        # Define handler with bytes annotation — _get_body_type returns bytes,
        # then _deserialize_body checks: target_type is not bytes → False → skip decode
        # Handler defined in _pipeline_handlers.py (no future annotations) so
        # annotation is the real bytes class, not the string 'bytes'.
        from tests.unit.core._pipeline_handlers import handler_bytes

        route = _make_route(handler=handler_bytes)
        pipeline.process_sync(route, msg)

        mock_serializer.decode.assert_not_called()
        assert msg._disposition == "acked"


    def test_sync_already_settled_logs_warning(self) -> None:
        """Lines 443-445: message already settled before exception — logs warning, no re-raise."""
        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            msg.ack()  # settle manually
            raise RuntimeError("error after settlement")

        route = _make_route(handler=handler, ack_policy=AckPolicy.AUTO)
        pipeline = HandlerPipeline()
        # Should not raise — message already settled
        pipeline.process_sync(route, msg)

        assert msg._disposition == "acked"


# ── Sync filter rejection (lines 70-72) ─────────────────────────────────


class TestSyncFilterRejection:
    def test_sync_filter_fn_rejects_message(self) -> None:
        """Lines 70-72: filter_fn returns False in process_sync → nack."""
        msg = _make_message()
        _, nack_fn, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler, filter_fn=lambda m: False)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        nack_fn.assert_called_once_with(False)

    def test_sync_filter_fn_already_settled_skips_nack(self) -> None:
        """Lines 70-72: filter_fn returns False but message already settled — no double-nack."""
        msg = _make_message()
        _, nack_fn, _ = _wire_sync(msg)
        # Pre-settle
        msg.ack()

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler, filter_fn=lambda m: False)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        # nack should NOT be called since message was already settled
        nack_fn.assert_not_called()

    def test_sync_filter_fn_passes_message(self) -> None:
        """filter_fn returns True → message is processed normally."""
        msg = _make_message()
        ack_fn, _, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler, filter_fn=lambda m: True)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        ack_fn.assert_called_once()


# ── _publish_result_sync/async with None publish_fn (lines 341, 363) ─────


class TestPublishResultNoPublishFn:
    def test_publish_result_sync_no_publish_fn(self) -> None:
        """Line 341: _publish_result_sync returns early when publish_fn is None."""
        pipeline = HandlerPipeline()
        msg = _make_message()
        route = _make_route()

        # Should return without error (no envelope built, no call made)
        pipeline._publish_result_sync(route, msg, b"result", None)

    async def test_publish_result_async_no_publish_fn(self) -> None:
        """Line 363: _publish_result_async returns early when publish_fn is None."""
        pipeline = HandlerPipeline()
        msg = _make_message()
        route = _make_route()

        # Should return without error
        await pipeline._publish_result_async(route, msg, b"result", None)
