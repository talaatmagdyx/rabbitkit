"""Tests for core/pipeline.py — HandlerPipeline, ack behavior, result publishing."""

from __future__ import annotations

import json
import typing
from typing import Annotated
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.route import ResultPublisher, RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy, MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.di import Depends

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


# ── M2: settlement metrics emission ──────────────────────────────────────


class TestSettlementMetricsEmission:
    def test_ack_emits_messages_acked_total(self) -> None:
        from rabbitkit.middleware.metrics import MetricsCollector, MetricsMiddleware

        collector = MagicMock(spec=MetricsCollector)
        metrics_mw = MetricsMiddleware(collector)
        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler, route_middlewares=[metrics_mw])
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        collector.inc_counter.assert_any_call("rabbitkit_messages_acked_total", {"queue": "orders.created"})

    def test_nack_emits_messages_nacked_total(self) -> None:
        from rabbitkit.middleware.metrics import MetricsCollector, MetricsMiddleware

        collector = MagicMock(spec=MetricsCollector)
        metrics_mw = MetricsMiddleware(collector)
        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise ConnectionResetError("lost connection")

        route = _make_route(handler=handler, route_middlewares=[metrics_mw])
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        collector.inc_counter.assert_any_call("rabbitkit_messages_nacked_total", {"queue": "orders.created"})

    def test_reject_emits_messages_rejected_total(self) -> None:
        from rabbitkit.middleware.metrics import MetricsCollector, MetricsMiddleware

        collector = MagicMock(spec=MetricsCollector)
        metrics_mw = MetricsMiddleware(collector)
        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise ValueError("bad data")

        route = _make_route(handler=handler, route_middlewares=[metrics_mw])
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        collector.inc_counter.assert_any_call("rabbitkit_messages_rejected_total", {"queue": "orders.created"})

    def test_filter_rejection_emits_messages_nacked_total(self) -> None:
        from rabbitkit.middleware.metrics import MetricsCollector, MetricsMiddleware

        collector = MagicMock(spec=MetricsCollector)
        metrics_mw = MetricsMiddleware(collector)
        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler, route_middlewares=[metrics_mw], filter_fn=lambda m: False)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        collector.inc_counter.assert_any_call("rabbitkit_messages_nacked_total", {"queue": "orders.created"})

    def test_no_metrics_middleware_is_noop(self) -> None:
        """No MetricsMiddleware on the route -- must not raise."""
        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)  # must not raise

    def test_manual_policy_pending_disposition_emits_nothing(self) -> None:
        """M2: a MANUAL handler that never settles has disposition=pending --
        nothing final to report yet, so no metric is emitted."""
        from rabbitkit.middleware.metrics import MetricsCollector, MetricsMiddleware

        collector = MagicMock(spec=MetricsCollector)
        metrics_mw = MetricsMiddleware(collector)
        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass  # MANUAL: never calls msg.ack()/nack()/reject()

        route = _make_route(handler=handler, ack_policy=AckPolicy.MANUAL, route_middlewares=[metrics_mw])
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        settlement_calls = [
            c
            for c in collector.inc_counter.call_args_list
            if c.args[0]
            in (
                "rabbitkit_messages_acked_total",
                "rabbitkit_messages_nacked_total",
                "rabbitkit_messages_rejected_total",
            )
        ]
        assert settlement_calls == []


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

    def test_success_without_manual_settlement_stays_pending(self) -> None:
        """M11: a MANUAL handler that returns without calling ack()/nack()/
        reject() must be left unsettled -- NOT auto-acked. Auto-acking here
        contradicted "handler owns settlement" and risked loss if the
        handler deferred settlement to run later (e.g. another thread) and
        the process crashed before that deferred call happened."""
        msg = _make_message()
        ack_fn, nack_fn, reject_fn = _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass  # never settles

        route = _make_route(handler=handler, ack_policy=AckPolicy.MANUAL)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        assert msg._disposition == "pending"
        ack_fn.assert_not_called()
        nack_fn.assert_not_called()
        reject_fn.assert_not_called()

    def test_success_with_manual_ack_is_respected(self) -> None:
        """A MANUAL handler that DOES call ack() itself must have that ack
        take effect -- the pipeline must not interfere."""
        msg = _make_message()
        ack_fn, _, _ = _wire_sync(msg)

        def handler(body: bytes) -> None:
            msg.ack()

        route = _make_route(handler=handler, ack_policy=AckPolicy.MANUAL)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg)

        ack_fn.assert_called_once()
        assert msg._disposition == "acked"

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

    def test_transient_redelivery_rejects_when_enabled(self) -> None:
        """M6: with reject_transient_on_redelivery, a transient error on an
        already-redelivered message rejects to DLQ instead of requeuing."""
        pipeline = HandlerPipeline(reject_transient_on_redelivery=True)
        msg = _make_message(redelivered=True)
        _ack, nack, reject = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise ConnectionError("transient")  # classified TRANSIENT

        pipeline.process_sync(_make_route(handler=handler), msg)
        reject.assert_called_once_with(False)
        nack.assert_not_called()

    def test_transient_first_delivery_still_requeues_when_enabled(self) -> None:
        """M6: the first delivery (redelivered=False) still nack-requeues — the
        cap only trips on the redelivery."""
        pipeline = HandlerPipeline(reject_transient_on_redelivery=True)
        msg = _make_message(redelivered=False)
        _ack, nack, reject = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise ConnectionError("transient")

        pipeline.process_sync(_make_route(handler=handler), msg)
        nack.assert_called_once_with(True)
        reject.assert_not_called()

    def test_transient_redelivery_requeues_when_disabled(self) -> None:
        """M6 default (off): transient errors requeue unbounded even on redelivery."""
        pipeline = HandlerPipeline()  # flag defaults False
        msg = _make_message(redelivered=True)
        _ack, nack, reject = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise ConnectionError("transient")

        pipeline.process_sync(_make_route(handler=handler), msg)
        nack.assert_called_once_with(True)
        reject.assert_not_called()

    async def test_transient_redelivery_rejects_when_enabled_async(self) -> None:
        msg = _make_message(redelivered=True)
        msg._ack_async_fn = AsyncMock()
        msg._nack_async_fn = AsyncMock()
        msg._reject_async_fn = AsyncMock()

        async def handler(body: bytes) -> None:
            raise ConnectionError("transient")

        await HandlerPipeline(reject_transient_on_redelivery=True).process_async(
            _make_route(handler=handler), msg
        )
        msg._reject_async_fn.assert_awaited_once_with(False)
        msg._nack_async_fn.assert_not_awaited()

    def test_requeued_for_retry_sentinel_not_published(self) -> None:
        """M7: when an inner middleware returns REQUEUED_FOR_RETRY (message
        already requeued+settled), the pipeline must NOT serialize the
        sentinel as a bogus result/RPC reply."""
        from rabbitkit.core.types import REQUEUED_FOR_RETRY

        msg = _make_message()
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> object:
            return REQUEUED_FOR_RETRY

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        HandlerPipeline().process_sync(route, msg, publish_fn=publish_fn)

        publish_fn.assert_not_called()  # sentinel is not a result

    async def test_requeued_for_retry_sentinel_not_published_async(self) -> None:
        from rabbitkit.core.types import REQUEUED_FOR_RETRY

        msg = _make_message()
        ack = AsyncMock()
        msg._ack_async_fn = ack
        msg._nack_async_fn = AsyncMock()
        publish_fn = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        async def handler(body: bytes) -> object:
            return REQUEUED_FOR_RETRY

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        await HandlerPipeline().process_async(route, msg, publish_fn=publish_fn)

        publish_fn.assert_not_called()

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

    @pytest.mark.asyncio
    async def test_async_manual_success_without_settlement_stays_pending(self) -> None:
        """M11, async: a MANUAL handler that returns without settling must
        be left unsettled -- NOT auto-acked."""
        msg = _make_message()
        ack_fn = MagicMock()
        msg._ack_async_fn = AsyncMock()
        msg._ack_fn = ack_fn

        async def handler(body: bytes) -> None:
            pass  # never settles

        route = _make_route(handler=handler, ack_policy=AckPolicy.MANUAL)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, msg)

        assert msg._disposition == "pending"
        ack_fn.assert_not_called()
        msg._ack_async_fn.assert_not_called()


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

    def test_sync_publish_failure_on_redelivered_message_logs_error(self) -> None:
        """L1: a result-publish failure on an already-redelivered message
        (i.e. this exact nack+requeue is repeating) escalates to ERROR
        instead of WARNING, so a sustained publish outage is loud."""
        msg = _make_message(redelivered=True)
        _wire_sync(msg)

        failed_outcome = PublishOutcome(status=PublishStatus.ERROR)
        publish_fn = MagicMock(return_value=failed_outcome)

        def handler(body: bytes) -> bytes:
            return b"result"

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        error_calls: list[tuple] = []
        with patch("rabbitkit.core.pipeline.logger") as mock_logger:
            mock_logger.warning = MagicMock(side_effect=AssertionError("should not warn"))
            mock_logger.error = MagicMock(side_effect=lambda *a, **kw: error_calls.append(a))
            pipeline.process_sync(route, msg, publish_fn=publish_fn)

        assert any("Result publish failed" in str(call) for call in error_calls)
        assert any("already redelivered" in str(call) for call in error_calls)

    async def test_async_publish_failure_on_redelivered_message_logs_error(self) -> None:
        """Async variant of the L1 redelivery-escalation test above."""
        msg = _make_message(redelivered=True)
        _wire_async(msg)

        failed_outcome = PublishOutcome(status=PublishStatus.ERROR)

        async def publish_fn(envelope: MessageEnvelope) -> PublishOutcome:
            return failed_outcome

        def handler(body: bytes) -> bytes:
            return b"result"

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        error_calls: list[tuple] = []
        with patch("rabbitkit.core.pipeline.logger") as mock_logger:
            mock_logger.warning = MagicMock(side_effect=AssertionError("should not warn"))
            mock_logger.error = MagicMock(side_effect=lambda *a, **kw: error_calls.append(a))
            await pipeline.process_async(route, msg, publish_fn=publish_fn)

        assert any("Result publish failed" in str(call) for call in error_calls)
        assert any("already redelivered" in str(call) for call in error_calls)


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

    def test_build_result_envelope_envelope_no_destination_warns(self) -> None:
        """Core-Low-1: a handler returning a MessageEnvelope with no
        result_publisher and no reply_to → None, and a warning is emitted.
        """
        from unittest.mock import patch

        route = _make_route(result_publisher=None)
        pipeline = HandlerPipeline()
        msg = _make_message()
        user_env = MessageEnvelope(routing_key="ignored", body=b"body", exchange="")

        warning_calls: list[tuple] = []
        with patch("rabbitkit.core.pipeline.logger") as mock_logger:
            mock_logger.warning = MagicMock(side_effect=lambda *a, **kw: warning_calls.append(a))
            envelope = pipeline._build_result_envelope(route, msg, user_env)

        assert envelope is None
        assert any("result dropped" in str(call) for call in warning_calls)


class TestMessageEnvelopeNoDestinationAcked:
    """Core-Low-1: a handler returning a MessageEnvelope with no destination is
    still acked (no behavior change) and a warning is logged for observability.
    """

    def test_sync_envelope_no_destination_acked_and_warned(self) -> None:
        msg = _make_message()
        ack_fn, _, _ = _wire_sync(msg)

        def handler(body: bytes) -> MessageEnvelope:
            return MessageEnvelope(routing_key="ignored", body=b"result", exchange="")

        route = _make_route(handler=handler, result_publisher=None)
        pipeline = HandlerPipeline()
        # A publish_fn must be supplied so _build_result_envelope is actually
        # invoked (otherwise _publish_result_sync short-circuits on publish_fn
        # is None). It never gets called because the envelope is dropped.
        publish_fn = MagicMock()  # never called: envelope is dropped (no destination)

        warning_calls: list[tuple] = []
        with patch("rabbitkit.core.pipeline.logger") as mock_logger:
            mock_logger.warning = MagicMock(side_effect=lambda *a, **kw: warning_calls.append(a))
            pipeline.process_sync(route, msg, publish_fn=publish_fn)

        # publish_fn is NOT called — the envelope was dropped (no destination).
        publish_fn.assert_not_called()
        # The message is still acked — no behavior change, just observability.
        ack_fn.assert_called_once()
        assert msg._disposition == "acked"
        assert any("result dropped" in str(call) for call in warning_calls)


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

    async def test_async_large_body_decode_offloaded_to_thread(self) -> None:
        """M10: a large body is decoded via asyncio.to_thread so it doesn't
        block the event loop."""
        import asyncio
        from unittest.mock import patch

        mock_serializer = MagicMock()
        mock_serializer.decode.return_value = {"id": 1}
        pipeline = HandlerPipeline(serializer=mock_serializer)

        big = b"x" * (256 * 1024 + 1)  # just over the offload threshold
        msg = _make_message(body=big)
        msg._ack_async_fn = AsyncMock()
        msg._nack_async_fn = AsyncMock()

        def handler(body: dict) -> None:
            pass

        route = _make_route(handler=handler)
        with patch("asyncio.to_thread", wraps=asyncio.to_thread) as spy:
            await pipeline.process_async(route, msg)
        spy.assert_called_once()  # decode ran off the loop
        mock_serializer.decode.assert_called_once()

    async def test_async_small_body_decode_inline(self) -> None:
        """M10: a small body decodes inline (no thread-hop overhead)."""
        import asyncio
        from unittest.mock import patch

        mock_serializer = MagicMock()
        mock_serializer.decode.return_value = {"id": 1}
        pipeline = HandlerPipeline(serializer=mock_serializer)

        msg = _make_message(body=b'{"id": 1}')
        msg._ack_async_fn = AsyncMock()
        msg._nack_async_fn = AsyncMock()

        def handler(body: dict) -> None:
            pass

        route = _make_route(handler=handler)
        with patch("asyncio.to_thread", wraps=asyncio.to_thread) as spy:
            await pipeline.process_async(route, msg)
        spy.assert_not_called()  # inline decode
        mock_serializer.decode.assert_called_once()

    async def test_async_bytes_body_type_returns_raw(self) -> None:
        """M10: async path with a serializer but a bytes body-type returns the
        raw body (no decode, no offload)."""
        mock_serializer = MagicMock()
        pipeline = HandlerPipeline(serializer=mock_serializer)
        msg = _make_message(body=b"raw-bytes")
        msg._ack_async_fn = AsyncMock()
        msg._nack_async_fn = AsyncMock()

        received: list[object] = []

        def handler(body: bytes) -> None:
            received.append(body)

        route = _make_route(handler=handler)
        await pipeline.process_async(route, msg)
        mock_serializer.decode.assert_not_called()
        assert received[0] == b"raw-bytes"

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


# ── DI generator teardown in the auto-DI path (Core-C8) ────────────────────
# Regression: a handler using Annotated[..., Depends(gen_factory)] with NO
# explicit di_resolver must still tear down generator dependencies (run their
# post-yield cleanup). Before the fix the auto-DI path never created a
# DependencyScope, so generators leaked (cleanup never ran).
#
# The dependency factories are module-level so the pipeline's marker-detection
# (``typing.get_type_hints``) can resolve the stringified annotations —
# closure-captured factories would be invisible to the auto-DI detector.

_auto_di_cleaned: list[bool] = []


def _auto_di_db() -> typing.Iterator[str]:
    yield "db-session"
    _auto_di_cleaned.append(True)  # post-yield teardown


def _auto_di_broken() -> None:
    raise RuntimeError("dependency unavailable")


class TestAutoDIGeneratorTeardown:
    def test_auto_di_generator_teardown_runs(self) -> None:
        """A generator dependency is torn down in the auto-DI (no-resolver) path."""
        _auto_di_cleaned.clear()

        def handler(body: bytes, db: Annotated[str, Depends(_auto_di_db)]) -> None:
            assert db == "db-session"

        pipeline = HandlerPipeline()  # NO explicit di_resolver → auto-DI path
        msg = _make_message()
        _wire_sync(msg)
        route = _make_route(handler=handler)

        pipeline.process_sync(route, msg)

        assert _auto_di_cleaned == [True], "generator teardown did not run in auto-DI path"
        assert msg.is_settled is True  # success → acked

    def test_auto_di_factory_raise_still_closes_earlier_generators(self) -> None:
        """When a later dependency factory raises during resolution, generators
        opened earlier in the same call are still closed (teardown runs).
        """
        _auto_di_cleaned.clear()

        def handler(
            body: bytes,
            db: Annotated[str, Depends(_auto_di_db)],
            bad: Annotated[None, Depends(_auto_di_broken)],
        ) -> None:
            pass

        pipeline = HandlerPipeline()  # auto-DI path
        msg = _make_message()
        _wire_sync(msg)
        route = _make_route(handler=handler)

        # Resolution raises (_auto_di_broken), so the handler never runs; the
        # finally block must still close the generator opened earlier.
        pipeline.process_sync(route, msg)

        assert _auto_di_cleaned == [True], "earlier generator was not closed on resolution failure"
        # The message is settled by the pipeline's exception handler (AUTO → reject)
        assert msg.is_settled is True

    async def test_async_auto_di_generator_teardown_runs(self) -> None:
        """Async auto-DI path also tears down sync generator dependencies."""
        _auto_di_cleaned.clear()

        async def handler(body: bytes, db: Annotated[str, Depends(_auto_di_db)]) -> None:
            assert db == "db-session"

        pipeline = HandlerPipeline()  # auto-DI path
        msg = _make_message()
        _wire_async(msg)
        route = _make_route(handler=handler)

        await pipeline.process_async(route, msg)

        assert _auto_di_cleaned == [True], "generator teardown did not run in async auto-DI path"
        assert msg.is_settled is True


# ── I-14: handler returning a MessageEnvelope preserves user fields ──────


class TestBuildResultEnvelopePreservesUserFields:
    """I-14: a handler returning a ``MessageEnvelope`` no longer silently drops
    all fields except ``body``. The user's headers/message_id/content_type/
    priority/expiration/etc. are preserved via ``dataclasses.replace``; only the
    precedence-driven destination is merged in.
    """

    def test_envelope_result_with_result_publisher_preserves_headers_priority(self) -> None:
        msg = _make_message()
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> MessageEnvelope:
            return MessageEnvelope(
                routing_key="user-key",
                body=b"payload",
                headers={"k": "v"},
                priority=5,
                content_type="application/x-custom",
                expiration="30000",
            )

        rp = ResultPublisher(exchange="out-ex", routing_key="out-key")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg, publish_fn=publish_fn)

        publish_fn.assert_called_once()
        env = publish_fn.call_args[0][0]
        # Precedence-driven destination overrides exchange/routing_key ONLY.
        assert env.exchange == "out-ex"
        assert env.routing_key == "out-key"
        # User fields preserved (the bug used to drop these).
        assert env.body == b"payload"
        assert env.headers == {"k": "v"}
        assert env.priority == 5
        assert env.content_type == "application/x-custom"
        assert env.expiration == "30000"

    def test_envelope_result_with_reply_to_preserves_fields_sets_correlation(self) -> None:
        msg = _make_message(reply_to="amq.rabbitmq.reply-to", correlation_id="corr-9")
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> MessageEnvelope:
            return MessageEnvelope(
                routing_key="user-key",
                body=b"resp",
                headers={"h": "1"},
                priority=3,
            )

        rp = ResultPublisher(exchange="out-ex", routing_key="out-key")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg, publish_fn=publish_fn)

        publish_fn.assert_called_once()
        env = publish_fn.call_args[0][0]
        # reply_to wins → routing_key=reply_to, exchange="", correlation_id from msg.
        assert env.routing_key == "amq.rabbitmq.reply-to"
        assert env.exchange == ""
        assert env.correlation_id == "corr-9"
        # User fields preserved.
        assert env.body == b"resp"
        assert env.headers == {"h": "1"}
        assert env.priority == 3

    def test_envelope_result_no_destination_no_publish(self) -> None:
        """A returned envelope with no reply_to and no result_publisher → no publish."""
        msg = _make_message()
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> MessageEnvelope:
            return MessageEnvelope(routing_key="x", body=b"...", headers={"k": "v"})

        route = _make_route(handler=handler, result_publisher=None)
        pipeline = HandlerPipeline()
        pipeline.process_sync(route, msg, publish_fn=publish_fn)
        publish_fn.assert_not_called()

    def test_build_envelope_direct_with_user_envelope_result_publisher(self) -> None:
        """White-box: _build_result_envelope with an envelope result + result_publisher."""
        rp = ResultPublisher(exchange="results", routing_key="output.key")
        route = _make_route(result_publisher=rp)
        pipeline = HandlerPipeline()
        msg = _make_message()

        user_env = MessageEnvelope(
            routing_key="ignored",
            body=b"body",
            headers={"x": "y"},
            priority=7,
            message_id="user-mid",
        )
        env = pipeline._build_result_envelope(route, msg, user_env)
        assert env is not None
        assert env.routing_key == "output.key"
        assert env.exchange == "results"
        assert env.headers == {"x": "y"}
        assert env.priority == 7
        assert env.message_id == "user-mid"
        assert env.body == b"body"


# ── I-18: publish-side middleware chain cache ────────────────────────────


class TestPublishChainCache:
    """I-18: the publish-side middleware chain is cached per route (keyed by
    ``id(route)``) — mirrors the consume cache so we don't allocate N closures
    per message.
    """

    def test_compose_publish_sync_caches_per_route(self) -> None:
        pipeline = HandlerPipeline()
        route = _make_route()
        fn1 = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        fn2 = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        chain1 = pipeline._compose_publish_sync(route, fn1)
        chain2 = pipeline._compose_publish_sync(route, fn2)

        # Cache hit → same callable returned.
        assert chain1 is chain2
        assert id(route) in pipeline._publish_chain_cache

        # L-1: publish_fn is NOT captured in the cached closure — it is threaded
        # through at call time, so the chain uses whichever fn is passed in.
        env = MessageEnvelope(routing_key="k", body=b"x", exchange="")
        chain1(env, fn1)
        fn1.assert_called_once()
        fn2.assert_not_called()
        chain2(env, fn2)
        fn2.assert_called_once()

    @pytest.mark.asyncio
    async def test_compose_publish_async_caches_per_route(self) -> None:
        pipeline = HandlerPipeline()
        route = _make_route()
        fn1 = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        fn2 = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        chain1 = pipeline._compose_publish_async(route, fn1)
        chain2 = pipeline._compose_publish_async(route, fn2)

        assert chain1 is chain2
        assert id(route) in pipeline._publish_chain_async_cache

        # L-1: publish_fn is threaded through at call time, not captured.
        env = MessageEnvelope(routing_key="k", body=b"x", exchange="")
        await chain1(env, fn1)
        fn1.assert_awaited_once()
        fn2.assert_not_awaited()
        await chain2(env, fn2)
        fn2.assert_awaited_once()

    def test_publish_chain_built_once_across_messages_sync(self) -> None:
        """Behavioral: two process_sync calls for the same route reuse one chain."""
        msg1 = _make_message()
        _wire_sync(msg1)
        msg2 = _make_message()
        _wire_sync(msg2)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> bytes:
            return b"r"

        rp = ResultPublisher(exchange="out", routing_key="k")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        pipeline.process_sync(route, msg1, publish_fn=publish_fn)
        pipeline.process_sync(route, msg2, publish_fn=publish_fn)

        assert publish_fn.call_count == 2
        assert len(pipeline._publish_chain_cache) == 1
        assert id(route) in pipeline._publish_chain_cache

    @pytest.mark.asyncio
    async def test_publish_chain_built_once_across_messages_async(self) -> None:
        msg1 = _make_message()
        _wire_async(msg1)
        msg2 = _make_message()
        _wire_async(msg2)
        publish_fn = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        async def handler(body: bytes) -> bytes:
            return b"r"

        rp = ResultPublisher(exchange="out", routing_key="k")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        await pipeline.process_async(route, msg1, publish_fn=publish_fn)
        await pipeline.process_async(route, msg2, publish_fn=publish_fn)

        assert publish_fn.await_count == 2
        assert len(pipeline._publish_chain_async_cache) == 1
        assert id(route) in pipeline._publish_chain_async_cache


# ── L-1: cached publish chain must use the per-call publish_fn ───────────────


class TestPublishChainPerCallPublishFn:
    """L-1: the cached publish-side middleware chain must NOT capture the first
    ``publish_fn``. A second call for the same route with a DIFFERENT
    ``publish_fn`` must route through the new one (the pre-fix cache silently
    reused the first ``publish_fn`` forever).
    """

    def test_sync_two_calls_with_different_publish_fns_route_correctly(self) -> None:
        msg1 = _make_message()
        _wire_sync(msg1)
        msg2 = _make_message()
        _wire_sync(msg2)

        fn_a = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        fn_b = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> bytes:
            return b"r"

        rp = ResultPublisher(exchange="out", routing_key="k")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        pipeline.process_sync(route, msg1, publish_fn=fn_a)
        pipeline.process_sync(route, msg2, publish_fn=fn_b)

        # L-1: the second call must use fn_b, not the cached fn_a (the bug would
        # call fn_a twice and fn_b never).
        fn_a.assert_called_once()
        fn_b.assert_called_once()
        assert len(pipeline._publish_chain_cache) == 1

    @pytest.mark.asyncio
    async def test_async_two_calls_with_different_publish_fns_route_correctly(self) -> None:
        msg1 = _make_message()
        _wire_async(msg1)
        msg2 = _make_message()
        _wire_async(msg2)

        fn_a = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        fn_b = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        async def handler(body: bytes) -> bytes:
            return b"r"

        rp = ResultPublisher(exchange="out", routing_key="k")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        await pipeline.process_async(route, msg1, publish_fn=fn_a)
        await pipeline.process_async(route, msg2, publish_fn=fn_b)

        fn_a.assert_awaited_once()
        fn_b.assert_awaited_once()
        assert len(pipeline._publish_chain_async_cache) == 1

    def test_sync_publish_fn_distinguished_by_marker(self) -> None:
        """Stronger: tag each publish_fn so we can prove the RIGHT one was used
        per call (not just 'both called once')."""
        msg1 = _make_message()
        _wire_sync(msg1)
        msg2 = _make_message()
        _wire_sync(msg2)

        seen: list[str] = []

        def fn_a(env: MessageEnvelope) -> PublishOutcome:
            seen.append("a")
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        def fn_b(env: MessageEnvelope) -> PublishOutcome:
            seen.append("b")
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        def handler(body: bytes) -> bytes:
            return b"r"

        rp = ResultPublisher(exchange="out", routing_key="k")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        pipeline.process_sync(route, msg1, publish_fn=fn_a)
        pipeline.process_sync(route, msg2, publish_fn=fn_b)

        # First call routed through fn_a, second through fn_b — in order.
        assert seen == ["a", "b"]


# ── clear_caches() (R8) ───────────────────────────────────────────────────


class TestClearCaches:
    """R8: HandlerPipeline.clear_caches() drops all four route-keyed caches so
    stale entries (keyed by the id of dropped routes) don't linger across
    reconnect/restart cycles.
    """

    def test_clear_caches_empties_all_four_caches(self) -> None:
        pipeline = HandlerPipeline()
        route = _make_route()
        fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        afn = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        # Populate the four route-keyed caches.
        pipeline._compose_publish_sync(route, fn)
        pipeline._compose_publish_async(route, afn)
        pipeline._consume_chain_cache[id(route)] = lambda _msg: None
        pipeline._consume_chain_async_cache[id(route)] = lambda _msg: None

        assert len(pipeline._consume_chain_cache) == 1
        assert len(pipeline._consume_chain_async_cache) == 1
        assert len(pipeline._publish_chain_cache) == 1
        assert len(pipeline._publish_chain_async_cache) == 1

        pipeline.clear_caches()

        assert pipeline._consume_chain_cache == {}
        assert pipeline._consume_chain_async_cache == {}
        assert pipeline._publish_chain_cache == {}
        assert pipeline._publish_chain_async_cache == {}

    def test_clear_caches_allows_rebuild(self) -> None:
        """After clearing, the next call rebuilds the chain lazily."""
        msg = _make_message()
        _wire_sync(msg)
        publish_fn = MagicMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))

        def handler(body: bytes) -> bytes:
            return b"r"

        rp = ResultPublisher(exchange="out", routing_key="k")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        pipeline.process_sync(route, msg, publish_fn=publish_fn)
        assert len(pipeline._publish_chain_cache) == 1

        pipeline.clear_caches()
        assert pipeline._publish_chain_cache == {}

        pipeline.process_sync(route, msg, publish_fn=publish_fn)
        assert len(pipeline._publish_chain_cache) == 1

    @pytest.mark.asyncio
    async def test_clear_caches_async(self) -> None:
        pipeline = HandlerPipeline()
        route = _make_route()
        fn = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
        pipeline._compose_publish_async(route, fn)
        assert len(pipeline._publish_chain_async_cache) == 1
        pipeline.clear_caches()
        assert pipeline._publish_chain_async_cache == {}
        assert pipeline._consume_chain_async_cache == {}

    def test_clear_caches_on_empty_pipeline_is_safe(self) -> None:
        """Calling clear_caches() before any processing is a no-op."""
        pipeline = HandlerPipeline()
        pipeline.clear_caches()  # no exception
        assert pipeline._consume_chain_cache == {}


# ── _AckFirstStrategy direct calls ───────────────────────────────────────


class TestAckFirstStrategyDirectCalls:
    """Direct calls to _AckFirstStrategy to cover lines 92 and 98-102."""

    def test_on_success_acks_unsettled_message(self) -> None:
        """Line 92: on_success acks msg when not already settled."""
        from rabbitkit.core.pipeline import _AckFirstStrategy

        msg = _make_message()
        ack, _, _ = _wire_sync(msg)
        _AckFirstStrategy().on_success(msg)
        ack.assert_called_once()

    def test_on_success_skips_settled_message(self) -> None:
        """Line 91: on_success skips ack when already settled."""
        from rabbitkit.core.pipeline import _AckFirstStrategy

        msg = _make_message()
        ack, _, _ = _wire_sync(msg)
        msg.ack()  # settle the message via the proper method (sets _disposition)
        ack.reset_mock()
        _AckFirstStrategy().on_success(msg)
        ack.assert_not_called()

    def test_on_error_transient_nacks(self) -> None:
        """Lines 99-100: on_error with transient error → nack(requeue=True)."""
        from rabbitkit.core.pipeline import _AckFirstStrategy

        msg = _make_message()
        _, nack, _ = _wire_sync(msg)
        _AckFirstStrategy().on_error(msg, ConnectionResetError("transient"))
        nack.assert_called_once_with(True)

    def test_on_error_permanent_rejects(self) -> None:
        """Lines 101-102: on_error with permanent error → reject(requeue=False)."""
        from rabbitkit.core.pipeline import _AckFirstStrategy

        msg = _make_message()
        _, _, reject = _wire_sync(msg)
        _AckFirstStrategy().on_error(msg, ValueError("permanent"))
        reject.assert_called_once_with(False)


# ── async strategy direct calls ───────────────────────────────────────────


class TestAsyncStrategyDirectCalls:
    """Direct calls to async strategy classes (lines 143-144, 155-156, 167, 170-174)."""

    @pytest.mark.asyncio
    async def test_manual_async_on_success_leaves_unsettled(self) -> None:
        """M11: _ManualStrategyAsync.on_success must NOT ack when unsettled
        -- MANUAL means the handler owns settlement entirely."""
        from rabbitkit.core.pipeline import _ManualStrategyAsync

        msg = _make_message()
        ack, _, _ = _wire_async(msg)
        await _ManualStrategyAsync().on_success(msg)
        ack.assert_not_called()
        assert msg._disposition == "pending"

    @pytest.mark.asyncio
    async def test_nack_on_error_async_on_success_acks_unsettled(self) -> None:
        """Lines 155-156: _NackOnErrorStrategyAsync.on_success acks when unsettled."""
        from rabbitkit.core.pipeline import _NackOnErrorStrategyAsync

        msg = _make_message()
        ack, _, _ = _wire_async(msg)
        await _NackOnErrorStrategyAsync().on_success(msg)
        ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_ack_first_async_on_success_acks_unsettled(self) -> None:
        """Line 167: _AckFirstStrategyAsync.on_success acks when unsettled."""
        from rabbitkit.core.pipeline import _AckFirstStrategyAsync

        msg = _make_message()
        ack, _, _ = _wire_async(msg)
        await _AckFirstStrategyAsync().on_success(msg)
        ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_ack_first_async_on_error_transient_nacks(self) -> None:
        """Lines 170-172: _AckFirstStrategyAsync.on_error transient → nack_async(True)."""
        from rabbitkit.core.pipeline import _AckFirstStrategyAsync

        msg = _make_message()
        _, nack, _ = _wire_async(msg)
        await _AckFirstStrategyAsync().on_error(msg, ConnectionResetError("transient"))
        nack.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_ack_first_async_on_error_permanent_rejects(self) -> None:
        """Lines 173-174: _AckFirstStrategyAsync.on_error permanent → reject_async(False)."""
        from rabbitkit.core.pipeline import _AckFirstStrategyAsync

        msg = _make_message()
        _, _, reject = _wire_async(msg)
        await _AckFirstStrategyAsync().on_error(msg, ValueError("permanent"))
        reject.assert_called_once_with(False)


# ── DEBUG logging path ────────────────────────────────────────────────────


class TestDebugLoggingPath:
    """Lines 280, 326, 347, 393: contextvars bind/clear on DEBUG logging."""

    def test_sync_debug_path_binds_and_clears_contextvars(self) -> None:
        """Lines 280, 326: bind/clear when DEBUG logging enabled."""
        import structlog.contextvars

        msg = _make_message()
        _wire_sync(msg)

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()

        with patch("rabbitkit.core.pipeline._stdlib_logger") as mock_logger:
            mock_logger.isEnabledFor.return_value = True
            with patch.object(structlog.contextvars, "bind_contextvars") as mock_bind:
                with patch.object(structlog.contextvars, "clear_contextvars") as mock_clear:
                    pipeline.process_sync(route, msg)

        mock_bind.assert_called_once()
        mock_clear.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_debug_path_binds_and_clears_contextvars(self) -> None:
        """Lines 347, 393: async debug path bind/clear."""
        import structlog.contextvars

        msg = _make_message()
        _wire_async(msg)

        async def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()

        with patch("rabbitkit.core.pipeline._stdlib_logger") as mock_logger:
            mock_logger.isEnabledFor.return_value = True
            with patch.object(structlog.contextvars, "bind_contextvars") as mock_bind:
                with patch.object(structlog.contextvars, "clear_contextvars") as mock_clear:
                    await pipeline.process_async(route, msg)

        mock_bind.assert_called_once()
        mock_clear.assert_called_once()


# ── _get_body_type cache hit ──────────────────────────────────────────────


class TestGetBodyTypeCacheHit:
    """Line 550: _get_body_type returns cached value on second call."""

    def test_cache_hit_returns_cached_type(self) -> None:
        pipeline = HandlerPipeline()

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler)
        # First call computes and caches
        result1 = pipeline._get_body_type(route)
        # Second call hits cache (line 550)
        result2 = pipeline._get_body_type(route)
        assert result1 == result2
        assert handler in pipeline._body_type_cache


# ── _compute_body_type get_type_hints fallback ────────────────────────────


class TestComputeBodyTypeFallback:
    """Lines 569-570: _compute_body_type falls back to {} when get_type_hints raises."""

    def test_get_type_hints_exception_falls_back_to_raw_annotation(self) -> None:
        """Lines 569-570: get_type_hints raises → hints={}, raw inspect annotation used."""
        pipeline = HandlerPipeline()

        def handler(body: bytes) -> None:
            pass

        route = _make_route(handler=handler)

        with patch("typing.get_type_hints", side_effect=NameError("undefined")):
            result = pipeline._compute_body_type(route)

        # When get_type_hints raises, fall back to the raw inspect annotation.
        # With `from __future__ import annotations` in this test file, the
        # raw annotation is the string "bytes" rather than the bytes type.
        assert result == "bytes" or result is bytes


# ── _publish_result_async success return ─────────────────────────────────


class TestPublishResultAsyncSuccess:
    """Line 841: _publish_result_async returns True on success."""

    @pytest.mark.asyncio
    async def test_publish_result_async_returns_true_on_confirmed(self) -> None:
        """Line 841/852: _publish_result_async returns True when publish succeeds."""
        msg = _make_message()
        _wire_async(msg)

        async def publish_fn(envelope: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        def handler(body: bytes) -> bytes:
            return b"result"

        rp = ResultPublisher(exchange="out", routing_key="result")
        route = _make_route(handler=handler, result_publisher=rp)
        pipeline = HandlerPipeline()

        result = await pipeline._publish_result_async(route, msg, b"result", publish_fn)
        assert result is True

    @pytest.mark.asyncio
    async def test_publish_result_async_returns_true_when_envelope_is_none(self) -> None:
        """Line 841: _publish_result_async returns True when publish_fn is not None
        but _build_result_envelope returns None (no reply_to, no result_publisher).

        This covers the second 'return True' branch — the one after the
        'if envelope is None' check (line 841), which is distinct from the
        first 'return True' (line 837) when publish_fn is None.
        """
        msg = _make_message()  # no reply_to

        async def publish_fn(envelope: MessageEnvelope) -> PublishOutcome:  # pragma: no cover
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        # Route with no result_publisher and message has no reply_to →
        # _build_result_envelope returns None.
        route = _make_route(result_publisher=None)
        pipeline = HandlerPipeline()

        # Pass a non-None result so _build_result_envelope is called.
        result = await pipeline._publish_result_async(route, msg, b"some-result", publish_fn)
        # publish_fn is not None but envelope is None → return True (line 841).
        assert result is True


class _FakeChannelWrongStateError(Exception):
    """Name-matched stand-in for pika's exception (core never imports pika)."""


_FakeChannelWrongStateError.__name__ = "ChannelWrongStateError"


class TestChannelGoneSettlement:
    """A settle attempt on a dead channel warns and leaves the message
    unsettled (broker redelivers) instead of escaping as a secondary
    ERROR traceback — the SIGTERM-drain noise seen in integration runs."""

    def test_sync_reject_on_closed_channel_swallowed(self) -> None:
        msg = _make_message()
        _, _, reject_fn = _wire_sync(msg)
        reject_fn.side_effect = _FakeChannelWrongStateError("Channel is closed.")

        def handler(body: bytes) -> None:
            raise ValueError("bad data")  # permanent → reject path

        route = _make_route(handler=handler)
        HandlerPipeline().process_sync(route, msg)  # must not raise

        reject_fn.assert_called_once()
        assert msg._disposition == "pending"  # unsettled → broker redelivers

    def test_sync_ack_on_closed_channel_swallowed(self) -> None:
        """Success-path ack fails on a dead channel; the follow-up settle
        (classified permanent → reject) fails the same way — no raise."""
        msg = _make_message()
        ack_fn, _, reject_fn = _wire_sync(msg)
        ack_fn.side_effect = _FakeChannelWrongStateError("Channel is closed.")
        reject_fn.side_effect = _FakeChannelWrongStateError("Channel is closed.")

        def handler(body: bytes) -> None:
            return None

        route = _make_route(handler=handler)
        HandlerPipeline().process_sync(route, msg)
        assert msg._disposition == "pending"

    def test_sync_non_channel_settle_failure_still_raises(self) -> None:
        msg = _make_message()
        _, _, reject_fn = _wire_sync(msg)
        reject_fn.side_effect = OSError("disk on fire")

        def handler(body: bytes) -> None:
            raise ValueError("bad data")

        route = _make_route(handler=handler)
        with pytest.raises(OSError, match="disk on fire"):
            HandlerPipeline()._handle_sync_exception(route, msg, ValueError("bad data"))

    @pytest.mark.asyncio
    async def test_async_reject_on_closed_channel_swallowed(self) -> None:
        msg = _make_message()
        _, _, reject_fn = _wire_async(msg)
        reject_fn.side_effect = _FakeChannelWrongStateError("Channel is closed.")

        async def handler(body: bytes) -> None:
            raise ValueError("bad data")

        route = _make_route(handler=handler)
        await HandlerPipeline().process_async(route, msg)
        assert msg._disposition == "pending"

    @pytest.mark.asyncio
    async def test_async_non_channel_settle_failure_still_raises(self) -> None:
        msg = _make_message()
        _, _, reject_fn = _wire_async(msg)
        reject_fn.side_effect = OSError("disk on fire")

        route = _make_route(handler=lambda body: None)
        with pytest.raises(OSError, match="disk on fire"):
            await HandlerPipeline()._handle_async_exception(route, msg, ValueError("bad"))


class TestIsChannelGone:
    def test_direct_name_match(self) -> None:
        from rabbitkit.core.pipeline import _is_channel_gone

        assert _is_channel_gone(_FakeChannelWrongStateError("x")) is True

    def test_cause_chain_match(self) -> None:
        from rabbitkit.core.pipeline import _is_channel_gone

        outer = RuntimeError("wrapped")
        outer.__cause__ = _FakeChannelWrongStateError("Channel is closed.")
        assert _is_channel_gone(outer) is True

    def test_unrelated_error_is_false(self) -> None:
        from rabbitkit.core.pipeline import _is_channel_gone

        assert _is_channel_gone(ValueError("nope")) is False

    def test_self_referential_context_terminates(self) -> None:
        from rabbitkit.core.pipeline import _is_channel_gone

        exc = ValueError("loop")
        exc.__context__ = exc  # pathological cycle must not hang
        assert _is_channel_gone(exc) is False
