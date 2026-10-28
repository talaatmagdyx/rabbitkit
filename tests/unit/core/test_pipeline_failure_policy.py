"""Failure settlement policy of HandlerPipeline: bounded transient loop,
MANUAL failures, diagnostics, ambiguous acks, processing
deadline and dead-letter target verification."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core import pipeline as pipeline_mod
from rabbitkit.core.errors import HandlerTimeoutError
from rabbitkit.core.message import NackMessage, RabbitMessage, RejectMessage, SettlementError
from rabbitkit.core.pipeline import HandlerPipeline, _WindowedLog
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import AckPolicy


def _msg(*, redelivered: bool = False, **kw: Any) -> RabbitMessage:
    return RabbitMessage(
        body=b"secret-payload",
        message_id="m-1",
        correlation_id="c-1",
        delivery_tag=7,
        redelivered=redelivered,
        **kw,
    )


def _wire(msg: RabbitMessage) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
    ack, nack, reject = AsyncMock(), AsyncMock(), AsyncMock()
    msg._ack_async_fn = ack
    msg._nack_async_fn = nack
    msg._reject_async_fn = reject
    return ack, nack, reject


def _wire_sync(msg: RabbitMessage) -> tuple[MagicMock, MagicMock, MagicMock]:
    ack, nack, reject = MagicMock(), MagicMock(), MagicMock()
    msg._ack_fn = ack
    msg._nack_fn = nack
    msg._reject_fn = reject
    return ack, nack, reject


def _route(handler: Any, ack_policy: AckPolicy = AckPolicy.AUTO, **kw: Any) -> RouteDefinition:
    return RouteDefinition(
        name="r", queue=RabbitQueue(name="orders"), exchange=None, handler=handler, ack_policy=ack_policy, **kw
    )


def _raiser(exc: BaseException) -> Any:
    async def handler(body: bytes) -> None:
        raise exc

    return handler


def _captured(pipeline: HandlerPipeline) -> MagicMock:
    log = MagicMock()
    pipeline._failure_log = _WindowedLog(clock=lambda: 0.0, log=log)
    return log


# ── transient failures are bounded by default ────────────────────────


class TestTransientLoopIsBoundedByDefault:
    async def test_first_transient_failure_requeues(self) -> None:
        msg = _msg()
        _ack, nack, reject = _wire(msg)
        await HandlerPipeline().process_async(_route(_raiser(TimeoutError("db"))), msg)
        nack.assert_awaited_once_with(True)
        reject.assert_not_awaited()

    async def test_transient_failure_on_a_redelivery_dead_letters(self) -> None:
        msg = _msg(redelivered=True)
        _ack, nack, reject = _wire(msg)
        await HandlerPipeline().process_async(_route(_raiser(OSError("reset"))), msg)
        reject.assert_awaited_once_with(False)
        nack.assert_not_awaited()

    async def test_opt_out_restores_unbounded_requeue(self) -> None:
        msg = _msg(redelivered=True)
        _ack, nack, reject = _wire(msg)
        await HandlerPipeline(reject_transient_on_redelivery=False).process_async(
            _route(_raiser(TimeoutError("db"))), msg
        )
        nack.assert_awaited_once_with(True)
        reject.assert_not_awaited()

    def test_sync_pipeline_has_the_same_default(self) -> None:
        msg = _msg(redelivered=True)
        _ack, nack, reject = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise TimeoutError("db")

        HandlerPipeline().process_sync(_route(handler), msg)
        reject.assert_called_once_with(False)
        nack.assert_not_called()


# ── MANUAL: a handler that raises without settling no longer stalls ───────


class TestManualFailureIsSettled:
    async def test_permanent_failure_dead_letters(self) -> None:
        msg = _msg()
        _ack, nack, reject = _wire(msg)
        await HandlerPipeline().process_async(_route(_raiser(RuntimeError("bug")), AckPolicy.MANUAL), msg)
        reject.assert_awaited_once_with(False)
        nack.assert_not_awaited()

    async def test_transient_failure_requeues_once_then_dead_letters(self) -> None:
        first, again = _msg(), _msg(redelivered=True)
        _a1, nack1, _r1 = _wire(first)
        _a2, _n2, reject2 = _wire(again)
        route = _route(_raiser(TimeoutError("db")), AckPolicy.MANUAL)
        pipeline = HandlerPipeline()
        await pipeline.process_async(route, first)
        await pipeline.process_async(route, again)
        nack1.assert_awaited_once_with(True)
        reject2.assert_awaited_once_with(False)

    async def test_a_handler_that_settled_then_raised_is_not_settled_twice(self) -> None:
        msg = _msg()
        ack, nack, reject = _wire(msg)

        async def handler(body: bytes, message: RabbitMessage) -> None:
            await message.ack_async()
            raise RuntimeError("after ack")

        await HandlerPipeline().process_async(_route(handler, AckPolicy.MANUAL), msg)
        ack.assert_awaited_once()
        nack.assert_not_awaited()
        reject.assert_not_awaited()

    async def test_opt_out_leaves_it_unsettled_and_re_raises(self) -> None:
        msg = _msg()
        _ack, nack, reject = _wire(msg)
        with pytest.raises(RuntimeError):
            await HandlerPipeline(manual_settle_on_error=False).process_async(
                _route(_raiser(RuntimeError("bug")), AckPolicy.MANUAL), msg
            )
        nack.assert_not_awaited()
        reject.assert_not_awaited()
        assert msg.disposition == "pending"

    def test_sync_manual_failure_is_settled(self) -> None:
        msg = _msg()
        _ack, _nack, reject = _wire_sync(msg)

        def handler(body: bytes) -> None:
            raise RuntimeError("bug")

        HandlerPipeline().process_sync(_route(handler, AckPolicy.MANUAL), msg)
        reject.assert_called_once_with(False)


# ── every handler failure is logged, without payload, rate-limited


class TestFailureDiagnostics:
    async def test_a_dead_lettered_failure_is_logged_with_identifiers(self) -> None:
        pipeline = HandlerPipeline()
        log = _captured(pipeline)
        msg = _msg()
        _wire(msg)
        await pipeline.process_async(_route(_raiser(ValueError("card=4111-secret"))), msg)
        log.warning.assert_called_once()
        rendered = log.warning.call_args.args[0] % log.warning.call_args.args[1:]
        for fragment in (
            "queue=orders",
            "message_id=m-1",
            "correlation_id=c-1",
            "error=ValueError",
            "reason=permanent",
            "action=dead_letter",
        ):
            assert fragment in rendered
        assert "4111" not in rendered and "secret-payload" not in rendered

    async def test_a_hot_loop_logs_one_line_per_window(self) -> None:
        now = [0.0]
        log = MagicMock()
        pipeline = HandlerPipeline()
        pipeline._failure_log = _WindowedLog(clock=lambda: now[0], log=log)
        route = _route(_raiser(ValueError("bad")))
        for _ in range(50):
            msg = _msg()
            _wire(msg)
            await pipeline.process_async(route, msg)
        assert log.warning.call_count == 1
        assert log.debug.call_count == 49
        now[0] = _WindowedLog.WINDOW_SECONDS + 1
        msg = _msg()
        _wire(msg)
        await pipeline.process_async(route, msg)
        assert log.warning.call_count == 2
        assert "49 similar event(s) suppressed" in log.warning.call_args.args[0] % log.warning.call_args.args[1:]

    def test_window_key_table_is_bounded(self) -> None:
        w = _WindowedLog(clock=lambda: 0.0, log=MagicMock())
        for i in range(_WindowedLog.MAX_KEYS + 10):
            w.emit((str(i),), "warning", "x")
        assert len(w._windows) <= _WindowedLog.MAX_KEYS


# ── a failing ack after success is ambiguous, not a handler failure


class TestAckOutcomeUnknown:
    async def test_failed_ack_is_counted_and_never_followed_by_nack_or_reject(self) -> None:
        pipeline = HandlerPipeline()
        log = _captured(pipeline)
        msg = _msg()
        ack, nack, reject = _wire(msg)
        ack.side_effect = ConnectionResetError("socket closed mid-write")

        async def handler(body: bytes) -> None:
            return None

        await pipeline.process_async(_route(handler), msg)
        nack.assert_not_awaited()
        reject.assert_not_awaited()
        assert pipeline.ack_outcome_unknown_total == 1
        rendered = log.warning.call_args.args[0] % log.warning.call_args.args[1:]
        assert "Ack outcome unknown" in rendered and "idempotent" in rendered
        assert "will be redelivered" not in rendered

    def test_sync_failed_ack_is_ambiguous_too(self) -> None:
        pipeline = HandlerPipeline()
        _captured(pipeline)
        msg = _msg()
        ack, nack, reject = _wire_sync(msg)
        ack.side_effect = OSError("broken pipe")

        def handler(body: bytes) -> None:
            return None

        pipeline.process_sync(_route(handler), msg)
        nack.assert_not_called()
        reject.assert_not_called()
        assert pipeline.ack_outcome_unknown_total == 1

    async def test_a_settlement_wiring_bug_still_raises(self) -> None:
        msg = _msg()  # no settlement functions at all

        async def handler(body: bytes) -> None:
            return None

        with pytest.raises(SettlementError):
            await HandlerPipeline().process_async(_route(handler), msg)

    async def test_metrics_middleware_counts_it(self) -> None:
        from rabbitkit.middleware.metrics import MetricsMiddleware

        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector)
        pipeline = HandlerPipeline()
        _captured(pipeline)
        msg = _msg()
        ack, _nack, _reject = _wire(msg)
        ack.side_effect = ConnectionResetError("x")

        async def handler(body: bytes) -> None:
            return None

        await pipeline.process_async(_route(handler, route_middlewares=[mw]), msg)
        names = [c.args[0] for c in collector.inc_counter.call_args_list]
        assert "rabbitkit_settlement_outcome_unknown_total" in names
        # the claimed-but-unsent ack is not also counted as acked.
        assert "rabbitkit_messages_acked_total" not in names


# ── processing deadline ─────────────────────────────────────


class TestProcessingTimeout:
    async def test_deadline_cancels_and_reports_outcome_unknown(self) -> None:
        pipeline = HandlerPipeline(processing_timeout=0.05)
        log = _captured(pipeline)
        cancelled = asyncio.Event()

        async def handler(body: bytes) -> None:
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        msg = _msg()
        _ack, nack, _reject = _wire(msg)
        async with asyncio.timeout(5):
            await pipeline.process_async(_route(handler), msg)
        assert cancelled.is_set()
        nack.assert_awaited_once_with(True)
        rendered = log.warning.call_args.args[0] % log.warning.call_args.args[1:]
        assert "error=HandlerTimeoutError" in rendered and "outcome_unknown=True" in rendered

    async def test_handlers_own_timeout_error_is_not_relabelled(self) -> None:
        pipeline = HandlerPipeline(processing_timeout=5.0)
        log = _captured(pipeline)
        msg = _msg()
        _wire(msg)
        await pipeline.process_async(_route(_raiser(TimeoutError("http client"))), msg)
        rendered = log.warning.call_args.args[0] % log.warning.call_args.args[1:]
        assert "error=TimeoutError" in rendered and "outcome_unknown=False" in rendered

    async def test_fast_handler_is_untouched(self) -> None:
        msg = _msg()
        ack, _nack, _reject = _wire(msg)

        async def handler(body: bytes) -> None:
            return None

        await HandlerPipeline(processing_timeout=5.0).process_async(_route(handler), msg)
        ack.assert_awaited_once()

    def test_handler_timeout_error_is_marked_outcome_unknown(self) -> None:
        from rabbitkit.core.errors import classify_error
        from rabbitkit.core.types import ErrorSeverity

        classified = classify_error(HandlerTimeoutError(1.0))
        assert classified.severity is ErrorSeverity.TRANSIENT
        assert classified.outcome_unknown is True
        assert "outcome unknown" in classified.reason
        assert classify_error(TimeoutError()).outcome_unknown is False


# ── never dead-letter into a missing DLQ ─────────────────────────────


@pytest.fixture
def fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_mod, "_DLQ_MISSING_BACKOFF_INITIAL", 0.001)
    monkeypatch.setattr(pipeline_mod, "_DLQ_MISSING_BACKOFF_MAX", 0.004)


@pytest.mark.usefixtures("fast_backoff")
class TestDeadLetterTargetMissing:
    async def test_missing_dlq_requeues_instead_of_rejecting(self) -> None:
        check = AsyncMock(return_value=False)
        pipeline = HandlerPipeline(dead_letter_check=check)
        log = _captured(pipeline)
        msg = _msg()
        _ack, nack, reject = _wire(msg)
        await pipeline.process_async(_route(_raiser(ValueError("bad"))), msg)
        reject.assert_not_awaited()
        nack.assert_awaited_once_with(True)
        assert pipeline.dead_letter_blocked_total == 1
        assert log.error.call_count == 2  # the failure line + the DLQ-missing line

    async def test_present_dlq_dead_letters_normally(self) -> None:
        pipeline = HandlerPipeline(dead_letter_check=AsyncMock(return_value=True))
        _captured(pipeline)
        msg = _msg()
        _ack, nack, reject = _wire(msg)
        await pipeline.process_async(_route(_raiser(ValueError("bad"))), msg)
        reject.assert_awaited_once_with(False)
        nack.assert_not_awaited()

    async def test_requeue_is_not_checked(self) -> None:
        check = AsyncMock(return_value=False)
        msg = _msg()
        _ack, nack, _reject = _wire(msg)
        await HandlerPipeline(dead_letter_check=check).process_async(_route(_raiser(TimeoutError())), msg)
        check.assert_not_awaited()
        nack.assert_awaited_once_with(True)

    @pytest.mark.parametrize("exc", [RejectMessage(requeue=False), NackMessage(requeue=False)])
    async def test_handler_requested_dead_letter_is_guarded(self, exc: Exception) -> None:
        pipeline = HandlerPipeline(dead_letter_check=AsyncMock(return_value=False))
        _captured(pipeline)
        msg = _msg()
        _ack, nack, reject = _wire(msg)
        await pipeline.process_async(_route(_raiser(exc)), msg)
        reject.assert_not_awaited()
        nack.assert_awaited_once_with(True)

    async def test_backoff_grows_and_resets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        delays: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            delays.append(seconds)

        monkeypatch.setattr(pipeline_mod.asyncio, "sleep", fake_sleep)
        answers = [False, False, False, False, True, False]
        pipeline = HandlerPipeline(dead_letter_check=AsyncMock(side_effect=answers))
        _captured(pipeline)
        route = _route(_raiser(ValueError("bad")))
        for _ in answers:
            msg = _msg()
            _wire(msg)
            await pipeline.process_async(route, msg)
        assert delays == [0.001, 0.002, 0.004, 0.004, 0.001]


# ── sync broker: settling an expired delivery ─────────


class TestSettleExpiredSync:
    def test_first_delivery_requeues_with_an_outcome_unknown_log(self) -> None:
        pipeline = HandlerPipeline()
        log = _captured(pipeline)
        msg = _msg()
        _ack, nack, reject = _wire_sync(msg)
        assert pipeline.settle_expired_sync(_route(_raiser(TimeoutError())), msg, HandlerTimeoutError(2.0)) is True
        nack.assert_called_once_with(True)
        reject.assert_not_called()
        rendered = log.warning.call_args.args[0] % log.warning.call_args.args[1:]
        assert "error=HandlerTimeoutError" in rendered and "category=unknown_outcome" in rendered
        assert "outcome_unknown=True" in rendered

    def test_a_redelivery_is_dead_lettered(self) -> None:
        msg = _msg(redelivered=True)
        _ack, _nack, reject = _wire_sync(msg)
        assert HandlerPipeline().settle_expired_sync(_route(_raiser(TimeoutError())), msg, HandlerTimeoutError(1.0))
        reject.assert_called_once_with(False)

    def test_manual_without_settle_on_error_leaves_it_to_the_handler(self) -> None:
        msg = _msg()
        ack, nack, reject = _wire_sync(msg)
        pipeline = HandlerPipeline(manual_settle_on_error=False)
        route = _route(_raiser(TimeoutError()), AckPolicy.MANUAL)
        assert pipeline.settle_expired_sync(route, msg, HandlerTimeoutError(1.0)) is False
        ack.assert_not_called()
        nack.assert_not_called()
        reject.assert_not_called()

    def test_an_unexpected_settlement_error_propagates(self) -> None:
        msg = _msg()
        _ack, nack, _reject = _wire_sync(msg)
        nack.side_effect = SettlementError("wiring bug")
        with pytest.raises(SettlementError):
            HandlerPipeline().settle_expired_sync(_route(_raiser(TimeoutError())), msg, HandlerTimeoutError(1.0))

    def test_a_different_handler_timeout_from_the_settlement_propagates(self) -> None:
        msg = _msg()
        _ack, nack, _reject = _wire_sync(msg)
        nack.side_effect = HandlerTimeoutError(9.0)
        with pytest.raises(HandlerTimeoutError) as info:
            HandlerPipeline().settle_expired_sync(_route(_raiser(TimeoutError())), msg, HandlerTimeoutError(1.0))
        assert info.value.timeout_seconds == 9.0

    def test_the_settlement_metric_is_counted_once_across_two_reporters(self) -> None:
        from rabbitkit.core.config import MetricsConfig
        from rabbitkit.middleware.metrics import MetricsMiddleware
        from tests.unit.middleware.test_metrics_inventory import RecordingCollector

        collector = RecordingCollector()
        route = _route(_raiser(TimeoutError()))
        route.route_middlewares.append(MetricsMiddleware(collector))
        msg = _msg()
        _wire_sync(msg)
        msg._settlement_metric_once = [None]
        HandlerPipeline().settle_expired_sync(route, msg, HandlerTimeoutError(1.0))
        pipeline_mod._emit_settlement_metric(route, msg)  # the late handler's finally
        assert collector.count(MetricsConfig().messages_nacked_total) == 1
