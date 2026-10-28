"""RetryMiddleware: settlement signals, terminal DLQ handoff with metadata,
message identity/attempt count across retries, retry policy, and the
handoff-failure brake."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from rabbitkit.core.config import RetryConfig, RetryHandoffConfig
from rabbitkit.core.errors import ConfigValidationError, classify_error
from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage
from rabbitkit.core.types import (
    REQUEUED_FOR_RETRY,
    ErrorSeverity,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    TerminalAction,
)
from rabbitkit.middleware.retry import RetryMiddleware
from rabbitkit.testing.broker import TestBroker

SECRET = "hunter2"
TS = datetime(2026, 1, 1, tzinfo=UTC)


def _message(**kw: Any) -> RabbitMessage:
    kw.setdefault("body", b"{}")
    kw.setdefault("headers", {"x-rabbitkit-original-queue": "orders"})
    kw.setdefault("routing_key", "orders.created")
    kw.setdefault("exchange", "orders-ex")
    kw.setdefault("message_id", "m1")
    m = RabbitMessage(**kw)
    calls: list[tuple[str, Any]] = []
    m.raw_message = calls

    def _ack() -> None:
        calls.append(("ack", None))

    def _nack(rq: bool) -> None:
        calls.append(("nack", rq))

    def _reject(rq: bool) -> None:
        calls.append(("reject", rq))

    async def _a() -> None:
        _ack()

    async def _n(rq: bool) -> None:
        _nack(rq)

    async def _r(rq: bool) -> None:
        _reject(rq)

    m._ack_fn, m._nack_fn, m._reject_fn = _ack, _nack, _reject
    m._ack_async_fn, m._nack_async_fn, m._reject_async_fn = _a, _n, _r
    return m


def _calls(m: RabbitMessage) -> list[tuple[str, Any]]:
    return m.raw_message  # type: ignore[no-any-return]


class _Recorder:
    def __init__(self, status: PublishStatus = PublishStatus.CONFIRMED) -> None:
        self.status = status
        self.envelopes: list[MessageEnvelope] = []

    def __call__(self, env: MessageEnvelope) -> PublishOutcome:
        self.envelopes.append(env)
        return PublishOutcome(status=self.status)

    async def async_(self, env: MessageEnvelope) -> PublishOutcome:
        return self(env)


def _mw(rec: _Recorder | None = None, **cfg: Any) -> RetryMiddleware:
    cfg.setdefault("max_retries", 3)
    cfg.setdefault("delays", (1, 2, 3))
    rec = rec or _Recorder()
    return RetryMiddleware(RetryConfig(**cfg), publish_fn=rec, publish_async_fn=rec.async_)


# ── R-D1: settlement signals are control flow ─────────────────────────────


class TestSettlementSignalsPassThrough:
    @pytest.mark.parametrize("signal", [AckMessage(), NackMessage(requeue=False), RejectMessage(requeue=False)])
    def test_signal_passes_through_sync_with_transient_unknown_policy(self, signal: Exception) -> None:
        rec = _Recorder()
        mw = RetryMiddleware(
            RetryConfig(max_retries=3, delays=(1, 2, 3), unknown_policy=ErrorSeverity.TRANSIENT),
            publish_fn=rec,
            predicates=[lambda exc: True],  # even a predicate saying "transient"
        )
        msg = _message()

        def handler(m: RabbitMessage) -> None:
            raise signal

        with pytest.raises(type(signal)) as info:
            mw.consume_scope(handler, msg)
        assert info.value is signal
        assert rec.envelopes == [] and _calls(msg) == []
        assert not getattr(signal, "_rabbitkit_terminal", False)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("signal", [AckMessage(), NackMessage(), RejectMessage()])
    async def test_signal_passes_through_async(self, signal: Exception) -> None:
        rec = _Recorder()
        mw = _mw(rec, unknown_policy=ErrorSeverity.TRANSIENT)
        msg = _message()

        async def handler(m: RabbitMessage) -> None:
            raise signal

        with pytest.raises(type(signal)):
            async with asyncio.timeout(5):
                await mw.consume_scope_async(handler, msg)
        assert rec.envelopes == [] and _calls(msg) == []

    def test_testbroker_ack_signal_runs_once_and_acks(self) -> None:
        broker = TestBroker()
        calls = {"n": 0}

        @broker.subscriber(queue="sig", retry=RetryConfig(max_retries=2, delays=(1, 1), unknown_policy="transient"))  # type: ignore[arg-type]
        def handle(body: bytes) -> None:
            calls["n"] += 1
            raise RejectMessage(requeue=False)

        broker.start()
        broker.publish("sig", b"{}")
        assert calls["n"] == 1
        broker.assert_rejected(broker.consumed_messages[0], requeue=False)
        assert not [e for e in broker.published_messages if e.routing_key != "sig"]


# ── R-D2: terminal handoff carries the current attempt's metadata ─────────


class TestTerminalDeadLetterHandoff:
    def test_first_attempt_permanent_failure_publishes_stamped_dlq_copy_then_acks(self) -> None:
        rec = _Recorder()
        mw = _mw(rec)
        msg = _message(correlation_id="corr-1", timestamp=TS)

        def handler(m: RabbitMessage) -> None:
            raise ValueError(f"bad payload password={SECRET}")

        with pytest.raises(ValueError) as info:
            mw.consume_scope(handler, msg)

        [env] = rec.envelopes
        assert env.routing_key == "orders.dlq" and env.exchange == "" and env.mandatory
        h = env.headers
        assert h["x-rabbitkit-error-class"] == "ValueError"
        assert h["x-rabbitkit-error-type"] == "ValueError"
        assert h["x-rabbitkit-error-category"] == "permanent"
        assert h["x-rabbitkit-failure-category"] == "permanent"
        assert SECRET not in h["x-rabbitkit-error-message"] and "***" in h["x-rabbitkit-error-message"]
        assert h["x-rabbitkit-attempts"] == 1
        assert h["x-rabbitkit-original-queue"] == "orders"
        assert h["x-rabbitkit-original-exchange"] == "orders-ex"
        assert h["x-rabbitkit-original-routing-key"] == "orders.created"
        assert h["x-rabbitkit-original-message-id"] == "m1"
        assert h["x-rabbitkit-first-failed-at"] and h["x-rabbitkit-dead-lettered-at"]
        assert env.message_id == "m1" and env.correlation_id == "corr-1" and env.timestamp == TS
        # confirmed publish, THEN ack; the pipeline is told not to settle again
        assert _calls(msg) == [("ack", None)]
        assert info.value._rabbitkit_terminal is True  # type: ignore[attr-defined]
        assert info.value._rabbitkit_dead_lettered is True  # type: ignore[attr-defined]

    def test_exhausted_copy_describes_the_final_attempt(self) -> None:
        rec = _Recorder()
        mw = _mw(rec, max_retries=1, delays=(1,))
        msg = _message(
            headers={
                "x-rabbitkit-original-queue": "orders",
                "x-rabbitkit-retry-count": 1,
                "x-rabbitkit-error-type": "TimeoutError",
                "x-rabbitkit-error-message": "previous attempt",
                "x-rabbitkit-first-failed-at": "2026-01-01T00:00:00+00:00",
            }
        )

        def handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("final attempt")

        with pytest.raises(ConnectionResetError):
            mw.consume_scope(handler, msg)
        h = rec.envelopes[0].headers
        assert h["x-rabbitkit-error-class"] == "ConnectionResetError"
        assert h["x-rabbitkit-error-message"] == "final attempt"
        assert h["x-rabbitkit-failure-category"] == "exhausted"
        assert h["x-rabbitkit-attempts"] == 2
        assert h["x-rabbitkit-first-failed-at"] == "2026-01-01T00:00:00+00:00"

    @pytest.mark.parametrize("status", [PublishStatus.RETURNED, PublishStatus.NACKED, PublishStatus.TIMEOUT])
    def test_failed_dlq_publish_falls_back_to_reject(self, status: PublishStatus) -> None:
        rec = _Recorder(status)
        mw = _mw(rec)
        msg = _message()

        def handler(m: RabbitMessage) -> None:
            raise ValueError("bad")

        with pytest.raises(ValueError) as info:
            mw.consume_scope(handler, msg)
        assert len(rec.envelopes) == 1
        assert _calls(msg) == []  # left for the pipeline's reject → broker DLX
        assert info.value._rabbitkit_dead_lettered is False  # type: ignore[attr-defined]

    def test_terminal_action_reject_does_not_publish(self) -> None:
        rec = _Recorder()
        mw = _mw(rec, terminal_action="reject")
        msg = _message()
        with pytest.raises(ValueError):
            mw.consume_scope(lambda m: (_ for _ in ()).throw(ValueError("x")), msg)
        assert rec.envelopes == [] and _calls(msg) == []

    def test_unknown_source_queue_does_not_publish(self) -> None:
        rec = _Recorder()
        mw = _mw(rec)
        msg = _message(headers={})
        with pytest.raises(ValueError):
            mw.consume_scope(lambda m: (_ for _ in ()).throw(ValueError("x")), msg)
        assert rec.envelopes == []

    @pytest.mark.asyncio
    async def test_async_terminal_handoff(self) -> None:
        rec = _Recorder()
        mw = _mw(rec)
        msg = _message()

        async def handler(m: RabbitMessage) -> None:
            raise KeyError("missing")

        with pytest.raises(KeyError) as info:
            async with asyncio.timeout(5):
                await mw.consume_scope_async(handler, msg)
        assert rec.envelopes[0].routing_key == "orders.dlq"
        assert _calls(msg) == [("ack", None)]
        assert info.value._rabbitkit_dead_lettered is True  # type: ignore[attr-defined]

    def test_terminal_log_is_sanitized(self, caplog: pytest.LogCaptureFixture) -> None:
        """the 'Terminal failure' log used to print raw exception text."""
        mw = _mw(_Recorder())
        with caplog.at_level(logging.WARNING, logger="rabbitkit.middleware.retry"), pytest.raises(ValueError):
            mw.consume_scope(lambda m: (_ for _ in ()).throw(ValueError(f"db password={SECRET}")), _message())
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "Terminal failure" in text
        assert SECRET not in text

    def test_testbroker_pipeline_does_not_warn_after_dead_letter(self, caplog: pytest.LogCaptureFixture) -> None:
        broker = TestBroker()

        @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=1, delays=(1,)))
        def handle(body: bytes) -> None:
            raise ValueError(f"password={SECRET}")

        broker.start()
        with caplog.at_level(logging.WARNING):
            broker.publish("orders", b"{}")
        assert "Exception after settlement" not in caplog.text
        assert SECRET not in caplog.text
        broker.assert_acked(broker.consumed_messages[0])


# ── R-D5 / 12.2 / 13.4: properties, identity, attempt count ───────────────


class TestRetryCopyIdentity:
    def test_retry_copy_preserves_properties(self) -> None:
        mw = _mw()
        msg = _message(
            timestamp=TS,
            type="order.created",
            app_id="svc",
            priority=5,
            content_encoding="gzip",
            delivery_mode=1,
            expiration="60000",
            correlation_id="c",
            headers={"x-rabbitkit-original-queue": "orders", "traceparent": "00-ab-cd-01", "x-idempotency-key": "k"},
        )
        env = mw._build_retry_envelope(msg, 0, TimeoutError("t"))
        assert env.timestamp == TS
        assert (env.type, env.app_id, env.priority, env.content_encoding) == ("order.created", "svc", 5, "gzip")
        assert env.delivery_mode == 1 and env.correlation_id == "c"
        assert env.expiration is None  # the delay queue's TTL is the timing authority
        assert env.headers["x-rabbitkit-original-expiration"] == "60000"
        assert env.headers["traceparent"] == "00-ab-cd-01" and env.headers["x-idempotency-key"] == "k"
        assert env.headers["x-rabbitkit-failure-category"] == "timeout-unknown"

    def test_unknown_delivery_mode_is_persistent(self) -> None:
        env = _mw()._build_retry_envelope(_message(), 0, ConnectionError("x"))
        assert env.delivery_mode == 2

    def test_id_less_message_gets_one_stable_id_across_attempts(self) -> None:
        mw = _mw()
        first = mw._build_retry_envelope(_message(message_id=None), 0, ConnectionError("x"))
        assert first.message_id and first.headers["x-rabbitkit-original-message-id"] == first.message_id
        # the next attempt is the delivered retry copy
        second_msg = _message(message_id=first.message_id, headers=dict(first.headers))
        second = mw._build_retry_envelope(second_msg, 1, ConnectionError("x"))
        assert second.message_id == first.message_id
        assert second.headers["x-rabbitkit-original-message-id"] == first.message_id
        dlq = mw._build_dlq_envelope(second_msg, 3, ValueError("x"), classify_error(ValueError("x")), _cat("permanent"))
        assert dlq is not None and dlq.message_id == first.message_id

    def test_count_and_first_failure_never_lowered(self) -> None:
        mw = _mw(max_retries=3)
        msg = _message(
            headers={
                "x-rabbitkit-original-queue": "orders",
                "x-rabbitkit-retry-count": 1,
                "x-rabbitkit-attempts": 7,
                "x-rabbitkit-first-failed-at": "2026-01-01T00:00:00+00:00",
            }
        )
        env = mw._build_retry_envelope(msg, 1, ConnectionError("x"))
        assert env.headers["x-rabbitkit-retry-count"] == 2
        assert env.headers["x-rabbitkit-attempts"] == 7
        assert env.headers["x-rabbitkit-first-failed-at"] == "2026-01-01T00:00:00+00:00"

    def test_xdeath_history_is_a_floor_for_a_reset_header(self) -> None:
        mw = _mw(max_retries=3)
        msg = _message(
            headers={
                "x-rabbitkit-original-queue": "orders",
                "x-rabbitkit-retry-count": 0,  # reset by some republish
                "x-death": [
                    {"queue": "orders.retry.2", "reason": "expired", "count": 1},
                    {"queue": b"orders.retry.1.s2", "reason": "expired", "count": 1},
                    {"queue": "other.retry.3", "reason": "expired", "count": 1},
                ],
            }
        )
        assert mw._get_retry_count(msg) == 2

    @pytest.mark.parametrize("raw", ["garbage", -5, None, True, b"x"])
    def test_malformed_count_reads_as_zero(self, raw: Any) -> None:
        msg = _message(headers={"x-rabbitkit-original-queue": "orders", "x-rabbitkit-retry-count": raw})
        assert _mw()._get_retry_count(msg) == 0


def _cat(value: str) -> Any:
    from rabbitkit.core.types import FailureCategory

    return FailureCategory(value)


# ── 13.2: retry policy ────────────────────────────────────────────────────


class TestRetryPolicy:
    def test_max_retry_age_exceeded_is_terminal(self) -> None:
        rec = _Recorder()
        mw = _mw(rec, max_retry_age=60)
        old = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        msg = _message(headers={"x-rabbitkit-original-queue": "orders", "x-rabbitkit-first-failed-at": old})
        with pytest.raises(ConnectionError):
            mw.consume_scope(lambda m: (_ for _ in ()).throw(ConnectionError("x")), msg)
        assert rec.envelopes[0].routing_key == "orders.dlq"
        assert rec.envelopes[0].headers["x-rabbitkit-failure-category"] == "retry-age-exceeded"

    def test_max_retry_age_within_budget_retries(self) -> None:
        rec = _Recorder()
        mw = _mw(rec, max_retry_age=3600)
        recent = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
        msg = _message(headers={"x-rabbitkit-original-queue": "orders", "x-rabbitkit-first-failed-at": recent})
        assert mw.consume_scope(lambda m: (_ for _ in ()).throw(ConnectionError("x")), msg) is REQUEUED_FOR_RETRY
        assert rec.envelopes[0].routing_key == "orders.retry.1"

    @pytest.mark.parametrize("bad", ["not-a-date", "2999-01-01T00:00:00+00:00"])
    def test_malformed_or_future_first_failure_never_exceeds(self, bad: str) -> None:
        rec = _Recorder()
        mw = _mw(rec, max_retry_age=1)
        msg = _message(headers={"x-rabbitkit-original-queue": "orders", "x-rabbitkit-first-failed-at": bad})
        assert mw.consume_scope(lambda m: (_ for _ in ()).throw(ConnectionError("x")), msg) is REQUEUED_FOR_RETRY

    def test_exponential_constructor(self) -> None:
        cfg = RetryConfig.exponential(max_retries=5, initial_delay=2, multiplier=3, max_delay=60, max_retry_age=900)
        assert cfg.delays == (2, 6, 18, 54, 60)
        assert cfg.max_retries == 5 and cfg.max_retry_age == 900

    @pytest.mark.parametrize(
        "kwargs", [{"initial_delay": 0}, {"multiplier": 0.5}, {"initial_delay": 10, "max_delay": 5}]
    )
    def test_exponential_validation(self, kwargs: dict[str, float]) -> None:
        with pytest.raises(ConfigValidationError):
            RetryConfig.exponential(**kwargs)  # type: ignore[arg-type]

    def test_policy_field_validation(self) -> None:
        with pytest.raises(ConfigValidationError):
            RetryConfig(max_retry_age=0)
        with pytest.raises(ConfigValidationError):
            RetryConfig(terminal_action="drop")  # type: ignore[arg-type]
        assert RetryConfig(terminal_action="reject").terminal_action is TerminalAction.REJECT  # type: ignore[arg-type]
        assert RetryConfig().terminal_action is TerminalAction.DEAD_LETTER

    def test_dead_letter_exchange_is_deprecated(self) -> None:
        with pytest.warns(DeprecationWarning, match="dead_letter_exchange"):
            RetryConfig(dead_letter_exchange="dlx")


# ── R-D3: brake before redelivery; exhausted pauses ───────────────────────


class TestHandoffBrake:
    @pytest.mark.asyncio
    async def test_backoff_happens_before_the_nack(self, monkeypatch: pytest.MonkeyPatch) -> None:
        events: list[str] = []

        async def fake_sleep(s: float) -> None:
            events.append(f"sleep:{s}")

        import rabbitkit.middleware.retry as retry_mod

        monkeypatch.setattr(retry_mod.asyncio, "sleep", fake_sleep)
        rec = _Recorder(PublishStatus.RETURNED)
        mw = _mw(rec, handoff=RetryHandoffConfig(jitter=0.0, backoff_initial=0.5))
        msg = _message()
        orig = msg._nack_async_fn

        async def nack(rq: bool) -> None:
            events.append("nack")
            assert orig is not None
            await orig(rq)

        msg._nack_async_fn = nack
        await mw._route_to_delay_queue_async(msg, 0, ConnectionError("x"))
        assert events == ["sleep:0.5", "nack"]

    @pytest.mark.asyncio
    async def test_cancelled_backoff_still_requeues(self) -> None:
        rec = _Recorder(PublishStatus.RETURNED)
        mw = _mw(rec, handoff=RetryHandoffConfig(jitter=0.0, backoff_initial=30.0, backoff_max=30.0))
        msg = _message()
        task = asyncio.ensure_future(mw._route_to_delay_queue_async(msg, 0, ConnectionError("x")))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            async with asyncio.timeout(5):
                await task
        assert _calls(msg) == [("nack", True)]

    @pytest.mark.asyncio
    async def test_exhausted_holds_for_backoff_max_and_logs_critical_once(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        slept: list[float] = []

        async def fake_sleep(s: float) -> None:
            slept.append(s)

        import rabbitkit.middleware.retry as retry_mod

        monkeypatch.setattr(retry_mod.asyncio, "sleep", fake_sleep)
        rec = _Recorder(PublishStatus.RETURNED)
        handoff = RetryHandoffConfig(jitter=0.0, backoff_initial=0.1, backoff_max=7.0, max_consecutive_failures=2)
        mw = _mw(rec, handoff=handoff)
        with caplog.at_level(logging.CRITICAL, logger="rabbitkit.middleware.retry"):
            for _ in range(4):
                await mw._route_to_delay_queue_async(_message(), 0, ConnectionError("x"))
        assert slept == [0.1, 7.0, 7.0, 7.0]
        assert sum("EXHAUSTED" in r.getMessage() for r in caplog.records) == 1
        # recovery re-arms the critical log
        rec.status = PublishStatus.CONFIRMED
        await mw._route_to_delay_queue_async(_message(), 0, ConnectionError("x"))
        assert not mw.handoff_tracker.is_paused
