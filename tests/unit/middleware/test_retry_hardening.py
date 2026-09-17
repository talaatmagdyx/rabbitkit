"""Plan §5 tests for RetryMiddleware hardening: sanitized terminal metadata,
configurable delay/DLQ queue types, and bounded retry-handoff failure handling."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.config import RetryConfig, RetryHandoffConfig
from rabbitkit.core.errors import ConfigValidationError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.retry_handoff import RetryHandoffTracker
from rabbitkit.core.sanitizer import ErrorSanitizer
from rabbitkit.core.types import HandoffState, MessageEnvelope, PublishOutcome, PublishStatus, QueueType
from rabbitkit.middleware.retry import RetryMiddleware, RetryRouter

SECRET = "s3cr3tP@ss"


def _message(**kw: Any) -> RabbitMessage:
    kw.setdefault("body", b"{}")
    kw.setdefault("headers", {"x-rabbitkit-original-queue": "orders"})
    kw.setdefault("routing_key", "orders")
    kw.setdefault("message_id", "m1")
    m = RabbitMessage(**kw)
    calls: list[tuple[str, Any]] = []
    m.raw_message = calls
    m._ack_fn = lambda: calls.append(("ack", None))
    m._nack_fn = lambda rq: calls.append(("nack", rq))
    m._reject_fn = lambda rq: calls.append(("reject", rq))

    async def _a() -> None:
        calls.append(("ack", None))

    async def _n(rq: bool) -> None:
        calls.append(("nack", rq))

    m._ack_async_fn = _a
    m._nack_async_fn = _n
    return m


def _calls(m: RabbitMessage) -> list[tuple[str, Any]]:
    return m.raw_message  # type: ignore[no-any-return]


# ── Config ────────────────────────────────────────────────────────────────


class TestRetryConfigDurabilityKnobs:
    def test_defaults_are_legacy(self) -> None:
        c = RetryConfig()
        assert c.delay_queue_type == "classic"
        assert c.dlq_queue_type == "inherit"
        assert c.error_detail == "sanitized"
        assert isinstance(c.handoff, RetryHandoffConfig)

    @pytest.mark.parametrize("field", ["delay_queue_type", "dlq_queue_type"])
    def test_queue_type_validation(self, field: str) -> None:
        with pytest.raises(ConfigValidationError):
            RetryConfig(**{field: "stream"})  # type: ignore[arg-type]
        for ok in ("classic", "quorum", "inherit"):
            RetryConfig(**{field: ok})  # type: ignore[arg-type]

    def test_error_detail_validation(self) -> None:
        with pytest.raises(ConfigValidationError):
            RetryConfig(error_detail="verbose")


# ── Sanitized terminal metadata (§5.3) ────────────────────────────────────


class TestSanitizedTriageHeaders:
    def test_default_redacts_secrets_and_adds_category(self) -> None:
        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)))
        env = mw._build_retry_envelope(
            _message(), retry_count=0, exc=ConnectionError(f"amqp://svc:{SECRET}@host/ refused")
        )
        h = env.headers
        assert SECRET not in h["x-rabbitkit-error-message"]
        assert "amqp://***:***@host/" in h["x-rabbitkit-error-message"]
        assert h["x-rabbitkit-error-type"] == "ConnectionError"
        assert h["x-rabbitkit-error-category"] == "transient"
        assert SECRET not in repr(h)

    def test_permanent_category(self) -> None:
        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)))
        env = mw._build_retry_envelope(_message(), retry_count=0, exc=ValueError("bad"))
        assert env.headers["x-rabbitkit-error-category"] == "permanent"

    def test_omit_policy_writes_no_message(self) -> None:
        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,), error_detail="omit"))
        env = mw._build_retry_envelope(_message(), retry_count=0, exc=ValueError(f"password={SECRET}"))
        assert "x-rabbitkit-error-message" not in env.headers
        assert env.headers["x-rabbitkit-error-type"] == "ValueError"

    def test_raw_policy_is_legacy(self) -> None:
        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,), error_detail="raw"))
        env = mw._build_retry_envelope(_message(), retry_count=0, exc=ValueError(f"password={SECRET}"))
        assert env.headers["x-rabbitkit-error-message"] == f"password={SECRET}"

    def test_stale_raw_message_header_from_previous_attempt_is_dropped(self) -> None:
        mw = RetryMiddleware(RetryConfig(max_retries=2, delays=(5, 5), error_detail="omit"))
        msg = _message(headers={"x-rabbitkit-original-queue": "orders", "x-rabbitkit-error-message": f"leak {SECRET}"})
        env = mw._build_retry_envelope(msg, retry_count=1, exc=ValueError("again"))
        assert "x-rabbitkit-error-message" not in env.headers

    def test_custom_sanitizer_injected(self) -> None:
        san = ErrorSanitizer(policy="sanitized", allowed_codes=frozenset({"TimeoutError"}))
        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)), sanitizer=san)
        assert mw.sanitizer is san
        env = mw._build_retry_envelope(_message(), retry_count=0, exc=KeyError("k"))
        assert env.headers["x-rabbitkit-error-type"] == "ApplicationError"

    def test_first_and_last_failed_preserved(self) -> None:
        mw = RetryMiddleware(RetryConfig(max_retries=2, delays=(5, 5)))
        env1 = mw._build_retry_envelope(_message(), retry_count=0, exc=ValueError("a"))
        first = env1.headers["x-rabbitkit-first-failed-at"]
        env2 = mw._build_retry_envelope(_message(headers=dict(env1.headers)), retry_count=1, exc=ValueError("b"))
        assert env2.headers["x-rabbitkit-first-failed-at"] == first
        assert env2.headers["x-rabbitkit-error-message"] == "b"


# ── Retry topology queue types (§5.1) ─────────────────────────────────────


class TestRetryRouterQueueTypes:
    def _types(self, cfg: RetryConfig, source: QueueType | None) -> tuple[list[str], QueueType]:
        router = RetryRouter(cfg)
        queues = router.get_delay_queue_definitions("orders", "ex", source_queue_type=source)
        delays = [str(q.arguments["x-queue-type"]) for q in queues if q.name != "orders.dlq"]
        dlq = next(q for q in queues if q.name == "orders.dlq")
        return delays, dlq.queue_type

    def test_legacy_defaults_classic_delay_inherit_dlq(self) -> None:
        cfg = RetryConfig(max_retries=2, delays=(1, 2))
        delays, dlq = self._types(cfg, QueueType.CLASSIC)
        assert delays == ["classic", "classic"] and dlq is QueueType.CLASSIC
        delays, dlq = self._types(cfg, QueueType.QUORUM)
        assert delays == ["classic", "classic"] and dlq is QueueType.QUORUM

    def test_quorum_delay_chain(self) -> None:
        cfg = RetryConfig(max_retries=2, delays=(1, 2), delay_queue_type="quorum")
        delays, _ = self._types(cfg, QueueType.CLASSIC)
        assert delays == ["quorum", "quorum"]

    def test_inherit_delay_follows_source(self) -> None:
        cfg = RetryConfig(max_retries=1, delays=(1,), delay_queue_type="inherit")
        assert self._types(cfg, QueueType.QUORUM)[0] == ["quorum"]
        assert self._types(cfg, QueueType.CLASSIC)[0] == ["classic"]
        # a stream source cannot be a delay target type → default classic
        assert self._types(cfg, QueueType.STREAM)[0] == ["classic"]
        assert self._types(cfg, None)[0] == ["classic"]

    def test_explicit_dlq_type(self) -> None:
        cfg = RetryConfig(max_retries=1, delays=(1,), dlq_queue_type="quorum")
        assert self._types(cfg, QueueType.CLASSIC)[1] is QueueType.QUORUM
        cfg = RetryConfig(max_retries=1, delays=(1,), dlq_queue_type="classic")
        assert self._types(cfg, QueueType.QUORUM)[1] is QueueType.CLASSIC

    def test_sharded_delay_queues_share_type(self) -> None:
        cfg = RetryConfig(
            max_retries=1, delays=(10,), jitter_mode="sharded", jitter_shards=3, delay_queue_type="quorum"
        )
        delays, _ = self._types(cfg, QueueType.CLASSIC)
        assert delays == ["quorum"] * 3


# ── Retry handoff failures (§5.2) ─────────────────────────────────────────


def _failing_publish(status: PublishStatus = PublishStatus.RETURNED) -> MagicMock:
    return MagicMock(return_value=PublishOutcome(status=status))


class TestHandoffFailureSync:
    def test_failed_handoff_nacks_and_records(self) -> None:
        hooks: list[tuple[float, RetryHandoffTracker]] = []
        cfg = RetryConfig(max_retries=3, delays=(1, 1, 1), handoff=RetryHandoffConfig(jitter=0.0, backoff_initial=0.25))
        mw = RetryMiddleware(cfg, publish_fn=_failing_publish(), on_handoff_failure=lambda b, t: hooks.append((b, t)))
        msg = _message()
        mw._route_to_delay_queue_sync(msg, retry_count=0, exc=ConnectionError("down"))
        assert _calls(msg) == [("nack", True)]  # never acked
        assert mw.handoff_tracker.consecutive_failures == 1
        assert mw.handoff_tracker.state is HandoffState.DEGRADED
        assert hooks == [(0.25, mw.handoff_tracker)]

    def test_raised_publish_is_a_handoff_failure(self) -> None:
        pub = MagicMock(side_effect=ConnectionResetError("socket"))
        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(1,)), publish_fn=pub)
        msg = _message()
        mw._route_to_delay_queue_sync(msg, retry_count=0, exc=ConnectionError("down"))
        assert _calls(msg) == [("nack", True)]
        assert mw.handoff_tracker.total_failures == 1

    def test_success_resets_streak(self) -> None:
        outcomes = iter([PublishOutcome(status=PublishStatus.RETURNED), PublishOutcome(status=PublishStatus.CONFIRMED)])
        mw = RetryMiddleware(RetryConfig(max_retries=2, delays=(1, 1)), publish_fn=lambda e: next(outcomes))
        m1, m2 = _message(), _message()
        mw._route_to_delay_queue_sync(m1, retry_count=0, exc=ConnectionError("x"))
        mw._route_to_delay_queue_sync(m2, retry_count=0, exc=ConnectionError("x"))
        assert _calls(m2) == [("ack", None)]
        assert mw.handoff_tracker.state is HandoffState.HEALTHY
        assert mw.handoff_tracker.total_failures == 1

    def test_exhausted_hook_fires(self) -> None:
        exhausted: list[RetryHandoffTracker] = []
        cfg = RetryConfig(
            max_retries=1, delays=(1,), handoff=RetryHandoffConfig(max_consecutive_failures=2, jitter=0.0)
        )
        mw = RetryMiddleware(cfg, publish_fn=_failing_publish(), on_handoff_exhausted=exhausted.append)
        for _ in range(2):
            mw._route_to_delay_queue_sync(_message(), retry_count=0, exc=ConnectionError("x"))
        assert exhausted == [mw.handoff_tracker]
        assert mw.handoff_tracker.is_exhausted

    def test_hook_exception_does_not_break_flow(self) -> None:
        def bad_hook(b: float, t: RetryHandoffTracker) -> None:
            raise RuntimeError("hook")

        mw = RetryMiddleware(
            RetryConfig(max_retries=1, delays=(1,)), publish_fn=_failing_publish(), on_handoff_failure=bad_hook
        )
        msg = _message()
        mw._route_to_delay_queue_sync(msg, retry_count=0, exc=ConnectionError("x"))
        assert _calls(msg) == [("nack", True)]

    def test_metrics_counter_and_gauge(self) -> None:
        collector = MagicMock()
        cfg_metrics = MagicMock()
        cfg_metrics.retry_handoff_failures_total = "rk_retry_handoff_failures_total"
        cfg_metrics.retry_handoff_paused = "rk_retry_handoff_paused"
        cfg_metrics.messages_retried_total = "rk_retried"
        outcomes = iter([PublishOutcome(status=PublishStatus.RETURNED), PublishOutcome(status=PublishStatus.CONFIRMED)])
        mw = RetryMiddleware(
            RetryConfig(max_retries=2, delays=(1, 1)),
            publish_fn=lambda e: next(outcomes),
            metrics_collector=collector,
            metrics_config=cfg_metrics,
        )
        mw._route_to_delay_queue_sync(_message(), retry_count=0, exc=ConnectionError("x"))
        collector.inc_counter.assert_any_call("rk_retry_handoff_failures_total", {"queue": "orders"})
        collector.set_gauge.assert_any_call("rk_retry_handoff_paused", {"queue": "orders"}, 1.0)
        mw._route_to_delay_queue_sync(_message(), retry_count=0, exc=ConnectionError("x"))
        collector.set_gauge.assert_any_call("rk_retry_handoff_paused", {"queue": "orders"}, 0.0)

    def test_sync_path_never_sleeps(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import time as _time

        slept: list[float] = []
        monkeypatch.setattr(_time, "sleep", lambda s: slept.append(s))
        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(1,)), publish_fn=_failing_publish())
        mw._route_to_delay_queue_sync(_message(), retry_count=0, exc=ConnectionError("x"))
        assert slept == []


class TestHandoffFailureAsync:
    async def test_async_backoff_is_awaited(self, monkeypatch: pytest.MonkeyPatch) -> None:
        slept: list[float] = []

        async def fake_sleep(s: float) -> None:
            slept.append(s)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        async def pub(e: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.TIMEOUT)

        cfg = RetryConfig(max_retries=1, delays=(1,), handoff=RetryHandoffConfig(jitter=0.0, backoff_initial=0.7))
        mw = RetryMiddleware(cfg, publish_async_fn=pub)
        msg = _message()
        await mw._route_to_delay_queue_async(msg, retry_count=0, exc=ConnectionError("x"))
        assert _calls(msg) == [("nack", True)]
        assert slept == [0.7]

    async def test_async_sleep_can_be_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        slept: list[float] = []

        async def fake_sleep(s: float) -> None:
            slept.append(s)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        async def pub(e: MessageEnvelope) -> PublishOutcome:
            raise RuntimeError("boom")

        mw = RetryMiddleware(
            RetryConfig(max_retries=1, delays=(1,)), publish_async_fn=pub, sleep_on_handoff_failure=False
        )
        msg = _message()
        await mw._route_to_delay_queue_async(msg, retry_count=0, exc=ConnectionError("x"))
        assert _calls(msg) == [("nack", True)] and slept == []
        assert mw.handoff_tracker.total_failures == 1

    async def test_async_success_acks_and_resets(self) -> None:
        async def pub(e: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(1,)), publish_async_fn=pub)
        mw.handoff_tracker.record_failure()
        msg = _message()
        await mw._route_to_delay_queue_async(msg, retry_count=0, exc=ConnectionError("x"))
        assert _calls(msg) == [("ack", None)]
        assert mw.handoff_tracker.state is HandoffState.HEALTHY

    def test_injected_tracker(self) -> None:
        tracker = RetryHandoffTracker(RetryHandoffConfig())
        mw = RetryMiddleware(RetryConfig(), handoff_tracker=tracker)
        assert mw.handoff_tracker is tracker
