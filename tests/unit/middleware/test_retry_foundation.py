"""RetryMiddleware: wire body on retry/DLQ copies (G-07), the failure
taxonomy on headers and metrics (G-16), producer-set retry counts (G-19),
and the terminal handoff composed with ExceptionMiddleware."""

from __future__ import annotations

import gzip
from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.config import CompressionConfig, MetricsConfig, RetryConfig
from rabbitkit.core.errors import HandlerTimeoutError, InfrastructureError, InvalidMessageError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.middleware.compression import CompressionMiddleware
from rabbitkit.middleware.exception import ExceptionMiddleware
from rabbitkit.middleware.retry import RetryMiddleware

PAYLOAD = b"a" * 20_000


def _message(**kw: Any) -> RabbitMessage:
    kw.setdefault("body", b"{}")
    kw.setdefault("headers", {"x-rabbitkit-original-queue": "orders"})
    kw.setdefault("routing_key", "orders")
    kw.setdefault("message_id", "m1")
    m = RabbitMessage(**kw)
    settled: list[tuple[str, Any]] = []
    m.raw_message = settled
    m._ack_fn = lambda: settled.append(("ack", None))
    m._nack_fn = lambda rq: settled.append(("nack", rq))
    m._reject_fn = lambda rq: settled.append(("reject", rq))

    async def _a() -> None:
        settled.append(("ack", None))

    async def _n(rq: bool) -> None:
        settled.append(("nack", rq))

    m._ack_async_fn, m._nack_async_fn = _a, _n
    return m


class _Recorder:
    def __init__(self, status: PublishStatus = PublishStatus.CONFIRMED) -> None:
        self.status = status
        self.envelopes: list[MessageEnvelope] = []

    def __call__(self, env: MessageEnvelope) -> PublishOutcome:
        self.envelopes.append(env)
        return PublishOutcome(status=self.status)

    async def async_(self, env: MessageEnvelope) -> PublishOutcome:
        return self(env)


def _mw(rec: _Recorder, **cfg: Any) -> RetryMiddleware:
    cfg.setdefault("max_retries", 2)
    cfg.setdefault("delays", (1, 2))
    return RetryMiddleware(RetryConfig(**cfg), publish_fn=rec, publish_async_fn=rec.async_)


def _compressed_message(**kw: Any) -> RabbitMessage:
    wire = gzip.compress(PAYLOAD)
    msg = _message(body=wire, content_encoding="gzip", **kw)
    CompressionMiddleware(CompressionConfig()).on_receive(msg)  # decompresses in place
    assert msg.body == PAYLOAD and msg.content_encoding == "gzip"
    return msg


def _raise(exc: Exception) -> Any:
    def handler(_m: RabbitMessage) -> Any:
        raise exc

    return handler


# ── G-07: retry and DLQ copies carry the wire body ────────────────────────


class TestWireBody:
    def test_message_keeps_the_delivered_body_and_encoding(self) -> None:
        msg = _message(body=b"wire", content_encoding="gzip")
        msg.body = b"decoded"
        msg.content_encoding = None
        assert msg.wire_body == b"wire"
        assert msg.wire_content_encoding == "gzip"

    def test_retry_copy_of_a_decompressed_message_is_still_decodable(self) -> None:
        rec = _Recorder()
        msg = _compressed_message()
        _mw(rec).consume_scope(_raise(OSError("transient")), msg)
        (env,) = rec.envelopes
        assert env.routing_key == "orders.retry.1"
        assert env.content_encoding == "gzip"
        assert gzip.decompress(env.body) == PAYLOAD

    async def test_terminal_dlq_copy_of_a_decompressed_message_is_decodable(self) -> None:
        rec = _Recorder()
        msg = _compressed_message()
        with pytest.raises(ValueError):
            await _mw(rec).consume_scope_async(_raise(ValueError("bad")), msg)
        (env,) = rec.envelopes
        assert env.routing_key == "orders.dlq"
        assert env.content_encoding == "gzip"
        assert gzip.decompress(env.body) == PAYLOAD

    def test_redelivered_retry_copy_decompresses_again(self) -> None:
        rec = _Recorder()
        _mw(rec).consume_scope(_raise(OSError("transient")), _compressed_message())
        env = rec.envelopes[0]
        again = _message(body=env.body, content_encoding=env.content_encoding, headers=dict(env.headers))
        CompressionMiddleware(CompressionConfig()).on_receive(again)
        assert again.body == PAYLOAD


# ── G-16: category on headers and metric labels ───────────────────────────


class TestCategoryHeaders:
    @pytest.mark.parametrize(
        ("exc", "category"),
        [
            (OSError("x"), "retryable"),
            (InfrastructureError("db down"), "infrastructure"),
            (HandlerTimeoutError(1.0), "unknown_outcome"),
        ],
    )
    def test_retry_copy_records_the_category(self, exc: Exception, category: str) -> None:
        rec = _Recorder()
        _mw(rec).consume_scope(_raise(exc), _message())
        assert rec.envelopes[0].headers["x-rabbitkit-error-category"] == category

    @pytest.mark.parametrize(
        ("exc", "category"),
        [(ValueError("x"), "permanent"), (InvalidMessageError("no schema"), "invalid_message")],
    )
    def test_dlq_copy_records_the_category_of_a_first_attempt_failure(self, exc: Exception, category: str) -> None:
        rec = _Recorder()
        with pytest.raises(type(exc)):
            _mw(rec).consume_scope(_raise(exc), _message())
        h = rec.envelopes[0].headers
        assert h["x-rabbitkit-error-category"] == category
        assert h["x-rabbitkit-failure-category"] == "permanent"
        assert h["x-rabbitkit-attempts"] == 1

    def test_metrics_are_labelled_with_the_category(self) -> None:
        collector = MagicMock()
        rec = _Recorder()
        mw = RetryMiddleware(
            RetryConfig(max_retries=1, delays=(1,)),
            publish_fn=rec,
            metrics_collector=collector,
            metrics_config=MetricsConfig(),
        )
        mw.consume_scope(_raise(InfrastructureError("db")), _message())
        with pytest.raises(InvalidMessageError):
            mw.consume_scope(_raise(InvalidMessageError("bad")), _message())
        calls = [c.args for c in collector.inc_counter.call_args_list]
        assert ("rabbitkit_messages_retried_total", {"queue": "orders", "category": "infrastructure"}) in calls
        assert (
            "rabbitkit_messages_dead_lettered_total",
            {"queue": "orders", "category": "invalid_message"},
        ) in calls


# ── G-19: a producer-set retry count ──────────────────────────────────────


class TestRetryCountTrust:
    def _forged(self, **headers: Any) -> RabbitMessage:
        return _message(headers={"x-rabbitkit-original-queue": "orders", "x-rabbitkit-retry-count": 2, **headers})

    def test_default_trusts_the_header(self) -> None:
        rec = _Recorder()
        with pytest.raises(OSError):
            _mw(rec).consume_scope(_raise(OSError("x")), self._forged())
        assert rec.envelopes[0].routing_key == "orders.dlq"

    def test_untrusted_header_cannot_skip_retries(self) -> None:
        rec = _Recorder()
        _mw(rec, trust_retry_header=False).consume_scope(_raise(OSError("x")), self._forged())
        env = rec.envelopes[0]
        assert env.routing_key == "orders.retry.1"
        # the copy still reports a count, and never lowers the claim
        assert env.headers["x-rabbitkit-retry-count"] == 2

    def test_untrusted_header_still_counts_broker_history(self) -> None:
        rec = _Recorder()
        msg = self._forged(**{"x-death": [{"queue": "orders.retry.2", "reason": "expired", "count": 1}]})
        with pytest.raises(OSError):
            _mw(rec, trust_retry_header=False).consume_scope(_raise(OSError("x")), msg)
        assert rec.envelopes[0].routing_key == "orders.dlq"
        assert rec.envelopes[0].headers["x-rabbitkit-failure-category"] == "exhausted"

    def test_timestamp_survives_retry_and_dlq_copies(self) -> None:
        from datetime import UTC, datetime

        ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        rec = _Recorder()
        mw = _mw(rec, max_retries=1, delays=(1,))
        mw.consume_scope(_raise(OSError("x")), _message(timestamp=ts))
        with pytest.raises(ValueError):
            mw.consume_scope(_raise(ValueError("x")), _message(timestamp=ts))
        assert [e.timestamp for e in rec.envelopes] == [ts, ts]


# ── terminal handoff composed with ExceptionMiddleware ────────────────────


class TestTerminalHandoffComposition:
    def _chain(self, rec: _Recorder, exc_mw: ExceptionMiddleware) -> Any:
        retry = _mw(rec)

        def run(msg: RabbitMessage, exc: Exception) -> Any:
            return exc_mw.consume_scope(lambda m: retry.consume_scope(_raise(exc), m), msg)

        return run

    def test_failed_dlq_copy_is_never_swallowed(self) -> None:
        rec = _Recorder(PublishStatus.RETURNED)
        msg = _message()
        run = self._chain(rec, ExceptionMiddleware(swallow_permanent=True))
        with pytest.raises(ValueError) as info:
            run(msg, ValueError("bad"))
        assert info.value._rabbitkit_dead_letter_failed is True  # type: ignore[attr-defined]
        assert info.value._rabbitkit_dead_lettered is False  # type: ignore[attr-defined]
        assert msg.raw_message == []  # left for the pipeline to settle

    def test_failed_dlq_copy_bypasses_a_fallback_handler(self) -> None:
        exc_mw = ExceptionMiddleware(swallow_permanent=True)
        exc_mw.add_handler(ValueError, lambda e: "fallback")
        run = self._chain(_Recorder(PublishStatus.NACKED), exc_mw)
        with pytest.raises(ValueError):
            run(_message(), ValueError("bad"))

    def test_confirmed_dlq_copy_can_still_be_swallowed(self) -> None:
        rec = _Recorder()
        msg = _message()
        run = self._chain(rec, ExceptionMiddleware(swallow_permanent=True))
        assert run(msg, ValueError("bad")) is None
        assert msg.raw_message == [("ack", None)]
        assert rec.envelopes[0].routing_key == "orders.dlq"

    def test_reject_action_is_not_a_failed_handoff(self) -> None:
        rec = _Recorder()
        retry_reject = RetryMiddleware(
            RetryConfig(max_retries=1, delays=(1,), terminal_action="reject"), publish_fn=rec
        )
        with pytest.raises(ValueError) as info:
            retry_reject.consume_scope(_raise(ValueError("bad")), _message())
        assert info.value._rabbitkit_dead_letter_failed is False  # type: ignore[attr-defined]
        assert rec.envelopes == []
