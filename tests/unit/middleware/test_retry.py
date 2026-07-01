"""Tests for middleware/retry.py — RetryMiddleware and RetryRouter."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rabbitkit.core.config import RetryConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import (
    ErrorSeverity,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
)
from rabbitkit.middleware.retry import RetryMiddleware, RetryRouter

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b'{"id": 1}',
        "routing_key": "orders.created",
        "exchange": "orders",
        "headers": {"x-rabbitkit-original-queue": "orders-queue"},
    }
    defaults.update(kwargs)
    msg = RabbitMessage(**defaults)  # type: ignore[arg-type]
    ack_fn = MagicMock()
    msg._ack_fn = ack_fn
    msg._nack_fn = MagicMock()
    msg._reject_fn = MagicMock()
    return msg


# ── RetryMiddleware — transient retry ────────────────────────────────────


class TestRetryTransient:
    def test_transient_routes_to_delay_queue(self) -> None:
        """Transient errors with retries left → publish to delay queue + ack source."""
        published: list[MessageEnvelope] = []

        def capture_publish(env: MessageEnvelope) -> None:
            published.append(env)

        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(config, publish_fn=capture_publish)

        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("connection lost")

        mw.consume_scope(failing_handler, msg)

        # Should publish to delay queue
        assert len(published) == 1
        assert "retry.1" in published[0].routing_key
        # Source message should be acked
        assert msg.is_settled
        assert msg._disposition == "acked"

    def test_transient_publish_failure_nacks_not_acks(self) -> None:
        """Delay-queue publish failure → NACK source for redelivery, never ack.

        Acking on a failed publish would lose the message permanently (it never
        reached the delay queue and was removed from the source queue).
        """
        config = RetryConfig(max_retries=3, delays=(5, 30, 120))

        def failing_publish(env: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(
                status=PublishStatus.ERROR,
                exchange=env.exchange,
                routing_key=env.routing_key,
            )

        mw = RetryMiddleware(config, publish_fn=failing_publish)
        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("connection lost")

        mw.consume_scope(failing_handler, msg)

        assert msg._disposition == "nacked"
        msg._nack_fn.assert_called_once_with(True)
        msg._ack_fn.assert_not_called()

    def test_retry_increments_header(self) -> None:
        """Retry count is incremented in the envelope headers."""
        published: list[MessageEnvelope] = []

        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(config, publish_fn=lambda env: published.append(env))

        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)

        assert published[0].headers["x-rabbitkit-retry-count"] == 1

    def test_retry_preserves_original_headers(self) -> None:
        """Original exchange/routing_key/queue are preserved in headers."""
        published: list[MessageEnvelope] = []

        config = RetryConfig(max_retries=3, delays=(5,), strict_delays=False)
        mw = RetryMiddleware(config, publish_fn=lambda env: published.append(env))

        msg = _make_message(exchange="my-exchange", routing_key="my.rk")

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)

        env = published[0]
        assert env.headers["x-rabbitkit-original-exchange"] == "my-exchange"
        assert env.headers["x-rabbitkit-original-routing-key"] == "my.rk"

    def test_retry_per_queue_naming(self) -> None:
        """per_queue=True uses source queue name in delay queue routing key."""
        published: list[MessageEnvelope] = []

        config = RetryConfig(max_retries=2, delays=(5, 30), per_queue=True)
        mw = RetryMiddleware(config, publish_fn=lambda env: published.append(env))

        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders-queue"})

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)

        assert published[0].routing_key == "orders-queue.retry.1"

    def test_retry_shared_naming(self) -> None:
        """per_queue=False uses shared 'rabbitkit' prefix."""
        published: list[MessageEnvelope] = []

        config = RetryConfig(max_retries=2, delays=(5,), per_queue=False, strict_delays=False)
        mw = RetryMiddleware(config, publish_fn=lambda env: published.append(env))

        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)

        assert published[0].routing_key == "rabbitkit.retry.1"

    def test_second_retry_uses_correct_attempt(self) -> None:
        """Message on retry attempt 1 → routes to retry.2."""
        published: list[MessageEnvelope] = []

        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(config, publish_fn=lambda env: published.append(env))

        msg = _make_message(
            headers={
                "x-rabbitkit-retry-count": 1,
                "x-rabbitkit-original-queue": "orders-queue",
                "x-rabbitkit-original-exchange": "orders",
                "x-rabbitkit-original-routing-key": "orders.created",
            }
        )

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost again")

        mw.consume_scope(failing_handler, msg)

        assert published[0].routing_key == "orders-queue.retry.2"
        assert published[0].headers["x-rabbitkit-retry-count"] == 2


# ── RetryMiddleware — terminal cases ─────────────────────────────────────


class TestRetryTerminal:
    def test_permanent_error_raises_terminal(self) -> None:
        """Permanent errors are tagged terminal and re-raised."""
        config = RetryConfig(max_retries=3, delays=(5,), strict_delays=False)
        mw = RetryMiddleware(config)

        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ValueError("bad payload")

        with pytest.raises(ValueError, match="bad payload") as exc_info:
            mw.consume_scope(failing_handler, msg)

        assert getattr(exc_info.value, "_rabbitkit_terminal", False) is True

    def test_retries_exhausted_raises_terminal(self) -> None:
        """Transient error with retries exhausted → terminal."""
        config = RetryConfig(max_retries=2, delays=(5, 30))
        mw = RetryMiddleware(config)

        # Message already at retry count 2 (exhausted for max_retries=2)
        msg = _make_message(
            headers={
                "x-rabbitkit-retry-count": 2,
                "x-rabbitkit-original-queue": "orders-queue",
            }
        )

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("still failing")

        with pytest.raises(ConnectionResetError) as exc_info:
            mw.consume_scope(failing_handler, msg)

        assert getattr(exc_info.value, "_rabbitkit_terminal", False) is True

    def test_success_passes_through(self) -> None:
        """Successful handler calls pass through unchanged."""
        config = RetryConfig(max_retries=3, delays=(5,), strict_delays=False)
        mw = RetryMiddleware(config)

        msg = _make_message()

        def handler(m: RabbitMessage) -> str:
            return "ok"

        result = mw.consume_scope(handler, msg)
        assert result == "ok"

    def test_source_not_acked_on_terminal(self) -> None:
        """Terminal failures do NOT ack the source (pipeline handles settlement)."""
        config = RetryConfig(max_retries=1, delays=(5,))
        mw = RetryMiddleware(config)

        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ValueError("bad")

        with pytest.raises(ValueError):
            mw.consume_scope(failing_handler, msg)

        # Source message should NOT be settled by retry middleware
        assert not msg.is_settled


# ── H5: retry-count header is producer-spoofable — clamp + no negative rk ─


class TestRetryCountSpoofing:
    """H5: a producer-supplied x-rabbitkit-retry-count header must not be
    able to force unbounded retries (negative value) or skip straight to the
    DLQ (huge value) beyond what max_retries allows."""

    def test_negative_retry_count_clamps_to_zero_and_retries_normally(self) -> None:
        """H5 exact spec: x-rabbitkit-retry-count = -5 must clamp to 0, not
        produce a negative delay-queue routing key, and route to the FIRST
        delay queue like a fresh message — not reset to unbounded retries."""
        published: list[MessageEnvelope] = []
        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(config, publish_fn=lambda env: published.append(env))

        msg = _make_message(headers={"x-rabbitkit-retry-count": -5, "x-rabbitkit-original-queue": "orders-queue"})

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)

        assert len(published) == 1
        # attempt = clamped retry_count(0) + 1 = 1 -> a real, declared delay
        # queue, never "...retry.-4" or similar.
        assert published[0].routing_key == "orders-queue.retry.1"
        assert "retry.-" not in published[0].routing_key
        assert published[0].headers["x-rabbitkit-retry-count"] == 1
        assert msg._disposition == "acked"

    def test_huge_retry_count_clamps_to_max_retries_and_routes_terminal(self) -> None:
        """H5 exact spec: x-rabbitkit-retry-count = 10**9 must clamp to
        max_retries and be treated as exhausted (terminal), not silently
        accepted as a valid attempt count."""
        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(config)

        msg = _make_message(
            headers={"x-rabbitkit-retry-count": 10**9, "x-rabbitkit-original-queue": "orders-queue"}
        )

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("still failing")

        with pytest.raises(ConnectionResetError) as exc_info:
            mw.consume_scope(failing_handler, msg)

        assert getattr(exc_info.value, "_rabbitkit_terminal", False) is True

    def test_get_retry_count_clamps_negative_to_zero(self) -> None:
        config = RetryConfig(max_retries=4)
        mw = RetryMiddleware(config)
        msg = _make_message(headers={"x-rabbitkit-retry-count": -5})
        assert mw._get_retry_count(msg) == 0

    def test_get_retry_count_clamps_huge_value_to_max_retries(self) -> None:
        config = RetryConfig(max_retries=4)
        mw = RetryMiddleware(config)
        msg = _make_message(headers={"x-rabbitkit-retry-count": 10**9})
        assert mw._get_retry_count(msg) == 4

    def test_get_retry_count_within_range_unchanged(self) -> None:
        config = RetryConfig(max_retries=4)
        mw = RetryMiddleware(config)
        msg = _make_message(headers={"x-rabbitkit-retry-count": 2})
        assert mw._get_retry_count(msg) == 2

    def test_get_retry_count_missing_header_defaults_to_zero(self) -> None:
        config = RetryConfig(max_retries=4)
        mw = RetryMiddleware(config)
        msg = _make_message(headers={})
        assert mw._get_retry_count(msg) == 0

    def test_get_retry_count_non_numeric_header_treated_as_zero(self) -> None:
        """A garbage (non-numeric) header must not crash the pipeline — it
        degrades to 0 rather than raising ValueError inside error handling."""
        config = RetryConfig(max_retries=4)
        mw = RetryMiddleware(config)
        msg = _make_message(headers={"x-rabbitkit-retry-count": "not-a-number"})
        assert mw._get_retry_count(msg) == 0

    def test_non_numeric_header_does_not_crash_consume_scope(self) -> None:
        """End-to-end: a garbage header must not raise from inside the retry
        middleware itself while handling the original exception."""
        published: list[MessageEnvelope] = []
        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(config, publish_fn=lambda env: published.append(env))
        msg = _make_message(
            headers={"x-rabbitkit-retry-count": "garbage", "x-rabbitkit-original-queue": "orders-queue"}
        )

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)  # must not raise

        assert len(published) == 1
        assert published[0].routing_key == "orders-queue.retry.1"


# ── RetryMiddleware — classification ─────────────────────────────────────


class TestRetryClassification:
    def test_unknown_error_uses_config_policy(self) -> None:
        """Unknown errors use the config's unknown_policy (default PERMANENT)."""
        config = RetryConfig(max_retries=3, delays=(5,), unknown_policy=ErrorSeverity.PERMANENT, strict_delays=False)
        mw = RetryMiddleware(config)

        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise RuntimeError("unknown issue")

        with pytest.raises(RuntimeError) as exc_info:
            mw.consume_scope(failing_handler, msg)

        assert getattr(exc_info.value, "_rabbitkit_terminal", False) is True

    def test_unknown_error_transient_policy(self) -> None:
        """Unknown errors treated as transient when configured."""
        published: list[MessageEnvelope] = []

        config = RetryConfig(max_retries=3, delays=(5,), unknown_policy=ErrorSeverity.TRANSIENT, strict_delays=False)
        mw = RetryMiddleware(config, publish_fn=lambda env: published.append(env))

        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise RuntimeError("unknown but retryable")

        mw.consume_scope(failing_handler, msg)

        assert len(published) == 1  # retried, not terminal


# ── RetryMiddleware — async ──────────────────────────────────────────────


class TestRetryAsync:
    @pytest.mark.asyncio
    async def test_async_transient_routes_to_delay_queue(self) -> None:
        """Async: transient error → delay queue + ack source."""
        published: list[MessageEnvelope] = []

        async def capture_publish(env: MessageEnvelope) -> None:
            published.append(env)

        config = RetryConfig(max_retries=3, delays=(5,), strict_delays=False)
        mw = RetryMiddleware(config, publish_async_fn=capture_publish)

        msg = _make_message()
        # Set async ack fn since we're testing async path
        ack_called = False

        async def async_ack() -> None:
            nonlocal ack_called
            ack_called = True

        msg._ack_async_fn = async_ack
        msg._ack_fn = None  # Force async path

        async def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        await mw.consume_scope_async(failing_handler, msg)

        assert len(published) == 1
        assert msg.is_settled
        assert ack_called

    @pytest.mark.asyncio
    async def test_async_permanent_raises_terminal(self) -> None:
        """Async: permanent error → terminal, re-raised."""
        config = RetryConfig(max_retries=3, delays=(5,), strict_delays=False)
        mw = RetryMiddleware(config)

        msg = _make_message()

        async def failing_handler(m: RabbitMessage) -> None:
            raise ValueError("bad")

        with pytest.raises(ValueError) as exc_info:
            await mw.consume_scope_async(failing_handler, msg)

        assert getattr(exc_info.value, "_rabbitkit_terminal", False) is True

    @pytest.mark.asyncio
    async def test_async_success_passes_through(self) -> None:
        """Async: successful handler returns normally."""
        config = RetryConfig(max_retries=3, delays=(5,), strict_delays=False)
        mw = RetryMiddleware(config)

        msg = _make_message()

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        result = await mw.consume_scope_async(handler, msg)
        assert result == "ok"


# ── RetryMiddleware — config property ────────────────────────────────────


class TestRetryConfig:
    def test_config_property(self) -> None:
        config = RetryConfig(max_retries=5, strict_delays=False)
        mw = RetryMiddleware(config)
        assert mw.config is config
        assert mw.config.max_retries == 5


# ── RetryMiddleware — delay computation ──────────────────────────────────


class TestRetryDelay:
    def test_delay_uses_correct_index(self) -> None:
        """Delay index maps correctly to delays tuple."""
        config = RetryConfig(max_retries=4, delays=(5, 30, 120, 600), jitter_factor=0.0)
        mw = RetryMiddleware(config)

        # With zero jitter, delay should be exact
        assert mw._compute_delay(0) == 5
        assert mw._compute_delay(1) == 30
        assert mw._compute_delay(2) == 120
        assert mw._compute_delay(3) == 600

    def test_delay_clamps_to_last(self) -> None:
        """Retry count beyond delays length clamps to last delay."""
        config = RetryConfig(max_retries=10, delays=(5, 30), jitter_factor=0.0, strict_delays=False)
        mw = RetryMiddleware(config)

        assert mw._compute_delay(5) == 30  # clamped to index 1

    def test_delay_with_jitter(self) -> None:
        """Jitter creates variation around base delay."""
        config = RetryConfig(max_retries=3, delays=(100,), jitter_factor=0.5, strict_delays=False)
        mw = RetryMiddleware(config)

        # With 50% jitter on delay=100, values should be in [50, 150]
        delays = {mw._compute_delay(0) for _ in range(100)}
        assert min(delays) >= 50
        assert max(delays) <= 150
        assert len(delays) > 1  # some variation expected


# ── RetryRouter — topology ───────────────────────────────────────────────


class TestRetryRouter:
    def test_per_queue_delay_queues(self) -> None:
        """Per-queue mode creates named delay queues + DLQ."""
        config = RetryConfig(max_retries=3, delays=(5, 30, 120), per_queue=True)
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("orders-queue", "orders-exchange")

        # 3 delay queues + 1 DLQ = 4
        assert len(queues) == 4

        # Delay queue naming
        assert queues[0].name == "orders-queue.retry.1"
        assert queues[1].name == "orders-queue.retry.2"
        assert queues[2].name == "orders-queue.retry.3"

        # DLQ naming
        assert queues[3].name == "orders-queue.dlq"

    def test_shared_delay_queues(self) -> None:
        """Shared mode creates shared delay queues + DLQ."""
        config = RetryConfig(max_retries=2, delays=(5, 30), per_queue=False)
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("orders-queue", "orders-exchange")

        assert len(queues) == 3
        assert queues[0].name == "rabbitkit.retry.1"
        assert queues[1].name == "rabbitkit.retry.2"
        assert queues[2].name == "rabbitkit.dlq"

    def test_delay_queue_has_ttl(self) -> None:
        """Delay queues have x-message-ttl set in milliseconds."""
        config = RetryConfig(max_retries=2, delays=(5, 30), per_queue=True)
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("q", "ex")

        # First delay queue: 5s = 5000ms
        assert queues[0].arguments["x-message-ttl"] == 5000
        # Second delay queue: 30s = 30000ms
        assert queues[1].arguments["x-message-ttl"] == 30000

    def test_delay_queue_has_dlx(self) -> None:
        """Delay queues have x-dead-letter-exchange pointing to source exchange."""
        config = RetryConfig(max_retries=1, delays=(5,), per_queue=True)
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("q", "my-exchange")

        assert queues[0].arguments["x-dead-letter-exchange"] == "my-exchange"
        assert queues[0].arguments["x-dead-letter-routing-key"] == "q"

    def test_delay_queues_are_classic(self) -> None:
        """Delay queues use classic queue type (lightweight, TTL-based)."""
        config = RetryConfig(max_retries=1, delays=(5,))
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("q", "ex")

        assert queues[0].arguments["x-queue-type"] == "classic"

    def test_delay_queues_are_durable(self) -> None:
        """Delay queues and DLQ are durable."""
        config = RetryConfig(max_retries=2, delays=(5, 30))
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("q", "ex")

        for q in queues:
            assert q.durable is True

    def test_dlq_has_no_ttl(self) -> None:
        """DLQ has no TTL — messages stay until consumed/purged."""
        config = RetryConfig(max_retries=1, delays=(5,))
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("q", "ex")
        dlq = queues[-1]

        assert "x-message-ttl" not in dlq.arguments

    def test_zero_max_retries(self) -> None:
        """max_retries=0 creates only DLQ, no delay queues."""
        config = RetryConfig(max_retries=0, delays=())
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("q", "ex")

        assert len(queues) == 1
        assert queues[0].name == "q.dlq"


class TestRetryEnvelopeNoOriginalQueue:
    def test_build_retry_envelope_sets_empty_original_queue(self) -> None:
        """Line 136: x-rabbitkit-original-queue set to '' when not in headers."""
        from rabbitkit.middleware.retry import RetryMiddleware

        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(config)
        transport = MagicMock()
        mw._transport = transport

        # Message with NO x-rabbitkit-original-queue header
        msg = _make_message(headers={}, routing_key="orders")

        envelope = mw._build_retry_envelope(msg, retry_count=0)

        assert envelope.headers.get("x-rabbitkit-original-queue") == ""


class TestRetryPredicates:
    def test_predicate_overrides_type_classification(self) -> None:
        """A predicate can make a normally-PERMANENT error retry as transient."""
        published: list[MessageEnvelope] = []

        def capture(env: MessageEnvelope) -> PublishOutcome:
            published.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        # ValueError is normally PERMANENT; the predicate marks it transient.
        mw = RetryMiddleware(
            config,
            publish_fn=capture,
            predicates=[lambda exc: isinstance(exc, ValueError)],
        )
        msg = _make_message()

        def handler(m: RabbitMessage) -> None:
            raise ValueError("normally permanent, but predicate says retry")

        mw.consume_scope(handler, msg)

        assert len(published) == 1  # routed to a delay queue
        assert ".retry." in published[0].routing_key
        assert msg._disposition == "acked"  # source acked, not DLQ'd


class TestMaxRetriesZero:
    def test_transient_error_with_max_retries_0_is_terminal_sync(self) -> None:
        """RetryConfig(max_retries=0): a transient error is immediately terminal
        (marked and re-raised). No publish to a delay queue ever happens."""
        published: list[MessageEnvelope] = []

        def capture(env: MessageEnvelope) -> PublishOutcome:
            published.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        config = RetryConfig(max_retries=0, delays=())
        mw = RetryMiddleware(config, publish_fn=capture)
        msg = _make_message()

        def handler(m: RabbitMessage) -> None:
            raise OSError("transient — but no retries allowed")

        with pytest.raises(OSError):
            mw.consume_scope(handler, msg)

        # No delay-queue publish — message was terminal immediately
        assert published == []
        # Exception is marked terminal
        try:
            mw.consume_scope(handler, msg)
        except OSError as exc:
            assert getattr(exc, "_rabbitkit_terminal", False) is True

    @pytest.mark.asyncio
    async def test_transient_error_with_max_retries_0_is_terminal_async(self) -> None:
        """RetryConfig(max_retries=0): async path — same immediate-terminal behavior."""
        published: list[MessageEnvelope] = []

        async def capture(env: MessageEnvelope) -> PublishOutcome:
            published.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        config = RetryConfig(max_retries=0, delays=())
        mw = RetryMiddleware(config, publish_async_fn=capture)
        msg = _make_message()

        async def handler(m: RabbitMessage) -> None:
            raise OSError("transient — but no retries allowed")

        with pytest.raises(OSError):
            await mw.consume_scope_async(handler, msg)

        assert published == []


class TestRetryAsyncPublishFailure:
    @pytest.mark.asyncio
    async def test_async_delay_publish_failure_nacks_source(self) -> None:
        """When async delay-queue publish returns a non-ok outcome, the source is nacked."""
        from rabbitkit.core.types import PublishOutcome, PublishStatus

        async def failing_publish(env: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.NACKED)

        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(config, publish_async_fn=failing_publish)
        msg = _make_message()

        async def handler(m: RabbitMessage) -> None:
            raise ConnectionError("transient")

        await mw.consume_scope_async(handler, msg)

        assert msg._disposition == "nacked"
