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


def _capture_ok(published: list[MessageEnvelope]):
    """Publish fn that records the envelope and returns a CONFIRMED outcome
    (L3: a None-returning publish fn is now treated as failure)."""

    def publish(env: MessageEnvelope) -> PublishOutcome:
        published.append(env)
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    return publish


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

        def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            published.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

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
        mw = RetryMiddleware(config, publish_fn=_capture_ok(published))

        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)

        assert published[0].headers["x-rabbitkit-retry-count"] == 1

    def test_retry_preserves_original_headers(self) -> None:
        """Original exchange/routing_key/queue are preserved in headers."""
        published: list[MessageEnvelope] = []

        config = RetryConfig(max_retries=3, delays=(5,), strict_delays=False)
        mw = RetryMiddleware(config, publish_fn=_capture_ok(published))

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
        mw = RetryMiddleware(config, publish_fn=_capture_ok(published))

        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders-queue"})

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)

        assert published[0].routing_key == "orders-queue.retry.1"

    def test_shared_mode_rejected(self) -> None:
        """H3: per_queue=False is unsafe (misroutes across queues) and now
        raises at construction rather than silently misrouting."""
        with pytest.raises(ValueError, match="per_queue=False"):
            RetryConfig(max_retries=2, delays=(5,), per_queue=False, strict_delays=False)

    def test_second_retry_uses_correct_attempt(self) -> None:
        """Message on retry attempt 1 → routes to retry.2."""
        published: list[MessageEnvelope] = []

        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(config, publish_fn=_capture_ok(published))

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


# ── M2: retried/dead-lettered metrics ─────────────────────────────────────


class TestRetryMetrics:
    def test_successful_retry_emits_messages_retried_total(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        config = RetryConfig(max_retries=3, delays=(5, 30, 120))
        mw = RetryMiddleware(
            config,
            publish_fn=lambda env: PublishOutcome(status=PublishStatus.CONFIRMED),
            metrics_collector=collector,
            metrics_config=MetricsConfig(),
        )
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders-queue"})

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("connection lost")

        mw.consume_scope(failing_handler, msg)

        collector.inc_counter.assert_called_once_with(
            "rabbitkit_messages_retried_total", {"queue": "orders-queue"}
        )

    @pytest.mark.asyncio
    async def test_successful_retry_async_emits_messages_retried_total(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        config = RetryConfig(max_retries=3, delays=(5, 30, 120))

        async def publish_async(env: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        mw = RetryMiddleware(
            config,
            publish_async_fn=publish_async,
            metrics_collector=collector,
            metrics_config=MetricsConfig(),
        )
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders-queue"})

        async def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("connection lost")

        await mw.consume_scope_async(failing_handler, msg)

        collector.inc_counter.assert_called_once_with(
            "rabbitkit_messages_retried_total", {"queue": "orders-queue"}
        )

    def test_permanent_error_emits_messages_dead_lettered_total(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        config = RetryConfig(max_retries=3, delays=(5,), strict_delays=False)
        mw = RetryMiddleware(config, metrics_collector=collector, metrics_config=MetricsConfig())
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders-queue"})

        def failing_handler(m: RabbitMessage) -> None:
            raise ValueError("bad payload")

        with pytest.raises(ValueError):
            mw.consume_scope(failing_handler, msg)

        collector.inc_counter.assert_called_once_with(
            "rabbitkit_messages_dead_lettered_total", {"queue": "orders-queue"}
        )

    def test_exhausted_retries_emits_messages_dead_lettered_total(self) -> None:
        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        config = RetryConfig(max_retries=2, delays=(5, 30))
        mw = RetryMiddleware(config, metrics_collector=collector, metrics_config=MetricsConfig())
        msg = _make_message(
            headers={"x-rabbitkit-retry-count": 2, "x-rabbitkit-original-queue": "orders-queue"}
        )

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("still failing")

        with pytest.raises(ConnectionResetError):
            mw.consume_scope(failing_handler, msg)

        collector.inc_counter.assert_called_once_with(
            "rabbitkit_messages_dead_lettered_total", {"queue": "orders-queue"}
        )

    def test_no_metrics_config_is_noop(self) -> None:
        """Without metrics_collector/metrics_config wired, no metric call is
        attempted -- must not raise."""
        config = RetryConfig(max_retries=1, delays=(5,))
        mw = RetryMiddleware(config, publish_fn=lambda env: PublishOutcome(status=PublishStatus.CONFIRMED))
        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)  # must not raise

    def test_metrics_config_without_collector_is_noop(self) -> None:
        """metrics_config set but metrics_collector None (e.g. a no-op-mode
        MetricsMiddleware) -- _record_metric itself must no-op, not raise."""
        from rabbitkit.core.config import MetricsConfig

        config = RetryConfig(max_retries=1, delays=(5,))
        mw = RetryMiddleware(config, publish_fn=_capture_ok([]), metrics_config=MetricsConfig())
        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("lost")

        mw.consume_scope(failing_handler, msg)  # must not raise


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
        mw = RetryMiddleware(config, publish_fn=_capture_ok(published))

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
        mw = RetryMiddleware(config, publish_fn=_capture_ok(published))
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
        mw = RetryMiddleware(config, publish_fn=_capture_ok(published))

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

        async def capture_publish(env: MessageEnvelope) -> PublishOutcome:
            published.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

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
    """Retry timing authority is the delay QUEUE's uniform x-message-ttl
    (RetryRouter._get_delay_ms) — never a per-message value. The old
    per-message _compute_delay helper was dead code contradicting that
    design and has been removed.
    """

    def test_no_per_message_delay_helper(self) -> None:
        config = RetryConfig(max_retries=4, delays=(5, 30, 120, 600))
        mw = RetryMiddleware(config)
        assert not hasattr(mw, "_compute_delay")

    def test_router_ttl_uses_correct_index_and_clamps(self) -> None:
        from rabbitkit.middleware.retry import RetryRouter

        config = RetryConfig(max_retries=10, delays=(5, 30), strict_delays=False)
        router = RetryRouter(config)
        assert router._get_delay_ms(0) == 5000
        assert router._get_delay_ms(1) == 30000
        assert router._get_delay_ms(5) == 30000  # clamped to last


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

    def test_shared_mode_rejected_at_config(self) -> None:
        """H3: shared mode (per_queue=False) is unsafe and rejected before a
        RetryRouter is ever built."""
        with pytest.raises(ValueError, match="per_queue=False"):
            RetryConfig(max_retries=2, delays=(5, 30), per_queue=False)

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
        """M5: delay queues dead-letter back to the source queue via the
        DEFAULT exchange (routing key = queue name always delivers directly
        to that queue, regardless of the queue's real bindings) -- NOT the
        source's real exchange, which would only route back correctly if the
        queue happened to be bound with routing key == its own name."""
        config = RetryConfig(max_retries=1, delays=(5,), per_queue=True)
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("q", "my-exchange")

        assert queues[0].arguments["x-dead-letter-exchange"] == ""
        assert queues[0].arguments["x-dead-letter-routing-key"] == "q"

    def test_delay_queue_dlx_ignores_source_exchange_argument(self) -> None:
        """M5 regression guard: source_exchange_name must never leak into
        x-dead-letter-exchange, regardless of its value."""
        config = RetryConfig(max_retries=1, delays=(5,), per_queue=True)
        router = RetryRouter(config)

        queues = router.get_delay_queue_definitions("q", "topic-exchange-with-pattern-bindings")

        assert queues[0].arguments["x-dead-letter-exchange"] == ""

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

        envelope = mw._build_retry_envelope(msg, retry_count=0, exc=RuntimeError("test error"))

        assert envelope.headers.get("x-rabbitkit-original-queue") == ""

    def test_build_retry_envelope_is_mandatory(self) -> None:
        """M4: retry publishes are mandatory so a deleted/missing delay queue
        RETURNs (outcome not-ok → nack+requeue) instead of confirming into
        the void and acking the source (silent loss)."""
        from rabbitkit.middleware.retry import RetryMiddleware

        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)))
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders"}, routing_key="orders")
        envelope = mw._build_retry_envelope(msg, retry_count=0, exc=RuntimeError("test error"))
        assert envelope.mandatory is True

    def test_build_retry_envelope_preserves_message_properties(self) -> None:
        """A retry republish used to silently drop priority/type/app_id/
        user_id/reply_to -- e.g. a priority-queue message lost its priority
        on its first retry, and an RPC request's reply_to never survived
        long enough for the eventual reply to route back.

        ``expiration`` is the deliberate EXCEPTION: preserving a producer
        TTL shorter than the delay tier's x-message-ttl would expire the
        message inside the delay queue and dead-letter it back to the
        source early, collapsing the whole backoff ladder into a fast
        retry storm. The delay queue's own TTL is the timing authority.
        """
        from rabbitkit.middleware.retry import RetryMiddleware

        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)))
        msg = _make_message(
            headers={"x-rabbitkit-original-queue": "orders"},
            routing_key="orders",
            reply_to="amq.rabbitmq.reply-to",
            priority=7,
            expiration="60000",
            type="order.created",
            app_id="order-service",
            user_id="guest",
        )
        envelope = mw._build_retry_envelope(msg, retry_count=0, exc=RuntimeError("test error"))

        assert envelope.reply_to == "amq.rabbitmq.reply-to"
        assert envelope.priority == 7
        assert envelope.expiration is None  # stripped — see docstring
        assert envelope.type == "order.created"
        assert envelope.app_id == "order-service"
        assert envelope.user_id == "guest"


class TestRetryEnvelopeDlqTriageHeaders:
    """Item 7: x-rabbitkit-error-type/-error-message/-first-failed-at/
    -last-failed-at -- previously the only way to learn why a message ended
    up on the DLQ was application logs, which may have long since rotated
    out. Deliberately skips x-rabbitkit-trace-id (superseded by existing
    OTel W3C traceparent/tracestate propagation)."""

    def test_error_type_and_message_set(self) -> None:
        from rabbitkit.middleware.retry import RetryMiddleware

        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)))
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders"}, routing_key="orders")
        envelope = mw._build_retry_envelope(msg, retry_count=0, exc=ValueError("bad payload"))

        assert envelope.headers["x-rabbitkit-error-type"] == "ValueError"
        assert envelope.headers["x-rabbitkit-error-message"] == "bad payload"

    def test_error_message_is_length_capped(self) -> None:
        from rabbitkit.middleware.retry import _ERROR_MESSAGE_MAX_LEN, RetryMiddleware

        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)))
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders"}, routing_key="orders")
        huge = "x" * (_ERROR_MESSAGE_MAX_LEN * 3)
        envelope = mw._build_retry_envelope(msg, retry_count=0, exc=ValueError(huge))

        assert len(envelope.headers["x-rabbitkit-error-message"]) == _ERROR_MESSAGE_MAX_LEN

    def test_first_failed_at_set_on_first_attempt(self) -> None:
        from rabbitkit.middleware.retry import RetryMiddleware

        mw = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120)))
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders"}, routing_key="orders")
        envelope = mw._build_retry_envelope(msg, retry_count=0, exc=RuntimeError("boom"))

        assert "x-rabbitkit-first-failed-at" in envelope.headers
        assert envelope.headers["x-rabbitkit-first-failed-at"] == envelope.headers["x-rabbitkit-last-failed-at"]

    def test_first_failed_at_preserved_across_retries(self) -> None:
        """The FIRST failure's timestamp must survive every subsequent
        retry hop -- mirrors the x-rabbitkit-original-* preservation
        pattern already used above in this file."""
        from rabbitkit.middleware.retry import RetryMiddleware

        mw = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120)))
        msg = _make_message(
            headers={
                "x-rabbitkit-original-queue": "orders",
                "x-rabbitkit-first-failed-at": "2020-01-01T00:00:00+00:00",
                "x-rabbitkit-last-failed-at": "2020-01-01T00:00:00+00:00",
            },
            routing_key="orders",
        )
        envelope = mw._build_retry_envelope(msg, retry_count=1, exc=RuntimeError("boom again"))

        assert envelope.headers["x-rabbitkit-first-failed-at"] == "2020-01-01T00:00:00+00:00"
        assert envelope.headers["x-rabbitkit-last-failed-at"] != "2020-01-01T00:00:00+00:00"

    def test_last_failed_at_updates_every_retry(self) -> None:
        from rabbitkit.middleware.retry import RetryMiddleware

        mw = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120)))
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders"}, routing_key="orders")

        env1 = mw._build_retry_envelope(msg, retry_count=0, exc=RuntimeError("first"))
        msg2 = _make_message(headers=dict(env1.headers), routing_key="orders")
        env2 = mw._build_retry_envelope(msg2, retry_count=1, exc=RuntimeError("second"))

        assert env2.headers["x-rabbitkit-first-failed-at"] == env1.headers["x-rabbitkit-first-failed-at"]
        assert env2.headers["x-rabbitkit-error-message"] == "second"

    def test_no_trace_id_header(self) -> None:
        """Explicitly NOT added — superseded by existing OTel propagation."""
        from rabbitkit.middleware.retry import RetryMiddleware

        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)))
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders"}, routing_key="orders")
        envelope = mw._build_retry_envelope(msg, retry_count=0, exc=RuntimeError("boom"))

        assert "x-rabbitkit-trace-id" not in envelope.headers


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


class TestShardedJitter:
    """F4: jitter_mode='sharded' — retry waves decorrelate via TTL-staggered
    shard queues while every individual queue keeps a UNIFORM TTL (the
    head-of-line-blocking safety invariant)."""

    def _router(self, shards: int = 3, jf: float = 0.1) -> RetryRouter:
        return RetryRouter(
            RetryConfig(
                max_retries=2, delays=(10, 60), jitter_mode="sharded",
                jitter_shards=shards, jitter_factor=jf,
            )
        )

    def test_off_mode_topology_byte_identical_to_legacy(self) -> None:
        """Default topology must not change at all (regression guard)."""
        router = RetryRouter(RetryConfig(max_retries=2, delays=(10, 60)))
        queues = router.get_delay_queue_definitions("orders", "")
        names = [q.name for q in queues]
        assert names == ["orders.retry.1", "orders.retry.2", "orders.dlq"]
        assert queues[0].arguments["x-message-ttl"] == 10_000

    def test_sharded_topology_names_and_ttls(self) -> None:
        queues = self._router(shards=3, jf=0.1).get_delay_queue_definitions("orders", "")
        names = [q.name for q in queues]
        # shard 0 keeps the legacy name (additive enablement, no 406s)
        assert names == [
            "orders.retry.1", "orders.retry.1.s1", "orders.retry.1.s2",
            "orders.retry.2", "orders.retry.2.s1", "orders.retry.2.s2",
            "orders.dlq",
        ]
        ttls_t1 = [q.arguments["x-message-ttl"] for q in queues[:3]]
        assert ttls_t1 == [10_000, 9_000, 11_000]  # 1.0, 1-jf, 1+jf

    def test_two_shards_spread_upward_only(self) -> None:
        queues = self._router(shards=2, jf=0.2).get_delay_queue_definitions("q", "")
        ttls = [q.arguments["x-message-ttl"] for q in queues[:2]]
        assert ttls == [10_000, 12_000]

    def test_each_shard_queue_ttl_is_uniform_within_queue(self) -> None:
        """The invariant itself: TTL is a QUEUE argument, never per-message."""
        for q in self._router().get_delay_queue_definitions("q", "")[:-1]:
            assert "x-message-ttl" in q.arguments  # queue-level, uniform

    def test_envelope_shard_pick_is_stable_and_deterministic(self) -> None:
        from rabbitkit.middleware.retry import _shard_index

        cfg = RetryConfig(
            max_retries=2, delays=(10, 60), jitter_mode="sharded", jitter_shards=3
        )
        mw = RetryMiddleware(cfg)
        msg = _make_message(message_id="stable-id-42")
        rk1 = mw._build_retry_envelope(msg, retry_count=0, exc=RuntimeError("test error")).routing_key
        rk2 = mw._build_retry_envelope(msg, retry_count=0, exc=RuntimeError("test error")).routing_key
        assert rk1 == rk2  # deterministic
        shard = _shard_index("stable-id-42", 3)
        expected = "orders-queue.retry.1" if shard == 0 else f"orders-queue.retry.1.s{shard}"
        assert rk1 == expected

    def test_shard_index_stable_across_processes(self) -> None:
        """md5-based, NOT Python hash() (which is salted per process)."""
        from rabbitkit.middleware.retry import _shard_index

        assert _shard_index("abc", 3) == _shard_index("abc", 3)
        assert _shard_index("", 3) == 0  # no id -> legacy shard

    def test_messages_spread_across_shards(self) -> None:
        from rabbitkit.middleware.retry import _shard_index

        shards = {_shard_index(f"id-{i}", 3) for i in range(60)}
        assert shards == {0, 1, 2}

    def test_off_mode_envelope_unchanged(self) -> None:
        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)))
        env = mw._build_retry_envelope(_make_message(message_id="x"), retry_count=0, exc=RuntimeError("test error"))
        assert env.routing_key == "orders-queue.retry.1"

    def test_config_validation(self) -> None:
        with pytest.raises(ValueError, match="jitter_mode"):
            RetryConfig(jitter_mode="bogus")
        with pytest.raises(ValueError, match="jitter_shards"):
            RetryConfig(jitter_mode="sharded", jitter_shards=1)
        with pytest.raises(ValueError, match="jitter_factor"):
            RetryConfig(jitter_mode="sharded", jitter_factor=0.0)


# ── No-publish-fn loss path (RabbitMQ-architect review H1) ───────────────


class TestRetryWithoutPublishFn:
    """A RetryMiddleware with no (matching) publish fn used to ACK a
    transient failure without ever publishing to the delay queue — silent
    message loss. It must now nack (requeue) and warn loudly.
    """

    def test_sync_no_publish_fn_nacks_and_warns_never_acks(self) -> None:
        mw = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120)))
        msg = _make_message()

        with pytest.warns(RuntimeWarning, match="no publish_fn"):
            mw._route_to_delay_queue_sync(msg, retry_count=0, exc=RuntimeError("test error"))

        msg._ack_fn.assert_not_called()
        msg._nack_fn.assert_called_once_with(True)

    def test_sync_broker_route_with_async_only_fn_nacks(self) -> None:
        """Cross-wiring (publish_async_fn on a sync route) is the same loss
        path — the sync routing method only reads _publish_fn."""

        async def async_publish(env: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)), publish_async_fn=async_publish)
        msg = _make_message()

        with pytest.warns(RuntimeWarning, match="no publish_fn"):
            mw._route_to_delay_queue_sync(msg, retry_count=0, exc=RuntimeError("test error"))

        msg._ack_fn.assert_not_called()
        msg._nack_fn.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_async_no_publish_fn_nacks_and_warns_never_acks(self) -> None:
        mw = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120)))
        msg = _make_message()
        nacked: list[bool] = []

        async def nack_async(requeue: bool = True) -> None:
            nacked.append(requeue)

        msg._nack_async_fn = nack_async
        msg._nack_fn = None

        with pytest.warns(RuntimeWarning, match="no publish_async_fn"):
            await mw._route_to_delay_queue_async(msg, retry_count=0, exc=RuntimeError("test error"))

        msg._ack_fn.assert_not_called()
        assert nacked == [True]

    def test_none_outcome_is_failure_not_success(self) -> None:
        """L3: a publish fn returning None is UNVERIFIED — nack, never ack."""
        mw = RetryMiddleware(
            RetryConfig(max_retries=1, delays=(5,)),
            publish_fn=lambda env: None,
        )
        msg = _make_message()
        mw._route_to_delay_queue_sync(msg, retry_count=0, exc=RuntimeError("test error"))
        msg._ack_fn.assert_not_called()
        msg._nack_fn.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_async_none_outcome_is_failure_not_success(self) -> None:
        """L3 async twin: a publish_async_fn returning None is UNVERIFIED —
        nack, never ack."""

        async def publish_none(env: MessageEnvelope) -> None:
            return None

        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)), publish_async_fn=publish_none)
        msg = _make_message()
        nacked: list[bool] = []

        async def nack_async(requeue: bool = True) -> None:
            nacked.append(requeue)

        msg._nack_async_fn = nack_async
        msg._nack_fn = None

        await mw._route_to_delay_queue_async(msg, retry_count=0, exc=RuntimeError("test error"))

        msg._ack_fn.assert_not_called()
        assert nacked == [True]

    def test_ensure_publish_fns_fills_only_unset(self) -> None:
        sentinel_sync = MagicMock()
        mw = RetryMiddleware(RetryConfig(max_retries=1, delays=(5,)), publish_fn=sentinel_sync)
        replacement = MagicMock()
        mw.ensure_publish_fns(publish_fn=replacement, publish_async_fn=replacement)
        assert mw._publish_fn is sentinel_sync  # explicit fn never overwritten
        assert mw._publish_async_fn is replacement  # unset one filled in


# ── DLQ queue-type inheritance (RabbitMQ-architect review M3) ─────────────


class TestDlqQueueTypeInheritance:
    def test_quorum_source_gets_quorum_dlq_classic_delay_queues(self) -> None:
        from rabbitkit.core.types import QueueType
        from rabbitkit.middleware.retry import RetryRouter

        router = RetryRouter(RetryConfig(max_retries=2, delays=(5, 30)))
        queues = router.get_delay_queue_definitions(
            "orders", "", source_queue_type=QueueType.QUORUM
        )
        dlq = queues[-1]
        assert dlq.name.endswith(".dlq")
        assert dlq.queue_type == QueueType.QUORUM
        for q in queues[:-1]:  # delay queues stay classic deliberately
            assert q.arguments["x-queue-type"] == "classic"

    def test_classic_source_keeps_classic_dlq(self) -> None:
        from rabbitkit.core.types import QueueType
        from rabbitkit.middleware.retry import RetryRouter

        router = RetryRouter(RetryConfig(max_retries=1, delays=(5,)))
        queues = router.get_delay_queue_definitions("orders", "")
        assert queues[-1].queue_type == QueueType.CLASSIC
