"""Tests for middleware/rate_limit.py — RateLimitMiddleware."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.rate_limit import (
    RateLimitConfig,
    RateLimitMiddleware,
    _TokenBucket,
)


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b"test",
        "routing_key": "test",
        "exchange": "",
        "headers": {},
    }
    defaults.update(kwargs)
    msg = RabbitMessage(**defaults)  # type: ignore[arg-type]
    msg._ack_fn = MagicMock()
    msg._nack_fn = MagicMock()
    msg._reject_fn = MagicMock()
    msg._ack_async_fn = AsyncMock()
    msg._nack_async_fn = AsyncMock()
    msg._reject_async_fn = AsyncMock()
    return msg


# ── RateLimitConfig ─────────────────────────────────────────────────────


class TestRateLimitConfig:
    def test_config_defaults(self) -> None:
        """burst defaults to 1, on_limited defaults to 'wait'."""
        cfg = RateLimitConfig(max_rate=10.0)
        assert cfg.burst == 1
        assert cfg.on_limited == "wait"
        assert cfg.max_rate == 10.0

    def test_config_invalid_rate(self) -> None:
        """max_rate=0 raises ValueError."""
        with pytest.raises(ValueError, match="max_rate must be positive"):
            RateLimitConfig(max_rate=0)

    def test_config_negative_rate(self) -> None:
        """Negative max_rate raises ValueError."""
        with pytest.raises(ValueError, match="max_rate must be positive"):
            RateLimitConfig(max_rate=-5.0)

    def test_config_invalid_burst(self) -> None:
        """burst < 1 raises ValueError."""
        with pytest.raises(ValueError, match="burst must be >= 1"):
            RateLimitConfig(max_rate=10.0, burst=0)

    def test_config_invalid_on_limited(self) -> None:
        """on_limited='invalid' raises ValueError."""
        with pytest.raises(ValueError, match="on_limited must be"):
            RateLimitConfig(max_rate=10.0, on_limited="invalid")


# ── _TokenBucket ────────────────────────────────────────────────────────


class TestTokenBucket:
    def test_token_bucket_acquire(self) -> None:
        """Can acquire up to burst capacity."""
        bucket = _TokenBucket(rate=10.0, capacity=3)
        assert bucket.try_acquire() is True
        assert bucket.try_acquire() is True
        assert bucket.try_acquire() is True

    def test_token_bucket_exhausted(self) -> None:
        """try_acquire returns False when empty."""
        bucket = _TokenBucket(rate=10.0, capacity=1)
        assert bucket.try_acquire() is True
        assert bucket.try_acquire() is False

    def test_token_bucket_refills(self) -> None:
        """Tokens refill after time passes."""
        bucket = _TokenBucket(rate=100.0, capacity=1)
        assert bucket.try_acquire() is True
        assert bucket.try_acquire() is False
        # Wait for refill (100 tokens/sec -> 10ms per token, wait 50ms for safety)
        time.sleep(0.05)
        assert bucket.try_acquire() is True

    def test_wait_time_zero_when_available(self) -> None:
        """wait_time returns 0.0 when tokens are available."""
        bucket = _TokenBucket(rate=10.0, capacity=1)
        assert bucket.wait_time() == 0.0

    def test_wait_time_positive_when_exhausted(self) -> None:
        """wait_time returns positive float when exhausted."""
        bucket = _TokenBucket(rate=10.0, capacity=1)
        bucket.try_acquire()
        wt = bucket.wait_time()
        assert wt > 0.0


# ── Consume scope (sync) ───────────────────────────────────────────────


class TestConsumeScopeSync:
    def test_consume_scope_passes_within_rate(self) -> None:
        """call_next is called when rate allows."""
        cfg = RateLimitConfig(max_rate=100.0, burst=10)
        mw = RateLimitMiddleware(cfg)
        msg = _make_message()
        call_next = MagicMock(return_value="result")

        result = mw.consume_scope(call_next, msg)

        assert result == "result"
        call_next.assert_called_once_with(msg)

    def test_consume_scope_nack_when_limited(self) -> None:
        """on_limited='nack' nacks message with requeue=True."""
        cfg = RateLimitConfig(max_rate=1.0, burst=1, on_limited="nack")
        mw = RateLimitMiddleware(cfg)

        # First message consumes the token
        msg1 = _make_message()
        call_next = MagicMock(return_value="ok")
        mw.consume_scope(call_next, msg1)

        # Second message should be nacked
        msg2 = _make_message()
        call_next2 = MagicMock()
        result = mw.consume_scope(call_next2, msg2)

        assert result is None
        call_next2.assert_not_called()
        msg2._nack_fn.assert_called_once_with(True)

    def test_consume_scope_drop_when_limited(self) -> None:
        """on_limited='drop' nacks with requeue=False."""
        cfg = RateLimitConfig(max_rate=1.0, burst=1, on_limited="drop")
        mw = RateLimitMiddleware(cfg)

        # First message consumes the token
        msg1 = _make_message()
        call_next = MagicMock(return_value="ok")
        mw.consume_scope(call_next, msg1)

        # Second message should be dropped
        msg2 = _make_message()
        call_next2 = MagicMock()
        result = mw.consume_scope(call_next2, msg2)

        assert result is None
        call_next2.assert_not_called()
        msg2._nack_fn.assert_called_once_with(False)

    def test_consume_scope_wait_when_limited(self) -> None:
        """on_limited='wait' sleeps then processes once a token is acquired."""
        cfg = RateLimitConfig(max_rate=1000.0, burst=1, on_limited="wait")
        mw = RateLimitMiddleware(cfg)

        # Exhaust the bucket
        msg1 = _make_message()
        call_next = MagicMock(return_value="ok")
        mw.consume_scope(call_next, msg1)

        # Second message should wait ~1ms (real sleep) then acquire a token.
        msg2 = _make_message()
        call_next2 = MagicMock(return_value="waited")

        result = mw.consume_scope(call_next2, msg2)

        call_next2.assert_called_once_with(msg2)
        assert result == "waited"

    def test_consume_scope_wait_no_token_falls_back_to_drop(self) -> None:
        """on_limited='wait' with no token within deadline drops (handler NOT called)."""
        cfg = RateLimitConfig(max_rate=0.001, burst=1, on_limited="wait")
        mw = RateLimitMiddleware(cfg)
        mw._wait_deadline = 0.05  # very short deadline

        # Exhaust the single-token bucket.
        msg1 = _make_message()
        mw.consume_scope(MagicMock(return_value="ok"), msg1)

        # Second message: token refills far too slowly within 50ms deadline.
        msg2 = _make_message()
        call_next2 = MagicMock(return_value="should-not-reach")

        result = mw.consume_scope(call_next2, msg2)

        assert result is None
        call_next2.assert_not_called()
        # Falls back to drop semantics: nack(requeue=False)
        msg2._nack_fn.assert_called_once_with(False)

    def test_nack_logs_and_emits_metric(self, caplog: pytest.LogCaptureFixture) -> None:
        """L5: on_limited='nack' logs at WARNING and, with a metrics_collector
        wired, increments rate_limit_dropped_total(reason=nack)."""
        import logging

        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        cfg = RateLimitConfig(max_rate=1.0, burst=1, on_limited="nack")
        mw = RateLimitMiddleware(cfg, metrics_collector=collector, metrics_config=MetricsConfig())

        mw.consume_scope(MagicMock(return_value="ok"), _make_message())  # consumes the token

        with caplog.at_level(logging.WARNING, logger="rabbitkit.middleware.rate_limit"):
            mw.consume_scope(MagicMock(), _make_message())

        assert any("reason=nack" in r.message for r in caplog.records)
        collector.inc_counter.assert_called_once_with(
            "rabbitkit_rate_limit_dropped_total", {"reason": "nack"}
        )

    def test_drop_logs_and_emits_metric(self) -> None:
        """L5: on_limited='drop' increments rate_limit_dropped_total(reason=drop)."""
        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        cfg = RateLimitConfig(max_rate=1.0, burst=1, on_limited="drop")
        mw = RateLimitMiddleware(cfg, metrics_collector=collector, metrics_config=MetricsConfig())

        mw.consume_scope(MagicMock(return_value="ok"), _make_message())
        mw.consume_scope(MagicMock(), _make_message())

        collector.inc_counter.assert_called_once_with(
            "rabbitkit_rate_limit_dropped_total", {"reason": "drop"}
        )

    def test_wait_deadline_exceeded_logs_and_emits_metric(self) -> None:
        """L5: the 'wait' policy's deadline-exceeded fallback increments
        rate_limit_dropped_total(reason=wait_deadline_exceeded)."""
        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        cfg = RateLimitConfig(max_rate=0.001, burst=1, on_limited="wait")
        mw = RateLimitMiddleware(cfg, metrics_collector=collector, metrics_config=MetricsConfig())
        mw._wait_deadline = 0.05

        mw.consume_scope(MagicMock(return_value="ok"), _make_message())
        mw.consume_scope(MagicMock(), _make_message())

        collector.inc_counter.assert_called_once_with(
            "rabbitkit_rate_limit_dropped_total", {"reason": "wait_deadline_exceeded"}
        )

    def test_no_metrics_wired_is_noop(self) -> None:
        cfg = RateLimitConfig(max_rate=1.0, burst=1, on_limited="drop")
        mw = RateLimitMiddleware(cfg)

        mw.consume_scope(MagicMock(return_value="ok"), _make_message())
        mw.consume_scope(MagicMock(), _make_message())  # must not raise

    def test_consume_scope_nack_skips_settled(self) -> None:
        """on_limited='nack' does not nack already-settled messages."""
        cfg = RateLimitConfig(max_rate=1.0, burst=1, on_limited="nack")
        mw = RateLimitMiddleware(cfg)

        # Exhaust the bucket
        msg1 = _make_message()
        mw.consume_scope(MagicMock(), msg1)

        # Pre-settle msg2
        msg2 = _make_message()
        msg2.ack()  # already settled

        call_next2 = MagicMock()
        result = mw.consume_scope(call_next2, msg2)

        assert result is None
        call_next2.assert_not_called()
        # nack should NOT be called because the message was already settled
        assert msg2._nack_fn.call_count == 0


# ── Consume scope (async) ──────────────────────────────────────────────


class TestConsumeScopeAsync:
    @pytest.mark.asyncio
    async def test_consume_scope_async_passes(self) -> None:
        """Async variant passes within rate."""
        cfg = RateLimitConfig(max_rate=100.0, burst=10)
        mw = RateLimitMiddleware(cfg)
        msg = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "async-result"

        result = await mw.consume_scope_async(call_next, msg)
        assert result == "async-result"

    @pytest.mark.asyncio
    async def test_consume_scope_async_nack(self) -> None:
        """Async variant nacks when limited."""
        cfg = RateLimitConfig(max_rate=1.0, burst=1, on_limited="nack")
        mw = RateLimitMiddleware(cfg)

        # Exhaust the bucket
        msg1 = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(call_next, msg1)

        # Second message should be nacked
        msg2 = _make_message()

        async def call_next2(m: RabbitMessage) -> str:
            return "should-not-reach"

        result = await mw.consume_scope_async(call_next2, msg2)
        assert result is None
        msg2._nack_async_fn.assert_awaited_once_with(True)

    @pytest.mark.asyncio
    async def test_consume_scope_async_drop(self) -> None:
        """Async variant drops when limited with on_limited='drop'."""
        cfg = RateLimitConfig(max_rate=1.0, burst=1, on_limited="drop")
        mw = RateLimitMiddleware(cfg)

        msg1 = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(call_next, msg1)

        msg2 = _make_message()

        async def call_next2(m: RabbitMessage) -> str:
            return "should-not-reach"

        result = await mw.consume_scope_async(call_next2, msg2)
        assert result is None
        msg2._nack_async_fn.assert_awaited_once_with(False)

    @pytest.mark.asyncio
    async def test_consume_scope_async_wait(self) -> None:
        """Async variant waits then processes once a token is acquired."""
        cfg = RateLimitConfig(max_rate=1000.0, burst=1, on_limited="wait")
        mw = RateLimitMiddleware(cfg)

        msg1 = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(call_next, msg1)

        msg2 = _make_message()

        async def call_next2(m: RabbitMessage) -> str:
            return "waited"

        result = await mw.consume_scope_async(call_next2, msg2)
        assert result == "waited"

    @pytest.mark.asyncio
    async def test_consume_scope_async_wait_no_token_falls_back_to_drop(self) -> None:
        """Async 'wait' with no token within deadline drops (handler NOT called)."""
        cfg = RateLimitConfig(max_rate=0.001, burst=1, on_limited="wait")
        mw = RateLimitMiddleware(cfg)
        mw._wait_deadline = 0.05

        msg1 = _make_message()

        async def call_next(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(call_next, msg1)

        msg2 = _make_message()
        call_count = 0

        async def call_next2(m: RabbitMessage) -> str:
            nonlocal call_count
            call_count += 1
            return "should-not-reach"

        result = await mw.consume_scope_async(call_next2, msg2)
        assert result is None
        assert call_count == 0
        msg2._nack_async_fn.assert_awaited_once_with(False)

    async def test_async_drop_emits_metric(self) -> None:
        """L5, async variant."""
        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        cfg = RateLimitConfig(max_rate=1.0, burst=1, on_limited="drop")
        mw = RateLimitMiddleware(cfg, metrics_collector=collector, metrics_config=MetricsConfig())

        async def call_next(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(call_next, _make_message())
        await mw.consume_scope_async(call_next, _make_message())

        collector.inc_counter.assert_called_once_with(
            "rabbitkit_rate_limit_dropped_total", {"reason": "drop"}
        )
