"""Tests for core/retry_handoff.py — bounded backoff for retry-publish failures."""

from __future__ import annotations

import pytest

from rabbitkit.core.config import RetryHandoffConfig
from rabbitkit.core.errors import ConfigValidationError
from rabbitkit.core.retry_handoff import RetryHandoffTracker
from rabbitkit.core.types import HandoffState


class _Clock:
    def __init__(self) -> None:
        self.t = 100.0

    def __call__(self) -> float:
        return self.t


def _tracker(**kw: float) -> tuple[RetryHandoffTracker, _Clock]:
    clock = _Clock()
    cfg = RetryHandoffConfig(**kw)  # type: ignore[arg-type]
    return RetryHandoffTracker(cfg, clock=clock, rng=lambda: 0.5), clock


class TestRetryHandoffConfig:
    def test_defaults(self) -> None:
        c = RetryHandoffConfig()
        assert c.backoff_initial > 0 and c.backoff_max >= c.backoff_initial
        assert 0 <= c.jitter < 1
        assert c.max_consecutive_failures >= 1 and c.recovery_deadline > 0

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"backoff_initial": 0},
            {"backoff_initial": 5, "backoff_max": 1},
            {"jitter": 1.0},
            {"jitter": -0.1},
            {"max_consecutive_failures": 0},
            {"recovery_deadline": 0},
        ],
    )
    def test_validation(self, kwargs: dict[str, float]) -> None:
        with pytest.raises(ConfigValidationError):
            RetryHandoffConfig(**kwargs)  # type: ignore[arg-type]


class TestRetryHandoffTracker:
    def test_healthy_initially(self) -> None:
        t, _ = _tracker()
        assert t.state is HandoffState.HEALTHY
        assert not t.is_paused and not t.is_exhausted
        assert t.consecutive_failures == 0 and t.total_failures == 0
        assert t.last_backoff == 0.0 and t.first_failure_at is None

    def test_exponential_capped_backoff_without_jitter(self) -> None:
        t, _ = _tracker(backoff_initial=1.0, backoff_max=4.0, jitter=0.0)
        assert [t.record_failure() for _ in range(5)] == [1.0, 2.0, 4.0, 4.0, 4.0]
        assert t.last_backoff == 4.0

    def test_jitter_is_symmetric_and_bounded(self) -> None:
        clock = _Clock()
        cfg = RetryHandoffConfig(backoff_initial=1.0, backoff_max=100.0, jitter=0.5)
        low = RetryHandoffTracker(cfg, clock=clock, rng=lambda: 0.0)
        high = RetryHandoffTracker(cfg, clock=clock, rng=lambda: 1.0)
        assert low.record_failure() == pytest.approx(0.5)
        assert high.record_failure() == pytest.approx(1.5)
        # never above backoff_max even with jitter pushing up
        cfg2 = RetryHandoffConfig(backoff_initial=1.0, backoff_max=1.0, jitter=0.5)
        capped = RetryHandoffTracker(cfg2, clock=clock, rng=lambda: 1.0)
        assert capped.record_failure() == 1.0

    def test_degraded_then_exhausted_by_count(self) -> None:
        t, _ = _tracker(max_consecutive_failures=3, jitter=0.0)
        t.record_failure()
        assert t.state is HandoffState.DEGRADED and t.is_paused
        t.record_failure()
        assert t.state is HandoffState.DEGRADED
        t.record_failure()
        assert t.state is HandoffState.EXHAUSTED and t.is_exhausted

    def test_exhausted_by_deadline(self) -> None:
        t, clock = _tracker(max_consecutive_failures=100, recovery_deadline=60.0, jitter=0.0)
        t.record_failure()
        assert t.state is HandoffState.DEGRADED
        clock.t += 59.0
        assert t.state is HandoffState.DEGRADED
        clock.t += 1.0
        assert t.state is HandoffState.EXHAUSTED
        assert t.first_failure_at == 100.0

    def test_success_resets_streak_but_not_total(self) -> None:
        t, _ = _tracker(jitter=0.0)
        t.record_failure()
        t.record_failure()
        t.record_success()
        assert t.state is HandoffState.HEALTHY
        assert t.consecutive_failures == 0
        assert t.total_failures == 2
        assert t.first_failure_at is None and t.last_backoff == 0.0
        # streak restarts from the initial backoff
        assert t.record_failure() == t.config.backoff_initial

    def test_reset_clears_everything(self) -> None:
        t, _ = _tracker()
        t.record_failure()
        t.reset()
        assert t.total_failures == 0 and t.state is HandoffState.HEALTHY

    def test_no_busy_loop_backoff_is_positive(self) -> None:
        """Acceptance (plan §5.3): recovery must not busy-loop — every failure
        yields a strictly positive backoff."""
        t, _ = _tracker()
        assert all(t.record_failure() > 0 for _ in range(10))
