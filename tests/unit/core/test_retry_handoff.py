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


class TestBackoffNeverOverflows:
    """R-D4: a long streak of handoff failures must never raise. The old
    ``initial * 2 ** (n - 1)`` overflowed a float after ~1,025 consecutive
    failures and the failure path itself crashed."""

    @pytest.mark.parametrize("failures", [1, 2, 10, 64, 65, 66, 1024, 1025, 1026, 5000, 10**6])
    def test_any_failure_count_gives_finite_delay_within_max(self, failures: int) -> None:
        from rabbitkit.core.retry_handoff import _capped_exponential

        delay = _capped_exponential(0.5, 30.0, failures)
        assert 0 < delay <= 30.0

    @pytest.mark.parametrize(("initial", "maximum"), [(0.001, 0.001), (0.001, 1e9), (0.5, 30.0), (1e-9, 1e300)])
    def test_extreme_configs_stay_finite(self, initial: float, maximum: float) -> None:
        from rabbitkit.core.retry_handoff import _capped_exponential

        for failures in (1, 50, 1100, 100_000):
            delay = _capped_exponential(initial, maximum, failures)
            assert initial <= delay <= maximum

    def test_tracker_survives_long_streak(self) -> None:
        t, _ = _tracker(max_consecutive_failures=10**9, recovery_deadline=10**9)
        delays = [t.record_failure() for _ in range(1_100)]
        assert all(0 < d <= t.config.backoff_max for d in delays)
        assert delays[-1] == t.config.backoff_max * (1 - t.config.jitter + 2 * t.config.jitter * 0.5)


class TestStreakEndsAfterAQuietPeriod:
    """A failure streak ends after ``recovery_deadline + backoff_max`` with no
    handoff failure, not only on a handoff success (0.20.4).

    Before, a route that recovered and then only handled deliveries
    successfully (no further handoff) kept its streak forever: it stayed
    paused, turned EXHAUSTED by time alone, and its next handoff failure,
    hours later, was EXHAUSTED at once.
    """

    def test_streak_is_cleared_once_the_quiet_period_has_passed(self) -> None:
        t, clock = _tracker(backoff_max=30.0, recovery_deadline=300.0, jitter=0.0)
        for _ in range(3):
            t.record_failure()
        clock.t += 329.0  # 1 s short of recovery_deadline + backoff_max after the last failure
        assert t.is_paused
        clock.t += 1.0
        assert t.state is HandoffState.HEALTHY
        assert not t.is_paused and not t.is_exhausted
        assert t.consecutive_failures == 0
        assert t.first_failure_at is None and t.last_backoff == 0.0
        assert t.total_failures == 3  # lifetime count is kept

    def test_a_failure_long_after_a_short_streak_starts_a_new_streak(self) -> None:
        t, clock = _tracker(max_consecutive_failures=100, recovery_deadline=300.0, jitter=0.0)
        for _ in range(3):
            t.record_failure()
        clock.t += 3600.0
        assert t.record_failure() == t.config.backoff_initial
        assert t.state is HandoffState.DEGRADED  # was EXHAUSTED by "deadline" at once
        assert t.consecutive_failures == 1
        assert t.first_failure_at == clock.t

    def test_a_failure_long_after_an_exhausted_streak_is_not_exhausted(self) -> None:
        t, clock = _tracker(max_consecutive_failures=20, jitter=0.0)
        for _ in range(20):
            t.record_failure()
        assert t.is_exhausted
        clock.t += 3600.0
        t.record_failure()
        assert t.state is HandoffState.DEGRADED
        assert t.consecutive_failures == 1

    def test_failures_spaced_just_below_the_quiet_period_stay_one_streak(self) -> None:
        t, clock = _tracker(max_consecutive_failures=10**6, recovery_deadline=300.0, backoff_max=30.0, jitter=0.0)
        backoffs: list[float] = []
        for _ in range(5):
            backoffs.append(t.record_failure())
            clock.t += 329.0  # just under the 330 s quiet period
        assert t.consecutive_failures == 5
        assert backoffs == [0.5, 1.0, 2.0, 4.0, 8.0]  # the backoff keeps growing
        assert t.state is HandoffState.EXHAUSTED  # 1,316 s without a handoff success
        assert t.first_failure_at == 100.0

    def test_failures_spaced_below_the_quiet_period_still_exhaust_by_count(self) -> None:
        t, clock = _tracker(max_consecutive_failures=5, recovery_deadline=10**6, backoff_max=30.0, jitter=0.0)
        for _ in range(4):
            t.record_failure()
            clock.t += 30.0
        assert t.state is HandoffState.DEGRADED
        t.record_failure()
        assert t.state is HandoffState.EXHAUSTED
        assert t.consecutive_failures == 5

    def test_continuous_failures_still_exhaust_by_deadline(self) -> None:
        t, clock = _tracker(max_consecutive_failures=10**6, recovery_deadline=300.0, backoff_max=30.0, jitter=0.0)
        t.record_failure()
        for _ in range(9):  # one failure per backoff_max, the EXHAUSTED hold
            clock.t += 30.0
            t.record_failure()
        assert t.state is HandoffState.DEGRADED  # 270 s into the streak
        clock.t += 30.0
        t.record_failure()
        assert t.state is HandoffState.EXHAUSTED  # 300 s: recovery_deadline reached
        assert t.first_failure_at == 100.0
