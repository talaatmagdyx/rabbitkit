"""RetryHandoffTracker — bounded backoff state for retry-publish failures.

Separates two things the old code conflated (plan §5.2):

* **handler attempts** — how many times the business handler ran
  (``x-rabbitkit-retry-count``; ``messages_retried_total``), and
* **handoff failures** — how many times rabbitkit failed to place the
  message on its delay queue (destination missing → returned, nacked,
  confirm timeout, connection drop). The source is nack-requeued in that
  case (never acked), which is correct but, unbraked, loops hot against a
  broken destination.

The tracker is transport-free and clock-injectable. It computes a capped
exponential backoff with jitter per consecutive failure and reports
``exhausted`` once either bound (count or wall-clock deadline) is hit so
an owner can stop the consumer and let the broker redeliver.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable

from rabbitkit.core.config import RetryHandoffConfig
from rabbitkit.core.types import HandoffState


def _capped_exponential(initial: float, maximum: float, failures: int) -> float:
    """``min(maximum, initial * 2 ** (failures - 1))`` without overflow.

    The exponent is capped before it is evaluated: a float ``2 ** 1024``
    raises ``OverflowError``, so an uncapped streak of ~1,025 consecutive
    handoff failures used to crash the middleware's failure path itself.
    Once ``initial * 2 ** n`` reaches ``maximum`` further doubling cannot
    change the result, so the exponent never needs to grow past that point.
    """
    if failures <= 1:
        return min(maximum, initial)
    # 2.0 ** 1000 is still a finite float; float multiplication saturates to
    # inf rather than raising, and min() then returns maximum.
    exponent = min(failures - 1, 1000)
    return min(maximum, initial * (2.0**exponent))


class RetryHandoffTracker:
    """Thread-safe consecutive-failure tracker for one route.

    ``record_failure()`` returns the backoff (seconds) the caller should
    apply before letting the next redelivery through. ``record_success()``
    resets the streak. The tracker never sleeps itself.
    """

    def __init__(
        self,
        config: RetryHandoffConfig | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self._config = config or RetryHandoffConfig()
        self._clock = clock
        self._rng = rng
        self._lock = threading.Lock()
        self._consecutive = 0
        self._total = 0
        self._first_failure_at: float | None = None
        self._last_failure_at: float | None = None
        self._last_backoff = 0.0

    # ── inspection ────────────────────────────────────────────────────────

    @property
    def config(self) -> RetryHandoffConfig:
        return self._config

    @property
    def consecutive_failures(self) -> int:
        with self._lock:
            return self._consecutive

    @property
    def total_failures(self) -> int:
        with self._lock:
            return self._total

    @property
    def last_backoff(self) -> float:
        with self._lock:
            return self._last_backoff

    @property
    def first_failure_at(self) -> float | None:
        with self._lock:
            return self._first_failure_at

    @property
    def state(self) -> HandoffState:
        with self._lock:
            return self._state_locked()

    def _state_locked(self) -> HandoffState:
        if self._consecutive == 0:
            return HandoffState.HEALTHY
        if self._consecutive >= self._config.max_consecutive_failures:
            return HandoffState.EXHAUSTED
        if self._first_failure_at is not None and (
            self._clock() - self._first_failure_at >= self._config.recovery_deadline
        ):
            return HandoffState.EXHAUSTED
        return HandoffState.DEGRADED

    @property
    def is_exhausted(self) -> bool:
        return self.state is HandoffState.EXHAUSTED

    @property
    def is_paused(self) -> bool:
        """True while degraded or exhausted (drives the ``retry_handoff_paused`` gauge)."""
        return self.state is not HandoffState.HEALTHY

    # ── transitions ───────────────────────────────────────────────────────

    def record_failure(self) -> float:
        """Register one handoff failure; return the backoff to apply (seconds)."""
        with self._lock:
            now = self._clock()
            self._consecutive += 1
            self._total += 1
            if self._first_failure_at is None:
                self._first_failure_at = now
            self._last_failure_at = now
            cfg = self._config
            base = _capped_exponential(cfg.backoff_initial, cfg.backoff_max, self._consecutive)
            if cfg.jitter > 0:
                spread = base * cfg.jitter
                base = base - spread + (2 * spread * self._rng())
            self._last_backoff = max(0.0, min(cfg.backoff_max, base))
            return self._last_backoff

    def record_success(self) -> None:
        """A handoff succeeded: the streak ends; the deadline clock resets."""
        with self._lock:
            self._consecutive = 0
            self._first_failure_at = None
            self._last_failure_at = None
            self._last_backoff = 0.0

    def reset(self) -> None:
        """Forget everything (e.g. after a deliberate consumer restart)."""
        with self._lock:
            self._consecutive = 0
            self._total = 0
            self._first_failure_at = None
            self._last_failure_at = None
            self._last_backoff = 0.0
