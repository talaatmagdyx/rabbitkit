"""RetryMiddleware: ``retry_handoff_paused`` and the handoff failure streak
follow whether the route is still failing its retry handoffs (0.20.4).

Before 0.20.4 only a successful delay-queue publish cleared them. A route
that recovered and then handled its deliveries without a new retry kept the
gauge at 1 forever (rabbitkit's own ``RetryStormDetected`` alert fired for
good), and its next handoff failure, hours later, was EXHAUSTED at once.

The clock is injected through the tracker; nothing here sleeps.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from rabbitkit.core.config import MetricsConfig, RetryConfig, RetryHandoffConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.retry_handoff import RetryHandoffTracker
from rabbitkit.core.types import REQUEUED_FOR_RETRY, HandoffState, MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.middleware.retry import RetryMiddleware
from tests.unit.middleware.test_metrics_inventory import RecordingCollector

if TYPE_CHECKING:
    import pytest

_CFG = MetricsConfig()
PAUSED = _CFG.retry_handoff_paused
#: RetryHandoffConfig defaults: recovery_deadline 300 s + backoff_max 30 s.
QUIET = 330.0


class _Clock:
    def __init__(self) -> None:
        self.t = 1_000.0

    def __call__(self) -> float:
        return self.t


class _DelayQueue:
    """The delay-queue publish: refused (NACKED) while ``broken``."""

    def __init__(self) -> None:
        self.broken = True

    def __call__(self, envelope: MessageEnvelope) -> PublishOutcome:
        return PublishOutcome(status=PublishStatus.NACKED if self.broken else PublishStatus.CONFIRMED)

    async def publish_async(self, envelope: MessageEnvelope) -> PublishOutcome:
        return self(envelope)


def _message() -> RabbitMessage:
    message = RabbitMessage(
        body=b"{}", routing_key="orders", message_id="m1", headers={"x-rabbitkit-original-queue": "orders"}
    )
    settled: list[tuple[str, Any]] = []
    message.raw_message = settled
    message._ack_fn = lambda: settled.append(("ack", None))
    message._nack_fn = lambda requeue: settled.append(("nack", requeue))

    async def ack() -> None:
        settled.append(("ack", None))

    async def nack(requeue: bool) -> None:
        settled.append(("nack", requeue))

    message._ack_async_fn = ack
    message._nack_async_fn = nack
    return message


def _ok(message: RabbitMessage) -> str:
    return "done"


def _fail(message: RabbitMessage) -> None:
    raise TimeoutError("redis down")  # transient: retried through the delay queue


async def _ok_async(message: RabbitMessage) -> str:
    return "done"


async def _fail_async(message: RabbitMessage) -> None:
    raise TimeoutError("redis down")


class _Route:
    """One RetryMiddleware with an injected clock, a fake delay queue and a
    recording collector, plus the hook calls it made."""

    def __init__(self, handoff: RetryHandoffConfig | None = None) -> None:
        self.handoff = handoff or RetryHandoffConfig(jitter=0.0)
        self.clock = _Clock()
        self.delay_queue = _DelayQueue()
        self.collector = RecordingCollector()
        self.backoffs: list[float] = []
        self.exhausted: list[RetryHandoffTracker] = []
        self.mw = RetryMiddleware(
            RetryConfig(max_retries=3, delays=(1, 1, 1), handoff=self.handoff),
            publish_fn=self.delay_queue,
            publish_async_fn=self.delay_queue.publish_async,
            metrics_collector=self.collector,
            metrics_config=_CFG,
            handoff_tracker=RetryHandoffTracker(self.handoff, clock=self.clock),
            on_handoff_failure=lambda backoff, _tracker: self.backoffs.append(backoff),
            on_handoff_exhausted=self.exhausted.append,
        )

    @property
    def tracker(self) -> RetryHandoffTracker:
        return self.mw.handoff_tracker

    @property
    def paused(self) -> float | None:
        return self.collector.gauge(PAUSED, queue="orders")

    def deliver(self, *, fails: bool) -> Any:
        return self.mw.consume_scope(_fail if fails else _ok, _message())

    async def deliver_async(self, *, fails: bool) -> Any:
        return await self.mw.consume_scope_async(_fail_async if fails else _ok_async, _message())


# ── the observed defect: the gauge stays 1 after the route recovered ──────


class TestGaugeClearsAfterRecovery:
    def test_a_successful_delivery_after_the_quiet_period_clears_the_gauge(self) -> None:
        route = _Route()
        assert route.deliver(fails=True) is REQUEUED_FOR_RETRY
        assert route.paused == 1.0
        route.delay_queue.broken = False  # the retry topology is restored...
        route.clock.t += QUIET
        assert route.deliver(fails=False) == "done"  # ...and deliveries succeed, with no new handoff
        assert route.paused == 0.0
        assert route.tracker.state is HandoffState.HEALTHY

    def test_an_idle_route_is_cleared_by_the_periodic_refresh(self) -> None:
        route = _Route()
        route.deliver(fails=True)
        route.clock.t += QUIET - 1.0
        route.mw.refresh_handoff_paused()
        assert route.paused == 1.0  # the route may still be failing: not cleared early
        route.clock.t += 1.0
        route.mw.refresh_handoff_paused()
        assert route.paused == 0.0

    def test_a_handoff_success_still_clears_at_once(self) -> None:
        route = _Route()
        route.deliver(fails=True)
        route.delay_queue.broken = False
        route.clock.t += 1.0
        route.deliver(fails=True)  # retried: the delay-queue publish is confirmed
        assert route.paused == 0.0
        assert route.tracker.state is HandoffState.HEALTHY

    async def test_async_route_clears_after_the_quiet_period(self, monkeypatch: pytest.MonkeyPatch) -> None:
        held: list[float] = []

        async def hold(seconds: float) -> None:
            held.append(seconds)

        import rabbitkit.middleware.retry as retry_mod

        monkeypatch.setattr(retry_mod.asyncio, "sleep", hold)
        route = _Route()
        async with asyncio.timeout(5):
            assert await route.deliver_async(fails=True) is REQUEUED_FOR_RETRY
            assert route.paused == 1.0 and held == [route.handoff.backoff_initial]
            route.clock.t += QUIET
            assert await route.deliver_async(fails=False) == "done"
        assert route.paused == 0.0


# ── what the cleanup must not hide ────────────────────────────────────────


class TestOngoingFailureIsNotHidden:
    def test_successful_deliveries_between_failing_handoffs_keep_the_gauge_at_1(self) -> None:
        """A broken retry topology with a mixed workload: handler successes are
        not handoffs, so they neither clear the gauge nor reset the streak."""
        # A long deadline, so only the count can make the streak EXHAUSTED.
        route = _Route(RetryHandoffConfig(jitter=0.0, max_consecutive_failures=20, recovery_deadline=10_000.0))
        seen: list[float | None] = []
        for _ in range(20):
            route.deliver(fails=True)
            seen.append(route.paused)
            route.clock.t += route.handoff.backoff_max  # the EXHAUSTED hold
            route.deliver(fails=False)
            route.mw.refresh_handoff_paused()
            seen.append(route.paused)
        assert seen == [1.0] * 40
        assert route.tracker.state is HandoffState.EXHAUSTED
        assert route.tracker.consecutive_failures == 20
        assert route.exhausted == [route.tracker]

    def test_a_streak_reaches_exhausted_by_deadline_while_failures_continue(self) -> None:
        route = _Route(RetryHandoffConfig(jitter=0.0, max_consecutive_failures=10**6))
        for _ in range(11):  # one failure every backoff_max for 300 s
            route.deliver(fails=True)
            route.clock.t += route.handoff.backoff_max
            route.mw.refresh_handoff_paused()
            assert route.paused == 1.0
        assert route.tracker.is_exhausted
        assert route.exhausted == [route.tracker]
        assert route.backoffs[-1] == route.handoff.backoff_max


# ── the failure streak after a recovered outage ────────────────────────────


class TestStreakAfterRecoveredOutage:
    def test_a_single_failure_long_after_an_exhausted_outage_is_not_exhausted(self) -> None:
        route = _Route(RetryHandoffConfig(jitter=0.0, max_consecutive_failures=20))
        for _ in range(20):
            route.deliver(fails=True)
            route.clock.t += 1.0
        assert route.exhausted == [route.tracker]
        route.clock.t += 3600.0  # an hour of successful deliveries, none of them a handoff
        route.deliver(fails=True)  # one later blip of the retry topology
        assert route.tracker.state is HandoffState.DEGRADED
        assert route.tracker.consecutive_failures == 1
        assert route.backoffs[-1] == route.handoff.backoff_initial  # not the backoff_max hold
        assert route.exhausted == [route.tracker]  # on_handoff_exhausted is not fired again

    def test_the_exhausted_alarm_is_logged_again_for_a_later_outage(self, caplog: pytest.LogCaptureFixture) -> None:
        route = _Route(RetryHandoffConfig(jitter=0.0, max_consecutive_failures=2))
        with caplog.at_level(logging.INFO, logger="rabbitkit.middleware.retry"):
            for _ in range(3):
                route.deliver(fails=True)
            route.clock.t += QUIET
            route.mw.refresh_handoff_paused()
            for _ in range(3):
                route.deliver(fails=True)
        exhausted = [r for r in caplog.records if "EXHAUSTED" in r.getMessage()]
        cleared = [r for r in caplog.records if "no longer paused" in r.getMessage()]
        assert len(exhausted) == 2
        assert len(cleared) == 1


# ── shutdown ───────────────────────────────────────────────────────────────


class TestShutdown:
    def test_clear_sets_the_gauge_to_0_and_a_new_failure_sets_it_again(self) -> None:
        route = _Route()
        route.deliver(fails=True)
        route.mw.clear_handoff_paused()
        assert route.paused == 0.0
        route.mw.refresh_handoff_paused()
        assert route.paused == 0.0
        route.deliver(fails=True)
        assert route.paused == 1.0

    def test_without_metrics_refresh_and_clear_are_no_ops(self) -> None:
        clock = _Clock()
        handoff = RetryHandoffConfig(jitter=0.0)
        mw = RetryMiddleware(
            RetryConfig(max_retries=1, delays=(1,), handoff=handoff),
            publish_fn=_DelayQueue(),
            handoff_tracker=RetryHandoffTracker(handoff, clock=clock),
        )
        mw.consume_scope(_fail, _message())
        clock.t += QUIET
        mw.refresh_handoff_paused()
        mw.clear_handoff_paused()
        assert mw.handoff_tracker.state is HandoffState.HEALTHY


class _BrokenGaugeCollector(RecordingCollector):
    def __init__(self) -> None:
        super().__init__()
        self.broken = False

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        if self.broken:
            raise RuntimeError("collector bug")
        super().set_gauge(name, labels, value)


def test_a_failing_gauge_refresh_never_fails_the_delivery(caplog: pytest.LogCaptureFixture) -> None:
    route = _Route()
    collector = _BrokenGaugeCollector()
    route.mw._metrics_collector = collector
    route.deliver(fails=True)
    collector.broken = True
    route.clock.t += QUIET
    with caplog.at_level(logging.WARNING, logger="rabbitkit.middleware.retry"):
        assert route.deliver(fails=False) == "done"
    assert "Refreshing retry_handoff_paused failed" in caplog.text
