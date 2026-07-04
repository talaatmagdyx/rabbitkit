"""Tests for HealthWatcher / AsyncHealthWatcher — debounced transitions."""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.health import (
    AsyncHealthWatcher,
    BrokerHealthResult,
    HealthStatus,
    HealthWatcher,
)


def _result(status: HealthStatus) -> BrokerHealthResult:
    return BrokerHealthResult(status=status)


def _watcher(**kwargs: Any) -> HealthWatcher:
    defaults: dict[str, Any] = {"interval": 10.0, "debounce": 2}
    defaults.update(kwargs)
    return HealthWatcher(MagicMock(), **defaults)


class TestDebouncedStateMachine:
    """Drive _apply directly — no clocks, no sleeps (the 1.2.0 deflaking
    lesson: logical assertions over wall time)."""

    def test_first_status_confirms_after_debounce(self) -> None:
        changes: list[tuple[Any, Any]] = []
        w = _watcher(on_change=lambda o, n, r: changes.append((o, n)))

        w._apply(_result(HealthStatus.HEALTHY))
        assert w.current_status is None  # 1 reading < debounce=2
        w._apply(_result(HealthStatus.HEALTHY))
        assert w.current_status == HealthStatus.HEALTHY
        assert changes == [(None, HealthStatus.HEALTHY)]

    def test_single_flap_never_fires(self) -> None:
        """The reason debounce exists: one bad poll must not page anyone."""
        changes: list[Any] = []
        w = _watcher(on_change=lambda o, n, r: changes.append(n))
        for _ in range(2):
            w._apply(_result(HealthStatus.HEALTHY))
        changes.clear()

        w._apply(_result(HealthStatus.DEGRADED))  # one flap
        w._apply(_result(HealthStatus.HEALTHY))   # back to confirmed state
        w._apply(_result(HealthStatus.HEALTHY))

        assert changes == []
        assert w.current_status == HealthStatus.HEALTHY

    def test_sustained_change_fires_all_edges(self) -> None:
        """Walk HEALTHY -> DEGRADED -> UNHEALTHY -> HEALTHY."""
        changes: list[tuple[Any, Any]] = []
        w = _watcher(on_change=lambda o, n, r: changes.append((o, n)))

        for status in (
            HealthStatus.HEALTHY,
            HealthStatus.DEGRADED,
            HealthStatus.UNHEALTHY,
            HealthStatus.HEALTHY,
        ):
            for _ in range(2):
                w._apply(_result(status))

        assert changes == [
            (None, HealthStatus.HEALTHY),
            (HealthStatus.HEALTHY, HealthStatus.DEGRADED),
            (HealthStatus.DEGRADED, HealthStatus.UNHEALTHY),
            (HealthStatus.UNHEALTHY, HealthStatus.HEALTHY),
        ]

    def test_candidate_switch_resets_count(self) -> None:
        """DEGRADED, then UNHEALTHY, then UNHEALTHY: the DEGRADED reading
        must not count toward UNHEALTHY's debounce."""
        changes: list[Any] = []
        w = _watcher(on_change=lambda o, n, r: changes.append(n))
        for _ in range(2):
            w._apply(_result(HealthStatus.HEALTHY))
        changes.clear()

        w._apply(_result(HealthStatus.DEGRADED))
        w._apply(_result(HealthStatus.UNHEALTHY))
        assert changes == []  # count restarted at the switch
        w._apply(_result(HealthStatus.UNHEALTHY))
        assert changes == [HealthStatus.UNHEALTHY]

    def test_debounce_one_fires_immediately(self) -> None:
        changes: list[Any] = []
        w = _watcher(debounce=1, on_change=lambda o, n, r: changes.append(n))
        w._apply(_result(HealthStatus.DEGRADED))
        assert changes == [HealthStatus.DEGRADED]

    def test_callback_exception_logged_never_raised(self, caplog: pytest.LogCaptureFixture) -> None:
        def bad_cb(o: Any, n: Any, r: Any) -> None:
            raise RuntimeError("pager exploded")

        w = _watcher(debounce=1, on_change=bad_cb)
        with caplog.at_level("ERROR", logger="rabbitkit.health"):
            w._apply(_result(HealthStatus.DEGRADED))  # must not raise
        assert w.current_status == HealthStatus.DEGRADED
        assert any("on_change callback raised" in r.message for r in caplog.records)

    def test_no_callback_is_fine(self) -> None:
        w = _watcher(on_change=None, debounce=1)
        w._apply(_result(HealthStatus.HEALTHY))
        assert w.current_status == HealthStatus.HEALTHY

    def test_gauge_emitted_every_apply(self) -> None:
        collector = MagicMock()
        w = _watcher(collector=collector, debounce=1)
        w._apply(_result(HealthStatus.HEALTHY))
        w._apply(_result(HealthStatus.UNHEALTHY))

        values = [c.args[2] for c in collector.set_gauge.call_args_list]
        names = {c.args[0] for c in collector.set_gauge.call_args_list}
        assert values == [0, 2]
        assert names == {"rabbitkit_health_state"}

    def test_invalid_construction(self) -> None:
        with pytest.raises(ValueError, match="interval"):
            HealthWatcher(MagicMock(), interval=0)
        with pytest.raises(ValueError, match="debounce"):
            HealthWatcher(MagicMock(), debounce=0)


class TestTickWiring:
    def test_tick_calls_health_check_with_management_client(self) -> None:
        mgmt = MagicMock()
        w = _watcher(management_client=mgmt, debounce=1)
        with patch("rabbitkit.health.broker_health_check", return_value=_result(HealthStatus.HEALTHY)) as hc:
            w._tick()
        assert hc.call_args.kwargs["management_client"] is mgmt
        assert w.current_status == HealthStatus.HEALTHY


class TestLifecycle:
    def test_start_stop_and_idempotency(self) -> None:
        w = _watcher(interval=0.01, debounce=1)
        ticked = threading.Event()
        w._tick = lambda: ticked.set()  # type: ignore[method-assign]

        w.start()
        first_thread = w._thread
        w.start()  # idempotent: same live thread kept
        assert w._thread is first_thread

        assert ticked.wait(timeout=5.0), "poller never ticked"
        w.stop()
        assert w._thread is None
        w.stop()  # idempotent


class TestAsyncWatcher:
    async def test_tick_async_uses_async_health_check(self) -> None:
        from unittest.mock import AsyncMock

        w = AsyncHealthWatcher(MagicMock(), interval=10.0, debounce=1)
        with patch(
            "rabbitkit.health.broker_health_check_async",
            new=AsyncMock(return_value=_result(HealthStatus.DEGRADED)),
        ):
            await w._tick_async()
        assert w.current_status == HealthStatus.DEGRADED

    async def test_run_loop_ticks_until_cancelled(self) -> None:
        import asyncio

        w = AsyncHealthWatcher(MagicMock(), interval=0.005, debounce=1)
        ticks = 0

        async def fake_tick() -> None:
            nonlocal ticks
            ticks += 1

        w._tick_async = fake_tick  # type: ignore[method-assign]
        task = asyncio.create_task(w.run())
        while ticks < 2:
            await asyncio.sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert ticks >= 2
