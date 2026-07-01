"""Tests for core/app.py — RabbitApp lifecycle, hooks, state machine."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from rabbitkit.core.app import AppState, RabbitApp

# ── state machine ────────────────────────────────────────────────────────


class TestStateMachine:
    def test_initial_state(self) -> None:
        app = RabbitApp()
        assert app.state == AppState.IDLE

    def test_start_transitions(self) -> None:
        app = RabbitApp()
        app.start()
        assert app.state == AppState.RUNNING

    def test_stop_transitions(self) -> None:
        app = RabbitApp()
        app.start()
        app.stop()
        assert app.state == AppState.STOPPED

    def test_start_idempotent(self) -> None:
        app = RabbitApp()
        call_count = 0

        @app.on_startup
        def hook() -> None:
            nonlocal call_count
            call_count += 1

        app.start()
        app.start()  # no-op
        assert call_count == 1
        assert app.state == AppState.RUNNING

    def test_stop_idempotent(self) -> None:
        app = RabbitApp()
        app.start()
        app.stop()
        app.stop()  # no-op
        assert app.state == AppState.STOPPED

    def test_stop_from_idle_noop(self) -> None:
        app = RabbitApp()
        app.stop()
        assert app.state == AppState.IDLE

    def test_title(self) -> None:
        app = RabbitApp(title="my-service")
        assert app.title == "my-service"


# ── sync lifecycle hooks ────────────────────────────────────────────────


class TestSyncHooks:
    def test_on_startup_hook(self) -> None:
        app = RabbitApp()
        called = False

        @app.on_startup
        def hook() -> None:
            nonlocal called
            called = True

        app.start()
        assert called

    def test_after_startup_hook(self) -> None:
        app = RabbitApp()
        order: list[str] = []

        @app.on_startup
        def startup() -> None:
            order.append("startup")

        @app.after_startup
        def after() -> None:
            order.append("after_startup")

        app.start()
        assert order == ["startup", "after_startup"]

    def test_on_shutdown_hook(self) -> None:
        app = RabbitApp()
        called = False

        @app.on_shutdown
        def hook() -> None:
            nonlocal called
            called = True

        app.start()
        app.stop()
        assert called

    def test_after_shutdown_hook(self) -> None:
        app = RabbitApp()
        order: list[str] = []

        @app.on_shutdown
        def shutdown() -> None:
            order.append("shutdown")

        @app.after_shutdown
        def after() -> None:
            order.append("after_shutdown")

        app.start()
        app.stop()
        assert order == ["shutdown", "after_shutdown"]

    def test_full_lifecycle_order(self) -> None:
        app = RabbitApp()
        order: list[str] = []

        @app.on_startup
        def s1() -> None:
            order.append("on_startup")

        @app.after_startup
        def s2() -> None:
            order.append("after_startup")

        @app.on_shutdown
        def s3() -> None:
            order.append("on_shutdown")

        @app.after_shutdown
        def s4() -> None:
            order.append("after_shutdown")

        app.start()
        app.stop()
        assert order == ["on_startup", "after_startup", "on_shutdown", "after_shutdown"]

    def test_startup_failure_runs_shutdown(self) -> None:
        app = RabbitApp()
        shutdown_called = False

        @app.on_startup
        def bad_startup() -> None:
            raise RuntimeError("startup failed")

        @app.on_shutdown
        def cleanup() -> None:
            nonlocal shutdown_called
            shutdown_called = True

        with pytest.raises(RuntimeError, match="startup failed"):
            app.start()

        assert shutdown_called
        assert app.state == AppState.STOPPED

    def test_multiple_startup_hooks(self) -> None:
        app = RabbitApp()
        order: list[int] = []

        @app.on_startup
        def hook1() -> None:
            order.append(1)

        @app.on_startup
        def hook2() -> None:
            order.append(2)

        app.start()
        assert order == [1, 2]

    def test_async_hook_in_sync_raises(self) -> None:
        app = RabbitApp()

        @app.on_startup
        async def bad_hook() -> None:
            pass

        with pytest.raises(TypeError, match="Async hook"):
            app.start()


# ── async lifecycle hooks ───────────────────────────────────────────────


class TestAsyncHooks:
    @pytest.mark.asyncio
    async def test_async_start_stop(self) -> None:
        app = RabbitApp()
        await app.start_async()
        assert app.state == AppState.RUNNING
        await app.stop_async()
        assert app.state == AppState.STOPPED

    @pytest.mark.asyncio
    async def test_async_startup_hooks(self) -> None:
        app = RabbitApp()
        order: list[str] = []

        @app.on_startup
        async def hook() -> None:
            order.append("async_startup")

        await app.start_async()
        assert order == ["async_startup"]

    @pytest.mark.asyncio
    async def test_async_shutdown_hooks(self) -> None:
        app = RabbitApp()
        called = False

        @app.on_shutdown
        async def hook() -> None:
            nonlocal called
            called = True

        await app.start_async()
        await app.stop_async()
        assert called

    @pytest.mark.asyncio
    async def test_async_mixed_hooks(self) -> None:
        """Async lifecycle supports both sync and async hooks."""
        app = RabbitApp()
        order: list[str] = []

        @app.on_startup
        def sync_hook() -> None:
            order.append("sync")

        @app.on_startup
        async def async_hook() -> None:
            order.append("async")

        await app.start_async()
        assert order == ["sync", "async"]

    @pytest.mark.asyncio
    async def test_async_startup_failure_runs_shutdown(self) -> None:
        app = RabbitApp()
        shutdown_called = False

        @app.on_startup
        async def bad_startup() -> None:
            raise RuntimeError("async startup failed")

        @app.on_shutdown
        async def cleanup() -> None:
            nonlocal shutdown_called
            shutdown_called = True

        with pytest.raises(RuntimeError, match="async startup failed"):
            await app.start_async()

        assert shutdown_called
        assert app.state == AppState.STOPPED

    @pytest.mark.asyncio
    async def test_async_start_idempotent(self) -> None:
        app = RabbitApp()
        count = 0

        @app.on_startup
        async def hook() -> None:
            nonlocal count
            count += 1

        await app.start_async()
        await app.start_async()  # no-op
        assert count == 1


# ── request_shutdown ─────────────────────────────────────────────────────


class TestRequestShutdown:
    def test_request_shutdown_no_event(self) -> None:
        app = RabbitApp()
        # Should not raise even without event
        app.request_shutdown()

    @pytest.mark.asyncio
    async def test_request_shutdown_sets_event(self) -> None:
        app = RabbitApp()
        app._shutdown_event = asyncio.Event()
        app.request_shutdown()
        assert app._shutdown_event.is_set()


# ── hook registration returns decorator ─────────────────────────────────


class TestHookDecorators:
    def test_on_startup_returns_func(self) -> None:
        app = RabbitApp()

        @app.on_startup
        def hook() -> None:
            pass

        assert callable(hook)

    def test_after_startup_returns_func(self) -> None:
        app = RabbitApp()

        @app.after_startup
        def hook() -> None:
            pass

        assert callable(hook)

    def test_on_shutdown_returns_func(self) -> None:
        app = RabbitApp()

        @app.on_shutdown
        def hook() -> None:
            pass

        assert callable(hook)

    def test_after_shutdown_returns_func(self) -> None:
        app = RabbitApp()

        @app.after_shutdown
        def hook() -> None:
            pass

        assert callable(hook)


# ── stop_async idempotent (line 162) ────────────────────────────────────


class TestAsyncStopIdempotent:
    async def test_stop_async_from_idle_noop(self) -> None:
        """stop_async on a never-started app is a no-op (IDLE guard)."""
        app = RabbitApp()
        await app.stop_async()
        # State remains IDLE — the early-return branch was hit
        assert app.state == AppState.IDLE

    async def test_stop_async_twice_noop(self) -> None:
        """Second stop_async after the app is already STOPPED is a no-op."""
        app = RabbitApp()
        await app.start_async()
        await app.stop_async()
        assert app.state == AppState.STOPPED

        # This second call should hit the early-return guard (line 162)
        await app.stop_async()
        assert app.state == AppState.STOPPED


# ── run_async (lines 179-193) ────────────────────────────────────────────


class TestRunAsync:
    async def test_run_async_starts_and_stops(self) -> None:
        """run_async starts the app, waits for shutdown signal, then stops."""
        app = RabbitApp()

        async def trigger_shutdown() -> None:
            await asyncio.sleep(0.01)
            app.request_shutdown()

        async with asyncio.timeout(2.0):
            _task = asyncio.create_task(trigger_shutdown())
            await app.run_async()

        assert app.state == AppState.STOPPED

    async def test_run_async_runs_hooks(self) -> None:
        """run_async fires startup and shutdown hooks in order."""
        app = RabbitApp()
        order: list[str] = []

        @app.on_startup
        async def on_start() -> None:
            order.append("startup")

        @app.on_shutdown
        async def on_stop() -> None:
            order.append("shutdown")

        async def trigger_shutdown() -> None:
            await asyncio.sleep(0.01)
            app.request_shutdown()

        async with asyncio.timeout(2.0):
            _task = asyncio.create_task(trigger_shutdown())
            await app.run_async()

        assert order == ["startup", "shutdown"]


# ── _signal_handler (lines 204-205) ─────────────────────────────────────


class TestSignalHandler:
    def test_signal_handler_calls_request_shutdown(self) -> None:
        """_signal_handler sets the shutdown event via request_shutdown."""
        app = RabbitApp()
        app._shutdown_event = asyncio.Event()

        app._signal_handler()

        assert app._shutdown_event.is_set()


# ── startup_timeout bounds hung hooks (I-12) ──────────────────────────────


class TestStartupTimeoutBounds:
    """I-12: ``startup_timeout`` must actually bound a hung hook instead of
    hanging the caller forever (the old ``with ThreadPoolExecutor()`` context
    manager called ``shutdown(wait=True)`` on ``__exit__``).
    """

    def test_hung_sync_hook_start_raises_timeout(self) -> None:
        app = RabbitApp(startup_timeout=0.1)

        @app.on_startup
        def slow_hook() -> None:
            time.sleep(0.4)  # well beyond the timeout

        with pytest.raises(TimeoutError, match="exceeded timeout"):
            app.start()

        # Startup failure → on_shutdown still called → STOPPED.
        assert app.state == AppState.STOPPED

    @pytest.mark.asyncio
    async def test_hung_sync_hook_start_async_raises_timeout(self) -> None:
        app = RabbitApp(startup_timeout=0.1)

        @app.on_startup
        def slow_hook() -> None:
            time.sleep(0.4)

        with pytest.raises(TimeoutError, match="exceeded timeout"):
            await app.start_async()

        assert app.state == AppState.STOPPED

    def test_normal_hook_completes_within_timeout(self) -> None:
        app = RabbitApp(startup_timeout=1.0)
        called = False

        @app.on_startup
        def quick_hook() -> None:
            nonlocal called
            called = True

        app.start()
        assert called
        assert app.state == AppState.RUNNING

    @pytest.mark.asyncio
    async def test_normal_async_hook_completes_within_timeout(self) -> None:
        app = RabbitApp(startup_timeout=1.0)
        called = False

        @app.on_startup
        async def quick_hook() -> None:
            nonlocal called
            called = True

        await app.start_async()
        assert called
        assert app.state == AppState.RUNNING

    @pytest.mark.asyncio
    async def test_hung_async_hook_start_async_raises_timeout(self) -> None:
        """A coroutine-function hook that never awaits completion is bounded."""
        app = RabbitApp(startup_timeout=0.1)
        event = asyncio.Event()

        @app.on_startup
        async def slow_async_hook() -> None:
            await event.wait()  # never set

        with pytest.raises(TimeoutError, match="exceeded timeout"):
            await app.start_async()

        assert app.state == AppState.STOPPED

    def test_threading_hook_not_confused_for_async(self) -> None:
        """A sync hook that uses threads is NOT mistaken for an async hook."""
        app = RabbitApp(startup_timeout=1.0)
        done = threading.Event()

        @app.on_startup
        def threaded_hook() -> None:
            done.set()

        app.start()
        assert done.is_set()
        assert app.state == AppState.RUNNING

    # ── SRE-M-2: default startup_timeout is finite (120s) ──────────────────

    def test_default_startup_timeout_is_finite(self) -> None:
        """Out-of-the-box ``RabbitApp()`` has a finite startup_timeout (120.0s)
        so a hung startup hook fails fast instead of hanging until SIGKILL.
        Passing ``startup_timeout=None`` explicitly restores unbounded behavior.
        """
        app = RabbitApp()
        assert app._startup_timeout == 120.0

        unbounded = RabbitApp(startup_timeout=None)
        assert unbounded._startup_timeout is None

    def test_default_startup_timeout_bounds_hung_hook(self) -> None:
        """With the default finite timeout, a hung startup hook raises
        TimeoutError. The real default (120s) is too long to wait in CI, so the
        instance timeout is overridden to a tiny value post-construction to
        exercise the same bounded-default code path quickly.
        """
        app = RabbitApp()
        assert app._startup_timeout is not None  # bounded by default
        app._startup_timeout = 0.1

        @app.on_startup
        def slow_hook() -> None:
            time.sleep(0.4)  # well beyond the overridden timeout

        with pytest.raises(TimeoutError, match="exceeded timeout"):
            app.start()

        # Startup failure → on_shutdown still called → STOPPED.
        assert app.state == AppState.STOPPED

    @pytest.mark.asyncio
    async def test_default_startup_timeout_bounds_hung_hook_async(self) -> None:
        """Async twin: the default finite timeout bounds a hung async hook."""
        app = RabbitApp()
        assert app._startup_timeout is not None
        app._startup_timeout = 0.1
        event = asyncio.Event()

        @app.on_startup
        async def slow_async_hook() -> None:
            await event.wait()  # never set

        with pytest.raises(TimeoutError, match="exceeded timeout"):
            await app.start_async()

        assert app.state == AppState.STOPPED


# ── _run_hooks_sync with timeout=None raises for async hook (line 269) ───


class TestRunHooksSyncNoTimeout:
    def test_async_hook_no_timeout_raises_typeerror(self) -> None:
        """Line 269: _run_hooks_sync(timeout=None) raises TypeError for async hook."""
        app = RabbitApp()

        async def async_hook() -> None:
            pass

        with pytest.raises(TypeError, match="Async hook"):
            app._run_hooks_sync([async_hook], timeout=None)


# ── _run_hooks_async: sync hook returning asyncio.Future (lines 318-322) ──


class TestRunHooksAsyncFutureResult:
    @pytest.mark.asyncio
    async def test_sync_hook_returning_future_is_awaited(self) -> None:
        """Lines 318-322: when a sync hook returns an asyncio.Future, it is awaited."""
        app = RabbitApp()

        # Pre-create a resolved future in the async context (not inside to_thread)
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[None] = loop.create_future()
        fut.set_result(None)

        # The hook is sync (not a coroutinefunction) and returns the pre-resolved future
        def hook_returning_future() -> asyncio.Future[None]:
            return fut

        await app._run_hooks_async([hook_returning_future], timeout=2.0)
        assert fut.done()

    @pytest.mark.asyncio
    async def test_sync_hook_returning_future_timeout_raises(self) -> None:
        """Lines 318-322: when the awaited Future exceeds timeout, TimeoutError is raised."""
        app = RabbitApp()

        # Pre-create an unresolved future in the async context
        loop = asyncio.get_event_loop()
        pending_fut: asyncio.Future[None] = loop.create_future()

        def hook_returning_pending_future() -> asyncio.Future[None]:
            return pending_fut

        with pytest.raises(TimeoutError, match="exceeded timeout"):
            await app._run_hooks_async([hook_returning_pending_future], timeout=0.05)
