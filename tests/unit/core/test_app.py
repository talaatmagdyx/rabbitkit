"""Tests for core/app.py — RabbitApp lifecycle, hooks, state machine."""

from __future__ import annotations

import asyncio

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
