"""Tests for testing/app.py — TestApp."""

from __future__ import annotations

import pytest

from rabbitkit.core.app import AppState, RabbitApp
from rabbitkit.testing.app import TestApp
from rabbitkit.testing.broker import TestBroker

# ── Sync lifecycle ───────────────────────────────────────────────────────


class TestSyncLifecycle:
    def test_start_runs_hooks_and_broker(self) -> None:
        app = RabbitApp(title="test")
        broker = TestBroker()
        ta = TestApp(app, broker)

        startup_called = False

        @app.on_startup
        def on_startup() -> None:
            nonlocal startup_called
            startup_called = True

        ta.start()

        assert startup_called
        assert ta.state == AppState.RUNNING

    def test_stop_runs_hooks(self) -> None:
        app = RabbitApp(title="test")
        broker = TestBroker()
        ta = TestApp(app, broker)

        shutdown_called = False

        @app.on_shutdown
        def on_shutdown() -> None:
            nonlocal shutdown_called
            shutdown_called = True

        ta.start()
        ta.stop()

        assert shutdown_called
        assert ta.state == AppState.STOPPED

    def test_context_manager(self) -> None:
        app = RabbitApp(title="test")
        broker = TestBroker()
        ta = TestApp(app, broker)

        with ta:
            assert ta.state == AppState.RUNNING

        assert ta.state == AppState.STOPPED

    def test_properties(self) -> None:
        app = RabbitApp(title="test")
        broker = TestBroker()
        ta = TestApp(app, broker)

        assert ta.app is app
        assert ta.broker is broker

    def test_reset(self) -> None:
        app = RabbitApp(title="test")
        broker = TestBroker()
        ta = TestApp(app, broker)

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            pass

        ta.start()
        broker.publish("orders", b"hello")

        assert len(broker.consumed_messages) == 1

        ta.reset()

        assert len(broker.consumed_messages) == 0


# ── Async lifecycle ──────────────────────────────────────────────────────


class TestAsyncLifecycle:
    @pytest.mark.asyncio
    async def test_async_start_stop(self) -> None:
        app = RabbitApp(title="test")
        broker = TestBroker()
        ta = TestApp(app, broker)

        startup_called = False

        @app.on_startup
        async def on_startup() -> None:
            nonlocal startup_called
            startup_called = True

        await ta.start_async()
        assert startup_called
        assert ta.state == AppState.RUNNING

        await ta.stop_async()
        assert ta.state == AppState.STOPPED

    @pytest.mark.asyncio
    async def test_async_context_manager(self) -> None:
        app = RabbitApp(title="test")
        broker = TestBroker()
        ta = TestApp(app, broker)

        async with ta:
            assert ta.state == AppState.RUNNING

        assert ta.state == AppState.STOPPED


# ── Integration with TestBroker ──────────────────────────────────────────


class TestIntegration:
    def test_full_flow(self) -> None:
        """Full lifecycle: register → start → publish → assert → stop."""
        app = RabbitApp(title="test")
        broker = TestBroker()
        ta = TestApp(app, broker)

        result_body = None

        @broker.subscriber(queue="orders")
        def handle(body: bytes) -> None:
            nonlocal result_body
            result_body = body

        with ta:
            broker.publish("orders", b'{"id": 1}')

        assert result_body == b'{"id": 1}'
        handle.mock.assert_called_once()
