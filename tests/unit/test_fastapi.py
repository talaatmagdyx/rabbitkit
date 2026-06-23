"""Tests for fastapi.py — rabbitkit_lifespan."""

from __future__ import annotations

from rabbitkit.fastapi import rabbitkit_lifespan

# ── helpers ───────────────────────────────────────────────────────────────


class _FakeAsyncBroker:
    """Fake async broker with start/stop coroutines."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class _FakeSyncBroker:
    """Fake sync broker with regular start/stop methods."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True


class _FakeRabbitApp:
    """Fake RabbitApp with async start/stop."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start_async(self) -> None:
        self.started = True

    async def stop_async(self) -> None:
        self.stopped = True


# ── async broker tests ───────────────────────────────────────────────────


class TestAsyncBroker:
    async def test_starts_and_stops_async_broker(self) -> None:
        """Async broker is started and stopped during lifespan."""
        broker = _FakeAsyncBroker()

        async with rabbitkit_lifespan(broker=broker):
            assert broker.started is True
            assert broker.stopped is False

        assert broker.stopped is True


# ── sync broker tests ────────────────────────────────────────────────────


class TestSyncBroker:
    async def test_starts_and_stops_sync_broker(self) -> None:
        """Sync broker is started and stopped during lifespan."""
        broker = _FakeSyncBroker()

        async with rabbitkit_lifespan(broker=broker):
            assert broker.started is True
            assert broker.stopped is False

        assert broker.stopped is True


# ── with rabbit_app ──────────────────────────────────────────────────────


class TestWithRabbitApp:
    async def test_starts_and_stops_rabbit_app(self) -> None:
        """RabbitApp lifecycle hooks are called."""
        rabbit_app = _FakeRabbitApp()
        broker = _FakeAsyncBroker()

        async with rabbitkit_lifespan(broker=broker, rabbit_app=rabbit_app):
            assert rabbit_app.started is True
            assert broker.started is True

        assert rabbit_app.stopped is True
        assert broker.stopped is True

    async def test_rabbit_app_only(self) -> None:
        """Lifespan works with rabbit_app only (no broker)."""
        rabbit_app = _FakeRabbitApp()

        async with rabbitkit_lifespan(rabbit_app=rabbit_app):
            assert rabbit_app.started is True

        assert rabbit_app.stopped is True


# ── broker only ──────────────────────────────────────────────────────────


class TestBrokerOnly:
    async def test_broker_only(self) -> None:
        """Lifespan works with broker only (no rabbit_app)."""
        broker = _FakeAsyncBroker()

        async with rabbitkit_lifespan(broker=broker):
            assert broker.started is True

        assert broker.stopped is True


# ── no components ────────────────────────────────────────────────────────


class TestNoComponents:
    async def test_no_broker_no_error(self) -> None:
        """Lifespan works with no broker and no rabbit_app."""
        async with rabbitkit_lifespan():
            pass  # should not raise


# ── stop on exception ───────────────────────────────────────────────────


class TestStopOnException:
    async def test_stop_on_exception(self) -> None:
        """Broker and app are stopped even when exception occurs in body."""
        broker = _FakeAsyncBroker()
        rabbit_app = _FakeRabbitApp()

        try:
            async with rabbitkit_lifespan(broker=broker, rabbit_app=rabbit_app):
                raise RuntimeError("app crashed")
        except RuntimeError:
            pass

        assert broker.stopped is True
        assert rabbit_app.stopped is True


# ── ordering ─────────────────────────────────────────────────────────────


class TestOrdering:
    async def test_start_order_app_then_broker(self) -> None:
        """Start order: rabbit_app first, then broker."""
        order: list[str] = []

        class _OrderedApp:
            async def start_async(self) -> None:
                order.append("app_start")

            async def stop_async(self) -> None:
                order.append("app_stop")

        class _OrderedBroker:
            async def start(self) -> None:
                order.append("broker_start")

            async def stop(self) -> None:
                order.append("broker_stop")

        async with rabbitkit_lifespan(broker=_OrderedBroker(), rabbit_app=_OrderedApp()):
            assert order == ["app_start", "broker_start"]

        assert order == ["app_start", "broker_start", "broker_stop", "app_stop"]


# ── sync-only rabbit_app (no start_async / stop_async) ───────────────────


class _SyncOnlyRabbitApp:
    """Fake RabbitApp that only has sync start/stop — no start_async/stop_async."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True


class TestSyncOnlyRabbitApp:
    async def test_starts_and_stops_sync_only_rabbit_app(self) -> None:
        """rabbit_app with only sync start/stop uses the sync fallback path."""
        rabbit_app = _SyncOnlyRabbitApp()

        async with rabbitkit_lifespan(rabbit_app=rabbit_app):
            assert rabbit_app.started is True
            assert rabbit_app.stopped is False

        assert rabbit_app.stopped is True

    async def test_sync_only_rabbit_app_stop_on_exception(self) -> None:
        """Sync-only rabbit_app.stop() is called even when body raises."""
        rabbit_app = _SyncOnlyRabbitApp()

        try:
            async with rabbitkit_lifespan(rabbit_app=rabbit_app):
                raise RuntimeError("crash")
        except RuntimeError:
            pass

        assert rabbit_app.stopped is True


class _CoroutineRabbitApp:
    """rabbit_app whose start/stop return coroutines (simulates async methods on sync path)."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    def start(self) -> object:
        async def _start() -> None:
            self.started = True
        return _start()

    def stop(self) -> object:
        async def _stop() -> None:
            self.stopped = True
        return _stop()


class TestCoroutineRabbitApp:
    async def test_start_coroutine_is_awaited(self) -> None:
        """Lines 62, 89: start()/stop() returning coroutines are awaited."""
        rabbit_app = _CoroutineRabbitApp()

        async with rabbitkit_lifespan(rabbit_app=rabbit_app):
            assert rabbit_app.started is True
            assert rabbit_app.stopped is False

        assert rabbit_app.stopped is True
