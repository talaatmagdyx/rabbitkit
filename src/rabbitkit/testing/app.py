"""TestApp — lifecycle testing wrapper.

Wraps TestBroker with RabbitApp lifecycle hooks.
Triggers startup/shutdown hooks in test context.
"""

from __future__ import annotations

from typing import Any

from rabbitkit.core.app import AppState, RabbitApp
from rabbitkit.testing.broker import TestBroker


class TestApp:
    """Full lifecycle wrapper — triggers startup/shutdown hooks in test context.

    Usage::

        broker = TestBroker()
        app = RabbitApp(title="my-app")
        test_app = TestApp(app, broker)

        @broker.subscriber(queue="orders")
        def handle_order(body: bytes) -> None:
            ...

        test_app.start()
        broker.publish("orders", b'{"id": 1}')
        handle_order.mock.assert_called_once()
        test_app.stop()

    Or as context manager::

        with TestApp(app, broker) as ta:
            broker.publish("orders", b'{"id": 1}')
    """

    __test__ = False  # Prevent pytest from collecting as test class

    def __init__(
        self,
        app: RabbitApp,
        broker: TestBroker,
    ) -> None:
        self._app = app
        self._broker = broker

    @property
    def app(self) -> RabbitApp:
        return self._app

    @property
    def broker(self) -> TestBroker:
        return self._broker

    @property
    def state(self) -> AppState:
        return self._app.state

    def start(self) -> None:
        """Start the test app — runs startup hooks and starts broker."""
        self._app.start()
        self._broker.start()

    def stop(self) -> None:
        """Stop the test app — stops broker and runs shutdown hooks."""
        self._broker.stop()
        self._app.stop()

    async def start_async(self) -> None:
        """Async start — runs startup hooks and starts broker."""
        await self._app.start_async()
        self._broker.start()

    async def stop_async(self) -> None:
        """Async stop — stops broker and runs shutdown hooks."""
        self._broker.stop()
        await self._app.stop_async()

    def reset(self) -> None:
        """Reset broker state (published messages, mocks)."""
        self._broker.reset()

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> TestApp:
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()

    async def __aenter__(self) -> TestApp:
        await self.start_async()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop_async()
