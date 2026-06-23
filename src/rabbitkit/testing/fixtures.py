"""Pytest fixtures for rabbitkit testing.

Provides standardized fixtures for TestBroker and TestApp.
Import these fixtures in your conftest.py.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest

from rabbitkit.core.app import RabbitApp
from rabbitkit.testing.app import TestApp
from rabbitkit.testing.broker import TestBroker


@pytest.fixture  # pragma: no cover
def test_broker() -> Generator[TestBroker, None, None]:
    """Provide a fresh TestBroker instance.

    Usage:
        def test_my_handler(test_broker):
            @test_broker.subscriber(queue="orders")
            def handle(body: bytes) -> None:
                ...

            test_broker.start()
            test_broker.publish("orders", b'hello')
            handle.mock.assert_called_once()
    """
    broker = TestBroker()
    yield broker
    broker.stop()


@pytest.fixture  # pragma: no cover
def test_app() -> Generator[TestApp, None, None]:
    """Provide a fresh TestApp instance with RabbitApp + TestBroker.

    Usage:
        def test_lifecycle(test_app):
            @test_app.broker.subscriber(queue="orders")
            def handle(body: bytes) -> None:
                ...

            test_app.start()
            test_app.broker.publish("orders", b'hello')
            test_app.stop()
    """
    app = RabbitApp(title="test-app")
    broker = TestBroker()
    ta = TestApp(app, broker)
    yield ta
    if ta.state.value not in ("stopped", "idle"):
        ta.stop()
