"""Testing utilities — TestBroker, TestApp, fixtures.

Usage:
    from rabbitkit.testing import TestBroker, TestApp

    broker = TestBroker()

    @broker.subscriber(queue="orders")
    def handle_order(body: bytes) -> None:
        ...

    broker.start()
    broker.publish("orders", b'{"id": 1}')
    handle_order.mock.assert_called_once()
"""

from rabbitkit.testing.app import TestApp
from rabbitkit.testing.broker import SettlementRecord, TestAsyncBroker, TestBroker

__all__ = ["SettlementRecord", "TestApp", "TestAsyncBroker", "TestBroker"]
