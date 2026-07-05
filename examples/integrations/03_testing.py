"""Integration: Testing with TestBroker and TestApp.

TestBroker runs in-memory — no RabbitMQ connection needed.
Every handler gets a .mock attribute for pytest assertions.

Run:
    python examples/integrations/03_testing.py
    # OR run as pytest tests:
    pytest examples/integrations/03_testing.py -v

Requirements:
    pip install "rabbitkit" pytest pytest-asyncio
"""

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit import MessageEnvelope, RabbitApp, RabbitConfig
from rabbitkit.testing import TestApp, TestBroker

# ─────────────────────────────────────────────────────────────────────────────
# Production code being tested
# ─────────────────────────────────────────────────────────────────────────────

def create_order_broker() -> "Any":
    """Factory function for the broker (avoids global state in tests)."""
    broker = TestBroker()

    @broker.subscriber(queue="orders")
    def handle_order(body: bytes) -> bytes:
        data = json.loads(body)
        if data.get("qty", 0) <= 0:
            raise ValueError(f"Invalid qty: {data['qty']}")
        result = {"order_id": data["id"], "status": "confirmed", "total": data["qty"] * data["price"]}
        return json.dumps(result).encode()

    @broker.subscriber(queue="notifications")
    async def send_notification(body: bytes) -> None:
        data = json.loads(body)
        print(f"  [notify] Sending email to {data['email']!r}")

    return broker, handle_order, send_notification


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests
# ─────────────────────────────────────────────────────────────────────────────

def test_handler_called_once():
    """TestBroker: handler is called with correct body."""
    broker, handle_order, _ = create_order_broker()
    broker.start()

    broker.publish("orders", json.dumps({"id": 1, "qty": 2, "price": 9.99}).encode())

    handle_order.mock.assert_called_once()
    print("PASS: test_handler_called_once")


def test_handler_call_args():
    """TestBroker: inspect handler arguments."""
    broker, handle_order, _ = create_order_broker()
    broker.start()

    payload = {"id": 42, "qty": 5, "price": 4.99}
    broker.publish("orders", json.dumps(payload).encode())

    # handler receives (body: bytes) — check the bytes argument
    call_args = handle_order.mock.call_args
    body_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("body")
    received = json.loads(body_arg)
    assert received == payload
    print(f"PASS: test_handler_call_args — received id={received['id']}")


def test_handler_not_called_for_other_queues():
    """TestBroker: handlers are not called for unrelated queue messages."""
    broker, handle_order, send_notification = create_order_broker()
    broker.start()

    broker.publish("notifications", json.dumps({"email": "alice@example.com"}).encode())

    handle_order.mock.assert_not_called()
    send_notification.mock.assert_called_once()
    print("PASS: test_handler_not_called_for_other_queues")


def test_multiple_messages():
    """TestBroker: handler called for each message."""
    broker, handle_order, _ = create_order_broker()
    broker.start()

    for i in range(3):
        broker.publish("orders", json.dumps({"id": i, "qty": 1, "price": 1.0}).encode())

    assert handle_order.mock.call_count == 3
    print(f"PASS: test_multiple_messages — called {handle_order.mock.call_count} times")


def test_exception_handling():
    """TestBroker: handler exception does not crash the broker."""
    broker, handle_order, _ = create_order_broker()
    broker.start()

    # Invalid qty=0 → ValueError → nacked (AUTO policy)
    broker.publish("orders", json.dumps({"id": 99, "qty": 0, "price": 1.0}).encode())

    # Handler was still called
    handle_order.mock.assert_called_once()
    print("PASS: test_exception_handling — exception handled gracefully")


@pytest.mark.asyncio
async def test_async_handler():
    """TestBroker: async handler tested with await."""
    broker, _, send_notification = create_order_broker()
    broker.start()

    await broker.publish_async(
        "notifications",
        json.dumps({"email": "bob@example.com", "subject": "Order confirmed"}).encode(),
    )

    send_notification.mock.assert_called_once()
    print("PASS: test_async_handler")


# ─────────────────────────────────────────────────────────────────────────────
# TestApp: full lifecycle testing with startup/shutdown hooks
# ─────────────────────────────────────────────────────────────────────────────

def test_with_testapp():
    """TestApp: test startup/shutdown hooks with context manager."""
    broker, handle_order, _ = create_order_broker()
    rabbit_app = RabbitApp(title="test-app")

    hook_calls: list[str] = []

    @rabbit_app.on_startup
    def startup_hook() -> None:
        hook_calls.append("startup")

    @rabbit_app.on_shutdown
    def shutdown_hook() -> None:
        hook_calls.append("shutdown")

    with TestApp(rabbit_app, broker) as ta:
        # Startup hooks ran
        assert "startup" in hook_calls

        broker.publish("orders", json.dumps({"id": 1, "qty": 1, "price": 9.99}).encode())
        handle_order.mock.assert_called_once()

    # Shutdown hooks ran after context exit
    assert "shutdown" in hook_calls
    print(f"PASS: test_with_testapp — hooks called: {hook_calls}")


# ─────────────────────────────────────────────────────────────────────────────
# Run manually (not via pytest)
# ─────────────────────────────────────────────────────────────────────────────

def run_all_tests() -> None:
    print("=== Running all tests ===\n")
    test_handler_called_once()
    test_handler_call_args()
    test_handler_not_called_for_other_queues()
    test_multiple_messages()
    test_exception_handling()
    asyncio.run(test_async_handler())
    test_with_testapp()
    print("\n=== All tests passed ===")


if __name__ == "__main__":
    run_all_tests()
