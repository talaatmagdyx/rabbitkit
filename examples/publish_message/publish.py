"""publish_message — send messages to RabbitMQ using AsyncBroker.

Demonstrates:
- Simple kwargs publish (no MessageEnvelope needed)
- JSON-encoded dict body
- Raw bytes body
- Custom headers
- Publishing to a named exchange

Run:
    docker run -d -p 5672:5672 rabbitmq:3.13-management-alpine
    python publish.py
"""

from __future__ import annotations

import asyncio

from rabbitkit import AsyncBroker, RabbitConfig
from rabbitkit.core.config import ConnectionConfig


async def main() -> None:
    config = RabbitConfig(
        connection=ConnectionConfig(host="localhost", port=5672),
    )
    broker = AsyncBroker(config)
    await broker.start()

    # --- Simplest form: dict body auto-serialized to JSON ---
    outcome = await broker.publish(
        routing_key="orders",
        body={"id": 1, "item": "widget", "qty": 3},
    )
    print(f"[1] published dict  → {outcome.status.value}")

    # --- Raw bytes body ---
    outcome = await broker.publish(
        routing_key="orders",
        body=b'{"id": 2, "item": "gadget"}',
    )
    print(f"[2] published bytes → {outcome.status.value}")

    # --- With headers ---
    outcome = await broker.publish(
        routing_key="orders",
        body={"id": 3, "item": "doohickey"},
        headers={"x-tenant": "acme", "x-priority": "high"},
    )
    print(f"[3] published with headers → {outcome.status.value}")

    # --- To a named exchange with a routing key ---
    outcome = await broker.publish(
        exchange="events",
        routing_key="orders.created",
        body={"id": 4, "event": "order.created"},
    )
    print(f"[4] published to exchange → {outcome.status.value}")

    await broker.stop()
    print("done.")


if __name__ == "__main__":
    asyncio.run(main())
