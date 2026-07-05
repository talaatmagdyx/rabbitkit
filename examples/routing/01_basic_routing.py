"""Routing: queue, exchange, and routing key basics.

Shows how to bind a queue to a topic exchange and route messages
using dot-separated routing keys.

Run:
    python examples/routing/01_basic_routing.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import ExchangeType

broker = AsyncBroker(RabbitConfig())

# ── Declare a topic exchange and queue explicitly ─────────────────────────────
events_exchange = RabbitExchange(
    name="routing.events",
    type=ExchangeType.TOPIC,
    durable=True,
)

orders_queue = RabbitQueue(
    name="orders-service",
    durable=True,
    routing_key="order.*",   # binds to any "order.<something>" key
)


# ── Handlers ──────────────────────────────────────────────────────────────────

@broker.subscriber(
    queue=orders_queue,
    exchange=events_exchange,
    routing_key="order.*",
)
async def handle_any_order_event(body: bytes) -> None:
    print(f"[any order event] {body.decode()}")


@broker.subscriber(
    queue="payments-service",
    exchange=events_exchange,
    routing_key="payment.#",   # matches payment.created, payment.failed.retry, etc.
)
async def handle_payment_event(body: bytes) -> None:
    print(f"[payment] {body.decode()}")


async def main() -> None:
    await broker.start()

    # Publish messages on different routing keys
    for routing_key, body in [
        ("order.created",           b'{"id": 1, "status": "created"}'),
        ("order.shipped",           b'{"id": 1, "status": "shipped"}'),
        ("payment.created",         b'{"amount": 99.99}'),
        ("payment.failed.retry-1",  b'{"reason": "insufficient_funds"}'),
    ]:
        await broker.publish(
            MessageEnvelope(
                exchange="routing.events",
                routing_key=routing_key,
                body=body,
            )
        )
        print(f"[published] {routing_key}")

    await asyncio.sleep(0.5)   # let handlers process
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
