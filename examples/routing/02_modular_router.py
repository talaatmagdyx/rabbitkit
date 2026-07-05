"""Routing: RabbitRouter — modular routing with prefix and shared exchange.

RabbitRouter lets you group related handlers in a separate module,
apply a routing key prefix, and include them into a broker.
Great for large projects split across multiple files.

Run:
    python examples/routing/02_modular_router.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig, RabbitRouter
from rabbitkit.async_ import AsyncBroker

# ── In a real app, these routers would live in separate modules ───────────────

# orders_router.py
orders_router = RabbitRouter(
    prefix="orders",             # all routing keys get "orders." prepended
    exchange="app-events",       # shared exchange for this router
)

@orders_router.subscriber(queue="order-created-q", routing_key="created")
async def handle_order_created(body: bytes) -> None:
    # routing key = "orders.created"
    print(f"[order created] {body.decode()}")

@orders_router.subscriber(queue="order-cancelled-q", routing_key="cancelled")
async def handle_order_cancelled(body: bytes) -> None:
    # routing key = "orders.cancelled"
    print(f"[order cancelled] {body.decode()}")


# payments_router.py
payments_router = RabbitRouter(
    prefix="payments",
    exchange="app-events",
)

@payments_router.subscriber(queue="payment-q", routing_key="processed")
async def handle_payment(body: bytes) -> None:
    # routing key = "payments.processed"
    print(f"[payment processed] {body.decode()}")


# ── main.py: assemble the broker ──────────────────────────────────────────────
broker = AsyncBroker(RabbitConfig())
broker.include_router(orders_router)
broker.include_router(payments_router)


async def main() -> None:
    await broker.start()
    print(f"Registered {len(broker.routes)} routes:")
    for route in broker.routes:
        print(f"  {route.queue.name!r:<30} exchange={route.exchange.name if route.exchange else '-'}")

    # Publish events
    for routing_key, body in [
        ("orders.created",      b'{"order_id": 42}'),
        ("orders.cancelled",    b'{"order_id": 17, "reason": "user request"}'),
        ("payments.processed",  b'{"payment_id": "PAY-99", "amount": 250.00}'),
    ]:
        await broker.publish(
            MessageEnvelope(exchange="app-events", routing_key=routing_key, body=body)
        )

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
