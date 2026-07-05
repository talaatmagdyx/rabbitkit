"""Serialization: Pydantic model serialization.

Demonstrates encoding/decoding Pydantic models as message bodies.
Combines the JSON serializer with Pydantic's model_validate for
automatic validation of incoming messages.

Run:
    python examples/serialization/02_pydantic.py

Requirements:
    pip install "rabbitkit[async,pydantic]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.serialization.json import JSONSerializer

broker = AsyncBroker(RabbitConfig(), serializer=JSONSerializer())


# ── Models ────────────────────────────────────────────────────────────────────

class UserEvent(BaseModel):
    event_type: Literal["signup", "login", "logout", "delete"]
    user_id: int
    email: str
    occurred_at: datetime = Field(default_factory=datetime.utcnow)


class Product(BaseModel):
    sku: str
    name: str
    price: float = Field(gt=0)
    stock: int = Field(ge=0)
    tags: list[str] = []


class OrderItem(BaseModel):
    product: Product
    quantity: int = Field(gt=0)


class Order(BaseModel):
    order_id: str
    customer_email: str
    items: list[OrderItem]

    @property
    def total(self) -> float:
        return sum(item.product.price * item.quantity for item in self.items)


# ── Handlers ──────────────────────────────────────────────────────────────────

@broker.subscriber(queue="user-events")
async def handle_user_event(event: UserEvent) -> None:
    """Auto-validated: invalid payloads → ValidationError → DLQ."""
    print(f"[user-event] {event.event_type}: user_id={event.user_id}, email={event.email!r}")


@broker.subscriber(queue="orders")
async def handle_order(order: Order) -> None:
    """Nested model validation — all OrderItem.Product fields validated."""
    print(f"[order] {order.order_id}: {len(order.items)} items, total=${order.total:.2f}")
    for item in order.items:
        print(f"  - {item.product.sku} x {item.quantity} @ ${item.product.price}")


# ── Publisher helper ──────────────────────────────────────────────────────────

def publish_model(broker: AsyncBroker, routing_key: str, model: BaseModel) -> "asyncio.coroutine":
    """Serialize a Pydantic model and publish it."""
    return broker.publish(MessageEnvelope(
        routing_key=routing_key,
        body=model.model_dump_json().encode(),
        content_type="application/json",
    ))


async def main() -> None:
    await broker.start()

    # Publish a UserEvent
    await publish_model(broker, "user-events", UserEvent(
        event_type="signup",
        user_id=101,
        email="alice@example.com",
    ))

    # Publish a nested Order
    await publish_model(broker, "orders", Order(
        order_id="ORD-2024-001",
        customer_email="bob@example.com",
        items=[
            OrderItem(
                product=Product(sku="WGT-A", name="Widget A", price=9.99, stock=50),
                quantity=3,
            ),
            OrderItem(
                product=Product(sku="GAD-B", name="Gadget B", price=24.99, stock=12, tags=["electronics"]),
                quantity=1,
            ),
        ],
    ))

    # Invalid message — price=-1 fails Field(gt=0) → ValidationError → DLQ
    await broker.publish(MessageEnvelope(
        routing_key="orders",
        body=json.dumps({
            "order_id": "BAD-001",
            "customer_email": "test@example.com",
            "items": [{"product": {"sku": "X", "name": "X", "price": -1, "stock": 0}, "quantity": 1}],
        }).encode(),
        content_type="application/json",
    ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
