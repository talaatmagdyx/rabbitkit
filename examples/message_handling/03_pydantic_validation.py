"""Message handling: Pydantic auto-validation.

Type-hint the handler body parameter with a Pydantic model and the pipeline
automatically calls model_validate() during deserialization.
ValidationError is classified as PERMANENT — message goes to DLQ.

Run:
    python examples/message_handling/03_pydantic_validation.py

Requirements:
    pip install "rabbitkit[async,pydantic]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, EmailStr, Field, field_validator

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.serialization.json import JSONSerializer

broker = AsyncBroker(RabbitConfig(), serializer=JSONSerializer())


# ── Simple model ──────────────────────────────────────────────────────────────

class Order(BaseModel):
    id: int
    item: str
    qty: int = Field(gt=0, description="Must be positive")
    price: float


@broker.subscriber(queue="orders")
async def handle_order(order: Order) -> None:
    """Receives a validated Order model — no manual JSON parsing needed."""
    total = order.qty * order.price
    print(f"[order] #{order.id}: {order.qty}x {order.item!r} = ${total:.2f}")


# ── Nested model ─────────────────────────────────────────────────────────────

class Address(BaseModel):
    street: str
    city: str
    country: str = "US"


class Customer(BaseModel):
    name: str
    email: str
    address: Address
    tier: Literal["free", "pro", "enterprise"] = "free"

    @field_validator("email")
    @classmethod
    def email_must_be_valid(cls, v: str) -> str:
        if "@" not in v:
            raise ValueError("invalid email")
        return v.lower()


@broker.subscriber(queue="customers")
async def handle_customer(customer: Customer) -> None:
    print(f"[customer] {customer.name} ({customer.tier}) → {customer.address.city}")


# ── List / collection body ────────────────────────────────────────────────────

class BulkOrders(BaseModel):
    orders: list[Order]
    source: str


@broker.subscriber(queue="bulk-orders")
async def handle_bulk(bulk: BulkOrders) -> None:
    print(f"[bulk] {len(bulk.orders)} orders from {bulk.source!r}")
    for o in bulk.orders:
        print(f"  - #{o.id} {o.item}")


async def main() -> None:
    await broker.start()

    # Valid order
    await broker.publish(MessageEnvelope(
        routing_key="orders",
        body=json.dumps({"id": 1, "item": "Widget", "qty": 5, "price": 9.99}).encode(),
        content_type="application/json",
    ))

    # Valid customer with nested address
    await broker.publish(MessageEnvelope(
        routing_key="customers",
        body=json.dumps({
            "name": "Alice Smith",
            "email": "ALICE@EXAMPLE.COM",   # will be lowercased by validator
            "address": {"street": "123 Main St", "city": "Boston"},
            "tier": "pro",
        }).encode(),
        content_type="application/json",
    ))

    # Bulk orders
    await broker.publish(MessageEnvelope(
        routing_key="bulk-orders",
        body=json.dumps({
            "source": "import-job-42",
            "orders": [
                {"id": 10, "item": "Widget A", "qty": 2, "price": 4.99},
                {"id": 11, "item": "Widget B", "qty": 1, "price": 14.99},
            ],
        }).encode(),
        content_type="application/json",
    ))

    # Invalid order — qty=0 fails Field(gt=0) → ValidationError → DLQ
    await broker.publish(MessageEnvelope(
        routing_key="orders",
        body=json.dumps({"id": 99, "item": "Bad", "qty": 0, "price": 1.0}).encode(),
        content_type="application/json",
    ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
