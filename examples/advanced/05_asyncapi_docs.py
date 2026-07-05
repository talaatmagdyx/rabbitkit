"""Advanced: AsyncAPI 2.6.0 documentation generation.

Generate an event contract spec from your broker routes automatically.
Useful for documentation, code generation, and API contracts.

Run:
    python examples/advanced/05_asyncapi_docs.py

Requirements:
    pip install "rabbitkit[async,pydantic]"
    (No RabbitMQ needed — this is a code-only demo)
"""

import json
from typing import Literal

from pydantic import BaseModel

from rabbitkit import RabbitConfig, RabbitRouter
from rabbitkit.async_ import AsyncBroker
from rabbitkit.asyncapi.generator import (
    AsyncAPIGeneratorConfig,
    generate_asyncapi_doc,
    generate_asyncapi_json,
)
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import ExchangeType

# ── Define Pydantic models (will become JSON Schema in AsyncAPI) ──────────────

class OrderCreated(BaseModel):
    order_id: str
    customer_id: int
    total_amount: float
    items: list[dict]  # type: ignore[type-arg]


class PaymentProcessed(BaseModel):
    payment_id: str
    order_id: str
    status: Literal["approved", "declined", "pending"]
    amount: float


class UserEvent(BaseModel):
    event_type: Literal["signup", "login", "logout"]
    user_id: int
    email: str


# ── Register handlers ─────────────────────────────────────────────────────────

broker = AsyncBroker(RabbitConfig())
events_exchange = RabbitExchange(name="events", type=ExchangeType.TOPIC)

@broker.subscriber(
    queue=RabbitQueue(name="order-processor", durable=True, routing_key="order.*"),
    exchange=events_exchange,
    routing_key="order.*",
    tags={"orders", "critical"},
    description="Processes all order-related events from the events exchange.",
)
async def handle_order(order: OrderCreated) -> PaymentProcessed:
    """Route order to payment service. Returns payment confirmation."""
    return PaymentProcessed(
        payment_id="PAY-001",
        order_id=order.order_id,
        status="approved",
        amount=order.total_amount,
    )


@broker.subscriber(
    queue="payment-events",
    exchange=events_exchange,
    routing_key="payment.*",
    tags={"payments"},
    description="Handles payment status updates.",
)
async def handle_payment(payment: PaymentProcessed) -> None:
    pass


@broker.subscriber(
    queue="user-events",
    description="Processes user lifecycle events.",
    tags={"users", "auth"},
)
async def handle_user(event: UserEvent) -> None:
    pass


# ── Generate AsyncAPI document ────────────────────────────────────────────────

def main() -> None:
    config = AsyncAPIGeneratorConfig(
        title="Order Platform Event API",
        version="3.0.0",
        description="RabbitMQ messaging API for the order platform microservices.",
        server_url="rabbitmq.prod.internal:5672",
        server_description="Production RabbitMQ cluster",
    )

    doc = generate_asyncapi_doc(broker.routes, config)

    print("=== AsyncAPI Document ===\n")
    print(json.dumps(doc, indent=2))

    print("\n=== Summary ===")
    print(f"asyncapi version: {doc['asyncapi']}")
    print(f"title:   {doc['info']['title']}")
    print(f"version: {doc['info']['version']}")
    print(f"channels: {len(doc['channels'])}")
    for channel_name, channel in doc["channels"].items():
        op = channel.get("subscribe", {})
        print(f"  {channel_name!r}: operationId={op.get('operationId')!r}")
        bindings = channel.get("bindings", {}).get("amqp", {})
        if "exchange" in bindings:
            print(f"    exchange: {bindings['exchange']['name']!r} ({bindings['exchange']['type']})")

    # Save to file
    json_str = generate_asyncapi_json(broker.routes, config, indent=2)
    output_path = "/tmp/asyncapi.json"
    with open(output_path, "w") as f:
        f.write(json_str)
    print(f"\nSpec saved to: {output_path}")
    print("Open in AsyncAPI Studio: https://studio.asyncapi.com/")


if __name__ == "__main__":
    main()
