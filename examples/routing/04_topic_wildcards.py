"""Routing: Topic wildcards — * (single word) and # (zero or more words).

Shows the difference between * and # and common multi-tenant routing patterns.

Run:
    python examples/routing/04_topic_wildcards.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.topology import RabbitExchange
from rabbitkit.core.types import ExchangeType

broker = AsyncBroker(RabbitConfig())
exchange = RabbitExchange(name="audit", type=ExchangeType.TOPIC, durable=True)

# ── Pattern: <tenant>.<service>.<action> ─────────────────────────────────────

@broker.subscriber(queue="all-events-q", exchange=exchange, routing_key="#")
async def handle_all(body: bytes) -> None:
    """Catch-all — receives every message on this exchange."""
    print(f"[#]          {body.decode()}")

@broker.subscriber(queue="acme-events-q", exchange=exchange, routing_key="acme.#")
async def handle_acme_all(body: bytes) -> None:
    """All events for tenant 'acme'."""
    print(f"[acme.#]     {body.decode()}")

@broker.subscriber(queue="auth-events-q", exchange=exchange, routing_key="*.auth.*")
async def handle_auth_for_any_tenant(body: bytes) -> None:
    """Auth events for any tenant (exactly <tenant>.auth.<action>)."""
    print(f"[*.auth.*]   {body.decode()}")

@broker.subscriber(queue="creates-q", exchange=exchange, routing_key="#.created")
async def handle_all_creates(body: bytes) -> None:
    """Any 'created' action at any depth."""
    print(f"[#.created]  {body.decode()}")


async def main() -> None:
    await broker.start()

    messages = [
        "acme.auth.login",
        "acme.auth.logout",
        "acme.orders.created",
        "beta.auth.login",
        "beta.payments.created",
        "gamma.audit.report.created",  # deep nesting — matched by # patterns
    ]

    print("Publishing messages and showing which handlers fire:\n")
    for rk in messages:
        print(f"  routing_key={rk!r}")
        await broker.publish(
            MessageEnvelope(exchange="audit", routing_key=rk, body=rk.encode())
        )
        await asyncio.sleep(0.1)
        print()

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
