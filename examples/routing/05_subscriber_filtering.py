"""Routing: Subscriber filtering — reject messages before deserialization.

filter_fn is called before ACK, deserialization, and DI resolution.
Messages that do NOT match are nacked with requeue=False (dropped, or
dead-lettered — the default safety policy auto-provisions a DLQ).

rabbitkit enforces ONE handler per queue: a filter selects which messages
a queue's single handler processes; it does not split one queue between
competing handlers. To route different message kinds to different
handlers, give each handler its own queue (bind them to one exchange) —
see 01_basic_routing.py and 04_topic_wildcards.py.

Run:
    python examples/routing/05_subscriber_filtering.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.message import RabbitMessage

broker = AsyncBroker(RabbitConfig())

# ── Filter 1: header-based tenant gate ───────────────────────────────────────
# Only acme-tenant events are processed; everything else dead-letters.

@broker.subscriber(
    queue="filter-events",
    filter_fn=lambda msg: msg.headers.get("x-tenant") == "acme",
)
async def handle_acme_events(body: bytes) -> None:
    print(f"[acme]  {body.decode()}")


# ── Filter 2: header-based priority gate (avoid parsing the body in filters) ─

def is_high_priority(msg: RabbitMessage) -> bool:
    """Only handle messages with x-priority: high header."""
    return msg.headers.get("x-priority") == "high"

@broker.subscriber(queue="filter-tasks", filter_fn=is_high_priority)
async def handle_priority_task(body: bytes) -> None:
    print(f"[high-priority task] {body.decode()}")


# ── Filter 3: routing-key prefix gate ────────────────────────────────────────

@broker.subscriber(
    queue="filter-notifications",
    filter_fn=lambda msg: msg.routing_key.startswith("email."),
)
async def handle_email_notification(body: bytes) -> None:
    print(f"[email notification] {body.decode()}")


async def main() -> None:
    await broker.start()

    # Tenant gate: acme passes, beta is filtered out (dead-lettered)
    for tenant, body in [("acme", b"order created"), ("beta", b"user signed up")]:
        await broker.publish(
            MessageEnvelope(
                routing_key="filter-events",
                body=body,
                headers={"x-tenant": tenant},
            )
        )

    # Priority gate: high passes, normal is filtered out
    for priority, task in [("high", b"send invoice"), ("normal", b"generate report")]:
        await broker.publish(
            MessageEnvelope(
                routing_key="filter-tasks",
                body=task,
                headers={"x-priority": priority},
            )
        )

    await asyncio.sleep(0.5)
    await broker.stop()
    print("done — non-matching messages were dead-lettered, not lost")


if __name__ == "__main__":
    asyncio.run(main())
