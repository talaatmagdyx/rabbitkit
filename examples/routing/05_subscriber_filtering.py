"""Routing: Subscriber filtering — reject messages before deserialization.

filter_fn is called before ACK, deserialization, and DI resolution.
Rejected messages are nacked with requeue=False (dropped / DLQ).
Use it to route at the application layer without adding more queues.

Run:
    python examples/routing/05_subscriber_filtering.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.message import RabbitMessage

broker = AsyncBroker(RabbitConfig())

# ── Filter 1: header-based tenant routing ────────────────────────────────────

@broker.subscriber(
    queue="events",
    filter_fn=lambda msg: msg.headers.get("x-tenant") == "acme",
)
async def handle_acme_events(body: bytes) -> None:
    print(f"[acme]  {body.decode()}")

@broker.subscriber(
    queue="events",
    filter_fn=lambda msg: msg.headers.get("x-tenant") == "beta",
)
async def handle_beta_events(body: bytes) -> None:
    print(f"[beta]  {body.decode()}")


# ── Filter 2: body-content filtering (avoid parsing in filter — use headers) ─

def is_high_priority(msg: RabbitMessage) -> bool:
    """Only handle messages with x-priority: high header."""
    return msg.headers.get("x-priority") == "high"

@broker.subscriber(queue="tasks", filter_fn=is_high_priority)
async def handle_priority_task(body: bytes) -> None:
    print(f"[high-priority task] {body.decode()}")

@broker.subscriber(
    queue="tasks",
    filter_fn=lambda msg: msg.headers.get("x-priority") != "high",
)
async def handle_normal_task(body: bytes) -> None:
    print(f"[normal task] {body.decode()}")


# ── Filter 3: routing key prefix matching ────────────────────────────────────

@broker.subscriber(
    queue="notifications",
    filter_fn=lambda msg: msg.routing_key.startswith("email."),
)
async def handle_email_notification(body: bytes) -> None:
    print(f"[email notification] {body.decode()}")

@broker.subscriber(
    queue="notifications",
    filter_fn=lambda msg: msg.routing_key.startswith("sms."),
)
async def handle_sms_notification(body: bytes) -> None:
    print(f"[sms notification] {body.decode()}")


async def main() -> None:
    await broker.start()

    # Tenant-routed events
    for tenant, body in [("acme", b"order created"), ("beta", b"user signed up")]:
        await broker.publish(
            MessageEnvelope(
                routing_key="events",
                body=body,
                headers={"x-tenant": tenant},
            )
        )

    # Priority-routed tasks
    for priority, task in [("high", b"send invoice"), ("normal", b"generate report")]:
        await broker.publish(
            MessageEnvelope(
                routing_key="tasks",
                body=task,
                headers={"x-priority": priority},
            )
        )

    # Notification type routing
    for rk, body in [
        ("email.confirm", b"Confirm your email: https://..."),
        ("sms.otp",       b"Your OTP is 123456"),
    ]:
        await broker.publish(
            MessageEnvelope(routing_key="notifications", body=body)
            # Note: routing_key is set on the envelope so msg.routing_key works
        )

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
