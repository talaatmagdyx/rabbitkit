"""publisher_confirms — delivery confirmation with PublishOutcome.

Publisher confirms let you know that RabbitMQ has durably stored the message
before your publish call returns. This prevents silent message loss when the
broker is busy, running out of memory, or restarting.

Demonstrates:
- Enabling publisher confirms via PublisherConfig
- Checking PublishOutcome.status
- Handling TIMEOUT / NACKED outcomes
- Batch publish with per-message confirm

Run:
    pip install rabbitkit[async]
    docker run -d -p 5672:5672 rabbitmq:3.13-management-alpine
    python worker.py
"""

from __future__ import annotations

import asyncio

from rabbitkit import AsyncBroker, RabbitConfig
from rabbitkit.core.config import ConnectionConfig, PublisherConfig
from rabbitkit.core.types import PublishStatus


async def main() -> None:
    config = RabbitConfig(
        connection=ConnectionConfig(host="localhost"),
        publisher=PublisherConfig(
            confirm_delivery=True,
            confirm_timeout=5.0,  # seconds to wait for broker confirm
        ),
    )
    broker = AsyncBroker(config)
    await broker.start()

    # --- Single confirmed publish ---
    outcome = await broker.publish(
        routing_key="orders",
        body={"id": 1, "item": "confirmed-widget"},
    )
    match outcome.status:
        case PublishStatus.CONFIRMED:
            print("[1] confirmed: broker durably stored the message")
        case PublishStatus.TIMEOUT:
            print("[1] TIMEOUT: broker did not confirm within 5s — message may be lost")
        case PublishStatus.NACKED:
            print("[1] NACKED: broker explicitly rejected the message")
        case _:
            print(f"[1] {outcome.status.value}")

    # --- Batch publish with confirm ---
    print("\nPublishing 10 messages with confirms...")
    results = await asyncio.gather(*(
        broker.publish(routing_key="orders", body={"id": i})
        for i in range(2, 12)
    ))

    confirmed = sum(1 for r in results if r.status == PublishStatus.CONFIRMED)
    failed = len(results) - confirmed
    print(f"batch: {confirmed} confirmed, {failed} failed")

    await broker.stop()
    print("\ndone.")


if __name__ == "__main__":
    asyncio.run(main())
