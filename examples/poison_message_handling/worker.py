"""Poison message handling — bad messages go to DLQ, good ones are processed.

A "poison message" raises a permanent error (ValueError). With AckPolicy.AUTO,
the framework classifies it as permanent → nack(requeue=False) → DLQ.
Transient errors (e.g., ConnectionError) are nacked with requeue=True.

Run:
    docker run -d --rm -p 5672:5672 rabbitmq:4-management
    python examples/poison_message_handling/worker.py

Inspect the DLQ after poison messages arrive:
    rabbitkit dlq inspect --queue ingest.queue.dlq
    rabbitkit dlq drain  --queue ingest.queue.dlq --action drop

Requirements:
    pip install "rabbitkit[async]"
"""

import asyncio
import json

from rabbitkit import ConnectionConfig, RabbitConfig, RetryConfig
from rabbitkit.aio import AsyncBroker
from rabbitkit.core.types import AckPolicy
from rabbitkit.middleware.retry import RetryMiddleware

RETRY = RetryConfig(max_retries=2, delays=(5, 30))

broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig(host="localhost"), retry=RETRY))


@broker.subscriber(
    queue="ingest.queue",
    exchange="ingest.exchange",
    routing_key="ingest.event",
    ack_policy=AckPolicy.AUTO,  # permanent → DLQ, transient → requeue
    retry=RETRY,
    middlewares=[RetryMiddleware(RETRY, publish_async_fn=None)],
)
async def handle_event(body: bytes) -> None:
    data = json.loads(body)
    event_type = data.get("type", "")

    if event_type == "bad":
        # Permanent error — immediately dead-lettered (no retries)
        raise ValueError(f"unrecognisable event type: {event_type!r}")

    print(f"[handler] processed event: {data}")


async def main() -> None:
    await broker.start()
    print("Waiting for messages (Ctrl+C to stop)...")
    print("Messages with type='bad' will be moved to ingest.queue.dlq")
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
