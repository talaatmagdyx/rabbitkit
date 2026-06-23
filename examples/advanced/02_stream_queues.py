"""Advanced: RabbitMQ stream queues.

Stream queues are append-only logs with offset-based consumption.
Messages are retained based on size/time policies (not just consumer ack).
Multiple consumers can read from the same offset independently.

Requirements: RabbitMQ >= 3.9 with stream plugin enabled.
    rabbitmq-plugins enable rabbitmq_stream

Run:
    python examples/advanced/02_stream_queues.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ >= 3.9 with streams enabled
"""

import asyncio
import json

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import QueueType

broker = AsyncBroker(RabbitConfig())


# ── Declare a stream queue ────────────────────────────────────────────────────
# Stream queues MUST be:
#   - durable=True
#   - exclusive=False (implied)
# Stream queues CANNOT have:
#   - message_ttl, max_priority, lazy

events_stream = RabbitQueue(
    name="events-stream",
    queue_type=QueueType.STREAM,
    durable=True,
    # Optional stream-specific x-arguments:
    arguments={
        "x-max-length-bytes": 1_000_000_000,  # 1GB max retention
        "x-stream-max-segment-size-bytes": 100_000_000,  # 100MB segments
    },
)


# ── Consumer: reads from offset "first" (beginning of stream) ─────────────────
@broker.subscriber(queue=events_stream)
async def handle_stream_event(body: bytes) -> None:
    """Processes each event in the stream."""
    data = json.loads(body)
    print(f"[stream] offset={data.get('seq')} type={data.get('type')!r}")


# ── Multiple independent consumers (different queues, same stream) ────────────
# In a real setup, you'd bind multiple queues to the stream with different offsets.
# rabbitkit registers each subscriber queue independently.


async def main() -> None:
    await broker.start()
    print("Stream queue created. Publishing 5 events...\n")

    for i in range(5):
        await broker.publish(MessageEnvelope(
            routing_key="events-stream",
            body=json.dumps({
                "seq": i,
                "type": "user.action",
                "user_id": i * 10,
                "action": ["click", "view", "purchase", "logout", "login"][i],
            }).encode(),
        ))
        await asyncio.sleep(0.1)

    print("\nWaiting for stream consumer...")
    await asyncio.sleep(1)
    await broker.stop()

    # ── Stream retention policy ──────────────────────────────────────────────
    # Configure via x-arguments:
    #
    # RabbitQueue(
    #     name="audit-stream",
    #     queue_type=QueueType.STREAM,
    #     durable=True,
    #     arguments={
    #         "x-max-age": "7D",                   # keep 7 days
    #         "x-max-length-bytes": 10_000_000_000, # or 10GB, whichever comes first
    #     },
    # )

    print("\nStream queue notes:")
    print("  - Messages are retained by policy, not just ACK")
    print("  - Multiple consumers can replay from any offset")
    print("  - Use x-stream-offset header for offset-based consumption")


if __name__ == "__main__":
    asyncio.run(main())
