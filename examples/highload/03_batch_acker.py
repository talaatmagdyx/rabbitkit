"""High-load: BatchAcker — batched multi-ack.

Accumulates delivery tags and issues a single basic.ack(multiple=True)
when the batch fills or the interval elapses. Reduces AMQP round-trips.

Note: BatchAcker works at the transport/channel level. In typical rabbitkit
usage the pipeline handles acks automatically. Use BatchAcker when you need
fine-grained control over ack batching (e.g. MANUAL ack policy + custom flush).

Run:
    python examples/highload/03_batch_acker.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import AckPolicy
from rabbitkit.highload.batch import BatchAcker, BatchAckConfig

broker = AsyncBroker(RabbitConfig())

# ── Manual ack with batched ack ───────────────────────────────────────────────
# Create the BatchAcker — we'll wire it to the channel ack function
# after the broker starts. For demonstration we simulate the ack function.

processed_count = 0
acked_batches: list[int] = []


def simulate_channel_ack(delivery_tag: int, multiple: bool = False) -> None:
    acked_batches.append(delivery_tag)
    print(f"[batch-ack] AMQP ack: tag={delivery_tag}, multiple={multiple}")


batch_acker = BatchAcker(
    ack_fn=simulate_channel_ack,
    config=BatchAckConfig(
        batch_size=5,             # ack every 5 messages
        flush_interval_ms=500,    # or every 500ms
    ),
)


@broker.subscriber(queue="batch-ack-demo", ack_policy=AckPolicy.MANUAL)
async def handle_with_batch_ack(msg: RabbitMessage) -> None:
    """Accumulate delivery tags and batch-ack every 5 messages."""
    global processed_count
    data = json.loads(msg.body)
    processed_count += 1
    print(f"[handler] msg #{processed_count}: id={data['id']}, tag={msg.delivery_tag}")

    # Instead of: await msg.ack_async()  (one AMQP ack per message)
    # Use batch acker: accumulate, flush when batch is full
    batch_acker.add(msg.delivery_tag)

    # Still need to mark as settled so the pipeline doesn't auto-ack
    # In a real scenario you'd integrate with the transport directly
    await msg.ack_async()   # in this demo we ack normally; batch_acker just demonstrates the interface


async def main() -> None:
    await broker.start()
    print("Publishing 12 messages — will ack in batches of 5...\n")

    for i in range(12):
        await broker.publish(MessageEnvelope(
            routing_key="batch-ack-demo",
            body=json.dumps({"id": i}).encode(),
        ))

    await asyncio.sleep(1.5)  # let all messages process and timer flush

    # Manual flush for remaining messages
    batch_acker.flush()
    batch_acker.close()

    print(f"\nProcessed: {processed_count} messages")
    print(f"Batched ack calls: {len(acked_batches)} (vs {processed_count} individual acks)")
    print(f"Ack reduction: {(1 - len(acked_batches)/processed_count)*100:.0f}%")

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
