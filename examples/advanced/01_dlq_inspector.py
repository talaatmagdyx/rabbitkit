"""Advanced: DLQInspector — peek, replay, and purge dead-letter queues.

When retry is exhausted, messages land in the DLQ (queue.dlq).
DLQInspector lets you inspect and recover them without writing
custom consumer code.

Run:
    python examples/advanced/01_dlq_inspector.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672 with some messages in a DLQ
"""

import asyncio
import json

from rabbitkit import RabbitConfig, MessageEnvelope, RetryConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.dlq import DLQInspector

broker = AsyncBroker(RabbitConfig(
    retry=RetryConfig(max_retries=1, delays=(1,))  # fast retry for demo
))


@broker.subscriber(queue="orders")
async def handle_order(body: bytes) -> None:
    """Always fails — messages will exhaust retries → orders.dlq"""
    data = json.loads(body)
    raise ValueError(f"Cannot process order {data['id']} — simulated failure")


async def main() -> None:
    await broker.start()
    print("Publishing orders that will fail and land in orders.dlq...\n")

    # Publish some orders that will all fail
    for i in range(3):
        await broker.publish(MessageEnvelope(
            routing_key="orders",
            body=json.dumps({"id": i, "item": f"product-{i}", "qty": i + 1}).encode(),
        ))

    # Wait for retries to exhaust (1 retry × 1s delay = ~2s)
    print("Waiting for retry exhaustion (2s)...")
    await asyncio.sleep(4)
    await broker.stop()

    # ── Now inspect the DLQ ───────────────────────────────────────────────────
    print("\n=== DLQ Inspection ===")

    # Re-connect for inspection
    from rabbitkit.sync import SyncBroker
    inspector_broker = SyncBroker(RabbitConfig())
    inspector_broker.start()

    inspector = DLQInspector(inspector_broker._transport)

    # Peek — read messages without consuming them (they stay in DLQ)
    print("\n1. Peek at DLQ messages (non-destructive):")
    messages = inspector.peek("orders.dlq", limit=10)
    print(f"   Found {len(messages)} messages in orders.dlq")
    for msg in messages:
        data = json.loads(msg.body)
        retry_count = msg.headers.get("x-death", [{}])[0].get("count", "?") if msg.headers.get("x-death") else "?"
        print(f"   - order #{data['id']}: {data['item']} (retried: {retry_count}x)")

    # Filter DLQ messages
    print("\n2. Filtered peek (only order id=1):")
    filtered = [m for m in messages if json.loads(m.body)["id"] == 1]
    print(f"   Found {len(filtered)} matching message(s)")

    # Replay — re-publish matching messages back to source queue
    print("\n3. Replay specific message(s) back to orders queue:")

    def should_replay(msg: "object") -> bool:
        """Only replay order id=0."""
        try:
            return json.loads(msg.body)["id"] == 0  # type: ignore[union-attr]
        except Exception:
            return False

    # NOTE: This will fail again since our handler always raises.
    # In a real scenario you'd fix the bug first, then replay.
    # count = inspector.replay(
    #     "orders.dlq",
    #     predicate=should_replay,
    #     target_queue="orders",
    # )
    # print(f"   Replayed {count} message(s)")

    # Async inspection
    print("\n4. Async peek:")
    async_broker = AsyncBroker(RabbitConfig())
    await async_broker.start()
    async_inspector = DLQInspector(async_broker._transport)
    async_msgs = await async_inspector.peek_async("orders.dlq", limit=5)
    print(f"   Async peek found {len(async_msgs)} messages")
    await async_broker.stop()

    # Purge — remove all messages from DLQ permanently
    print("\n5. Purging DLQ...")
    count = inspector.purge("orders.dlq")
    print(f"   Purged {count} message(s)")

    inspector_broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
