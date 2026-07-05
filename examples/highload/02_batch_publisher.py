"""High-load: BatchPublisher — buffered batch publishing.

Buffers messages and flushes them in batches for higher throughput.
Auto-flushes when batch_size is reached or flush_interval_ms elapses.

Run:
    python examples/highload/02_batch_publisher.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json
import time

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.highload.batch import BatchPublishConfig, BatchPublisher

broker = AsyncBroker(RabbitConfig())


@broker.subscriber(queue="batch-ingest")
async def handle_ingest(body: bytes) -> None:
    data = json.loads(body)
    print(f"[consumer] received event_id={data['id']}")


async def main() -> None:
    await broker.start()

    # ── Async BatchPublisher ──────────────────────────────────────────────────
    bp = BatchPublisher(
        publish_fn=broker.publish,
        config=BatchPublishConfig(
            batch_size=10,        # auto-flush every 10 messages
            flush_interval_ms=100, # or every 100ms
            max_in_flight=500,
        ),
    )

    print("Publishing 25 messages in batches of 10...")
    start = time.monotonic()

    for i in range(25):
        await bp.add_async(MessageEnvelope(
            routing_key="batch-ingest",
            body=json.dumps({"id": i, "ts": time.time()}).encode(),
        ))
        # No await per message — add_async buffers, auto-flushes at batch_size=10

    # Flush remaining messages (25 % 10 = 5 remaining)
    await bp.flush_async()
    elapsed = time.monotonic() - start
    print(f"Published 25 messages in {elapsed*1000:.1f}ms\n")

    await asyncio.sleep(0.3)  # let consumer catch up

    # ── Timed auto-flush ─────────────────────────────────────────────────────
    print("Testing timed flush (flush_interval_ms=50)...")
    bp2 = BatchPublisher(
        publish_fn=broker.publish,
        config=BatchPublishConfig(
            batch_size=1000,      # large batch_size — won't trigger
            flush_interval_ms=50, # but 50ms timer will flush
        ),
    )

    for i in range(3):
        await bp2.add_async(MessageEnvelope(
            routing_key="batch-ingest",
            body=json.dumps({"id": 1000 + i}).encode(),
        ))

    print("Waiting for timed auto-flush...")
    await asyncio.sleep(0.1)   # 50ms timer should fire

    await bp2.close_async()    # flush remaining + cleanup

    await asyncio.sleep(0.3)

    # ── Sync BatchPublisher ───────────────────────────────────────────────────
    print("\nSync BatchPublisher example:")
    from rabbitkit.sync import SyncBroker

    sync_broker = SyncBroker(RabbitConfig())
    sync_broker.start()

    sync_bp = BatchPublisher(
        publish_fn=sync_broker.publish,
        config=BatchPublishConfig(batch_size=5, flush_interval_ms=200),
    )

    for i in range(12):
        sync_bp.add(MessageEnvelope(
            routing_key="batch-ingest",
            body=json.dumps({"id": 2000 + i}).encode(),
        ))

    sync_bp.flush()
    sync_bp.close()
    sync_broker.stop()

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
