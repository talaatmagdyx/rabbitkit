"""High-load: one service, 10 worker queues, high message volume.

The common microservice deployment shape: ONE broker process consuming from
TEN distinct queues (orders, payments, emails, ...), each with its own
handler, all sharing one connection — then pushed hard: thousands of
messages published concurrently and drained, with per-queue counts and
overall throughput reported at the end.

What this demonstrates:
  - Registering N handlers from a loop (one handler per queue — registering
    two handlers on the SAME queue raises DuplicateRouteError by design;
    use one handler + filter_fn for that).
  - prefetch_count as the consume-side concurrency knob (per-queue consumer,
    so a slow queue does not starve the other nine).
  - Concurrent publishing via asyncio.gather — each publish acquires a
    pooled channel, so size PoolConfig.channel_pool_size to your publish
    concurrency (a gather() wave wider than the pool just queues on channel
    acquire, with a warning). For even higher publish rates see
    02_batch_publisher.py.

Run:
    python examples/highload/05_ten_queues_high_volume.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import time
from collections.abc import Callable, Coroutine
from typing import Any

from rabbitkit import ConsumerConfig, MessageEnvelope, PoolConfig, RabbitConfig
from rabbitkit.async_ import AsyncBroker

NUM_QUEUES = 10
MSGS_PER_QUEUE = 300  # 10 x 300 = 3,000 messages total
TOTAL = NUM_QUEUES * MSGS_PER_QUEUE
PUBLISH_CHUNK = 32  # concurrent publishes per gather() wave — match the channel pool

QUEUES = [f"hl-tenq-{i}" for i in range(NUM_QUEUES)]

broker = AsyncBroker(
    RabbitConfig(
        # Per-queue consumer channel: up to 50 unacked messages in flight
        # PER QUEUE — this is the consume-side concurrency knob.
        consumer=ConsumerConfig(prefetch_count=50),
        # Publish-side: each concurrent publish() holds one pooled channel,
        # so the pool must be at least as wide as the gather() wave.
        pool=PoolConfig(channel_pool_size=PUBLISH_CHUNK),
    )
)

received: dict[str, int] = dict.fromkeys(QUEUES, 0)
processed = 0
all_done = asyncio.Event()


def make_handler(queue: str) -> Callable[[bytes], Coroutine[Any, Any, None]]:
    """One handler per queue, built from a loop.

    Each queue needs its OWN handler function — a second @subscriber on the
    same queue raises DuplicateRouteError.
    """

    async def handle(body: bytes) -> None:
        global processed
        received[queue] += 1
        processed += 1
        if processed >= TOTAL:
            all_done.set()

    return handle


for q in QUEUES:
    broker.subscriber(queue=q)(make_handler(q))


async def publish_all() -> float:
    """Publish TOTAL messages round-robin across the 10 queues, in
    concurrent waves of PUBLISH_CHUNK (each publish acquires a pooled
    channel, so gather() is safe)."""
    envelopes = [
        MessageEnvelope(
            routing_key=QUEUES[n % NUM_QUEUES],
            body=f'{{"seq": {n}}}'.encode(),
        )
        for n in range(TOTAL)
    ]

    start = time.monotonic()
    for i in range(0, TOTAL, PUBLISH_CHUNK):
        await asyncio.gather(*(broker.publish(env) for env in envelopes[i : i + PUBLISH_CHUNK]))
    return time.monotonic() - start


async def main() -> None:
    await broker.start()
    print(f"Broker started: 1 connection, {NUM_QUEUES} queues, prefetch=50 each")

    # Consumers are live while we publish, so the honest number is
    # end-to-end: first publish → last message handled.
    start = time.monotonic()
    publish_elapsed = await publish_all()
    await asyncio.wait_for(all_done.wait(), timeout=60.0)
    total_elapsed = time.monotonic() - start

    print(
        f"Published {TOTAL:,} messages in {publish_elapsed:.2f}s "
        f"({TOTAL / publish_elapsed:,.0f} msg/s, confirms on)"
    )
    print(
        f"End-to-end (publish + consume, overlapped): {total_elapsed:.2f}s "
        f"({TOTAL / total_elapsed:,.0f} msg/s)"
    )
    print("\nPer-queue counts (each handler saw ONLY its own queue):")
    for q in QUEUES:
        print(f"  {q}: {received[q]}")

    assert all(count == MSGS_PER_QUEUE for count in received.values())
    await broker.stop()
    print("\nAll queues drained evenly — done.")


if __name__ == "__main__":
    asyncio.run(main())
