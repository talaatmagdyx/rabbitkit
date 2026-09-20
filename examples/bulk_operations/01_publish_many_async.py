"""Bulk operations: ``AsyncBroker.publish_many`` with per-item outcomes.

Publishes a page of events in one call, bounded by in-flight count, buffered
bytes and an overall deadline, and shows how each input maps to exactly one
outcome — including an UNROUTABLE item (mandatory publish to a queue that
does not exist) and an INVALID item (oversized body) that never reaches the
wire. Nothing is retried automatically: UNKNOWN items are for reconciliation.

Run:
    python examples/bulk_operations/01_publish_many_async.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import (
    AsyncBroker,
    BulkPublishOptions,
    BulkPublishStatus,
    MessageEnvelope,
    PublisherConfig,
    RabbitConfig,
)

QUEUE = "bulk-demo-events"

broker = AsyncBroker(
    RabbitConfig(
        # mandatory=True: an unroutable message comes back UNROUTABLE instead of
        # being confirmed into the void; 64 KiB cap: oversized → INVALID up front.
        publisher=PublisherConfig(mandatory=True, max_message_bytes=64 * 1024),
    )
)

received: list[str] = []


@broker.subscriber(queue=QUEUE)
async def handle(body: bytes) -> None:
    received.append(json.loads(body)["event_id"])


def build_page(n: int) -> list[MessageEnvelope]:
    """Project application rows into envelopes (the 'pluck' step lives HERE,
    in application code — RabbitMQ has no fetch-by-id)."""
    return [
        MessageEnvelope(
            routing_key=QUEUE,
            body=json.dumps({"event_id": f"evt-{i}", "amount": i * 10}).encode(),
            message_id=f"evt-{i}",  # stable, caller-owned id → reconciliation + dedup
            mandatory=True,
        )
        for i in range(n)
    ]


async def main() -> None:
    await broker.start()
    await asyncio.sleep(0.3)  # let the consumer register

    envelopes = build_page(20)
    envelopes.append(MessageEnvelope(routing_key=f"{QUEUE}-does-not-exist", body=b"{}", mandatory=True))
    envelopes.append(MessageEnvelope(routing_key=QUEUE, body=b"x" * (65 * 1024)))

    result = await broker.publish_many(
        envelopes,
        BulkPublishOptions(
            max_in_flight=8,  # at most 8 unsettled publishes
            max_buffer_bytes=1024 * 1024,  # at most 1 MiB of bodies in flight
            admission_timeout=5.0,
            overall_timeout=30.0,
        ),
    )

    print(f"published {len(result)} items → { {k.value: v for k, v in result.counts.items()} }")
    for item in result.items:
        if item.status is not BulkPublishStatus.CONFIRMED:
            print(f"  index={item.index} status={item.status.value:<10} reason={item.reason}")

    # What to do with each non-confirmed class:
    for item in result.unknown:
        print(f"  reconcile message_id={item.message_id} attempt_id={item.attempt_id}")
    for item in result.resubmittable:
        print(f"  safe to resubmit index={item.index} ({item.reason}) after fixing the input")

    await asyncio.sleep(1.0)
    print(f"consumer received {len(received)} of {result.counts.get(BulkPublishStatus.CONFIRMED, 0)} confirmed")
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
