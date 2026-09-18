"""Bulk operations: ``SyncBroker.publish_many`` and streaming ``iter_publish``.

The sync transport confirms one publish at a time (pika cannot pipeline
confirms), so ``publish_many`` is sequential there — but it still gives you
the same per-item outcome contract as the async broker, and ``iter_publish``
lets you stream an unbounded generator (e.g. a paged DB cursor) without
holding every result in memory.

Run:
    python examples/bulk_operations/02_publish_many_sync.py

Requirements:
    pip install "rabbitkit[sync]"
    RabbitMQ running on localhost:5672
"""

import json
from collections.abc import Iterator

from rabbitkit import BulkPublishOptions, BulkPublishStatus, MessageEnvelope, PublisherConfig, RabbitConfig
from rabbitkit.sync import SyncBroker

QUEUE = "bulk-demo-sync"

broker = SyncBroker(RabbitConfig(publisher=PublisherConfig(mandatory=True)))
received: list[bytes] = []


@broker.subscriber(queue=QUEUE)
def handle(body: bytes) -> None:
    received.append(body)


def rows_from_database(pages: int, page_size: int) -> Iterator[MessageEnvelope]:
    """Simulate a paged cursor: yields envelopes page by page, never a full list."""
    for page in range(pages):
        for i in range(page_size):
            n = page * page_size + i
            yield MessageEnvelope(
                routing_key=QUEUE,
                body=json.dumps({"row": n}).encode(),
                message_id=f"row-{n}",
                mandatory=True,
            )


def main() -> None:
    broker.start()

    # ── bounded collection → input-ordered result ─────────────────────────
    page = list(rows_from_database(pages=1, page_size=10))
    result = broker.publish_many(page, BulkPublishOptions(overall_timeout=30.0))
    print(f"publish_many: {len(result)} items, all confirmed = {result.all_confirmed}")
    result.raise_for_status()  # raises BulkPublishError unless EVERY item is CONFIRMED

    # ── unbounded stream → results yielded as each publish settles ────────
    confirmed = unknown = 0
    for item in broker.iter_publish(rows_from_database(pages=5, page_size=10)):
        if item.status is BulkPublishStatus.CONFIRMED:
            confirmed += 1
        elif item.status is BulkPublishStatus.UNKNOWN:
            unknown += 1
            print(f"  reconcile {item.message_id} ({item.reason})")
    print(f"iter_publish: confirmed={confirmed} unknown={unknown}")

    # ── drive the consumer from the owner thread until everything arrived ──
    expected = 10 + 50
    for _ in range(200):
        if len(received) >= expected:
            break
        broker.pump_idle(0.05)
    print(f"consumer received {len(received)} / {expected}")

    broker.stop()


if __name__ == "__main__":
    main()
