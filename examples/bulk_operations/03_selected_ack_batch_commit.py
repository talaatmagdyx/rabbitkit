"""Bulk operations: selected ack after a batch database commit.

MANUAL handlers park each delivery; every N deliveries the batch is
"committed" to a (simulated) database that reports the EXACT subset of rows
it accepted. Only those deliveries are acked — one ``multiple=False`` frame
each via ``ack_many`` — and the rejected rows are nacked without requeue so
they dead-letter. A still-running sibling on the same channel is never
touched, because ``ack_many`` never issues a cumulative ack.

Run:
    python examples/bulk_operations/03_selected_ack_batch_commit.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import AckPolicy, AsyncBroker, ConsumerConfig, MessageEnvelope, RabbitConfig, RabbitMessage

QUEUE = "bulk-demo-batch-commit"
BATCH = 5

broker = AsyncBroker(RabbitConfig(consumer=ConsumerConfig(prefetch_count=2 * BATCH)))
pending: list[RabbitMessage] = []
committed_rows: list[int] = []
commit_signal = asyncio.Event()


class FakeDatabase:
    """Accepts every row except odd multiples of 5 — returns the exact successful subset."""

    async def commit_rows(self, rows: list[dict]) -> tuple[list[int], list[int]]:
        await asyncio.sleep(0.05)
        ok = [i for i, r in enumerate(rows) if not (r["n"] % 5 == 0 and r["n"] % 2 == 1)]
        failed = [i for i in range(len(rows)) if i not in ok]
        return ok, failed


db = FakeDatabase()


@broker.subscriber(queue=QUEUE, ack_policy=AckPolicy.MANUAL)
async def collect(body: bytes, msg: RabbitMessage) -> None:
    pending.append(msg)  # defer settlement to the batch commit
    if len(pending) >= BATCH:
        commit_signal.set()


async def commit_batch(deliveries: list[RabbitMessage]) -> None:
    rows = [json.loads(m.body) for m in deliveries]
    ok, failed = await db.commit_rows(rows)

    report = await broker.ack_many([deliveries[i] for i in ok])
    report.raise_for_status()  # every selected delivery DISPATCHED (or already settled)
    committed_rows.extend(rows[i]["n"] for i in ok)

    if failed:
        # requeue=False → dead-letters into the auto-provisioned <queue>.dlq
        nack_report = await broker.nack_many([deliveries[i] for i in failed], requeue=False)
        print(f"  nacked {len(nack_report.dispatched)} rows to DLQ: {[rows[i]['n'] for i in failed]}")

    print(
        f"  committed batch of {len(deliveries)}: acked={len(report.dispatched)} "
        f"statuses={ {s.value: n for s, n in report.counts.items()} }"
    )


async def main() -> None:
    await broker.start()
    await asyncio.sleep(0.3)

    total = 4 * BATCH
    result = await broker.publish_many(
        [
            MessageEnvelope(routing_key=QUEUE, body=json.dumps({"n": n}).encode(), message_id=f"n-{n}")
            for n in range(total)
        ]
    )
    result.raise_for_status()
    print(f"published {len(result)} rows; committing in batches of {BATCH}")

    while len(committed_rows) + 2 < total:  # 2 rows (5 and 15) are rejected by the fake DB
        await asyncio.wait_for(commit_signal.wait(), timeout=15)
        commit_signal.clear()
        batch, pending[:] = pending[:BATCH], pending[BATCH:]
        await commit_batch(batch)

    print(f"done: {len(committed_rows)} rows committed, {total - len(committed_rows)} dead-lettered")
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
