"""Bulk operations: transactional outbox → publish_many → transactional inbox.

The end-to-end at-least-once recipe (plan §12), with SQLite standing in for
your database:

* **Outbox.** The business write and the outbox row commit in ONE database
  transaction, so an event exists iff its business change exists.
* **Relay.** Claims a bounded page of unpublished rows, calls
  ``publish_many`` (stable ``message_id`` = outbox id), and marks ONLY the
  CONFIRMED rows as published. UNKNOWN rows stay claimed for reconciliation;
  NOT_SENT rows are released for the next page. A crash between publish and
  mark produces a duplicate on the wire — which the inbox absorbs.
* **Inbox.** The consumer inserts ``(consumer, event_id)`` into an inbox
  table and applies the business effect in the same transaction; a duplicate
  key means "already processed" and is acked without re-applying. The ack is
  issued AFTER the commit via ``ack_many``.

Run:
    python examples/bulk_operations/07_transactional_outbox_inbox.py

Requirements:
    pip install "rabbitkit[sync]"
    RabbitMQ running on localhost:5672
"""

import json
import sqlite3
import uuid

from rabbitkit import (
    AckPolicy,
    BulkPublishStatus,
    ConsumerConfig,
    MessageEnvelope,
    PublisherConfig,
    RabbitConfig,
    RabbitMessage,
)
from rabbitkit.sync import SyncBroker

QUEUE = "bulk-demo-outbox-events"
CONSUMER = "ledger-service"
INBOX_BATCH = 13  # 12 events + 1 deliberate duplicate, committed in one transaction

db = sqlite3.connect(":memory:")
db.executescript(
    """
    CREATE TABLE orders (id TEXT PRIMARY KEY, amount INTEGER);
    CREATE TABLE outbox (id TEXT PRIMARY KEY, payload TEXT, claimed INTEGER DEFAULT 0, published INTEGER DEFAULT 0);
    CREATE TABLE inbox (consumer TEXT, event_id TEXT, PRIMARY KEY (consumer, event_id));
    CREATE TABLE ledger (event_id TEXT PRIMARY KEY, amount INTEGER);
    """
)

# Prefetch MUST cover the inbox batch window: a MANUAL consumer that parks
# deliveries until the commit receives at most ``prefetch_count`` unacked
# messages, so a batch larger than the prefetch would wait forever.
broker = SyncBroker(
    RabbitConfig(
        publisher=PublisherConfig(mandatory=True),
        consumer=ConsumerConfig(prefetch_count=2 * INBOX_BATCH),
    )
)
held: list[RabbitMessage] = []


# ── 1. Business write + outbox row, one transaction ─────────────────────────
def place_order(amount: int) -> str:
    order_id = str(uuid.uuid4())
    with db:  # atomic
        db.execute("INSERT INTO orders VALUES (?, ?)", (order_id, amount))
        db.execute(
            "INSERT INTO outbox (id, payload) VALUES (?, ?)",
            (order_id, json.dumps({"order_id": order_id, "amount": amount})),
        )
    return order_id


# ── 2. Relay: claim a page, publish_many, mark only CONFIRMED rows ───────────
def relay_page(page_size: int) -> int:
    with db:
        rows = db.execute(
            "SELECT id, payload FROM outbox WHERE published = 0 AND claimed = 0 ORDER BY rowid LIMIT ?", (page_size,)
        ).fetchall()
        ids = [r[0] for r in rows]
        db.executemany("UPDATE outbox SET claimed = 1 WHERE id = ?", [(i,) for i in ids])
    if not rows:
        return 0

    envelopes = [
        MessageEnvelope(routing_key=QUEUE, body=payload.encode(), message_id=oid, mandatory=True)
        for oid, payload in rows
    ]
    result = broker.publish_many(envelopes)

    with db:
        for item in result.items:
            oid = ids[item.index]
            if item.status is BulkPublishStatus.CONFIRMED:
                db.execute("UPDATE outbox SET published = 1 WHERE id = ?", (oid,))
            elif item.status is BulkPublishStatus.UNKNOWN:
                print(f"  relay: {oid} UNKNOWN ({item.reason}) — left claimed for reconciliation")
            else:
                db.execute("UPDATE outbox SET claimed = 0 WHERE id = ?", (oid,))  # NOT_SENT/INVALID: release
    return len(result.confirmed)


# ── 3. Inbox consumer: idempotent apply, ack after commit ───────────────────
@broker.subscriber(queue=QUEUE, ack_policy=AckPolicy.MANUAL)
def apply_event(body: bytes, msg: RabbitMessage) -> None:
    held.append(msg)  # settled in a batch after the transaction below


def commit_inbox_batch(deliveries: list[RabbitMessage]) -> None:
    applied = duplicates = 0
    with db:  # inbox marker + business effect in ONE transaction
        for m in deliveries:
            event = json.loads(m.body)
            try:
                db.execute("INSERT INTO inbox VALUES (?, ?)", (CONSUMER, m.message_id))
            except sqlite3.IntegrityError:
                duplicates += 1  # already processed: ack without re-applying
                continue
            db.execute("INSERT INTO ledger VALUES (?, ?)", (m.message_id, event["amount"]))
            applied += 1
    # Only AFTER the commit is it safe to ack — and only these deliveries.
    broker.ack_many(deliveries).raise_for_status()
    print(f"  inbox: applied={applied} duplicates={duplicates} acked={len(deliveries)}")


def drain_residue() -> None:
    """A reused local broker keeps durable queues between runs; a previous run's
    unacked deliveries come back first. Settle them so the demo's counts are
    about THIS run — and to show ``ack_many`` on an arbitrary set of handles."""
    drained = 0
    while True:
        before = len(held)
        for _ in range(10):
            broker.pump_idle(0.05)
        if len(held) == before:
            break
        broker.ack_many(held).raise_for_status()
        drained += len(held)
        held.clear()
    if drained:
        print(f"drained {drained} leftover deliveries from a previous run")


def main() -> None:
    for amount in range(1, 13):
        place_order(amount * 10)
    broker.start()
    drain_residue()

    published = relay_page(page_size=5) + relay_page(page_size=5) + relay_page(page_size=5)
    # Simulate "crash between publish and mark": re-publish an already published row.
    dup_id, dup_payload = db.execute("SELECT id, payload FROM outbox WHERE published = 1 LIMIT 1").fetchone()
    broker.publish_many([MessageEnvelope(routing_key=QUEUE, body=dup_payload.encode(), message_id=dup_id)])
    print(f"relay: published {published} events (+1 deliberate duplicate)")

    expected = published + 1
    assert expected <= INBOX_BATCH
    for _ in range(300):
        if len(held) >= expected:
            break
        broker.pump_idle(0.05)
    assert len(held) == expected, f"only {len(held)} of {expected} delivered — is prefetch >= the batch window?"
    commit_inbox_batch(held[:expected])
    for _ in range(10):
        broker.pump_idle(0.02)  # let the acks leave

    (ledger_rows,) = db.execute("SELECT COUNT(*) FROM ledger").fetchone()
    (unpublished,) = db.execute("SELECT COUNT(*) FROM outbox WHERE published = 0").fetchone()
    print(f"ledger rows={ledger_rows} (12 orders, 1 duplicate absorbed) unpublished outbox rows={unpublished}")
    broker.stop()


if __name__ == "__main__":
    main()
