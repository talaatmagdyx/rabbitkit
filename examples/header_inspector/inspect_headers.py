"""Header inspector — publish dynamically-generated messages with rich headers +
AMQP properties, then dump EVERY field a consumer sees on each delivery.

Proves exactly what's available on an incoming RabbitMessage (and what is NOT —
e.g. msg.timestamp is None on consume; priority/expiration only via raw_message).

Run:
    docker compose up -d                 # RabbitMQ on 127.0.0.1:5672
    pip install -e ../..[async]          # or: pip install rabbitkit[async]
    python inspect_headers.py
    docker compose down

NOTE: no `from __future__ import annotations` — keeps annotations real (the rule
for Pydantic body decoding; harmless here but the house convention for examples).
"""

import asyncio
import itertools
import json
import random
import uuid
from datetime import UTC, datetime

from rabbitkit import ConnectionConfig, MessageEnvelope, RabbitConfig, RabbitExchange
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import AckPolicy, ExchangeType

TENANTS = ("acme", "globex", "initech", "umbrella")
EVENT_TYPES = ("order.created", "order.updated", "order.cancelled")

broker = AsyncBroker(
    RabbitConfig(connection=ConnectionConfig(host="127.0.0.1", connection_name="header-inspector"))
)
_seq = itertools.count(1)


# ── The point of the example: dump everything a consumer receives ────────────
def dump_message(msg: RabbitMessage) -> None:
    n = next(_seq)
    print(f"\n┌─ consumed message #{n}  (delivery_tag={msg.delivery_tag}) " + "─" * 22)

    print("│ HEADERS  (msg.headers — custom + protocol + RabbitMQ's x-death):")
    if msg.headers:
        for k, v in msg.headers.items():
            print(f"│     {k!r}: {v!r}")
    else:
        print("│     (none)")

    print("│ AMQP PROPERTIES (surfaced as typed attributes):")
    for name in ("message_id", "correlation_id", "reply_to", "content_type",
                 "content_encoding", "type", "app_id", "timestamp"):
        note = "   <- NOT populated on consume (always None)" if name == "timestamp" else ""
        print(f"│     {name:<16} = {getattr(msg, name)!r}{note}")

    print("│ DELIVERY METADATA:")
    for name in ("routing_key", "exchange", "delivery_tag", "redelivered", "consumer_tag"):
        print(f"│     {name:<16} = {getattr(msg, name)!r}")

    print("│ OTHER:")
    print(f"│     body             = {msg.body!r}")
    print(f"│     path             = {msg.path!r}   (topic-wildcard captures, read by Path() DI)")
    print(f"│     is_settled       = {msg.is_settled!r}")
    print(f"│     raw_message set  = {msg.raw_message is not None}")

    raw = msg.raw_message
    if raw is not None:
        print("│ VIA raw_message (async-only escape hatch — these are NOT on RabbitMessage):")
        for name in ("priority", "expiration", "timestamp", "delivery_mode", "user_id"):
            print(f"│     {name:<16} = {getattr(raw, name, None)!r}")
    print("└" + "─" * 60)


@broker.subscriber(
    queue="demo.queue",
    exchange=RabbitExchange(name="demo.exchange", type=ExchangeType.TOPIC),
    routing_key="order.*",
    ack_policy=AckPolicy.AUTO,
)
async def consume(msg: RabbitMessage) -> None:
    dump_message(msg)


# ── Dynamic producer — varied headers/properties on every run ────────────────
async def publish_batch(count: int) -> None:
    for i in range(count):
        event_type = random.choice(EVENT_TYPES)
        body = json.dumps({
            "seq": i,
            "order_id": f"ord-{uuid.uuid4().hex[:8]}",
            "amount_cents": random.randint(100, 50_000),
        }).encode()
        env = MessageEnvelope(
            routing_key=event_type,
            exchange="demo.exchange",
            body=body,
            headers={
                "x-tenant": random.choice(TENANTS),
                "trace-id": str(uuid.uuid4()),
                "x-attempt": i,
                "x-source": "header-inspector",
                "x-flag": random.choice([True, False]),
            },
            message_id=str(uuid.uuid4()),
            correlation_id=f"corr-{i}",
            reply_to="demo.reply",
            content_type="application/json",
            type=event_type,
            app_id="header-inspector-producer",
            priority=random.randint(0, 9),
            timestamp=datetime.now(UTC),
            expiration=str(random.choice((30_000, 60_000))),  # per-message TTL (ms)
        )
        outcome = await broker.publish(env)
        print(f"[publish] #{i} rk={event_type:<16} tenant={env.headers['x-tenant']:<9} -> {outcome.status.value}")


async def main() -> None:
    await broker.start()
    print("broker started; publishing dynamic messages...\n")
    await publish_batch(8)
    print("\nwaiting for the consumer to drain...")
    await asyncio.sleep(2.0)
    await broker.stop()
    print("\ndone.")


if __name__ == "__main__":
    asyncio.run(main())
