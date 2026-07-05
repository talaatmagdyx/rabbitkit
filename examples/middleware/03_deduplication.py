"""Middleware: Redis-based message deduplication.

DeduplicationMiddleware ensures each message is processed exactly once.
Uses Redis SETNX keyed by message_id (or correlation_id, or body hash).

Run:
    python examples/middleware/03_deduplication.py

Requirements:
    pip install "rabbitkit[async,redis]"
    RabbitMQ running on localhost:5672
    Redis running on localhost:6379
"""

import asyncio
import uuid

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.middleware.deduplication import DeduplicationConfig, DeduplicationMiddleware

try:
    import redis.asyncio as aioredis  # type: ignore[import-untyped]
    redis_client = aioredis.Redis(host="localhost", port=6379, decode_responses=False)
except ImportError:
    print("redis not installed — run: pip install redis")
    raise

broker = AsyncBroker(RabbitConfig())

# ── Dedup by message_id (recommended) ────────────────────────────────────────
dedup_mw = DeduplicationMiddleware(
    redis_client=redis_client,
    config=DeduplicationConfig(
        key_source="message_id",     # use AMQP message_id property as dedup key
        key_prefix="myapp:dedup:",   # Redis key prefix
        ttl=86400,                   # remember processed IDs for 24 hours
        fallback_on_redis_error=True,  # process anyway if Redis is down
    ),
)

@broker.subscriber(queue="idempotent-orders", middlewares=[dedup_mw])
async def handle_order(body: bytes) -> None:
    """This handler is called exactly once per unique message_id."""
    print(f"[dedup] processing order: {body.decode()}")


# ── Dedup by body hash ────────────────────────────────────────────────────────
body_hash_mw = DeduplicationMiddleware(
    redis_client=redis_client,
    config=DeduplicationConfig(
        key_source="body_hash",   # SHA-256 of body bytes
        ttl=3600,
    ),
)

@broker.subscriber(queue="content-dedup", middlewares=[body_hash_mw])
async def handle_unique_content(body: bytes) -> None:
    """Dedup by content — same body bytes = same logical message."""
    print(f"[body-hash dedup] {body.decode()}")


# ── Custom key function ───────────────────────────────────────────────────────
import json


def extract_order_id(msg: "object") -> str:
    """Extract order_id from body as the dedup key."""
    try:
        return json.loads(msg.body)["order_id"]  # type: ignore[union-attr]
    except Exception:
        return ""

custom_dedup_mw = DeduplicationMiddleware(
    redis_client=redis_client,
    key_fn=extract_order_id,
    config=DeduplicationConfig(ttl=7200),
)

@broker.subscriber(queue="custom-keyed-orders", middlewares=[custom_dedup_mw])
async def handle_custom_dedup(body: bytes) -> None:
    data = json.loads(body)
    print(f"[custom-dedup] order_id={data['order_id']}")


async def main() -> None:
    await broker.start()

    # Same message_id → only processed once
    msg_id = str(uuid.uuid4())
    for _ in range(3):
        await broker.publish(MessageEnvelope(
            routing_key="idempotent-orders",
            body=b'{"order": 1}',
            message_id=msg_id,  # same ID → deduplicated
        ))
    print(f"Published 3 messages with same message_id={msg_id[:8]}...")
    print("Only 1 should be processed.")

    await asyncio.sleep(0.5)

    # Same body bytes → only processed once (body_hash mode)
    for _ in range(2):
        await broker.publish(MessageEnvelope(
            routing_key="content-dedup",
            body=b'{"event": "user.registered", "email": "alice@example.com"}',
        ))
    print("Published 2 identical messages — only 1 should be processed.")

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
