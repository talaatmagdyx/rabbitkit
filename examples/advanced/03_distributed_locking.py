"""Advanced: Distributed locking — LockMiddleware + RedisLock.

Ensures only one consumer across the entire cluster processes a message
for a given key at a time. Prevents duplicate processing in multi-instance
deployments.

Run:
    python examples/advanced/03_distributed_locking.py

Requirements:
    pip install "rabbitkit[async,redis]"
    RabbitMQ running on localhost:5672
    Redis running on localhost:6379
"""

import asyncio
import json

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.locking import LockMiddleware, RedisLock

try:
    import redis.asyncio as aioredis
    r = aioredis.Redis(host="localhost", port=6379)
except ImportError:
    print("redis not installed — run: pip install redis")
    raise

broker = AsyncBroker(RabbitConfig())


# ── 1. Default key: routing_key ───────────────────────────────────────────────
lock = RedisLock(r, prefix="myapp:lock:", ttl=30)

lock_mw = LockMiddleware(lock, timeout=5.0)

@broker.subscriber(queue="exclusive-tasks", middlewares=[lock_mw])
async def handle_exclusive(body: bytes) -> None:
    """Only one instance processes at a time (keyed by routing_key)."""
    print(f"[lock] acquired for routing_key — processing: {body.decode()}")
    await asyncio.sleep(0.5)  # simulate work
    print("[lock] done, releasing lock")


# ── 2. Custom key: order_id from body ────────────────────────────────────────
def order_id_key(msg: "object") -> str:
    """Extract order_id as the lock key — prevents concurrent processing of the same order."""
    try:
        return json.loads(msg.body)["order_id"]  # type: ignore[union-attr]
    except Exception:
        return ""


order_lock_mw = LockMiddleware(lock, key_fn=order_id_key, timeout=10.0)

@broker.subscriber(queue="order-processor", middlewares=[order_lock_mw])
async def handle_order(body: bytes) -> None:
    data = json.loads(body)
    order_id = data["order_id"]
    print(f"[order-lock] acquired lock for order_id={order_id!r}")
    await asyncio.sleep(0.3)
    print(f"[order-lock] processed order {order_id!r}")


# ── 3. When lock is unavailable ───────────────────────────────────────────────
# If another instance holds the lock, the message is nacked with requeue=True.
# With retry configured, this becomes a natural wait-and-retry loop.

from rabbitkit.core.config import RetryConfig

broker_with_retry = AsyncBroker(RabbitConfig(
    retry=RetryConfig(max_retries=5, delays=(1, 2, 4, 8, 16))
))

retry_lock_mw = LockMiddleware(lock, key_fn=order_id_key, timeout=0.1)  # short timeout

@broker_with_retry.subscriber(queue="lock-with-retry", middlewares=[retry_lock_mw])
async def handle_with_lock_retry(body: bytes) -> None:
    data = json.loads(body)
    print(f"[retry-lock] processing order {data['order_id']!r}")


# ── 4. Custom lock implementation ────────────────────────────────────────────
# Any object with acquire/release + async variants works:
#
# class PostgresAdvisoryLock:
#     def acquire(self, key: str, timeout: float = 10.0) -> bool:
#         lock_id = hash(key) % (2**31)
#         return db.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,)).scalar()
#
#     def release(self, key: str) -> None:
#         lock_id = hash(key) % (2**31)
#         db.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
#
#     async def acquire_async(self, key: str, timeout: float = 10.0) -> bool: ...
#     async def release_async(self, key: str) -> None: ...
#
# lock_mw = LockMiddleware(PostgresAdvisoryLock())


async def main() -> None:
    await broker.start()

    print("=== Default routing_key locking ===")
    await broker.publish(MessageEnvelope(
        routing_key="exclusive-tasks",
        body=b"important task",
    ))
    await asyncio.sleep(1)

    print("\n=== Order ID locking ===")
    # Both messages have the same order_id — second should be nacked
    # (or wait, depending on how fast they're processed)
    for i in range(3):
        await broker.publish(MessageEnvelope(
            routing_key="order-processor",
            body=json.dumps({"order_id": "ORD-001", "attempt": i}).encode(),
        ))
    await asyncio.sleep(2)

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
