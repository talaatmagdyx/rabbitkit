"""Middleware: Token-bucket rate limiting.

Limits how fast the consumer processes messages.
Three overflow strategies: wait, nack (requeue), drop.

Run:
    python examples/middleware/05_rate_limit.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import time

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.middleware.rate_limit import RateLimitConfig, RateLimitMiddleware

broker = AsyncBroker(RabbitConfig())


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 1: "wait" — sleep until a token is available (default)
# Best for: queues where you can't afford to lose messages but need to
#           protect a downstream service.
# ─────────────────────────────────────────────────────────────────────────────
wait_mw = RateLimitMiddleware(
    RateLimitConfig(max_rate=5.0, burst=2, on_limited="wait")
)

@broker.subscriber(queue="rate-wait", middlewares=[wait_mw])
async def handle_wait(body: bytes) -> None:
    print(f"[wait]  processed at {time.time():.2f}: {body.decode()}")


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 2: "nack" — nack with requeue=True so another consumer handles it
# Best for: multi-consumer setups where overflow is shed to other instances.
# ─────────────────────────────────────────────────────────────────────────────
nack_mw = RateLimitMiddleware(
    RateLimitConfig(max_rate=10.0, burst=3, on_limited="nack")
)

@broker.subscriber(queue="rate-nack", middlewares=[nack_mw])
async def handle_nack(body: bytes) -> None:
    print(f"[nack]  processed: {body.decode()}")


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 3: "drop" — nack with requeue=False (lost or DLQ)
# Best for: analytics / metrics pipelines where dropping some messages
#           under load is acceptable.
# ─────────────────────────────────────────────────────────────────────────────
drop_mw = RateLimitMiddleware(
    RateLimitConfig(max_rate=3.0, burst=1, on_limited="drop")
)

@broker.subscriber(queue="rate-drop", middlewares=[drop_mw])
async def handle_drop(body: bytes) -> None:
    print(f"[drop]  processed: {body.decode()}")


async def main() -> None:
    await broker.start()

    # Burst 10 messages — rate limit is 5/s with burst=2
    print("Sending 10 messages to rate-wait queue (5/s limit)...")
    start = time.time()
    for i in range(10):
        await broker.publish(MessageEnvelope(
            routing_key="rate-wait",
            body=f"msg-{i}".encode(),
        ))
    await asyncio.sleep(3)   # wait for "wait" mode to process all
    print(f"All processed in {time.time()-start:.1f}s (expected ~2s)\n")

    # Drop mode — send 10, expect only ~3-4 to be processed
    print("Sending 10 messages to rate-drop queue (3/s limit, drop mode)...")
    for i in range(10):
        await broker.publish(MessageEnvelope(
            routing_key="rate-drop",
            body=f"msg-{i}".encode(),
        ))
    await asyncio.sleep(0.5)
    print("Some messages were dropped (rate limit exceeded).")

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
