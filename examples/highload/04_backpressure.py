"""High-load: FlowController — publish-side backpressure.

Three pressure signals:
  1. connection.blocked — broker signals memory/disk pressure
  2. In-flight limit — max unconfirmed publishes outstanding
  3. Token-bucket rate limit — messages per second

Run:
    python examples/highload/04_backpressure.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import time

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.highload.backpressure import BackpressureConfig, FlowController

broker = AsyncBroker(RabbitConfig())


@broker.subscriber(queue="backpressure-demo")
async def handle(body: bytes) -> None:
    pass  # Just consume for the demo


async def main() -> None:
    await broker.start()

    # ── 1. Rate-limited publishing ────────────────────────────────────────────
    print("=== Rate-limited publishing (100 msg/s) ===")
    fc = FlowController(BackpressureConfig(
        rate_limit=100.0,          # 100 messages per second
        max_in_flight=200,         # max 200 unconfirmed
        blocked_timeout=30.0,      # wait up to 30s if broker blocks
        on_blocked="wait",         # "wait" | "raise" | "drop"
    ))

    start = time.monotonic()
    published = 0

    for i in range(50):
        # acquire() waits for a rate-limit token
        acquired = await fc.acquire_async(timeout=5.0)
        if acquired:
            await broker.publish(MessageEnvelope(
                routing_key="backpressure-demo",
                body=f"msg-{i}".encode(),
            ))
            await fc.release_async()
            published += 1

    elapsed = time.monotonic() - start
    print(f"Published {published} messages in {elapsed:.2f}s")
    print(f"Effective rate: {published/elapsed:.1f} msg/s\n")

    # ── 2. In-flight limiting ─────────────────────────────────────────────────
    print("=== In-flight limiting (max 5 concurrent) ===")
    fc2 = FlowController(BackpressureConfig(
        max_in_flight=5,
        rate_limit=None,         # no rate limit, only in-flight limit
    ))

    async def publish_with_confirm(i: int) -> None:
        await fc2.acquire_async(timeout=2.0)
        try:
            await broker.publish(MessageEnvelope(
                routing_key="backpressure-demo",
                body=f"inflight-{i}".encode(),
            ))
            await asyncio.sleep(0.1)  # simulate confirm delay
        finally:
            await fc2.release_async()

    tasks = [publish_with_confirm(i) for i in range(20)]
    await asyncio.gather(*tasks)
    print("All 20 messages published with in-flight limit of 5\n")

    # ── 3. connection.blocked handling ───────────────────────────────────────
    print("=== connection.blocked handling ===")
    fc3 = FlowController(BackpressureConfig(
        on_blocked="raise",   # raise BlockedConnectionError immediately
    ))

    # Simulate broker sending connection.blocked
    # In production this is wired to the transport:
    # transport.on_blocked(fc3.on_blocked)
    # transport.on_unblocked(fc3.on_unblocked)
    #
    # fc3.on_blocked()   # simulate blocked
    # try:
    #     await fc3.acquire_async()
    # except BlockedConnectionError:
    #     print("Broker is blocking — backing off")
    # fc3.on_unblocked()

    print("connection.blocked demo: wire fc.on_blocked/on_unblocked to transport signals")

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
