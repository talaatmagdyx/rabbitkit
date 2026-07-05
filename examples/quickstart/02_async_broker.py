"""Quickstart: AsyncBroker — asyncio consumer loop.

Demonstrates the minimal setup needed to receive and publish messages
using the asynchronous (aio-pika-based) broker.

Run:
    python examples/quickstart/02_async_broker.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker

# ── 1. Create broker with default config (localhost:5672, guest/guest) ────────
broker = AsyncBroker(RabbitConfig())


# ── 2. Register an async handler ─────────────────────────────────────────────
@broker.subscriber(queue="greetings")
async def handle_greeting(body: bytes) -> None:
    """Receive a greeting message and print it."""
    print(f"[handler] received: {body.decode()}")


# ── 3. Publish, consume, then stop ───────────────────────────────────────────
async def main() -> None:
    # start() connects, declares topology, begins consuming
    await broker.start()
    print("Connected. Publishing a test message...")

    await broker.publish(
        MessageEnvelope(
            routing_key="greetings",
            body=b"Hello from async rabbitkit!",
        )
    )

    print("Waiting for messages (press Ctrl+C to stop)...")
    try:
        # Keep the event loop alive
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await broker.stop()
        print("Broker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
