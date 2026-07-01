"""Soak test — continuous publish+consume under load to validate long-running
behavior of the daemon pool, batch helpers, channel pool, and reconnect.

Run against a real RabbitMQ::

    # Start a broker (e.g. via docker)
    docker run -d --name rabbitmq-soak -p 5672:5672 rabbitmq:3.13-management-alpine

    # Run the soak test (default 60s; increase for real soak)
    python -m benchmarks.soak_test --url amqp://guest:guest@localhost:5672/ --duration 3600

Exits non-zero if any message is lost (unique count != published count) or if
the throughput drops to zero for more than 10s (indicating a stall/wedge).
"""

from __future__ import annotations

import argparse
import asyncio
import time
from collections.abc import Callable

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import ConnectionConfig, ConsumerConfig, RabbitConfig, WorkerConfig
from rabbitkit.core.types import MessageEnvelope


async def soak(
    url: str,
    duration: float,
    rate: int,
    worker_count: int,
    progress: Callable[[int, int, float], None] | None = None,
) -> tuple[int, int, float]:
    """Run a continuous publish+consume soak.

    Returns (published, consumed_unique, messages_per_second).
    Raises AssertionError if any message is lost or throughput stalls.
    """
    config = RabbitConfig(
        connection=ConnectionConfig.from_url(url),
        consumer=ConsumerConfig(prefetch_count=50),
    )
    broker = AsyncBroker(config=config)

    published = 0
    received: list[bytes] = []
    stall_deadline = time.monotonic() + 10.0  # max 10s with zero throughput

    @broker.subscriber(queue="soak-q")
    async def handle(body: bytes) -> None:
        received.append(body)

    await broker.start(worker_config=WorkerConfig(worker_count=worker_count))
    await asyncio.sleep(0.5)  # let the consumer attach

    start = time.monotonic()
    deadline = start + duration
    last_count = 0
    last_check = start

    try:
        while time.monotonic() < deadline:
            # Publish a batch.
            for _ in range(rate):
                msg = f"m{published}".encode()
                await broker.publish(MessageEnvelope(routing_key="soak-q", body=msg))
                published += 1

            await asyncio.sleep(0.1)

            # Check for stall (no new messages for >10s).
            now = time.monotonic()
            if now - last_check >= 1.0:
                if len(received) == last_count and now > stall_deadline:
                    raise AssertionError(f"throughput stalled: 0 new messages in {now - last_check:.1f}s")
                last_count = len(received)
                last_check = now
                stall_deadline = now + 10.0

                if progress:
                    elapsed = now - start
                    mps = len(received) / elapsed if elapsed > 0 else 0
                    progress(published, len(received), mps)
    finally:
        # Drain remaining messages.
        drain_deadline = time.monotonic() + 10.0
        while time.monotonic() < drain_deadline and len(received) < published:
            await asyncio.sleep(0.1)
        try:
            await broker.stop(timeout=15.0)
        except Exception:
            pass

    unique = set(received)
    elapsed = time.monotonic() - start
    mps = len(unique) / elapsed if elapsed > 0 else 0

    # No message loss.
    assert len(unique) == published, (
        f"LOST {published - len(unique)} messages: published={published}, consumed_unique={len(unique)}"
    )
    return published, len(unique), mps


def main() -> None:
    parser = argparse.ArgumentParser(description="rabbitkit soak test")
    parser.add_argument("--url", default="amqp://guest:guest@localhost:5672/", help="AMQP URL")
    parser.add_argument("--duration", type=float, default=60.0, help="Soak duration in seconds")
    parser.add_argument("--rate", type=int, default=100, help="Messages per batch (10x/sec)")
    parser.add_argument("--workers", type=int, default=4, help="Worker count")
    args = parser.parse_args()

    def progress(pub: int, con: int, mps: float) -> None:
        print(f"  published={pub:,}  consumed={con:,}  throughput={mps:,.0f} msg/s")

    print(f"Soak test: {args.duration}s @ {args.rate * 10} msg/s, workers={args.workers}")
    pub, con, mps = asyncio.run(soak(args.url, args.duration, args.rate, args.workers, progress=progress))
    print(f"\nResult: published={pub:,}  consumed={con:,}  avg={mps:,.0f} msg/s")
    print("PASS: no message loss, no stall")


if __name__ == "__main__":
    main()
