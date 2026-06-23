"""Worker-pool throughput load test against a real RabbitMQ (testcontainers).

Answers the original "is this fast enough for high throughput?" question
empirically: measures end-to-end consume throughput (publish -> handler -> ack)
for the async broker at several worker_count / prefetch settings, for both a
CPU-trivial handler (raw pipeline+ack ceiling) and an IO-bound handler
(concurrency scaling).

Run:
    python benchmarks/load_test_worker_pool.py
Requires Docker + `pip install rabbitkit[integration]`.
"""

from __future__ import annotations

import asyncio
import logging
import time

try:
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    raise SystemExit("testcontainers not installed — pip install rabbitkit[integration]") from None

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import ConnectionConfig, PoolConfig, RabbitConfig, WorkerConfig
from rabbitkit.core.types import MessageEnvelope

logging.getLogger("rabbitkit").setLevel(logging.ERROR)  # quiet per-message info logs

_PUBLISH_CONCURRENCY = 48  # bounded so publishes don't starve the channel pool


async def _publish_n(broker: AsyncBroker, queue: str, n: int) -> None:
    sem = asyncio.Semaphore(_PUBLISH_CONCURRENCY)

    async def one() -> None:
        async with sem:
            await broker.publish(MessageEnvelope(routing_key=queue, body=b"x"))

    await asyncio.gather(*(one() for _ in range(n)))


async def _run_case(
    url: str, *, label: str, n: int, worker_count: int, prefetch: int, work_s: float
) -> None:
    config = RabbitConfig(
        connection=ConnectionConfig.from_url(url),
        pool=PoolConfig(channel_pool_size=64),  # > publish concurrency, no exhaustion
    )
    broker = AsyncBroker(config)

    done = asyncio.Event()
    count = 0
    queue = f"load-{label}-{worker_count}-{prefetch}"

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: bytes) -> None:
        nonlocal count
        if work_s:
            await asyncio.sleep(work_s)  # simulate IO-bound work
        count += 1
        if count >= n:
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=worker_count))
    await asyncio.sleep(0.3)  # let the consumer settle

    t0 = time.monotonic()
    await _publish_n(broker, queue, n)
    await asyncio.wait_for(done.wait(), timeout=120.0)
    elapsed = time.monotonic() - t0

    await broker.stop()
    print(f"  {label:9} workers={worker_count:>2} prefetch={prefetch:>3}  "
          f"{n} msgs in {elapsed:6.2f}s  ->  {n / elapsed:9.0f} msg/s")


async def main() -> None:
    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as c:
        url = f"amqp://guest:guest@{c.get_container_host_ip()}:{c.get_exposed_port(5672)}/"

        print("\nCPU-trivial handler (raw pipeline + ack ceiling):")
        await _run_case(url, label="trivial", n=20000, worker_count=1, prefetch=50, work_s=0)
        await _run_case(url, label="trivial", n=20000, worker_count=8, prefetch=200, work_s=0)

        print("\nIO-bound handler (5ms work — concurrency scaling):")
        await _run_case(url, label="io", n=4000, worker_count=1, prefetch=50, work_s=0.005)
        await _run_case(url, label="io", n=4000, worker_count=20, prefetch=200, work_s=0.005)
        await _run_case(url, label="io", n=4000, worker_count=50, prefetch=400, work_s=0.005)


if __name__ == "__main__":
    asyncio.run(main())
