"""Worker-pool throughput load test against a real RabbitMQ (testcontainers).

Answers the original "is this fast enough for high throughput?" question
empirically. The PRODUCER runs in a SEPARATE process so it does not share the
consumer's event loop, and throughput is measured as the pure consume drain rate
(first handled message -> last), excluding producer/process startup. This gives
an honest consumer ceiling rather than a publish+consume-on-one-loop artifact.

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python benchmarks/load_test_worker_pool.py
Requires Docker + `pip install rabbitkit[integration]`.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import time

try:
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    raise SystemExit("testcontainers not installed — pip install rabbitkit[integration]") from None

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import ConnectionConfig, PoolConfig, RabbitConfig, WorkerConfig
from rabbitkit.core.types import MessageEnvelope

logging.getLogger("rabbitkit").setLevel(logging.ERROR)

_PUBLISH_CONCURRENCY = 48


def _publisher_proc(url: str, queue: str, n: int) -> None:
    """Runs in a separate process: publish n messages, then exit."""
    logging.getLogger("rabbitkit").setLevel(logging.ERROR)

    async def _go() -> None:
        config = RabbitConfig(
            connection=ConnectionConfig.from_url(url),
            pool=PoolConfig(channel_pool_size=64),
        )
        broker = AsyncBroker(config)
        await broker.start()  # no subscribers — just a publisher connection
        sem = asyncio.Semaphore(_PUBLISH_CONCURRENCY)

        async def one() -> None:
            async with sem:
                await broker.publish(MessageEnvelope(routing_key=queue, body=b"x"))

        await asyncio.gather(*(one() for _ in range(n)))
        await broker.stop()

    asyncio.run(_go())


async def _run_case(
    url: str, *, label: str, n: int, worker_count: int, prefetch: int, work_s: float
) -> None:
    config = RabbitConfig(
        connection=ConnectionConfig.from_url(url),
        pool=PoolConfig(channel_pool_size=64),
    )
    broker = AsyncBroker(config)

    done = asyncio.Event()
    count = 0
    span: dict[str, float] = {}
    queue = f"load-{label}-{worker_count}-{prefetch}"

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: bytes) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()  # first message handled
        if work_s:
            await asyncio.sleep(work_s)
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=worker_count))
    await asyncio.sleep(0.5)  # queue declared, consumer ready

    proc = mp.Process(target=_publisher_proc, args=(url, queue, n))
    proc.start()
    try:
        await asyncio.wait_for(done.wait(), timeout=180.0)
    finally:
        proc.join(timeout=15.0)
        await broker.stop()

    elapsed = span["end"] - span["start"]  # pure consume drain, excl. startup
    print(f"  {label:9} workers={worker_count:>2} prefetch={prefetch:>3}  "
          f"{n} msgs drained in {elapsed:6.2f}s  ->  {n / elapsed:9.0f} msg/s")


async def main() -> None:
    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as c:
        url = f"amqp://guest:guest@{c.get_container_host_ip()}:{c.get_exposed_port(5672)}/"

        print("\nCPU-trivial handler (raw pipeline + ack ceiling, external producer):")
        await _run_case(url, label="trivial", n=20000, worker_count=1, prefetch=50, work_s=0)
        await _run_case(url, label="trivial", n=20000, worker_count=8, prefetch=200, work_s=0)

        print("\nIO-bound handler (5ms work — concurrency scaling):")
        await _run_case(url, label="io", n=4000, worker_count=1, prefetch=50, work_s=0.005)
        await _run_case(url, label="io", n=4000, worker_count=50, prefetch=400, work_s=0.005)


if __name__ == "__main__":
    asyncio.run(main())
