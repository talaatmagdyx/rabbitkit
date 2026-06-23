"""Consumer throughput load test against a real RabbitMQ (testcontainers).

Measures the HONEST consumer ceiling: the queue is fully PRE-LOADED by a separate
producer process first, then consumers drain it and we time first-handled to
last-handled. (An earlier version published while consuming on the same loop, so
it measured the single producer's ~5k/s publish rate, not the consumer.)

Findings on a local container:
  - single consumer process:  ~14k msg/s  (pipeline is CPU-cheap; bound by
    aio-pika + asyncio + ack round-trip on one event loop)
  - scales near-linearly with PROCESSES (not connections, not worker_count):
    2 proc ~28k, 4 proc ~38k msg/s
  - a single producer publishes ~5k/s (publisher-confirm round-trip bound) —
    publish throughput also scales out with more producer processes.

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python benchmarks/load_test_worker_pool.py
Requires Docker + `pip install rabbitkit[integration]`.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import time
from typing import Any

from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import ConnectionConfig, PoolConfig, RabbitConfig
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import MessageEnvelope

logging.getLogger("rabbitkit").setLevel(logging.ERROR)


def _preload(url: str, queue: str, n: int) -> None:
    """Declare the durable queue and publish n messages, then exit."""
    logging.getLogger("rabbitkit").setLevel(logging.ERROR)

    async def go() -> None:
        broker = AsyncBroker(
            RabbitConfig(connection=ConnectionConfig.from_url(url), pool=PoolConfig(channel_pool_size=64))
        )
        await broker.start()
        await broker._transport.declare_queue(RabbitQueue(name=queue))
        sem = asyncio.Semaphore(48)

        async def one() -> None:
            async with sem:
                await broker.publish(MessageEnvelope(routing_key=queue, body=b"x"))

        await asyncio.gather(*(one() for _ in range(n)))
        await broker.stop()

    asyncio.run(go())


def _consumer(url: str, queue: str, counter: Any, n: int, prefetch: int) -> None:
    logging.getLogger("rabbitkit").setLevel(logging.ERROR)

    async def go() -> None:
        broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))

        @broker.subscriber(queue=queue, prefetch_count=prefetch)
        async def handle(body: bytes) -> None:
            with counter.get_lock():
                counter.value += 1

        await broker.start()
        while counter.value < n:
            await asyncio.sleep(0.05)
        await broker.stop()

    asyncio.run(go())


def _preload_and_measure(url: str, *, procs: int, n: int, prefetch: int) -> None:
    queue = f"load-{procs}"
    prod = mp.Process(target=_preload, args=(url, queue, n))
    prod.start()
    prod.join(timeout=120)  # fully pre-load before consuming

    counter = mp.Value("i", 0)
    consumers = [mp.Process(target=_consumer, args=(url, queue, counter, n, prefetch)) for _ in range(procs)]
    for cons in consumers:
        cons.start()
    while counter.value == 0:
        time.sleep(0.001)
    t0 = time.monotonic()
    while counter.value < n:
        time.sleep(0.005)
    elapsed = time.monotonic() - t0

    for cons in consumers:
        cons.join(timeout=10)
        if cons.is_alive():
            cons.terminate()
    print(f"  {procs} consumer process(es)  drained {n} msgs in {elapsed:5.2f}s  ->  {n / elapsed:9.0f} msg/s")


def main() -> None:
    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as c:
        url = f"amqp://guest:guest@{c.get_container_host_ip()}:{c.get_exposed_port(5672)}/"
        print("\nConsumer throughput (queue pre-loaded; pure drain rate):")
        for procs in (1, 2, 4):
            _preload_and_measure(url, procs=procs, n=20000, prefetch=100)


if __name__ == "__main__":
    main()
