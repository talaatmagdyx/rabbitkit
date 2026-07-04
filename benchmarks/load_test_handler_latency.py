"""Map per-process consume throughput vs handler work, for async and sync wc=1.
Answers: when can one worker sustain >= 10k msg/s?"""
from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import threading
import time

from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

from benchmarks._common import _bench_safety
from rabbitkit.core.config import ConnectionConfig, PoolConfig, RabbitConfig, WorkerConfig
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import MessageEnvelope

logging.getLogger("rabbitkit").setLevel(logging.ERROR)


def _preload(url: str, queue: str, n: int) -> None:
    logging.getLogger("rabbitkit").setLevel(logging.ERROR)
    from rabbitkit.async_.broker import AsyncBroker

    async def go() -> None:
        b = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url),
                                     pool=PoolConfig(channel_pool_size=64)))
        await b.start()
        await b._transport.declare_queue(RabbitQueue(name=queue))
        sem = asyncio.Semaphore(48)
        async def one() -> None:
            async with sem:
                await b.publish(MessageEnvelope(routing_key=queue, body=b"x"))
        await asyncio.gather(*(one() for _ in range(n)))
        await b.stop()
    asyncio.run(go())


def _busy(us: float) -> None:
    end = time.perf_counter() + us / 1e6
    while time.perf_counter() < end:
        pass


async def _async_case(url: str, queue: str, n: int, prefetch: int, work: str) -> float:
    from rabbitkit.async_.broker import AsyncBroker
    b = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
    span: dict[str, float] = {}
    count = 0

    @b.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: bytes) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        if work == "cpu100":
            _busy(100)
        elif work == "io1ms":
            await asyncio.sleep(0.001)
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    done = asyncio.Event()
    await b.start()
    await asyncio.wait_for(done.wait(), timeout=120)
    await b.stop()
    return n / (span["end"] - span["start"])


def _sync_case(url: str, queue: str, n: int, prefetch: int, work: str) -> float:
    from rabbitkit.sync.broker import SyncBroker
    b = SyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
    span: dict[str, float] = {}
    count = 0
    done = threading.Event()

    @b.subscriber(queue=queue, prefetch_count=prefetch)
    def handle(body: bytes) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        if work == "cpu100":
            _busy(100)
        elif work == "io1ms":
            time.sleep(0.001)
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    def loop() -> None:
        b.start(worker_config=WorkerConfig(worker_count=1))
        b._transport.start_consuming()
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    done.wait(timeout=120)
    b._transport._connection.add_callback_threadsafe(b._transport.stop_consuming)
    t.join(timeout=10)
    b.stop()
    return n / (span["end"] - span["start"])


def main() -> None:
    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as c:
        url = f"amqp://guest:guest@{c.get_container_host_ip()}:{c.get_exposed_port(5672)}/"
        n = 20000
        works = [("trivial", "none"), ("100us CPU", "cpu100"), ("1ms I/O", "io1ms")]
        print("\nASYNC (one process, prefetch=300):")
        for label, w in works:
            q = f"a-{w}"
            pre = mp.Process(target=_preload, args=(url, q, n))
            pre.start()
            pre.join(120)
            r = asyncio.run(_async_case(url, q, n, 300, w))
            print(f"  handler={label:10}  ->  {r:9,.0f} msg/s   {'>=10k OK' if r >= 10000 else '< 10k'}")
        print("\nSYNC worker_count=1 (one process, prefetch=300):")
        for label, w in works:
            q = f"s-{w}"
            pre = mp.Process(target=_preload, args=(url, q, n))
            pre.start()
            pre.join(120)
            r = _sync_case(url, q, n, 300, w)
            print(f"  handler={label:10}  ->  {r:9,.0f} msg/s   {'>=10k OK' if r >= 10000 else '< 10k'}")


if __name__ == "__main__":
    main()
