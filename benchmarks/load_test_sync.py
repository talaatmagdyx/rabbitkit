"""Sync broker throughput vs the async broker, against a real RabbitMQ.

Consumer: queue is pre-loaded (async producer process), then the SyncBroker
drains it; we time first-handled to last-handled. With worker_count > 1, every
ack/nack is marshaled to the connection's I/O thread (the C1 thread-safety fix),
so this measures that path's real ceiling.

Publisher: single-threaded SyncBroker.publish loop with confirms.

Run: TESTCONTAINERS_RYUK_DISABLED=true python benchmarks/load_test_sync.py
"""

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
    """Async producer (separate process): declare durable queue + publish n."""
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


def _sync_consume(url: str, *, worker_count: int, n: int, prefetch: int) -> float:
    from rabbitkit.sync.broker import SyncBroker

    queue = f"sync-cons-{worker_count}"
    p = mp.Process(target=_preload, args=(url, queue, n))  # pre-load the queue first
    p.start()
    p.join(timeout=120)

    broker = SyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
    count = 0
    lock = threading.Lock()
    span: dict[str, float] = {}
    done = threading.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    def handle(body: bytes) -> None:
        nonlocal count
        with lock:
            if count == 0:
                span["start"] = time.monotonic()
            count += 1
            if count >= n:
                span["end"] = time.monotonic()
                done.set()

    # IMPORTANT: connect() AND start_consuming() must run on the SAME thread so
    # the C1 owner-thread detection is correct (as SyncBroker.run() does). Start
    # the broker inside the consume thread.
    def consume_loop() -> None:
        broker.start(worker_config=WorkerConfig(worker_count=worker_count))
        broker._transport.start_consuming()

    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    done.wait(timeout=120)
    conn = broker._transport._connection
    conn.add_callback_threadsafe(broker._transport.stop_consuming)
    t.join(timeout=10)
    broker.stop()
    return n / (span["end"] - span["start"])


def _sync_publish(url: str, *, n: int) -> float:
    from rabbitkit.sync.broker import SyncBroker

    broker = SyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
    broker.start()
    broker._transport.declare_queue(RabbitQueue(name="sync-pub"))
    t0 = time.monotonic()
    for _ in range(n):
        broker.publish(MessageEnvelope(routing_key="sync-pub", body=b"x"))
    rate = n / (time.monotonic() - t0)
    broker.stop()
    return rate


def main() -> None:
    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as c:
        url = f"amqp://guest:guest@{c.get_container_host_ip()}:{c.get_exposed_port(5672)}/"

        print("\nSYNC consumer (queue pre-loaded; pure drain rate):")
        for wc in (1, 4, 8):
            r = _sync_consume(url, worker_count=wc, n=20000, prefetch=100)
            print(f"  worker_count={wc:>2}  ->  {r:9,.0f} msg/s")

        print("\nSYNC publisher (single thread, confirms on):")
        r = _sync_publish(url, n=10000)
        print(f"  blocking publish  ->  {r:9,.0f} msg/s")


if __name__ == "__main__":
    main()
