"""Publisher throughput experiment — find the lever for fast publishing.

Tests, against a real broker:
  - publish concurrency (in-flight publishes) with confirms ON
  - confirms ON vs OFF (raw aio-pika channel, to show the round-trip cost)
  - multi-process publish scaling

Run: TESTCONTAINERS_RYUK_DISABLED=true python benchmarks/load_test_publisher.py
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import time
from typing import Any

from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

from benchmarks._common import _bench_safety
from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import ConnectionConfig, PoolConfig, RabbitConfig
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import MessageEnvelope

logging.getLogger("rabbitkit").setLevel(logging.ERROR)


async def _publish_via_broker(url: str, queue: str, n: int, concurrency: int) -> float:
    broker = AsyncBroker(
        RabbitConfig(
        safety=_bench_safety(),
        connection=ConnectionConfig.from_url(url),
        pool=PoolConfig(channel_pool_size=max(64, concurrency)),
    )
    )
    await broker.start()
    await broker._transport.declare_queue(RabbitQueue(name=queue))
    sem = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with sem:
            await broker.publish(MessageEnvelope(routing_key=queue, body=b"x"))

    t0 = time.monotonic()
    await asyncio.gather(*(one() for _ in range(n)))
    elapsed = time.monotonic() - t0
    await broker.stop()
    return elapsed


async def _publish_raw_aiopika(url: str, queue: str, n: int, concurrency: int, confirms: bool) -> float:
    import aio_pika

    conn = await aio_pika.connect_robust(url)
    channel = await conn.channel(publisher_confirms=confirms)
    await channel.declare_queue(queue, durable=True)
    ex = channel.default_exchange
    sem = asyncio.Semaphore(concurrency)
    msg_body = b"x"

    async def one() -> None:
        async with sem:
            await ex.publish(aio_pika.Message(body=msg_body), routing_key=queue)

    t0 = time.monotonic()
    await asyncio.gather(*(one() for _ in range(n)))
    elapsed = time.monotonic() - t0
    await conn.close()
    return elapsed


def _producer_proc(url: str, queue: str, n: int, concurrency: int, counter: Any) -> None:
    logging.getLogger("rabbitkit").setLevel(logging.ERROR)
    elapsed = asyncio.run(_publish_via_broker(url, queue, n, concurrency))
    with counter.get_lock():
        counter.value += 1
    # store elapsed*1000 ms in a second shared slot is overkill; just print here
    print(f"    [proc] published {n} in {elapsed:.2f}s  ({n / elapsed:,.0f} msg/s)")


async def main() -> None:
    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as c:
        url = f"amqp://guest:guest@{c.get_container_host_ip()}:{c.get_exposed_port(5672)}/"
        n = 20000

        print("\n[1] broker.publish (confirms ON) — concurrency sweep:")
        for conc in (48, 256, 1024):
            el = await _publish_via_broker(url, f"pub-c{conc}", n, conc)
            print(f"  concurrency={conc:>5}  {n / el:9,.0f} msg/s")

        print("\n[2] raw aio-pika — confirms ON vs OFF (concurrency=256):")
        el_on = await _publish_raw_aiopika(url, "pub-raw-on", n, 256, confirms=True)
        el_off = await _publish_raw_aiopika(url, "pub-raw-off", n, 256, confirms=False)
        print(f"  confirms=ON   {n / el_on:9,.0f} msg/s")
        print(f"  confirms=OFF  {n / el_off:9,.0f} msg/s  ({el_on / el_off:.1f}x faster)")

        print("\n[3] multi-PROCESS publish (confirms ON, concurrency=256):")
        for k in (1, 2, 4):
            counter = mp.Value("i", 0)
            t0 = time.monotonic()
            procs = [
                mp.Process(target=_producer_proc, args=(url, f"pub-mp{k}-{i}", n, 256, counter))
                for i in range(k)
            ]
            for p in procs:
                p.start()
            for p in procs:
                p.join()
            elapsed = time.monotonic() - t0
            print(f"  {k} producer process(es)  aggregate {k * n / elapsed:9,.0f} msg/s")


if __name__ == "__main__":
    asyncio.run(main())
