"""Shared utilities for real-broker benchmarks."""

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
from typing import Any

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

IMAGE = "rabbitmq:3.13-management-alpine"
PRELOAD_CONCURRENCY = 64

def _bench_safety():  # lazy: keep module importable without rabbitkit extras
    from rabbitkit.core.config import SafetyConfig

    # Benchmarks pre-load queues with plain durable declarations; the
    # auto_provision safety default (C3) would re-declare them with an
    # x-dead-letter-exchange argument and hit a 406 inequivalent-arg error.
    # Message-safety topology is not what these benches measure.
    return SafetyConfig(reject_without_dlx="discard", warn_on_discard=False)



def make_url(container: Any) -> str:
    return (
        f"amqp://guest:guest@"
        f"{container.get_container_host_ip()}:{container.get_exposed_port(5672)}/"
    )


def preload(url: str, queue: str, n: int, body: bytes = b'{"id":1}', passive: bool = False) -> None:
    """Fill *queue* with *n* messages. Runs in a subprocess.

    When *passive=True* the queue is assumed to already exist (e.g. pre-declared
    with DLX by a retry-enabled broker). Declaration is skipped so we do not
    overwrite queue arguments with a plain durable declaration.
    """
    logging.getLogger("rabbitkit").setLevel(logging.ERROR)

    async def go() -> None:
        from rabbitkit.async_.broker import AsyncBroker
        from rabbitkit.core.config import ConnectionConfig, PoolConfig, RabbitConfig
        from rabbitkit.core.topology import RabbitQueue

        broker = AsyncBroker(
            RabbitConfig(safety=_bench_safety(),
                connection=ConnectionConfig.from_url(url),
                pool=PoolConfig(channel_pool_size=PRELOAD_CONCURRENCY),
            )
        )
        await broker.start()
        if not passive:
            await broker._transport.declare_queue(RabbitQueue(name=queue, durable=True))

        sem = asyncio.Semaphore(PRELOAD_CONCURRENCY)

        async def one(b: bytes) -> None:
            async with sem:
                await broker.publish(routing_key=queue, body=b)

        await asyncio.gather(*(one(body) for _ in range(n)))
        await broker.stop()

    asyncio.run(go())


def preload_proc(url: str, queue: str, n: int, body: bytes = b'{"id":1}', passive: bool = False) -> None:
    """Spawn a subprocess that fills the queue, wait for it."""
    p = mp.Process(target=preload, args=(url, queue, n, body, passive))
    p.start()
    p.join(timeout=180)


def percentiles(samples: list[float]) -> tuple[float, float, float, float, float]:
    """Return (p50, p95, p99, min, max) of *samples*."""
    s = sorted(samples)
    n = len(s)
    return (
        s[int(n * 0.50)],
        s[int(n * 0.95)],
        s[int(n * 0.99)],
        s[0],
        s[-1],
    )
