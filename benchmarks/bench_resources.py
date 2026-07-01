"""rabbitkit resource benchmarks — CPU and memory usage with real RabbitMQ.

Pre-loads a queue then measures RSS delta and CPU time while the consumer
drains it. Isolates consume-side resource usage from publish overhead.

Requires: psutil (pip install psutil)
Requires: Docker (for testcontainers)

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_resources
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_resources --url amqp://guest:guest@localhost/
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import logging
import os
import time
from typing import Any

from benchmarks._common import preload_proc

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

ITERATIONS = 5_000
PREFETCH = 200


def _require_psutil() -> Any:
    try:
        import psutil
        return psutil
    except ImportError as e:
        raise ImportError("psutil required: pip install psutil") from e


async def _consume_raw(url: str, queue: str, n: int, prefetch: int) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, WorkerConfig

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))
    count = 0
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: bytes) -> None:
        nonlocal count
        count += 1
        if count >= n:
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()


async def _consume_json(url: str, queue: str, n: int, prefetch: int) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, WorkerConfig

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))
    count = 0
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: dict[str, Any]) -> None:
        nonlocal count
        count += 1
        if count >= n:
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()


async def _consume_pydantic(url: str, queue: str, n: int, prefetch: int) -> None:
    from pydantic import BaseModel

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, WorkerConfig

    class Order(BaseModel):
        id: int
        name: str
        amount: float

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))
    count = 0
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: Order) -> None:
        nonlocal count
        count += 1
        if count >= n:
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()


async def _consume_retry(url: str, queue: str, n: int, prefetch: int) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, RetryConfig, WorkerConfig

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))
    count = 0
    done = asyncio.Event()

    @broker.subscriber(
        queue=queue,
        prefetch_count=prefetch,
        retry=RetryConfig(max_retries=3, delays=(1, 5, 30)),
    )
    async def handle(body: bytes) -> None:
        nonlocal count
        count += 1
        if count >= n:
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()


def _measure(url: str, queue: str, body: bytes, coro_fn: Any) -> dict[str, float]:
    """Pre-load queue, then measure resource usage while consuming."""
    psutil = _require_psutil()
    proc = psutil.Process(os.getpid())

    preload_proc(url, queue, ITERATIONS, body)

    gc.collect()
    gc.disable()

    mem_before = proc.memory_info().rss
    cpu_before = sum(proc.cpu_times()[:2])
    t0 = time.perf_counter()

    asyncio.run(coro_fn(url, queue, ITERATIONS, PREFETCH))

    elapsed = time.perf_counter() - t0
    cpu_after = sum(proc.cpu_times()[:2])
    mem_after = proc.memory_info().rss

    gc.enable()
    gc.collect()

    return {
        "duration_s": elapsed,
        "throughput_msg_s": ITERATIONS / elapsed,
        "rss_delta_kb": (mem_after - mem_before) / 1024,
        "cpu_ns_per_msg": (cpu_after - cpu_before) * 1e9 / ITERATIONS,
    }


def run_all(url: str) -> None:
    try:
        _require_psutil()
    except ImportError as e:
        print(f"SKIP: {e}")
        return

    plain_body = b'{"id":1}'
    rich_body = json.dumps({"id": 1, "name": "order", "amount": 99.5}).encode()

    scenarios: list[tuple[str, str, bytes, Any]] = [
        ("Raw (bytes handler)", "res-raw", plain_body, _consume_raw),
        ("JSON deserialization", "res-json", rich_body, _consume_json),
        ("Retry middleware (success path)", "res-retry", plain_body, _consume_retry),
    ]

    print("=" * 78)
    print("rabbitkit Resource Benchmarks (real RabbitMQ + psutil)")
    print(f"  {ITERATIONS:,} messages per scenario · prefetch={PREFETCH}")
    print("=" * 78)
    print(f"  {'Scenario':<36}  {'msg/s':>10}  {'CPU ns/msg':>12}  {'RSS delta':>10}")
    print(f"  {'-'*36}  {'-'*10}  {'-'*12}  {'-'*10}")

    for name, queue, body, coro_fn in scenarios:
        try:
            m = _measure(url, queue, body, coro_fn)
            rss = m["rss_delta_kb"]
            rss_str = f"{rss:+.0f} KB" if abs(rss) < 1024 else f"{rss/1024:+.1f} MB"
            print(
                f"  {name:<36}  {m['throughput_msg_s']:>10,.0f}"
                f"  {m['cpu_ns_per_msg']:>12.0f}"
                f"  {rss_str:>10}"
            )
        except Exception as e:
            print(f"  {name:<36}  ERROR: {e}")

    # Pydantic separate (may be missing)
    try:
        m = _measure(url, "res-pydantic", rich_body, _consume_pydantic)
        rss = m["rss_delta_kb"]
        rss_str = f"{rss:+.0f} KB" if abs(rss) < 1024 else f"{rss/1024:+.1f} MB"
        name = "Pydantic model"
        print(
            f"  {name:<36}  {m['throughput_msg_s']:>10,.0f}"
            f"  {m['cpu_ns_per_msg']:>12.0f}"
            f"  {rss_str:>10}"
        )
    except ImportError:
        print(f"  {'Pydantic model':<36}  SKIP (pydantic not installed)")
    except Exception as e:
        print(f"  {'Pydantic model':<36}  ERROR: {e}")

    print("=" * 78)
    print("RSS delta = process RSS after consume - before (GC collected at boundaries).")
    print("CPU ns/msg = (user + sys CPU time) / messages consumed.")
    print("=" * 78)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None, help="AMQP URL (default: auto-start testcontainers)")
    args = parser.parse_args()

    if args.url:
        run_all(args.url)
    else:
        from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

        from benchmarks._common import make_url
        with RabbitMqContainer("rabbitmq:3.13-management-alpine") as c:
            run_all(make_url(c))


if __name__ == "__main__":
    main()
