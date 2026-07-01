"""rabbitkit latency benchmarks — real RabbitMQ, p50/p95/p99.

Measures end-to-end per-message latency:
  publish timestamp embedded in body → time.monotonic() inside handler

This captures: AMQP publish + broker routing + delivery + pipeline overhead.
Consumer is started before publishing so queue wait is minimal.

The first WARMUP_N messages are discarded to exclude JIT / connection
warm-up effects from the reported percentiles.

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_latency
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_latency --url amqp://guest:guest@localhost/
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from typing import Any

from benchmarks._common import percentiles

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

N = 3_000
PREFETCH = 50
WARMUP_N = 100  # messages consumed but not counted in reported percentiles

# Pydantic model at module level — required because `from __future__ import annotations`
# makes all annotations lazy strings; models defined inside functions cannot be
# resolved via get_type_hints() in the subscriber DI layer.
try:
    from pydantic import BaseModel as _BaseModel

    class _LatencyPayload(_BaseModel):
        ts: float
        id: int

    _PYDANTIC_AVAILABLE = True
except ImportError:
    _PYDANTIC_AVAILABLE = False

try:
    import msgspec as _msgspec_mod

    class _LatencyStruct(_msgspec_mod.Struct):
        ts: float
        id: int

    _MSGSPEC_AVAILABLE = True
except ImportError:
    _MSGSPEC_AVAILABLE = False


async def _measure_latency(
    url: str,
    queue: str,
    n: int,
    prefetch: int,
    subscriber_kwargs: dict[str, Any],
) -> list[float]:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))
    total = n + WARMUP_N
    all_latencies: list[float] = []
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch, **subscriber_kwargs)
    async def handle(body: bytes) -> None:
        data = json.loads(body)
        lat = (time.monotonic() - data["ts"]) * 1000
        all_latencies.append(lat)
        if len(all_latencies) >= total:
            done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    for i in range(total):
        body = json.dumps({"ts": time.monotonic(), "id": i}).encode()
        await broker.publish(routing_key=queue, body=body)

    await asyncio.wait_for(done.wait(), timeout=120)
    await broker.stop()
    return all_latencies[WARMUP_N:]  # discard warmup window


async def _measure_pydantic_latency(url: str, queue: str, n: int, prefetch: int) -> list[float]:
    if not _PYDANTIC_AVAILABLE:
        return []

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig
    from rabbitkit.serialization.json import JSONSerializer

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)), serializer=JSONSerializer())
    total = n + WARMUP_N
    all_latencies: list[float] = []
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: _LatencyPayload) -> None:
        lat = (time.monotonic() - body.ts) * 1000
        all_latencies.append(lat)
        if len(all_latencies) >= total:
            done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    for i in range(total):
        body_bytes = json.dumps({"ts": time.monotonic(), "id": i}).encode()
        await broker.publish(routing_key=queue, body=body_bytes)

    await asyncio.wait_for(done.wait(), timeout=120)
    await broker.stop()
    return all_latencies[WARMUP_N:]


async def _measure_msgspec_latency(url: str, queue: str, n: int, prefetch: int) -> list[float]:
    if not _MSGSPEC_AVAILABLE:
        return []

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig
    from rabbitkit.serialization.msgspec import MsgspecSerializer

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)), serializer=MsgspecSerializer())
    total = n + WARMUP_N
    all_latencies: list[float] = []
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: _LatencyStruct) -> None:
        lat = (time.monotonic() - body.ts) * 1000
        all_latencies.append(lat)
        if len(all_latencies) >= total:
            done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    for i in range(total):
        body_bytes = json.dumps({"ts": time.monotonic(), "id": i}).encode()
        await broker.publish(routing_key=queue, body=body_bytes)

    await asyncio.wait_for(done.wait(), timeout=120)
    await broker.stop()
    return all_latencies[WARMUP_N:]


def run_all(url: str) -> dict[str, float]:
    from rabbitkit.core.config import RetryConfig

    print("=" * 72)
    print("rabbitkit Latency Benchmarks (real RabbitMQ, ms per message)")
    print(f"  {N:,} samples · prefetch={PREFETCH} · warmup={WARMUP_N} discarded")
    print("=" * 72)
    print(f"  {'Scenario':<40}  {'p50':>7}  {'p95':>7}  {'p99':>7}  {'max':>7}")
    print(f"  {'-'*40}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*7}")

    scenarios: list[tuple[str, Any]] = [
        ("Raw (bytes handler)", lambda: asyncio.run(
            _measure_latency(url, "lat-raw", N, PREFETCH, {})
        )),
        ("JSON deserialization", lambda: asyncio.run(
            _measure_latency(url, "lat-json", N, PREFETCH, {})
        )),
        ("Pydantic model", lambda: asyncio.run(
            _measure_pydantic_latency(url, "lat-pydantic", N, PREFETCH)
        )),
        ("msgspec Struct", lambda: asyncio.run(
            _measure_msgspec_latency(url, "lat-msgspec", N, PREFETCH)
        )),
        ("Retry middleware (success path)", lambda: asyncio.run(
            _measure_latency(url, "lat-retry", N, PREFETCH, {
                "retry": RetryConfig(max_retries=3, delays=(5, 30, 120))
            })
        )),
    ]

    results: dict[str, float] = {}
    for name, fn in scenarios:
        try:
            samples = fn()
            if not samples:
                print(f"  {name:<40}  SKIP (missing dep)")
                continue
            p50, p95, p99, _mn, mx = percentiles(samples)
            print(f"  {name:<40}  {p50:>6.1f}ms  {p95:>6.1f}ms  {p99:>6.1f}ms  {mx:>6.1f}ms")
            key = name.lower().replace(" ", "_").replace("(", "").replace(")", "")
            results[f"lat_{key}_p50_ms"] = p50
            results[f"lat_{key}_p99_ms"] = p99
        except Exception as e:
            print(f"  {name:<40}  ERROR: {e}")

    print("=" * 72)
    print("Latency = AMQP publish + broker routing + delivery + pipeline overhead.")
    print(f"Warmup: first {WARMUP_N} samples discarded to exclude connection warm-up.")
    print("=" * 72)
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
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
