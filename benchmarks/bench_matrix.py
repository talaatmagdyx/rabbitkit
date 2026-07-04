"""Dimension sweeps + open-loop paced latency — what the classic suite hides.

Three measurements the flat single-point suite cannot make:

1. **Payload-size sweep** — at the classic suite's 9-byte bodies every
   serializer scenario collapses into pure AMQP round-trip time; the thing
   being varied is invisible under the thing being measured. Sweeping
   100 B → 64 KB shows where serialization/framing actually starts to cost.

2. **Queue-type A/B (classic vs quorum)** — the production checklist
   mandates quorum queues; benchmarking only classic measures a topology
   the docs tell users not to run.

3. **Open-loop paced latency** — the classic latency bench publishes
   self-clocked on the consumer's own event loop, which (a) self-interferes
   and (b) hides queueing delay (coordinated omission: a stalled system
   pauses its own load generator). Here a paced publisher sends on an
   ABSOLUTE schedule (t0 + i/rate) and latency is measured from the
   *intended* send time, so scheduler lag and queue buildup are charged to
   the system under test, not silently forgiven.

Run standalone:
    python -m benchmarks.bench_matrix --url amqp://guest:guest@localhost/
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from benchmarks._common import _bench_safety, preload_proc
from benchmarks._stats import percentiles

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

SWEEP_N = 3_000
PREFETCH = 200
PAYLOAD_SIZES = (100, 4_096, 65_536)  # bytes
PACED_RATE = 400  # msg/s — open-loop; well under the ~5k ceiling
PACED_SECONDS = 15


def _payload(size: int) -> bytes:
    return b'{"pad":"' + b"x" * max(0, size - 12) + b'"}'


async def _drain_kit(url: str, queue: str, n: int, queue_obj: Any = None) -> float:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    broker = AsyncBroker(
        RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url))
    )
    count = 0
    span: dict[str, float] = {}
    done = asyncio.Event()

    @broker.subscriber(queue=queue_obj if queue_obj is not None else queue, prefetch_count=PREFETCH)
    async def handle(body: bytes) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    await broker.start()
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()
    return n / (span["end"] - span["start"])


async def _publish_kit(url: str, queue: str, n: int, body: bytes, concurrency: int = 64) -> float:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, PoolConfig, RabbitConfig

    broker = AsyncBroker(
        RabbitConfig(
            safety=_bench_safety(),
            connection=ConnectionConfig.from_url(url),
            pool=PoolConfig(channel_pool_size=concurrency),
        )
    )
    await broker.start()
    sem = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with sem:
            await broker.publish(routing_key=queue, body=body)

    t0 = time.monotonic()
    await asyncio.gather(*(one() for _ in range(n)))
    elapsed = time.monotonic() - t0
    await broker.stop()
    return n / elapsed


# ── 1. Payload-size sweep ──────────────────────────────────────────────────


def sweep_payloads(url: str, results: dict[str, float]) -> None:
    print("\n  Payload-size sweep (consume drain + concurrent confirmed publish):")
    print(f"  {'Size':>8}  {'consume msg/s':>14}  {'consume MB/s':>13}  {'publish msg/s':>14}")
    for size in PAYLOAD_SIZES:
        body = _payload(size)
        cq, pq = f"mx-c-{size}", f"mx-p-{size}"
        preload_proc(url, cq, SWEEP_N, body)
        try:
            crate = asyncio.run(_drain_kit(url, cq, SWEEP_N))
            prate = asyncio.run(_publish_kit(url, pq, SWEEP_N, body))
            mbs = crate * size / 1e6
            print(f"  {size:>7,}B  {crate:>14,.0f}  {mbs:>12.1f}  {prate:>14,.0f}")
            results[f"matrix_consume_{size}b_msg_s"] = crate
            results[f"matrix_publish_{size}b_msg_s"] = prate
        except Exception as e:
            print(f"  {size:>7,}B  ERROR: {e}")


# ── 2. Classic vs quorum queue ─────────────────────────────────────────────


def sweep_queue_types(url: str, results: dict[str, float]) -> None:
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import QueueType

    print("\n  Queue type A/B (consume drain, durable, same payload):")
    print(f"  {'Type':>8}  {'msg/s':>10}")
    body = _payload(100)
    for label, qtype in (("classic", QueueType.CLASSIC), ("quorum", QueueType.QUORUM)):
        queue = f"mx-qt-{label}"
        qobj = RabbitQueue(name=queue, durable=True, queue_type=qtype)
        try:
            # broker declares the queue with the right type FIRST (a quorum
            # queue cannot be redeclared over a preloaded classic one)
            asyncio.run(_declare_only(url, qobj))
            preload_proc(url, queue, SWEEP_N, body, passive=True)
            rate = asyncio.run(_drain_kit(url, queue, SWEEP_N, queue_obj=qobj))
            print(f"  {label:>8}  {rate:>10,.0f}")
            results[f"matrix_{label}_consume_msg_s"] = rate
        except Exception as e:
            print(f"  {label:>8}  ERROR: {e}")


async def _declare_only(url: str, qobj: Any) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    broker = AsyncBroker(
        RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url))
    )

    @broker.subscriber(queue=qobj, prefetch_count=PREFETCH)
    async def _noop(body: bytes) -> None:
        pass

    await broker.start()
    await broker.stop()


# ── 3. Open-loop paced latency ─────────────────────────────────────────────


async def _paced_latency(url: str, queue: str, rate: int, seconds: int) -> dict[str, Any]:
    import json

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    broker = AsyncBroker(
        RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url))
    )
    n = rate * seconds
    latencies: list[float] = []
    send_lag: list[float] = []
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=PREFETCH)
    async def handle(body: bytes) -> None:
        data = json.loads(body)
        # measured from the INTENDED send instant — queueing/scheduler lag is
        # charged to the system, not forgiven (no coordinated omission)
        latencies.append((time.monotonic() - data["due"]) * 1000)
        if len(latencies) >= n:
            done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    async def publisher() -> None:
        t0 = time.monotonic() + 0.05
        for i in range(n):
            due = t0 + i / rate
            now = time.monotonic()
            if due > now:
                await asyncio.sleep(due - now)
            send_lag.append((time.monotonic() - due) * 1000)
            await broker.publish(
                routing_key=queue,
                body=json.dumps({"due": due, "id": i}).encode(),
            )

    await publisher()
    await asyncio.wait_for(done.wait(), timeout=seconds * 4 + 30)
    await broker.stop()
    return {"latencies": latencies, "send_lag": send_lag}


def paced_latency(url: str, results: dict[str, float]) -> None:
    n = PACED_RATE * PACED_SECONDS
    print(f"\n  Open-loop paced latency ({PACED_RATE} msg/s x {PACED_SECONDS}s = {n:,} samples):")
    queue = "mx-paced"
    try:
        out = asyncio.run(_paced_latency(url, queue, PACED_RATE, PACED_SECONDS))
        pts = percentiles(out["latencies"])
        lagp = percentiles(out["send_lag"], points=(99,))
        cols = "  ".join(f"{k}={v:.1f}ms" for k, v in pts.items())
        print(f"    e2e   {cols}")
        print(f"    publisher schedule lag p99={lagp.get('p99', 0.0):.2f}ms "
              "(high lag → publisher couldn't hold the pace; latencies above include it)")
        for k, v in pts.items():
            results[f"matrix_paced_{k.replace('.', '_')}_ms"] = v
    except Exception as e:
        print(f"    ERROR: {e}")


def run_all(url: str) -> dict[str, float]:
    results: dict[str, float] = {}
    print("=" * 78)
    print("rabbitkit Matrix Benchmarks (payload sweep · queue types · paced latency)")
    print("=" * 78)
    sweep_payloads(url, results)
    sweep_queue_types(url, results)
    paced_latency(url, results)
    print("=" * 78)
    return results


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
    args = parser.parse_args()

    if args.url:
        run_all(args.url)
    else:
        from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

        from benchmarks._common import IMAGE, make_url

        with RabbitMqContainer(IMAGE) as c:
            run_all(make_url(c))


if __name__ == "__main__":
    main()
