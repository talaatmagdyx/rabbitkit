"""rabbitkit overhead vs raw aio-pika — the toolkit's headline cost number.

Measures the SAME work twice — once through bare aio-pika, once through
rabbitkit — and reports the delta. Reps are INTERLEAVED (raw, kit, raw,
kit, …) rather than batched so slow drift on a shared runner (thermal,
noisy neighbor, broker page-cache warmup) biases both sides equally
instead of whichever ran last.

Scenarios (fresh queue per rep — no cross-rep state):
- Consume drain: N preloaded messages, prefetch P, manual ack (raw) vs
  the full rabbitkit pipeline (middleware chain, DI resolution, AUTO ack).
- Confirmed publish: N sequential confirmed publishes on one channel.

Run standalone:
    python -m benchmarks.bench_overhead --url amqp://guest:guest@localhost/
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from benchmarks._common import _bench_safety, preload_proc
from benchmarks._stats import fmt_rate, robust

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

N_CONSUME = 5_000
N_PUBLISH = 2_000
PREFETCH = 200
REPS = 5
BODY = b'{"id":1}'


# ── Raw aio-pika sides ─────────────────────────────────────────────────────


async def _raw_consume(url: str, queue: str, n: int) -> float:
    import aio_pika

    conn = await aio_pika.connect_robust(url)
    ch = await conn.channel()
    await ch.set_qos(prefetch_count=PREFETCH)
    q = await ch.declare_queue(queue, durable=True)

    count = 0
    span: dict[str, float] = {}
    done = asyncio.Event()

    async def _cb(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        await message.ack()
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    await q.consume(_cb)
    await asyncio.wait_for(done.wait(), timeout=300)
    await conn.close()
    return n / (span["end"] - span["start"])


async def _raw_publish(url: str, queue: str, n: int) -> float:
    import aio_pika

    conn = await aio_pika.connect_robust(url)
    ch = await conn.channel()  # publisher confirms are on by default in aio-pika
    await ch.declare_queue(queue, durable=True)

    msg = aio_pika.Message(body=BODY)
    t0 = time.monotonic()
    for _ in range(n):
        await ch.default_exchange.publish(msg, routing_key=queue)
    elapsed = time.monotonic() - t0
    await conn.close()
    return n / elapsed


# ── rabbitkit sides ────────────────────────────────────────────────────────


async def _kit_consume(url: str, queue: str, n: int) -> float:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    broker = AsyncBroker(
        RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url))
    )
    count = 0
    span: dict[str, float] = {}
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=PREFETCH)
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


async def _kit_publish(url: str, queue: str, n: int) -> float:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    broker = AsyncBroker(
        RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url))
    )
    await broker.start()
    # match the raw side: same queue exists, confirmed publish per message
    t0 = time.monotonic()
    for _ in range(n):
        await broker.publish(routing_key=queue, body=BODY)
    elapsed = time.monotonic() - t0
    await broker.stop()
    return n / elapsed


# ── Interleaved A/B runner ─────────────────────────────────────────────────


def _overhead_pct(raw_median: float, kit_median: float) -> float:
    """Positive = rabbitkit is slower than raw by this fraction."""
    if raw_median <= 0:
        return 0.0
    return (raw_median - kit_median) / raw_median * 100


def run_all(url: str, reps: int = REPS) -> dict[str, float]:
    results: dict[str, float] = {}

    print("=" * 78)
    print("rabbitkit Overhead Benchmarks (vs raw aio-pika, interleaved A/B)")
    print(f"  consume: {N_CONSUME:,} msgs · prefetch={PREFETCH} · publish: "
          f"{N_PUBLISH:,} confirmed · reps={reps} (median ± CV)")
    print("=" * 78)

    matchups: list[tuple[str, Any, Any, str, int]] = [
        ("Consume drain", _raw_consume, _kit_consume, "ovh-consume", N_CONSUME),
        ("Confirmed publish (sequential)", _raw_publish, _kit_publish, "ovh-publish", N_PUBLISH),
    ]

    for name, raw_fn, kit_fn, qprefix, n in matchups:
        raw_rates: list[float] = []
        kit_rates: list[float] = []
        for rep in range(reps):
            # fresh queues per rep: no leftover consumers/state
            for side, fn, rates in (
                ("raw", raw_fn, raw_rates),
                ("kit", kit_fn, kit_rates),
            ):
                queue = f"{qprefix}-{side}-{rep}"
                if fn in (_raw_consume, _kit_consume):
                    preload_proc(url, queue, n, BODY)
                try:
                    rates.append(asyncio.run(fn(url, queue, n)))
                except Exception as e:  # keep the A/B pairing intact
                    print(f"  {name} [{side} rep {rep}] ERROR: {e}")

        raw_s, kit_s = robust(raw_rates), robust(kit_rates)
        ovh = _overhead_pct(raw_s["median"], kit_s["median"])
        print(f"\n  {name}:")
        print(f"    raw aio-pika   {fmt_rate(raw_s)}  msg/s")
        print(f"    rabbitkit      {fmt_rate(kit_s)}  msg/s")
        print(f"    overhead       {ovh:+.1f}%  (positive = rabbitkit slower)")
        key = qprefix.replace("-", "_")
        results[f"{key}_raw_msg_s"] = raw_s["median"]
        results[f"{key}_kit_msg_s"] = kit_s["median"]
        results[f"{key}_overhead_pct"] = ovh

    print("=" * 78)
    return results


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
    parser.add_argument("--reps", type=int, default=REPS)
    args = parser.parse_args()

    if args.url:
        run_all(args.url, reps=args.reps)
    else:
        from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

        from benchmarks._common import IMAGE, make_url

        with RabbitMqContainer(IMAGE) as c:
            run_all(make_url(c), reps=args.reps)


if __name__ == "__main__":
    main()
