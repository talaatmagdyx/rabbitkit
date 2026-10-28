"""Reliability-profile benchmark: the workload the messaging foundation targets.

Every message here is what a reliable workload publishes: ``size`` bytes
(1 KiB default), persistent, publisher confirms on, ``mandatory=True``, into a
durable **quorum** queue (classic with ``--queue-type classic``), consumed
with manual acks after the handler (AUTO policy).

Scenarios
---------

* ``publish``: publisher-only throughput (``publish_many``), with confirm
  accounting (every input must end CONFIRMED).
* ``consume``: consumer-only throughput from a preloaded queue.
* ``sustained``: open-loop end-to-end at ``--rate`` msg/s for ``--seconds``.
  Publishers send on an absolute schedule (``t0 + i/rate``) and latency is
  measured from that *intended* send time, so publisher lag and broker queueing
  are charged to the system (no coordinated omission). Default 2,778 msg/s,
  the 10M events/hour design target.
* ``burst``: the same at ``--burst-rate`` (default 20,000 msg/s) for
  ``--burst-seconds``; reports the rate actually achieved, the peak backlog and
  the drain time.

Publishers and consumers run in separate processes (``--publishers`` /
``--consumers``), as separately deployed workloads would; the orchestrator
samples queue depth. Every run accounts for every message: lost (never
consumed), duplicated (consumed twice) and non-CONFIRMED publishes are reported,
never dropped from the numbers.

Caveats printed with every report: a single-node broker (quorum replication
factor 1, so no Raft network round trip) on the same machine as the clients.

Usage::

    python -m benchmarks.bench_foundation                        # all scenarios, throwaway broker
    python -m benchmarks.bench_foundation --scenario sustained --seconds 120
    python -m benchmarks.bench_foundation --url amqp://... --json out.json
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import multiprocessing as mp
import os
import platform
import resource
import statistics
import struct
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from benchmarks._common import _bench_safety

_HEADER = struct.Struct("!Qd")  # message index, intended send time (epoch seconds)
_DEPTH_SAMPLE_S = 0.25


def _rss_mib() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024  # bytes on macOS, KiB on Linux


def _pct(samples: list[float], p: float) -> float:
    if not samples:
        return float("nan")
    ordered = sorted(samples)
    k = min(len(ordered) - 1, max(0, round(p / 100 * (len(ordered) - 1))))
    return ordered[k]


def _body(index: int, due: float, size: int) -> bytes:
    head = _HEADER.pack(index, due)
    return head + b"x" * max(0, size - len(head))


def _config(url: str, *, pool: int = 10, prefetch: int = 10) -> Any:
    from rabbitkit.core.config import ConnectionConfig, ConsumerConfig, PoolConfig, PublisherConfig, RabbitConfig

    return RabbitConfig(
        connection=ConnectionConfig.from_url(url),
        pool=PoolConfig(channel_pool_size=pool),
        publisher=PublisherConfig(confirm_delivery=True, persistent=True, mandatory=True),
        consumer=ConsumerConfig(prefetch_count=prefetch),
        safety=_bench_safety(),
    )


def _queue(name: str, queue_type: str) -> Any:
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import QueueType

    return RabbitQueue(name=name, durable=True, queue_type=QueueType(queue_type))


# ── worker processes ───────────────────────────────────────────────────────


@dataclass
class PublisherStats:
    sent: int = 0
    statuses: dict[str, int] = field(default_factory=dict)
    schedule_lag_ms: list[float] = field(default_factory=list)
    confirm_ms: list[float] = field(default_factory=list)
    cpu_s: float = 0.0
    rss_mib: float = 0.0
    last_sent_at: float = 0.0


@dataclass
class ConsumerStats:
    indices: list[int] = field(default_factory=list)
    latency_ms: list[float] = field(default_factory=list)
    last_receive: float = 0.0
    cpu_s: float = 0.0
    rss_mib: float = 0.0


def _publisher_proc(
    url: str, queue: str, indices: range, rate: float, t0: float, size: int, max_in_flight: int, out: Any
) -> None:
    out.put(asdict(asyncio.run(_publish_paced(url, queue, indices, rate, t0, size, max_in_flight))))


async def _publish_paced(
    url: str, queue: str, indices: range, rate: float, t0: float, size: int, max_in_flight: int
) -> PublisherStats:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.types import MessageEnvelope

    stats = PublisherStats()
    broker = AsyncBroker(_config(url, pool=max_in_flight))
    await broker.start(install_signal_handlers=False)
    cpu0 = time.process_time()
    slots = asyncio.Semaphore(max_in_flight)
    tasks: set[asyncio.Task[None]] = set()

    async def one(index: int, due: float) -> None:
        started = time.time()
        try:
            outcome = await broker.publish(
                MessageEnvelope(routing_key=queue, body=_body(index, due, size), mandatory=True)
            )
            status = outcome.status.value
            if status == "confirmed":
                stats.confirm_ms.append((time.time() - started) * 1000)
        except Exception as exc:  # benchmark accounting: every input gets a recorded state
            status = f"raised:{type(exc).__name__}"
        finally:
            slots.release()
        stats.statuses[status] = stats.statuses.get(status, 0) + 1

    for index in indices:
        due = t0 + index / rate
        delay = due - time.time()
        if delay > 0:
            await asyncio.sleep(delay)
        await slots.acquire()  # bounded in-flight: if saturated, the schedule lag shows it
        stats.schedule_lag_ms.append(max(0.0, time.time() - due) * 1000)
        task = asyncio.create_task(one(index, due))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        stats.sent += 1
        stats.last_sent_at = time.time()
    if tasks:
        await asyncio.gather(*tasks)
    stats.cpu_s = time.process_time() - cpu0
    stats.rss_mib = _rss_mib()
    await broker.stop()
    return stats


def _consumer_proc(
    url: str, queue: str, queue_type: str, prefetch: int, expected: int, idle_s: float, ready: Any, out: Any
) -> None:
    out.put(asdict(asyncio.run(_consume(url, queue, queue_type, prefetch, expected, idle_s, ready))))


async def _consume(
    url: str, queue: str, queue_type: str, prefetch: int, expected: int, idle_s: float, ready: Any
) -> ConsumerStats:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.message import RabbitMessage

    stats = ConsumerStats()
    broker = AsyncBroker(_config(url, prefetch=prefetch))
    last = [time.monotonic()]

    @broker.subscriber(_queue(queue, queue_type), prefetch_count=prefetch)
    async def handle(message: RabbitMessage) -> None:
        now = time.time()
        index, due = _HEADER.unpack_from(message.body)
        stats.indices.append(index)
        stats.latency_ms.append((now - due) * 1000)
        stats.last_receive = now
        last[0] = time.monotonic()

    await broker.start(install_signal_handlers=False)
    cpu0 = time.process_time()
    ready.set()
    # Stop once this consumer has been idle for idle_s after the expected total
    # has (collectively) been seen, or the hard idle limit passes.
    while time.monotonic() - last[0] < idle_s or (expected and len(stats.indices) == 0):
        await asyncio.sleep(0.2)
    stats.cpu_s = time.process_time() - cpu0
    stats.rss_mib = _rss_mib()
    await broker.stop()
    return stats


# ── orchestration ──────────────────────────────────────────────────────────


async def _declare(url: str, queue: str, queue_type: str) -> None:
    from rabbitkit.async_.broker import AsyncBroker

    broker = AsyncBroker(_config(url))
    await broker.start(install_signal_handlers=False)
    await broker.declare_queue(_queue(queue, queue_type))
    await broker.stop()


async def _depth_sampler(url: str, queue: str, stop: asyncio.Event, samples: list[tuple[float, int]]) -> None:
    import aio_pika

    conn = await aio_pika.connect(url)
    async with conn:
        channel = await conn.channel()
        while not stop.is_set():
            with contextlib.suppress(Exception):
                declared = await channel.declare_queue(queue, passive=True)
                samples.append((time.time(), declared.declaration_result.message_count or 0))
            await asyncio.sleep(_DEPTH_SAMPLE_S)


def _sample_depth_in_thread(url: str, queue: str, stop_flag: Any, samples: list[tuple[float, int]]) -> None:
    async def run() -> None:
        stop = asyncio.Event()

        async def watch() -> None:
            while not stop_flag.is_set():
                await asyncio.sleep(0.05)
            stop.set()

        await asyncio.gather(_depth_sampler(url, queue, stop, samples), watch())

    asyncio.run(run())


def end_to_end(
    url: str,
    *,
    rate: float,
    seconds: float,
    size: int,
    queue_type: str,
    publishers: int,
    consumers: int,
    prefetch: int,
    max_in_flight: int,
) -> dict[str, Any]:
    import threading

    n = int(rate * seconds)
    queue = f"bench-foundation-{uuid.uuid4().hex[:8]}"
    asyncio.run(_declare(url, queue, queue_type))
    ctx = mp.get_context("spawn")
    results: Any = ctx.Queue()

    consumer_procs = []
    for _ in range(consumers):
        ready = ctx.Event()
        proc = ctx.Process(
            target=_consumer_proc, args=(url, queue, queue_type, prefetch, n, 5.0, ready, results), daemon=True
        )
        proc.start()
        if not ready.wait(60):
            raise RuntimeError("consumer did not start")
        consumer_procs.append(proc)

    t0 = time.time() + 2.0  # every publisher process shares the same schedule origin
    publisher_procs = []
    for p in range(publishers):
        indices = range(p, n, publishers)  # interleaved: together they offer `rate`
        proc = ctx.Process(
            target=_publisher_proc,
            args=(url, queue, indices, rate, t0, size, max_in_flight, results),
            daemon=True,
        )
        proc.start()
        publisher_procs.append(proc)

    depth: list[tuple[float, int]] = []
    stop_flag = threading.Event()
    sampler = threading.Thread(target=_sample_depth_in_thread, args=(url, queue, stop_flag, depth), daemon=True)
    sampler.start()

    collected = [results.get(timeout=seconds + 600) for _ in range(publishers + consumers)]
    for proc in publisher_procs + consumer_procs:
        proc.join(timeout=30)
    stop_flag.set()
    sampler.join(timeout=10)

    pubs = [PublisherStats(**c) for c in collected if "sent" in c]
    cons = [ConsumerStats(**c) for c in collected if "indices" in c]
    statuses: dict[str, int] = {}
    for pub in pubs:
        for key, count in pub.statuses.items():
            statuses[key] = statuses.get(key, 0) + count
    received = [i for c in cons for i in c.indices]
    unique = set(received)
    latency = [x for c in cons for x in c.latency_ms]
    lag = [x for p in pubs for x in p.schedule_lag_ms]
    confirm = [x for p in pubs for x in p.confirm_ms]
    last_due = t0 + (n - 1) / rate
    last_receive = max((c.last_receive for c in cons), default=float("nan"))
    # Measured: from the shared schedule origin to the last frame any publisher handed off.
    publish_window = max(max((p.last_sent_at for p in pubs), default=t0) - t0, 1e-9)
    return {
        "offered_rate": rate,
        "seconds": seconds,
        "messages": n,
        "publish_statuses": statuses,
        "confirmed": statuses.get("confirmed", 0),
        "consumed_unique": len(unique),
        "lost": n - len(unique),
        "duplicates": len(received) - len(unique),
        "achieved_publish_rate": n / publish_window,
        "end_to_end_ms": {p: _pct(latency, p) for p in (50, 95, 99, 99.9)}
        | {"max": max(latency, default=float("nan"))},
        "confirm_ms": {p: _pct(confirm, p) for p in (50, 95, 99)},
        "publisher_schedule_lag_ms": {p: _pct(lag, p) for p in (50, 99)} | {"max": max(lag, default=float("nan"))},
        "peak_backlog": max((d for _, d in depth), default=0),
        "drain_after_last_send_s": last_receive - last_due,
        "client_cpu_us_per_msg": {
            "publish": 1e6 * sum(p.cpu_s for p in pubs) / max(n, 1),
            "consume": 1e6 * sum(c.cpu_s for c in cons) / max(n, 1),
        },
        "peak_rss_mib": {
            "publisher_max": max((p.rss_mib for p in pubs), default=0.0),
            "consumer_max": max((c.rss_mib for c in cons), default=0.0),
        },
        "publishers": publishers,
        "consumers": consumers,
        "prefetch": prefetch,
        "max_in_flight_per_publisher": max_in_flight,
    }


async def publish_only(url: str, *, n: int, size: int, queue_type: str, max_in_flight: int) -> dict[str, Any]:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.bulk import BulkPublishOptions
    from rabbitkit.core.types import MessageEnvelope

    queue = f"bench-foundation-pub-{uuid.uuid4().hex[:8]}"
    await _declare(url, queue, queue_type)
    broker = AsyncBroker(_config(url, pool=max_in_flight))
    await broker.start(install_signal_handlers=False)
    envelopes = [MessageEnvelope(routing_key=queue, body=_body(i, 0.0, size), mandatory=True) for i in range(n)]
    cpu0 = time.process_time()
    started = time.perf_counter()
    result = await broker.publish_many(envelopes, BulkPublishOptions(max_in_flight=max_in_flight))
    elapsed = time.perf_counter() - started
    cpu = time.process_time() - cpu0
    counts = {status.value: count for status, count in result.counts.items() if count}
    await broker.stop()
    return {
        "messages": n,
        "msg_s": n / elapsed,
        "mb_s": n * size / elapsed / 1e6,
        "statuses": counts,
        "client_cpu_us_per_msg": 1e6 * cpu / n,
        "max_in_flight": max_in_flight,
    }


async def consume_only(url: str, *, n: int, size: int, queue_type: str, prefetch: int) -> dict[str, Any]:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.bulk import BulkPublishOptions
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import MessageEnvelope

    queue = f"bench-foundation-con-{uuid.uuid4().hex[:8]}"
    await _declare(url, queue, queue_type)
    loader = AsyncBroker(_config(url, pool=128))
    await loader.start(install_signal_handlers=False)
    envelopes = [MessageEnvelope(routing_key=queue, body=_body(i, 0.0, size), mandatory=True) for i in range(n)]
    preload = await loader.publish_many(envelopes, BulkPublishOptions(max_in_flight=128))
    await loader.stop()
    preloaded = sum(1 for item in preload.items if item.status.value == "confirmed")

    seen: set[int] = set()
    done = asyncio.Event()
    broker = AsyncBroker(_config(url, prefetch=prefetch))

    @broker.subscriber(_queue(queue, queue_type), prefetch_count=prefetch)
    async def handle(message: RabbitMessage) -> None:
        index, _ = _HEADER.unpack_from(message.body)
        seen.add(index)
        if len(seen) >= preloaded:
            done.set()

    cpu0 = time.process_time()
    started = time.perf_counter()
    await broker.start(install_signal_handlers=False)
    await asyncio.wait_for(done.wait(), timeout=600)
    elapsed = time.perf_counter() - started
    cpu = time.process_time() - cpu0
    await broker.stop()
    return {
        "messages": preloaded,
        "msg_s": preloaded / elapsed,
        "client_cpu_us_per_msg": 1e6 * cpu / max(preloaded, 1),
        "prefetch": prefetch,
    }


def _git_revision() -> str:
    import subprocess

    try:
        done = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=False)
    except OSError:
        return "unknown"
    return done.stdout.strip() or "unknown"


def environment(image: str | None, url_given: bool) -> dict[str, Any]:
    import aio_pika

    import rabbitkit

    return {
        "rabbitkit": rabbitkit.__version__,
        "git": _git_revision(),
        "python": platform.python_version(),
        "aio_pika": aio_pika.__version__,
        "platform": f"{platform.system()} {platform.machine()}",
        "cpu_count": os.cpu_count(),
        "broker": "external (--url)" if url_given else image,
        "load_average_at_start": os.getloadavg(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default=None, help="existing broker; omitted → throwaway testcontainers broker")
    parser.add_argument("--image", default="rabbitmq:3.13-management-alpine")
    parser.add_argument("--scenario", choices=["publish", "consume", "sustained", "burst", "all"], default="all")
    parser.add_argument("--queue-type", choices=["quorum", "classic"], default="quorum")
    parser.add_argument("--size", type=int, default=1024)
    parser.add_argument("--n", type=int, default=50_000, help="messages for publish/consume")
    parser.add_argument("--reps", type=int, default=3, help="repetitions for publish/consume")
    parser.add_argument("--rate", type=float, default=2778.0)
    parser.add_argument("--seconds", type=float, default=60.0)
    parser.add_argument("--burst-rate", type=float, default=20_000.0)
    parser.add_argument("--burst-seconds", type=float, default=10.0)
    parser.add_argument("--publishers", type=int, default=1)
    parser.add_argument("--burst-publishers", type=int, default=4)
    parser.add_argument("--consumers", type=int, default=1)
    parser.add_argument("--burst-consumers", type=int, default=2)
    parser.add_argument("--prefetch", type=int, default=256)
    parser.add_argument("--max-in-flight", type=int, default=256)
    parser.add_argument("--json", default=None, help="write the report to this path")
    args = parser.parse_args()

    import logging

    for name in ("rabbitkit", "aio_pika", "aiormq"):
        logging.getLogger(name).setLevel(logging.CRITICAL)

    container = None
    url = args.url
    if url is None:
        from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

        container = RabbitMqContainer(args.image).with_exposed_ports(15672)
        container.start()
        url = f"amqp://guest:guest@{container.get_container_host_ip()}:{container.get_exposed_port(5672)}/"
    report: dict[str, Any] = {"environment": environment(args.image, args.url is not None), "args": vars(args)}
    run = {args.scenario} if args.scenario != "all" else {"publish", "consume", "sustained", "burst"}
    try:
        if "publish" in run:
            reps = [
                asyncio.run(
                    publish_only(
                        url, n=args.n, size=args.size, queue_type=args.queue_type, max_in_flight=args.max_in_flight
                    )
                )
                for _ in range(args.reps)
            ]
            rates = [r["msg_s"] for r in reps]
            report["publish"] = {"median_msg_s": statistics.median(rates), "runs": reps}
            print(
                f"publish  median {statistics.median(rates):,.0f} msg/s  "
                f"runs={[round(r) for r in rates]}  statuses={reps[-1]['statuses']}"
            )
        if "consume" in run:
            reps = [
                asyncio.run(
                    consume_only(url, n=args.n, size=args.size, queue_type=args.queue_type, prefetch=args.prefetch)
                )
                for _ in range(args.reps)
            ]
            rates = [r["msg_s"] for r in reps]
            report["consume"] = {"median_msg_s": statistics.median(rates), "runs": reps}
            print(f"consume  median {statistics.median(rates):,.0f} msg/s  runs={[round(r) for r in rates]}")
        for name, rate, seconds, pubs, cons in (
            ("sustained", args.rate, args.seconds, args.publishers, args.consumers),
            ("burst", args.burst_rate, args.burst_seconds, args.burst_publishers, args.burst_consumers),
        ):
            if name not in run:
                continue
            res = end_to_end(
                url,
                rate=rate,
                seconds=seconds,
                size=args.size,
                queue_type=args.queue_type,
                publishers=pubs,
                consumers=cons,
                prefetch=args.prefetch,
                max_in_flight=args.max_in_flight,
            )
            report[name] = res
            e2e = res["end_to_end_ms"]
            print(
                f"{name:<9} offered {rate:,.0f}/s x {seconds:g}s  achieved {res['achieved_publish_rate']:,.0f}/s  "
                f"confirmed {res['confirmed']}/{res['messages']}  lost {res['lost']}  dup {res['duplicates']}  "
                f"e2e p50/p95/p99 {e2e[50]:.1f}/{e2e[95]:.1f}/{e2e[99]:.1f} ms  "
                f"peak backlog {res['peak_backlog']:,}  drain {res['drain_after_last_send_s']:.2f}s"
            )
    finally:
        if container is not None:
            container.stop()
    print(
        "caveat: single-node broker (quorum replication factor 1, no Raft network hop) on the same host as the "
        "clients; absolute numbers are this machine's."
    )
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(report, fh, indent=2, default=str)


if __name__ == "__main__":
    main()
