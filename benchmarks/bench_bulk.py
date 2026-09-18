"""Bulk vs single — publish and ack, measured against a real broker.

Answers "what do I actually gain from ``publish_many`` / ``ack_many`` /
``CoalescingAcker`` over one call per message?" with numbers from THIS
machine, plus the correctness accounting that must accompany any throughput
figure (every input has a reported state; the queue really drains).

    python -m benchmarks.bench_bulk                          # localhost:5672
    python -m benchmarks.bench_bulk --url amqp://... --n 5000 --size 1024 --reps 3

Scenarios (N messages of ``--size`` bytes, persistent, confirms on,
``mandatory=True``, durable classic queue):

Publish
  single/async        ``await broker.publish(e)`` one after another
  gather/async x10    hand-rolled ``asyncio.gather`` under a 10-slot semaphore (no accounting)
  publish_many x10    default options → effective in-flight = channel pool (10)
  publish_many x64    ``PoolConfig(channel_pool_size=64)`` + ``max_in_flight=64``
  publish_many+batch  ``AsyncBatchPublisher`` configured (pipelined confirms), in-flight 256
  single/sync         ``SyncBroker.publish`` one after another
  publish_many/sync   ``SyncBroker.publish_many`` (sequential by design — same speed, adds accounting)

Ack (consume a preloaded queue with MANUAL handlers, no work in the handler)
  ack each            ``await msg.ack_async()`` per delivery → N frames
  ack_many x100       park 100 deliveries, one ``ack_many`` call → N frames, N/100 API calls
  coalescing x100     ``CoalescingAcker(batch_size=100)`` → far fewer frames (cumulative acks)

Results land in ``benchmarks/results/bulk_<ts>.json`` with an environment
fingerprint. Absolute msg/s is machine-specific; the RATIOS travel.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
import uuid
from pathlib import Path
from typing import Any

from benchmarks._common import _bench_safety
from benchmarks._stats import env_fingerprint

RESULTS_DIR = Path(__file__).parent / "results"


def _envelopes(queue: str, n: int, size: int) -> list[Any]:
    from rabbitkit.core.types import MessageEnvelope

    body = b"x" * size
    return [MessageEnvelope(routing_key=queue, body=body, message_id=f"{queue}-{i}", mandatory=True) for i in range(n)]


def _config(url: str, **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, PublisherConfig, RabbitConfig

    kw.setdefault("publisher", PublisherConfig(mandatory=True))
    kw.setdefault("safety", _bench_safety())
    return RabbitConfig(connection=ConnectionConfig.from_url(url), **kw)


# ── publish scenarios ──────────────────────────────────────────────────────


async def _async_broker(url: str, queue: str, **kw: Any) -> Any:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.topology import RabbitQueue

    batch_config = kw.pop("batch_config", None)
    broker = AsyncBroker(_config(url, **kw), batch_config=batch_config)
    await broker.start()
    await broker._transport.declare_queue(RabbitQueue(name=queue, durable=True))
    return broker


async def pub_single_async(url: str, queue: str, envs: list[Any]) -> dict[str, Any]:
    broker = await _async_broker(url, queue)
    try:
        t0 = time.perf_counter()
        ok = 0
        for e in envs:
            ok += (await broker.publish(e)).ok
        el = time.perf_counter() - t0
        return {"elapsed": el, "confirmed": ok, "accounted": len(envs)}
    finally:
        await broker.stop()


async def pub_gather_async(url: str, queue: str, envs: list[Any], concurrency: int = 10) -> dict[str, Any]:
    broker = await _async_broker(url, queue)
    sem = asyncio.Semaphore(concurrency)

    async def one(e: Any) -> bool:
        async with sem:
            return bool((await broker.publish(e)).ok)

    try:
        t0 = time.perf_counter()
        results = await asyncio.gather(*(one(e) for e in envs))
        el = time.perf_counter() - t0
        return {"elapsed": el, "confirmed": sum(results), "accounted": len(envs)}
    finally:
        await broker.stop()


async def pub_many_async(
    url: str, queue: str, envs: list[Any], *, pool: int, in_flight: int, batch: bool
) -> dict[str, Any]:
    from rabbitkit.core.bulk import BulkPublishOptions
    from rabbitkit.core.config import BatchPublishConfig, PoolConfig

    kw: dict[str, Any] = {"pool": PoolConfig(channel_pool_size=pool)}
    if batch:
        kw["batch_config"] = BatchPublishConfig(batch_size=100, flush_interval_ms=10, max_in_flight=in_flight)
    broker = await _async_broker(url, queue, **kw)
    try:
        t0 = time.perf_counter()
        result = await broker.publish_many(envs, BulkPublishOptions(max_in_flight=in_flight, overall_timeout=600))
        el = time.perf_counter() - t0
        return {
            "elapsed": el,
            "confirmed": len(result.confirmed),
            "accounted": len(result),
            "statuses": {k.value: v for k, v in result.counts.items()},
        }
    finally:
        await broker.stop()


def pub_sync(url: str, queue: str, envs: list[Any], *, bulk: bool) -> dict[str, Any]:
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.sync.broker import SyncBroker

    broker = SyncBroker(_config(url))
    broker.start()
    broker._transport.declare_queue(RabbitQueue(name=queue, durable=True))
    try:
        t0 = time.perf_counter()
        if bulk:
            result = broker.publish_many(envs)
            el = time.perf_counter() - t0
            return {"elapsed": el, "confirmed": len(result.confirmed), "accounted": len(result)}
        ok = 0
        for e in envs:
            ok += broker.publish(e).ok
        el = time.perf_counter() - t0
        return {"elapsed": el, "confirmed": ok, "accounted": len(envs)}
    finally:
        broker.stop()


# ── ack scenarios ──────────────────────────────────────────────────────────


async def _preload(url: str, queue: str, envs: list[Any]) -> None:
    from rabbitkit.core.bulk import BulkPublishOptions
    from rabbitkit.core.config import BatchPublishConfig, PoolConfig

    broker = await _async_broker(
        url,
        queue,
        pool=PoolConfig(channel_pool_size=16),
        batch_config=BatchPublishConfig(batch_size=100, flush_interval_ms=10, max_in_flight=512),
    )
    try:
        (await broker.publish_many(envs, BulkPublishOptions(max_in_flight=512, overall_timeout=600))).raise_for_status()
    finally:
        await broker.stop()


def _mgmt_counts(mgmt_url: str, queue: str) -> tuple[int, int]:
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    info = RabbitManagementClient(ManagementConfig(url=mgmt_url)).get_queue(queue)
    return int(info.get("messages_ready", 0)), int(info.get("messages_unacknowledged", 0))


async def _drained(mgmt_url: str | None, queue: str, timeout: float = 60.0) -> bool | None:
    """Broker-side proof the queue really drained (None = no management URL)."""
    if mgmt_url is None:
        return None
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        try:
            if await loop.run_in_executor(None, _mgmt_counts, mgmt_url, queue) == (0, 0):
                return True
        except Exception:  # management API not ready / stats lag
            pass
        await asyncio.sleep(0.5)
    return False


async def ack_scenario(url: str, mgmt_url: str | None, queue: str, envs: list[Any], mode: str) -> dict[str, Any]:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import BatchAckConfig, ConsumerConfig
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import AckPolicy
    from rabbitkit.highload.batch import CoalescingAcker

    n = len(envs)
    await _preload(url, queue, envs)

    broker = AsyncBroker(_config(url, consumer=ConsumerConfig(prefetch_count=min(n, 65535))))
    done = asyncio.Event()
    settled = 0
    frames = 0
    api_calls = 0
    parked: list[RabbitMessage] = []
    channel: dict[str, Any] = {}
    loop = asyncio.get_running_loop()
    started: list[float] = []

    wire_tasks: set[asyncio.Task[Any]] = set()

    def ack_fn(tag: int, multiple: bool) -> None:
        nonlocal frames
        frames += 1
        task = loop.create_task(channel["ch"].basic_ack(delivery_tag=tag, multiple=multiple))
        wire_tasks.add(task)
        task.add_done_callback(wire_tasks.discard)

    acker = CoalescingAcker(
        ack_fn=ack_fn,
        nack_fn=lambda t, r: None,
        reject_fn=lambda t, r: None,
        config=BatchAckConfig(batch_size=100, flush_interval_ms=0),
    )

    async def flush_parked() -> None:
        nonlocal frames, api_calls, settled
        batch, parked[:] = parked[:], []
        report = await broker.ack_many(batch)
        api_calls += 1
        frames += len(report.dispatched)  # one multiple=False frame per dispatched delivery
        settled += len(report.dispatched)
        if settled >= n:
            done.set()

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: RabbitMessage) -> None:
        nonlocal frames, api_calls, settled
        if not started:
            started.append(time.perf_counter())
        if mode == "each":
            await msg.ack_async()
            frames += 1
            api_calls += 1
            settled += 1
            if settled >= n:
                done.set()
        elif mode == "ack_many":
            parked.append(msg)
            if len(parked) >= 100 or settled + len(parked) >= n:
                await flush_parked()
        else:  # coalescing
            channel.setdefault("ch", msg.raw_message.channel)
            assert msg.delivery_tag is not None
            acker.register(msg.delivery_tag)
            acker.complete(msg.delivery_tag)  # in-order completion → maximal coalescing
            settled += 1
            if settled >= n:
                acker.flush()
                done.set()

    await broker.start()
    try:
        await asyncio.wait_for(done.wait(), timeout=600)
        el = time.perf_counter() - started[0]
        if mode == "coalescing":
            api_calls = frames  # each flush emits its frames directly
        await asyncio.sleep(0.2)  # let the last frames leave the socket
        drained = await _drained(mgmt_url, queue)
        return {"elapsed": el, "settled": settled, "frames": frames, "api_calls": api_calls, "queue_drained": drained}
    finally:
        await broker.stop()


# ── runner ─────────────────────────────────────────────────────────────────


def _rate(res: dict[str, Any], n: int) -> float:
    return n / res["elapsed"] if res["elapsed"] > 0 else float("inf")


async def run(url: str, mgmt_url: str | None, n: int, size: int, reps: int) -> dict[str, Any]:
    envs_cache: dict[str, list[Any]] = {}

    def envs(queue: str) -> list[Any]:
        return envs_cache.setdefault(queue, _envelopes(queue, n, size))

    scenarios: list[tuple[str, Any]] = [
        ("publish single/async", lambda q: pub_single_async(url, q, envs(q))),
        ("publish gather/async x10", lambda q: pub_gather_async(url, q, envs(q), 10)),
        ("publish_many x10 (default)", lambda q: pub_many_async(url, q, envs(q), pool=10, in_flight=10, batch=False)),
        ("publish_many x64", lambda q: pub_many_async(url, q, envs(q), pool=64, in_flight=64, batch=False)),
        (
            "publish_many + batch publisher",
            lambda q: pub_many_async(url, q, envs(q), pool=16, in_flight=256, batch=True),
        ),
        ("publish single/sync", lambda q: asyncio.to_thread(pub_sync, url, q, envs(q), bulk=False)),
        ("publish_many/sync", lambda q: asyncio.to_thread(pub_sync, url, q, envs(q), bulk=True)),
        ("ack each", lambda q: ack_scenario(url, mgmt_url, q, envs(q), "each")),
        ("ack_many x100", lambda q: ack_scenario(url, mgmt_url, q, envs(q), "ack_many")),
        ("coalescing acker x100", lambda q: ack_scenario(url, mgmt_url, q, envs(q), "coalescing")),
    ]
    out: dict[str, Any] = {}
    for name, fn in scenarios:
        rates: list[float] = []
        last: dict[str, Any] = {}
        for _ in range(reps):
            queue = f"bench-bulk-{uuid.uuid4().hex[:8]}"
            last = await fn(queue)
            rates.append(_rate(last, n))
        summary = {
            "median_msg_per_s": statistics.median(rates),
            "min_msg_per_s": min(rates),
            "max_msg_per_s": max(rates),
            **{k: v for k, v in last.items() if k != "elapsed"},
        }
        out[name] = summary
        extra = ""
        if "frames" in last:
            extra = (
                f"  frames={last['frames']} ({last['frames'] / n:.3f}/msg) "
                f"api_calls={last['api_calls']} drained={last['queue_drained']}"
            )
        elif "statuses" in last:
            extra = f"  statuses={last['statuses']}"
        else:
            extra = f"  confirmed={last.get('confirmed')}/{last.get('accounted')}"
        print(f"{name:<34} {summary['median_msg_per_s']:>9.0f} msg/s (median of {reps}){extra}", flush=True)
    return out


def _quiet_logging() -> None:
    """The ack scenarios park deliveries on purpose, which trips rabbitkit's
    per-message "MANUAL handler returned without settling" warning — 2N lines
    of noise per run. Keep only errors (structlog bypasses the stdlib level
    that ``benchmarks._common`` already lowers)."""
    import logging

    import structlog

    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.ERROR))


def main() -> None:
    _quiet_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url",
        default=None,
        help="AMQP URL of an existing broker; omitted → start a throwaway testcontainers broker",
    )
    parser.add_argument(
        "--mgmt-url",
        default=None,
        help="management API URL for drain verification (default: derived for testcontainers, "
        "http://localhost:15672 for a --url on localhost); '' to skip",
    )
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--size", type=int, default=1024)
    parser.add_argument("--reps", type=int, default=3)
    args = parser.parse_args()

    if args.url:
        mgmt = (
            args.mgmt_url
            if args.mgmt_url is not None
            else ("http://localhost:15672" if "localhost" in args.url else "")
        )
        print(f"bulk vs single — n={args.n} size={args.size}B reps={args.reps} url={args.url}\n", flush=True)
        metrics = asyncio.run(run(args.url, mgmt or None, args.n, args.size, args.reps))
    else:
        from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

        from benchmarks._common import IMAGE, make_url

        with RabbitMqContainer(IMAGE).with_exposed_ports(15672) as c:
            url = make_url(c)
            mgmt = f"http://{c.get_container_host_ip()}:{c.get_exposed_port(15672)}"
            print(
                f"bulk vs single — n={args.n} size={args.size}B reps={args.reps} url={url} (testcontainers)\n",
                flush=True,
            )
            metrics = asyncio.run(run(url, mgmt, args.n, args.size, args.reps))
    RESULTS_DIR.mkdir(exist_ok=True)
    payload = {
        "timestamp": int(time.time()),
        "params": {"n": args.n, "size": args.size, "reps": args.reps},
        "env": env_fingerprint(),
        "metrics": metrics,
    }
    path = RESULTS_DIR / f"bulk_{payload['timestamp']}.json"
    path.write_text(json.dumps(payload, indent=2))
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
