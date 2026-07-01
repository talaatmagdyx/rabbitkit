"""rabbitkit SyncBroker benchmarks — real RabbitMQ, threading.

Measures consume and publish throughput of the synchronous broker (pika-based).
SyncBroker.run() blocks, so consuming runs in a background daemon thread.

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_sync
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_sync --url amqp://guest:guest@localhost/
"""

from __future__ import annotations

import argparse
import logging
import threading
import time
from typing import Any

from benchmarks._common import preload_proc

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("pika").setLevel(logging.CRITICAL)

N = 5_000
PREFETCH = 200
N_PUBLISH = 5_000


def _bench_sync_consume(url: str, queue: str, n: int, prefetch: int, **subscriber_kwargs: Any) -> float:
    """Start SyncBroker.run() in a daemon thread, drain n pre-loaded messages, return msg/s."""
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig
    from rabbitkit.sync.broker import SyncBroker

    broker = SyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))
    count = 0
    span: dict[str, float] = {}
    done = threading.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch, **subscriber_kwargs)
    def handle(body: bytes) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    # Run consuming loop in a daemon thread (broker.run() blocks)
    t = threading.Thread(target=broker.run, daemon=True)
    t.start()

    if not done.wait(timeout=300):
        raise TimeoutError(f"sync consume timed out after 300s (got {count}/{n})")

    broker.stop()
    t.join(timeout=10)

    return n / (span["end"] - span["start"])


def _bench_sync_consume_pydantic(url: str, queue: str, n: int, prefetch: int) -> float:
    try:
        from pydantic import BaseModel
    except ImportError:
        return 0.0

    from rabbitkit.core.config import ConnectionConfig, RabbitConfig
    from rabbitkit.sync.broker import SyncBroker

    class Payload(BaseModel):
        id: int

    broker = SyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))
    count = 0
    span: dict[str, float] = {}
    done = threading.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    def handle(body: Payload) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    t = threading.Thread(target=broker.run, daemon=True)
    t.start()

    if not done.wait(timeout=300):
        raise TimeoutError(f"sync pydantic consume timed out (got {count}/{n})")

    broker.stop()
    t.join(timeout=10)
    return n / (span["end"] - span["start"])


def _bench_sync_publish(url: str, queue: str, n: int) -> float:
    """Sequential sync publish (SyncBroker has no concurrency on publish path)."""
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig
    from rabbitkit.sync.broker import SyncBroker

    broker = SyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))
    broker.start()

    t0 = time.perf_counter()
    for _ in range(n):
        broker.publish(routing_key=queue, body=b'{"id":1}')
    elapsed = time.perf_counter() - t0
    broker.stop()
    return n / elapsed


def run_all(url: str) -> dict[str, float]:
    body = b'{"id":1}'
    results: dict[str, float] = {}

    print("=" * 64)
    print("rabbitkit SyncBroker Benchmarks (real RabbitMQ, pika-based)")
    print(f"  Consume: {N:,} msgs pre-loaded . prefetch={PREFETCH}")
    print(f"  Publish: {N_PUBLISH:,} msgs sequential")
    print("=" * 64)

    # ── Consume throughput ────────────────────────────────────────────────────
    consume_scenarios: list[tuple[str, str, dict[str, Any]]] = [
        ("Raw (bytes handler)", "sync-raw", {}),
        ("JSON deserialization", "sync-json", {}),
    ]

    print(f"\n  Consume throughput ({N:,} msgs each):")
    print(f"  {'Scenario':<36}  {'msg/s':>10}")
    print(f"  {'-'*36}  {'-'*10}")

    for name, queue, kwargs in consume_scenarios:
        preload_proc(url, queue, N, body)
        try:
            rate = _bench_sync_consume(url, queue, N, PREFETCH, **kwargs)
            print(f"  {name:<36}  {rate:>10,.0f}")
            key = name.lower().replace(" ", "_").replace("(", "").replace(")", "")
            results[f"sync_{key}_msg_s"] = rate
        except Exception as e:
            print(f"  {name:<36}  ERROR: {e}")

    preload_proc(url, "sync-pydantic", N, body)
    try:
        rate = _bench_sync_consume_pydantic(url, "sync-pydantic", N, PREFETCH)
        if rate:
            print(f"  {'Pydantic model':<36}  {rate:>10,.0f}")
            results["sync_pydantic_msg_s"] = rate
        else:
            print(f"  {'Pydantic model':<36}  SKIP (pydantic not installed)")
    except Exception as e:
        print(f"  {'Pydantic model':<36}  ERROR: {e}")

    # ── Publish throughput ────────────────────────────────────────────────────
    print(f"\n  Publish throughput ({N_PUBLISH:,} msgs sequential):")
    print(f"  {'Scenario':<36}  {'msg/s':>10}")
    print(f"  {'-'*36}  {'-'*10}")

    try:
        rate = _bench_sync_publish(url, "sync-pub", N_PUBLISH)
        print(f"  {'Sequential publish (raw bytes)':<36}  {rate:>10,.0f}")
        results["sync_publish_msg_s"] = rate
    except Exception as e:
        print(f"  {'Sequential publish (raw bytes)':<36}  ERROR: {e}")

    print("=" * 64)
    return results


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
