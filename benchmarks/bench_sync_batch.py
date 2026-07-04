"""rabbitkit SyncBatchPublisher benchmarks — real RabbitMQ, threading.

Compares confirmed publish throughput of the pipelined SyncBatchPublisher
(SelectConnection, confirms serviced concurrently) against the baseline
sequential SyncBroker publish (BlockingChannel, one blocking confirm per
message, ~0.9k msg/s ceiling).

NOT part of the CI/`python -m benchmarks` suite — requires a real broker.

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_sync_batch
    python -m benchmarks.bench_sync_batch --url amqp://guest:guest@localhost/
"""

from __future__ import annotations

import argparse
import logging
import threading
import time

from benchmarks._common import _bench_safety

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("pika").setLevel(logging.CRITICAL)

N_PUBLISH = 5_000
QUEUE = "sync-batch-pub"
BODY = b'{"id":1}'


def _declare_queue(url: str, queue: str) -> None:
    """Ensure the target queue exists so confirmed publishes are routable."""
    from rabbitkit.core.config import ConnectionConfig
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.sync.transport import SyncTransport

    transport = SyncTransport(connection_config=ConnectionConfig.from_url(url))
    transport.connect()
    transport.declare_queue(RabbitQueue(name=queue, durable=True))
    transport.disconnect()


def _bench_baseline_blocking_publish(url: str, queue: str, n: int) -> float:
    """Sequential confirmed publish on the BlockingConnection broker (baseline)."""
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig
    from rabbitkit.sync.broker import SyncBroker

    broker = SyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
    broker.start()

    t0 = time.perf_counter()
    for _ in range(n):
        broker.publish(routing_key=queue, body=BODY)
    elapsed = time.perf_counter() - t0
    broker.stop()
    return n / elapsed


def _bench_batch_publish(url: str, queue: str, n: int, callers: int) -> float:
    """Pipelined confirmed publish via SyncBatchPublisher with N caller threads."""
    from rabbitkit.core.config import ConnectionConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.sync.batch import SyncBatchPublisher

    per_caller = n // callers
    errors: list[str] = []

    with SyncBatchPublisher(
        connection_config=ConnectionConfig.from_url(url), confirm_timeout=30.0
    ) as pub:

        def worker() -> None:
            for _ in range(per_caller):
                outcome = pub.publish(MessageEnvelope(routing_key=queue, body=BODY))
                if not outcome.ok:
                    errors.append(str(outcome.status))
                    return

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(callers)]
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=300)
        elapsed = time.perf_counter() - t0

    if errors:
        raise RuntimeError(f"batch publish failed: {errors[:3]}")
    return (per_caller * callers) / elapsed


def run_all(url: str) -> dict[str, float]:
    results: dict[str, float] = {}

    print("=" * 64)
    print("rabbitkit SyncBatchPublisher Benchmarks (real RabbitMQ)")
    print(f"  Publish: {N_PUBLISH:,} confirmed msgs per scenario")
    print("=" * 64)

    _declare_queue(url, QUEUE)

    print(f"\n  Confirmed publish throughput ({N_PUBLISH:,} msgs each):")
    print(f"  {'Scenario':<36}  {'msg/s':>10}")
    print(f"  {'-' * 36}  {'-' * 10}")

    try:
        rate = _bench_baseline_blocking_publish(url, QUEUE, N_PUBLISH)
        print(f"  {'Baseline (blocking, per-confirm)':<36}  {rate:>10,.0f}")
        results["sync_publish_blocking_msg_s"] = rate
    except Exception as e:
        print(f"  {'Baseline (blocking, per-confirm)':<36}  ERROR: {e}")

    for callers in (1, 8, 32):
        name = f"SyncBatchPublisher ({callers} callers)"
        try:
            rate = _bench_batch_publish(url, QUEUE, N_PUBLISH, callers)
            print(f"  {name:<36}  {rate:>10,.0f}")
            results[f"sync_batch_publish_{callers}c_msg_s"] = rate
        except Exception as e:
            print(f"  {name:<36}  ERROR: {e}")

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
