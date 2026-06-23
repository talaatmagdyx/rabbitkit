"""rabbitkit pipeline benchmarks.

Measures message processing throughput using the in-memory TestBroker.
Run with: python -m benchmarks.bench_pipeline

Results show messages/second for different scenarios:
- Raw pipeline (no middleware)
- Pipeline with serialization
- Pipeline with DI resolution
- Pipeline with compression
- Pipeline with retry middleware
"""

from __future__ import annotations

import json
import statistics
import time
from typing import Any

from rabbitkit.testing.broker import TestBroker


def bench_raw_pipeline(iterations: int = 10000) -> float:
    """Benchmark: raw pipeline with no middleware, bytes handler."""
    broker = TestBroker()

    @broker.subscriber(queue="bench")
    def handle(body: bytes) -> None:
        pass

    broker.start()

    body = b'{"key": "value", "number": 42}'

    # Warmup
    for _ in range(100):
        broker.publish("bench", body)

    # Benchmark
    start = time.perf_counter()
    for _ in range(iterations):
        broker.publish("bench", body)
    elapsed = time.perf_counter() - start

    rate = iterations / elapsed
    broker.stop()
    return rate


def bench_json_serialization(iterations: int = 10000) -> float:
    """Benchmark: pipeline with JSON deserialization."""
    broker = TestBroker()

    @broker.subscriber(queue="bench")
    def handle(body: dict[str, Any]) -> None:
        pass

    broker.start()

    body = json.dumps({"key": "value", "number": 42, "nested": {"a": 1, "b": 2}}).encode()

    # Warmup
    for _ in range(100):
        broker.publish("bench", body)

    start = time.perf_counter()
    for _ in range(iterations):
        broker.publish("bench", body)
    elapsed = time.perf_counter() - start

    rate = iterations / elapsed
    broker.stop()
    return rate


def bench_multiple_routes(iterations: int = 5000) -> float:
    """Benchmark: pipeline with multiple registered routes."""
    broker = TestBroker()

    for i in range(10):

        @broker.subscriber(queue=f"bench-{i}")
        def handle(body: bytes) -> None:
            pass

    broker.start()

    body = b'{"key": "value"}'

    # Warmup
    for _ in range(50):
        broker.publish("bench-0", body)

    start = time.perf_counter()
    for _ in range(iterations):
        broker.publish("bench-0", body)
    elapsed = time.perf_counter() - start

    rate = iterations / elapsed
    broker.stop()
    return rate


def bench_result_publishing(iterations: int = 5000) -> float:
    """Benchmark: pipeline with result publishing (handler returns value)."""
    broker = TestBroker()

    @broker.publisher(exchange="out", routing_key="result")
    @broker.subscriber(queue="bench")
    def handle(body: bytes) -> bytes:
        return b'{"status": "ok"}'

    broker.start()

    body = b'{"key": "value"}'

    # Warmup
    for _ in range(50):
        broker.publish("bench", body)

    start = time.perf_counter()
    for _ in range(iterations):
        broker.publish("bench", body)
    elapsed = time.perf_counter() - start

    rate = iterations / elapsed
    broker.stop()
    return rate


def run_all() -> None:
    """Run all benchmarks and print results."""
    print("=" * 60)
    print("rabbitkit Pipeline Benchmarks (in-memory TestBroker)")
    print("=" * 60)
    print()

    benchmarks = [
        ("Raw pipeline (bytes handler)", bench_raw_pipeline),
        ("JSON deserialization", bench_json_serialization),
        ("Multiple routes (10)", bench_multiple_routes),
        ("Result publishing", bench_result_publishing),
    ]

    for name, fn in benchmarks:
        # Run 3 times, take median
        rates = [fn() for _ in range(3)]
        median_rate = statistics.median(rates)
        print(f"  {name:<40} {median_rate:>10,.0f} msg/s")

    print()
    print("=" * 60)
    print("Note: These are in-memory benchmarks. Real RabbitMQ")
    print("throughput depends on network, persistence, and broker config.")
    print("=" * 60)


if __name__ == "__main__":
    run_all()
