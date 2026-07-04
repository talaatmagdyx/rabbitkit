"""rabbitkit failure scenario benchmarks — real RabbitMQ.

Measures the overhead of failure-handling paths against a live broker.

Scenarios
---------
- Success baseline: raw bytes handler, no errors
- Retry middleware: success path overhead vs baseline
- Exception + retry: round-trip latency (fail -> retry-queue -> re-deliver)
- Dedup on_start new message (Redis AsyncMock, AMQP real)
- Dedup on_success new message (Redis AsyncMock, AMQP real)
- Dedup duplicate skip/ack (count via redis.set side-effect, AMQP real)

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_failure
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_failure --url amqp://...
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from benchmarks._common import _bench_safety, percentiles, preload_proc

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

N = 5_000
PREFETCH = 200

# Module-level Pydantic model so get_type_hints() can resolve it even under
# `from __future__ import annotations` (local classes are not in module globals).
try:
    from pydantic import BaseModel as _BaseModel

    class _FailurePayload(_BaseModel):
        id: int

    _PYDANTIC_AVAILABLE = True
except ImportError:
    _PYDANTIC_AVAILABLE = False


# ── Topology pre-declaration helper ──────────────────────────────────────────


async def _predeclare_topology(url: str, queue: str, **subscriber_kwargs: Any) -> None:
    """Start a broker to declare queue topology (incl. DLX for retry), then stop.

    Needed before preload_proc for retry-enabled queues so the queue exists with
    the correct x-dead-letter-exchange argument. preload_proc must then be called
    with passive=True to skip re-declaration.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    broker = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))

    @broker.subscriber(queue=queue, prefetch_count=PREFETCH, **subscriber_kwargs)
    async def _noop(body: bytes) -> None:
        pass

    await broker.start()
    await broker.stop()


# ── Throughput-style overhead scenarios ──────────────────────────────────────


async def _drain_with_kwargs(url: str, queue: str, n: int, prefetch: int, **subscriber_kwargs: Any) -> float:
    """Consume n messages, return msg/s."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, WorkerConfig

    broker = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
    count = 0
    span: dict[str, float] = {}
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch, **subscriber_kwargs)
    async def handle(body: bytes) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()
    return n / (span["end"] - span["start"])


async def _drain_with_pydantic(url: str, queue: str, n: int, prefetch: int) -> float:
    if not _PYDANTIC_AVAILABLE:
        return 0.0

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, WorkerConfig
    from rabbitkit.serialization.json import JSONSerializer

    broker = AsyncBroker(RabbitConfig(
        safety=_bench_safety(), connection=ConnectionConfig.from_url(url)), serializer=JSONSerializer())
    count = 0
    span: dict[str, float] = {}
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: _FailurePayload) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()
    return n / (span["end"] - span["start"])


# ── Retry round-trip latency ──────────────────────────────────────────────────


async def _bench_retry_latency(url: str, n_msgs: int = 50) -> list[float]:
    """Handler fails on attempt 1, succeeds on attempt 2. Measure re-delivery ms.

    Each message is sent with a unique id embedded in the body. On first
    delivery the handler raises (triggering the retry path). On re-delivery
    the round-trip time is recorded.
    """
    import json as _json

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, RetryConfig

    broker = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
    roundtrips: list[float] = []
    fail_times: dict[int, float] = {}
    done = asyncio.Event()

    @broker.subscriber(
        queue="fail-retry-roundtrip",
        prefetch_count=n_msgs,  # allow all retries to be processed concurrently
        retry=RetryConfig(max_retries=1, delays=(1,)),
    )
    async def handle(body: bytes) -> None:
        data = _json.loads(body)
        mid = data["id"]
        if mid not in fail_times:
            fail_times[mid] = time.monotonic()
            raise OSError("transient")  # OSError is classified as transient → retry
        roundtrips.append((time.monotonic() - fail_times[mid]) * 1000)
        if len(roundtrips) >= n_msgs:
            done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    for i in range(n_msgs):
        await broker.publish(
            routing_key="fail-retry-roundtrip",
            body=_json.dumps({"id": i}).encode(),
        )

    # Allow up to 60s for all retries (1s delay + processing overhead)
    await asyncio.wait_for(done.wait(), timeout=60)
    await broker.stop()
    return roundtrips


# ── Dedup overhead (Redis AsyncMock, AMQP real) ───────────────────────────────


async def _drain_with_dedup_new(
    url: str,
    queue: str,
    n: int,
    prefetch: int,
    mark_policy: str,
) -> float:
    """Measure dedup overhead for new messages — handler is called for every message.

    Redis.set is mocked as an AsyncMock (always returns True = new message).
    AMQP delivery goes through the real broker.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, DeduplicationConfig, RabbitConfig, WorkerConfig
    from rabbitkit.middleware.deduplication import DeduplicationMiddleware

    redis_mock = MagicMock()
    redis_mock.set = AsyncMock(return_value=True)  # always new

    mw = DeduplicationMiddleware(
        redis_client=redis_mock,
        config=DeduplicationConfig(mark_policy=mark_policy),
    )

    broker = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
    count = 0
    span: dict[str, float] = {}
    done = asyncio.Event()

    @broker.subscriber(queue=queue, prefetch_count=prefetch, middlewares=[mw])
    async def handle(body: bytes) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()
    return n / (span["end"] - span["start"])


async def _drain_with_dedup_dup(
    url: str,
    queue: str,
    n: int,
    prefetch: int,
) -> float:
    """Measure dedup overhead for duplicate messages — handler is NEVER called.

    Redis.set returns None (= already seen) so the middleware acks and skips the
    handler for every message. We count deliveries via a side-effect on redis.set,
    which is called for every delivery regardless of outcome.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, DeduplicationConfig, RabbitConfig, WorkerConfig
    from rabbitkit.middleware.deduplication import DeduplicationMiddleware

    redis_mock = MagicMock()
    count = 0
    span: dict[str, float] = {}
    done = asyncio.Event()

    async def _counting_set(*args: Any, **kwargs: Any) -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()
        return None  # always duplicate

    redis_mock.set = _counting_set

    mw = DeduplicationMiddleware(
        redis_client=redis_mock,
        config=DeduplicationConfig(mark_policy="on_start"),
    )

    broker = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))

    @broker.subscriber(queue=queue, prefetch_count=prefetch, middlewares=[mw])
    async def handle(body: bytes) -> None:
        pass  # never reached — all messages are duplicates

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()
    return n / (span["end"] - span["start"])


async def _drain_with_dedup_lru(
    url: str,
    queue: str,
    n: int,
    prefetch: int,
) -> float:
    """Measure dedup overhead with LRU cache pre-warmed — Redis.set is never called.

    All preloaded messages share body b'{"id":1}', hashing to the same dedup key.
    Pre-warming the local LRU with that single key means every delivery is a local
    cache hit → Redis is never consulted. Demonstrates the LRU fast path.
    """
    import hashlib

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, DeduplicationConfig, RabbitConfig, WorkerConfig
    from rabbitkit.middleware.deduplication import DeduplicationMiddleware

    redis_mock = MagicMock()
    redis_mock.set = AsyncMock(return_value=True)  # should never be called

    # All preloaded messages have body b'{"id":1}' → same body-hash key
    body_hash = hashlib.sha256(b'{"id":1}').hexdigest()
    full_key = f"rabbitkit:dedup:{body_hash}"

    mw = DeduplicationMiddleware(
        redis_client=redis_mock,
        config=DeduplicationConfig(mark_policy="on_start", local_cache_size=1, key_source="body_hash"),
    )
    mw._local_mark(full_key)  # pre-warm: all N messages → same key → LRU hit → no Redis

    count = 0
    span: dict[str, float] = {}
    done = asyncio.Event()

    orig_scope = mw.consume_scope_async

    async def _counted_scope(call_next: Any, message: Any) -> Any:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        result = await orig_scope(call_next, message)
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()
        return result

    mw.consume_scope_async = _counted_scope  # type: ignore[method-assign]

    broker = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))

    @broker.subscriber(queue=queue, prefetch_count=prefetch, middlewares=[mw])
    async def handle(body_bytes: bytes) -> None:
        pass  # never reached — all messages are LRU cache hits

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()
    return n / (span["end"] - span["start"])


def run_all(url: str) -> dict[str, float]:
    from rabbitkit.core.config import RetryConfig

    body = b'{"id":1}'
    results: dict[str, float] = {}

    print("=" * 72)
    print("rabbitkit Failure Scenario Benchmarks (real RabbitMQ)")
    print(f"  {N:,} messages . prefetch={PREFETCH}")
    print("=" * 72)

    # ── Throughput comparison ─────────────────────────────────────────────────
    throughput_scenarios: list[tuple[str, str, dict[str, Any], bool]] = [
        ("Success baseline (raw bytes)", "fail-base", {}, False),
        # needs_retry_topo=True: pre-declare queue WITH DLX before preloading so
        # the plain durable declaration in preload_proc does not conflict.
        ("Success + retry middleware", "fail-retry-sw", {
            "retry": RetryConfig(max_retries=3, delays=(5, 30, 120)),
        }, True),
    ]

    print(f"\n  Throughput overhead comparison ({N:,} msgs each):")
    print(f"  {'Scenario':<44}  {'msg/s':>10}")
    print(f"  {'-'*44}  {'-'*10}")

    for name, queue, kwargs, needs_retry_topo in throughput_scenarios:
        if needs_retry_topo:
            asyncio.run(_predeclare_topology(url, queue, **kwargs))
            preload_proc(url, queue, N, body, passive=True)
        else:
            preload_proc(url, queue, N, body)
        try:
            rate = asyncio.run(_drain_with_kwargs(url, queue, N, PREFETCH, **kwargs))
            print(f"  {name:<44}  {rate:>10,.0f}")
            results[f"fail_{queue}_msg_s"] = rate
        except Exception as e:
            print(f"  {name:<44}  ERROR: {e}")

    pydantic_queue = "fail-pydantic"
    preload_proc(url, pydantic_queue, N, body)
    try:
        rate = asyncio.run(_drain_with_pydantic(url, pydantic_queue, N, PREFETCH))
        if rate:
            print(f"  {'Pydantic model':<44}  {rate:>10,.0f}")
            results["fail_pydantic_msg_s"] = rate
        else:
            print(f"  {'Pydantic model':<44}  SKIP (missing dep)")
    except Exception as e:
        print(f"  {'Pydantic model':<44}  ERROR: {e}")

    # ── Dedup overhead ────────────────────────────────────────────────────────
    print("\n  Deduplication overhead (Redis AsyncMock, AMQP real):")
    print(f"  {'Scenario':<44}  {'msg/s':>10}")
    print(f"  {'-'*44}  {'-'*10}")

    dedup_new_scenarios: list[tuple[str, str, str]] = [
        ("Dedup on_start -- new message", "fail-dedup-start", "on_start"),
        ("Dedup on_success -- new message", "fail-dedup-succ", "on_success"),
    ]
    for name, queue, policy in dedup_new_scenarios:
        preload_proc(url, queue, N, body)
        try:
            rate = asyncio.run(_drain_with_dedup_new(url, queue, N, PREFETCH, policy))
            print(f"  {name:<44}  {rate:>10,.0f}")
            results[f"fail_{queue}_msg_s"] = rate
        except Exception as e:
            print(f"  {name:<44}  ERROR: {e}")

    # Duplicate skip path
    preload_proc(url, "fail-dedup-dup", N, body)
    try:
        rate = asyncio.run(_drain_with_dedup_dup(url, "fail-dedup-dup", N, PREFETCH))
        print(f"  {'Dedup -- duplicate skip/ack path':<44}  {rate:>10,.0f}")
        results["fail_dedup_dup_msg_s"] = rate
    except Exception as e:
        print(f"  {'Dedup -- duplicate skip/ack path':<44}  ERROR: {e}")

    # Local LRU pre-filter: warm cache (all keys already seen) — Redis skipped entirely
    preload_proc(url, "fail-dedup-lru", N, body)
    try:
        rate = asyncio.run(_drain_with_dedup_lru(url, "fail-dedup-lru", N, PREFETCH))
        print(f"  {'Dedup -- LRU cache hit (no Redis)':<44}  {rate:>10,.0f}")
        results["fail_dedup_lru_msg_s"] = rate
    except Exception as e:
        print(f"  {'Dedup -- LRU cache hit (no Redis)':<44}  ERROR: {e}")

    # ── Retry round-trip latency ──────────────────────────────────────────────
    n_retry = 50
    print(f"\n  Retry round-trip latency ({n_retry} msgs . delay=1s):")
    print("  fail -> retry-queue -> re-deliver (includes broker TTL delay)")
    try:
        samples = asyncio.run(_bench_retry_latency(url, n_retry))
        if samples:
            p50, p95, p99, _mn, mx = percentiles(samples)
            print(f"    p50={p50:.0f}ms  p95={p95:.0f}ms  p99={p99:.0f}ms  max={mx:.0f}ms")
            results["fail_retry_roundtrip_p50_ms"] = p50
            results["fail_retry_roundtrip_p99_ms"] = p99
        else:
            print("    No samples collected")
    except Exception as e:
        print(f"    ERROR: {e}")

    print("\n" + "=" * 72)
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
