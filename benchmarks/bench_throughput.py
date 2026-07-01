"""rabbitkit throughput benchmarks — real RabbitMQ.

Measures drain rate (msg/s) for each middleware scenario.
Queue is pre-loaded before the consumer starts so we measure
pure consume throughput, not publish/consume interleave.

Scenarios
---------
- Raw bytes handler
- JSON deserialization  (dict annotation)
- Pydantic model        (BaseModel annotation)
- Retry middleware      (success path — no retries triggered)
- JSON + retry middleware

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_throughput
    TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks.bench_throughput --url amqp://guest:guest@localhost/
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from typing import Any

from benchmarks._common import preload_proc

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

N = 20_000
PREFETCH = 500

# Module-level Pydantic model so get_type_hints() can resolve it even under
# `from __future__ import annotations` (local classes are not in module globals).
try:
    from pydantic import BaseModel as _BaseModel

    class _ThroughputPayload(_BaseModel):
        id: int

    _PYDANTIC_AVAILABLE = True
except ImportError:
    _PYDANTIC_AVAILABLE = False

# Module-level msgspec Struct for the msgspec scenario.
try:
    import msgspec as _msgspec

    class _ThroughputStruct(_msgspec.Struct):
        id: int

    _MSGSPEC_AVAILABLE = True
except ImportError:
    _MSGSPEC_AVAILABLE = False


async def _drain(url: str, queue: str, n: int, prefetch: int, handler_factory: Any, serializer: Any = None) -> float:
    """Start a consumer, wait for it to drain *n* messages, return msg/s."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, WorkerConfig

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)), serializer=serializer)
    count = 0
    span: dict[str, float] = {}
    done = asyncio.Event()

    def on_message() -> None:
        nonlocal count
        if count == 0:
            span["start"] = time.monotonic()
        count += 1
        if count >= n:
            span["end"] = time.monotonic()
            done.set()

    handler_factory(broker, queue, prefetch, on_message)

    await broker.start(worker_config=WorkerConfig(worker_count=1))
    await asyncio.wait_for(done.wait(), timeout=300)
    await broker.stop()
    return n / (span["end"] - span["start"])


def _raw_factory(broker: Any, queue: str, prefetch: int, on_msg: Any) -> None:
    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: bytes) -> None:
        on_msg()


def _json_factory(broker: Any, queue: str, prefetch: int, on_msg: Any) -> None:
    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: dict[str, Any]) -> None:
        on_msg()


def _pydantic_factory(broker: Any, queue: str, prefetch: int, on_msg: Any) -> None:
    if not _PYDANTIC_AVAILABLE:
        _raw_factory(broker, queue, prefetch, on_msg)
        return

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: _ThroughputPayload) -> None:
        on_msg()


def _retry_factory(broker: Any, queue: str, prefetch: int, on_msg: Any) -> None:
    from rabbitkit.core.config import RetryConfig

    @broker.subscriber(
        queue=queue,
        prefetch_count=prefetch,
        retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),
    )
    async def handle(body: bytes) -> None:
        on_msg()


def _json_retry_factory(broker: Any, queue: str, prefetch: int, on_msg: Any) -> None:
    from rabbitkit.core.config import RetryConfig

    @broker.subscriber(
        queue=queue,
        prefetch_count=prefetch,
        retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),
    )
    async def handle(body: dict[str, Any]) -> None:
        on_msg()


def _msgspec_factory(broker: Any, queue: str, prefetch: int, on_msg: Any) -> None:
    if not _MSGSPEC_AVAILABLE:
        _raw_factory(broker, queue, prefetch, on_msg)
        return

    @broker.subscriber(queue=queue, prefetch_count=prefetch)
    async def handle(body: _ThroughputStruct) -> None:
        on_msg()


def _make_json_serializer() -> Any:
    from rabbitkit.serialization.json import JSONSerializer
    return JSONSerializer()


def _make_msgspec_serializer() -> Any:
    from rabbitkit.serialization.msgspec import MsgspecSerializer
    return MsgspecSerializer()


# (label, factory, body, needs_retry_topology, serializer_factory_or_None)
# serializer_factory is a callable so each scenario gets its own instance.
SCENARIOS: list[tuple[str, Any, bytes, bool, Any]] = [
    ("Raw (bytes handler)", _raw_factory, b'{"id":1}', False, None),
    ("JSON deserialization", _json_factory, b'{"id":1}', False, _make_json_serializer),
    ("Pydantic model", _pydantic_factory, b'{"id":1}', False, _make_json_serializer),
    ("msgspec Struct", _msgspec_factory, b'{"id":1}', False, _make_msgspec_serializer),
    ("Retry middleware (success path)", _retry_factory, b'{"id":1}', True, None),
    ("JSON + retry middleware", _json_retry_factory, b'{"id":1}', True, _make_json_serializer),
]


async def _predeclare_topology(url: str, factory: Any, queue: str, prefetch: int) -> None:
    """Start a broker to let it declare queue topology (incl. DLX for retry), then stop.

    Required before preloading retry-enabled queues so the queue is declared
    with the correct x-dead-letter-exchange argument before preload_proc
    declares it as a plain durable queue (which would conflict).
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(url)))
    factory(broker, queue, prefetch, lambda: None)  # register subscriber → topology declared on start
    await broker.start()
    await broker.stop()


N_PUBLISH = 20_000
PUBLISH_CONCURRENCY = 128
PUBLISH_WARMUP = 500


async def _bench_publish(url: str, queue: str, n: int, concurrency: int, confirm_delivery: bool = True) -> float:
    """Publish n messages with given concurrency; return msg/s."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, PoolConfig, PublisherConfig, RabbitConfig
    from rabbitkit.core.topology import RabbitQueue

    broker = AsyncBroker(
        RabbitConfig(
            connection=ConnectionConfig.from_url(url),
            pool=PoolConfig(channel_pool_size=concurrency),
            publisher=PublisherConfig(confirm_delivery=confirm_delivery),
        )
    )
    await broker.start()
    await broker._transport.declare_queue(RabbitQueue(name=queue, durable=True))

    sem = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with sem:
            await broker.publish(routing_key=queue, body=b'{"id":1}')

    # Warm up: pool channels created, connection hot, fast channel opened (no-confirm path)
    await asyncio.gather(*(one() for _ in range(PUBLISH_WARMUP)))

    t0 = time.monotonic()
    await asyncio.gather(*(one() for _ in range(n)))
    elapsed = time.monotonic() - t0
    await broker.stop()
    return n / elapsed


async def _bench_publish_batch(
    url: str,
    queue: str,
    n: int,
    batch_size: int = 64,
    flush_workers: int = 0,
    prewarm: bool = False,
) -> float:
    """Batch publish: N messages via AsyncBatchPublisher; return msg/s."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import BatchPublishConfig, ConnectionConfig, PoolConfig, RabbitConfig
    from rabbitkit.core.topology import RabbitQueue

    broker = AsyncBroker(
        RabbitConfig(
            connection=ConnectionConfig.from_url(url),
            pool=PoolConfig(channel_pool_size=PUBLISH_CONCURRENCY, prewarm_channels=prewarm),
        ),
        batch_config=BatchPublishConfig(
            batch_size=batch_size, flush_interval_ms=5, flush_workers=flush_workers
        ),
    )
    await broker.start()
    await broker._transport.declare_queue(RabbitQueue(name=queue, durable=True))

    sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)

    async def one() -> None:
        async with sem:
            await broker.publish(routing_key=queue, body=b'{"id":1}')

    # Warm up
    await asyncio.gather(*(one() for _ in range(PUBLISH_WARMUP)))

    t0 = time.monotonic()
    await asyncio.gather(*(one() for _ in range(n)))
    elapsed = time.monotonic() - t0
    await broker.stop()
    return n / elapsed


def run_all(url: str) -> dict[str, float]:
    results: dict[str, float] = {}

    print("=" * 64)
    print("rabbitkit Throughput Benchmarks (real RabbitMQ)")
    print(f"  Consume: {N:,} msgs pre-loaded · prefetch={PREFETCH}")
    print(f"  Publish: {N_PUBLISH:,} msgs · concurrency={PUBLISH_CONCURRENCY}")
    print("=" * 64)

    # ── Consume throughput ────────────────────────────────────────────────────
    print(f"\n  Consume throughput ({N:,} msgs each):")
    print(f"  {'Scenario':<40}  {'msg/s':>10}")
    print(f"  {'-'*40}  {'-'*10}")

    for i, (name, factory, body, needs_retry_topo, ser_factory) in enumerate(SCENARIOS):
        queue = f"bench-tp-{i}"
        if needs_retry_topo:
            # Declare queue WITH DLX before preloading; then preload passively
            # (skip re-declaration) so we don't overwrite queue args.
            asyncio.run(_predeclare_topology(url, factory, queue, PREFETCH))
            preload_proc(url, queue, N, body, passive=True)
        else:
            preload_proc(url, queue, N, body)
        try:
            serializer = ser_factory() if ser_factory is not None else None
            rate = asyncio.run(_drain(url, queue, N, PREFETCH, factory, serializer=serializer))
            print(f"  {name:<40}  {rate:>10,.0f}")
            key = name.lower().replace(" ", "_").replace("(", "").replace(")", "")
            results[f"tp_{key}_msg_s"] = rate
        except Exception as e:
            print(f"  {name:<40}  ERROR: {e}")

    # ── Publish throughput ────────────────────────────────────────────────────
    print(f"\n  Publish throughput ({N_PUBLISH:,} msgs . concurrency={PUBLISH_CONCURRENCY}):")
    print(f"  {'Scenario':<40}  {'msg/s':>10}")
    print(f"  {'-'*40}  {'-'*10}")

    publish_scenarios: list[tuple[str, str, bool]] = [
        ("Async publish (dict body, JSON encoded)", "bench-pub-json", True),
        ("Async publish (raw bytes body)", "bench-pub-raw", True),
        ("Async publish (no-confirms, raw bytes)", "bench-pub-noconfirm", False),
    ]
    for name, queue, confirm in publish_scenarios:
        try:
            rate = asyncio.run(_bench_publish(url, queue, N_PUBLISH, PUBLISH_CONCURRENCY, confirm_delivery=confirm))
            print(f"  {name:<40}  {rate:>10,.0f}")
            key = name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(",", "")
            results[f"pub_{key}_msg_s"] = rate
        except Exception as e:
            print(f"  {name:<40}  ERROR: {e}")

    # Batch publish scenarios
    batch_scenarios: list[tuple[str, str, int, bool]] = [
        ("Async publish (batch, 1 worker)", "bench-pub-batch-1w", 1, False),
        ("Async publish (batch, auto workers)", "bench-pub-batch-auto", 0, False),
        ("Async publish (batch, auto+prewarm)", "bench-pub-batch-prewarm", 0, True),
    ]
    for name, bqueue, workers, prewarm in batch_scenarios:
        try:
            rate = asyncio.run(
                _bench_publish_batch(url, bqueue, N_PUBLISH, flush_workers=workers, prewarm=prewarm)
            )
            print(f"  {name:<40}  {rate:>10,.0f}")
            key = name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(",", "").replace("+", "")
            results[f"pub_{key}_msg_s"] = rate
        except Exception as e:
            print(f"  {name:<40}  ERROR: {e}")

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
