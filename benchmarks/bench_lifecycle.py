"""rabbitkit lifecycle benchmarks — shutdown drain time and reconnect recovery.

Requires a real RabbitMQ broker (managed via testcontainers/Docker).

Scenarios
---------
- Graceful shutdown drain time: how long broker.stop() takes to drain
  in-flight messages at different in-flight counts.
- Reconnect recovery time: time from connection drop to consumer resuming.
- Startup time: time from broker.start() to first message handled.

Run: TESTCONTAINERS_RYUK_DISABLED=true python benchmarks/bench_lifecycle.py
Requires: Docker + testcontainers (pip install testcontainers)
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from typing import Any

from benchmarks._common import _bench_safety

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)


async def _bench_startup_time(url: str, runs: int = 5) -> list[float]:
    """Time from AsyncBroker() to first message handled."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    times: list[float] = []
    for _ in range(runs):
        broker = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
        evt: asyncio.Event = asyncio.Event()

        def _make_startup_handler(event: asyncio.Event) -> Any:
            async def handle(body: bytes) -> None:
                if not event.is_set():
                    event.set()
            return handle

        broker.subscriber(queue="lifecycle-startup")(_make_startup_handler(evt))

        t0 = time.monotonic()
        await broker.start()

        await broker.publish(routing_key="lifecycle-startup", body=b"ping")
        await asyncio.wait_for(evt.wait(), timeout=10.0)
        elapsed = time.monotonic() - t0

        await broker.stop()
        times.append(elapsed * 1000)
    return times


async def _bench_shutdown_drain(url: str, in_flight: int) -> float:
    """Time for broker.stop() to drain exactly `in_flight` slow messages."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, ConsumerConfig, RabbitConfig, WorkerConfig

    broker = AsyncBroker(
        RabbitConfig(safety=_bench_safety(),
            connection=ConnectionConfig.from_url(url),
            consumer=ConsumerConfig(prefetch_count=in_flight),
        )
    )
    handled = 0
    started_at: float = 0.0

    @broker.subscriber(queue=f"lifecycle-drain-{in_flight}", prefetch_count=in_flight)
    async def handle(body: bytes) -> None:
        nonlocal handled, started_at
        if handled == 0:
            started_at = time.monotonic()
        handled += 1
        await asyncio.sleep(0.05)  # simulate slow handler (50ms each)

    await broker.start(worker_config=WorkerConfig(worker_count=4))

    # Pre-load the queue
    for i in range(in_flight):
        await broker.publish(routing_key=f"lifecycle-drain-{in_flight}", body=f'{{"id":{i}}}'.encode())

    await asyncio.sleep(0.2)  # let some handlers start

    t0 = time.monotonic()
    await broker.stop()
    drain_ms = (time.monotonic() - t0) * 1000
    return drain_ms


async def _bench_reconnect_recovery(url: str, container: Any, runs: int = 3) -> list[float]:
    """Time from connection drop to consumer receiving a new message."""
    import subprocess

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    recovery_times: list[float] = []

    for _ in range(runs):
        broker = AsyncBroker(RabbitConfig(safety=_bench_safety(), connection=ConnectionConfig.from_url(url)))
        recovered: asyncio.Event = asyncio.Event()
        drop_time: list[float] = []

        def _make_reconnect_handler(event: asyncio.Event) -> Any:
            async def handle(body: bytes) -> None:
                if body == b"after-reconnect" and not event.is_set():
                    event.set()
            return handle

        broker.subscriber(queue="lifecycle-reconnect")(_make_reconnect_handler(recovered))

        await broker.start()

        # Send a message before drop to confirm consumer is alive
        await broker.publish(routing_key="lifecycle-reconnect", body=b"before-drop")
        await asyncio.sleep(0.5)

        # Force-close the RabbitMQ container's port (simulate network drop)
        container_id = container.get_wrapped_container().id
        subprocess.run(
            ["docker", "pause", container_id], capture_output=True, check=False
        )
        drop_time.append(time.monotonic())
        await asyncio.sleep(1.0)  # keep paused for 1s

        subprocess.run(
            ["docker", "unpause", container_id], capture_output=True, check=False
        )
        unpause_t = time.monotonic()

        # Publish after unpause — broker must reconnect before it can handle
        for _ in range(20):
            await asyncio.sleep(0.5)
            try:
                await broker.publish(routing_key="lifecycle-reconnect", body=b"after-reconnect")
            except Exception:
                pass
            if recovered.is_set():
                break

        if recovered.is_set():
            recovery_ms = (time.monotonic() - unpause_t) * 1000
            recovery_times.append(recovery_ms)

        await broker.stop()
        recovered.clear()

    return recovery_times


def run_all(url: str | None = None, container: Any = None) -> None:
    if url is None:
        try:
            from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]
        except ImportError:
            print("testcontainers not installed. Run: pip install testcontainers")
            return
        with RabbitMqContainer("rabbitmq:3.13-management-alpine") as c:
            _run(c.get_container_host_ip(), c.get_exposed_port(5672), c)
    else:
        _run_with_url(url, container)


def _run(host: str, port: int, container: Any) -> None:
    url = f"amqp://guest:guest@{host}:{port}/"
    _run_with_url(url, container)


def _run_with_url(url: str, container: Any = None) -> None:
    print("=" * 64)
    print("rabbitkit Lifecycle Benchmarks (real RabbitMQ via Docker)")
    print("=" * 64)

    # 1. Startup time
    print("\n[1] Startup time (broker.start() → first message handled):")
    times = asyncio.run(_bench_startup_time(url, runs=5))
    print(f"    p50 = {statistics.median(times):.0f} ms")
    print(f"    p95 = {sorted(times)[int(len(times)*0.95)]:.0f} ms")
    print(f"    min = {min(times):.0f} ms  max = {max(times):.0f} ms")

    # 2. Shutdown drain time
    print("\n[2] Graceful shutdown drain time (50ms handlers):")
    for in_flight in (5, 20, 50):
        drain_ms = asyncio.run(_bench_shutdown_drain(url, in_flight))
        print(f"    in_flight={in_flight:<3}  drain = {drain_ms:.0f} ms")

    # 3. Reconnect recovery (only when we own the container)
    if container is not None:
        print("\n[3] Reconnect recovery time (docker pause/unpause simulation):")
        try:
            times = asyncio.run(_bench_reconnect_recovery(url, container, runs=3))
            if times:
                print(f"    p50 = {statistics.median(times):.0f} ms")
                print(f"    min = {min(times):.0f} ms  max = {max(times):.0f} ms")
            else:
                print("    No successful recovery measured (check Docker access)")
        except Exception as e:
            print(f"    Skipped: {e}")
    else:
        print("\n[3] Reconnect recovery: skipped (no container handle for pause/unpause)")

    print("\n" + "=" * 64)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
    a = parser.parse_args()
    if a.url:
        _run_with_url(a.url)
    else:
        run_all()
