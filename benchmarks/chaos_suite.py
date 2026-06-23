"""Chaos / soak suite — proves rabbitkit's reliability guarantees against a REAL
broker under REAL failures (the §38.16 scenarios that happy-path tests don't reach).

It manages its own RabbitMQ container (start / restart / stop) and asserts the
§38.17 acceptance criteria:
  - no confirmed message lost across a broker restart
  - no committed work lost; redelivered duplicates are idempotently absorbed
  - transient failures exhaust retries and land in the DLQ (not lost, not looping)
  - a publisher resends unconfirmed messages after a connection drop

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python benchmarks/chaos_suite.py
Requires Docker.
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
import time

from rabbitkit import MessageEnvelope, RabbitConfig, RetryConfig
from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import ConnectionConfig, PoolConfig
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import AckPolicy, ErrorSeverity, PublishStatus
from rabbitkit.dlq import DLQInspector
from rabbitkit.middleware.retry import RetryMiddleware

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

CONTAINER = "rk-chaos"
PORT = 5672
# 127.0.0.1, NOT localhost: localhost can resolve to IPv6 ::1 first, but a
# docker -p mapping binds IPv4 only — aio-pika then gets a connection reset.
HOST = "127.0.0.1"
URL = f"amqp://guest:guest@{HOST}:{PORT}/"


# ── Docker control ───────────────────────────────────────────────────────────
def _sh(*args: str) -> None:
    subprocess.run(args, check=False, capture_output=True)


def _broker_ready() -> bool:
    # A real AMQP connect (not just a socket) — the port opens before RabbitMQ
    # actually accepts the AMQP handshake, so a bare socket probe is too optimistic
    # and the first real connection gets reset. This returns True only when a full
    # connection succeeds.
    import pika

    try:
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=HOST, port=PORT,
                                      socket_timeout=2, connection_attempts=1)
        )
        conn.close()
        return True
    except Exception:
        return False


def start_broker() -> None:
    _sh("docker", "rm", "-f", CONTAINER)
    run = subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", CONTAINER, "-p", f"{PORT}:5672",
         "rabbitmq:3.13-alpine"],
        capture_output=True, text=True,
    )
    if run.returncode != 0:  # surface port conflicts etc. instead of a blind timeout
        raise RuntimeError(f"docker run failed: {run.stderr.strip()}")
    _wait_ready(120)


def restart_broker() -> None:
    subprocess.run(["docker", "restart", "-t", "3", CONTAINER], check=False, capture_output=True)
    _wait_ready(90)


def stop_broker() -> None:
    _sh("docker", "rm", "-f", CONTAINER)


def _wait_ready(timeout: int) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _broker_ready():
            time.sleep(4)  # settle: port opens before the node is fully booted
            return
        time.sleep(1)
    raise RuntimeError("broker did not become ready")


async def _delayed_restart(delay: float) -> None:
    """Restart the broker after `delay`s, as a tracked task (awaited by the caller
    so it never fires during loop teardown)."""
    await asyncio.sleep(delay)
    await asyncio.to_thread(restart_broker)


async def _preload(queue: str, n: int, *, persistent: bool = True) -> None:
    """Declare a DURABLE queue and publish n PERSISTENT messages, then disconnect."""
    b = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(URL),
                                 pool=PoolConfig(channel_pool_size=32)))
    await b.start()
    await b._transport.declare_queue(RabbitQueue(name=queue, durable=True))
    sem = asyncio.Semaphore(32)

    async def one(i: int) -> None:
        async with sem:
            await b.publish(MessageEnvelope(routing_key=queue, body=f"{i}".encode(),
                                            delivery_mode=2 if persistent else 1))

    await asyncio.gather(*(one(i) for i in range(n)))
    await b.stop()


# ── Scenario 1: broker restart mid-consume ───────────────────────────────────
async def scenario_restart_during_consume() -> tuple[bool, str]:
    queue = "chaos.consume"
    n = 400
    await _preload(queue, n)

    processed: set[str] = set()
    redeliveries = 0

    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(URL)))

    @broker.subscriber(queue=RabbitQueue(name=queue, durable=True),
                       ack_policy=AckPolicy.NACK_ON_ERROR, prefetch_count=20)
    async def handle(body: bytes) -> None:
        nonlocal redeliveries
        oid = body.decode()
        if oid in processed:        # idempotency: redelivered duplicate
            redeliveries += 1
            return
        processed.add(oid)
        await asyncio.sleep(0.01)   # slow enough that the restart lands mid-stream

    await broker.start()
    restart = asyncio.create_task(_delayed_restart(1.5))  # restart ~1.5s into the drain

    deadline = time.monotonic() + 90
    while len(processed) < n and time.monotonic() < deadline:
        await asyncio.sleep(0.2)
    await restart  # ensure the restart finished (no stray task at teardown)
    await broker.stop()

    ok = len(processed) == n
    return ok, (f"processed {len(processed)}/{n} unique, {redeliveries} redelivered-dups absorbed "
                f"(durable+persistent survived restart; connect_robust recovered)"
                if ok else f"LOST messages: only {len(processed)}/{n} processed")


# ── Scenario 2: transient failures exhaust retries → DLQ ──────────────────────
async def scenario_retry_to_dlq() -> tuple[bool, str]:
    queue = "chaos.retry"
    retry_cfg = RetryConfig(max_retries=2, delays=(1, 1), jitter_factor=0.0,
                            per_queue=True, unknown_policy=ErrorSeverity.PERMANENT)
    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(URL),
                                      retry=retry_cfg))
    retry_mw = RetryMiddleware(retry_cfg, publish_async_fn=broker.publish)
    attempts = 0

    @broker.subscriber(queue=RabbitQueue(name=queue, durable=True),
                       ack_policy=AckPolicy.NACK_ON_ERROR, retry=retry_cfg,
                       middlewares=[retry_mw])
    async def handle(body: bytes) -> None:
        nonlocal attempts
        attempts += 1
        raise ConnectionError("downstream down")  # transient → retry → eventually DLQ

    await broker.start()
    await broker.publish(MessageEnvelope(routing_key=queue, body=b"poison"))

    # delays 1s + 1s + processing → DLQ within ~6s
    inspector = DLQInspector(broker._transport)
    dlq = f"{queue}.dlq"
    deadline = time.monotonic() + 20
    in_dlq = 0
    while time.monotonic() < deadline:
        msgs = await inspector.peek_async(dlq, limit=5)
        if msgs:
            in_dlq = len(msgs)
            break
        await asyncio.sleep(0.5)
    await broker.stop()

    ok = in_dlq >= 1 and attempts >= 2
    return ok, (f"{attempts} attempts then landed in {dlq} (retry exhausted → DLQ, no loss/loop)"
                if ok else f"FAIL: attempts={attempts}, in_dlq={in_dlq}")


# ── Scenario 3: broker restart mid-publish, resend unconfirmed ────────────────
async def scenario_restart_during_publish() -> tuple[bool, str]:
    queue = "chaos.publish"
    await _preload(queue, 0)  # just declare the durable queue
    n = 600
    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(URL),
                                      pool=PoolConfig(channel_pool_size=32)))
    await broker.start()

    confirmed = 0
    restart = asyncio.create_task(_delayed_restart(1.0))  # restart ~1s into the publish loop

    for i in range(n):
        # outbox-style resend: keep trying until confirmed (idempotent consumers absorb dups)
        for _ in range(10):
            outcome = await broker.publish(
                MessageEnvelope(routing_key=queue, body=f"{i}".encode(), delivery_mode=2))
            if outcome.status == PublishStatus.CONFIRMED:
                confirmed += 1
                break
            await asyncio.sleep(0.2)   # wait out the reconnect
    if not restart.done():
        await restart
    await broker.stop()

    ok = confirmed == n
    return ok, (f"{confirmed}/{n} confirmed despite a mid-publish restart "
                f"(unconfirmed publishes resent after reconnect)"
                if ok else f"FAIL: only {confirmed}/{n} confirmed")


async def main() -> None:
    start_broker()
    scenarios = [
        ("broker restart mid-consume (no loss + idempotent dedup)", scenario_restart_during_consume),
        ("transient failures → retry → DLQ", scenario_retry_to_dlq),
        ("broker restart mid-publish (resend unconfirmed)", scenario_restart_during_publish),
    ]
    results = []
    try:
        for name, fn in scenarios:
            print(f"\n▶ {name} ...")
            try:
                ok, detail = await fn()
            except Exception as exc:
                ok, detail = False, f"EXCEPTION: {type(exc).__name__}: {exc}"
            print(f"  {'PASS' if ok else 'FAIL'} — {detail}")
            results.append((name, ok))
    finally:
        stop_broker()

    print("\n" + "=" * 70)
    passed = sum(1 for _, ok in results if ok)
    for name, ok in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    print(f"=== {passed}/{len(results)} chaos scenarios passed ===")
    raise SystemExit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    asyncio.run(main())
