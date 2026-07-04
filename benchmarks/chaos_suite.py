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
import os
import subprocess
import threading
import time

from rabbitkit import MessageEnvelope, RabbitConfig, RetryConfig
from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SafetyConfig
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import AckPolicy, ErrorSeverity, PublishStatus
from rabbitkit.dlq import DLQInspector
from rabbitkit.middleware.retry import RetryMiddleware
from rabbitkit.sync.broker import SyncBroker

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

CONTAINER = "rk-chaos"
PORT = int(os.environ.get("RK_CHAOS_PORT", "5672"))  # override to avoid a busy 5672
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
    b = AsyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard", warn_on_discard=False), connection=ConnectionConfig.from_url(URL),
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

    broker = AsyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard", warn_on_discard=False), connection=ConnectionConfig.from_url(URL)))

    at_restart = -1

    @broker.subscriber(queue=RabbitQueue(name=queue, durable=True),
                       ack_policy=AckPolicy.NACK_ON_ERROR, prefetch_count=20)
    async def handle(body: bytes) -> None:
        nonlocal redeliveries
        oid = body.decode()
        if oid in processed:        # idempotency: redelivered duplicate
            redeliveries += 1
            return
        processed.add(oid)
        # MUST be slow enough that the restart lands mid-drain. A fast handler
        # empties the queue before the restart fires, so the scenario passes
        # without ever exercising consumer recovery (the original false positive).
        await asyncio.sleep(0.15)

    await broker.start()

    async def restart_mid_drain() -> None:
        nonlocal at_restart
        await asyncio.sleep(1.0)
        at_restart = len(processed)          # how far we got before the bounce
        await asyncio.to_thread(restart_broker)
    restart = asyncio.create_task(restart_mid_drain())

    deadline = time.monotonic() + 120
    while len(processed) < n and time.monotonic() < deadline:
        await asyncio.sleep(0.2)
    await restart  # ensure the restart finished (no stray task at teardown)
    await broker.stop()

    landed_mid = 0 < at_restart < n          # guard: the bounce truly interrupted the drain
    ok = len(processed) == n and landed_mid
    return ok, (f"processed {len(processed)}/{n} unique ({at_restart} before the bounce, "
                f"{redeliveries} redelivered-dups absorbed); consumer RESUMED after reconnect"
                if ok else
                f"FAIL: processed={len(processed)}/{n}, at_restart={at_restart}, "
                f"landed_mid={landed_mid} — consumer did not resume (or restart missed the drain)")


# ── Scenario 2: transient failures exhaust retries → DLQ ──────────────────────
async def scenario_retry_to_dlq() -> tuple[bool, str]:
    queue = "chaos.retry"
    retry_cfg = RetryConfig(max_retries=2, delays=(1, 1), jitter_factor=0.0,
                            per_queue=True, unknown_policy=ErrorSeverity.PERMANENT)
    broker = AsyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard", warn_on_discard=False), connection=ConnectionConfig.from_url(URL),
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
    broker = AsyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard", warn_on_discard=False), connection=ConnectionConfig.from_url(URL),
                                      pool=PoolConfig(channel_pool_size=32)))
    await broker.start()

    confirmed = 0
    restart = asyncio.create_task(_delayed_restart(1.0))  # restart ~1s into the publish loop

    for i in range(n):
        # outbox-style resend: keep trying until confirmed (idempotent consumers absorb dups).
        # Budget must outlast a full broker reboot (~15s) — a 2s budget gives up while the
        # broker is still down and undercounts (the app's policy, not a rabbitkit loss).
        for _ in range(120):
            outcome = await broker.publish(
                MessageEnvelope(routing_key=queue, body=f"{i}".encode(), delivery_mode=2))
            if outcome.status == PublishStatus.CONFIRMED:
                confirmed += 1
                break
            await asyncio.sleep(0.5)   # wait out the reconnect
    if not restart.done():
        await restart
    await broker.stop()

    ok = confirmed == n
    return ok, (f"{confirmed}/{n} confirmed despite a mid-publish restart "
                f"(unconfirmed publishes resent after reconnect)"
                if ok else f"FAIL: only {confirmed}/{n} confirmed")


# ── Scenario 4: SYNC broker restart mid-consume ──────────────────────────────
def _run_sync_consumer(queue: str, n: int, processed: set[str], dups: list[int],
                       ready: threading.Event) -> None:
    """Runs in a thread: a SyncBroker consumer with recovery that self-stops at n.

    worker_count=1 runs the handler inline on the connection's I/O thread, so the
    handler can call stop_consuming() directly (no cross-thread channel call) to
    unblock run() once the queue is drained.
    """
    broker = SyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard", warn_on_discard=False), connection=ConnectionConfig.from_url(URL)))

    @broker.subscriber(queue=RabbitQueue(name=queue, durable=True),
                       ack_policy=AckPolicy.NACK_ON_ERROR, prefetch_count=20)
    def handle(body: bytes) -> None:
        oid = body.decode()
        if oid in processed:
            dups[0] += 1
            return
        processed.add(oid)
        time.sleep(0.05)  # slow enough that the restart lands mid-drain
        if len(processed) >= n and broker._transport is not None:
            broker._transport.stop_consuming()  # on the I/O thread → unblocks run()

    ready.set()
    broker.run()


async def scenario_sync_restart_during_consume() -> tuple[bool, str]:
    queue = "chaos.sync.consume"
    n = 250
    await _preload(queue, n)

    processed: set[str] = set()
    dups = [0]
    ready = threading.Event()
    thread = threading.Thread(
        target=_run_sync_consumer, args=(queue, n, processed, dups, ready), daemon=True
    )
    thread.start()
    ready.wait(10)

    await asyncio.sleep(1.0)             # let some drain before the bounce
    at_restart = len(processed)
    await asyncio.to_thread(restart_broker)

    deadline = time.monotonic() + 120
    while thread.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(0.5)

    landed_mid = 0 < at_restart < n
    ok = len(processed) == n and landed_mid
    return ok, (f"processed {len(processed)}/{n} unique ({at_restart} before the bounce, "
                f"{dups[0]} redelivered-dups absorbed); sync consumer RESUMED after reconnect"
                if ok else
                f"FAIL: processed={len(processed)}/{n}, at_restart={at_restart}, "
                f"landed_mid={landed_mid} — sync consumer did not resume (or restart missed)")


# ── Scenario 5: SYNC transient failures → retry → DLQ ────────────────────────
def _run_sync_retry_consumer(queue: str, retry_cfg: RetryConfig, attempts: list[int],
                             ready: threading.Event) -> None:
    broker = SyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard", warn_on_discard=False), connection=ConnectionConfig.from_url(URL), retry=retry_cfg))
    retry_mw = RetryMiddleware(retry_cfg, publish_fn=broker.publish)

    @broker.subscriber(queue=RabbitQueue(name=queue, durable=True),
                       ack_policy=AckPolicy.NACK_ON_ERROR, retry=retry_cfg, middlewares=[retry_mw])
    def handle(body: bytes) -> None:
        attempts[0] += 1
        if attempts[0] >= 3 and broker._transport is not None:
            broker._transport.stop_consuming()  # exhausted → let the consume loop exit
        raise ConnectionError("downstream down")  # transient → retry → eventually DLQ

    broker.start()  # declares the delay-queue + DLQ topology
    broker.publish(MessageEnvelope(routing_key=queue, body=b"poison"))
    ready.set()
    try:
        broker._transport.start_consuming()
    except Exception:  # connection torn down by stop()/shutdown
        pass
    broker.stop()


async def scenario_sync_retry_to_dlq() -> tuple[bool, str]:
    queue = "chaos.sync.retry"
    retry_cfg = RetryConfig(max_retries=2, delays=(1, 1), jitter_factor=0.0,
                            per_queue=True, unknown_policy=ErrorSeverity.PERMANENT)
    attempts = [0]
    ready = threading.Event()
    thread = threading.Thread(
        target=_run_sync_retry_consumer, args=(queue, retry_cfg, attempts, ready), daemon=True
    )
    thread.start()
    ready.wait(10)

    inspect_broker = AsyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard", warn_on_discard=False), connection=ConnectionConfig.from_url(URL)))
    await inspect_broker.start()
    inspector = DLQInspector(inspect_broker._transport)
    dlq = f"{queue}.dlq"
    deadline = time.monotonic() + 30
    in_dlq = 0
    while time.monotonic() < deadline:
        msgs = await inspector.peek_async(dlq, limit=5)
        if msgs:
            in_dlq = len(msgs)
            break
        await asyncio.sleep(0.5)
    await inspect_broker.stop()
    thread.join(timeout=10)

    ok = in_dlq >= 1 and attempts[0] >= 3
    return ok, (f"{attempts[0]} attempts then landed in {dlq} (sync retry exhausted → DLQ, no loss/loop)"
                if ok else f"FAIL: attempts={attempts[0]}, in_dlq={in_dlq}")


# ── Scenario 6: SYNC broker restart mid-publish, resend unconfirmed ──────────
def _run_sync_publisher(queue: str, n: int, confirmed: list[int],
                        ready: threading.Event, done: threading.Event) -> None:
    broker = SyncBroker(RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard", warn_on_discard=False), connection=ConnectionConfig.from_url(URL)))
    broker.start()
    broker._transport.declare_queue(RabbitQueue(name=queue, durable=True))
    ready.set()
    for i in range(n):
        for _ in range(120):  # outlast a full reboot
            try:
                outcome = broker.publish(
                    MessageEnvelope(routing_key=queue, body=f"{i}".encode(), delivery_mode=2))
                if outcome.status == PublishStatus.CONFIRMED:
                    confirmed[0] += 1
                    break
            except Exception:  # connection dropped mid-publish; resend
                pass
            time.sleep(0.5)
    broker.stop()
    done.set()


async def scenario_sync_restart_during_publish() -> tuple[bool, str]:
    queue = "chaos.sync.publish"
    n = 300
    confirmed = [0]
    ready = threading.Event()
    done = threading.Event()
    thread = threading.Thread(
        target=_run_sync_publisher, args=(queue, n, confirmed, ready, done), daemon=True
    )
    thread.start()
    ready.wait(10)

    await asyncio.sleep(0.8)             # restart ~0.8s into the publish loop
    await asyncio.to_thread(restart_broker)

    deadline = time.monotonic() + 150
    while not done.is_set() and time.monotonic() < deadline:
        await asyncio.sleep(0.5)
    thread.join(timeout=5)

    ok = confirmed[0] == n
    return ok, (f"{confirmed[0]}/{n} confirmed despite a mid-publish restart "
                f"(sync publisher reconnected + resent unconfirmed)"
                if ok else f"FAIL: only {confirmed[0]}/{n} confirmed")


async def main(only: list[str] | None = None) -> None:
    """Run chaos scenarios. ``only`` filters to scenarios whose name contains
    any of the given substrings (case-insensitive) — used by the gating CI
    step (M15) to run just the critical restart-mid-consume scenarios."""
    start_broker()
    scenarios = [
        ("ASYNC restart mid-consume (no loss + idempotent dedup)", scenario_restart_during_consume),
        ("ASYNC transient failures → retry → DLQ", scenario_retry_to_dlq),
        ("ASYNC restart mid-publish (resend unconfirmed)", scenario_restart_during_publish),
        ("SYNC restart mid-consume (recovery)", scenario_sync_restart_during_consume),
        ("SYNC transient failures → retry → DLQ", scenario_sync_retry_to_dlq),
        ("SYNC restart mid-publish (resend unconfirmed)", scenario_sync_restart_during_publish),
    ]
    if only:
        needles = [s.lower() for s in only]
        scenarios = [(n, fn) for (n, fn) in scenarios if any(s in n.lower() for s in needles)]
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
    import sys

    asyncio.run(main(sys.argv[1:] or None))
