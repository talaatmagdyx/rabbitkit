"""Chaos test for the header_inspector flow: send 100 messages, lose the network
mid-flight, and check that rabbitkit reconnects, resends, and loses nothing.

Manages its OWN RabbitMQ container on port 5673 (so the broker on 5672 is untouched)
and injects two failures while 100 persistent messages are in flight:
  1. hard `docker restart`  — TCP drop → connect_robust must reconnect
  2. `docker pause`/unpause — a brief network freeze the connection must survive

Asserts: 100/100 publishes CONFIRMED (unconfirmed resent after reconnect) and 100
unique messages consumed (durable queue + persistent + confirms → zero loss),
with redelivered duplicates idempotently absorbed.

Heartbeat is now plumbed on async too: ConnectionConfig.heartbeat is carried to
aio_pika.connect_robust via the URL query (it used to be silently dropped on async;
the sync transport already honored it). This test exercises reconnect + consumer
recovery + resend, which is what actually protects messages here.

Run:
    TESTCONTAINERS_RYUK_DISABLED=true python examples/header_inspector/chaos_reconnect.py
Requires Docker.
"""

import asyncio
import json
import logging
import random
import subprocess
import time
import uuid

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.config import ConnectionConfig, PoolConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import AckPolicy, PublishStatus

logging.getLogger("rabbitkit").setLevel(logging.ERROR)
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

CONTAINER = "rk-header-chaos"
PORT = 5673
HOST = "127.0.0.1"  # IPv4 — docker -p binds IPv4 only; localhost may resolve to ::1
URL = f"amqp://guest:guest@{HOST}:{PORT}/"
QUEUE = "chaos.headers"
N = 100


# ── docker control ───────────────────────────────────────────────────────────
def _sh(*args: str) -> None:
    subprocess.run(args, check=False, capture_output=True)


def _broker_ready() -> bool:
    import pika
    try:
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=HOST, port=PORT, socket_timeout=2, connection_attempts=1)
        )
        conn.close()
        return True
    except Exception:
        return False


def _wait_ready(timeout: int) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _broker_ready():
            time.sleep(4)  # port opens before the node fully boots
            return
        time.sleep(1)
    raise RuntimeError("broker did not become ready")


def start_broker() -> None:
    _sh("docker", "rm", "-f", CONTAINER)
    run = subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", CONTAINER, "-p", f"{PORT}:5672", "rabbitmq:3.13-alpine"],
        capture_output=True, text=True,
    )
    if run.returncode != 0:
        raise RuntimeError(f"docker run failed: {run.stderr.strip()}")
    _wait_ready(120)


def restart_broker() -> None:
    subprocess.run(["docker", "restart", "-t", "3", CONTAINER], check=False, capture_output=True)
    _wait_ready(90)


def stop_broker() -> None:
    _sh("docker", "rm", "-f", CONTAINER)


# ── chaos injection: lose the network while 100 messages are in flight ────────
async def _inject_network_loss() -> None:
    await asyncio.sleep(1.5)
    print("  >> NETWORK LOSS: docker restart (hard TCP drop) ...")
    await asyncio.to_thread(restart_broker)
    print("  >> broker back online; now FREEZE: docker pause 2s ...")
    await asyncio.to_thread(_sh, "docker", "pause", CONTAINER)
    await asyncio.sleep(2)
    await asyncio.to_thread(_sh, "docker", "unpause", CONTAINER)
    print("  >> unfrozen — connection should resume")


async def run() -> int:
    start_broker()
    try:
        # declare the DURABLE queue up front
        seeder = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(URL)))
        await seeder.start()
        await seeder.stop()

        consumed: set[str] = set()
        redeliveries = 0
        broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(URL),
                                          pool=PoolConfig(channel_pool_size=32)))

        @broker.subscriber(queue=RabbitQueue(name=QUEUE, durable=True),
                           ack_policy=AckPolicy.NACK_ON_ERROR, prefetch_count=20)
        async def consume(msg: RabbitMessage) -> None:
            nonlocal redeliveries
            mid = msg.message_id or ""
            if mid in consumed:
                redeliveries += 1
                return
            consumed.add(mid)

        await broker.start()
        chaos = asyncio.create_task(_inject_network_loss())

        # publish 100 persistent messages; outbox-style resend until CONFIRMED
        confirmed = 0
        for i in range(N):
            mid = str(uuid.uuid4())
            env = MessageEnvelope(
                routing_key=QUEUE,
                body=json.dumps({"seq": i, "order_id": f"ord-{mid[:8]}"}).encode(),
                message_id=mid,
                delivery_mode=2,  # persistent
                headers={"x-tenant": random.choice(("acme", "globex", "initech")), "x-attempt": i},
            )
            for _ in range(60):  # keep retrying long enough to outlast a full reboot
                try:
                    outcome = await broker.publish(env)
                    if outcome.status == PublishStatus.CONFIRMED:
                        confirmed += 1
                        break
                except Exception:  # connection dropped mid-publish; resend
                    pass
                await asyncio.sleep(0.5)  # wait out the reconnect
            await asyncio.sleep(0.03)
        print(f"  published: {confirmed}/{N} confirmed")

        if not chaos.done():
            await chaos

        # let the consumer drain (queue survived the restart; messages are persistent)
        deadline = time.monotonic() + 90
        last = -1
        while len(consumed) < N and time.monotonic() < deadline:
            if len(consumed) != last and len(consumed) % 10 == 0:
                print(f"  draining... {len(consumed)}/{N}")
                last = len(consumed)
            await asyncio.sleep(0.2)
        await broker.stop()

        ok = confirmed == N and len(consumed) == N
        print("\n" + "=" * 68)
        print(f"  publishes confirmed : {confirmed}/{N}")
        print(f"  unique consumed     : {len(consumed)}/{N}")
        print(f"  redelivered dups    : {redeliveries} (idempotently absorbed)")
        verdict = (
            "PASS — reconnect + resend handled; zero message loss across a hard "
            "restart + freeze" if ok else
            f"FAIL — confirmed={confirmed}/{N}, consumed={len(consumed)}/{N} (loss or no recovery)"
        )
        print(f"  {verdict}")
        print("  heartbeat: ConnectionConfig.heartbeat is now carried to connect_robust "
              "via the URL (honored on async too)")
        print("=" * 68)
        return 0 if ok else 1
    finally:
        stop_broker()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
