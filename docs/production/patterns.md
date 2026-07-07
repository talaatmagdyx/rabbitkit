# Production patterns — reference implementations

The shortest path to a deployment that survives broker restarts, deploys,
poison messages, and 3 a.m. Written from the architect's seat: every knob
below is set for a reason, and the reason is a specific production incident
it prevents. Copy the reference code, then delete what you can justify
deleting.

Companions: the [scale & reliability handbook](scale.md) (throughput math,
reconnect/heartbeat/retry mechanics end to end), the
[production checklist](checklist.md) (the audit view of the
same material), [the idempotency contract](idempotency.md) (read it before
any handler with side effects), [Kubernetes guide](../kubernetes.md),
[troubleshooting](../troubleshooting.md).

---

## 1. The reference consumer (async)

One file, production-shaped. Numbered comments are explained below the code.

```python
"""orders_worker.py — reference production consumer."""

import asyncio
import os

from aiohttp import web

from rabbitkit import (
    AsyncBroker,
    ConnectionConfig,
    ConsumerConfig,
    LoggingConfig,
    PublisherConfig,
    RabbitConfig,
    RabbitQueue,
    RetryConfig,
    broker_liveness,
    broker_readiness,
)
from rabbitkit.core.types import QueueType
from rabbitkit.serialization import JSONSerializer


def make_config() -> RabbitConfig:
    return RabbitConfig(
        connection=ConnectionConfig(
            host=os.environ.get("RABBITMQ_HOST", "127.0.0.1"),   # (1)
            port=int(os.environ.get("RABBITMQ_PORT", "5672")),
            username=os.environ.get("RABBITMQ_USER", "guest"),
            password=os.environ.get("RABBITMQ_PASSWORD", "guest"),
            vhost=os.environ.get("RABBITMQ_VHOST", "/"),
            nodes=tuple(                                         # (2)
                n for n in os.environ.get("RABBITMQ_NODES", "").split(",") if n
            ),
            heartbeat=30,                                        # (3)
            blocked_connection_timeout=60.0,                     # (4)
            connection_name=f"orders-worker@{os.environ.get('HOSTNAME', 'local')}",  # (5)
        ),
        consumer=ConsumerConfig(
            prefetch_count=16,                                   # (6)
            graceful_timeout=45.0,                               # (7)
        ),
        publisher=PublisherConfig(
            confirm_delivery=True,                               # (8)
            persistent=True,
        ),
        retry=RetryConfig(                                       # (9)
            max_retries=4,
            delays=(5, 30, 120, 600),
        ),
        logging=LoggingConfig(render_json=True),                 # (10)
    )


broker = AsyncBroker(make_config(), serializer=JSONSerializer())  # (10b)

ORDERS_QUEUE = RabbitQueue(
    name="orders",
    queue_type=QueueType.QUORUM,                                 # (11)
    durable=True,
    delivery_limit=6,                                            # (12)
    consumer_timeout=3_600_000,                                  # (13)
)


@broker.subscriber(queue=ORDERS_QUEUE)
async def handle_order(body: dict) -> None:
    """MUST be idempotent — at-least-once delivery means this can run
    twice for the same order (see docs/production/idempotency.md).
    Pattern: make the side effect keyed on a natural/business id so the
    second run is a no-op, e.g. INSERT ... ON CONFLICT DO NOTHING on
    order_id, or an idempotency key on the downstream API call."""
    order_id = body["id"]
    await process_order(order_id, body)                          # (14)


async def process_order(order_id: str, body: dict) -> None:
    ...  # your business logic


# ── Health endpoints: served IN-PROCESS (exec probes cannot work) ── (15)

async def _live(_r: web.Request) -> web.Response:
    ok = broker_liveness(broker)
    return web.Response(status=200 if ok else 503, text="ok" if ok else "wedged")


async def _ready(_r: web.Request) -> web.Response:
    ok = broker_readiness(broker)
    return web.Response(status=200 if ok else 503, text="ok" if ok else "not ready")


async def serve_health() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/healthz", _live)
    app.router.add_get("/readyz", _ready)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8080).start()
    return runner


async def main() -> None:
    runner = await serve_health()
    try:
        await broker.run()                                       # (16)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
```

Why each number:

1. **`127.0.0.1`, not `localhost`, for local dev** — avoids IPv6 `::1` vs
   IPv4-only-Docker connection resets. In cluster, the service name.
2. **`nodes=` for cluster failover** — a single `host=` against a
   multi-node cluster means one dead node takes the client down for the
   whole backoff window.
3. **`heartbeat=30`** — dead-peer detection in ~60 s. The negotiated value
   is min(client, server); 30 beats the server's 60 default.
4. **`blocked_connection_timeout=60`** — a memory/disk alarm makes
   publishes stall while everything *looks* connected. This fails the
   connection fast instead, and readiness (not liveness) goes red.
5. **`connection_name`** — shows in the management UI. Costs nothing;
   priceless mid-incident.
6. **`prefetch_count=16`** — the consume-side concurrency AND memory
   bound, **per queue**. Size it: worst-case handler seconds x desired
   parallelism, then check `prefetch x avg_message_size` fits in memory.
   `0` is rejected at construction (it would mean *unlimited* in AMQP —
   the whole backlog in RAM).
7. **`graceful_timeout=45`** — the drain budget on SIGTERM; must exceed
   your worst-case handler. Kubernetes `terminationGracePeriodSeconds`
   must exceed *this plus your preStop sleep*.
8. **Confirms + persistence on** (the defaults, stated explicitly) —
   retry/DLQ correctness assumes the retry republish is confirm-gated.
9. **The retry ladder** — one delay queue per attempt, uniform TTL per
   queue (broker-enforced backoff; the consumer never sleeps). `retry=`
   declares the topology AND installs the middleware — one switch.
10. **JSON logs** — for the aggregator, with secret redaction built in.
    **(10b) `serializer=JSONSerializer()`** — this is what turns raw bytes
    into your handler's annotation; without it, a `body: dict` handler
    receives `bytes` and dies on first index. (Verified by executing this
    exact file against a real broker — the missing serializer is the most
    common "works in the README, fails in prod" mistake.)
11. **Quorum queue** — replicated; a node death doesn't take the queue
    (or its messages) with it. The auto-declared `orders.dlq` inherits
    quorum too — the DLQ stores failures indefinitely, exactly the data
    you chose replication for.
12. **`delivery_limit=6`** — the *broker-enforced* poison-message
    backstop, independent of every client-side mechanism. Size it above
    `max_retries + 1` so it only fires when the app-level ladder is
    somehow bypassed.
13. **`consumer_timeout=` (1 h)** — only if handlers can legitimately hold
    a delivery past the server's **30-minute** ack timeout (which the
    server never advertises; the symptom is a channel force-closed
    mid-handler). Delete this line if your handlers finish in minutes.
14. **The handler takes `dict`** — body deserialization is pipeline work.
    Keep handlers pure business logic; middleware owns cross-cutting
    concerns.
15. **In-process HTTP health, never exec probes** — an exec probe spawns a
    fresh Python that cannot see the running broker. `broker_liveness`
    ignores broker connectivity (an outage must not restart pods — the
    reconnect loop itself refreshes the heartbeat); `broker_readiness`
    goes red on disconnect, dead consumer channel, or a blocked
    connection.
16. **`await broker.run()`**, not bare `start()` — `run()` joins the
    signal-driven drain, so SIGTERM finishes in-flight work before the
    process exits instead of fire-and-forgetting the stop.

---

## 2. The reference publisher

```python
from rabbitkit import MessageEnvelope, MessageTooLargeError, PublishError
from rabbitkit.core.types import PublishStatus


async def publish_order_event(broker, order_id: str, payload: bytes) -> None:
    """Publish with the outcome actually checked — the API never raises on
    its own; ignoring the outcome is how fire-and-forget sneaks back in."""
    try:
        outcome = await broker.publish(
            MessageEnvelope(
                exchange="events",
                routing_key="orders.created",
                body=payload,
                message_id=f"orders.created:{order_id}",   # stable id → enables dedup
                mandatory=True,                            # unroutable → RETURNED, not void
            )
        )
    except MessageTooLargeError:
        # >16 MiB (the server would reject it anyway, destructively).
        # Store the payload externally and publish a reference instead.
        raise

    outcome.raise_for_status()                             # NACKED/TIMEOUT/RETURNED/ERROR → PublishError

    # If something else is settled on the strength of this publish
    # (e.g. acking an inbound message), require a REAL broker confirm:
    assert outcome.status is PublishStatus.CONFIRMED
```

Rules of the road:

- **Never ignore `PublishOutcome`.** `.raise_for_status()` is the
  one-liner. `.ok` is `True` for `SENT` (written to the socket,
  unconfirmed) as well as `CONFIRMED` — when durability gates another
  action, check `CONFIRMED` specifically.
- **`mandatory=True` on anything that must not vanish** — an unroutable
  message comes back as `RETURNED` instead of being confirmed into the
  void by a binding typo.
- **A stable `message_id`** makes consumer-side deduplication
  (`DeduplicationMiddleware` + Redis) possible later without producer
  changes.
- **Bursty producers**: wrap publishes with a `FlowController`
  (`BackpressureConfig(max_in_flight=...)`) so a slow/blocked broker
  applies backpressure to *your* code instead of ballooning memory.
- **High throughput**: `AsyncBroker` + `AsyncBatchPublisher` (confirms
  pipelined). Don't scale sync publishers with threads — pika serializes
  confirms per channel.

---

## 3. If you must use the sync broker

The sync broker is production-grade with three rules the async one doesn't
need:

1. **Consumers use `broker.run(worker_config=WorkerConfig(worker_count=N))`**
   with `N > 1` whenever handlers are slow or publish with confirms —
   single-worker handlers run on the connection's I/O thread, where a slow
   handler starves heartbeats and a confirm wait is unbounded (a startup
   `RuntimeWarning` flags both).
2. **Publish-only processes call `broker.pump_idle()` from their idle
   loop** — nothing else services heartbeats between publishes, and the
   first publish after a quiet night otherwise hits a dead socket (it now
   reconnects and retries once, but proactive pumping avoids the hiccup).
3. **Everything connection-touching happens on the thread that called
   `start()`** — worker-thread settlement is marshalled for you; don't
   wire your own cross-thread calls. A non-owner thread touching a dead
   connection gets a clean error instead of corrupting the connection —
   treat that error as transient.

Roll out sync multi-worker services through one canary deploy + broker
restart drill before the fleet (see the checklist).

---

## 4. Topology lifecycle: how production avoids every 406

`AUTO_DECLARE` (the default) is right for dev and for services that own
their queues. At scale, split ownership:

```python
# One-time provisioning job / CI step, privileged credentials:
provisioner = AsyncBroker(RabbitConfig(topology_mode=TopologyMode.AUTO_DECLARE))
# declare queues, exchanges, retry ladders — then exits.

# The long-running service, least-privilege credentials, no `configure` grant:
service = AsyncBroker(RabbitConfig(topology_mode=TopologyMode.PASSIVE_ONLY))
# verifies entities exist; never re-declares → argument drift cannot 406 at 3 a.m.
```

This is also the upgrade story: when a rabbitkit version changes what it
would declare (e.g. quorum DLQ inheritance — see `docs/migration.md`), a
`PASSIVE_ONLY` service is untouched; you reconcile topology deliberately in
the provisioning step. Never set `on_topology_conflict="warn_continue"` on
a reliability path — rabbitkit refuses to sever a dead-letter path even in
that mode, but the mode's whole premise is accepting drift you haven't
reviewed.

---

## 5. Anti-patterns → what to do instead

| Anti-pattern | The incident it causes | Instead |
|---|---|---|
| Handler with side effects, no idempotency | Duplicate charges/emails on every redeploy or redelivery | Key side effects on a business id; read [the contract](idempotency.md) |
| Ignoring `PublishOutcome` | Silent loss under broker pressure, discovered weeks later | `.raise_for_status()`, `CONFIRMED` when it gates settlement |
| `time.sleep()`/`asyncio.sleep()` retry loops in handlers | Prefetch slots pinned, heartbeats starved, throughput collapse | `retry=RetryConfig(...)` — the broker does the waiting |
| Classic queues for money-path traffic | One node's disk loses the queue | Quorum + `delivery_limit` (the DLQ inherits quorum) |
| `prefetch_count` sized by vibes | OOMKill under backlog (too high) or idle workers (too low) | Size by handler time x parallelism; memory-check it |
| Liveness probe checks broker connectivity | Broker maintenance restarts every pod simultaneously | `broker_liveness` for liveness, `broker_readiness` for readiness |
| exec-based k8s probes | CrashLoopBackOff — a fresh process can't see the broker | In-process HTTP endpoints (§1) |
| `confirm_delivery=False` to "go faster" on reliability paths | A lost republish acks the source anyway | Keep confirms; use batch publishing for throughput |
| Producers setting `x-rabbitkit-retry-count` / `x-rabbitkit-original-queue` | Nothing — rabbitkit clamps/overwrites them — but it signals confusion | Producers set business headers only |
| Sharing one queue between two handlers | `DuplicateRouteError` (by design) | One handler + `filter_fn`, or two queues on one exchange |
| Deploying from stale docs/runbooks | Probes and topology that don't match the shipped code | The docs ship with the version — `mkdocs` site or the repo at your installed tag |
