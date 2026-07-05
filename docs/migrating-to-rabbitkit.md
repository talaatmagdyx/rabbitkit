# Migrating to rabbitkit

Practical before/after paths from the three places teams usually come
from. rabbitkit sits *on top of* `pika` and `aio-pika`, so migration is
incremental — one consumer at a time, old and new code running side by
side against the same broker.

## From aio-pika

**Before** — hand-rolled consumer with manual ack and no retry story:

```python
import aio_pika

async def main() -> None:
    conn = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    ch = await conn.channel()
    await ch.set_qos(prefetch_count=50)
    queue = await ch.declare_queue("orders.created", durable=True)

    async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        async with message.process():  # ack on success, reject on exception
            data = json.loads(message.body)
            await handle_order(data)

    await queue.consume(on_message)
```

**After** — same behavior plus a retry ladder, a DLQ, and classified
errors:

```python
from rabbitkit import AsyncBroker, RabbitConfig, RetryConfig

broker = AsyncBroker(RabbitConfig())

@broker.subscriber(
    queue="orders.created",
    prefetch_count=50,
    retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),
)
async def on_order(body: dict) -> None:
    await handle_order(body)
```

What you stop maintaining: connection/channel lifecycle, QoS wiring,
declaration boilerplate, the ack/reject decision tree, and everything
retry-related (delay queues, retry-count headers, DLX wiring). What you
keep: direct access to the underlying client whenever you need an AMQP
primitive rabbitkit doesn't wrap.

Migration mechanics:

1. Point a rabbitkit consumer at the **same queue** your aio-pika
   consumer uses. Both can run simultaneously (RabbitMQ round-robins).
2. If the queue pre-exists with arguments rabbitkit doesn't know about,
   use `TopologyMode.PASSIVE_ONLY` to verify-not-declare.
3. Publishers migrate independently: `await broker.publish(...)` returns
   a checked `PublishOutcome` instead of fire-and-forget.

## From pika (blocking)

**Before** — the classic callback loop:

```python
import pika

conn = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@localhost/"))
ch = conn.channel()
ch.queue_declare(queue="orders.created", durable=True)

def on_message(ch, method, properties, body):
    try:
        handle_order(json.loads(body))
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

ch.basic_qos(prefetch_count=50)
ch.basic_consume(queue="orders.created", on_message_callback=on_message)
ch.start_consuming()  # dies permanently on the first connection blip
```

**After**:

```python
from rabbitkit import RabbitConfig, RetryConfig
from rabbitkit.sync import SyncBroker

broker = SyncBroker(RabbitConfig())

@broker.subscriber(
    queue="orders.created",
    prefetch_count=50,
    retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),
)
def on_order(body: dict) -> None:
    handle_order(body)

broker.run()  # blocks; reconnects on connection loss; drains on SIGTERM
```

The single biggest win over raw `BlockingConnection`: **recovery**.
pika's blocking adapter has no built-in reconnect — one heartbeat
timeout or broker restart kills the consumer permanently. `broker.run()`
reconnects, re-declares, and re-subscribes (this is the path the chaos
gate and weekly soak exercise). Second biggest: nacked messages go
somewhere (retry ladder → DLQ) instead of hot-looping or vanishing.

## From Celery

This is an *architecture* migration, not a syntax one — do it only if
the "Where rabbitkit fits" reasoning from the
[README](https://github.com/talaatmagdyx/rabbitkit#where-rabbitkit-fits)
applies (you want direct broker semantics, not a task framework). Concept mapping:

| Celery | rabbitkit equivalent | Notes |
|---|---|---|
| `@app.task` | `@broker.subscriber(queue=...)` | You name the queue; routing is explicit, not name-mangled |
| `task.delay(args)` | `broker.publish(routing_key=..., body={...})` | Body is an explicit contract, not pickled args |
| `autoretry_for` / `retry_backoff` | `RetryConfig(max_retries=, delays=)` | Broker-side TTL ladders — retries survive worker crashes |
| dead-letter handling (manual) | automatic | Every rejecting route gets a DLQ by default |
| result backend | `store_results=True` dedup, or result backends | Only if you actually need request/response — most events don't |
| `celery beat` | **no equivalent** | rabbitkit is not a scheduler; keep beat or use cron/K8s CronJobs |
| `celery -A app worker` | `rabbitkit run myapp.main:broker` | |
| task events / Flower | structured logs, metrics, dashboard, `rabbitkit dlq inspect` | |

What you gain: visible AMQP semantics (exchanges, bindings, acks,
confirms are yours), lighter dependencies, DLQ tooling, checked publish
outcomes. What you lose: the scheduler, `chord`/`chain` canvas
primitives, and result-first ergonomics — if your workload leans on
those, **stay on Celery**; that's what it's for.

Migration mechanics: run both against the same RabbitMQ. Move one task
at a time — replace `task.delay()` call sites with `broker.publish()`
to a named queue and stand up a rabbitkit consumer for it. Because
Celery's queues and rabbitkit's queues are just AMQP queues on the same
broker, there is no big-bang cutover.
