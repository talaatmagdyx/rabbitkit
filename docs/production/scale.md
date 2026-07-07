# Handling millions of messages — scale & reliability handbook

How to run rabbitkit at millions of messages per day (and beyond), async
and sync, and exactly what the machinery underneath — reconnect,
heartbeat, retry, backpressure, shutdown — does for you and where its
limits are. Written from the architect's seat; the numbers are measured,
not aspirational (see [benchmarking](../benchmarking.md) for methodology).

Companion docs: [Production patterns](patterns.md) (the reference
consumer/publisher this builds on), [checklist](checklist.md),
[concurrency model](../concurrency-model.md), [Kubernetes](../kubernetes.md).

---

## 1. Do the math first

Measured on the reference hardware (ratios transfer; absolutes vary):

| Path | Throughput (single process) |
|---|---|
| Async consume (prefetch 200) | ~6,300 msg/s |
| Async confirmed publish, sequential | ~2,500 msg/s |
| Async confirmed publish, `AsyncBatchPublisher` (pipelined confirms) | ~6,100 msg/s |
| Sync confirmed publish, sequential | ~900 msg/s |
| Sync `SyncBatchPublisher` (dedicated I/O thread, pipelined) | thousands/s |
| Quorum vs classic consume | quorum ≈ −16% |

Now the arithmetic that decides your architecture:

- **1 million messages/day ≈ 12 msg/s sustained.** A single async process
  is ~500x over-provisioned for that. One pod, boring config, done.
- **100 million/day ≈ 1,160 msg/s sustained** — still one async process,
  but bursts matter now: provision for peak (often 5–10x sustained), so
  2–4 pods behind one queue.
- **1 billion/day ≈ 11,600 msg/s sustained** — this is a fleet: ~4–10
  async consumer pods per queue (RabbitMQ round-robins across consumers),
  batch publishing on the producer side, quorum queues, and queue-depth
  autoscaling.

**The scale-out unit is the process/pod, not threads or connections.**
Both transports are event-loop/I/O-thread bound internally
(`PoolConfig.publisher_connections` is deliberately reserved — more
connections per loop showed no gain). Ten pods with `prefetch_count=50`
each give you 500 in-flight messages and linear consume scaling until the
queue or the downstream becomes the bottleneck.

---

## 2. High-volume async (the default choice at scale)

### Consumer

Start from the [reference consumer](patterns.md#1-the-reference-consumer-async)
and change exactly two things for volume:

```python
config = RabbitConfig(
    connection=ConnectionConfig(...),          # as in the reference
    consumer=ConsumerConfig(
        prefetch_count=200,                    # ← the throughput knob
        graceful_timeout=45.0,
    ),
    retry=RetryConfig(max_retries=4, delays=(5, 30, 120, 600)),
)
```

**Prefetch math.** Effective concurrency = `prefetch_count` per queue
(handlers run as concurrent asyncio tasks up to that bound). Size it as
`target_msg_per_sec x avg_handler_seconds`, then sanity-check memory:
`prefetch x avg_message_bytes` is held in process. 200 x 5 KB = 1 MB —
trivial; 200 x 5 MB = 1 GB — not. High prefetch with SLOW handlers also
concentrates redelivery pain on pod death: everything unacked comes back
at once.

**Fan out by pods, not prefetch.** Past ~200–500 prefetch you're usually
moving the bottleneck to your downstream (DB, API), not RabbitMQ. Add
replicas; RabbitMQ distributes deliveries across consumers on the same
queue automatically.

**CPU-bound handlers don't belong on the event loop.** A JSON-transform
handler is fine; a 200 ms Pandas crunch stalls every other task,
heartbeats included. Offload to `asyncio.to_thread`/a process pool, or use
a sync multi-worker consumer for that queue.

### Publisher — pipelined confirms

Sequential confirmed publishing pays one broker round-trip per message
(~2.5k msg/s). At volume, coalesce confirms — same durability, one shared
round-trip per batch:

```python
from rabbitkit import BatchPublishConfig, RabbitConfig
from rabbitkit.async_ import AsyncBroker

broker = AsyncBroker(
    RabbitConfig(),   # confirms stay ON — batching pipelines them, not skips them
    batch_config=BatchPublishConfig(
        batch_size=100,          # max publishes coalesced onto one channel
        flush_interval_ms=50,    # latency bound for a partially-filled batch
        max_in_flight=1000,      # backpressure: callers wait past this
    ),
)
# broker.publish() keeps the exact same signature and semantics — each
# caller's coroutine resolves when ITS confirm resolves (~6.1k msg/s measured).
```

Blast-radius note: a batch shares one channel, so one channel-level
failure fails every publish in that batch — each caller gets the error
(never silent loss). Lower `batch_size` to bound it; use direct
`broker.publish()` for messages that must fail independently.

**Producer-side overload protection.** If your producers can outrun the
broker (bulk imports, replays), wrap publishing with a `FlowController`
so pressure lands on your code, not your memory:

```python
from rabbitkit import BackpressureConfig, FlowController

fc = FlowController(BackpressureConfig(max_in_flight=1000, on_blocked="wait"))
broker.flow_controller = fc   # also wires connection.blocked → pause publishing
```

---

## 3. High-volume sync (when you can't be async)

The sync broker is correct at scale but has hard ceilings you design
around rather than fight:

**Consuming** — one connection, handlers on a thread pool:

```python
from rabbitkit import RabbitConfig, ConsumerConfig, WorkerConfig
from rabbitkit.sync import SyncBroker

broker = SyncBroker(RabbitConfig(consumer=ConsumerConfig(prefetch_count=64)))

@broker.subscriber(queue="orders", retry=RetryConfig(max_retries=3, delays=(5, 30, 120)))
def handle(body: bytes) -> None:
    ...

broker.run(worker_config=WorkerConfig(worker_count=16))   # blocks; reconnects; drains on SIGTERM
```

- `worker_count > 1` is **mandatory** for slow or publishing handlers: a
  single worker runs handlers on the I/O thread, starving heartbeats
  (connection killed mid-handler) and making confirm waits unbounded —
  both warned at startup.
- Keep `prefetch_count >= worker_count` or the pool starves. Effective
  prefetch is per queue.
- Worker publishes marshal through the I/O thread — each confirmed
  publish briefly serializes the loop for **all** queues. Publishing-heavy
  workloads at volume: batch (below) or go async.

**Publishing at volume** — `SyncBatchPublisher` owns a dedicated
`SelectConnection` I/O thread, is explicitly thread-safe, and pipelines
confirms (each caller blocks only on its own confirm):

```python
from rabbitkit import SyncBatchPublisher  # dedicated connection, N caller threads OK
```

**Scale-out and ceilings:** like async, scale sync by processes. If a
sync service needs more than a few thousand msg/s in one process, the
honest answer is the async broker — the config is shared, only the broker
class and `async def` change.

---

## 4. The reliability machinery, end to end

What actually happens on every failure mode — so you can predict behavior
instead of discovering it.

### 4.1 Heartbeats

- **Negotiated:** the effective interval is `min(client, server)`.
  rabbitkit's default `heartbeat=30` beats the server's 60 → dead-peer
  detection in ~2 missed beats ≈ 60 s. Raise it only for handlers that
  legitimately monopolize a single-worker sync loop (better: don't do
  that).
- **Who services them:**
  - *Async:* aio-pika runs heartbeats as an independent task — always
    serviced, no action needed, even mid-reconnect.
  - *Sync, consuming:* the `run()`/`start_consuming()` loop services them
    every I/O tick.
  - *Sync, publish-only:* **nothing** services them between publishes —
    call `broker.pump_idle()` from your idle loop, or accept that the
    first publish after an idle gap hits a dead socket (it reconnects and
    retries once automatically, but proactive pumping avoids the hiccup).
  - *Sync, single worker:* a handler longer than ~2x the heartbeat starves
    the loop → broker kills the connection mid-handler → redelivery +
    duplicate side effects. Warned at startup; fix with `worker_count > 1`.

### 4.2 Auto-reconnect

Both transports reconnect on their own; the design goal is that a broker
bounce costs you seconds of throughput and zero messages (unacked
in-flight work is redelivered — this is why handlers are idempotent).

- **Async:** `aio_pika.connect_robust` — automatic reconnection with a
  per-process jittered interval (avoids fleet-wide thundering herd), robust
  channels restore consumers and QoS, and rabbitkit re-applies queue/
  exchange **bindings** after reconnect (bounded retry with backoff — they
  are the one thing robust recovery doesn't track). Initial connect also
  retries with full jitter. Multi-node: endpoints from
  `ConnectionConfig.nodes` are cycled at connect; put a LB/DNS round-robin
  in front for per-reconnect failover.
- **Sync:** exponential backoff with **full jitter**, bounded by attempts
  and wall-time (never infinite); on success the recovery loop re-declares
  topology, re-applies per-queue QoS, re-enables confirms, and
  re-subscribes every consumer with fresh tags. Reconnection is owned by
  **one thread** (the one that ran `start()`/`run()`): any other thread
  touching a dead connection gets a clean `ConnectionError`/ERROR outcome
  instead of corrupting the connection — treat it as transient and let the
  recovery loop do its job. Each backoff iteration refreshes the liveness
  heartbeat, so **a broker outage never trips liveness** and your fleet
  doesn't restart mid-outage.
- **Reconnect visibility:** wire `MetricsMiddleware` and alert on the
  reconnect counter — a flapping network looks like nothing else.

### 4.3 Retry, DLQ, and the poison-message defense in depth

Layered, so no single mechanism is load-bearing:

1. **Classification** — transient (`TimeoutError`, `OSError`, …) retries;
   permanent (`ValueError`, schema errors, …) goes straight to the DLQ.
   Unknown defaults to permanent (no retry storms from bugs). Custom:
   predicates or `unknown_policy`.
2. **The delay ladder** — `retry=RetryConfig(max_retries=4, delays=(5, 30,
   120, 600))` declares one delay queue per attempt with a uniform
   queue-level TTL (broker-enforced backoff; no consumer sleeps, no
   head-of-line TTL bug) and installs the middleware. The source message
   is acked **only after** the delay-queue publish is confirmed
   (`mandatory=True`, confirm-gated even with confirms globally off);
   anything less — failure, timeout, unroutable, unverified — nacks for
   redelivery. Retry-count and origin headers are clamped/overwritten:
   producers cannot spoof them.
3. **The DLQ** — exhausted or permanent → rejected to the source queue's
   DLX → `{queue}.dlq` (auto-provisioned for every rejecting route;
   inherits QUORUM from a quorum source). Operate it with
   `DLQInspector.peek/replay(limit=, reset_retry_count=)` or
   `rabbitkit dlq` CLI — replay acks only after a confirmed republish and
   continues past individual failures.
4. **The broker backstop** — quorum queue `delivery_limit=`: even if every
   client-side layer is bypassed, RabbitMQ itself dead-letters after N
   deliveries. This is the layer that ends crash-loops.
5. **At millions/day, dedup matters:** at-least-once × volume = duplicates
   daily, not theoretically. Either natural idempotency (upserts keyed on
   business ids) or `DeduplicationMiddleware` + Redis with stable
   `message_id`s.

### 4.4 Broker alarms and flow control

When RabbitMQ hits a memory/disk watermark it **blocks** publishing
connections — everything looks connected while publishes stall. rabbitkit:
tracks blocked state passively → `broker_readiness()` goes red (traffic
stops routing to the pod; liveness deliberately ignores it, so no restart
storm); a watchdog force-closes a connection blocked longer than
`blocked_connection_timeout` (60 s default) so it fails fast; a wired
`FlowController` pauses your publishers instead of buffering unbounded.
Alarms are a *broker capacity* signal — scale/clean the broker; the
clients are behaving correctly by slowing down.

### 4.5 Graceful shutdown under load

On SIGTERM (`run()` on either broker): consumers are cancelled first (no
new deliveries; channels stay open), in-flight handlers drain within
`graceful_timeout`, marshalled acks complete, then channels/connection
close. Handlers exceeding the budget are abandoned (sync) or cancelled +
nacked (async) — either way the message redelivers, which is duplicate
territory, not loss (idempotency again). Size
`terminationGracePeriodSeconds > graceful_timeout + preStop`. At high
prefetch, a killed pod redelivers up to `prefetch x queues` messages —
another reason not to set prefetch by vibes.

---

## 5. Scaling operations

- **Autoscale on queue depth, never CPU** — a consumer starved by a slow
  downstream is CPU-idle with a growing backlog. KEDA's `rabbitmq` scaler,
  target ~50 ready messages per replica: manifest in
  [Kubernetes → HPA scaling](../kubernetes.md).
- **Watch these five signals:** queue depth (per queue), DLQ depth (any
  growth = incident), reconnect counter (flapping), `messages_retried_total`
  vs consumed (downstream health), unacked count vs `prefetch x pods`
  (stuck handlers).
- **Millions of messages need drills, not hope:** before go-live, run the
  [soak harness](../benchmarking.md) pattern against staging — sustained
  load + a broker bounce every N seconds; verdicts require zero confirmed
  loss and full recovery. That is the test that proves §4 end to end.

## 6. Sizing cheat sheet

| Daily volume | Sustained | Architecture |
|---|---|---|
| ≤ 10 M | ≤ 120 msg/s | 1–2 async pods, prefetch 50, defaults everywhere |
| ~100 M | ~1.2k msg/s | 2–4 async pods/queue, prefetch 100–200, batch publishing, KEDA |
| ~1 B | ~12k msg/s | 4–10 async pods/queue, split by queue/domain, `AsyncBatchPublisher`, quorum + `delivery_limit`, dedup via Redis, KEDA, soak-tested |

Sync deployments: same table, divide per-process throughput by ~3–6 and
add pods accordingly — or spend the rewrite (it's one keyword per
function) and take the async column.
