# Bulk Operations and Reliability Profiles

rabbitkit 0.12 adds first-class **bulk publishing** (`publish_many`,
`iter_publish`), **selected acknowledgement** (`ack_many`, `nack_many`),
**safe ack coalescing** (`CoalescingAcker`), **reliability profiles**
(`standard` / `critical`) with preflight verification, and bounded handling
of **retry-handoff failures**. Every bulk API shares the exact safeguards of
its single-message counterpart: the same middleware chain, flow controller,
size limit and transport path.

"Bulk" here means *one API call with per-item outcomes*. It never means an
atomic multi-message command (RabbitMQ has none) and it never turns an
uncertain outcome into a success.

---

## Safety invariants

These hold across sync, async, single and bulk paths:

1. A source delivery is never acked before its required work or a
   confirmed downstream handoff succeeded.
2. A publish timeout or disconnect means **UNKNOWN**, not "rejected".
   rabbitkit preserves that uncertainty in the result instead of guessing.
3. Acks are never coalesced across an unfinished or retry-pending delivery.
4. Delivery handles belong to one channel generation; a rebuilt channel
   invalidates them (`STALE`), they are never replayed.
5. Queued work is bounded by count **and** bytes; waiting is bounded by time.
6. No automatic replay of a partially successful batch.
7. Consumer settlement has no broker confirmation; reports say
   `DISPATCHED`, never "confirmed".

---

## Bulk publishing

```python
from rabbitkit import AsyncBroker, BulkPublishOptions, BulkPublishStatus, MessageEnvelope

async def publish_page(broker: AsyncBroker, rows: list[dict]) -> None:
    envelopes = [
        MessageEnvelope(
            message_id=row["event_id"],          # stable, caller-owned id
            exchange="events",
            routing_key="tweet.received",
            body=row["payload"],
            mandatory=True,
        )
        for row in rows
        if row["ready"]
    ]
    result = await broker.publish_many(
        envelopes,
        BulkPublishOptions(
            max_in_flight=256,
            max_buffer_bytes=8 * 1024 * 1024,
            admission_timeout=5.0,
            confirm_timeout=10.0,
            overall_timeout=30.0,
        ),
    )
    for item in result.items:                    # input-ordered, one per envelope
        if item.status is BulkPublishStatus.UNKNOWN:
            await reconcile(item.message_id, item.attempt_id)
        elif item.status is BulkPublishStatus.NOT_SENT:
            resubmit_later(item.index)            # provably never left the process
    result.raise_for_status()                     # optional: raise unless ALL confirmed
```

`SyncBroker.publish_many` has the same contract without `await`. The sync
transport confirms one publish at a time (pika cannot pipeline confirms), so
it is sequential; `max_in_flight` has no effect there but `overall_timeout`
does. For unbounded inputs use `iter_publish`, which yields items as they
settle and never holds the whole result set in memory.

### Per-item result model

| Status | Meaning | Caller action |
|---|---|---|
| `CONFIRMED` | Broker confirm observed, no mandatory return | Do not replay |
| `UNROUTABLE` | Mandatory `Basic.Return` observed | Fix routing before retrying |
| `NACKED` | Negative publisher confirm | Retry only under an idempotency policy |
| `INVALID` | Local validation failed before submission | Fix the input |
| `NOT_SENT` | Definitively never submitted (admission/overall deadline, backpressure drop) | Safe to resubmit |
| `UNKNOWN` | May have reached the broker (`SENT` with confirms off, confirm timeout, exception mid-publish, cancellation) | Reconcile by `message_id` + `attempt_id`; retry only with stable ids and deduplication |

Every item carries `index`, `message_id`, a fresh `attempt_id` per
submission, a **bounded** `reason` code (safe as a metric label), body size,
and submit/settle timestamps. Results are keyed by input index, never solely
by `message_id`, because input ids may repeat. `UNROUTABLE` wins over a
positive confirm: an unroutable message is not a successful routed
publication.

### Bounds

| Option | Bounds |
|---|---|
| `max_in_flight` | Submitted-but-unsettled envelopes (async) |
| `max_buffer_bytes` | Sum of in-flight body bytes; a larger envelope is `INVALID` up front |
| `admission_timeout` | Wait for a slot before `NOT_SENT` |
| `confirm_timeout` | Per-item confirm wait override before `UNKNOWN` |
| `overall_timeout` + `drain_grace` | Whole operation; remaining inputs `NOT_SENT`, in-flight `UNKNOWN` |
| `max_items` | `publish_many` input length; exceeding it raises **before** anything is published |

The defaults are conservative starting points, not tuning advice. Measure.

---

## Selected ack and nack

```python
from rabbitkit import AckPolicy, RabbitMessage

held: list[RabbitMessage] = []

@broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
async def handle(body: bytes, msg: RabbitMessage) -> None:
    held.append(msg)                              # defer settlement to the batch commit

async def commit_batch(rows, deliveries) -> None:
    ok, failed = await database.commit(rows)      # returns the exact successful subset
    report = await broker.ack_many([deliveries[i] for i in ok])
    report.raise_for_status()
    await broker.nack_many([deliveries[i] for i in failed], requeue=False)
```

`ack_many` acks **exactly** the given deliveries, each with its own
`multiple=False` frame. It never issues a cumulative ack, so a still-running
sibling on the same channel is untouched even if the selected tags are 1 and
3. Validation runs before any I/O and reports per item:

| Status | Meaning |
|---|---|
| `DISPATCHED` | Handed to the transport (local wire-write boundary, not a broker confirmation) |
| `ALREADY_SETTLED` | Idempotent no-op |
| `DUPLICATE` | Same message object listed twice |
| `INVALID` | Not a `RabbitMessage`, a no-ack delivery, or wired for the other runtime |
| `STALE` | Channel rebuilt since delivery; the tag would name a different message on the new channel |
| `NOT_ATTEMPTED` | Aborted because another item failed validation (`fail_fast=True`, the default) |
| `FAILED` | Transport raised; the message stays unsettled |

There is no message-id based ack: a logical message may have several
delivery attempts and an id does not identify a channel delivery.
`TestBroker.ack_many` / `nack_many` mirror the contract for unit tests.

---

## Safe coalescing

`CoalescingAcker` is the only place a `multiple=True` ack may originate. It
wraps a channel-wide `SettlementCoordinator` that must know **every**
delivery on the channel:

```python
from rabbitkit import BatchAckConfig, CoalescingAcker

acker = CoalescingAcker(
    ack_fn=lambda tag, multiple: connection.add_callback_threadsafe(
        lambda: channel.basic_ack(delivery_tag=tag, multiple=multiple)
    ),
    nack_fn=lambda tag, requeue: ...,
    reject_fn=lambda tag, requeue: ...,
    config=BatchAckConfig(batch_size=50, flush_interval_ms=200),
)

acker.register(tag)              # BEFORE the handler runs
acker.complete(tag)              # handler succeeded
acker.fail(tag, requeue=False)   # handler failed terminally
acker.retry_pending(tag)         # retry middleware owns it; blocks coalescing above it
acker.on_reconnect()             # channel rebuilt: drop the ledger, never replay
```

| Tag | State | Action |
|---|---|---|
| 101 | Success | ack 101 |
| 102 | Still running | nothing |
| 103 | Success | individual ack 103 (or hold briefly, then individual) |
| 104 | Retry handoff pending | nothing |

A cumulative ack through tag *T* is emitted only when every still-outstanding
lower tag is approved for success. If your process cannot guarantee that it
registers every delivery on the channel, construct with `coalesce=False`.

### One acker per channel

Delivery tags are a **per-channel counter**: tag 7 on channel A and tag 7 on
channel B are different messages. rabbitkit gives every subscriber queue its
own channel, so a consumer with several queues needs several ledgers:

```
Channel A → CoalescingAcker A → SettlementCoordinator A
Channel B → CoalescingAcker B → SettlementCoordinator B
```

Feeding two channels into one acker would let a cumulative ack computed from
A's completed prefix settle B's messages. Pass `channel_key=` and that
convention becomes an enforced invariant: a delivery from any other channel
raises `ChannelMismatchError` at `register()` time, before it can corrupt the
ledger. An unbound acker binds to the first key it is given.

`CoalescingAckerGroup` does the bookkeeping for you — one acker per channel,
created on demand from your factory:

```python
from rabbitkit import BatchAckConfig, CoalescingAcker, CoalescingAckerGroup

def build(channel):                     # only you know how to reach your transport
    return CoalescingAcker(
        ack_fn=lambda t, m: emit(channel.basic_ack(delivery_tag=t, multiple=m)),
        nack_fn=lambda t, r: emit(channel.basic_nack(delivery_tag=t, requeue=r)),
        reject_fn=lambda t, r: emit(channel.basic_reject(delivery_tag=t, requeue=r)),
        config=BatchAckConfig(batch_size=50, flush_interval_ms=200),
        channel_key=channel,
    )

group = CoalescingAckerGroup(factory=build)

@broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
async def handle(body: bytes, msg: RabbitMessage) -> None:
    channel = msg.raw_message.channel
    group.register(channel, msg.delivery_tag)   # BEFORE the work
    ...
    group.complete(channel, msg.delivery_tag)
```

| Call | Effect |
|---|---|
| `group.for_channel(ch)` | The acker owning that channel, built on first use |
| `group.register/complete/fail/retry_pending/release(ch, tag)` | Routed to that channel's ledger, channel-checked |
| `group.flush()` | Fans out; returns a `GroupFlushReport` aggregating every channel |
| `group.on_reconnect(ch)` | One channel rebuilt: drop its ledger, retire its acker, return the dropped tags |
| `group.reset()` | Whole connection rebuilt: drop every ledger |
| `group.close()` | Drain approved work on every channel, then reject further use |

Channels are dict keys (pika and aio-pika channels are identity-hashable) and
the group holds a strong reference to each, so call `on_reconnect` or `reset`
when they are rebuilt. Totals (`settled_total`, `coalesced_total`) survive a
channel being retired, so a reconnect does not reset your metrics.

The legacy `BatchAcker` now defaults to `mode="individual"`. The old
`ack(max_tag, multiple=True)` behaviour is `mode="cumulative"` and requires
`ordered_exclusive_owner=True` as an explicit attestation.

---

## Reliability profiles

```python
from rabbitkit import RabbitConfig, ReliabilityProfile, SyncBroker, apply_profile, critical_config

config = critical_config(RabbitConfig(connection=...))   # or apply_profile(cfg, "standard")
broker = SyncBroker(config)

report = broker.preflight(ReliabilityProfile.CRITICAL, management_client=mgmt)
if not report.ok:
    raise SystemExit("\n".join(str(c) for c in report.failed))
for check in report.unverified:                           # never hidden
    log.warning("unverified: %s", check)
```

| Setting | Standard | Critical |
|---|---|---|
| Confirms / mandatory / persistence | on | required |
| Body limit | bounded | ≤ 256 KiB |
| Queue type | explicit | quorum for every durable route queue |
| Retry topology | explicit | quorum delay chain + quorum DLQ |
| Error text on DLQ headers | sanitized | sanitized or omitted |
| Dead-letter path | auto-provisioned | never `discard` |

`apply_profile` fills in requirements and **raises** on a contradiction (a
value you pinned explicitly to something the profile forbids). It never
silently flips a deliberate choice, and it never re-declares an existing
queue with a different type: create versioned queues and migrate.

`preflight` verifies what it can locally and, given a read-only management
client, verifies queue type, `dead-letter-strategy: at-least-once`,
`overflow: reject-publish` and `delivery-limit` on the broker. Without a
client those checks are `UNVERIFIED`, never green. `policy_templates()`
renders reviewed policy definitions (management-API JSON and `rabbitmqctl`
form) for the cluster owner to apply; rabbitkit never mutates policies.

---

## Retry hardening

* `RetryConfig.delay_queue_type` (`classic` default, `quorum`, `inherit`) and
  `dlq_queue_type` (`inherit` default, `classic`, `quorum`) make the retry
  chain's durability explicit. Defaults keep the legacy topology so an
  upgrade never 406s.
* `RetryConfig.error_detail` controls the DLQ triage headers: `sanitized`
  (default: credentials, tokens, URL passwords redacted; length-capped),
  `omit`, or `raw` (legacy). A new `x-rabbitkit-error-category` header
  carries `transient` / `permanent`.
* `RetryConfig.handoff` (`RetryHandoffConfig`) bounds what happens when the
  **retry publish itself** fails (returned, nacked, timed out, connection
  drop). The source is nack-requeued, never acked; consecutive failures get
  capped exponential backoff with jitter (awaited on async handlers, surfaced
  through the `on_handoff_failure` hook on sync ones, which never sleep on the
  I/O thread); `retry_handoff_failures_total` and `retry_handoff_paused` are
  emitted; after `max_consecutive_failures` or `recovery_deadline` the tracker
  is `EXHAUSTED` and `on_handoff_exhausted` fires so the owner can stop the
  consumer and let the broker redeliver.

---

## Observability

New and newly emitted metrics (all labels bounded, never a message id or
raw exception text):

| Metric | Labels |
|---|---|
| `rabbitkit_bulk_publish_items_total` | `status`, `reason` |
| `rabbitkit_bulk_publish_batch_size` | |
| `rabbitkit_settlement_items_total` | `action`, `status` |
| `rabbitkit_retry_handoff_failures_total` | `queue` |
| `rabbitkit_retry_handoff_paused` (gauge) | `queue` |
| `rabbitkit_publish_confirm_latency_seconds` | `exchange` |
| `rabbitkit_in_flight_messages` (gauge) | `queue` |
| `rabbitkit_broker_connected`, `rabbitkit_consumer_active`, `rabbitkit_worker_pool_pending` (gauges) | |

Alert on a sustained `status="unknown"` rate (reconcile), on
`retry_handoff_failures_total` growth (destination outage), and on
`settlement_items_total{status="stale"}` (channel churn under deferred acks).

---

## Measured: bulk vs single

Real numbers from `python -m benchmarks.bench_bulk` (details, caveats and
how to reproduce in [Benchmarking → Tier 2b](benchmarking.md#tier-2b-bulk-vs-single-python--m-benchmarksbench_bulk)):

| Scenario | What it does | Median msg/s | Wire frames / msg | Accounting |
|---|---|---|---|---|
| publish single / async | `await broker.publish(e)` one after another | **932** | 1 publish + 1 confirm | `.ok` per call |
| publish gather / async ×10 | hand-rolled `asyncio.gather` under a 10-slot semaphore | 4,685 | same | none (you write it) |
| `publish_many` ×10 (default) | default options → in-flight capped to the 10-channel pool | 4,548 | same | one `BulkPublishItem` per input |
| `publish_many` ×64 | `PoolConfig(channel_pool_size=64)`, `max_in_flight=64` | 8,951 | same | same |
| `publish_many` + `AsyncBatchPublisher` | batch config, in-flight 256 (pipelined confirms) | **9,862** | same | same |
| publish single / sync | `SyncBroker.publish` one after another | 833 | same | `.ok` per call |
| `publish_many` / sync | `SyncBroker.publish_many` (sequential by design) | 1,015 | same | one item per input |
| ack each | `await msg.ack_async()` per delivery | 15,308 | 1.000 | — |
| `ack_many` ×100 | park 100, one `ack_many` call | 11,808 | 1.000 (20 API calls) | one `SettlementItem` per delivery |
| `CoalescingAcker` ×100 | register/complete, in-order completion | **15,670** | **0.010** (20 frames for 2,000) | coordinator ledger |

Environment: Apple Silicon (arm64, 12 cores), Python 3.12.2, RabbitMQ 3.13
in Docker on the same machine, 2,000 messages × 1 KiB, persistent,
confirms on, `mandatory=True`, durable classic queue, median of 3 runs,
git `ed2e8bc`. Every scenario reported 2,000/2,000 CONFIRMED (or settled)
and the management API showed the queue drained to 0 ready / 0 unacked.
Absolute numbers are this machine's; the ratios are what travel.

In one sentence each: **bulk publish** is worth ~5× over a sequential loop
at default settings and ~10× with a bigger pool or the batch publisher, and
it costs nothing over hand-rolled concurrency while adding per-item
outcomes; **sync bulk** adds outcomes, not speed; **`ack_many`** is a
correctness API (one frame per delivery by design), not a throughput one;
**`CoalescingAcker`** is the wire optimisation — 100× fewer ack frames when
completions arrive in order, never across an unfinished sibling.

## The settlement safety model

Coalescing is a correctness-critical state machine, not just a performance
optimization, so these eight invariants are enforced in code and pinned by
tests (`tests/unit/core/test_settlement_state_machine.py`, the Hypothesis
machine in `tests/property/`, and real-broker runs in
`tests/integration/test_settlement_chaos.py`).

| | Invariant | Enforced by |
|---|---|---|
| **I1** | Never cumulative-ack across an unsafe gap | `plan()` coalesces only contiguous ack-safe runs |
| **I2** | Never settle a delivery from another channel/generation | `channel_key` binding + per-generation ledger |
| **I3** | Never ack a delivery that did not reach ACK_READY | `SUCCESS` reachable only from `OUTSTANDING`/`RETRY_PENDING` |
| **I4** | A contradictory settlement is never silently accepted | transition table + `ContradictorySettlementError` |
| **I5** | Channel loss invalidates every pending settlement | `invalidate()` / `on_reconnect()` drop the whole ledger |
| **I6** | Shutdown may leave messages unacked, never acks to flush | `drain_plan()` emits only approved work |
| **I7** | Optimization failure costs throughput, never semantics | `max_pending` raises; it never relaxes the ack rules |
| **I8** | Coalescing changes frame count, not settlement semantics | same tags, same kinds, with `coalesce` on or off |

The last one is the design principle: **turning coalescing on or off changes
performance and wire traffic, never which messages your application considers
processed.**

### The delivery state machine

A delivery being *finished* is not the same as being *safe to ack*:

```
OUTSTANDING ──┬─> SUCCESS         ack-safe; advances the frontier
              ├─> NACK            emitted individually, then settled
              ├─> REJECT          emitted individually, then settled
              ├─> RETRY_PENDING ─┐  owned by the retry path; blocks
              ├─> FAILED         │  blocks the frontier, never emitted
              └─> CANCELLED      │  blocks the frontier, never emitted
                                 └─> SUCCESS / NACK / REJECT
```

`FAILED` (`acker.abandon(tag)`) and `CANCELLED` (`acker.cancel(tag)`) are
finished but never acked — the delivery stays unacked so the broker
redelivers it. Neither can ever become `SUCCESS`; both may still be settled
explicitly with a nack or reject.

The **frontier** is the end of the leading ack-safe run, *not* the highest
completed tag. Given 101 done, 102 done, 103 running, 104 done, 105 done,
the maximum safe cumulative ack is `ack(102, multiple=True)` — never 105.

### Nack in the middle

A nack or reject settles its own tag on the wire, so the run *after* it can
form a new cumulative range (commands are always emitted in ascending tag
order, so the lower frames land first):

```
101 SUCCESS  102 SUCCESS  103 NACK  104 SUCCESS  105 SUCCESS
→ ack(102, multiple=True), nack(103), ack(105, multiple=True)
```

A *blocking* delivery is different: once one is reached, nothing above it
may be coalesced in that plan.

### Duplicate and contradictory settlement

Repeating the same decision is an idempotent no-op. Changing it is refused:

| Call | Result |
|---|---|
| `complete(t)` twice | no-op |
| `fail(t, requeue=True)` twice | no-op |
| `fail(t, requeue=True)` then `fail(t, requeue=False)` | `ContradictorySettlementError` |
| `complete(t)` then `fail(t)` | `ContradictorySettlementError` |
| settling a tag already emitted | `ContradictorySettlementError` |
| settling a tag that was never registered | `UnknownDeliveryError` |
| settling a tag from a dropped generation | `StaleGenerationError` |

All four subclass `CoordinatorError`, so one `except` catches them.

### Requeue and redelivery

A nacked-with-requeue message comes back with a **new, higher** delivery tag.
Registration is strictly increasing, so the old tag can never be reused and
the redelivery is simply a new delivery.

### Bounds

`CoalescingAcker(max_pending=N)` caps the ledger. Exceeding it raises
`LedgerFullError` from `register()`, so you apply backpressure (stop
consuming, lower prefetch); the delivery stays unacked and the broker
redelivers it. A full ledger **never** relaxes the ack rules.

Watch `oldest_pending_age` and `gap_count` to see a straggler holding the
frontier back before the ceiling is reached.

### Instrumentation

`acker.metrics` (and `group.metrics`, summed) exposes `registered`,
`pending`, `outstanding`, `ack_ready`, `frontier`, `gap_count`,
`oldest_pending_age`, `settled`, `coalesced`, `frames_sent`,
`coalescing_ratio`, `invalidations` and `dropped_on_invalidate`. Pass
`collector=` and `metrics_config=` to have the gauges emitted on every flush
(see [Observability](observability.md)).

`coalescing_ratio` is deliveries settled per frame sent — the benchmark's
2,000 deliveries in 20 frames is a ratio of 100.

## Transactional outbox and inbox

The end-to-end at-least-once recipe. `examples/bulk_operations/07_transactional_outbox_inbox.py`
runs it with SQLite.

**Outbox (producer side).** Write the business row and the outbox row in one
database transaction. A relay claims a bounded page of unpublished rows,
calls `publish_many` with `message_id` = outbox id, and marks **only the
CONFIRMED rows** as published. `UNKNOWN` rows stay claimed for
reconciliation; `NOT_SENT` / `INVALID` rows are released for the next page.
A crash between publish and mark yields a duplicate on the wire, by design.

**Inbox (consumer side).** Insert `(consumer, event_id)` into an inbox table
and apply the business effect in the same transaction. A duplicate key means
"already processed": ack without re-applying. Ack **after** the commit, via
`ack_many` for the exact committed subset. If one event fans out to several
tenants, put the tenant in the idempotency key.

Do not present a Redis deduplication marker as atomic with a database write;
the inbox row is what makes the effect idempotent. Claim leases, lease
renewal and dedup retention must outlive the longest replay you allow.

## DLQ replay

Replay through the existing `DLQInspector` / `rabbitkit dlq replay` with an
explicit destination, a bounded rate and a stable original `message_id`
plus a replay audit id. Do not default to replaying everything, and do not
reset attempt counters indefinitely: a replayed message re-enters the retry
ladder at attempt 0 by design, so bound replays by a budget.

## FAQ

**Is there a "publish by id" or "pluck from the queue" API?** No, and there
cannot be one: RabbitMQ queues are not queryable stores and a message id is
not a delivery handle. Selection always happens in application code (a paged
query), and `publish_many` takes the resulting envelopes. Deliveries are
settled by the `RabbitMessage` objects the broker handed you, never by id.

**Does aio-pika / pika have bulk publish or bulk ack?** No. The only bulk
primitive either client exposes is `basic_ack(multiple=True)`, which is
exactly what `CoalescingAcker` decides *when* it is safe to call. Everything
else here (per-item outcomes, bounded admission, stale-handle detection) is
built on the clients' ordinary `publish`/`ack`/`nack`.

**Why is `SENT` reported as `UNKNOWN`?** With confirms off the frame was
written to the socket and nothing more is known. Reporting it as confirmed
would make a lost publish look successful.

**Why does `publish_many` refuse more than `max_items`?** A bulk call is a
bounded collection you hold in memory; refusing up front (before publishing
anything) beats discovering the bound halfway through. Use `iter_publish`
for streams.

## Examples

Runnable, CI-smoke-tested against a real broker:
[`examples/bulk_operations/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/bulk_operations)
— async and sync `publish_many`, streaming `iter_publish`, batch-commit
`ack_many`, `CoalescingAcker` with out-of-order completion, critical-profile
preflight against the management API, sanitized headers and handoff backoff
(no broker needed), the transactional outbox/inbox, and two-queue
per-channel ack isolation.

## Migration notes

* `BatchAcker` default changed to individual acks. If you relied on the
  cumulative frame **and** you are the channel's sole settler with in-order
  completions, opt in with `BatchAckConfig(mode="cumulative",
  ordered_exclusive_owner=True)`. Otherwise use `CoalescingAcker`.
* `BatchPublisher.flush()` still returns an `int`, but it now counts only
  items whose publish did not fail locally, and a publish that **raises**
  surfaces as `BatchFlushError` carrying a `FlushReport` with the unsent tail
  (nothing is re-buffered silently). `add()` after `close()` raises.
* Retry DLQ headers are sanitized by default. Set `error_detail="raw"` to
  keep the old text.
* No existing queue is re-declared by any of these features. Profiles and
  queue-type knobs apply to new topology; migrate deliberately.
