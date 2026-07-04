# Retry and Dead-Letter Queues

RabbitKit provides a built-in retry system based on RabbitMQ dead-letter exchanges and per-queue TTL. Messages that fail are routed through a series of delay queues before being promoted to a dead-letter queue (DLQ) if all retry attempts are exhausted.

---

## Every Route Gets a Dead-Letter Path by Default

In RabbitMQ, a message rejected with `requeue=False` is **permanently discarded** unless the queue has a dead-letter exchange. A plain subscriber with no retry can still reject — a handler raising `ValueError` on a malformed payload is classified as a permanent error and rejected. To prevent silent loss, RabbitKit auto-provisions a `{queue}.dlq` for **every** route that can reject, controlled by `SafetyConfig.reject_without_dlx`:

| Policy | Behavior | Use |
|---|---|---|
| `"auto_provision"` (default) | Declares `{queue}.dlq` and wires the source queue's DLX to it | Most consumers |
| `"error"` | Startup fails with `UnsafeTopologyError` if a rejecting route has no DLX | Topology managed externally (Terraform/definitions) |
| `"discard"` | Rejected messages may be discarded; warns once per route | Low-value/ephemeral data only — explicit opt-in to loss |

```python
from rabbitkit import RabbitConfig, SafetyConfig, RejectWithoutDLXPolicy

config = RabbitConfig(safety=SafetyConfig(reject_without_dlx=RejectWithoutDLXPolicy.ERROR))

# Or per route:
@broker.subscriber(queue="low-value-telemetry", reject_without_dlx="discard")
def handle(body: bytes) -> None: ...
```

Retry-enabled routes and queues with a manually configured `dead_letter_exchange` already have a dead-letter path and are left untouched. `ACK_FIRST` routes (which ack before the handler and can never reject) are skipped. The policy applies only under `TopologyMode.AUTO_DECLARE` — in `PASSIVE_ONLY`/`MANUAL` modes RabbitKit does not own queue arguments and cannot know whether an externally managed DLX exists.

**Upgrading an existing deployment:** auto-provisioning adds `x-dead-letter-exchange`/`x-dead-letter-routing-key` to the source queue's declare arguments. RabbitMQ rejects re-declaring an existing queue with different arguments (406 `PRECONDITION_FAILED` → a clear `ConfigurationError` at startup). Either delete/re-create the queue, apply the matching arguments via a policy on the broker, switch that route to `reject_without_dlx="discard"`/`"error"`, run with `TopologyMode.PASSIVE_ONLY`, or set `SafetyConfig(on_topology_conflict="warn_continue")` — which warns and continues using the *existing* queue definition for just the conflicting queues while still declaring the rest (unlike `PASSIVE_ONLY`, which skips declaration entirely).

---

## Full Topology Example

For a queue named `orders.created`, RabbitKit declares the following topology automatically when `RetryConfig` is provided:

```
orders.exchange (topic)
        |
        | routing key: orders.created
        v
orders.created.queue  ──[DLX]──> orders.retry.exchange
        |                                |
        |                       ┌────────┴─────────┐
        |                       v                   v
        |              orders.created.retry.5s   orders.created.retry.30s
        |              (TTL=5000ms, DLX back     (TTL=30000ms, DLX back
        |               to orders.exchange)       to orders.exchange)
        |
        |  (after max_retries exhausted)
        v
orders.created.dlq
```

Each retry queue is a standard durable queue with a `x-message-ttl` and a `x-dead-letter-exchange` pointing back to the original exchange. When the TTL expires, the message is re-routed to the original queue for reprocessing.

---

## How Retry Queues Are Declared with TTL

For `RetryConfig(max_retries=3, delays=(5, 30, 120))`, RabbitKit declares:

| Queue | x-message-ttl | x-dead-letter-exchange |
|---|---|---|
| `orders.created.retry.5s` | 5000 ms | `orders.exchange` |
| `orders.created.retry.30s` | 30000 ms | `orders.exchange` |
| `orders.created.retry.120s` | 120000 ms | `orders.exchange` |

The number of retry queues equals the number of delay values in the `delays` tuple. If `max_retries` exceeds `len(delays)`, the last delay is reused for subsequent retries.

---

## How Retry Count Is Tracked

The retry count is stored in the AMQP message header `x-retry-count`. On each retry delivery:

1. RabbitKit reads `x-retry-count` from the incoming message headers.
2. It increments the value and sets it on the outbound retry message.
3. The incremented value is used to select the appropriate delay queue for the current attempt.

Headers are preserved across re-deliveries. If `x-retry-count` is absent, it is treated as `0` (first attempt).

---

## The Retry-Count Header Is Not Trusted Input (H5)

The retry-count header is read verbatim from an inbound AMQP message — there
is no broker-side attestation distinguishing a value this middleware wrote
during its own delay-queue round trip from one a producer set directly.
`RetryMiddleware` clamps every value read from the header to `[0,
max_retries]` before using it, regardless of what the header claims:

- A negative value (e.g. a producer trying to reset the counter for
  unbounded retries) clamps to `0` and is treated as a fresh first attempt —
  it can never produce a negative attempt number, which would otherwise
  target a delay queue like `orders.retry.-4` that was never declared (the
  publish would silently target a non-existent queue on the default exchange
  and the message would be lost, not retried).
- An absurdly large value (e.g. a producer trying to force every message
  straight to the DLQ, skipping retries) clamps to `max_retries` and is
  treated as exhausted.
- A non-numeric/malformed value is treated the same as a missing header (`0`)
  rather than raising, so a garbage header degrades gracefully instead of
  crashing the pipeline.

This makes `max_retries` an enforced ceiling independent of the header's
configured value being read from a trusted or untrusted source — but it is
still an application-level check, not a broker-enforced one. For a
broker-enforced backstop on top of this (e.g. against a misbehaving consumer
that never settles a message, independent of anything RetryMiddleware does),
prefer **quorum queues** for the source queue and set `x-delivery-limit` — the
broker itself dead-letters a message after that many redeliveries, with no
dependency on any header at all.

### Crash-loop backstop (M5)

The header count only advances on the delay-queue round trip
(`RetryMiddleware` acks the source and republishes). A handler that *crashes
the process* mid-message (OOM, segfault, SIGKILL) never acks, so the broker
redelivers the same message forever — the header never increments and the
retry ladder is never engaged. The broker-side backstop for this is a quorum
source queue with `x-delivery-limit`: it counts *redeliveries* and
dead-letters once the limit is hit, regardless of the header. rabbitkit
preserves both the quorum type and the delivery limit when retry re-declares
the source queue with its DLX routing, so the two compose:

```python
from rabbitkit import RabbitQueue, QueueType, RetryConfig

@broker.subscriber(
    queue=RabbitQueue(name="orders", queue_type=QueueType.QUORUM, delivery_limit=20),
    retry=RetryConfig(max_retries=4, delays=(5, 30, 120, 600)),
)
def handle(order: Order) -> None:
    ...
# Normal retries ack+republish (source redelivery count stays ~0 per cycle),
# so delivery_limit only trips on a true crash-loop → message dead-lettered
# to orders.dlq by the broker. Keep delivery_limit comfortably above the
# redeliveries a normal restart might cause.
```

---

## What Happens After max_retries

When `x-retry-count` equals `max_retries` and the handler raises again (or the
error is permanent), `RetryMiddleware` tags the exception `_rabbitkit_terminal`
and re-raises it rather than routing to another delay queue. The pipeline
recognizes that marker and rejects the message
(`message.reject(requeue=False)`) instead of applying the normal AckPolicy
classification -- this is what stops an exhausted *transient* error from being
re-classified as transient again and `nack(requeue=True)`'d into a hot loop.

RabbitMQ itself then routes the rejected message to the DLQ, via the
`x-dead-letter-exchange` / `x-dead-letter-routing-key` arguments the broker set
on the *source* queue when it declared the retry topology -- rabbitkit does not
publish to the DLQ directly. The DLQ is a plain durable queue with no TTL.
Messages accumulate there until replayed or discarded manually.

---

## Validated against a real broker

Both halves of retry -- the delay-queue topology *and* the `RetryMiddleware`
that actually routes failures into it -- are exercised end-to-end against a
real RabbitMQ broker (via [testcontainers](https://testcontainers-python.readthedocs.io/),
not a mock) in
[`test_async_retry_exhaustion_to_dlq`](https://github.com/talaatmagdy/rabbitkit/blob/main/tests/integration/test_real_rabbitmq.py).
That test raises a transient error, asserts the handler is called exactly
`max_retries + 1` times (proving the delay queue actually redelivers), and
consumes the `.dlq` queue to confirm the exhausted message lands there. It runs
on every PR and nightly. See the README's
[Real-broker integration tests](https://github.com/talaatmagdy/rabbitkit#real-broker-integration-tests)
section for how to run it locally.

---

## Code Example

```python
from rabbitkit import AsyncBroker, RabbitConfig
from rabbitkit.core.config import RetryConfig

config = RabbitConfig(url="amqp://guest:guest@localhost/")
broker = AsyncBroker(config)

retry = RetryConfig(
    max_retries=3,
    delays=(5, 30, 120),  # seconds: attempt 1→5s, attempt 2→30s, attempt 3→120s
)

@broker.subscriber(
    queue="orders.created",
    retry_config=retry,
)
async def handle_order(order_id: str) -> None:
    await process_order(order_id)


async def main() -> None:
    async with broker:
        await broker.start()
        # broker runs until shutdown signal
```

With this configuration:

- First failure: message waits 5 seconds in `orders.created.retry.5s` then redelivered.
- Second failure: message waits 30 seconds in `orders.created.retry.30s`.
- Third failure: message waits 120 seconds in `orders.created.retry.120s`.
- Fourth failure: message is published to `orders.created.dlq`.

---

## Error Classification

Not all errors should be retried. RabbitKit distinguishes transient errors (infrastructure problems that may resolve) from permanent errors (data problems that will always fail).

| Exception | Class | Action |
|---|---|---|
| `TimeoutError` | Transient | Retry |
| `OSError` | Transient | Retry |
| `ConnectionError` | Transient | Retry |
| `aio_pika.exceptions.AMQPConnectionError` | Transient | Retry |
| `json.JSONDecodeError` | Permanent | DLQ |
| `pydantic.ValidationError` | Permanent | DLQ |
| `ValueError` (schema mismatch) | Permanent | DLQ |
| `KeyError` (missing required field) | Permanent | DLQ |

To mark an exception as permanent and route directly to DLQ without retrying, raise `rabbitkit.core.errors.PermanentError` (or a subclass) from your handler:

```python
from rabbitkit.core.errors import PermanentError

async def handle_order(payload: dict) -> None:
    if "order_id" not in payload:
        raise PermanentError("Missing order_id — cannot process")
    ...
```

---

## Inspecting the DLQ

Use the RabbitKit CLI to view messages sitting in a DLQ without consuming them:

```bash
rabbitkit dlq inspect orders.created.dlq
```

This prints a summary of messages including headers, routing key, original exchange, and failure reason if recorded.

To see the full body of each message:

```bash
rabbitkit dlq inspect orders.created.dlq --full
```

---

## Replaying from the DLQ

To re-publish all messages from a DLQ back to the original exchange for reprocessing:

```bash
rabbitkit dlq replay orders.created.dlq orders
```

The second argument is the target exchange name. Messages are republished with the original routing key and headers, with `x-retry-count` reset to `0` so the full retry budget is available again.

To replay only a subset:

```bash
rabbitkit dlq replay orders.created.dlq orders --limit 10
```

Replay uses publisher confirms. If a message fails to publish, replay stops and reports the error rather than silently dropping messages.


## Retry jitter: `jitter_mode="sharded"`

By default every message that fails at the same moment retries at the same
moment — the delay-queue TTLs are exact, so a burst of correlated failures
re-hammers the recovering dependency as a phase-locked wave. Per-message
TTL jitter is NOT an option here: mixed TTLs in one classic queue
reintroduce head-of-line blocking (a long-TTL message parks everything
behind it).

`RetryConfig(jitter_mode="sharded", jitter_shards=3, jitter_factor=0.1)`
decorrelates the wave while keeping every queue's TTL uniform: each tier
becomes N sub-queues whose TTLs stagger across ±`jitter_factor`
(`orders.retry.2` at 30s, `.s1` at 27s, `.s2` at 33s), and a message picks
its shard by a **stable** hash of its `message_id` — the same message keeps
the same cadence across redeliveries and processes. Shard 0 keeps the
legacy queue name and exact TTL, so enabling this on an existing topology
is purely additive: no 406s, no migration. The default `"off"` produces
byte-identical topology to previous releases.
