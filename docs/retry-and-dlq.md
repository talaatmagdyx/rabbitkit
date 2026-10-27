# Retry and Dead-Letter Queues

RabbitKit provides a built-in retry system based on RabbitMQ dead-letter exchanges and per-queue TTL. Messages that fail are routed through a series of delay queues before being promoted to a dead-letter queue (DLQ) if all retry attempts are exhausted.

---

## Durability, sanitized triage headers, and handoff failures (0.12)

Three `RetryConfig` knobs make the retry chain's reliability explicit. All
defaults preserve the legacy topology, so upgrading never re-declares a queue:

| Knob | Values | Default |
|---|---|---|
| `delay_queue_type` | `classic`, `quorum`, `inherit` (match the source queue) | `classic` |
| `dlq_queue_type` | `inherit` (quorum when the source is quorum), `classic`, `quorum` | `inherit` |
| `error_detail` | `sanitized` (redacted + capped), `omit`, `raw` (legacy) | `sanitized` |
| `handoff` | `RetryHandoffConfig(...)` — bounded backoff when the **retry publish** itself fails | bounded |

The DLQ triage headers carry `x-rabbitkit-error-category` (the failure
category, see [Error Classification](#error-classification)) alongside
`x-rabbitkit-error-type`, and
`x-rabbitkit-error-message` is redacted (URL passwords, `password=`/`token=`
pairs, bearer tokens, long opaque tokens) and length-capped. A failed
delay-queue handoff never acks the source; it is nack-requeued and counted in
`rabbitkit_retry_handoff_failures_total` with capped exponential backoff. See
[Bulk Operations & Reliability Profiles](bulk-operations.md#retry-hardening).

---

## Retry policy

| Field | Meaning | Default |
|---|---|---|
| `max_retries` / `delays` | the ladder; one fixed-TTL delay queue per tier (whole seconds) | `4` / `(5, 30, 120, 600)` |
| `RetryConfig.exponential(max_retries=, initial_delay=, multiplier=, max_delay=)` | builds `delays` as `min(max_delay, initial_delay * multiplier**i)` | — |
| `max_retry_age` | seconds since the FIRST failure after which a transient failure is terminal (`retry-age-exceeded`) | `None` (unbounded) |
| `terminal_action` | `"dead_letter"`: confirmed, metadata-stamped copy to `<queue>.dlq`, then ack; `"reject"`: legacy broker DLX | `"dead_letter"` |
| `jitter_mode` / `jitter_shards` / `jitter_factor` | TTL spread across shard queues (see the end of this page) | off |
| `unknown_policy` | severity for exceptions no predicate/type tuple classifies | `PERMANENT` |

```python
from rabbitkit import RetryConfig

retry = RetryConfig.exponential(max_retries=5, initial_delay=2, multiplier=3, max_delay=300, max_retry_age=3600)
# delays == (2, 6, 18, 54, 162)
```

There is no immediate-retry mode: every retry waits in a delay queue, and a
failed handoff to the delay queue is braked before the message can come back
(see *Handoff failures* below), so a failure can never spin the handler.

**Handler settlement signals are not failures.** `AckMessage`,
`NackMessage` and `RejectMessage` raised by a handler pass straight through
`RetryMiddleware` — whatever `unknown_policy` or a predicate says — and the
pipeline settles exactly as asked.

### Handoff failures

If the publish to the delay queue is returned, nacked, times out or raises
(the delay queue was deleted, a broker alarm), the source is never acked. On
async routes the middleware **holds the delivery** for the backoff
(`RetryHandoffConfig`: capped exponential, `backoff_max` 30 s by default,
cancellable) and only then nack-requeues it, so the broker cannot hand it
straight back. Once the tracker is `EXHAUSTED` (`max_consecutive_failures` or
`recovery_deadline`), every failing delivery is held for the full
`backoff_max` — consumption of that queue is effectively paused at
`prefetch` messages per `backoff_max` — and one CRITICAL log line is written
per outage; wire `on_handoff_exhausted` to stop the route if you prefer.
The sync path never sleeps on pika's I/O thread; it reports the backoff via
`on_handoff_failure`.

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

### A missing DLQ never swallows a message

The dead-letter path only protects a message if the DLQ exists when the
message is rejected. A classic queue (and a quorum queue without
`dead-letter-strategy: at-least-once`) dead-letters at most once: if the
target queue is gone, RabbitMQ drops the message and nobody is told.
Measured on 3.13 and 4.1: a permanent failure after the auto-provisioned
`{queue}.dlq` was deleted lost the message on both queue types.

The async broker therefore checks that the DLQ exists before it
dead-letters anything (handler failures, retry-exhausted messages,
`RejectMessage(requeue=False)`, `NackMessage(requeue=False)`,
`NACK_ON_ERROR`). If it is missing, the message is requeued instead, after
a backoff that starts at 0.5 s and doubles to 5 s while the DLQ stays
missing, and an ERROR is logged once per 10 s window
(`HandlerPipeline.dead_letter_blocked_total` counts them). The message
survives until an operator restores the DLQ; restarting the service with
`TopologyMode.AUTO_DECLARE` recreates it.

Limits: the check covers dead-letter targets reached through the default
exchange (`x-dead-letter-exchange: ""` with the DLQ name as the routing
key), which is what the retry and `auto_provision` paths declare. A
dead-letter *exchange* with its own bindings is not verified. A positive
check is cached for 5 s, so a DLQ deleted inside that window can still drop
a message. The sync broker does not run the check yet.

A quorum source declared with `dead_letter_strategy="at-least-once"` is not
probed: the broker itself keeps a dead-lettered message on the source until
the DLQ exists again (`messages_dlx` in the management API), so the message
is rejected at once and the handler is not run again. Every other source
gets the requeue-with-backoff above; once the DLQ is back, the next failure
lands there with the full metadata. Verified on RabbitMQ 4.1 for both
strategies.

---

## Full Topology Example

For a queue named `orders.created`, RabbitKit declares the following topology automatically when `RetryConfig` is provided:

```
orders.exchange (topic)
        |
        | routing key: orders.created
        v
orders.created.queue ──(transient failure: middleware republishes)──┐
        ^                                                            v
        |                                     orders.created.queue.retry.1
        |                                     (TTL=5000ms, DLX="" routed
        |                                      back by QUEUE NAME)
        └──(TTL expiry dead-letters via the ────┘
            DEFAULT exchange, routing key =
            "orders.created.queue")
        |
        |  (after max_retries exhausted: reject → source queue's own DLX)
        v
orders.created.queue.dlq
```

There is **no retry exchange**. Each delay queue is a standard durable
classic queue with a uniform queue-level `x-message-ttl` and
`x-dead-letter-exchange: ""` (the **default exchange**) with the source
queue's *name* as the dead-letter routing key. On the default exchange a
routing key that equals a queue's name always delivers directly to that
queue — completely independent of how the source queue is bound to its
real exchange. (Dead-lettering back through the source's own exchange —
what an earlier design did — silently loses the message whenever the queue
is bound by a pattern like `orders.*` rather than literally by its name.)

---

## How Retry Queues Are Declared with TTL

For `RetryConfig(max_retries=3, delays=(5, 30, 120))` on a queue named
`orders.created.queue`, RabbitKit declares one delay queue **per attempt**:

| Queue | x-message-ttl | x-dead-letter-exchange | x-dead-letter-routing-key |
|---|---|---|---|
| `orders.created.queue.retry.1` | 5000 ms | `""` (default) | `orders.created.queue` |
| `orders.created.queue.retry.2` | 30000 ms | `""` (default) | `orders.created.queue` |
| `orders.created.queue.retry.3` | 120000 ms | `""` (default) | `orders.created.queue` |

One delay queue per retry attempt, each with a **uniform** queue-level TTL
(never per-message TTL — RabbitMQ only expires messages at the queue head,
so mixed TTLs in one queue would let a long delay block expired short ones).
By default `RetryConfig` requires `len(delays) >= max_retries`
(`strict_delays=True` raises `ConfigValidationError` otherwise); pass
`strict_delays=False` to reuse the last delay for the remaining attempts.

---

## How Retry Count Is Tracked

The retry count is stored in the AMQP message header `x-rabbitkit-retry-count` (configurable via `RetryConfig.retry_header`). On each retry delivery:

1. RabbitKit reads `x-rabbitkit-retry-count` from the incoming message headers
   and takes the larger of it and the delay tiers the broker's `x-death`
   history shows (`<queue>.retry.<n>` ⇒ at least `n` retries happened).
2. It increments the value and sets it on the outbound retry message — never
   lowering a larger value already on the message.
3. The incremented value is used to select the appropriate delay queue for the current attempt.

Headers are preserved across re-deliveries. If `x-rabbitkit-retry-count` is absent, it is treated as `0` (first attempt).

### Metadata on every retry and DLQ copy

| Header | Meaning |
|---|---|
| `x-rabbitkit-retry-count` | retries done so far (selects the delay tier) |
| `x-rabbitkit-attempts` | handler executions including this one (never lowered) |
| `x-rabbitkit-original-message-id` | the id every copy carries; an id-less message gets one on its first handoff |
| `x-rabbitkit-original-queue` / `-exchange` / `-routing-key` | where the message was consumed / originally published |
| `x-rabbitkit-first-failed-at` / `-last-failed-at` | ISO-8601; first is set once (the `max_retry_age` clock) |
| `x-rabbitkit-error-class` / `-error-type` / `-error-message` | the CURRENT attempt's exception class and sanitized message |
| `x-rabbitkit-error-category` | the CURRENT attempt's failure category: `retryable`, `permanent`, `invalid_message`, `unknown_outcome`, `infrastructure` (see [Error Classification](#error-classification)) |
| `x-rabbitkit-failure-category` | `transient` / `timeout-unknown` on a retry copy; `permanent` / `exhausted` / `retry-age-exceeded` on a DLQ copy |
| `x-rabbitkit-dead-lettered-at` | DLQ copies only |
| `x-rabbitkit-original-expiration` | the producer's `expiration`, which is not copied onto delay/DLQ copies |

The AMQP properties are preserved too: `message_id`, `correlation_id`,
`timestamp`, `type`, `app_id`, `user_id`, `priority`, `reply_to`,
`content_type`, `delivery_mode`, and every application header (trace context
such as `traceparent`, idempotency keys).

**The body and `content_encoding` are the ones that came off the wire.**
`CompressionMiddleware` decompresses `message.body` in place (and leaves
`content_encoding` as delivered), so a copy built from `message.body` would
carry a decoded body labelled `gzip`; before this was fixed every retried
compressed message failed to decompress on its next delivery and went to the
DLQ. Every retry and DLQ copy is built from `RabbitMessage.wire_body` /
`wire_content_encoding`, which an `on_receive` hook cannot change, so the copy
decodes exactly like the original (verified on a real broker: a compressed
message that fails once succeeds on attempt 2, and its terminal DLQ copy is
still gzip).
`expiration` is the one exception: on a delay queue it would expire the copy
early and collapse the backoff; on the DLQ it would delete the failure
record.

**Trust model.** These headers are client-controlled. rabbitkit never lowers
them and clamps the count to `max_retries`, but a producer that strips every
header (including `x-death`) restarts the budget. Bound that case with
`max_retry_age` and, against crash loops, a quorum `x-delivery-limit`.

A producer that sets `x-rabbitkit-retry-count` HIGH skips the retries. With
`RetryConfig(trust_retry_header=False)` the header is not read at all: the
count comes only from the `x-death` entries RabbitMQ adds when a copy expires
out of `<queue>.retry.<n>`, so a forged count changes nothing (verified on a
real broker: a message published with `x-rabbitkit-retry-count: 99` still
gets every retry). The header is still written for observability. The
default stays `True` because `TestBroker` writes no `x-death`, and suites
that simulate a later attempt by setting the header rely on it.

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

### Stripping reserved headers from application publishes (G-19)

`RetryConfig(trust_retry_header=False)` ignores the header and counts only the
broker's `x-death` history, but a producer can put a forged `x-death` (or
`x-rabbitkit-first-failed-at`, which drives `max_retry_age`) on a publish too.
`PublisherConfig(strip_reserved_headers=True)` removes rabbitkit's
retry/DLQ/replay headers, `x-death` and a custom `retry_header` from every
`broker.publish()` (and `publish_many` / `iter_publish`), logging one WARNING
per header name. rabbitkit's own retry and DLQ copies and `DLQInspector`
replays do not go through `broker.publish()` and keep them. Both settings are
off by default and on in the CRITICAL profile (see
[Reliable Messaging](reliable-messaging.md#reserved-headers)).

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

A failure is terminal when the error is permanent, the retries are exhausted
(`x-rabbitkit-retry-count` equals `max_retries`), or `max_retry_age` has
passed since the first failure. With the default
`terminal_action="dead_letter"`, `RetryMiddleware` then publishes a copy to
`<queue>.dlq` (mandatory, confirm-gated) carrying the CURRENT attempt's
metadata (table above), and only after the broker confirms it acks the
source — the same confirmed-publish-then-ack pattern as a retry. The
exception is re-raised tagged `_rabbitkit_terminal` (so an outer
`ExceptionMiddleware` still sees it) and `_rabbitkit_dead_lettered`, which
tells the pipeline the message is already settled.

If that DLQ publish fails (DLQ missing, returned, nacked, timed out), the
middleware hands the delivery back to the pipeline, which settles it exactly
once:

| DLQ | Source | What happens |
|---|---|---|
| exists (publish failed for another reason) | any | reject; RabbitMQ moves it via the source's `x-dead-letter-*` arguments (only `x-death`, not the terminal metadata) |
| missing | quorum, `dead_letter_strategy="at-least-once"` | reject; the broker keeps it on the source until the DLQ is back; the handler is not run again |
| missing | anything else (async broker) | requeued after a 0.5 s to 5 s backoff (see *A missing DLQ never swallows a message*); once the DLQ is back the next failure lands there with metadata |

The exception carries `_rabbitkit_dead_letter_failed`: an outer
`ExceptionMiddleware` re-raises it even with `swallow_permanent=True` or a
matching fallback handler, because recovering it would ack a message that
exists nowhere else. `terminal_action="reject"` always takes the broker
path.

Note for `TestBroker` users: on the default path the consumed message's
disposition is `acked` (the DLQ copy is in `broker.published_messages`), not
`rejected`.

The DLQ is a plain durable queue with no TTL. Messages accumulate there until
replayed or discarded manually.

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

Every handler failure is classified into a **category**. The category's
severity decides retry versus DLQ; the category itself is written to
`x-rabbitkit-error-category` on every retry and DLQ copy, labels
`rabbitkit_messages_retried_total` / `rabbitkit_messages_dead_lettered_total`
(`category="..."`), and appears in the pipeline's failure log line.

| Category | Severity | Default examples | What happens (with `RetryConfig`) |
|---|---|---|---|
| `retryable` | transient | `TimeoutError`, `ConnectionError` / `OSError`, `EOFError` | delayed retry |
| `infrastructure` | transient | aio-pika/aiormq `AMQPConnectionError`, `ChannelClosed`, `ChannelInvalidStateError`; pika's connection/channel errors; `InfrastructureError` | delayed retry, labelled separately |
| `unknown_outcome` | transient | `HandlerTimeoutError` (a processing deadline expired) | delayed retry; the handler must be idempotent |
| `permanent` | permanent | `ValueError`, `KeyError`, `TypeError`, ..., and **any unclassified exception** | DLQ on the first attempt |
| `invalid_message` | permanent | body decode/validation failures, `on_receive` failures (bad gzip, bad signature), `MissingDependencyError`, `InvalidMessageError` | DLQ on the first attempt |
| `shutdown` | — | `asyncio.CancelledError`, `KeyboardInterrupt`, `SystemExit` | never retried: requeued, and the cancellation propagates |

Evaluation order: shutdown; a category the exception declares itself
(`rabbitkit_error_category` on the class or instance, which the typed
exceptions below and rabbitkit's decode/`on_receive` tagging set); your
`predicates`; the registered infrastructure errors; the transient and
permanent type tuples; `unknown_policy` (default `PERMANENT`). A declared
category wins over predicates, so a decode failure stays terminal even with
a catch-all predicate or `unknown_policy=TRANSIENT`.

`core/` imports no transport, so the client-library errors are registered by
the brokers when they are constructed
(`rabbitkit.core.errors.register_infrastructure_errors`). aiormq's
`ChannelClosed` is the base of every channel-level close, so a 403/404/406
raised inside a handler is retried too, bounded by `max_retries`.

Classify your own errors with typed exceptions or predicates:

```python
from rabbitkit import ErrorCategory, InfrastructureError, InvalidMessageError, RetryConfig


def handle_order(payload: dict) -> None:
    if "order_id" not in payload:
        raise InvalidMessageError("missing order_id")  # DLQ, category invalid_message
    try:
        save(payload)
    except DatabaseUnavailable as exc:
        raise InfrastructureError("orders DB unavailable") from exc  # retried


def by_http_status(exc: BaseException) -> bool | ErrorCategory | None:
    status = getattr(exc, "status", None)
    if status in (502, 503, 504):
        return ErrorCategory.INFRASTRUCTURE
    if status is not None and 400 <= status < 500:
        return ErrorCategory.PERMANENT
    return None  # no opinion


retry = RetryConfig(max_retries=3, delays=(5, 30, 120))
# RetryMiddleware(retry, predicates=[by_http_status])
```

A predicate may still return `True` (transient) / `False` (permanent).

---

## Inspecting the DLQ

Use the RabbitKit CLI to view messages sitting in a DLQ without consuming them:

```bash
rabbitkit dlq inspect orders.created.dlq
```

This prints a summary of messages including headers, routing key, original exchange, and failure reason if recorded.

`inspect` fetches up to `--limit` messages with `basic_get`, holds them all
unacked until the fetch ends, then requeues them, so each message is shown
once. Before 0.19 it requeued each message straight away; the message went
back to the head and `--limit 20` printed the first one 20 times.

---

## Replaying from the DLQ

To re-publish messages from a DLQ back to the queue each one failed on:

```bash
rabbitkit dlq replay orders.created.dlq ""
```

or through an exchange, by each message's original routing key:

```bash
rabbitkit dlq replay orders.created.dlq orders
```

The second argument is the target exchange name (`""` = default exchange).
**Routing rule, shared by the CLI and `DLQInspector.replay()`:** an explicit
routing key (`--routing-key` / `target_queue`) wins; to the default exchange
a message is routed by its original QUEUE (`x-rabbitkit-original-queue`, then
the broker's `x-last-death-queue` / `x-death`); to a named exchange by its
original ROUTING KEY (`x-rabbitkit-original-routing-key`, then `x-death`).

Headers are kept — including `x-rabbitkit-retry-count`, meaning a previously
max-retried message is terminal after ONE failed attempt and returns to the
DLQ. Pass `--reset-retry-count` to grant a fresh retry ladder: it strips the
count, the broker's `x-death`/`x-first-death-*`/`x-last-death-*` history and
`x-rabbitkit-first-failed-at`, keeping the old count as
`x-rabbitkit-previous-retry-count`.

**Loop prevention.** Each replayed copy carries `x-rabbitkit-replay-count`,
`x-rabbitkit-replayed-at` and `x-rabbitkit-replayed-by`. A message already
replayed `--max-replays` times (default 3; `DLQInspector.replay(max_replays=3)`)
is refused and stays on the DLQ; `--ignore-replay-limit` /
`max_replays=None` lifts that, deliberately.

**Audit.** Every run logs a start record (actor, queue, target, predicate,
limit, reset flag, replay limit) and a finish record with the counts
(replayed, failed, requeued, skipped, refused) to the `rabbitkit.dlq.audit`
logger; route it to your audit sink. `--actor` / `actor=` (default: the OS
user) names who ran it.

**Authorization.** The hard boundary is RabbitMQ's: run replays as a
dedicated user with `read` on the DLQs and `write` on the replay targets
only. On top of that:

- `DLQInspector(transport, authorizer=fn)` calls `fn(ReplayRequest)` before
  every `replay` and `purge`. The request carries the operation, the actor,
  whether the actor was passed explicitly, the queue, the target and the
  options (`max_replays=None` shows a lifted loop limit). `False` or an
  exception raises `DLQOperationDeniedError` before anything is read, and a
  `DLQ replay DENIED` record goes to the audit logger. `purge(queue,
  actor=...)` is audited too.
- The CLI's `--require-actor` (or `RABBITKIT_REPLAY_REQUIRE_ACTOR=1` in a
  shared ops environment) refuses, audited, a replay that does not name its
  `--actor`.

```python
from rabbitkit import DLQInspector, ReplayRequest

ALLOWED = {"alice", "ops-bot"}


def only_named_operators(request: ReplayRequest) -> bool:
    return request.actor_explicit and request.actor in ALLOWED and request.operation == "replay"


def make_inspector(transport: object) -> DLQInspector:
    return DLQInspector(transport, authorizer=only_named_operators)
```

To replay only a subset:

```bash
rabbitkit dlq replay orders.created.dlq orders --limit 10
```

Replay uses publisher confirms and `mandatory=True`, and acks each DLQ
original only after its republish is confirmed. A message whose republish
fails is **left on the DLQ** (requeued once the fetch loop ends) and replay
**continues** with the rest, reporting the failure count (non-zero exit) at
the end — one poison target doesn't strand the remaining replayable messages.

A broker dead-lettered message's own routing key is the DLQ's name, so when
no origin can be resolved a message that would be published straight back
into the DLQ is skipped and left there (`ReplayResult.skipped`; the CLI
prints `SKIPPED`), instead of republishing it into the queue being drained,
which used to make `replay(dlq)` loop forever. Pass `allow_self_replay=True`
to opt out.

Replay copies every property the original carried (`timestamp`,
`delivery_mode`, `expiration` to the millisecond, priority, ...) and leaves
absent ones absent. One exception on the async transport: aiormq stamps a
random `message_id` on any publish that lacks one.

## Quorum DLQs and delivery limits

AMQP 0-9-1 has no browse, so a peek is `basic_get` plus a requeue, and a
**quorum queue counts every requeue as a delivery** (closing the channel
counts too). Past the queue's delivery limit RabbitMQ dead-letters the
message, or drops it if the queue has no dead-letter exchange. rabbitkit's
DLQs have none. Measured on 3.13.7 and 4.1.8:

| | RabbitMQ 3.x | RabbitMQ 4.x |
|---|---|---|
| no limit configured | unlimited | **20** (the default) |
| `x-delivery-limit: -1` | **drops on the first return** | unlimited |
| argument and policy both set | lowest wins | lowest non-negative wins |

So on 4.x, 20 peeks of a quorum DLQ deleted its messages.

**What rabbitkit declares.** A DLQ inherits quorum from a quorum source
queue (retry DLQs and the safety DLX alike). On 4.x a *new* quorum DLQ is
declared with `x-delivery-limit: -1`; on 3.x it gets no limit argument.
RabbitMQ refuses to add or remove that argument on an existing queue (406),
so a DLQ that already exists is not redeclared. Fix an existing 4.x quorum
DLQ with a policy, which needs no redeclare:

```bash
rabbitmqctl set_policy dlq-unlimited '\.dlq$' '{"delivery-limit": -1}' --apply-to quorum_queues
```

RabbitMQ applies only the highest-priority matching policy. If one already
matches the DLQ (for example a `rabbitkit-<queue>-dlq` policy rendered by
`policy_templates()`, priority 10), add `"delivery-limit": -1` to it rather
than creating a second one that would be shadowed.

A route that consumes another route's quorum DLQ declares it the same way.
Note that a 4.x DLQ created by 0.19 carries the argument, so an older
rabbitkit that redeclares it without the argument gets a 406; set
`SafetyConfig(on_topology_conflict="warn_continue")` before rolling back.
In a mixed 3.x/4.x cluster mid-upgrade, the version is the node the
connection reached: let new quorum DLQs be created only once every node runs
4.x.

**What the inspector checks.** Give `DLQInspector` a management client and
`peek` / filtered `replay` read the queue's type, arguments and effective
policy first. They raise `UnsafeToBrowseError` for a quorum queue whose
limit isn't unlimited, and fail closed when they can't tell (unknown broker
version, or statistics not emitted yet):

```python
from rabbitkit import DLQInspector, RabbitManagementClient

inspector = DLQInspector(transport, management=RabbitManagementClient())
```

Without a management client (or when the API answers 404, e.g. a wrong
vhost; `vhost` defaults to the transport's) the inspector stops as soon as a
fetched message carries `x-delivery-count`, a header only quorum queues set
(from a message's second delivery on), and refuses that queue from then on.
That caps the damage at about two deliveries per message. A `replay()`
without a predicate acks what it republishes, so it checks lazily, at the
first message it has to requeue (a failed or skipped one): a message that
keeps failing would otherwise lose a delivery every run.
`check_delivery_limit=False` turns both checks off.

The CLI does the same. `rabbitkit dlq inspect` and `replay` take
`--management-url` (or `RABBITMQ_MANAGEMENT_URL`), stop on
`x-delivery-count` without it, and exit 2 when they refuse.


## Quorum dead-letter strategy and replica count

`RabbitQueue(queue_type=QueueType.QUORUM, dead_letter_strategy="at-least-once",
overflow="reject-publish")` makes the broker keep a dead-lettered message on
the source queue until the dead-letter target confirms it, instead of
dropping it when the DLQ is missing or full (RabbitMQ 3.10+;
`at-least-once` without `overflow="reject-publish"` is rejected at
construction, because RabbitMQ would silently fall back to at-most-once).
Verified on RabbitMQ 4.1: with the DLQ deleted, a terminally failed message
stays on the source (`messages_dlx=1` in the management API); with
`at-most-once` it is gone. `quorum_initial_group_size=3` sets
`x-quorum-initial-group-size`.

For the retry chain: `RetryConfig(delay_queue_type="quorum",
delay_queue_dead_letter_strategy="at-least-once", quorum_initial_group_size=3)`
applies the same to the delay queues (adding `x-overflow=reject-publish`) and
the replica count to a quorum DLQ. All default to `None`, so existing
topology is declared exactly as before.

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

`jitter_factor` has no effect with `jitter_mode="off"`. This is deliberate:
the only way to jitter a single TTL delay queue is a per-message
`expiration`, and a classic queue expires messages only at its head, so one
long-TTL message would hold back every shorter one queued behind it.
