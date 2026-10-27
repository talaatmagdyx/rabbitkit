# Reliable Messaging

What rabbitkit guarantees for publishing, consuming, retries and dead-lettering,
what it does not, and how to turn on the settings that close the gaps (the
CRITICAL reliability profile). Every statement here describes the code as it
is; anything not implemented is marked **Not implemented**. Unless noted, the
tables describe `AsyncBroker`; `SyncBroker` differences are listed where they
exist.

Related pages: [Message Safety](message-safety.md) (details and real-broker
evidence), [Retry and DLQ](retry-and-dlq.md), [Event Metadata](event-metadata.md),
[Idempotency Contract](production/idempotency.md),
[Ordering Guarantees](ordering-guarantees.md), [Kubernetes](kubernetes.md).

## Delivery guarantee in one paragraph

RabbitMQ with rabbitkit is **at-least-once** end to end when publisher
confirms are on, consumers ack only after the handler succeeds, and the queues
and dead-letter hops are durable (quorum, at-least-once). A message can be
delivered, and a handler can run, more than once; nothing in the library
deduplicates. rabbitkit's job is to never report a message as safe when it is
not, and never acknowledge a delivery before its retry or DLQ copy is
confirmed.

## Publishing

### Outcomes

`broker.publish()` returns a `PublishOutcome`; broker outcomes are returned,
not raised. `outcome.ok` is `True` only for `CONFIRMED` (and `SENT` with
confirms off). `outcome.reason` is a bounded code (`PublishReason`).

| Status | Meaning | `reason` values | Publish again? |
|---|---|---|---|
| `CONFIRMED` | Broker `basic.ack` observed and, for `mandatory=True`, no `basic.return` | — | No |
| `SENT` | `confirm_delivery=False`: written to the socket, never confirmed | — | Unknown; do not use for reliable publishing |
| `RETURNED` | `mandatory=True` and unroutable (`reply_code` / `reply_text`, e.g. `312 NO_ROUTE`); on every path (direct, batch, reply-to) | `returned` | After fixing routing |
| `NACKED` | Broker `basic.nack` | `nacked` | Yes, by policy |
| `NOT_SENT` | Definitely never left the client | `admission_timeout`, `publisher_closed`, `not_connected`, `channel_unavailable`, `exceeds_max_in_flight_bytes`, `backpressure_dropped`, `cancelled` | Yes |
| `UNKNOWN` | Frames may have reached the broker; no confirm, return or nack seen | `connection_lost`, `channel_closed`, `shutdown`, `cancelled`, `no_outcome` (bulk) | Only with the **same `message_id`** |
| `TIMEOUT` | No confirm within `confirm_timeout` or the per-call `timeout` (`is_unknown` is `True`) | `confirm_timeout`, `publish_timeout` | Only with the same `message_id` |
| `ERROR` | Broker refused the publish, invalid input, or unclassified | `exchange_not_found`, `access_refused`, `precondition_failed`, `invalid_envelope`, `publish_error` | After fixing the cause |

Raised instead of returned: `MessageTooLargeError` (body above
`PublisherConfig.max_message_bytes`), `EventMetadataError` (invalid
[event metadata](event-metadata.md), when you use it), `ValueError` for an
invalid `timeout` or `message_id` with an envelope, `BrokerNotStartedError`
before `start()` / after `stop()` began, and `CancelledError` (treat a
cancelled publish as unknown; its channel and bytes are released).

Rules the async broker follows:

- It never publishes again on its own once frames may have left the client.
  A connection lost mid-publish is `UNKNOWN`. Failures before the frame (no
  channel) are healed by reconnecting and re-acquiring, which cannot duplicate.
- `SyncBroker` follows the same contract. `NOT_SENT`: no connection and none
  could be made, or the publishing thread may not reconnect (`not_connected`:
  a worker thread, or a handler running inside the consume loop, whose
  connection is down); the channel already closed before `basic_publish`
  (`channel_unavailable`); a worker-thread publish the I/O loop did not pick
  up within `confirm_timeout` (`admission_timeout`). `UNKNOWN`: the
  connection (`connection_lost`) or channel (`channel_closed`) was lost after
  `basic_publish`. `TIMEOUT` (`confirm_timeout`): no confirm in time. `ERROR`
  with `reply_code` / `reply_text`: `exchange_not_found` (404),
  `access_refused` (403), `precondition_failed` (406), sent once; the next
  publish reopens the publisher channel on the same connection. A
  publish-only `SyncBroker` whose connection the broker closed while it sat
  idle notices that before sending (it reads the socket first) and
  reconnects, so the first publish after a quiet period is `CONFIRMED`, not
  `UNKNOWN`. The sync transport no longer republishes after a dead
  connection (it used to, once, which could duplicate a message and still
  report `CONFIRMED`).
- Admission is bounded: a free pooled channel within
  `PoolConfig.channel_acquire_timeout`, the byte budget
  `PublisherConfig.max_in_flight_bytes` when set, and the per-call
  `publish(..., timeout=)`. Expiry before sending is `NOT_SENT`
  (`admission_timeout`); after sending, `TIMEOUT` (`publish_timeout`). A busy
  pool never rebuilds the connection. `timeout=` and the byte budget are
  async-only.
- To republish the same logical message after an unknown outcome, reuse its
  id: `MessageEnvelope.message_id` or `publish(..., message_id=...)`.

### Reserved headers

`PublisherConfig.strip_reserved_headers=True` removes rabbitkit's retry, DLQ
and replay bookkeeping headers (`x-rabbitkit-retry-count`,
`x-rabbitkit-first-failed-at`, `x-rabbitkit-original-*`,
`x-rabbitkit-error-*`, `x-rabbitkit-replay-*`, ...), the broker's `x-death`,
and a custom `RetryConfig.retry_header` from **application** publishes
(`broker.publish()` and `publish_many` / `iter_publish`, which use it). A
forged or copied header then cannot make a new message skip its retries.
rabbitkit's own retry and DLQ copies and `DLQInspector` replays do not go
through `broker.publish()` and keep them. Handler-result publishes are not
stripped. Default `False`; the CRITICAL profile turns it on. With
`RetryConfig.trust_retry_header=False` (also CRITICAL) the retry count is read
only from `x-death`, which the strip also covers for application publishes.

### Publisher shutdown

`AsyncBroker.stop()` drains consumers first, then closes publish admission (a
later `publish()` raises `BrokerNotStartedError`; nothing reconnects), waits
for in-flight confirms within the remaining stop timeout, and reports the
rest: still queued or waiting for a channel → `NOT_SENT` (`publisher_closed`),
sent but unconfirmed → `UNKNOWN` (`shutdown`).

## Consuming

### Settlement per outcome

Default `AckPolicy.AUTO`. "With retry" means a `RetryConfig` on the broker or
the route (`RetryMiddleware` is wired automatically, outside your route
middlewares).

| Handler outcome | Category | With retry | Without retry |
|---|---|---|---|
| Returns normally | — | ack after return | same |
| Returns, but its result publish is not `ok` | — | nack(requeue) | same |
| Raises a transient error (`TimeoutError`, `OSError`/`ConnectionError`, `EOFError`) | `RETRYABLE` | copy to `<queue>.retry.<n>` (mandatory, confirmed), then ack | first delivery: nack(requeue); redelivery (`redelivered=True`): reject → DLQ (`reject_transient_on_redelivery`, default on) |
| Raises an AMQP client error or `InfrastructureError` | `INFRASTRUCTURE` | as `RETRYABLE` | as `RETRYABLE` |
| Exceeds `ConsumerConfig.processing_timeout` (async only) | `UNKNOWN_OUTCOME` | task cancelled, then as `RETRYABLE` | as `RETRYABLE` |
| Raises anything else (unclassified) or a permanent error | `PERMANENT` | confirmed DLQ copy with failure metadata, then ack | reject → source queue's DLX → `<queue>.dlq` |
| Body cannot be decoded, an `on_receive` hook fails, `InvalidMessageError` (incl. `EventMetadataError`) | `INVALID_MESSAGE` | decode/validation inside the chain: confirmed DLQ copy with metadata; `on_receive` failure: reject → DLX (copy with metadata first when `dead_letter_with_metadata`) | reject → DLX (copy with metadata first when `dead_letter_with_metadata`) |
| Body larger than `ConsumerConfig.max_body_bytes` | `INVALID_MESSAGE` | reject → DLX, before the filter, middleware or handler (copy with metadata first when `dead_letter_with_metadata`, e.g. under CRITICAL) | same |
| Retries exhausted, or `max_retry_age` passed | terminal | confirmed DLQ copy with metadata, then ack | — |
| Cancelled by `stop()` | `SHUTDOWN` | nack(requeue); `CancelledError` propagates | same |
| Filtered out by `filter_fn` | — | nack(requeue=False) → DLX | same |

"reject → DLX" rows: with `SafetyConfig.dead_letter_with_metadata=True`
(async broker; the CRITICAL profile sets it) the delivery first gets a
confirmed DLQ copy carrying the same failure metadata as a retry terminal
copy and is then acked; only if that copy is not confirmed does the reject
(checked as below) or the requeue for a missing DLQ happen. Default `False`:
the reject, and the DLQ entry carries only `x-death`. Handler-requested
rejects (`RejectMessage`, `msg.reject_async()`) and `filter_fn` rejects are
never copied. `SyncBroker` always rejects.

Rules:

- Never ack before the handler succeeds, except under `AckPolicy.ACK_FIRST`
  (acked before the handler runs: at-most-once). The CRITICAL profile rejects
  `ACK_FIRST`.
- `AckPolicy.MANUAL`: the handler settles. A handler that raises without
  settling gets the `AUTO` path above (`manual_settle_on_error`, default on).
- `AckPolicy.NACK_ON_ERROR`: errors not handled by retry are nacked with
  `requeue=False`.
- One settlement frame per delivery (async): concurrent or late settlements
  are no-ops. If the frame's write fails, `settlement_outcome_unknown` is
  `True` and nothing else is sent; the broker redelivers if it never got it.
- If the ack after a successful handler fails, the outcome is unknown: logged
  and counted, nothing else is sent; the handler runs again if the broker
  redelivers.

### Retry and DLQ guarantees

- A retry or DLQ copy is published with `mandatory=True` and must be
  confirmed before the source is acked. If the handoff is not confirmed, the
  source is not acked: async routes hold the delivery for a capped backoff
  and then nack(requeue); after `RetryHandoffConfig.max_consecutive_failures`
  or `recovery_deadline` the route is effectively paused (each failing
  delivery held `backoff_max`) and `on_handoff_exhausted` fires. The sync path
  nacks at once (it never sleeps on pika's I/O thread). **Not implemented:**
  turning an exhausted handoff into a terminal DLQ transfer.
- If the terminal DLQ copy is not confirmed, the pipeline rejects the
  delivery into the source's DLX when the DLQ exists (async: checked first) or
  the source is a quorum queue declared `at-least-once`; otherwise (async) it
  requeues after a 0.5–5 s backoff. A broker-dead-lettered copy carries only
  `x-death`, not the failure metadata. A delivery is never acked without a
  confirmed copy; a copy that landed without its confirm can leave a
  duplicate in the DLQ after that fallback.
- `rabbitkit_messages_dead_lettered_total` counts completed dead-letters:
  a confirmed DLQ copy, or the reject the pipeline actually sent. A delivery
  requeued because its DLQ is missing is not counted. The G-17 copies of
  failures outside `RetryMiddleware` are not counted by it.
- Copies keep the wire body and encoding, `message_id` (a message without one
  gets a stable id on its first handoff), `correlation_id`, `type`, `app_id`,
  `user_id`, `reply_to`, `priority`, `timestamp`, `delivery_mode` and every
  header, including [event metadata](event-metadata.md). `expiration` is moved
  to `x-rabbitkit-original-expiration`.
- Failure metadata on retry and DLQ copies: `x-rabbitkit-retry-count`,
  `-attempts`, `-first-failed-at`, `-last-failed-at`, `-error-category`
  (the category in the table above), `-failure-category`, `-error-type`,
  `-error-message` (sanitized, or omitted), `-original-queue`,
  `-original-exchange`, `-original-routing-key`, `-original-message-id`, and
  `-dead-lettered-at` on DLQ copies. See
  [Retry and DLQ](retry-and-dlq.md#metadata-on-every-retry-and-dlq-copy).
- Retry count: the larger of the header and the broker's `x-death` history,
  clamped to `max_retries`; with `trust_retry_header=False`, `x-death` only.
- `DLQInspector` replay acks a DLQ message only after its republish is
  confirmed, keeps every property and header, records
  `x-rabbitkit-replay-count` / `-replayed-at` / `-replayed-by`, can refuse
  messages replayed `max_replays` times, and can require an authorizer.

### Remaining loss windows

| Where | When a message can be lost | Closed by |
|---|---|---|
| Producer, before `CONFIRMED` | Process crash, or an unhandled non-`ok` outcome | Application (an outbox if needed). **Not implemented** in rabbitkit |
| `confirm_delivery=False` | A `SENT` message lost in flight | Confirms on (default; required by both profiles) |
| Classic queues | Loss of the node's storage | Quorum queues |
| Broker dead-letter hops (reject → DLX, delay-queue TTL expiry) from a classic queue or an `at-most-once` quorum queue | Dead-letter target missing or full: dropped by the broker | Quorum source and delay queues with `x-dead-letter-strategy=at-least-once` + `overflow=reject-publish` (CRITICAL). The async broker also refuses to reject into a missing DLQ; **SyncBroker does not check** |
| `AckPolicy.ACK_FIRST` | Handler fails or the process crashes after the early ack | Not using it (CRITICAL rejects it) |
| Queue length limits with `drop-head` | Oldest messages dropped | `overflow=reject-publish` |

A consumer crash after the handler's side effects but before the ack is not a
loss but a duplicate (the broker redelivers): see [Idempotency](#idempotency).

### Consumer shutdown

`AsyncBroker.stop()`: cancel every consumer (a reconnect during the drain
does not resubscribe them); hand prefetched-but-not-started worker-pool
deliveries back at once (nack, requeue); give in-flight handlers
`graceful_timeout`; then cancel them, wait at most 1 s more, and nack the rest
for redelivery. Then the publisher shuts down (above). `stop()` is bounded
even if a handler swallows cancellation.

Signals: `broker.run()` and `start(install_signal_handlers=True)` install
SIGINT/SIGTERM handlers and `stop()` restores the previous ones; a handler an
application registered with `loop.add_signal_handler` is left alone. Embedding
applications should pass `install_signal_handlers=False` (`rabbitkit_lifespan`
does, and so does `async with AsyncBroker(config) as broker:`, which starts
the broker on entry and stops it on exit). `RabbitApp.run_async()` follows the same rules: it restores the
handlers it replaced and leaves an existing asyncio handler alone. Keep
`graceful_timeout` below Kubernetes' `terminationGracePeriodSeconds`.

## Failure taxonomy

`classify_error()` decides, in order: shutdown signals (`CancelledError`,
`KeyboardInterrupt`, `SystemExit` → `SHUTDOWN`); a category the exception
declares (`rabbitkit_error_category`, e.g. `InvalidMessageError`,
`InfrastructureError`); `RetryMiddleware` predicates; registered client
connection/channel errors (`INFRASTRUCTURE`); the transient tuples
(`RETRYABLE`, or `UNKNOWN_OUTCOME` for `HandlerTimeoutError`); the permanent
tuple; then `RetryConfig.unknown_policy` (default `PERMANENT`).

| Category | Retried | Typical cause |
|---|---|---|
| `RETRYABLE` | yes | `TimeoutError`, `ConnectionError` / `OSError`, `EOFError` |
| `INFRASTRUCTURE` | yes, labelled separately | AMQP client errors in the handler, `InfrastructureError` |
| `UNKNOWN_OUTCOME` | yes (handler must be idempotent) | `processing_timeout` expired |
| `PERMANENT` | no → DLQ | Business rule violation, unclassified exceptions |
| `INVALID_MESSAGE` | no → DLQ | Decode failure, `on_receive` failure, `InvalidMessageError`, `EventMetadataError`, body over `max_body_bytes` |
| `SHUTDOWN` | no; requeued | Cancellation |

The category is written to `x-rabbitkit-error-category` and used as the
bounded `category` label of `rabbitkit_messages_retried_total` and
`rabbitkit_messages_dead_lettered_total`.

## Durability scope

| Layer | Guarantee | Owner |
|---|---|---|
| Producer memory, before `CONFIRMED` | None | Application |
| Broker, `CONFIRMED` on a quorum queue, persistent message | Survives the loss of a minority of nodes | Topology |
| Broker, `CONFIRMED` on a classic durable queue | Single node | Topology |
| Retry and dead-letter hops | At-least-once only with quorum queues using the at-least-once dead-letter strategy; otherwise at-most-once | Topology (CRITICAL declares it for the retry chain; the source queue is yours to declare or cover by policy) |
| Consumer, after processing, before ack | Redelivered | Application (idempotency) |

## Idempotency

1. rabbitkit propagates `message_id`, `correlation_id` and every header
   (including `idempotency-key` from [event metadata](event-metadata.md))
   unchanged through publish, retry, DLQ and replay. RabbitMQ does not
   deduplicate, and neither does rabbitkit by default.
2. Handlers with side effects need a **durable deduplication record keyed by
   the idempotency key, committed in the same transaction as the business
   change, before the ack** (an "inbox"). This is application code; rabbitkit
   does not ship a database-backed deduplication engine (**Not implemented**,
   by design). Sketch:

   ```python
   @broker.subscriber("orders", middlewares=[EventMetadataMiddleware(validate_publish=False)])
   async def handle(body: dict, message: RabbitMessage) -> None:
       key = EventMetadata.from_message(message).idempotency_key
       async with db.transaction() as tx:
           inserted = await tx.execute(
               "INSERT INTO inbox (idempotency_key) VALUES ($1) ON CONFLICT DO NOTHING", key
           )
           if inserted == 0:
               return  # already processed: ack the duplicate
           await apply_business_change(tx, body)
       # commit happened; returning acks the delivery
   ```

   A crash before the commit leaves no record and the redelivery is
   processed; a crash after the commit but before the ack makes the
   redelivery hit the record and be acked.
3. `DeduplicationMiddleware` (Redis) is an optional fast-path filter, not the
   guarantee: with `fallback_on_redis_error=True` (default) it fails open, and
   it is not atomic with your side effects.

## Ordering

Strict FIFO per queue holds only with one consumer, `prefetch_count=1`, no
worker pool and no retry. Retries (delay queues), DLQ replay, requeues,
redeliveries after a reconnect, and several consumers all reorder messages.
rabbitkit adds no ordering features. See
[Ordering Guarantees](ordering-guarantees.md).

## Enabling the CRITICAL profile

```python
from rabbitkit import (
    AsyncBroker,
    ConnectionConfig,
    ConsumerConfig,
    DeadLetterStrategy,
    QueueType,
    RabbitConfig,
    RabbitQueue,
    ReliabilityProfile,
    RetryConfig,
    critical_config,
)

config = critical_config(
    RabbitConfig(
        connection=ConnectionConfig.from_url("amqp://..."),
        consumer=ConsumerConfig(processing_timeout=30.0),  # required: no safe default
        retry=RetryConfig.exponential(max_retries=4, initial_delay=5),
    )
)
broker = AsyncBroker(config)


@broker.subscriber(
    RabbitQueue(
        "orders",
        queue_type=QueueType.QUORUM,
        dead_letter_strategy=DeadLetterStrategy.AT_LEAST_ONCE,
        overflow="reject-publish",
    )
)
async def handle(body: dict) -> None: ...


report = broker.preflight(ReliabilityProfile.CRITICAL, management_client=mgmt)
if not report.ok:
    raise SystemExit("\n".join(c.detail for c in report.failed))
```

`apply_profile(config, "critical")` / `critical_config(base)` return a new
config and **raise `ConfigValidationError`** on a setting you pinned that the
profile forbids, and when `consumer.processing_timeout` is unset.

| Requirement | Applied by `apply_profile` | Checked by `validate_profile` / `preflight` | Contradiction (raises) |
|---|---|---|---|
| Publisher confirms, persistent, mandatory | set | yes | `confirm_delivery=False`, `persistent=False` |
| Body limit <= 256 KiB | lowered | yes | an explicit larger value, or `0` |
| `max_in_flight_bytes` bounded | 64 MiB unless set (`CRITICAL_MAX_IN_FLIGHT_BYTES`) | yes | — |
| `strip_reserved_headers` | set | yes | — |
| Retry configured | a default `RetryConfig()` when none, else yours | yes, and per route (`RETRY_DISABLED` is an error; a route `RetryConfig` is checked like the broker's) | — |
| Quorum delay queues, dead-lettering at-least-once | set | yes | `delay_queue_dead_letter_strategy="at-most-once"` |
| Quorum DLQ | set | yes | `dlq_queue_type="classic"` |
| Terminal DLQ copy with metadata | `terminal_action="dead_letter"` | yes | `terminal_action="reject"` |
| Retry count from `x-death` only | `trust_retry_header=False` | yes | — |
| Error text sanitized or omitted | — | yes | `error_detail="raw"` |
| Bounded transient loop without retry | — (default on) | yes | `reject_transient_on_redelivery=False` |
| MANUAL handler raising → AUTO path | — (default on) | yes | `manual_settle_on_error=False` |
| `processing_timeout` | **must be set by you** | yes | unset raises |
| No `AckPolicy.ACK_FIRST` | — | per route | — |
| Quorum source queues | — | per route (error) | — |
| Source `x-dead-letter-strategy=at-least-once` + `overflow=reject-publish` | — | per route: warning if not declared; verified on the broker by `preflight(management_client=...)` (which then replaces the warning) | — |
| Dead-letter path never `discard` | — | yes | `reject_without_dlx="discard"` |
| DLQ copy with metadata for failures outside retry (`on_receive`, body cap, no-retry routes) | `safety.dead_letter_with_metadata=True` (async) | yes | — |
| Event metadata validation | — (opt-in) | not checked | — |
| Heartbeat on (`connection.heartbeat > 0`) | — (default 30 s) | yes | `heartbeat=0` |
| Async reconnect budget | — (unlimited by default) | warning when `reconnect_budget_seconds < reconnect_backoff_max` | — |
| TLS | — (deployment choice) | warning when enabled with `cert_reqs="CERT_NONE"` | — |

Why the connection rows look like this:

- **Heartbeats are required.** Without them a peer that disappears without
  closing TCP (a crashed or partitioned node) is never detected: publishes
  wait on a dead socket and unacked deliveries stay assigned to a consumer
  that cannot settle them. The default (30 s) satisfies the profile.
- **The reconnect budget stays unlimited by default.** No guarantee depends
  on it: a publish during an outage is reported `NOT_SENT` / `UNKNOWN`, never
  `CONFIRMED`, and unacked deliveries are redelivered after the reconnect. An
  unlimited budget keeps a consumer available through a long broker outage or
  a rolling restart without a restart storm; a connection that fails for a
  reason retrying cannot fix (refused credentials, vhost or TLS) stops at once
  anyway, and the supervisor (Kubernetes) restarts the pod on a terminal
  failure. Set `reconnect_budget_attempts` / `reconnect_budget_seconds` when
  you prefer a restart after a bounded outage; the profile only checks that a
  seconds budget is not shorter than `reconnect_backoff_max` (the backoff it
  bounds could never reach its own cap). The budgets apply to `AsyncBroker`;
  `SyncBroker` keeps its own bounded loop (`reconnect_max_attempts`).
- **TLS is not required.** Whether broker traffic is encrypted by the client,
  a service mesh or a private network is a deployment decision the library
  cannot see. When TLS is enabled through `SSLConfig`, turning certificate
  verification off (`cert_reqs="CERT_NONE"`) is reported.

Per-route requirements cannot be applied to a config: register the
subscribers, then call `validate_profile(config, "critical",
routes=broker.routes)` or `broker.preflight("critical", ...)` and refuse to
start when it reports errors. `preflight()` reports broker-side checks it could
not perform as `UNVERIFIED`, never as passed. `policy_templates()` renders
policies (at-least-once dead-lettering, `reject-publish`, delivery limits) for
whoever owns the cluster to apply.

`processing_timeout` and `max_in_flight_bytes` are enforced by `AsyncBroker`
only. `SyncBroker` logs that it ignores `processing_timeout`; use
`TimeoutMiddleware` there.
