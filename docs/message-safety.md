# Message Safety Guarantees

RabbitKit is designed so that a message is never lost between handler failure and retry or dead-letter delivery. The core guarantee is:

> **RabbitKit never acknowledges the original message before the retry or DLQ publish is confirmed by the broker.**

---

## The Full Settlement Flow

```
Broker delivers message
        |
        v
Handler raises an exception
        |
        v
RabbitKit publishes retry message to retry exchange
  (with publisher confirms enabled)
        |
        v
Broker sends Basic.Ack for the retry publish
        |
        v
RabbitKit acks the original message
```

If the broker never confirms the retry publish, the original message is **not** acked. Depending on the ack policy and connection state, it will be nacked or requeued.

---

## What Happens When the Retry Publish Fails

If the retry publish times out or receives a nack from the broker:

1. RabbitKit does **not** ack the original message.
2. Depending on the configured `AckPolicy`, it either nacks with `requeue=True` or lets the message remain unacked until the channel closes and the broker redelivers it.
3. The original message is never silently dropped.

This prevents the dual-failure scenario where the original is acked but the retry message never arrives.

---

## Publisher Confirms

Publisher confirms (AMQP `Confirm.Select`) are the mechanism RabbitKit uses to know that the broker has durably accepted a published message before proceeding with the ack of the original.

- For the async broker (`AsyncBroker`), publisher confirms are enabled automatically on the channel used for retry and DLQ publishing.
- For the sync broker (`SyncBroker`), the channel is put into confirm mode and `channel.wait_for_confirms()` is called before the original message is acked.

Without publisher confirms, there is no way to distinguish a successful publish from a silent broker failure. RabbitKit requires them for all retry and DLQ paths.

---

## Mandatory Publishes and Unroutable Messages

`MessageEnvelope(mandatory=True)` asks the broker to return the message instead of
silently dropping it when no queue is bound to match the routing key. `broker.publish()`
reports this via `PublishOutcome.status`:

| Status | Meaning | Publish again? |
|---|---|---|
| `PublishStatus.CONFIRMED` | Broker accepted and routed the message. `outcome.ok` is `True`. | No |
| `PublishStatus.RETURNED` | `mandatory=True` and the broker could not route it to any queue (`Basic.Return`). `reply_code` / `reply_text` hold the broker's answer (e.g. `312` / `NO_ROUTE`). | After fixing routing |
| `PublishStatus.NACKED` | Broker rejected the message (`Basic.Nack`) — e.g. an internal broker error. | Yes, by policy |
| `PublishStatus.NOT_SENT` | The message definitely never left the client: publisher closed, admission timed out (no free channel or byte budget), no connection, flow-control drop. | Yes |
| `PublishStatus.UNKNOWN` | The frames may have reached the broker, but no confirm, return or nack was seen (connection or channel lost mid-publish, shutdown). | Only with the **same `message_id`** |
| `PublishStatus.TIMEOUT` | No confirm arrived within `confirm_timeout` (or the per-call `timeout`). A kind of unknown outcome. | Only with the same `message_id` |
| `PublishStatus.ERROR` | The broker refused this publish (`reason` says why: `exchange_not_found`, `access_refused`, `precondition_failed`), invalid input, or an unclassified failure. | After fixing the cause |

`outcome.reason` is a bounded code (`rabbitkit.PublishReason`), safe as a metric label.
`outcome.is_unknown` is `True` for `UNKNOWN` and `TIMEOUT`.

`PublishOutcome.ok` is `True` only for `CONFIRMED` (and `SENT` with confirms off) — treat
every other status the same way you would treat a failed publish (e.g. don't ack an upstream
message on the strength of an outcome that isn't `.ok`).

Neither broker publishes a message again on its own once its frames may have left
the client: a lost connection is reported `UNKNOWN` instead of being retried behind your
back (that retry could deliver a duplicate reported as `CONFIRMED`). To republish after an
unknown outcome, reuse the message id — `MessageEnvelope.message_id`, or
`broker.publish(..., message_id=...)` in the keyword form — so consumers can deduplicate.

**Admission and timeouts (async broker).** Each confirmed publish holds one pooled channel
until its confirm arrives. When all `PoolConfig.channel_pool_size` channels are busy, a
publish waits up to `PoolConfig.channel_acquire_timeout` and then returns `NOT_SENT`
(`admission_timeout`); a busy pool never rebuilds the connection. With
`PublisherConfig.max_in_flight_bytes` set, a publish also waits (same bound) for its body to
fit in the in-flight byte budget. `broker.publish(..., timeout=2.0)` bounds the whole call —
admission, send and confirm: on expiry the outcome is `NOT_SENT` if nothing was sent yet,
otherwise `TIMEOUT` (`publish_timeout`). Cancelling a publish never leaks its channel or
bytes; treat a cancelled publish as unknown.

**Byte budget (sync broker).** `SyncBroker` enforces `PublisherConfig.max_in_flight_bytes`
the same way: a body larger than the budget is `NOT_SENT` (`exceeds_max_in_flight_bytes`),
and a publish waits first come first served, up to `PoolConfig.channel_acquire_timeout`, for
room before it returns `NOT_SENT` (`admission_timeout`). Worker-thread publishes waiting for
the I/O thread hold their bytes too, so the budget bounds them.

**Publisher shutdown (async broker).** `stop()` first drains consumers, then closes publish
admission (a later `publish()` raises `BrokerNotStartedError`; it never reconnects), waits for
in-flight confirms within what is left of the stop timeout, and reports the rest: still
waiting for a channel or still queued in the batch publisher → `NOT_SENT`
(`publisher_closed`); sent but unconfirmed → `UNKNOWN` (`shutdown`).

**Sync transport residual limitation:** pika's `BlockingChannel.basic_publish()` has no
timeout parameter of its own. `confirm_timeout` bounds every publish path *except* one: a
publish made from the connection's owner thread *while a consume loop it also drives is
active* (the default `worker_count=1` consumer publishing a result/retry from inside a
handler) cannot be safely bounded — interrupting it would leave a background thread possibly
still touching the same connection at the exact moment `start_consuming()` resumes touching
it too, which pika does not support from two threads at once. Every other sync publish path
(a pure producer with no consumer, publishing before any consuming has started, and any
publish from a non-owner worker thread) is fully bounded, including on a broker that accepts
the TCP connection but never sends the confirm frame back (disk full, internally wedged) — the
connection is discarded and transparently re-established on the next call rather than reused.
**Mitigation for the one unbounded case:** pass `worker_config=WorkerConfig(worker_count=2+)` to
`SyncBroker.start()` — a handler's publish then runs on a worker thread, which already marshals
through the bounded cross-thread path.

A `mandatory=True` publish always detects an unroutable message and reports `RETURNED`,
regardless of the broker's `confirm_delivery` setting — `SyncBroker` upgrades the target
channel to confirm mode on demand, and `AsyncBroker` routes the publish through a dedicated,
always-confirmed channel.

```python
outcome = await broker.publish(
    MessageEnvelope(routing_key="orders.created", body=b"...", mandatory=True)
)
if not outcome.ok:
    # outcome.status is RETURNED if no queue was bound to "orders.created"
    logger.error("order publish not routed: %s", outcome.status)
```

---

## Manual Ack Policy for Full Settlement Control

When you need precise control over when acknowledgement happens — for example, after writing to a database — use `AckPolicy.MANUAL`. You receive the `RabbitMessage` directly and settle it yourself: `await msg.ack_async()` / `nack_async()` / `reject_async()` on `AsyncBroker`, `msg.ack()` / `nack()` / `reject()` on `SyncBroker`.

```python
from rabbitkit import AsyncBroker, ConnectionConfig, RabbitConfig, RabbitMessage
from rabbitkit.core.types import AckPolicy

config = RabbitConfig(connection=ConnectionConfig.from_url("amqp://guest:guest@localhost/"))
broker = AsyncBroker(config)

@broker.subscriber(
    queue="orders.created",
    ack_policy=AckPolicy.MANUAL,
)
async def handle_order(msg: RabbitMessage) -> None:
    try:
        await write_to_database(msg.body)
        # Only ack after the side-effect is confirmed durable.
        await msg.ack_async()
    except DatabaseUnavailableError:
        # Requeue so the message is retried after reconnect.
        await msg.nack_async(requeue=True)
    except ValidationError:
        # Reject permanently; route to DLQ via broker dead-letter config.
        await msg.reject_async(requeue=False)
```

With `AckPolicy.MANUAL`, RabbitKit does **not** touch the ack state after your handler *returns*. If the handler *raises* without having settled the message, RabbitKit applies the `AUTO` failure settlement (transient: requeue once, then DLQ; permanent: DLQ) and logs it, so a failing handler cannot pin prefetch slots and stall the consumer. Opt out with `ConsumerConfig(manual_settle_on_error=False)` if you settle outside `RabbitMessage` and re-raise (see [Acknowledgement Policies](ack-policy.md#manual)).

---

## Consumer Settlement, Cancellation and Shutdown (async broker)

### One settlement frame per delivery

`ack_async()`, `nack_async()` and `reject_async()` claim the delivery's
settlement **before** they await the network write. A second call — a
concurrent `ack`/`nack` from the handler, or `stop()` nacking a handler that
was cancelled while its ack was being written — is a no-op. Sending two frames
for one delivery tag makes RabbitMQ close the channel with
`PRECONDITION_FAILED - unknown delivery tag`, which redelivers every unacked
message on that channel.

If the write itself raises or is cancelled, the frame may or may not have
reached the broker. The claim is kept (`msg.disposition` stays at the claimed
value), `msg.settlement_outcome_unknown` becomes `True`, and nothing else is
sent for that delivery. If the frame was lost, the broker redelivers the
message when the channel closes (at-least-once). The sync broker's methods are
unchanged: a failed `ack()` leaves the message unsettled.

### Failure settlement (verified on a real broker)

- `AUTO`, no retry configured, transient error (`OSError`, `TimeoutError`,
  ...): requeued once; the redelivery is dead-lettered if it fails too. The
  audit's hot loop (thousands of runs per second) does not happen.
- `MANUAL`, handler raises without settling: settled with the `AUTO` failure
  policy and logged, so it cannot hold a prefetch slot. With `prefetch_count=1`
  the next message is delivered.
- `ConsumerConfig.processing_timeout`: the failure is a `HandlerTimeoutError`
  with `outcome_unknown=True` (its side effects may or may not have happened),
  settled as a transient failure. The async broker cancels the handler task;
  the sync broker settles the delivery from its I/O thread at the deadline
  and lets the handler thread finish, discarding its late result (see
  [Reliable Messaging](reliable-messaging.md#processing-deadline-processing_timeout)).

### Connection or channel loss while a handler runs

- **Inline handlers** (no worker pool, the default): each delivery runs in a
  task owned by aiormq's channel. When the channel or connection closes,
  aiormq **cancels those tasks**, so the handler receives `CancelledError`
  wherever it is awaiting. Its side effects may be partial, and the broker
  redelivers the message. Handlers must be idempotent and must not swallow
  `CancelledError`.
- **Worker-pool handlers** (`WorkerConfig(worker_count > 1)`) are not
  cancelled by a channel loss: they run to completion, their ack fails
  ("Ack outcome unknown") and the broker redelivers the message, so it is
  processed twice.

### The worker pool is shared by all routes

One `AsyncWorkerPool` (one semaphore of `worker_count` slots) serves every
route of the broker. A slow queue can occupy every slot and starve the other
routes. Per-route `prefetch_count` bounds how many deliveries each route
holds, but not how many slots it uses. Run queues with very different
latency in separate brokers (or processes) if that matters.

### `stop()` sequence

1. Consumers are cancelled through the queue object they were started on, so
   aio-pika's reconnect logic forgets them: a connection drop during the
   drain cannot re-subscribe them. A delivery that still reaches a cancelled
   consumer is requeued and that consumer is cancelled again.
2. Worker-pool deliveries that were prefetched but had not started are handed
   back at once (`nack`, `requeue=True`), so another consumer can take them.
3. In-flight handlers get `graceful_timeout`; then they are cancelled, given
   at most 1 s to unwind, and their deliveries are nacked for redelivery.

### Consumers cancelled by the broker

RabbitMQ cancels a consumer itself (`basic.cancel`) when its queue is deleted,
a quorum queue's leader moves, or an operator closes it. Both brokers:

- detect it (async: aiormq's consumer-cancel callback on aiormq 7, otherwise
  by checking the channel's consumer table every second; sync: pika's channel
  cancel callback, dispatched on the I/O thread);
- logs a WARNING, sets the route's `runtime_state.consumer_cancelled`, so
  `broker_health_check()` reports it as not consuming (DEGRADED) and
  `broker_readiness()` fails, and calls `broker.on_consumer_cancelled(queue)`
  if set;
- re-subscribes with the same consumer tag once a passive declare finds the
  queue again, waiting 1 s, then doubling up to 30 s between probes; then
  calls `broker.on_consumer_resumed(queue)`.

They never re-create the queue. aio-pika 10 also re-subscribes such consumers
on its own; whichever gets there first wins, without a duplicate consumer.
`SyncBroker` probes and re-subscribes from its consume loop (`run()`), on the
thread that owns the connection, and calls both hooks on that thread.

Limitation (from reading aio-pika, not reproduced): if the connection drops
while the queue is still missing, aio-pika's reconnect re-declares the
consumer's queue passively and fails, and it retries the whole connection
until the queue exists again. With `TopologyMode.AUTO_DECLARE` the broker's
own topology restore re-creates the queue first; with `PASSIVE_ONLY` every
route on that connection waits for the queue to come back.

---

## Guarantee Summary

| Action | Guarantee |
|---|---|
| Handler succeeds | Message acked after handler returns |
| Handler raises, retry published, broker confirms | Original acked; retry message in broker |
| Handler raises, retry publish fails | Original not acked; requeued or redelivered |
| Handler raises, max retries exceeded | Original rejected into the source queue's dead-letter path (the DLQ) — only after the DLQ was verified to exist (async broker) |
| Dead-letter target missing (async broker) | Not rejected: requeued after a backoff, ERROR logged; never dropped |
| Transient failure, no retry | Requeued once; dead-lettered if the redelivery fails too (default) |
| Handler succeeds but the ack write fails | Outcome unknown: logged and counted, nothing else sent; redelivered if the broker never got the ack |
| Manual ack: handler calls `msg.ack()` | Message acked at that point, no earlier |
| Manual ack: handler returns without acking | Message remains unacked; redelivered when channel closes |
| Manual ack: handler raises without settling | `AUTO` failure settlement (requeue once / DLQ), logged |
| `stop()` with a handler past `graceful_timeout` | Handler cancelled; nacked for redelivery; abandoned after 1 s more if it ignores cancellation, so `stop()` stays bounded |
| `stop()` with worker-pool deliveries not started yet | Nacked with requeue at once; never run during the grace period |
| Reconnect during `stop()`'s drain | Cancelled consumers stay cancelled; stray deliveries are requeued |
| Two settlements of one delivery at once (async) | One frame is sent; the channel stays open |
| Settlement write fails or is cancelled (async) | `settlement_outcome_unknown=True`; no second frame; redelivered if the frame was lost |
| Broker cancels the consumer (queue deleted, ...) | Health reports the route as not consuming; re-subscribed when the queue exists again |
| Connection drops mid-processing | Broker redelivers; duplicate delivery is possible and expected |
| Publisher confirms disabled (unsupported path) | Not supported for retry/DLQ paths; confirms are always required |
| `mandatory=True` publish, no matching binding | `PublishOutcome.status == RETURNED` on every path (direct, batch, reply-to); `.ok` is `False` — never reported as `CONFIRMED` |
| Connection lost after the publish frame left (async and sync) | `UNKNOWN`; never republished by the library |
| Publish-only `SyncBroker` connection closed by the broker while idle | Noticed before sending; reconnected; the publish is sent once |
| Publisher channel pool exhausted (async) | Waits `channel_acquire_timeout`, then `NOT_SENT` (`admission_timeout`); connection untouched |
| Publish after `stop()` (async) | `BrokerNotStartedError`; nothing is sent, nothing reconnects |
| Delivery body over `ConsumerConfig.max_body_bytes` | `INVALID_MESSAGE`: rejected into the DLX before the filter, middleware or handler run (with `SafetyConfig.dead_letter_with_metadata`, async: a confirmed DLQ copy with failure metadata first, then ack) |
| `on_receive` failure, or a failure on a route without retry, with `SafetyConfig.dead_letter_with_metadata=True` (async) | Confirmed DLQ copy with failure metadata, then ack; if the copy is not confirmed, the usual reject (or requeue for a missing DLQ) |
| Application publish carrying `x-rabbitkit-retry-count` / `x-death` with `strip_reserved_headers=True` | Headers removed before sending; the library's own retry/DLQ/replay copies keep theirs |
