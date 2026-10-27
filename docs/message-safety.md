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

The async broker never publishes a message again on its own once its frames may have left
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

When you need precise control over when acknowledgement happens — for example, after writing to a database — use `AckPolicy.MANUAL`. You receive the `RabbitMessage` directly and call `.ack()`, `.nack()`, or `.reject()` yourself.

```python
from rabbitkit import AsyncBroker, RabbitConfig, RabbitMessage
from rabbitkit.core.types import AckPolicy

config = RabbitConfig(url="amqp://guest:guest@localhost/")
broker = AsyncBroker(config)

@broker.subscriber(
    queue="orders.created",
    ack_policy=AckPolicy.MANUAL,
)
async def handle_order(msg: RabbitMessage) -> None:
    try:
        await write_to_database(msg.body)
        # Only ack after the side-effect is confirmed durable.
        await msg.ack()
    except DatabaseUnavailableError:
        # Requeue so the message is retried after reconnect.
        await msg.nack(requeue=True)
    except ValidationError:
        # Reject permanently; route to DLQ via broker dead-letter config.
        await msg.reject(requeue=False)
```

With `AckPolicy.MANUAL`, RabbitKit does **not** touch the ack state after your handler *returns*. If the handler *raises* without having settled the message, RabbitKit applies the `AUTO` failure settlement (transient: requeue once, then DLQ; permanent: DLQ) and logs it, so a failing handler cannot pin prefetch slots and stall the consumer. Opt out with `ConsumerConfig(manual_settle_on_error=False)` if you settle outside `RabbitMessage` and re-raise (see [Acknowledgement Policies](ack-policy.md#manual)).

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
| Connection drops mid-processing | Broker redelivers; duplicate delivery is possible and expected |
| Publisher confirms disabled (unsupported path) | Not supported for retry/DLQ paths; confirms are always required |
| `mandatory=True` publish, no matching binding | `PublishOutcome.status == RETURNED` on every path (direct, batch, reply-to); `.ok` is `False` — never reported as `CONFIRMED` |
| Connection lost after the publish frame left (async) | `UNKNOWN`; never republished by the library |
| Publisher channel pool exhausted (async) | Waits `channel_acquire_timeout`, then `NOT_SENT` (`admission_timeout`); connection untouched |
| Publish after `stop()` (async) | `BrokerNotStartedError`; nothing is sent, nothing reconnects |
