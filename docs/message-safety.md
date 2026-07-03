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

| Status | Meaning |
|---|---|
| `PublishStatus.CONFIRMED` | Broker accepted and routed the message. `outcome.ok` is `True`. |
| `PublishStatus.RETURNED` | `mandatory=True` and the broker could not route it to any queue (`Basic.Return`). |
| `PublishStatus.NACKED` | Broker rejected the message (`Basic.Nack`) — e.g. an internal broker error. |
| `PublishStatus.TIMEOUT` | No confirm arrived within `confirm_timeout`. |
| `PublishStatus.ERROR` | Any other publish-time failure (connection error, serialization, etc). |

`PublishOutcome.ok` is `True` only for `CONFIRMED` — treat `RETURNED`/`NACKED`/`TIMEOUT`/`ERROR`
the same way you would treat a failed publish (e.g. don't ack an upstream message on the
strength of an outcome that isn't `.ok`).

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

With `AckPolicy.MANUAL`, RabbitKit does **not** touch the ack state after your handler returns. Settlement is entirely your responsibility.

---

## Guarantee Summary

| Action | Guarantee |
|---|---|
| Handler succeeds | Message acked after handler returns |
| Handler raises, retry published, broker confirms | Original acked; retry message in broker |
| Handler raises, retry publish fails | Original not acked; requeued or redelivered |
| Handler raises, max retries exceeded | Message published to DLQ, original acked only after DLQ confirm |
| Manual ack: handler calls `msg.ack()` | Message acked at that point, no earlier |
| Manual ack: handler returns without acking | Message remains unacked; redelivered when channel closes |
| Connection drops mid-processing | Broker redelivers; duplicate delivery is possible and expected |
| Publisher confirms disabled (unsupported path) | Not supported for retry/DLQ paths; confirms are always required |
| `mandatory=True` publish, no matching binding | `PublishOutcome.status == RETURNED`; `.ok` is `False` — never reported as `CONFIRMED` |
