# Acknowledgement Policies

rabbitkit supports four acknowledgement policies, selected per subscriber via `ack_policy=`.

## AUTO (default)

rabbitkit manages settlement automatically based on handler outcome.

| Handler result | Action |
|----------------|--------|
| Returns normally | `basic.ack` |
| Raises transient exception, with `RetryConfig` | republished to a delay queue (retry) |
| Raises transient exception, no retry, first delivery | `basic.nack(requeue=True)` |
| Raises transient exception, no retry, message already redelivered | `basic.reject` → DLQ |
| Raises permanent exception | `basic.reject` → DLQ |

Transient means `TimeoutError`, `OSError` (connection resets and so on),
`EOFError` and `HandlerTimeoutError`; everything else, including unknown
exception types, is permanent.

**The transient loop is bounded by default.** Without retry, a transient
failure is requeued once; if the redelivery fails again it is dead-lettered
(`ConsumerConfig.reject_transient_on_redelivery=True`, the default). Earlier
releases requeued transient failures immediately and forever: a poison
message that always times out ran thousands of times per second and never
reached the DLQ. The broker's `redelivered` flag is the only per-message
signal a classic queue carries, so the cap is two attempts, and it also
counts a redelivery caused by a crash or a lost connection. Use
`RetryConfig` for more attempts with delays, or set
`reject_transient_on_redelivery=False` to restore unbounded requeueing.

Every handler failure is logged as one structured line: queue, message id,
correlation id, delivery tag, `redelivered`, the exception class, the reason
for the decision and the action taken. The body, headers and exception text
are never logged (they may contain payload data or secrets); the traceback
is logged at DEBUG. Repeats of the same failure on the same queue are
collapsed to one line per 10 s window, with a count.

```python
@broker.subscriber(queue="orders")  # ack_policy=AckPolicy.AUTO is the default
async def handle(body: dict) -> None:
    process(body)  # ack on success, nack/retry on exception
```

Use `AUTO` for most consumers. Combine with `RetryConfig` for transient failures.

## MANUAL

The handler receives the raw `RabbitMessage` and settles it explicitly. rabbitkit never auto-acks.

```python
from rabbitkit import AckPolicy, RabbitMessage

@broker.subscriber(queue="payments", ack_policy=AckPolicy.MANUAL)
async def handle(msg: RabbitMessage) -> None:
    try:
        result = await charge(msg.body)
        await msg.ack_async()
    except TransientError:
        await msg.nack_async(requeue=True)
    except PermanentError:
        await msg.reject_async(requeue=False)
```

Use `MANUAL` when the handler needs full settlement control — for example, when settlement depends on an external confirmation or a two-phase commit.

**If the handler raises without settling**, rabbitkit applies the same
failure settlement as `AUTO` (transient: requeue once, then DLQ; permanent:
DLQ) and logs it. Earlier releases left the delivery unacked until the
channel closed; each such failure held a prefetch slot, so `prefetch_count`
failures stalled the consumer. A handler that settled before raising is not
settled again.

If your handler settles through something rabbitkit cannot see, a raw
channel or a `CoalescingAcker`, and then re-raises, set
`ConsumerConfig(manual_settle_on_error=False)`. Otherwise the pipeline
settles the same delivery tag a second time and the broker closes the
channel. Better: do not re-raise after settling.

**Warning:** If the handler *returns* without acking or nacking, the message stays unacked until the connection closes (logged as a warning). rabbitkit never auto-acks under `MANUAL`.

## NACK_ON_ERROR

Successful handlers are auto-acked. Failed handlers are nacked with `requeue=False`.

```python
@broker.subscriber(queue="events", ack_policy=AckPolicy.NACK_ON_ERROR)
async def handle(body: bytes) -> None:
    process(body)  # nack (no requeue) on any exception
```

Use when you want errors to go straight to the DLQ without retry. Do not combine with `RetryConfig`.

## ACK_FIRST

The message is acknowledged **before** the handler runs.

```python
@broker.subscriber(queue="logs", ack_policy=AckPolicy.ACK_FIRST)
async def handle(body: bytes) -> None:
    write_log(body)  # message already acked — loss is possible if this fails
```

**Warning:** If the handler fails after the ack, the message is lost permanently. Use only when message loss is acceptable — for example, best-effort audit logging.

Never use `ACK_FIRST` for financial transactions, order processing, or any flow where at-least-once delivery is required.

## Handler deadline (`processing_timeout`)

`ConsumerConfig(processing_timeout=30.0)` gives every handler a deadline
(default `None`, off). When it expires, `HandlerTimeoutError` goes through the
failure settlement above: the async broker cancels the handler task; the sync
broker settles the delivery from its I/O thread and lets the handler thread
run to its end, discarding its late result. It is transient, and it carries `outcome_unknown=True`: the handler
was cut off part-way, so its side effects may or may not have happened. The
failure log says so (`outcome_unknown=True`), and `classify_error` reports
it on `ClassifiedError.outcome_unknown`. Retrying it is only safe for
idempotent handlers.

Only the deadline produces `HandlerTimeoutError`. A `TimeoutError` raised by
the handler's own code (an HTTP client timeout, say) stays an ordinary
transient error. The sync broker cannot interrupt a running thread: with a
deadline set it runs handlers off the I/O thread (one dedicated thread when
`worker_count=1`), and a timed-out handler keeps its worker until it
returns. See
[Reliable Messaging](reliable-messaging.md#processing-deadline-processing_timeout).

## When the ack itself fails

An ack is a frame written to the channel; the broker does not confirm it.
If writing it raises after the handler succeeded (channel closed, socket
reset), rabbitkit does not know whether the broker received it. It logs
"Ack outcome unknown", increments `HandlerPipeline.ack_outcome_unknown_total`
(and `<namespace>_settlement_outcome_unknown_total` when the route has a
`MetricsMiddleware`), and sends nothing else for that delivery. If the ack
was lost, the broker redelivers the message when the channel closes and the
handler runs again, so handlers must be idempotent. Earlier releases sent
the failed ack down the handler-failure path, which tried to reject a
message that had been processed successfully.

## Message Safety

For `AUTO` and `MANUAL` policies, rabbitkit follows this guarantee:

> The original message is never acknowledged before the retry or DLQ publish is confirmed by the broker.

If retry publishing fails, the original message is not acked. It remains unacked and will be redelivered after the connection recovers or the channel times out.

See [Message Safety](message-safety.md) for the full guarantee and failure-case analysis.

## Choosing a policy

| Scenario | Recommended policy |
|----------|--------------------|
| Standard consumer with retries | `AUTO` + `RetryConfig` |
| Handler needs external confirmation before ack | `MANUAL` |
| No retries, failed messages go straight to DLQ | `NACK_ON_ERROR` |
| Best-effort processing, loss acceptable | `ACK_FIRST` |
