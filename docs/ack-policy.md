# Acknowledgement Policies

rabbitkit supports four acknowledgement policies, selected per subscriber via `ack_policy=`.

## AUTO (default)

rabbitkit manages settlement automatically based on handler outcome.

| Handler result | Action |
|----------------|--------|
| Returns normally | `basic.ack` |
| Raises transient exception | retry or `basic.nack(requeue=False)` |
| Raises permanent exception | `basic.reject` → DLQ |

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

**Warning:** If you forget to ack/nack, the message stays unacked until the connection closes.

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
