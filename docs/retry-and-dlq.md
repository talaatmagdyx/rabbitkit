# Retry and Dead-Letter Queues

RabbitKit provides a built-in retry system based on RabbitMQ dead-letter exchanges and per-queue TTL. Messages that fail are routed through a series of delay queues before being promoted to a dead-letter queue (DLQ) if all retry attempts are exhausted.

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
