# RabbitKit Improvement Plan

## Positioning

**RabbitKit is a RabbitMQ-first production toolkit for Python services.**

RabbitKit helps Python teams run RabbitMQ consumers safely in production, with safe retries, dead-letter queues, explicit acknowledgement policies, topology validation, testing, observability, and Kubernetes-ready lifecycle management.

FastStream is a great multi-broker framework. RabbitKit is narrower but deeper — focused on RabbitMQ-specific production safety and operational correctness.

---

## 1. Simplify the README

The README should focus only on the core flow:

1. Install
2. Create a consumer
3. Publish a message
4. Add retry/DLQ
5. Test without RabbitMQ
6. Run in FastAPI or Kubernetes

Move advanced features to separate documentation pages.

### Recommended documentation structure

```
README.md
docs/
  getting-started.md
  core-concepts.md
  retry-and-dlq.md
  message-safety.md
  testing.md
  fastapi-integration.md
  kubernetes.md
  observability.md
  advanced/
    rpc.md
    compression.md
    signing.md
    deduplication.md
    distributed-locking.md
    stream-queues.md
    management-api.md
```

### What the README should NOT start with

- distributed locking
- circuit breaker
- message signing
- result backends
- stream queues
- dashboard
- management API

These are useful but should not be part of the beginner path.

---

## 2. Make "Message Safety" a First-Class Feature

Add a dedicated page: **Message Safety Guarantees**

### Core guarantee

RabbitKit never acknowledges the original message before the retry or DLQ message is successfully published and confirmed.

### Documented flow

1. Consumer receives a message
2. Handler raises a transient exception
3. RabbitKit publishes the message to a retry delay queue
4. RabbitKit waits for publisher confirm
5. Only after publish confirmation succeeds, RabbitKit acknowledges the original message
6. If retry publishing fails, the original message is not acknowledged

### Failure case

If retry publishing fails, RabbitKit must not ack the original message. The message should be nacked/requeued or left unacked depending on the configured policy.

This must be explicit, not vague.

---

## 3. Add a Complete Retry/DLQ Guide

Add a dedicated guide: **Retry and Dead Letter Queues**

### Full topology example

```
orders.exchange
  → orders.created.queue
  → orders.created.retry.5s
  → orders.created.retry.30s
  → orders.created.retry.120s
  → orders.created.dlq
```

### Explain

- how retry queues are declared
- how TTL is used
- how messages are routed back
- how retry count is stored
- what happens after max retries
- how poison messages are handled
- how DLQ messages can be inspected or replayed

### Example code

```python
@broker.subscriber(
    queue="orders.created",
    exchange="orders",
    routing_key="orders.created",
    retry=RetryConfig(
        max_retries=3,
        delays=(5, 30, 120),
    ),
)
async def handle_order(order: Order) -> None:
    ...
```

### Error classification table

| Case | Classification | Behavior |
|------|----------------|----------|
| TimeoutError | Transient | Retry |
| OSError | Transient | Retry |
| JSONDecodeError | Permanent | Reject to DLQ |
| ValidationError | Permanent | Reject to DLQ |
| Unknown error | Permanent by default | Reject to DLQ |

---

## 4. Clarify Ack Policies

Add a dedicated section: **Acknowledgement Policies**

### AUTO

Use for most consumers.

- Success → ack
- Transient error → retry or nack
- Permanent error → reject/DLQ

### MANUAL

Use when the handler needs full settlement control.

```python
@broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
async def handle(msg: RabbitMessage) -> None:
    try:
        await process(msg.body)
        await msg.ack_async()
    except RetryLater:
        await msg.nack_async(requeue=True)
```

### NACK_ON_ERROR

Use when messages should not be requeued automatically.

### ACK_FIRST

Use only for at-most-once processing.

**Warning:** ACK_FIRST can lose messages if the handler fails after the message is acknowledged. Use it only when message loss is acceptable.

---

## 5. Add Ordering Guarantees Section

Add a page or section: **Ordering Guarantees**

### Statement

RabbitKit follows RabbitMQ ordering semantics. It does not guarantee strict ordering when concurrency, retries, multiple consumers, or prefetch greater than one are used.

### What can affect ordering

- `worker_count > 1`
- `prefetch_count > 1`
- multiple consumers on the same queue
- retry delay queues
- requeues
- batch acknowledgements
- parallel handler execution

### Guidance for strict ordering

- set `worker_count=1`
- set `prefetch_count=1`
- avoid parallel consumers for the same queue
- avoid retry flows that can reorder messages
- partition messages by key
- use one queue per ordered stream when necessary

---

## 6. Clarify Deduplication Semantics

Add configuration for when the deduplication key is stored:

```python
DeduplicationConfig(
    mark_policy="on_success"  # or "on_start"
)
```

### mark_on_start

The message is marked as processed before the handler runs.

- **Benefit:** prevents concurrent duplicate processing
- **Risk:** if the handler fails, a retry may be skipped incorrectly

### mark_on_success

The message is marked as processed only after the handler succeeds.

- **Benefit:** safer for retries
- **Risk:** the same message may run concurrently if delivered twice at the same time

### Recommended default

`mark_on_success` — safer for most business workflows.

---

## 7. Reduce the Public API Surface

### Stable core

- `RabbitConfig`
- `AsyncBroker`
- `SyncBroker`
- `subscriber`
- `publish`
- `RabbitMessage`
- `MessageEnvelope`
- `AckPolicy`
- `RetryConfig`
- `RabbitQueue`
- `RabbitExchange`
- `TestBroker`
- health checks

### Experimental

- RPC
- dashboard
- stream queues
- distributed locking
- message signing
- result backends

Move to `rabbitkit.experimental.*` or do not release yet.

This makes the project easier to trust.

---

## 8. Improve the Publish API

### Keep the advanced API

```python
await broker.publish(MessageEnvelope(
    routing_key="orders.created",
    body=b"...",
    exchange="orders",
))
```

### Add a simpler API

```python
await broker.publish(
    exchange="orders",
    routing_key="orders.created",
    body={"order_id": 123},
    headers={"x-tenant": "acme"},
)
```

The broker internally creates the `MessageEnvelope`.

---

## 9. Rename `async_` to `aio`

### Current

```python
from rabbitkit.async_ import AsyncBroker
```

### Proposed

```python
from rabbitkit.aio import AsyncBroker
```

Short, clean, familiar to Python developers.

---

## 10. Add Production Examples

```
examples/
  basic_consumer/
  publish_message/
  retry_dlx/
  pydantic_validation/
  fastapi_lifespan/
  testbroker_pytest/
  kubernetes_worker/
  publisher_confirms/
  graceful_shutdown/
  poison_message_handling/
```

Each example should be runnable:

```bash
cd examples/retry_dlx
docker compose up
python worker.py
python publish.py
```

---

## 11. Add a Comparison Page

### Tone

Do not attack other projects. Be fair.

### Suggested comparison

| Use Case | Recommended |
|----------|-------------|
| Multi-broker framework | FastStream |
| Low-level async RabbitMQ client | aio-pika |
| Low-level sync RabbitMQ client | pika |
| Task queue with workers and scheduling | Celery |
| RabbitMQ production toolkit with safe retry/DLQ | RabbitKit |

### Positioning statement

FastStream is a great multi-broker framework. RabbitKit is for teams that are deeply invested in RabbitMQ and need stronger guarantees around retries, DLQs, acknowledgements, topology, testing, and production operations.

---

## 12. Add Benchmarks

### What to measure

- raw aio-pika
- FastStream RabbitBroker
- RabbitKit without middleware
- RabbitKit with JSON
- RabbitKit with Pydantic
- RabbitKit with retry middleware

### Metrics

- messages/second
- p50 latency
- p95 latency
- p99 latency
- CPU usage
- memory usage
- shutdown drain time
- reconnect recovery time

### Failure case benchmarks

- handler exception
- retry publish
- DLQ publish
- publisher confirm timeout
- RabbitMQ restart

---

## 13. Add Observability Defaults

### Recommended metric names

```
rabbitkit_messages_consumed_total
rabbitkit_messages_acked_total
rabbitkit_messages_nacked_total
rabbitkit_messages_rejected_total
rabbitkit_messages_retried_total
rabbitkit_messages_dead_lettered_total
rabbitkit_handler_duration_seconds
rabbitkit_handler_errors_total
rabbitkit_publish_total
rabbitkit_publish_failures_total
rabbitkit_publish_confirm_latency_seconds
rabbitkit_in_flight_messages
rabbitkit_worker_pool_pending
rabbitkit_broker_connected
rabbitkit_consumer_active
```

### Structured log fields

```
message_id
correlation_id
routing_key
exchange
queue
attempt
tenant_id
trace_id
handler
duration_ms
settlement
error_class
```

---

## 14. Add Kubernetes Production Guide

### Key points

- liveness should not fail on temporary RabbitMQ disconnect
- readiness should fail when broker is disconnected or consumers are not active
- pods should stop consuming before shutdown
- in-flight messages should drain before exit
- `terminationGracePeriodSeconds` should be greater than graceful timeout
- `preStop` can help Kubernetes remove the pod from endpoints before SIGTERM

### Example manifest

```yaml
terminationGracePeriodSeconds: 60
livenessProbe:
  exec:
    command: ["rabbitkit", "health", "liveness"]
readinessProbe:
  exec:
    command: ["rabbitkit", "health", "readiness"]
lifecycle:
  preStop:
    exec:
      command: ["sleep", "10"]
```

---

## 15. Add a Stability Policy

### Example

```
RabbitKit is pre-1.0. APIs may change.

Stable:
- broker creation
- subscriber decorator
- publish API
- RabbitConfig
- AckPolicy
- RetryConfig
- TestBroker

Experimental:
- RPC
- dashboard
- stream queues
- distributed locking
- message signing
- result backends
```

This builds trust.

---

## 16. Add Security Notes

Include:

- do not log message bodies by default
- limit maximum message size
- limit maximum decompressed size
- use TLS for broker connections
- use separate credentials per service
- validate headers
- avoid trusting user-controlled routing keys
- use constant-time comparison for HMAC
- prevent replay attacks with nonce TTL

---

## 17. Add Operational CLI Commands

### Future commands

```bash
rabbitkit topology validate
rabbitkit topology diff
rabbitkit topology apply
rabbitkit dlq inspect orders.created.dlq
rabbitkit dlq replay orders.created.dlq --limit 100
rabbitkit health readiness
rabbitkit health liveness
rabbitkit routes list
rabbitkit routes describe orders.created
```

---

## 18. Suggested Roadmap

### v0.1 — Core

- AsyncBroker
- SyncBroker
- subscriber decorator
- publish API
- JSON serialization
- ack policies
- basic topology declaration
- graceful shutdown

### v0.2 — Reliability

- RetryConfig
- DLQ support
- error classification
- publisher confirms
- message safety guarantees
- poison message handling

### v0.3 — Testing

- TestBroker
- settlement assertions
- publish failure injection
- pytest examples

### v0.4 — Production

- health checks
- metrics
- FastAPI lifespan
- Kubernetes guide
- structured logging

### v0.5 — RabbitMQ Operations

- topology validation
- management API
- DLQ inspect/replay CLI

### v1.0 — Stable API

- full documentation
- benchmarks
- migration guides
- stability guarantees

---

## 19. What to Avoid

1. **Do not market as a FastStream replacement.** RabbitKit is narrower but deeper.
2. **Do not expose every feature too early.** A huge API surface makes the project harder to trust.
3. **Do not claim production-grade without failure tests.** Production-grade means tested under failure, not just feature-rich.
4. **Do not make RPC a core selling point.** RabbitMQ RPC creates tight coupling. Keep it advanced.
5. **Do not make the dashboard the main feature.** Operators care more about metrics, logs, health checks, DLQ tools, and safe shutdown.

---

## 20. Core Promise

RabbitKit helps Python teams run RabbitMQ consumers safely in production.

The first version should focus on:

- clean consumer API
- safe retry and DLQ
- explicit ack policies
- testing without RabbitMQ
- Kubernetes-ready lifecycle
- observability
- topology validation

Advanced features should come later.

The best differentiation is not feature count. It is RabbitMQ-specific production correctness.