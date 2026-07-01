# Overview

rabbitkit is a RabbitMQ-first production toolkit for Python. Core features:

- **Decorator-based routing** — `@subscriber` and `@publisher` with Pydantic body validation
- **Safe retry + DLQ** — delay queues, publisher confirms, error classification; original message never acked before retry is confirmed
- **Explicit ack policies** — `AUTO`, `MANUAL`, `NACK_ON_ERROR`, `ACK_FIRST`
- **Middleware pipeline** — deduplication, rate limiting, compression, timeout, metrics
- **Dependency injection** — `Depends()`, `Header()`, `Path()`, `Context()`
- **Sync and async** transports — pika (`SyncBroker`) + aio-pika (`AsyncBroker`)
- **Health checks** — liveness + readiness for Kubernetes
- **Topology CLI** — `validate`, `diff`, `apply` against a live broker
- **DLQ CLI** — `inspect` and `replay` dead-letter queues
- **Testing** — in-memory `TestBroker` (no RabbitMQ required)

## Import paths

```python
# Top-level (recommended for stable APIs)
from rabbitkit import AsyncBroker, SyncBroker, RabbitConfig

# rabbitkit.aio is a clean alias for rabbitkit.async_
from rabbitkit.aio import AsyncBroker

# Experimental features — may change between releases
from rabbitkit.experimental import rpc, locking, dashboard
```

## Deduplication mark policy

`DeduplicationConfig` now supports a `mark_policy` field:

```python
from rabbitkit.core.config import DeduplicationConfig

DeduplicationConfig(
    mark_policy="on_success",  # default — safer for retries
    # mark_policy="on_start",  # marks before handler — prevents concurrent duplicates
)
```

- `"on_success"` — the dedup key is recorded only after the handler succeeds. A retry of a failed message will be reprocessed.
- `"on_start"` — the key is recorded before the handler runs. Prevents two concurrent deliveries of the same message from both executing, but a failed handler will block retries.

## Simple publish API

```python
# Kwargs form — no MessageEnvelope required
await broker.publish(routing_key="orders", body={"id": 1})
await broker.publish(routing_key="orders.created", body=b"...", exchange="events", headers={"x-tenant": "acme"})

# Full control via MessageEnvelope
from rabbitkit import MessageEnvelope
await broker.publish(MessageEnvelope(routing_key="orders", body=b"..."))
```

See the [Full Guide](full-guide.md) for detailed usage.
