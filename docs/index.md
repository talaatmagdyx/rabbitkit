# rabbitkit

**RabbitMQ-first production toolkit for Python** — safe retries, dead-letter queues,
explicit acknowledgement policies, topology validation, in-process testing, and
Kubernetes-ready lifecycle management.

## Quick Start

```python
from rabbitkit import AsyncBroker, RabbitConfig
from rabbitkit.core.config import RetryConfig

broker = AsyncBroker(RabbitConfig())

@broker.subscriber(
    queue="orders",
    retry_config=RetryConfig(max_retries=3, delays=(5, 30, 120)),
)
async def handle(body: dict) -> None:
    print(f"Order: {body}")

# Publish (simple kwargs form)
await broker.publish(routing_key="orders", body={"id": 1})
```

## Documentation

- [Getting Started](guide/getting-started.md) — install, first consumer, publish, retry, test
- [User Guide](guide/full-guide.md) — 30 sections, beginner to advanced
- [API Reference](api/brokers.md) — every class, method, and config field
- [Ack Policies](ack-policy.md) — AUTO / MANUAL / NACK_ON_ERROR / ACK_FIRST
- [Retry & DLQ](retry-and-dlq.md) — topology, retry count tracking, error classification
- [Message Safety](message-safety.md) — guarantee: original never acked before retry confirmed
- [Ordering Guarantees](ordering-guarantees.md) — when order is and isn't preserved
- [Kubernetes](kubernetes.md) — liveness/readiness probes, graceful shutdown, HPA
- [Security](security.md) — TLS, HMAC signing, decompression limits, credential hygiene
- [Stability Policy](stability-policy.md) — stable vs experimental API surface
- [Comparison](comparison.md) — vs FastStream, aio-pika, Celery
- [Roadmap](roadmap.md) — what's shipped and what's planned
- [CLI Reference](api/cli.md) — `topology validate/diff/apply`, `dlq inspect/replay`, `health`, `routes`

## Installation

```bash
pip install rabbitkit[async]    # async transport (aio-pika)
pip install rabbitkit[sync]     # sync transport (pika)
pip install rabbitkit[all]      # everything
```

## Imports

```python
# Top-level (recommended)
from rabbitkit import AsyncBroker, SyncBroker, RabbitConfig

# Alias for the async package
from rabbitkit.aio import AsyncBroker

# Experimental features (may change between releases)
from rabbitkit.experimental import rpc, locking, dashboard
```
