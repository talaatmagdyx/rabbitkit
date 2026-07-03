# rabbitkit

**The RabbitMQ client that treats message loss as a bug, not an edge case.**

rabbitkit is a RabbitMQ-first toolkit for Python services that can't afford
to lose messages. It wraps `pika` and `aio-pika` with safe retry + dead-letter
topology, explicit acknowledgement policies, publisher confirms, and an
in-memory `TestBroker` so your CI never needs a real broker.

No multi-broker abstraction, no task-queue semantics — just RabbitMQ, done
carefully. A durable, retrying consumer in under 10 minutes.

**Docs:** [Full Guide](docs/guide/full-guide.md) · [Getting Started](docs/guide/getting-started.md) · [Retry & DLQ](docs/retry-and-dlq.md) · [Ack Policies](docs/ack-policy.md) · [Message Safety](docs/message-safety.md) · [Kubernetes](docs/kubernetes.md) · [Production Checklist](docs/production/checklist.md) · [Security](docs/security.md) · [Stability Policy](docs/stability-policy.md) · [Troubleshooting](docs/troubleshooting.md) · [Comparison](docs/comparison.md)

## Stable core

- **Decorator-based routing** — `@broker.subscriber(queue=..., exchange=..., routing_key=...)`
- **Sync + async** — `SyncBroker` (pika) and `AsyncBroker` (aio-pika) with identical public APIs
- **Retry with delay queues** — TTL + DLX topology, configurable backoff, per-queue isolation, and a trusted retry-count header (a producer can't spoof it into skipping the DLQ or retrying forever)
- **Dead-letter queues** — automatic DLQ routing after max retries
- **Explicit ack policies** — `AUTO`, `MANUAL`, `NACK_ON_ERROR`, `ACK_FIRST` — you always know who owns settlement
- **Publisher confirms** — real delivery confirmation via `PublishOutcome`, not a fire-and-forget guess
- **Topology management** — `AUTO_DECLARE`, `PASSIVE_ONLY`, `MANUAL` modes
- **Testing without RabbitMQ** — `TestBroker` exercises the *real* pipeline (real settlement, real ack/nack tracking) in memory
- **Health checks for Kubernetes** — liveness, readiness, and full health status, correctly separated so a transient broker blip doesn't cause cascading pod restarts
- **Structured logging** — structlog with per-message context and secret redaction
- **Serialization** — JSON, msgspec, Pydantic auto-validation
- **Dependency injection** — `Depends()`, `Header()`, `Path()`, `Context()` markers, zero setup required
- **FastAPI integration** — `rabbitkit_lifespan()` async context manager

**Beyond the core:** publish-side backpressure, batch publishing, a DLQ
inspector CLI, a management API client, and a circuit breaker are genuinely
useful production features with real complexity — see the
[Advanced features](#advanced-and-experimental-features) section.
RPC, distributed locking, message signing, result backends, stream queues,
and the monitoring dashboard are **experimental** — real, but with sharp
edges you should understand before depending on them. See
[`docs/stability-policy.md`](docs/stability-policy.md) for exactly what
"stable" means here and what isn't covered yet.

## Installation

```bash
pip install rabbitkit[sync]         # SyncBroker (pika)
pip install rabbitkit[async]        # AsyncBroker (aio-pika)
pip install rabbitkit[all-brokers]  # both transports
pip install rabbitkit[all]          # everything, including advanced/experimental extras
```

Requires **Python >= 3.11**. See [`docs/guide/full-guide.md`](docs/guide/full-guide.md#1-getting-started) for the full extras table (compression, redis, pydantic, msgspec, management, dashboard, cli, and more).

## Quick Start

### 1. A consumer

```python
from rabbitkit import RabbitConfig, AsyncBroker

broker = AsyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
async def handle_order(body: dict) -> None:
    print(f"order id={body['id']}")

await broker.start()
```

### 2. Publish a message

```python
await broker.publish(
    exchange="orders",
    routing_key="orders",
    body={"id": 42, "item": "widget"},
)
```

### 3. Add retry and a dead-letter queue

```python
from rabbitkit import RetryConfig

@broker.subscriber(
    queue="orders",
    retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),
)
async def handle_order(body: dict) -> None:
    ...   # retried with backoff on transient errors; dead-lettered after 3 failures
```

Setting `retry=` declares the delay-queue/DLX topology *and* installs the
retry middleware together, from one switch — see
[`docs/retry-and-dlq.md`](docs/retry-and-dlq.md) for the full topology,
error classification, and DLQ recovery.

**Before you rely on this for anything with side effects (payments, emails,
external API calls): read [the idempotency contract](docs/production/idempotency.md).**
At-least-once delivery means a handler can run more than once for the same
message — retry and DLQ correctness don't remove that requirement, they
make it safe *if* your handler is idempotent.

### 4. Test it without RabbitMQ

```python
from rabbitkit.testing import TestBroker

def test_order_handler():
    broker = TestBroker()

    @broker.subscriber(queue="orders")
    def handle(body: dict) -> None:
        assert body["id"] == 42

    broker.start()
    broker.publish("orders", b'{"id": 42}')
    broker.stop()
```

`TestBroker` is not a mock — it runs the same pipeline, settlement, and
ack/nack tracking your production broker does, just without a socket. See
[`docs/guide/full-guide.md#25-testing`](docs/guide/full-guide.md).

### 5. Run it in FastAPI

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from rabbitkit.fastapi import rabbitkit_lifespan

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with rabbitkit_lifespan(broker):
        yield

app = FastAPI(lifespan=lifespan)
```

### Sync example

```python
from rabbitkit import RabbitConfig
from rabbitkit.sync import SyncBroker

broker = SyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
def handle_order(body: bytes) -> None:
    print(f"Received order: {body}")

broker.start()
```

**Publish-only `SyncBroker` (no subscribers, never calls `run()`):** nothing
else drives the connection's I/O loop, so a long idle gap between
`publish()` calls can get the connection heartbeat-timed-out broker-side.
Call `broker.pump_idle()` periodically from your own idle loop (same thread
that called `start()`) to reconnect proactively and keep the liveness
heartbeat fresh. `AsyncBroker` needs no equivalent — see
[the sync-vs-async connection model note](docs/guide/full-guide.md#sync-vs-async-two-different-connection-models)
for why.

**Sync confirmed-publish throughput ceiling (~0.9k msg/s):** `SyncBroker.publish()`
waits for a publisher confirm per message on a single channel, so throughput
is bounded by broker round-trip latency (~900 msg/s in the benchmarks),
*regardless of how many worker threads publish* — pika's `BlockingConnection`
serializes confirms and does not pipeline them. This is fine for
publish-alongside-consume workloads, but if you need to drain a large backlog
(e.g. an outbox after an outage), use `AsyncBroker` with `AsyncBatchPublisher`
(pipelined confirms, ~6.1k msg/s) or scale out across processes. `worker_count`
does **not** raise sync publish throughput. `highload.BatchPublisher` improves
ergonomics, not the confirm ceiling.

## What's next

- **[Full Guide](docs/guide/full-guide.md)** — every feature, in depth: configuration, routing, ack policies, DI, middleware, retry, serialization, high-load infrastructure, RPC, backpressure, health checks, locking, deduplication, circuit breaking, signing, compression, result backends, stream queues, AsyncAPI, the management API, the dashboard, the CLI, testing, FastAPI, environment config, Kubernetes, app lifecycle, and architecture.
- **[Production Checklist](docs/production/checklist.md)** — what to configure before you trust this with real traffic.
- **[Idempotency Contract](docs/production/idempotency.md)** — the one thing "safe retries" doesn't do for you automatically.
- **[Troubleshooting](docs/troubleshooting.md)** — symptom → cause → fix for the issues people actually hit.
- **[Stability Policy](docs/stability-policy.md)** — what's frozen, what's advanced, what's experimental, and why.
- **[Kubernetes Guide](docs/kubernetes.md)** — probes, graceful shutdown, and a full deployment manifest.
- **[Security](docs/security.md)** — signing, replay protection, TLS, and safe defaults.

## Advanced and experimental features

Real features, real complexity — deliberately not part of the 10-minute
story above. See the [Full Guide](docs/guide/full-guide.md) for usage of
each.

**Advanced Stable** (production-grade, opt in deliberately):
publish-side backpressure (`FlowController`), batch publishing/acking,
the DLQ inspector + CLI, the RabbitMQ Management API client, the CLI, and
`CircuitBreakerMiddleware` (note: it's a no-op without a real circuit
breaker implementation such as `obskit` — see the guide before adopting it).

**Experimental** (`rabbitkit.experimental` — may change without a
deprecation cycle, read the caveats before depending on them):
RPC over direct reply-to, distributed locking (`RedisLock` has no TTL
renewal), message signing (default nonce cache is per-process — wire a
shared cache for real replay protection), result backends (task-queue-style
semantics, deliberately out of scope for rabbitkit's core), RabbitMQ stream
queues, and the monitoring dashboard (**unauthenticated by default** — never
expose it without `auth_token=` and a reverse proxy).

## Architecture

```
rabbitkit/
  core/                # Business logic -- ZERO transport imports
  sync/                # pika adapter (AMQP 0-9-1)
  async_/              # aio-pika adapter (AMQP 0-9-1)
  middleware/           # retry, compression, dedup, rate limit, timeout, tracing, metrics
                         #   (+ circuit breaker, signing -- see stability policy)
  serialization/        # JSON, msgspec, pydantic, two-stage pipeline
  di/                   # Depends, Header, Path, Context, resolver
  testing/              # TestBroker, TestApp, fixtures
  highload/             # FlowController, BatchPublisher, BatchAcker
  experimental/         # RPC, locking, signing, results, streams, dashboard -- see stability policy
  fastapi.py            # rabbitkit_lifespan
```

The shared core has zero transport dependencies; sync and async adapters are
thin I/O layers. See [`docs/guide/full-guide.md#30-architecture--design-patterns`](docs/guide/full-guide.md)
for design patterns, invariants, and the sync-vs-async connection model.

## Compatibility

- **Python**: >= 3.11 (3.11, 3.12, 3.13)
- **RabbitMQ**: >= 3.12
- **pika**: >= 1.3, < 2.0
- **aio-pika**: >= 9.1, < 10.0 (`9.0.0` imports `pkg_resources`, which recent `setuptools` no longer ships -- see `docs/troubleshooting.md`)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for dev setup, quality gates, and PR
guidelines. This project follows the
[Contributor Covenant](CODE_OF_CONDUCT.md).

## Security

Found a vulnerability? See [SECURITY.md](SECURITY.md) for how to report it
privately — please do not open a public issue.

## License

MIT
