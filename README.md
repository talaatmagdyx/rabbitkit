<p align="center"><img src="https://raw.githubusercontent.com/talaatmagdyx/rabbitkit/main/assets/logo.svg" alt="rabbitkit" width="420"></p>

# rabbitkit

**RabbitMQ made enjoyable — less broker plumbing, more business logic.**

[![PyPI](https://img.shields.io/pypi/v/rabbitkit)](https://pypi.org/project/rabbitkit/)
[![CI](https://github.com/talaatmagdyx/rabbitkit/actions/workflows/ci.yml/badge.svg)](https://github.com/talaatmagdyx/rabbitkit/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)](https://github.com/talaatmagdyx/rabbitkit/blob/main/pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/talaatmagdyx/rabbitkit/blob/main/LICENSE)
[![Typed](https://img.shields.io/badge/types-mypy%20--strict-blue)](https://github.com/talaatmagdyx/rabbitkit/blob/main/pyproject.toml)
[![Style](https://img.shields.io/badge/style-ruff-261230)](https://github.com/talaatmagdyx/rabbitkit/blob/main/pyproject.toml)

rabbitkit is a **RabbitMQ-first toolkit for Python services**. It gives you
clean decorators, safe retries, dead-letter queues, publisher confirms,
explicit acknowledgement policies, Kubernetes-ready lifecycle hooks,
structured logging, OpenTelemetry tracing, and real in-memory testing — so
your team can focus on what each message *means*, not how the broker behaves
when things fail.

RabbitMQ is powerful. Production RabbitMQ is full of sharp edges.
rabbitkit smooths those edges **without hiding the broker from you**.

```python
from rabbitkit import AsyncBroker, RabbitConfig
from rabbitkit.serialization import JSONSerializer

broker = AsyncBroker(RabbitConfig(), serializer=JSONSerializer())

@broker.subscriber(queue="orders.created")
async def handle_order(order: dict) -> None:
    await fulfill_order(order)
```

That should feel like application code. The retry topology,
acknowledgements, confirms, DLQs, shutdown behavior, and test harness should
not be rewritten in every service. **That is what rabbitkit is for.**

**Contents:** 
- [Believes](#what-rabbitkit-believes)
- [Why](#why-rabbitkit-exists)
- [Install](#installation)
- [Quick start](#quick-start)
- [Safety model](#message-safety-model)
- [Failure table](#what-happens-when-things-fail)
- [Ack policies](#acknowledgement-policies)
- [Production profile](#production-profile)
- [Observability](#observability)
- [DI](#dependency-injection)
- [Middleware](#middleware-batteries-included)
- [CLI](#operate-it-from-the-terminal)
- [Where it fits](#where-rabbitkit-fits)
- [Performance](#performance)
- [Migrating](#migrating-from-aio-pika-pika-or-celery)
- [Examples](#examples)
- [Architecture](#architecture)
- [Docs](#documentation)

---

## What rabbitkit believes

Most services need the same things:

- a clean way to register consumers
- safe retry behavior
- a place for poison messages to go
- publisher confirms that are **checked**
- explicit acknowledgement ownership
- graceful shutdown
- useful logs and traces
- health checks that behave correctly in Kubernetes
- tests that do not require a live broker

rabbitkit packages those concerns into one focused toolkit. The philosophy:

> **Make RabbitMQ pleasant for developers and predictable for operators.**

Developers get a clean programming model. Operators get visible message
outcomes. Production gets fewer silent failure paths.

## Why rabbitkit exists

Starting with RabbitMQ is easy: `basic_publish(...)`, `basic_consume(...)`.
Then production asks better questions:

- What happens if a handler fails *forever*?
- Where does a malformed payload go?
- Can a rejected message disappear because the queue had no DLX?
- Did the retry publish **confirm** before the original was acknowledged?
- Can a DLQ replay remove a message before the republish is confirmed?
- Can a pod shut down without interrupting in-flight work?
- Can CI test real consumer behavior without starting RabbitMQ?

rabbitkit exists for those questions. Its goal is not to turn RabbitMQ into
something else — it is to make direct RabbitMQ usage feel like good
application code: clear routing, safe defaults, explicit outcomes, real
tests, production-ready lifecycle.

**rabbitkit is:**

- a RabbitMQ-first toolkit
- a clean consumer/publisher API
- a reliability layer over `pika` and `aio-pika`
- a testing layer for handlers
- a production lifecycle layer
- safety defaults for retry, DLQ, confirms, and acks

**rabbitkit is not:**

- a task queue
- a scheduler
- a generic event-streaming abstraction
- a replacement for understanding RabbitMQ
- an exactly-once delivery system

---

## Installation

Available on PyPI: **[pypi.org/project/rabbitkit](https://pypi.org/project/rabbitkit/)**

```bash
pip install rabbitkit[async]        # AsyncBroker (aio-pika)
pip install rabbitkit[sync]         # SyncBroker (pika)
pip install rabbitkit[all-brokers]  # both transports
pip install rabbitkit[all]          # everything optional
```

Requires Python ≥ 3.11.

## The 10-minute path

A durable, retrying, DLQ-backed consumer — tested without a broker:

1. `pip install rabbitkit[async]`
2. Create an `AsyncBroker(RabbitConfig())`
3. Register a handler with `@broker.subscriber(queue=...)`
4. Add `retry=RetryConfig(max_retries=3, delays=(5, 30, 120))`
5. Run it: `rabbitkit run myapp.main:broker`
6. Test it in CI with `TestBroker` — no RabbitMQ required

Each step is shown below.

## Quick start

### 1. Create a consumer

```python
from rabbitkit import RabbitConfig, AsyncBroker
from rabbitkit.serialization import JSONSerializer

# serializer= is what turns the raw bytes into your handler's annotation —
# without it, `body: dict` receives bytes.
broker = AsyncBroker(RabbitConfig(), serializer=JSONSerializer())

@broker.subscriber(queue="orders.created")
async def handle_order(body: dict) -> None:
    print(f"order id={body['id']}")

async def main() -> None:
    await broker.start()
```

That is enough to consume messages. But production usually needs more than
"enough".

### 2. Publish — and check the outcome

```python
async def publish_order() -> None:
    outcome = await broker.publish(
        exchange="orders",
        routing_key="orders.created",
        body={"id": 42, "item": "widget"},
    )
    outcome.raise_for_status()
```

A publish can be `CONFIRMED`, `SENT`, `RETURNED`, `NACKED`, `TIMEOUT`, or
`ERROR`. Application code can treat those as different states instead of
assuming "publish called" means "message safe".

### 3. Add retry and DLQ handling

```python
from rabbitkit import RetryConfig

@broker.subscriber(
    queue="orders.created",
    exchange="orders",
    routing_key="orders.created",
    retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),
)
async def handle_order_with_retry(body: dict) -> None:
    await fulfill_order(body)
```

This wires the reliability path — **broker-side**, carried in hardened
headers, surviving crashes and reconnects:

```
orders.created
  → orders.created.retry.1   (5s)
  → orders.created.retry.2   (30s)
  → orders.created.retry.3   (120s)
  → orders.created.dlq
```

Transient failures retry with backoff. Permanent failures skip the ladder
and go straight to the DLQ. By default, rejected messages do not disappear
silently — every rejecting route gets a DLQ unless you explicitly opt into
discard behavior.

### 4. Test it without RabbitMQ

```python
from rabbitkit.testing import TestBroker
from rabbitkit.serialization import JSONSerializer

def test_order_handler():
    broker = TestBroker(serializer=JSONSerializer())

    @broker.subscriber(queue="orders.created")
    def handle(body: dict) -> None:
        assert body["id"] == 42

    broker.start()
    broker.publish("orders.created", b'{"id": 42}')
    broker.stop()
```

`TestBroker` is not a mock. It runs the real routing, middleware,
serialization, dependency resolution, settlement, and ack/nack pipeline in
memory. Your CI can test RabbitMQ behavior without running RabbitMQ.

### 5. Run with FastAPI

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from rabbitkit import RabbitConfig, AsyncBroker
from rabbitkit.fastapi import rabbitkit_lifespan

api_broker = AsyncBroker(RabbitConfig(), serializer=JSONSerializer())

@api_broker.subscriber(queue="orders.created")
async def handle_order_event(body: dict) -> None:
    ...

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with rabbitkit_lifespan(broker=api_broker):
        yield

app = FastAPI(lifespan=lifespan)
```

### Sync example

```python
from rabbitkit import RabbitConfig
from rabbitkit.sync import SyncBroker

sync_broker = SyncBroker(RabbitConfig())

@sync_broker.subscriber(queue="orders.created")
def handle_order_sync(body: bytes) -> None:
    print(f"received order: {body!r}")

def main() -> None:
    # Blocks until SIGINT/SIGTERM or stop(); reconnects on connection drops
    # and drains in-flight work on pod termination.
    sync_broker.run()
```

The sync broker fits simple workers, scripts, legacy services, and teams
that do not want an asyncio runtime. **Throughput note:** sync confirmed
publishing waits one confirm per message (~0.9k msg/s measured);
`worker_count` does not raise it. For high-throughput confirmed publishing
use `AsyncBroker` + `AsyncBatchPublisher` (pipelined confirms, ~6.1k msg/s
measured) or `SyncBatchPublisher` (pipelined confirms for sync code on a
dedicated I/O thread), or scale out across processes.

---

## Message safety model

rabbitkit is an **at-least-once** toolkit: a handler may run more than once
(crash after work but before ack, connection death mid-handler, DLQ replay,
producer retry after a confirm timeout…). rabbitkit removes dangerous
ambiguity around those cases — it does not remove the need for idempotency.

For payments, emails, tickets, webhooks, external API calls: design the
handler so running it twice is safe (idempotency keys, unique constraints,
processed-event tables, outbox patterns — or rabbitkit's deduplication
middleware, whose `store_results` mode replays the original result to
duplicates). The rule is simple:

> rabbitkit can help you retry safely. Your business logic must still be
> safe to retry.

## What happens when things fail?

| Failure mode | rabbitkit behavior |
|---|---|
| Handler raises forever | Retry ladder, then DLQ |
| Malformed payload | Classified permanent, preserved in DLQ |
| Reject with no DLX | Safe default auto-provisions a DLQ — no silent discard |
| Retry publish times out | Original is **not** acked as if the retry succeeded |
| DLQ replay publish fails | DLQ message is **not** removed as if replay succeeded |
| Message unroutable | Mandatory publishing returns a distinct `RETURNED` outcome |
| Broker blips | Readiness changes; liveness does **not** kill the pod |
| Pod gets SIGTERM | Consumers stop first, in-flight work drains |
| CI has no RabbitMQ | `TestBroker` runs the real pipeline in memory |
| Your own mistake (bad config, invalid topology, publish before `start()`, oversized body) | A **typed exception** at the line that made it — `ConfigValidationError`, `TopologyValidationError`, `BrokerNotStartedError`, `MessageTooLargeError`, … — each subclassing the builtin (`ValueError`/`RuntimeError`) it replaces, so plain `except ValueError` still works |

## Acknowledgement policies

Settlement is a decision, not a side effect hidden in a callback.

| Policy | Behavior | Use case |
|---|---|---|
| `AUTO` | Ack on success, retry/reject on failure | Most consumers |
| `MANUAL` | Handler owns ack/nack/reject | Custom settlement flows |
| `NACK_ON_ERROR` | Ack on success, nack on failure | Never silently accept failed work |
| `ACK_FIRST` | Ack before the handler runs | At-most-once workloads |

`ACK_FIRST` can lose messages if the handler fails after the ack — use it
only when loss is acceptable.

## Production profile

The recommended baseline (see the
[production checklist](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/production/checklist.md)): quorum queues (+
`delivery_limit`), per-queue retry/DLQ topology, publisher confirms on,
mandatory publishing where routing matters, checked `PublishOutcome`s,
explicit ack policies, structured logs, split readiness/liveness probes,
management-API metrics for queue depth and consumer lag, idempotent
handlers. Migrating existing classic queues to quorum? There's a tool:
`rabbitkit topology migrate` ([guide](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/quorum-migration.md)).

## Observability

Structured logs carry message context (`message_id`, `correlation_id`,
routing, queue, handler, retry count, settlement, duration, error type) with
secret redaction on by default. Metrics cover consumed/acked/nacked/
retried/dead-lettered counts, publish outcomes, handler latency,
redeliveries, reconnects, and — via the management API poller — queue depth
and consumer lag. Tracing is standard OpenTelemetry
(`pip install rabbitkit[otel]`): W3C context propagation over AMQP headers,
one continuous trace from publish to consume.

## Advanced & experimental

**Advanced stable** (enable deliberately): publish-side backpressure
(`FlowController`), batch publishing/acking, pipelined sync confirms
(`SyncBatchPublisher`), DLQ inspector + replay CLI, management API client,
topology validation/drift/migration CLI, health watcher, circuit-breaker
middleware (bring any `CircuitBreakerProtocol` implementation, e.g.
pybreaker).

**Experimental** (may change without a deprecation cycle — read the
[stability policy](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/stability-policy.md)): RPC over direct reply-to,
distributed locking, message signing, result backends, stream queues, the
monitoring dashboard. Notable caveats: the default signing nonce cache is
per-process (use a shared cache for real replay protection), and never
expose the dashboard publicly without authentication.

## Dependency injection

Handlers resolve request-like context declaratively — typed body, headers,
routing-key segments, and shared dependencies:

```python
from rabbitkit import AsyncBroker, Context, Depends, Header, Path, RabbitConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.serialization import JSONSerializer

di_broker = AsyncBroker(RabbitConfig(), serializer=JSONSerializer())

def get_db() -> str:
    return "db-connection"

@di_broker.subscriber(queue="tenants.{tenant_id}.orders")
async def handle_tenant_order(
    body: dict,
    tenant_id: str = Path(),
    trace_id: str = Header("x-trace-id", default=""),
    db: str = Depends(get_db),
    message: RabbitMessage = Context(),
) -> None:
    ...
```

Serialization is pluggable per route: raw bytes, JSON (default), Pydantic
models, msgspec structs, or a custom parser/decoder pipeline — annotate the
body parameter with the type you want and pick the serializer that
validates it.

## Middleware, batteries included

| Middleware | Job |
|---|---|
| `RetryMiddleware` | Broker-side retry ladder (auto-wired with `retry=`) |
| `DeduplicationMiddleware` | Redis-backed duplicate suppression; `claim` policy is crash-safe; `store_results` replays the original answer to duplicates |
| `MetricsMiddleware` | Counters + latency histograms, cardinality-guarded labels |
| `OTelTracingMiddleware` | Standard OpenTelemetry spans + W3C propagation |
| `CompressionMiddleware` | gzip/zstd with streaming zip-bomb guards |
| `RateLimitMiddleware` | Token-bucket consume throttling (nack/drop/wait) |
| `TimeoutMiddleware` | Per-handler deadlines, retry-classified |
| `CircuitBreakerMiddleware` | Wraps any `CircuitBreakerProtocol` implementation |
| `SigningMiddleware` | HMAC signing + replay protection (experimental) |

## Operate it from the terminal

```bash
rabbitkit run myapp.main:broker                   # run consumers
rabbitkit dlq inspect orders.dlq                  # peek at poison messages
rabbitkit dlq replay orders.dlq orders --reset-retry-count
rabbitkit topology validate myapp.main:broker     # declared vs live drift
rabbitkit topology migrate myapp.main:broker      # classic -> quorum, planned & resumable
rabbitkit health myapp.main:broker
```

The DLQ replay acks a message only after its republish is broker-confirmed —
the recovery tool cannot itself lose messages.

<p align="center"><img src="https://raw.githubusercontent.com/talaatmagdyx/rabbitkit/main/assets/demo.svg" alt="rabbitkit dlq inspect and replay demo" width="720"></p>

## Where rabbitkit fits

rabbitkit sits *above* `pika` and `aio-pika` — a reliability layer, not a
replacement; drop to the underlying client any time. It is for teams that
use RabbitMQ directly and want production-safe messaging without rebuilding
retries, DLQs, publisher confirms, acknowledgements, lifecycle handling, and
test infrastructure in every service.

**A good fit when:**

- RabbitMQ is your primary broker
- message loss would be an incident
- retry and DLQ behavior must be explicit
- CI should test handlers without a real broker
- Kubernetes shutdown and readiness matter
- operators need visibility into message outcomes

**Do NOT use rabbitkit when:**

- **You need scheduled or delayed jobs as a first-class feature.** There
  is no `beat`, no cron, no `eta=`. Use Celery/arq/APScheduler, or keep
  a scheduler alongside rabbitkit.
- **You need task canvases** (chains, chords, groups, result-first
  workflows). That's a task framework's job; rabbitkit will not grow one.
- **You might switch brokers.** rabbitkit is RabbitMQ-only on purpose —
  Kafka/SQS/Redis Streams are not coming. Broker portability → FastStream.
- **Your team must not see AMQP concepts.** rabbitkit deliberately keeps
  exchanges, bindings, acks, and confirms visible; it smooths them, it
  does not hide them.
- **At-most-once is fine and volume is trivial.** A 30-line raw
  `aio-pika` consumer with `message.process()` may be all you need —
  rabbitkit earns its keep when loss, retries, and operations matter.
- **You need exactly-once delivery.** Nothing on RabbitMQ gives you
  that, including rabbitkit; the dedup middleware gives idempotent
  *processing*, which is a different (weaker, honest) guarantee.

A detailed framework-by-framework comparison lives in
[docs/comparison.md](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/comparison.md).

## Performance

Measured against **bare aio-pika doing identical work** — interleaved
A/B, 5 repetitions, median ± CV, GitHub-hosted runner (methodology:
[docs/benchmarking.md](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/benchmarking.md)):

| Metric | raw aio-pika | rabbitkit | delta |
|---|---|---|---|
| Consume (drain, prefetch 200) | 6,899 ± 4.8% msg/s | 6,273 ± 0.9% msg/s | **+9.1% overhead** |
| Confirmed publish (sequential) | 2,544 ± 2.4% msg/s | 2,558 ± 3.5% msg/s | ≈ 0% |
| End-to-end latency @400 msg/s (open-loop) | — | p50 1.3 ms · p99 1.8 ms · p99.9 44 ms | — |
| Quorum vs classic queue (consume) | — | 4,659 vs 5,546 msg/s | quorum ≈ −16% |

That +9.1% buys the full pipeline: middleware chain, DI resolution,
serialization dispatch, ack orchestration, retry/DLQ safety, and
metrics. Absolute numbers vary by machine — the ratios are the stable
part. In-memory pipeline overhead alone (no broker) runs 32–38k msg/s.

## Migrating from aio-pika, pika, or Celery

rabbitkit consumers and your existing consumers can share the same
broker and even the same queues, so migration is one consumer at a
time — no big-bang cutover. The
[migration guide](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/migrating-to-rabbitkit.md)
has before/after code for all three paths; the short version:

- **From aio-pika/pika**: your `on_message` callback body becomes the
  handler; connection lifecycle, QoS, declaration, ack/reject logic, and
  all retry topology disappear. Sync consumers additionally gain
  reconnect-on-failure, which `BlockingConnection` never had.
- **From Celery**: `@app.task` → `@broker.subscriber(queue=...)`,
  `task.delay()` → `broker.publish()` — but read the guide's honest
  table first: if you depend on beat or canvases, stay on Celery.

## Examples

**[examples/](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples)** —
25 self-contained, runnable projects covering every feature, each with its
own README. They run against a real broker in CI on every nightly build, so
they can't silently drift from the API.

Start here:

| Want to… | Example |
|---|---|
| See the smallest working consumer | [`quickstart/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/quickstart) |
| Wire retry ladders + DLQ | [`retry_dlx/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/retry_dlx) · [`poison_message_handling/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/poison_message_handling) |
| Serve alongside FastAPI | [`fastapi_lifespan/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/fastapi_lifespan) |
| Run in Kubernetes with probes | [`kubernetes_worker/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/kubernetes_worker) |
| Test handlers without a broker | [`testbroker_pytest/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/testbroker_pytest) |
| Do RPC over RabbitMQ | [`rpc/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/rpc) |
| Push throughput (batching, pools, backpressure) | [`highload/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/highload) |
| See a full production service | [`order_service/`](https://github.com/talaatmagdyx/rabbitkit/tree/main/examples/order_service) |

```bash
# every example expects RabbitMQ on localhost:5672 (some also Redis on 6379)
docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
python examples/quickstart/02_async_broker.py
```

The full index (all 25, grouped by topic) is in
[examples/README.md](https://github.com/talaatmagdyx/rabbitkit/blob/main/examples/README.md).

## Architecture

```
rabbitkit/
  core/                 # route registry, topology, pipeline, settlement, config
  sync/                 # pika adapter (+ SyncBatchPublisher)
  async_/               # aio-pika adapter (+ AsyncBatchPublisher)
  middleware/           # retry, dedup, metrics, otel, compression, rate limit…
  serialization/        # JSON, msgspec, Pydantic, parser/decoder pipeline
  di/                   # Depends, Header, Path, Context
  testing/              # TestBroker and friends
  highload/             # FlowController, BatchPublisher, BatchAcker
  cli/                  # dlq, topology, migrate, health, run, shell
  fastapi.py            # FastAPI lifespan integration
```

The shared core has **zero** imports from `pika` or `aio-pika` — both
transports are adapters over the same registry, pipeline, topology model,
and settlement rules.

## Compatibility

Python ≥ 3.11 (tested: 3.11 / 3.12 / 3.13 / 3.14; 3.15 pre-release experimental) · RabbitMQ ≥ 3.12 recommended ·
`pika >= 1.3, < 2.0` · `aio-pika >= 9.1, < 10.0`

## Documentation

**📚 Full rendered docs: [talaatmagdyx.github.io/rabbitkit](https://talaatmagdyx.github.io/rabbitkit/)**

- [Getting Started](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/guide/getting-started.md)
- [Full Guide](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/guide/full-guide.md)
- [Message Safety](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/message-safety.md)
- [Retry & DLQ](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/retry-and-dlq.md)
- [Production Patterns — reference code](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/production/patterns.md)
- [Production Checklist](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/production/checklist.md)
- [Idempotency Contract](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/production/idempotency.md)
- [Kubernetes](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/kubernetes.md)
- [Quorum Migration](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/quorum-migration.md)
- [Security](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/security.md)
- [Stability Policy](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/stability-policy.md)
- [Troubleshooting](https://github.com/talaatmagdyx/rabbitkit/blob/main/docs/troubleshooting.md)

## Contributing & security

See [CONTRIBUTING.md](https://github.com/talaatmagdyx/rabbitkit/blob/main/CONTRIBUTING.md) for local development and quality
gates (ruff, `mypy --strict`, near-total test coverage — the bar is real).
Found a vulnerability? Follow [SECURITY.md](https://github.com/talaatmagdyx/rabbitkit/blob/main/SECURITY.md) and report it
privately.

## License

[MIT](https://github.com/talaatmagdyx/rabbitkit/blob/main/LICENSE)
