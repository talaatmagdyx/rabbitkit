# rabbitkit — Complete User Guide

> **Production-grade RabbitMQ toolkit for Python** — sync (pika) and async (aio-pika),
> decorator-based routing, middleware pipeline, retry, compression, DI, RPC, and more.
> Version 1.1.1 · Python ≥ 3.11 · MIT License

## Table of Contents

1. [Getting Started](#1-getting-started)
2. [Core Concepts](#2-core-concepts)
3. [Configuration](#3-configuration)
4. [Routing](#4-routing)
5. [Message Processing & Ack Policies](#5-message-processing--ack-policies)
6. [Dependency Injection](#6-dependency-injection)
7. [Middleware](#7-middleware)
8. [Retry & Error Handling](#8-retry--error-handling)
9. [Serialization](#9-serialization)
10. [High-Load Infrastructure](#10-high-load-infrastructure)
11. [RPC (Request/Response)](#11-rpc-requestresponse)
12. [Backpressure & Rate Limiting](#12-backpressure--rate-limiting)
13. [Health Checks](#13-health-checks)
14. [Distributed Locking](#14-distributed-locking)
15. [Deduplication](#15-deduplication)
16. [Circuit Breaker](#16-circuit-breaker)
17. [Message Signing](#17-message-signing)
18. [Compression](#18-compression)
19. [Result Backends](#19-result-backends)
20. [Stream Queues](#20-stream-queues)
21. [AsyncAPI Documentation](#21-asyncapi-documentation)
22. [Management API](#22-management-api)
23. [Monitoring Dashboard](#23-monitoring-dashboard)
24. [CLI](#24-cli)
25. [Testing](#25-testing)
26. [FastAPI Integration](#26-fastapi-integration)
27. [Environment-Based Configuration](#27-environment-based-configuration)
28. [Running in Kubernetes](#28-running-in-kubernetes)
29. [App Lifecycle](#29-app-lifecycle)
30. [Architecture & Design Patterns](#30-architecture--design-patterns)

---

## 1. Getting Started

### Installation

```bash
# Sync transport (pika-based)
pip install rabbitkit[sync]

# Async transport (aio-pika-based)
pip install rabbitkit[async]

# Both transports
pip install rabbitkit[all-brokers]

# Everything (transports + all optional dependencies)
pip install rabbitkit[all]
```

**Optional extras:**

| Extra | Package | What it enables |
|-------|---------|----------------|
| `sync` | pika | Sync broker/transport |
| `async` | aio-pika | Async broker/transport |
| `redis` | redis | Deduplication, distributed locking, result backends |
| `pydantic` | pydantic | Pydantic model auto-validation |
| `msgspec` | msgspec | High-performance msgspec serialization |
| `compression` | zstandard | zstd compression middleware |
| `fastapi` | fastapi | FastAPI lifespan integration |
| `cli` | typer | CLI tooling (`rabbitkit run/health/topology/shell`) |
| `dashboard` | starlette, uvicorn | Monitoring dashboard |
| `settings` | pydantic-settings | Env-based configuration |
| `management` | aiohttp | RabbitMQ management API client |
| `obskit` | obskit | OpenTelemetry tracing integration |
| `reload` | watchfiles | Hot reload during development |

### Your First Consumer (Sync)

```python
from rabbitkit import RabbitConfig
from rabbitkit.sync import SyncBroker

# Create a broker with default config (localhost:5672, guest/guest)
broker = SyncBroker(RabbitConfig())

# Register a handler for the "orders" queue
@broker.subscriber(queue="orders")
def handle_order(body: bytes) -> None:
    print(f"Received order: {body}")

# Start consuming (blocks until SIGINT/SIGTERM)
broker.run()
```

### Your First Consumer (Async)

```python
import asyncio
from rabbitkit import RabbitConfig
from rabbitkit.async_ import AsyncBroker

broker = AsyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
async def handle_order(body: bytes) -> None:
    print(f"Received order: {body}")

async def main():
    await broker.start()
    # Keep running
    await asyncio.Event().wait()

asyncio.run(main())
```

Or use `RabbitApp` for built-in signal handling:

```python
import asyncio
from rabbitkit import RabbitConfig, RabbitApp
from rabbitkit.async_ import AsyncBroker

broker = AsyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
async def handle_order(body: bytes) -> None:
    print(f"Received: {body}")

app = RabbitApp("order-service")

@app.on_startup
async def start_broker():
    await broker.start()

@app.on_shutdown
async def stop_broker():
    await broker.stop()

# run_async installs SIGINT/SIGTERM handlers and drains gracefully
asyncio.run(app.run_async())
```

### Publishing Messages

```python
# Simple kwargs form (recommended)
await broker.publish(routing_key="orders.created", body={"order_id": 123})
await broker.publish(
    routing_key="orders.created",
    body={"order_id": 123, "total": 99.99},
    exchange="events",
    headers={"x-tenant": "acme"},
)

# Full control via MessageEnvelope
from rabbitkit import MessageEnvelope
await broker.publish(MessageEnvelope(
    routing_key="orders.created",
    body=b'{"order_id": 123}',
    exchange="events",
))
```

For sync:

```python
broker.publish(routing_key="orders.created", body={"order_id": 123})
```

### Connecting to a Remote Broker

```python
from rabbitkit import RabbitConfig, ConnectionConfig

config = RabbitConfig(
    connection=ConnectionConfig(
        host="rabbitmq.prod.internal",
        port=5672,
        username="producer",
        password="s3cr3t",
        vhost="/production",
        heartbeat=30,
    )
)
broker = AsyncBroker(config)
```

Or from a URL:

```python
config = RabbitConfig(
    connection=ConnectionConfig.from_url(
        "amqp://producer:s3cr3t@rabbitmq.prod.internal:5672/production?heartbeat=30"
    )
)
```

---

## 2. Core Concepts

### The Broker

The **broker** is your main entry point. It wires together:

- **Registry** — stores `@subscriber` / `@publisher` route definitions
- **Pipeline** — processes each message through middleware → handler → settlement
- **Transport** — manages the RabbitMQ connection, channels, and I/O

Two implementations:

| Broker | Transport | Import |
|--------|-----------|--------|
| `SyncBroker` | pika (blocking) | `from rabbitkit.sync import SyncBroker` |
| `AsyncBroker` | aio-pika (asyncio) | `from rabbitkit.async_ import AsyncBroker` |

Both share the **same API** (decorators, config, middleware, DI). Choose sync for simple scripts
or threaded apps; async for FastAPI/uvicorn/event-loop apps.

### The Message Flow

```
RabbitMQ → Transport → Pipeline → [Filter] → [Middleware Chain] → [DI Resolve] → Handler → [Result Publish] → Settlement (ack/nack/reject)
```

1. The transport receives a message from RabbitMQ and builds a `RabbitMessage`
2. The pipeline checks the filter (if any)
3. Middleware runs in order (outer → inner) around the handler
4. Parameters are resolved (body deserialization, DI markers)
5. The handler executes
6. If the handler returns a value, it's published (RPC reply or result publisher)
7. The message is settled (ack on success, nack/reject on error)

### Topology

rabbitkit manages RabbitMQ topology (exchanges, queues, bindings) declaratively:

```python
from rabbitkit import RabbitExchange, RabbitQueue, ExchangeType, QueueType

# Define topology inline
@broker.subscriber(
    queue=RabbitQueue(
        name="orders",
        queue_type=QueueType.QUORUM,
        dead_letter_exchange="orders-dlx",
        max_length=10000,
    ),
    exchange=RabbitExchange(
        name="events",
        type=ExchangeType.TOPIC,
        durable=True,
    ),
    routing_key="orders.*",
)
async def handle(body: bytes) -> None:
    ...
```

Or use `TopologyMode.MANUAL` to skip auto-declaration (you manage topology externally).

**Declaring a queue/exchange with different arguments than an existing one
(M6)** — e.g. ops tooling created a quorum queue but your config declares
classic — raises a `ConfigurationError` naming the conflicting queue/exchange
and quoting the broker's own error (AMQP 406 PRECONDITION_FAILED), instead
of an opaque low-level channel-closed traceback. Delete/reconcile the
existing object, adjust your `RabbitQueue`/`RabbitExchange` definition to
match, or use `TopologyMode.PASSIVE_ONLY` to just verify it exists.

---

## 3. Configuration

All config is composed from **frozen, immutable dataclasses** (`@dataclass(frozen=True, slots=True)`).
Use `dataclasses.replace()` to create modified copies — never mutate in place.

### RabbitConfig (top-level)

```python
from rabbitkit import RabbitConfig, ConnectionConfig, ConsumerConfig, PublisherConfig, PoolConfig

config = RabbitConfig(
    connection=ConnectionConfig(host="localhost", port=5672),
    publisher=PublisherConfig(confirm_delivery=True, confirm_timeout=5.0),
    consumer=ConsumerConfig(prefetch_count=50, graceful_timeout=30.0),
    pool=PoolConfig(channel_pool_size=20, channel_acquire_timeout=10.0),
    topology_mode=TopologyMode.AUTO_DECLARE,  # or PASSIVE_ONLY, MANUAL
    retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),
    compression=CompressionConfig(algorithm="zstd", threshold=1024),
    logging=LoggingConfig(render_json=True),
)
```

### ConnectionConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | str | `"localhost"` | RabbitMQ host |
| `port` | int | `5672` | AMQP port |
| `username` | str | `"guest"` | SASL username |
| `password` | str | `"guest"` | SASL password |
| `vhost` | str | `"/"` | Virtual host |
| `heartbeat` | int | `30` | Heartbeat interval (seconds) |
| `socket_timeout` | float | `10.0` | TCP socket timeout |
| `blocked_connection_timeout` | float | `60.0` | Fail-fast on broker alarm (k8s-friendly) |
| `connection_name` | str \| None | `None` | Shows in RabbitMQ management UI |
| `reconnect_backoff_base` | float | `1.0` | Initial reconnect backoff |
| `reconnect_backoff_max` | float | `30.0` | Max reconnect backoff |

### ConsumerConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `prefetch_count` | int | `10` | QoS prefetch (unacked messages) |
| `graceful_timeout` | float | `30.0` | Max seconds to drain on shutdown |

### PublisherConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `exchange` | str | `""` | Default exchange |
| `confirm_delivery` | bool | `True` | Enable publisher confirms |
| `confirm_timeout` | float | `5.0` | Confirm wait timeout |
| `mandatory` | bool | `False` | Mandatory flag (unroutable → return) |
| `persistent` | bool | `True` | Delivery mode 2 (persistent) |

### PoolConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `channel_pool_size` | int | `10` | Publisher channel pool size |
| `channel_acquire_timeout` | float | `10.0` | Wait for a pooled channel |

### RetryConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | int | `4` | Max retry attempts |
| `delays` | tuple[int, ...] | `(5, 30, 120, 600)` | Delay per retry (seconds) |
| `jitter_factor` | float | `0.1` | Jitter (±10% of delay) |
| `strict_delays` | bool | `True` | Raise if `len(delays) < max_retries` |
| `per_queue` | bool | `True` | Per-queue delay queue naming |
| `unknown_policy` | ErrorSeverity | `PERMANENT` | How to classify unknown errors |

### WorkerConfig (passed to `broker.start()`, NOT part of RabbitConfig)

```python
from rabbitkit import WorkerConfig

await broker.start(worker_config=WorkerConfig(
    worker_count=4,          # concurrent handlers
    prefetch_per_worker=10,  # prefetch = worker_count × prefetch_per_worker
    stop_timeout=30.0,       # graceful stop timeout
))
```

---

## 4. Routing

### @subscriber

```python
@broker.subscriber(
    queue="orders",                    # str → auto-creates RabbitQueue(name=str)
    exchange="events",                  # str → auto-creates RabbitExchange(name=str)
    routing_key="orders.created",      # binding routing key
    ack_policy=AckPolicy.AUTO,         # see §5
    middlewares=[MyMiddleware()],      # see §7
    serializer=JSONSerializer(),       # see §9
    retry=RetryConfig(max_retries=3),  # see §8
    tags=frozenset({"billing", "v2"}), # for grouping/filtering
    description="Handle new orders",    # human-readable
    name="order-handler",              # explicit name (auto-generated if None)
    prefetch_count=20,                 # per-route prefetch override
    filter_fn=lambda msg: msg.headers.get("x-tenant") == "acme",  # reject before processing
)
async def handle_order(body: bytes) -> None:
    ...
```

### @publisher (result publishing)

```python
@broker.publisher(exchange="results", routing_key="processed")
@broker.subscriber(queue="orders")
async def handle_order(body: bytes) -> dict:
    # The return value is published to exchange="results", routing_key="processed"
    return {"status": "done", "original": body}
```

**Precedence:** If the incoming message has `reply_to` (RPC), the result goes there
instead. `@publisher` is the fallback destination.

### RabbitRouter (modular routing)

```python
from rabbitkit import RabbitRouter

orders_router = RabbitRouter(
    prefix="orders",
    exchange="orders-exchange",
    middlewares=[LoggingMiddleware()],
    tags=frozenset({"orders"}),
)

@orders_router.subscriber(queue="orders-created", routing_key="created")
async def handle_created(body: bytes) -> None:
    ...

@orders_router.subscriber(queue="orders-returned", routing_key="returned")
async def handle_returned(body: bytes) -> None:
    ...

# Include in the broker (prefix prepended to routing keys)
broker.include_router(orders_router)
```

### RabbitExchange / RabbitQueue (advanced topology)

```python
from rabbitkit import RabbitExchange, RabbitQueue, ExchangeType, QueueType

@broker.subscriber(
    queue=RabbitQueue(
        name="orders",
        queue_type=QueueType.QUORUM,      # or CLASSIC, STREAM
        durable=True,
        dead_letter_exchange="orders-dlx",
        dead_letter_routing_key="orders-dlq",
        message_ttl=60000,                # ms
        max_length=10000,
        max_priority=10,                   # classic only
        delivery_limit=3,                  # quorum only
    ),
    exchange=RabbitExchange(
        name="events",
        type=ExchangeType.TOPIC,
        durable=True,
    ),
    routing_key="orders.*",
)
async def handle(body: bytes) -> None:
    ...
```

---

## 5. Message Processing & Ack Policies

### AckPolicy

```python
from rabbitkit import AckPolicy
```

| Policy | Success | Exception (transient) | Exception (permanent) | When to use |
|--------|---------|----------------------|-----------------------|-------------|
| `AUTO` (default) | ack | nack(requeue=True) | reject(requeue=False) → DLQ | Most cases |
| `MANUAL` | handler owns settlement | handler owns settlement | handler owns settlement | Custom ack logic |
| `NACK_ON_ERROR` | ack | nack(requeue=False) | nack(requeue=False) | Never requeue |
| `ACK_FIRST` | ack before handler | n/a (already acked) | n/a | At-most-once |

### RabbitMessage

The handler receives a `RabbitMessage` (if the parameter is annotated as `RabbitMessage`):

```python
from rabbitkit import RabbitMessage

@broker.subscriber(queue="orders", ack_policy=AckPolicy.MANUAL)
async def handle(msg: RabbitMessage) -> None:
    # Access message fields
    print(msg.body)           # bytes
    print(msg.headers)        # dict[str, Any]
    print(msg.message_id)     # str | None
    print(msg.correlation_id) # str | None
    print(msg.routing_key)    # str
    print(msg.reply_to)       # str | None (RPC)
    print(msg.timestamp)     # datetime | None
    print(msg.redelivered)   # bool
    print(msg.delivery_tag)  # int | None

    # Manual settlement
    msg.ack()                 # async: await msg.ack_async()
    msg.nack(requeue=True)    # async: await msg.nack_async(requeue=True)
    msg.reject(requeue=False) # async: await msg.reject_async(requeue=False)
```

### Exception-based settlement

```python
from rabbitkit import AckMessage, NackMessage, RejectMessage

@broker.subscriber(queue="orders")
async def handle(body: bytes) -> None:
    if not body:
        raise NackMessage(requeue=True)   # requeue for redelivery
    if is_poison(body):
        raise RejectMessage(requeue=False) # send to DLQ
    if should_skip(body):
        raise AckMessage()                 # ack and skip
```

---

## 6. Dependency Injection

rabbitkit supports parameter-level DI via `Annotated` type markers. Works with **zero setup**
(the pipeline auto-detects markers and creates a resolver).

### Depends()

```python
from typing import Annotated
from rabbitkit import Depends

async def get_db_session():
    session = Session()
    try:
        yield session          # generator — session is available
    finally:
        await session.close()  # teardown runs after the handler

@broker.subscriber(queue="orders")
async def handle(
    body: bytes,
    db: Annotated[Session, Depends(get_db_session)],
) -> None:
    order = json.loads(body)
    db.save(order)
```

### Header()

```python
from rabbitkit import Header

@broker.subscriber(queue="orders")
async def handle(
    body: bytes,
    tenant: Annotated[str, Header("x-tenant")],
    trace_id: Annotated[str, Header("x-trace-id")],
) -> None:
    print(f"Tenant: {tenant}, Trace: {trace_id}")
```

### Path() (topic wildcard extraction)

```python
from rabbitkit import Path

@broker.subscriber(queue="events", routing_key="events.{level}.#")
async def handle(
    body: bytes,
    level: Annotated[str, Path("level")],  # extracts the {level} segment
) -> None:
    print(f"Level: {level}")
```

### Context()

```python
from rabbitkit import Context, ContextRepo

# Set up context (e.g. in a startup hook)
context_repo = ContextRepo()
context_repo.set_global("app_name", "order-service")

@broker.subscriber(queue="orders")
async def handle(
    body: bytes,
    app_name: Annotated[str, Context("app_name")],
) -> None:
    print(f"Processing in {app_name}")
```

> **Note:** `ContextRepo` uses `contextvars.ContextVar` (not `threading.local`) for correct
> isolation across async coroutines. Each in-flight message gets its own context snapshot.

**Optional values (H10):** `Header()`/`Path()`/`Context()` are required by
default -- a missing value raises `MissingDependencyError` (PERMANENT,
straight to the DLQ). Make one optional with `default=` on the marker
(`Header("x-tenant", default="anonymous")`) or a Python default on the
parameter (`Annotated[str | None, Header("x-tenant")] = None`) -- the marker's
own default wins if both are given.

---

## 7. Middleware

Middleware wraps the handler call. Middlewares are applied **outer → inner** (first in the
list is outermost). The chain is **cached per route** for performance.

### Built-in middleware

| Middleware | Purpose |
|------------|---------|
| `ExceptionMiddleware` | Catches and logs handler exceptions |
| `RetryMiddleware` | Transient error → delay queue + redelivery |
| `CompressionMiddleware` | Compress/decompress bodies (gzip/zstd) |
| `MetricsMiddleware` | Prometheus counters/histograms |
| `TracedConsumerMiddleware` | OpenTelemetry tracing (obskit) |
| `DeduplicationMiddleware` | Redis-based idempotent processing |
| `CircuitBreakerMiddleware` | Fail-fast on cascading failures |
| `RateLimitMiddleware` | Token-bucket rate limiting |
| `SigningMiddleware` | HMAC signing + replay protection |
| `TimeoutMiddleware` | Handler execution timeout |
| `ResultMiddleware` | Store results in a backend (Redis) |
| `LockMiddleware` | Distributed locking for single-consumer |

**`MetricsMiddleware` metrics (M2/M3):** emits
`messages_consumed_total`/`message_processing_seconds` (handler success/error
+ duration) and `messages_published_total`/`message_publish_seconds`
directly from its own `consume_scope`/`publish_scope`. It ALSO emits
`messages_acked_total`/`messages_nacked_total`/`messages_rejected_total`
(via a hook `HandlerPipeline` calls once a message's disposition is final)
and, when a `RetryMiddleware` is also on the route,
`messages_retried_total`/`messages_dead_lettered_total`. The `queue` label
on every consume-side metric is the BOUND queue name, not the raw routing
key — a topic/`Path()` routing key can embed an unbounded per-message value
(tenant id, order id, etc.), which would otherwise blow up your metrics
backend's cardinality.

### Using middleware

```python
from rabbitkit import RetryConfig
from rabbitkit.middleware import RetryMiddleware, CompressionMiddleware

# Global (applied to all routes via broker config)
config = RabbitConfig(retry=RetryConfig(max_retries=3, delays=(5, 30)))

# Per-route
@broker.subscriber(
    queue="orders",
    middlewares=[
        CompressionMiddleware(CompressionConfig(algorithm="zstd")),
        RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30))),
    ],
)
async def handle(body: bytes) -> None:
    ...
```

### Custom middleware

```python
from rabbitkit.middleware import BaseMiddleware
from rabbitkit import RabbitMessage, MessageEnvelope

class LoggingMiddleware(BaseMiddleware):
    def on_receive(self, message: RabbitMessage) -> None:
        # Called before the handler (side-effect hook)
        print(f"Received: {message.message_id}")

    def consume_scope(self, call_next, message):
        # Wraps the handler call (sync)
        print(f"Before: {message.routing_key}")
        result = call_next(message)
        print(f"After: {message.routing_key}")
        return result

    async def consume_scope_async(self, call_next, message):
        # Wraps the handler call (async)
        print(f"Before: {message.routing_key}")
        result = await call_next(message)
        print(f"After: {message.routing_key}")
        return result

    def publish_scope(self, call_next, envelope):
        # Wraps result publishing (sync)
        print(f"Publishing: {envelope.routing_key}")
        return call_next(envelope)

    async def publish_scope_async(self, call_next, envelope):
        print(f"Publishing: {envelope.routing_key}")
        return await call_next(envelope)
```

### Middleware ordering

```
on_receive (side effects, no wrapping) → consume_scope (outer→inner chain) → handler
```

The first middleware in the list is **outermost** (runs first on the way in, last on the
way out). The last middleware is **innermost** (closest to the handler).

---

## 8. Retry & Error Handling

### RetryConfig

```python
from rabbitkit import RetryConfig, RetryDisabled, RETRY_DISABLED

# Broker-wide default
config = RabbitConfig(retry=RetryConfig(max_retries=3, delays=(5, 30, 120)))

# Per-route override
@broker.subscriber(queue="critical", retry=RetryConfig(max_retries=5, delays=(1, 5, 30, 120, 600)))
async def handle(body: bytes) -> None:
    ...

# Disable retry for a route
@broker.subscriber(queue="ephemeral", retry=RETRY_DISABLED)
async def handle(body: bytes) -> None:
    ...
```

### How retry works

1. A transient error (network, connection) is classified as transient
2. The message is published to a **delay queue** with a per-retry TTL
3. After the TTL expires, RabbitMQ routes it back to the source queue
4. The handler is called again (up to `max_retries`)
5. After exhaustion, the message is rejected → DLQ

Setting `retry=` (broker-wide or per-route) does two things at once: it
declares the delay/DLQ topology **and** installs `RetryMiddleware` on the
route automatically, as the outermost middleware. You don't construct
`RetryMiddleware` yourself for the common case — only for advanced needs
(custom error `predicates`), and even then the broker detects an
already-present `RetryMiddleware` in `middlewares=[...]` and doesn't add a
second one.

With `per_queue=True` (default), each source queue gets isolated delay
infrastructure:

```
orders-queue
  -> orders-queue.retry.0   (TTL=5s,   DLX back to orders exchange)
  -> orders-queue.retry.1   (TTL=30s,  DLX back to orders exchange)
  -> orders-queue.retry.2   (TTL=120s, DLX back to orders exchange)
  -> orders-queue.dlq       (terminal failures)
```

**The retry-count header is not trusted input.** `x-rabbitkit-retry-count` is
read from the inbound message and clamped to `[0, max_retries]` regardless of
what a producer sets it to — a spoofed negative value can't reset the
counter (or produce a non-existent negative delay-queue routing key), and a
spoofed huge value can't skip straight to the DLQ beyond the configured cap.
For a broker-enforced backstop that's completely independent of this
app-level header, use a **quorum queue** with `x-delivery-limit` (see
`RabbitQueue(queue_type=QueueType.QUORUM, delivery_limit=...)` in the
Topology section) — RabbitMQ itself dead-letters after the limit regardless
of any application-level retry logic.

### Error classification

```python
from rabbitkit.core.errors import classify_error, ErrorSeverity

# Built-in transient errors: TimeoutError, EOFError, OSError (and subclasses)
# Built-in permanent errors: JSONDecodeError, KeyError, ValueError, TypeError, ...

# Custom classification
from rabbitkit.core.errors import ErrorPredicate

def is_retryable(exc: BaseException) -> bool | None:
    if isinstance(exc, MyExternalServiceError):
        return True   # transient
    if isinstance(exc, SchemaValidationError):
        return False  # permanent
    return None       # no opinion → use default policy

retry_config = RetryConfig(
    max_retries=3,
    delays=(5, 30, 120),
    unknown_policy=ErrorSeverity.PERMANENT,  # default for unknown errors
)
```

---

## 9. Serialization

### Built-in serializers

```python
from rabbitkit import JSONSerializer, SerializationPipeline, JsonParser, PydanticDecoder

# Default JSON (built-in, no extras)
serializer = JSONSerializer()  # strict: raises on non-serializable objects
serializer = JSONSerializer(coerce_unknown_to_str=True)  # legacy: str() fallback
serializer = JSONSerializer(max_parse_bytes=10_000_000)  # cap input size

# msgspec (pip install rabbitkit[msgspec])
from rabbitkit.serialization import MsgspecSerializer
serializer = MsgspecSerializer()  # high-performance, caches decoders per type

# Two-stage pipeline (parse → decode)
pipeline = SerializationPipeline(
    parser=JsonParser(),           # bytes → dict
    decoder=PydanticDecoder(),     # dict → pydantic model
)
```

### Using a serializer

```python
# Global (all routes)
broker = AsyncBroker(config, serializer=JSONSerializer())

# Per-route
@broker.subscriber(queue="orders", serializer=MsgspecSerializer())
async def handle(body: bytes) -> None:
    ...
```

### Pydantic auto-validation

If the handler's body parameter is a Pydantic model, the pipeline automatically
validates the deserialized body:

```python
from pydantic import BaseModel

class Order(BaseModel):
    order_id: int
    total: float
    items: list[str]

@broker.subscriber(queue="orders")
async def handle(order: Order) -> None:
    # `order` is a validated Order instance — model_validate was called
    print(order.order_id)
```

### Custom serialization

```python
from rabbitkit.serialization import MessageParser, MessageDecoder

class ProtobufParser(MessageParser):
    def parse(self, data: bytes) -> Any:
        return MyProto_pb2.MyMessage().ParseFromString(data)

    def serialize(self, obj: Any) -> bytes:
        return obj.SerializeToString()

    @property
    def content_type(self) -> str:
        return "application/protobuf"
```

---

## 10. High-Load Infrastructure

### Worker Pools

```python
from rabbitkit import WorkerConfig

# Multiple concurrent handlers (sync: daemon thread pool, async: semaphore)
await broker.start(worker_config=WorkerConfig(worker_count=4, prefetch_per_worker=10))
```

**Sync workers** use a daemon-thread pool (the process can exit even if a handler is stuck).
**Async workers** use an `asyncio.Semaphore` to limit concurrent handler tasks.

### FlowController (backpressure)

```python
from rabbitkit import FlowController, BackpressureConfig

fc = FlowController(BackpressureConfig(
    max_in_flight=1000,       # max concurrent in-flight publishes
    rate_limit=100,            # max publishes per second (None = no limit)
    blocked_timeout=60.0,     # max seconds to wait when broker is blocked
    on_blocked="wait",         # "wait" | "raise" | "drop"
))

# Wire to the transport (transports call on_blocked/on_unblocked on broker alarms)
broker.flow_controller = fc

# Use in publish path
if fc.acquire():
    try:
        await broker.publish(envelope)
    finally:
        fc.release()
```

### BatchPublisher

```python
from rabbitkit import BatchPublisher, BatchPublishConfig

bp = BatchPublisher(
    publish_fn=broker.publish,
    config=BatchPublishConfig(batch_size=100, flush_interval_ms=50),
)

# Add messages (auto-flushes on batch_size or interval)
bp.add(MessageEnvelope(routing_key="events", body=b"..."))
bp.add(MessageEnvelope(routing_key="events", body=b"..."))
# ...
bp.close()  # flush remaining + stop timer
```

### BatchAcker

```python
from rabbitkit import BatchAcker, BatchAckConfig

# ack_fn must NOT be a raw channel.basic_ack: the flush_interval_ms timer
# fires from a background threading.Timer thread, not pika's I/O thread,
# and pika channel methods are not thread-safe. Marshal onto the I/O
# thread via connection.add_callback_threadsafe instead.
def safe_ack(tag: int, multiple: bool = False) -> None:
    connection.add_callback_threadsafe(
        lambda: channel.basic_ack(delivery_tag=tag, multiple=multiple)
    )

acker = BatchAcker(
    ack_fn=safe_ack,
    config=BatchAckConfig(batch_size=100, flush_interval_ms=200),
)

# Add delivery tags (auto-acks in batches via multiple=True)
acker.add(delivery_tag)
acker.add(delivery_tag)
# ...
acker.close()
```

> **Important:** Do NOT mix sync and async APIs on the same batch instance.
> Sync uses `threading.Lock` + `threading.Timer`; async uses `asyncio.Lock` + `asyncio.Task`.

---

## 11. RPC (Request/Response)

### Using broker.request() (shorthand)

```python
# Async
response = await broker.request(
    routing_key="compute-queue",
    body=b'{"x": 1, "y": 2}',
    timeout=5.0,
    exchange="",
    headers={"x-trace-id": "abc123"},
)
print(response.body)  # bytes

# Sync
response = broker.request(
    routing_key="compute-queue",
    body=b'{"x": 1, "y": 2}',
    timeout=5.0,
)
```

### RPCClient / AsyncRPCClient (direct)

```python
from rabbitkit.experimental import RPCClient, AsyncRPCClient

# Sync
client = RPCClient(transport, max_reply_bytes=1_000_000)
response = client.call("compute-queue", b'{"x":1}', timeout=5.0)
client.close()

# Async
client = AsyncRPCClient(transport, max_reply_bytes=1_000_000)
response = await client.call("compute-queue", b'{"x":1}', timeout=5.0)
await client.close()
```

### RPC handler (server side)

```python
@broker.subscriber(queue="compute-queue")
async def compute(body: bytes) -> dict:
    data = json.loads(body)
    return {"result": data["x"] + data["y"]}
    # The pipeline sees reply_to in the incoming message and routes the return value there
```

---

## 12. Backpressure & Rate Limiting

### RateLimitMiddleware

```python
from rabbitkit import RateLimitConfig, RateLimitMiddleware

@broker.subscriber(
    queue="api-events",
    middlewares=[RateLimitMiddleware(RateLimitConfig(
        max_rate=100,        # 100 messages per second
        on_limited="wait",   # "wait" | "nack" | "drop"
    ))],
)
async def handle(body: bytes) -> None:
    ...
```

### FlowController policies

| `on_blocked` | Behavior |
|--------------|----------|
| `"wait"` | Block the publish until the broker unblocks or `blocked_timeout` expires |
| `"raise"` | Raise `BackpressureError` immediately |
| `"drop"` | Return `False` (message dropped) |

---

## 13. Health Checks

### For Kubernetes probes

```python
from rabbitkit import broker_liveness, broker_readiness, broker_health_check, HealthStatus

# Liveness — should k8s restart this pod?
# Returns True when the process is alive and not wedged.
# Does NOT fail on transient broker disconnect (so a broker restart doesn't
# cause cascading pod restarts).
live = broker_liveness(broker, wedged_timeout=60.0)

# Readiness — should k8s route traffic to this pod?
# Returns True when connected, consumers are active, and consumer_count == route_count.
ready = broker_readiness(broker)

# Full tri-state check
result = broker_health_check(broker)
# result.status: HEALTHY | DEGRADED | UNHEALTHY
# result.connected: bool
# result.blocked: bool           -- connection.blocked (broker memory/disk alarm);
#                                    orthogonal to connected -- a blocked connection
#                                    can't publish even though it's still "connected".
#                                    Forces DEGRADED and makes broker_readiness() False.
# result.consumer_count: int
# result.route_count: int
# result.worker_pool_pending: int
```

### Prometheus metrics

```python
from rabbitkit import metrics_app, start_metrics_server

# Option 1: ASGI app (mount in your existing server)
app = metrics_app()
# uvicorn or starlette mounts it at /metrics

# Option 2: Background HTTP server
start_metrics_server(port=9090)  # defaults to 127.0.0.1; pass host="0.0.0.0" for k8s
```

---

## 14. Distributed Locking

```python
from rabbitkit.experimental import RedisLock, LockMiddleware

# Redis-backed distributed lock
lock = RedisLock(redis_client, ttl=30)

# Acquire/release
if lock.acquire("order-123", timeout=10.0):
    try:
        process_order("123")
    finally:
        lock.release("order-123")

# Fencing token (for downstream monotonic-write protection)
token = lock.fencing_token("order-123")

# As middleware (auto-lock per message)
@broker.subscriber(
    queue="orders",
    middlewares=[LockMiddleware(lock, key_fn=lambda msg: f"order-{msg.message_id}")],
)
async def handle(body: bytes) -> None:
    # Only one consumer processes this message at a time
    ...
```

> **`ttl` has no auto-renewal.** A handler that runs longer than `ttl` loses
> the lock mid-work — a second consumer can then acquire the same key and
> process concurrently, defeating the lock entirely. Release itself is
> correctly atomic (a Lua compare-and-delete, so a stale holder can never
> delete someone else's lock) — the gap is purely "did the handler outlive
> the TTL." Set `ttl` comfortably above your worst-case handler time. For a
> downstream write that must not be applied twice even if the lock is lost
> mid-work, use `lock.fencing_token(key)` and have the downstream store
> reject a token older than the one it already recorded — the lock alone is
> not a sufficient correctness guarantee for that case.

If the lock cannot be acquired (another instance holds it), `LockMiddleware`
nacks the message with `requeue=True` so it's retried later rather than
dropped.

---

## 15. Deduplication

```python
from rabbitkit import DeduplicationMiddleware, DeduplicationConfig

@broker.subscriber(
    queue="idempotent-events",
    middlewares=[DeduplicationMiddleware(
        redis_client,
        DeduplicationConfig(
            key_prefix="rabbitkit:dedup",
            ttl=86400,                        # 24h dedup window
            key_source="message_id",           # or "correlation_id"
            fallback_on_redis_error=True,      # process on Redis outage (at-least-once)
            mark_policy="on_success",          # or "on_start"
        ),
    )],
)
async def handle(body: bytes) -> None:
    # Called at most once per message_id within the TTL window
    ...
```

### mark_policy

| Value | When the dedup key is stored | Trade-off |
|---|---|---|
| `"on_success"` (default) | After the handler returns successfully | Safer for retries — a failed handler is retried. Two concurrent deliveries may both run. |
| `"on_start"` | Before the handler runs | Prevents concurrent duplicate execution. A failed handler will not be retried for the same message_id. |

Prefer `"on_success"` for most workflows. Use `"on_start"` only when concurrent dual-delivery is a larger risk than missed retries.

**Composing with `RetryMiddleware`:** `RetryMiddleware` swallows a transient
handler failure (routes it to a delay queue, acks the source) rather than
raising — so from an outer middleware's point of view, `call_next(message)`
returns normally either way, indistinguishable from the handler succeeding.
`DeduplicationMiddleware` checks for a sentinel `RetryMiddleware` returns
instead of `None` in this case, and skips marking the message as processed
(or, for `mark_policy="on_start"`, retroactively undoes the premature mark)
— so the actual retry redelivery is processed instead of being silently
dropped as a "duplicate," regardless of which of the two you list first.
Any custom middleware with similar "mark as done" side effects wrapping a
route that may contain a `RetryMiddleware` should check for the same
sentinel.

---

## 16. Circuit Breaker

> **Advanced Stable, not dependency-free.** `CircuitBreakerMiddleware` is a
> no-op passthrough unless you give it a real circuit breaker implementation
> — it does not ship its own breaker logic. In practice that means
> `pip install rabbitkit[obskit]` (or any object satisfying
> `CircuitBreakerProtocol`/`AsyncCircuitBreakerProtocol` yourself). Don't
> add this middleware expecting circuit-breaking "for free."

```python
from rabbitkit import CircuitBreakerMiddleware
from rabbitkit.middleware.circuit_breaker import CircuitBreakerProtocol

# Using a provided circuit breaker implementation (e.g. obskit.resilience.CircuitBreaker)
breaker = MyCircuitBreaker(failure_threshold=5, recovery_timeout=30)
@broker.subscriber(
    queue="external-api-calls",
    middlewares=[CircuitBreakerMiddleware(breaker)],
)
async def handle(body: bytes) -> None:
    # When the circuit is open, messages are rejected immediately (fail-fast)
    ...
```

An async handler requires `async_circuit_breaker=` — passing only a sync
`circuit_breaker=` and using it with an async handler raises `TypeError` at
call time rather than silently no-op-ing.

---

## 17. Message Signing

```python
from rabbitkit.experimental import SigningMiddleware, SigningConfig

@broker.subscriber(
    queue="secure-events",
    middlewares=[SigningMiddleware(SigningConfig(
        secret_key="my-secret-hmac-key",
        algorithm="hmac-sha256",         # "hmac-sha256" or "hmac-sha512"
        header_name="x-rabbitkit-signature",
        max_skew=60,                     # max timestamp skew (seconds) -- also the nonce replay window
        require_freshness=True,          # reject messages lacking timestamp/nonce headers
    ))],
)
async def handle(body: bytes) -> None:
    # Messages are verified for HMAC integrity + freshness (timestamp + nonce)
    # Replayed messages are rejected (nonce cache prevents duplicates)
    ...
```

**Replay protection:** the default `TTLSetNonceCache` is a bounded, in-memory
seen-set. At capacity, it reclaims genuinely-*expired* entries first; if it's
still full after that (i.e. genuinely full of *live*, unexpired nonces), the
**new** nonce is rejected rather than evicting a live one to make room —
evicting a live nonce would let an attacker flood unique nonces to force out
a target's still-valid one and then replay it. **This cache is per-process**
— in any multi-process/multi-pod deployment, use
`nonce_cache=RedisNonceCache(redis.Redis(...))`
(`from rabbitkit.middleware.signing import RedisNonceCache`) so the seen-set
is shared across every process; a `RuntimeWarning` is emitted if you don't.

**What's covered:** the default signature covers `exchange`, `routing_key`,
`content_encoding`, and `reply_to` in addition to `timestamp`/`nonce`/`body` —
not just the body. A captured message re-published under a different routing
key, an RPC reply redirected via `reply_to`, or `content_encoding` flipped to
hit a different decompression path all fail verification. Other headers are
not covered — don't use freeform headers for security-critical routing.

---

## 18. Compression

```python
from rabbitkit import CompressionMiddleware, CompressionConfig

@broker.subscriber(
    queue="large-payloads",
    middlewares=[CompressionMiddleware(CompressionConfig(
        algorithm="zstd",            # "gzip" (built-in) or "zstd" (requires rabbitkit[compression])
        threshold=1024,               # only compress bodies > 1KB
        level=6,                      # compression level (1-19 for zstd)
    ))],
    serializer=JSONSerializer(),      # compression wraps the serializer
)
async def handle(body: bytes) -> None:
    # body is automatically decompressed before deserialization
    ...
```

**Security:** The decompression path uses **streaming** decompression with a running
byte counter that aborts at `max_decompressed_size` (default 64MB) — prevents zip-bomb
OOM attacks.

---

## 19. Result Backends

```python
from rabbitkit.experimental import ResultMiddleware
from rabbitkit.results import RedisResultBackend

backend = RedisResultBackend(redis_client, ttl=3600)

@broker.subscriber(
    queue="compute-jobs",
    middlewares=[ResultMiddleware(backend, ttl=3600)],
)
async def compute(body: bytes) -> dict:
    return {"result": 42}
    # The result is stored in Redis; callers can retrieve it later by correlation_id

# Retrieve a result
result = await backend.fetch(correlation_id)
```

---

## 20. Stream Queues

```python
from rabbitkit.experimental import StreamOffset, StreamOffsetType, StreamConsumerConfig

@broker.subscriber(
    queue=RabbitQueue(name="telemetry", queue_type=QueueType.STREAM),
)
async def handle(body: bytes) -> None:
    ...

# With stream offset configuration
config = StreamConsumerConfig(
    offset=StreamOffset(
        type=StreamOffsetType.FIRST,  # FIRST | LAST | NEXT | TIMESTAMP | OFFSET
        value=None,                    # for TIMESTAMP: unix ms; for OFFSET: int
    ),
    consumer_name="consumer-1",        # for single-active-consumer
)
```

---

## 21. AsyncAPI Documentation

```python
from rabbitkit import generate_asyncapi_doc, generate_asyncapi_json

# Generate AsyncAPI 2.6.0 spec from your broker's routes
doc = generate_asyncapi_doc(broker)    # dict
json_str = generate_asyncapi_json(broker)  # JSON string

# Save to file
with open("asyncapi.json", "w") as f:
    f.write(json_str)

# View in AsyncAPI Studio: https://studio.asyncapi.com
```

---

## 22. Management API

```python
from rabbitkit import RabbitManagementClient, ManagementConfig

client = RabbitManagementClient(ManagementConfig(
    url="http://localhost:15672",
    username="guest",
    password="guest",
    timeout=10.0,
))

# Queue operations
queues = client.list_queues(vhost="/")
queue = client.get_queue("orders", vhost="/")
client.purge_queue("orders")

# Exchange operations
exchanges = client.list_exchanges()

# Connection operations
connections = client.list_connections()

# Health
is_healthy = client.health_check()  # bool

# Async variants
queues = await client.list_queues_async(vhost="/")
```

---

## 23. Monitoring Dashboard

```python
from rabbitkit.experimental import create_dashboard_app

# Create a Starlette ASGI dashboard app
app = create_dashboard_app(
    broker=broker,
    auth_token="my-secret-token",  # optional: Bearer token auth
)

# Run with uvicorn
# uvicorn mymodule:app --port 8080
```

The dashboard provides:
- `/` — overview (route count, consumer count, health status)
- `/api/routes` — all registered routes with details
- `/api/health` — broker health check result

---

## 24. CLI

Install the CLI:

```bash
pip install rabbitkit[cli]
```

### Running a consumer

```bash
rabbitkit run myapp.main:broker --worker-count 4
rabbitkit run myapp.main:broker --reload          # hot-reload on code changes
```

### Health probes (Kubernetes)

```bash
rabbitkit health liveness myapp.main:broker   # exit 0 even while reconnecting
rabbitkit health readiness myapp.main:broker  # exit 1 when disconnected/consumers inactive
```

### Topology management

```bash
# List registered routes/queues/exchanges
rabbitkit topology list myapp.main:broker

# Compare declared vs live (exits 1 on mismatch)
rabbitkit topology validate myapp.main:broker --url http://guest:guest@localhost:15672

# Show diff (+/~/! lines)
rabbitkit topology diff myapp.main:broker
rabbitkit topology diff myapp.main:broker --format json

# Declare all registered resources in RabbitMQ
rabbitkit topology apply myapp.main:broker
rabbitkit topology apply myapp.main:broker --dry-run
```

### DLQ management

```bash
rabbitkit dlq inspect orders.created.dlq
rabbitkit dlq inspect orders.created.dlq --full --limit 50

rabbitkit dlq replay orders.created.dlq orders          # replay to exchange "orders"
rabbitkit dlq replay orders.created.dlq orders --limit 10
```

### Route inspection

```bash
rabbitkit routes list myapp.main:broker
rabbitkit routes describe myapp.main:broker orders.created
```

### Interactive shell

```bash
rabbitkit shell myapp.main:broker   # IPython shell with broker pre-loaded
```

---

## 25. Testing

### TestBroker (in-memory, no RabbitMQ needed)

```python
from rabbitkit.testing import TestBroker

broker = TestBroker()

@broker.subscriber(queue="orders")
def handle(body: bytes) -> None:
    handle.called = True

broker.start()

# Publish a message (in-memory — exercises the real pipeline)
broker.publish(MessageEnvelope(routing_key="orders", body=b"test"))

# Assert settlement
broker.assert_acked(broker.consumed_messages[0])

# Inject a failed publish (tests the nack-on-publish-failure path)
broker.fail_next_publish()

# Custom publish outcome
broker.publish_outcome = PublishOutcome(status=PublishStatus.NACKED)
```

### TestApp

```python
from rabbitkit.testing import TestApp

app = TestApp()

@app.on_startup
def init():
    print("Starting")

@app.on_shutdown
def cleanup():
    print("Stopping")

with app.test_with_lifecycle():
    # Startup hooks have run; app.state == AppState.RUNNING
    ...
# Shutdown hooks have run; app.state == AppState.STOPPED
```

---

## 26. FastAPI Integration

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from rabbitkit import AsyncBroker, RabbitConfig, rabbitkit_lifespan

broker = AsyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
async def handle(body: bytes) -> None:
    ...

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with rabbitkit_lifespan(broker):
        yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "ok"}
```

---

## 27. Environment-Based Configuration

```python
from rabbitkit.core.env_config import RabbitSettings

# Load from environment variables (RABBITMQ_* prefix)
settings = RabbitSettings()
config = settings.to_rabbit_config()

# Environment variables:
# RABBITMQ_HOST=localhost
# RABBITMQ_PORT=5672
# RABBITMQ_USERNAME=guest
# RABBITMQ_PASSWORD=guest
# RABBITMQ_VHOST=/
# RABBITMQ_HEARTBEAT=30
# RABBITMQ_PREFETCH_COUNT=10
# RABBITMQ_BLOCKED_CONNECTION_TIMEOUT=60
# RABBITMQ_CONFIRM_TIMEOUT=5.0
# RABBITMQ_CONNECTION_NAME=my-service
```

---

## 28. Running in Kubernetes

### Probes

```yaml
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 60   # must exceed graceful_timeout + drain margin
      containers:
      - name: consumer
        env:
        - name: RABBITMQ_HOST
          valueFrom: { configMapKeyRef: { name: rabbit-cfg, key: host } }
        - name: RABBITMQ_USERNAME
          valueFrom: { secretKeyRef: { name: rabbit-secret, key: username } }
        - name: RABBITMQ_PASSWORD
          valueFrom: { secretKeyRef: { name: rabbit-secret, key: password } }
        - name: RABBITMQ_BLOCKED_CONNECTION_TIMEOUT
          value: "60"
        - name: RABBITMQ_CONNECTION_NAME
          value: "order-service@$(POD_NAME)"
        startupProbe:
          exec: { command: ["/app/bin/health", "liveness"] }
          failureThreshold: 15
          periodSeconds: 10
        livenessProbe:           # restart only on a wedged process, NOT broker disconnect
          exec: { command: ["/app/bin/health", "liveness"] }
          periodSeconds: 10
          failureThreshold: 6    # 60s — tolerate a broker restart
        readinessProbe:          # route traffic only when broker + consumers are active
          exec: { command: ["/app/bin/health", "readiness"] }
          periodSeconds: 5
          failureThreshold: 2
        lifecycle:
          preStop:
            exec:
              command: ["sleep", "10"]   # let endpoint controller deregister
```

### PodDisruptionBudget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: order-consumer
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: order-consumer
```

### Key points

- **Liveness** uses `broker_liveness(broker)` — fails only on a wedged process, NOT on
  transient broker disconnect (prevents cascading restarts during broker maintenance).
- **Readiness** uses `broker_readiness(broker)` — fails on disconnect, missing consumers,
  or consumer_count != route_count (removes the pod from the load balancer).
- **`terminationGracePeriodSeconds`** must exceed `ConsumerConfig.graceful_timeout` + a
  drain margin (default 60s = 30s graceful_timeout + 30s headroom).
- **`preStop: sleep 10`** lets the k8s endpoint controller deregister the pod before SIGTERM.
- Sync consumers handle SIGTERM via a daemon thread (the `broker.run()` entrypoint
  installs a signal-safe handler that drains in-flight work).
- Async `broker.start()` installs SIGTERM handlers by default (pass
  `install_signal_handlers=False` when driven by `RabbitApp.run_async()`).
  `start()` alone is fire-and-forget on signal -- use `asyncio.run(broker.run())`
  when the async broker is used directly (not via `RabbitApp`), so the drain
  is joined before the process exits (H11).

---

## 29. App Lifecycle

`RabbitApp` manages startup/shutdown hooks and signal handling:

```python
from rabbitkit import RabbitApp

app = RabbitApp("order-service", startup_timeout=120.0)

@app.on_startup
async def connect_db():
    app.db = await connect_database()

@app.after_startup
async def log_ready():
    logger.info("Service started")

@app.on_shutdown
async def close_db():
    await app.db.close()

@app.after_shutdown
async def log_stopped():
    logger.info("Service stopped")

# Run with signal handling (SIGINT/SIGTERM → graceful shutdown)
asyncio.run(app.run_async())
```

**Lifecycle:**
```
IDLE → STARTING → RUNNING → STOPPING → STOPPED
```

- **Idempotent:** `start()` twice is a no-op (guarded by a lock)
- **Startup failure rollback:** if a startup hook raises, shutdown hooks still run
- **Bounded:** `startup_timeout` bounds each hook (default 120s)
- **Signal-portable:** `loop.add_signal_handler` on Linux; falls back to `signal.signal`
  on Windows / non-main-thread

---

## 30. Architecture & Design Patterns

### Design patterns used

| Pattern | Where | Purpose |
|---------|-------|---------|
| **Strategy** | `AckPolicy` → `AckStrategy` implementations | Pluggable ack policies (AUTO, MANUAL, NACK_ON_ERROR, ACK_FIRST) |
| **Strategy** | `FlowController` → `_BlockedPolicy` (Wait/Raise/Drop) | Pluggable backpressure policies |
| **Chain of Responsibility** | Middleware pipeline (`consume_scope`/`publish_scope`) | Ordered middleware wrapping the handler |
| **Adapter** | `SyncTransport` / `AsyncTransportImpl` | Adapt pika/aio-pika to the transport protocol |
| **Protocol** | `core/protocols.py` (`Transport`, `HealthProvider`, `MetricsCollector`) | Structural typing, zero transport imports in core |
| **Factory** | `ConnectionConfig.from_url()`, `RabbitSettings.to_rabbit_config()` | Build config from URLs / env vars |
| **Template Method** | `BaseMiddleware` (no-op defaults) | Subclasses override only what they need |
| **Observer** | `RabbitApp` lifecycle hooks, `on_blocked`/`on_unblocked` callbacks | Decoupled event notification |
| **Token Bucket** | `RateLimitMiddleware`, `FlowController._TokenBucket` | Rate limiting with lazy refill |
| **Circuit Breaker** | `CircuitBreakerMiddleware` → `CircuitBreakerProtocol` | Fail-fast on cascading failures |
| **Object Pool** | `AsyncChannelPool` / `SyncChannelPool` | Reuse channels; `@asynccontextmanager` acquire prevents leaks |
| **Null Object** | `NoOpMiddleware` | Zero-overhead pass-through (eliminates `if x is None` branches) |
| **Producer-Consumer** | `_DaemonWorkerPool` (queue.Queue + daemon threads) | Multi-worker sync processing |
| **Fencing Token** | `RedisLock.fencing_token()` | Monotonic-write protection for distributed locks |
| **Test Double (Spy)** | `TestBroker` with `SettlementRecord` | In-memory testing with real pipeline + real settlement |
| **Reply Router** | `_ReplyRouter` + `concurrent.futures.Future` | Shared RPC reply matching (sync+async) |
| **Topology Dispatcher** | `TopologyDispatcher` + `TopoAction` enum | Single place for TopologyMode dispatch (shared by both transports) |

### Advanced Python features

- `from __future__ import annotations` + string-annotation handling (`is_rabbit_message_annotation`)
- `@dataclass(frozen=True, slots=True)` for all config and topology models
- `contextvars.ContextVar` (not `threading.local`) for correct async context isolation
- `asyncio.timeout` (3.11+) for bounded startup hooks
- `asyncio.to_thread` for offloading zstd decompression off the event loop
- `threading.local` for per-thread zstd contexts (zstandard contexts are not thread-safe)
- `concurrent.futures.Future` for sync RPC pending calls
- `asyncio.Future` for async RPC pending calls
- `OrderedDict`-backed nonce cache — reclaims *expired* entries first; rejects
  new nonces rather than evicting a still-live one when genuinely full (see
  the Message Signing section for why evicting live entries would be exploitable)
- `weakref`-aware pool tracking (`_in_use` set for leak detection)
- `@asynccontextmanager` / `@contextmanager` for pool acquire (prevents leaks)
- `__aenter__`/`__aexit__` on transports for idiomatic `async with transport:`
- `TypedDict` for management API responses (`QueueInfo`)
- `Protocol` with `@runtime_checkable` for `HealthProvider`, `Serializer`, `Transport`
- `hmac.compare_digest` for constant-time HMAC comparison
- `zlib.decompressobj` streaming decompression with `unconsumed_tail` for zip-bomb resistance
- `ssl.SSLContext` with `minimum_version=TLSv1_2` + `load_default_certs()` for TLS hardening
- `functools`-style caching for per-handler reflection (`inspect.signature` + `typing.get_type_hints`)
- Daemon threads (`threading.Thread(daemon=True)`) for worker pool (process can exit if a handler is stuck)

### Architecture invariants

- **`core/` has ZERO transport imports** — no pika, no aio-pika in `core/`. Transports adapt
  to the `Transport` protocol defined in `core/protocols.py`.
- **`types.py` is the SINGLE canonical location for all enums** (`AppState`, `ExchangeType`,
  `QueueType`, `AckPolicy`, `TopologyMode`, `ErrorSeverity`, `PublishStatus`, `AckStrategy`).
- **Config dataclasses are frozen+slots** — use `dataclasses.replace()` for modifications.
- **`WorkerConfig` is NOT part of `RabbitConfig`** — passed to `broker.start(worker_config=)`.
- **Registration-time validation** — routes are validated at decoration time (fail-fast on
  DLX cycles, retry+ack conflicts, duplicate queues, invalid handler signatures).
- **Middleware chain is cached per route** — composed once, reused per message (no per-message
  closure allocation).
- **Settlement happens AFTER the transport call** — `RabbitMessage.ack()` sets `_disposition`
  only after `_ack_fn()` returns successfully (a failed ack propagates, not silently swallowed).

### Sync vs. async: two different connection models

This is the one place the two brokers are genuinely asymmetric, not just
syntactically different (`await` vs. not) — and it's caused real confusion,
so it's documented explicitly here.

- **`SyncTransport`** (pika, `BlockingConnection`) shares **one** connection
  for both publishing and consuming. `SyncBroker.run()`'s consume loop calls
  `process_data_events(time_limit=1.0)` in a tight loop, which — as a side
  effect — is what keeps that single connection's heartbeats serviced,
  whether or not any message is actually flowing. A **publish-only**
  `SyncBroker` (no subscribers, `run()`/`start_consuming()` never called)
  has nothing driving that pump: the connection is only touched when
  `publish()` runs. A long idle gap can get it heartbeat-timed-out
  broker-side; the *next* publish still transparently reconnects
  (`ensure_connected()` runs before every publish), but only reactively.
  Call `broker.pump_idle()` periodically, from the same thread that called
  `start()`, to reconnect proactively and keep the connection (and the
  liveness heartbeat) alive between publishes — see the Quick Start section
  for the pattern.

- **`AsyncTransportImpl`** (aio-pika) eagerly establishes **two** dedicated
  connections via `aio_pika.connect_robust()` — one for publishing, one for
  consuming — at `start()` time, regardless of whether you have any
  subscribers. `connect_robust()` manages its own heartbeat-sending and
  reconnection as independent asyncio tasks that keep running as long as the
  event loop is alive, whether or not your code is actively publishing.
  There is no async equivalent of `pump_idle()` because there's nothing to
  pump — the mechanism that makes it necessary on the sync side (a single
  connection that only gets touched by whatever the application code
  happens to call) doesn't exist on the async side.

The practical rule: if you're running a **publish-only `SyncBroker`**, call
`pump_idle()` on a timer. Every other combination (any consumer, or any
`AsyncBroker`) needs no equivalent wiring.

---

> **rabbitkit 1.1.0** — Python ≥ 3.11 · MIT License
>
> Built with Strategy patterns, Protocol-based typing, contextvars for async safety,
> daemon-thread worker pools for k8s-safe shutdown, streaming zip-bomb guards,
> HMAC replay protection with a reject-when-full (never evict-live) nonce cache,
> and a real in-memory TestBroker that exercises the production pipeline —
> not mock theater.