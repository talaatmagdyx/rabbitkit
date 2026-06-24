# rabbitkit

Production-grade RabbitMQ toolkit for Python -- sync (pika) and async (aio-pika) support.

Decorator-based routing, middleware pipeline, retry with delay queues, compression, dependency injection, RPC, circuit breaker, deduplication, backpressure, batch operations, worker pools, and full configurability.

## Features

- **Decorator-based routing** -- `@broker.subscriber(queue=..., exchange=..., routing_key=...)`
- **Modular routers** -- `RabbitRouter` with prefix, shared exchange, middleware, and `include_router`
- **Subscriber filtering** -- `filter_fn=` to reject messages before deserialization
- **Middleware pipeline** -- exception handling, error classification, retry, compression, tracing, deduplication, circuit breaker, rate limiting, signing, timeout
- **Retry with delay queues** -- TTL + DLX topology, configurable backoff with jitter, per-queue isolation
- **Compression** -- gzip (built-in) and zstd (optional) with automatic content-encoding headers
- **Dependency injection** -- `Depends()`, `Header()`, `Path()`, `Context()` parameter markers with generator support
- **Serialization** -- JSON (built-in), msgspec (optional), pydantic (optional), two-stage pipeline
- **Pydantic auto-validation** -- body type hints trigger automatic `model_validate()` during deserialization
- **RPC** -- `RPCClient` / `AsyncRPCClient` with direct reply-to; `broker.request()` shorthand
- **Error classification** -- transient vs permanent errors with configurable routing
- **Publisher confirms** -- optional delivery confirmation with `PublishOutcome`
- **Topology management** -- `AUTO_DECLARE`, `PASSIVE_ONLY`, `MANUAL` modes
- **Connection pooling** -- separate publisher/consumer connections, channel pools
- **Backpressure** -- `FlowController` with in-flight limits, rate limiting, connection.blocked handling
- **Batch operations** -- `BatchPublisher` for buffered publishes, `BatchAcker` for multi-ack
- **Worker pools** -- `SyncWorkerPool` (thread pool) and `AsyncWorkerPool` (semaphore-based) concurrency
- **Circuit breaker** -- `CircuitBreakerMiddleware` for fail-fast rejection on cascading failures
- **Deduplication** -- `DeduplicationMiddleware` for idempotent processing via Redis
- **Distributed locking** -- `LockMiddleware` + `RedisLock` for single-consumer guarantee
- **Tracing** -- `TracedConsumerMiddleware` with OpenTelemetry semantic attributes (obskit integration)
- **Structured logging** -- `LoggingConfig` activates structlog with per-message context binding
- **Env-based config** -- `RabbitSettings` loads `RABBITMQ_*` env vars via pydantic-settings
- **Result backends** -- `ResultMiddleware` + `RedisResultBackend` for fire-and-retrieve pattern
- **AsyncAPI docs** -- `generate_asyncapi_doc()` produces AsyncAPI 2.6.0 spec from broker routes
- **Management API** -- `RabbitManagementClient` for queue/exchange/connection inspection
- **Monitoring dashboard** -- `create_dashboard_app()` Starlette ASGI dashboard
- **CLI** -- `rabbitkit run/health/topology/shell` commands
- **Hot reload** -- `rabbitkit run --reload` restarts on file changes (watchfiles)
- **DLQ inspector** -- peek, replay, and purge dead-letter queues
- **FastAPI integration** -- `rabbitkit_lifespan()` async context manager
- **App lifecycle** -- startup/shutdown hooks, signal handling, state tracking
- **Testing** -- `TestBroker` (in-memory, no RabbitMQ needed), `TestApp`, pytest fixtures

## Installation

```bash
# Sync transport (pika)
pip install rabbitkit[sync]

# Async transport (aio-pika)
pip install rabbitkit[async]

# Both transports
pip install rabbitkit[all-brokers]

# Everything (all transports + all optional extras)
pip install rabbitkit[all]
```

### Optional extras

| Extra           | What it adds                                         |
|-----------------|------------------------------------------------------|
| `sync`          | pika >= 1.3                                          |
| `async`         | aio-pika >= 9.0                                      |
| `all-brokers`   | Both sync and async transports                       |
| `compression`   | zstd compression (zstandard >= 0.22)                 |
| `pydantic`      | Pydantic model serialization (>= 2.0)                |
| `msgspec`       | msgspec serialization (>= 0.18)                      |
| `redis`         | Redis for deduplication/locking/results (>= 5.0)     |
| `obskit`        | OpenTelemetry tracing via obskit (>= 3.1)            |
| `fastapi`       | FastAPI lifespan integration (>= 0.111)              |
| `settings`      | Env-var config via pydantic-settings (>= 2.0)        |
| `cli`           | CLI commands via typer (>= 0.12)                     |
| `reload`        | Hot reload via watchfiles (>= 0.21)                  |
| `management`    | Async management API via aiohttp (>= 3.9)            |
| `dashboard`     | Monitoring dashboard via starlette + uvicorn         |
| `all`           | All of the above                                     |

Requires **Python >= 3.11**.

## Quick Start

### Sync Example

```python
from rabbitkit import RabbitConfig
from rabbitkit.sync import SyncBroker

broker = SyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
def handle_order(body: bytes) -> None:
    print(f"Received order: {body}")

broker.start()
```

### Async Example

```python
from rabbitkit import RabbitConfig
from rabbitkit.async_ import AsyncBroker

broker = AsyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
async def handle_order(body: bytes) -> None:
    print(f"Received order: {body}")

await broker.start()
```

### Publishing

```python
from rabbitkit import MessageEnvelope

await broker.publish(MessageEnvelope(
    routing_key="notifications",
    body=b'{"user": 1, "msg": "hello"}',
    exchange="events",
))
```

## Configuration Reference

All configuration is composed from focused, immutable dataclasses. `RabbitConfig` is the top-level object that groups them together.

### RabbitConfig (top-level)

```python
from rabbitkit import RabbitConfig, ConnectionConfig, RetryConfig, CompressionConfig

config = RabbitConfig(
    connection=ConnectionConfig(...),
    socket=SocketConfig(...),
    security=SecurityConfig(...),
    publisher=PublisherConfig(...),
    consumer=ConsumerConfig(...),
    pool=PoolConfig(...),
    topology_mode=TopologyMode.AUTO_DECLARE,
    retry=RetryConfig(...),         # None = no retry middleware
    compression=CompressionConfig(...),  # None = no compression
)
```

### ConnectionConfig

Core connection parameters for the AMQP broker.

```python
from rabbitkit import ConnectionConfig

conn = ConnectionConfig(
    host="rabbitmq.example.com",    # default: "localhost"
    port=5672,                       # default: 5672
    username="myapp",                # default: "guest"
    password="secret",               # default: "guest"
    vhost="/production",             # default: "/"
    heartbeat=30,                    # default: 30 seconds
    socket_timeout=10.0,             # default: 10.0 seconds
    blocked_connection_timeout=300.0,# default: 300.0 seconds
    connection_name="order-service", # default: None
    reconnect_backoff_base=1.0,      # default: 1.0 seconds
    reconnect_backoff_max=30.0,      # default: 30.0 seconds
)

# Or parse from AMQP URL:
conn = ConnectionConfig.from_url("amqp://user:pass@host:5672/vhost?heartbeat=30")
```

### SocketConfig

Low-level TCP tuning applied best-effort depending on OS and backend.

```python
from rabbitkit import SocketConfig

socket = SocketConfig(
    tcp_nodelay=True,       # default: True
    tcp_keepidle=10,        # default: 10
    tcp_keepintvl=5,        # default: 5
    tcp_keepcnt=3,          # default: 3
    tcp_sndbuf=196608,      # default: 192KB
    tcp_rcvbuf=196608,      # default: 192KB
)
```

### SSLConfig / SecurityConfig

TLS and SASL authentication configuration.

```python
from rabbitkit import SSLConfig, SecurityConfig

ssl = SSLConfig(
    enabled=True,
    certfile="/path/to/cert.pem",
    keyfile="/path/to/key.pem",
    ca_certs="/path/to/ca.pem",
    cert_reqs="CERT_REQUIRED",      # default: "CERT_REQUIRED"
    server_hostname="rabbitmq.example.com",
)

security = SecurityConfig(
    mechanism="PLAIN",               # default: "PLAIN"
    ssl=ssl,
)
```

### PublisherConfig

Controls publisher behavior and delivery guarantees.

```python
from rabbitkit import PublisherConfig

publisher = PublisherConfig(
    exchange="",              # default exchange
    confirm_delivery=True,    # default: True (publisher confirms)
    confirm_timeout=5.0,      # default: 5.0 seconds
    mandatory=False,          # default: False
    persistent=True,          # default: True (delivery_mode=2)
)
```

### ConsumerConfig

Consumer prefetch and shutdown settings.

```python
from rabbitkit import ConsumerConfig

consumer = ConsumerConfig(
    prefetch_count=10,        # default: 10
    graceful_timeout=30.0,    # default: 30.0 seconds
)
```

### PoolConfig

Connection and channel pool sizing.

```python
from rabbitkit import PoolConfig

pool = PoolConfig(
    channel_pool_size=10,         # default: 10
    publisher_connections=1,      # default: 1
    consumer_connections=1,       # default: 1
)
```

### RetryConfig

Retry with delay queues. Can be set as a broker default or per-route override.

```python
from rabbitkit import RetryConfig
from rabbitkit.core.types import ErrorSeverity

retry = RetryConfig(
    max_retries=4,                              # default: 4
    delays=(5, 30, 120, 600),                   # seconds per attempt
    retry_header="x-rabbitkit-retry-count",     # default header name
    jitter_factor=0.1,                          # default: 0.1 (10%)
    dead_letter_exchange="",                    # default: ""
    per_queue=True,                             # default: True (isolated delay queues)
    unknown_policy=ErrorSeverity.PERMANENT,     # default: treat unknown errors as permanent
)
```

### CompressionConfig

Compression settings accepted by `CompressionMiddleware`.

```python
from rabbitkit import CompressionConfig

compression = CompressionConfig(
    algorithm="gzip",    # "gzip" or "zstd" (requires rabbitkit[compression])
    threshold=1024,      # compress only bodies >= 1024 bytes
    level=6,             # compression level (1-9 for gzip, 1-22 for zstd)
)
```

## Routing

### @subscriber Decorator

Register handlers with the `@broker.subscriber()` decorator.

```python
from rabbitkit import AckPolicy, RabbitExchange, RabbitQueue

@broker.subscriber(
    queue="orders",                              # queue name or RabbitQueue
    exchange=RabbitExchange(name="events"),       # optional exchange
    routing_key="order.created",                 # optional routing key
    ack_policy=AckPolicy.AUTO,                   # default: AUTO
    middlewares=[my_middleware],                  # per-route middleware
    retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),  # per-route retry
)
def handle_order(body: bytes) -> None:
    process_order(body)
```

### @publisher Decorator

Declare where a handler's return value is published.

```python
@broker.publisher(exchange="notifications", routing_key="order.confirmed")
@broker.subscriber(queue="orders")
def handle_order(body: bytes) -> bytes:
    # Return value is auto-published to notifications exchange
    return b'{"status": "confirmed"}'
```

### RabbitRouter (modular routing with prefix)

Group related handlers with shared defaults using `RabbitRouter`.

```python
from rabbitkit import RabbitRouter

orders_router = RabbitRouter(
    prefix="orders",
    exchange="orders-exchange",
    middlewares=[logging_middleware],
)

@orders_router.subscriber(queue="orders-queue", routing_key="created")
def handle_order_created(body: bytes) -> None:
    # routing_key becomes "orders.created" (prefix applied)
    process_order(body)

@orders_router.subscriber(queue="returns-queue", routing_key="returned")
def handle_order_returned(body: bytes) -> None:
    # routing_key becomes "orders.returned"
    process_return(body)
```

### include_router

Include router routes into a broker or another router.

```python
broker.include_router(orders_router)
```

## Message Processing

### AckPolicy

Controls how messages are acknowledged after handler execution.

| Policy           | Behavior                                                       |
|------------------|----------------------------------------------------------------|
| `AUTO`           | Success -> ack. Exception -> classify -> nack/reject.          |
| `MANUAL`         | Handler owns ack/nack/reject entirely via `msg.ack()` etc.    |
| `NACK_ON_ERROR`  | Success -> ack. Exception -> nack(requeue=False).              |
| `ACK_FIRST`      | Ack BEFORE handler runs (at-most-once delivery).               |

```python
from rabbitkit import AckPolicy

@broker.subscriber(queue="events", ack_policy=AckPolicy.MANUAL)
def handle_event(msg: RabbitMessage) -> None:
    try:
        process(msg.body)
        msg.ack()
    except Exception:
        msg.nack(requeue=True)
```

### RabbitMessage

Rich incoming message with transport-aware ack/nack/reject. Key attributes:

- `body: bytes` -- raw message body
- `headers: dict[str, Any]` -- AMQP headers
- `message_id`, `correlation_id`, `reply_to` -- standard AMQP properties
- `routing_key`, `exchange` -- delivery info
- `delivery_tag`, `redelivered`, `consumer_tag` -- transport metadata
- `path: dict[str, str]` -- extracted topic wildcard segments
- `is_settled: bool` -- whether ack/nack/reject has been called

Settlement methods (idempotent -- double-ack is a no-op):

```python
# Sync
msg.ack()
msg.nack(requeue=True)
msg.reject(requeue=False)

# Async
await msg.ack_async()
await msg.nack_async(requeue=True)
await msg.reject_async(requeue=False)
```

Exception-based settlement control:

```python
from rabbitkit import AckMessage, NackMessage, RejectMessage

@broker.subscriber(queue="orders")
def handle(body: bytes) -> None:
    if invalid(body):
        raise RejectMessage(requeue=False)  # reject the message
    if temporary_issue():
        raise NackMessage(requeue=True)     # nack with requeue
    raise AckMessage()                       # force ack
```

### Middleware Pipeline (ordering)

Middleware is applied as a chain. The outermost middleware runs first on receive and last on complete. Recommended ordering:

1. `TracedConsumerMiddleware` -- tracing span wraps everything
2. `ExceptionMiddleware` -- catches exceptions after retry gives up
3. `CircuitBreakerMiddleware` -- fail-fast when circuit is open
4. `DeduplicationMiddleware` -- skip duplicates before processing
5. `RetryMiddleware` -- retry transient failures with delay queues
6. `CompressionMiddleware` -- decompress before handler, compress on publish

## Dependency Injection

> **Enable DI:** the `Depends()` / `Header()` / `Path()` / `Context()` markers are
> resolved only when the broker is given a resolver:
> `AsyncBroker(config, di_resolver=DIResolver())` (from `rabbitkit.di.resolver`).
> Without it, handlers get only the body (and a `RabbitMessage`-typed parameter).

### Depends()

Inject dependencies into handler parameters using `Depends()` with `typing.Annotated`.

```python
from typing import Annotated
from rabbitkit.di.depends import Depends

def get_db():
    return DatabaseSession()

@broker.subscriber(queue="orders")
def handle_order(
    body: bytes,
    db: Annotated[DatabaseSession, Depends(get_db)],
) -> None:
    db.save(body)
```

Dependencies are cached per message by default. Disable caching with `Depends(factory, use_cache=False)`.

### Generator Dependencies (yield-based)

Generators are supported for setup/teardown patterns. The generator is cleaned up in `finally` after the handler completes, in reverse order.

```python
from typing import Annotated
from rabbitkit.di.depends import Depends

def get_db_session():
    session = Session()
    try:
        yield session
    finally:
        session.close()

@broker.subscriber(queue="orders")
def handle(
    body: bytes,
    db: Annotated[Session, Depends(get_db_session)],
) -> None:
    db.execute(...)
```

Async generators are also supported in async handlers.

### Header(), Path(), Context()

Extract values from message headers, topic wildcard segments, or application context.

```python
from typing import Annotated
from rabbitkit.di.context import Header, Path, Context

# Name a routing-key segment with {name}; it binds to AMQP as '*' and is
# extracted into message.path for Path() to read.
@broker.subscriber(queue="events", routing_key="events.{level}.#")
def handle(
    body: bytes,
    tenant: Annotated[str, Header("x-tenant")],
    level: Annotated[str, Path("level")],
    app_name: Annotated[str, Context("app")],
) -> None:
    print(f"Tenant: {tenant}, Level: {level}, App: {app_name}")
```

## Middleware

### Built-in Middleware

#### ExceptionMiddleware

Outermost middleware that catches exceptions after retry gives up. Provides fallback values for error recovery.

```python
from rabbitkit.middleware.exception import ExceptionMiddleware

exc_mw = ExceptionMiddleware(swallow_permanent=False)
exc_mw.add_handler(ValueError, lambda exc: {"error": str(exc)})
```

#### RetryMiddleware

Routes failed messages to delay queues using TTL + DLX topology. Errors are classified as transient (retryable) or permanent (sent to DLQ).

```python
from rabbitkit.middleware.retry import RetryMiddleware
from rabbitkit import RetryConfig

retry_mw = RetryMiddleware(
    config=RetryConfig(max_retries=4, delays=(5, 30, 120, 600)),
    publish_fn=transport.publish,
)
```

Delay queue topology (per_queue=True):
- `{source_queue}.retry.{attempt}` -- delay queues with TTL
- `{source_queue}.dlq` -- dead-letter queue for exhausted messages

#### CompressionMiddleware

Compresses outgoing message bodies and decompresses incoming ones. Supports gzip (built-in) and zstd (optional).

```python
from rabbitkit.middleware.compression import CompressionMiddleware
from rabbitkit import CompressionConfig

comp_mw = CompressionMiddleware(CompressionConfig(
    algorithm="gzip",    # or "zstd" with rabbitkit[compression]
    threshold=1024,      # only compress bodies >= 1KB
    level=6,
))
```

#### TracedConsumerMiddleware (obskit)

OpenTelemetry tracing integration via obskit. No-op passthrough when obskit is not installed.

```python
from rabbitkit import TracedConsumerMiddleware

tracing_mw = TracedConsumerMiddleware(service_name="order-service")
# Wraps consume/publish in trace spans with semantic attributes:
#   messaging.system, messaging.operation, messaging.rabbitmq.routing_key, etc.
```

#### DeduplicationMiddleware (Redis)

Idempotent message processing using Redis SETNX. Duplicate messages are silently acked and skipped.

```python
import redis
from rabbitkit import DeduplicationMiddleware, DeduplicationConfig

dedup_mw = DeduplicationMiddleware(
    redis_client=redis.Redis(),
    config=DeduplicationConfig(
        key_source="message_id",      # "message_id", "correlation_id", or "body_hash"
        key_prefix="rabbitkit:dedup",  # Redis key prefix
        ttl=86400,                     # 24 hours
        fallback_on_redis_error=True,  # process anyway if Redis is down
    ),
    # Or provide a custom key function:
    # key_fn=lambda msg: f"custom:{msg.headers['idempotency-key']}",
)
```

#### CircuitBreakerMiddleware

Wraps consume and publish operations with a circuit breaker for fail-fast rejection. No-op when no circuit breaker is provided.

```python
from rabbitkit import CircuitBreakerMiddleware

# With obskit circuit breaker:
# from obskit.resilience import CircuitBreaker
# cb = CircuitBreaker(name="rabbitmq", fail_max=5, reset_timeout=60)

middleware = CircuitBreakerMiddleware(
    circuit_breaker=consume_cb,            # for consume operations
    publish_circuit_breaker=publish_cb,    # separate CB for publish (optional)
)
```

When the circuit is open, `CircuitBreakerOpenError` is raised immediately without hitting the broker.

### Custom Middleware

Extend `BaseMiddleware` and override the hooks you need. All hooks have no-op defaults.

```python
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.core.message import RabbitMessage

class LoggingMiddleware(BaseMiddleware):
    def consume_scope(self, call_next, message):
        print(f"Processing: {message.routing_key}")
        result = call_next(message)
        print(f"Done: {message.routing_key}")
        return result

    async def consume_scope_async(self, call_next, message):
        print(f"Processing: {message.routing_key}")
        result = await call_next(message)
        print(f"Done: {message.routing_key}")
        return result
```

Available hooks:
- `on_receive(message)` / `on_receive_async(message)` -- notification on receipt
- `consume_scope(call_next, message)` / `consume_scope_async(...)` -- wrap handler execution
- `after_processed(message, exc)` / `after_processed_async(...)` -- post-processing notification
- `publish_scope(call_next, envelope)` / `publish_scope_async(...)` -- wrap outgoing publish

## Retry & Error Handling

### Error Classification

Errors are classified into two severities:

- **TRANSIENT** -- temporary failures that may succeed on retry (network errors, timeouts)
- **PERMANENT** -- unrecoverable failures that should go to the DLQ (validation errors, business logic failures)

The `unknown_policy` setting in `RetryConfig` determines how unclassified errors are treated (default: `PERMANENT`).

### Delay Queue Topology

When `per_queue=True` (default), each source queue gets isolated delay infrastructure:

```
orders-queue
  -> orders-queue.retry.0   (TTL=5s,   DLX back to orders exchange)
  -> orders-queue.retry.1   (TTL=30s,  DLX back to orders exchange)
  -> orders-queue.retry.2   (TTL=120s, DLX back to orders exchange)
  -> orders-queue.retry.3   (TTL=600s, DLX back to orders exchange)
  -> orders-queue.dlq       (terminal failures)
```

### Per-route Retry Configuration

Override retry settings on individual routes.

```python
from rabbitkit import RetryConfig, RETRY_DISABLED

# Custom retry for this route
@broker.subscriber(
    queue="critical-orders",
    retry=RetryConfig(max_retries=10, delays=(1, 5, 15, 60, 300, 600, 1800, 3600, 7200, 14400)),
)
def handle_critical(body: bytes) -> None:
    process(body)

# Disable retry entirely for this route
@broker.subscriber(queue="fire-and-forget", retry=RETRY_DISABLED)
def handle_ephemeral(body: bytes) -> None:
    log_event(body)
```

### RETRY_DISABLED Sentinel

`RETRY_DISABLED` is a typed singleton that explicitly disables retry on a route. It is distinct from `RetryConfig(max_retries=0)`, which means retry-owned terminal semantics with zero retry attempts (immediate DLQ on any classified error).

## High-Load Infrastructure

### FlowController (backpressure)

Publish-side flow control with three pressure signals: connection.blocked, in-flight limit, and token-bucket rate limiting.

```python
from rabbitkit import FlowController, BackpressureConfig

fc = FlowController(BackpressureConfig(
    max_in_flight=1000,       # max concurrent unconfirmed publishes
    rate_limit=5000,          # messages per second (None = unlimited)
    blocked_timeout=60.0,     # timeout waiting for unblock
    on_blocked="wait",        # "wait", "raise", or "drop"
))

# Before publish:
if fc.acquire(timeout=5.0):
    transport.publish(envelope)
    fc.release()

# Register with transport for connection.blocked signals:
transport.on_blocked(fc.on_blocked)
transport.on_unblocked(fc.on_unblocked)

# Async variant:
if await fc.acquire_async(timeout=5.0):
    await transport.publish(envelope)
    await fc.release_async()
```

### BatchPublisher

Buffer outgoing envelopes and flush as a batch with optional delivery confirmation.

```python
from rabbitkit import BatchPublisher, BatchPublishConfig

bp = BatchPublisher(
    publish_fn=transport.publish,
    config=BatchPublishConfig(
        batch_size=100,           # auto-flush at this count
        flush_interval_ms=50,     # flush interval
        max_in_flight=1000,       # max buffered
    ),
    confirm_fn=transport.wait_for_confirms,  # optional
)

bp.add(envelope1)
bp.add(envelope2)
# Auto-flushes at batch_size, or manually:
bp.flush()
bp.close()  # flush remaining + cleanup

# Async:
await bp.add_async(envelope)
await bp.flush_async()
await bp.close_async()
```

### BatchAcker

Accumulate delivery tags and issue a single `ack(max_tag, multiple=True)` when the batch fills.

```python
from rabbitkit import BatchAcker, BatchAckConfig

ba = BatchAcker(
    ack_fn=channel.basic_ack,
    config=BatchAckConfig(
        batch_size=100,           # ack every 100 messages
        flush_interval_ms=200,    # or every 200ms
    ),
)

ba.add(delivery_tag=1)
ba.add(delivery_tag=2)
ba.flush()  # ack(max_tag=2, multiple=True)
ba.close()  # flush remaining
```

### WorkerPool (SyncWorkerPool, AsyncWorkerPool)

Concurrent message processing within a single broker.

```python
from rabbitkit import SyncWorkerPool, AsyncWorkerPool, WorkerConfig

# Sync: ThreadPoolExecutor-based
pool = SyncWorkerPool(config=WorkerConfig(worker_count=4))
pool.start()
pool.submit(callback, message)  # runs in thread pool
pool.stop(timeout=30.0)

# Async: asyncio.Semaphore-based
pool = AsyncWorkerPool(config=WorkerConfig(worker_count=8))
pool.start()
await pool.submit(callback, message)  # runs as async task with semaphore
await pool.stop(timeout=30.0)
```

When `worker_count=1` (default), handlers run inline with no pool overhead.

## RPC (Request/Response)

### RPCClient / AsyncRPCClient

Request/response over RabbitMQ using direct reply-to (`amq.rabbitmq.reply-to`).

```python
from rabbitkit import RPCClient, RPCTimeoutError

# Sync
client = RPCClient(transport, max_pending=100)
try:
    response = client.call(
        "rpc.orders",
        b'{"method": "get_user", "id": 1}',
        timeout=5.0,
    )
    print(response.body)
except RPCTimeoutError:
    print("RPC call timed out")
finally:
    client.close()
```

```python
from rabbitkit import AsyncRPCClient

# Async
client = AsyncRPCClient(transport, max_pending=100)
response = await client.call(
    "rpc.orders",
    b'{"method": "get_user", "id": 1}',
    timeout=5.0,
)
print(response.body)
await client.close()
```

Each call gets a unique `correlation_id` for response matching. `RPCTimeoutError` is raised if the response is not received within the timeout.

## DLQ Inspector

Inspect and recover messages from dead-letter queues.

```python
from rabbitkit import DLQInspector

inspector = DLQInspector(transport)

# Peek at messages without consuming them (requeued after inspection)
messages = inspector.peek("orders-queue.dlq", limit=5)
for msg in messages:
    print(msg.headers, msg.body)

# Replay matching messages back to source queue
count = inspector.replay(
    "orders-queue.dlq",
    predicate=lambda msg: msg.headers.get("x-error") == "timeout",
    target_queue="orders-queue",
)
print(f"Replayed {count} messages")

# Purge entire DLQ (immediate, unfiltered)
count = inspector.purge("orders-queue.dlq")

# Async variants
messages = await inspector.peek_async("orders-queue.dlq", limit=5)
count = await inspector.replay_async("orders-queue.dlq", target_queue="orders-queue")
count = await inspector.purge_async("orders-queue.dlq")
```

## Stream Queues

Use `QueueType.STREAM` for RabbitMQ stream queues. Stream queues are append-only logs with time-based retention.

```python
from rabbitkit import RabbitQueue, QueueType

stream_queue = RabbitQueue(
    name="events-stream",
    queue_type=QueueType.STREAM,
    durable=True,  # required for streams
    # Note: stream queues do NOT support:
    #   exclusive, lazy, max_priority, message_ttl
)

@broker.subscriber(queue=stream_queue)
def handle_event(body: bytes) -> None:
    process(body)
```

## Health Checks

Integrate with obskit or your own health check system by registering startup/shutdown hooks on `RabbitApp` and checking `AppState`.

```python
from rabbitkit import RabbitApp, AppState

app = RabbitApp(title="order-service")

# State tracking: IDLE -> STARTING -> RUNNING -> STOPPING -> STOPPED
assert app.state == AppState.IDLE

app.start()
assert app.state == AppState.RUNNING

# Use app.state in health check endpoints
def is_healthy() -> bool:
    return app.state == AppState.RUNNING
```

## FastAPI Integration

Use `rabbitkit_lifespan()` as the FastAPI lifespan context manager. It handles start/stop ordering for both the broker and `RabbitApp`.

```python
from fastapi import FastAPI
from rabbitkit import RabbitConfig, RabbitApp
from rabbitkit.async_ import AsyncBroker
from rabbitkit.fastapi import rabbitkit_lifespan

rabbit_app = RabbitApp(title="my-service")
broker = AsyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
async def handle_order(body: bytes) -> None:
    process(body)

app = FastAPI(lifespan=rabbitkit_lifespan(broker=broker, rabbit_app=rabbit_app))
```

Start order: `rabbit_app.start_async()` -> `broker.start()`.
Stop order: `broker.stop()` -> `rabbit_app.stop_async()`.

Supports both sync and async brokers with duck-typed detection.

## Testing

### TestBroker

In-memory broker for unit testing -- no RabbitMQ required. Every handler gets a `.mock` attribute for assertions.

```python
from rabbitkit.testing import TestBroker

def test_order_handler():
    broker = TestBroker()

    @broker.subscriber(queue="orders")
    def handle_order(body: bytes) -> None:
        assert body == b'{"id": 1}'

    broker.start()
    broker.publish("orders", b'{"id": 1}')

    handle_order.mock.assert_called_once()
```

Async testing:

```python
import pytest
from rabbitkit.testing import TestBroker

@pytest.mark.asyncio
async def test_async_handler():
    broker = TestBroker()

    @broker.subscriber(queue="orders")
    async def handle_order(body: bytes) -> None:
        assert body == b'{"id": 1}'

    broker.start()
    await broker.publish_async("orders", b'{"id": 1}')
    handle_order.mock.assert_called_once()
```

Useful assertion properties on `TestBroker`:

- `broker.published_messages` -- all `MessageEnvelope` objects published by handlers
- `broker.consumed_messages` -- all `RabbitMessage` objects consumed during tests
- `broker.routes` -- all registered `RouteDefinition` objects
- `broker.declared_exchanges` / `broker.declared_queues` -- recorded topology

### TestApp

Full lifecycle wrapper that triggers startup/shutdown hooks. Supports context manager usage.

```python
from rabbitkit.testing import TestBroker, TestApp
from rabbitkit import RabbitApp

def test_with_lifecycle():
    rabbit_app = RabbitApp(title="test-app")
    broker = TestBroker()

    @broker.subscriber(queue="orders")
    def handle_order(body: bytes) -> None:
        pass

    with TestApp(rabbit_app, broker) as ta:
        broker.publish("orders", b'{"id": 1}')
        handle_order.mock.assert_called_once()

    # Or async:
    # async with TestApp(rabbit_app, broker) as ta:
    #     await broker.publish_async("orders", b'...')
```

## Topology

### RabbitExchange

Exchange declaration model with all AMQP exchange types.

```python
from rabbitkit import RabbitExchange, ExchangeType

exchange = RabbitExchange(
    name="events",
    type=ExchangeType.TOPIC,     # DIRECT, FANOUT, TOPIC, HEADERS
    durable=True,                # default: True
    auto_delete=False,           # default: False
    passive=False,               # default: False
    internal=False,              # default: False
    arguments={},                # extra x-arguments
    bind_to="upstream-exchange", # exchange-to-exchange binding
)
```

### RabbitQueue

Queue declaration model with type-specific validation and all RabbitMQ queue features.

```python
from rabbitkit import RabbitQueue, QueueType

queue = RabbitQueue(
    name="orders",
    queue_type=QueueType.CLASSIC,  # CLASSIC, QUORUM, STREAM
    durable=True,
    exclusive=False,
    auto_delete=False,

    # Dead-letter
    dead_letter_exchange="dlx",
    dead_letter_routing_key="orders.dlq",

    # Limits
    message_ttl=60000,         # ms
    max_length=100000,
    max_length_bytes=104857600,

    # Classic-only
    lazy=True,                 # x-queue-mode: lazy
    max_priority=10,           # priority queue (0-255)

    # Quorum-only
    delivery_limit=5,          # x-delivery-limit
    single_active_consumer=True,

    # Overflow and expiry
    overflow="reject-publish", # "drop-head", "reject-publish", "reject-publish-dlx"
    expires=3600000,           # auto-delete after idle (ms)

    # Escape hatch for any x-argument
    arguments={"x-custom": "value"},
)
```

Validation is enforced at creation time. For example, quorum queues must be durable and cannot be exclusive. Stream queues cannot have message TTL.

### TopologyMode

Controls how exchanges/queues/bindings are handled on startup.

| Mode             | Behavior                                              |
|------------------|-------------------------------------------------------|
| `AUTO_DECLARE`   | Declare exchanges, queues, and bindings on startup.   |
| `PASSIVE_ONLY`   | All declarations use `passive=True` (verify only).    |
| `MANUAL`         | Skip all topology operations.                         |

```python
from rabbitkit import RabbitConfig, TopologyMode

config = RabbitConfig(topology_mode=TopologyMode.PASSIVE_ONLY)
```

## App Lifecycle

`RabbitApp` manages startup/shutdown hooks with signal handling and state tracking.

```python
from rabbitkit import RabbitApp, AppState

app = RabbitApp(title="order-service")

@app.on_startup
def init_db():
    db.connect()

@app.after_startup
def log_ready():
    print("Service ready")

@app.on_shutdown
def close_db():
    db.disconnect()

@app.after_shutdown
def log_stopped():
    print("Service stopped")

# Sync lifecycle
app.start()   # IDLE -> STARTING -> RUNNING
app.stop()    # RUNNING -> STOPPING -> STOPPED

# Async lifecycle with signal handling (SIGINT/SIGTERM)
await app.run_async()  # start, wait for signal, stop

# Programmatic shutdown
app.request_shutdown()
```

Lifecycle states: `IDLE` -> `STARTING` -> `RUNNING` -> `STOPPING` -> `STOPPED`.

Startup failure rollback: if any startup hook fails, `on_shutdown` hooks are still called.

## Subscriber Filtering

Reject messages before deserialization with a synchronous predicate function.

```python
# Only process messages for a specific tenant
@broker.subscriber(
    queue="events",
    filter_fn=lambda msg: msg.headers.get("x-tenant") == "acme",
)
async def handle_acme_event(body: bytes) -> None:
    ...

# Filter by routing key pattern
@broker.subscriber(
    queue="notifications",
    filter_fn=lambda msg: msg.routing_key.startswith("order."),
)
def handle_order_notification(body: bytes) -> None:
    ...
```

Messages that fail the filter are nacked with `requeue=False` (discarded/DLQ).
The filter runs before ACK_FIRST, deserialization, and DI resolution —
so it is extremely cheap for high-volume routing.

## Structured Logging

rabbitkit uses **structlog** for all internal logging.  Activate it with
`LoggingConfig` in `RabbitConfig` and the broker configures structlog on
startup.

```python
from rabbitkit import RabbitConfig
from rabbitkit.core.logging import LoggingConfig
from rabbitkit.async_ import AsyncBroker

# Development — coloured console output with caller info
broker = AsyncBroker(RabbitConfig(
    logging=LoggingConfig(
        render_json=False,
        include_caller_info=True,
    )
))

# Production — JSON lines for log aggregators (Loki, Elasticsearch, etc.)
broker = AsyncBroker(RabbitConfig(
    logging=LoggingConfig(
        render_json=True,
        timestamper_fmt="iso",
    )
))
```

Every log line emitted while handling a message automatically includes:
`message_id`, `routing_key`, `queue`, `handler`.

Manual setup (without `RabbitConfig.logging`):

```python
from rabbitkit.core.logging import configure_structlog, LoggingConfig

configure_structlog(LoggingConfig(render_json=True))
```

## Environment-based Configuration

Load `RabbitConfig` from `RABBITMQ_*` environment variables or a `.env` file.
Requires `pip install rabbitkit[settings]`.

```python
from rabbitkit.core.env_config import RabbitSettings
from rabbitkit.async_ import AsyncBroker

settings = RabbitSettings()   # reads env vars automatically
broker   = AsyncBroker(settings.to_rabbit_config())
```

`.env` file:

```env
RABBITMQ_HOST=rabbitmq.prod.internal
RABBITMQ_USER=myapp
RABBITMQ_PASSWORD=secret
RABBITMQ_VHOST=/production
RABBITMQ_PREFETCH_COUNT=20
RABBITMQ_CONFIRM_DELIVERY=true
RABBITMQ_TOPOLOGY_MODE=AUTO_DECLARE
```

Override at runtime (constructor kwargs take precedence over env):

```python
settings = RabbitSettings(host="staging-rabbit", prefetch_count=5)
config   = settings.to_rabbit_config()
```

Supported variables: `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`,
`RABBITMQ_PASSWORD`, `RABBITMQ_VHOST`, `RABBITMQ_HEARTBEAT`,
`RABBITMQ_SOCKET_TIMEOUT`, `RABBITMQ_PREFETCH_COUNT`, `RABBITMQ_CONFIRM_DELIVERY`,
`RABBITMQ_CHANNEL_POOL_SIZE`, `RABBITMQ_TOPOLOGY_MODE`.

## RPC Shorthand: `broker.request()`

A convenience method on both `AsyncBroker` and `SyncBroker` that sends an
RPC request and waits for the response without manually creating an
`RPCClient`.

```python
# Async
response = await broker.request(
    routing_key="rpc.users",
    body=b'{"method": "get", "id": 42}',
    timeout=5.0,
)
print(response.body)

# Sync
response = broker.request(
    routing_key="rpc.inventory",
    body=b'{"sku": "ABC-123"}',
    timeout=3.0,
    exchange="rpc-exchange",
    headers={"x-priority": "high"},
)
```

The RPC client is lazily initialized on first call and reused for subsequent
calls.  It is closed automatically in `broker.stop()`.

## Rate Limiting Middleware

Limits how fast the consumer processes messages using a **token bucket**
algorithm.  Requires `pip install rabbitkit[cli]` is _not_ needed — it is
bundled in the core.

```python
from rabbitkit.middleware.rate_limit import RateLimitMiddleware, RateLimitConfig

# Process at most 100 messages/s; block if exceeded
rate_mw = RateLimitMiddleware(RateLimitConfig(max_rate=100.0, burst=10))

# Nack instead of blocking (lets another consumer handle the overflow)
rate_mw = RateLimitMiddleware(
    RateLimitConfig(max_rate=50.0, on_limited="nack")
)

# Drop excess messages (fire-and-forget)
rate_mw = RateLimitMiddleware(
    RateLimitConfig(max_rate=200.0, burst=20, on_limited="drop")
)

@broker.subscriber(queue="events", middlewares=[rate_mw])
async def handle_event(body: bytes) -> None:
    ...
```

`on_limited` options:

| Value    | Behaviour                                        |
|----------|--------------------------------------------------|
| `"wait"` | Sleep until a token is available (default)       |
| `"nack"` | Nack with `requeue=True` — another consumer tries|
| `"drop"` | Nack with `requeue=False` — message discarded    |

## Message Signing Middleware

Sign outgoing messages with HMAC and verify incoming signatures.
No extra dependencies — uses stdlib `hmac` + `hashlib`.

```python
from rabbitkit.middleware.signing import SigningMiddleware, SigningConfig

signing_mw = SigningMiddleware(
    SigningConfig(
        secret_key="shared-secret",   # or bytes
        algorithm="hmac-sha256",      # or "hmac-sha512"
        reject_unsigned=True,         # reject messages with no signature
        reject_invalid=True,          # reject messages with wrong signature (default)
    )
)

# On the publisher side — signs all outgoing messages
@broker.publisher(exchange="events", routing_key="order.created")
@broker.subscriber(queue="orders-input", middlewares=[signing_mw])
async def process_order(body: bytes) -> bytes:
    return b'{"status": "confirmed"}'

# On the consumer side (different service, same secret)
@broker.subscriber(queue="order-results", middlewares=[signing_mw])
async def handle_result(body: bytes) -> None:
    ...
```

`InvalidSignatureError` is raised on verification failure. Combined with
the default error classifier, it goes straight to the DLQ (permanent error).

## Handler Timeout Middleware

Enforce a maximum processing time per message.

```python
from rabbitkit.middleware.timeout import TimeoutMiddleware, TimeoutConfig

timeout_mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=10.0))

@broker.subscriber(queue="slow-tasks", middlewares=[timeout_mw])
async def handle_slow_task(body: bytes) -> None:
    await some_potentially_slow_operation()

# Default timeout is 30 seconds
timeout_mw = TimeoutMiddleware()
```

Combine with retry for automatic re-queuing on timeout:

```python
from rabbitkit import RetryConfig
from rabbitkit.middleware.retry import RetryMiddleware

retry_mw = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120)))
timeout_mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=15.0))

@broker.subscriber(
    queue="jobs",
    middlewares=[retry_mw, timeout_mw],  # retry outermost
)
async def run_job(body: bytes) -> None:
    ...
```

`HandlerTimeoutError` (subclass of `TimeoutError`) is classified as TRANSIENT
by default, so retry middleware will re-queue the message.

## Pydantic Auto-Validation

Type-hint the handler body parameter with a Pydantic model and deserialization
automatically calls `model_validate()`.  No extra setup required.

```python
from pydantic import BaseModel

class Order(BaseModel):
    id: int
    item: str
    qty: int
    price: float

@broker.subscriber(queue="orders")
async def handle_order(order: Order) -> None:
    # `order` is already validated — invalid data raises ValidationError
    print(f"Order #{order.id}: {order.qty}x {order.item}")
```

Validation errors are classified as **PERMANENT** (invalid input) by the
default classifier and will not be retried — the message goes to the DLQ.

Nested models and field validators work as expected:

```python
from pydantic import BaseModel, validator

class Address(BaseModel):
    street: str
    city: str

class Customer(BaseModel):
    name: str
    email: str
    address: Address

@broker.subscriber(queue="customers")
async def handle_customer(customer: Customer) -> None:
    print(customer.address.city)
```

## Distributed Locking Middleware

Ensure only one consumer across the cluster processes a message for a
given key at a time.

```python
import redis
from rabbitkit.locking import RedisLock, LockMiddleware

r = redis.Redis(host="redis")
lock = RedisLock(r, prefix="myapp:lock:", ttl=30)

# Default key: message.routing_key
lock_mw = LockMiddleware(lock, timeout=5.0)

# Custom key: extracted from body
import json
lock_mw = LockMiddleware(
    lock,
    key_fn=lambda msg: json.loads(msg.body)["order_id"],
    timeout=10.0,
)

@broker.subscriber(queue="orders", middlewares=[lock_mw])
async def handle_order(body: bytes) -> None:
    # Guaranteed exclusive access per order_id
    ...
```

If the lock cannot be acquired (another instance holds it), the message is
nacked with `requeue=True` so it can be retried later.

Bring your own lock implementation — any object implementing the
`DistributedLock` protocol works.

## Result Backends

Store handler return values so callers can retrieve them by `correlation_id`.
Implements the **fire-and-retrieve** pattern.

```python
import redis
from rabbitkit.results.backend import RedisResultBackend
from rabbitkit.results.middleware import ResultMiddleware

r = redis.Redis()
backend = RedisResultBackend(r, key_prefix="orders:result:")
result_mw = ResultMiddleware(backend, ttl=300)  # store for 5 minutes

@broker.subscriber(queue="compute", middlewares=[result_mw])
def compute(body: bytes) -> dict:
    return {"answer": 42}   # automatically stored in Redis
```

Retrieval from the caller side:

```python
import time, json

correlation_id = "req-abc-123"

# Send request
broker.publish(MessageEnvelope(
    routing_key="compute",
    body=b'{"n": 7}',
    correlation_id=correlation_id,
))

# Poll for result
for _ in range(50):
    result = backend.fetch(correlation_id)
    if result:
        print(json.loads(result))
        break
    time.sleep(0.1)
```

Async variant:

```python
await backend.store_async(correlation_id, result_bytes, ttl=600)
result = await backend.fetch_async(correlation_id)
```

## AsyncAPI Documentation

Generate an **AsyncAPI 2.6.0** specification from your broker's routes.
No extra dependencies.

```python
from rabbitkit.asyncapi.generator import (
    generate_asyncapi_doc,
    generate_asyncapi_json,
    AsyncAPIGeneratorConfig,
)

config = AsyncAPIGeneratorConfig(
    title="Order Service",
    version="2.1.0",
    description="Processes orders and emits confirmations.",
    server_url="rabbitmq.prod.internal:5672",
)

# As a dict (JSON-serializable)
doc = generate_asyncapi_doc(broker.routes, config)

# As a JSON string
json_str = generate_asyncapi_json(broker.routes, config, indent=2)

# Save to file
import json
with open("asyncapi.json", "w") as f:
    json.dump(doc, f, indent=2)
```

Via the CLI:

```bash
rabbitkit docs generate myapp.main:broker > asyncapi.json
rabbitkit docs serve   myapp.main:broker   # opens Studio in browser
```

The generator extracts message payload schemas from handler type hints:
- Pydantic models → `model_json_schema()`
- stdlib dataclasses → field introspection
- Primitives (`str`, `int`, `float`, `bool`) → JSON primitives
- `bytes` / untyped → empty schema

## Serialization Pipeline

Compose parser and decoder independently for flexible wire-format handling.
Conforms to the `Serializer` protocol — plugs into `broker(serializer=...)`.

```python
from rabbitkit.serialization.pipeline import (
    SerializationPipeline,
    JsonParser,
    PydanticDecoder,
    DataclassDecoder,
    RawDecoder,
)

# JSON → Pydantic model
pipeline = SerializationPipeline(JsonParser(), PydanticDecoder())
broker = AsyncBroker(config, serializer=pipeline)

@broker.subscriber(queue="orders")
async def handle(order: Order) -> None:   # auto-validated Pydantic model
    ...
```

JSON → stdlib dataclass:

```python
from dataclasses import dataclass

@dataclass
class Event:
    type: str
    payload: dict

pipeline = SerializationPipeline(JsonParser(), DataclassDecoder())

@broker.subscriber(queue="events", serializer=pipeline)
def handle(event: Event) -> None:
    print(event.type)
```

Custom parser (msgpack):

```python
import msgpack

class MsgpackParser:
    def parse(self, data: bytes, content_type=None):
        return msgpack.unpackb(data, raw=False)

    def serialize(self, data) -> bytes:
        return msgpack.packb(data, use_bin_type=True)

    @property
    def content_type(self) -> str:
        return "application/msgpack"

pipeline = SerializationPipeline(MsgpackParser(), PydanticDecoder())
```

## RabbitMQ Management API Client

Inspect queues, exchanges, connections, and node health via the
RabbitMQ HTTP Management API.

```python
from rabbitkit.management import RabbitManagementClient, ManagementConfig

client = RabbitManagementClient(
    ManagementConfig(
        url="http://rabbitmq:15672",
        username="admin",
        password="secret",
        timeout=10.0,
    )
)

# List all queues
for q in client.list_queues():
    print(q["name"], "—", q["messages"], "messages")

# Get details for a specific queue
queue = client.get_queue("orders", vhost="/production")

# Purge a queue (removes all messages)
client.purge_queue("orders-test")

# Delete a queue entirely
client.delete_queue("temp-queue")

# List exchanges
exchanges = client.list_exchanges()

# Overview — cluster-wide statistics
info = client.overview()
print(info["rabbitmq_version"])

# Health check (True = healthy)
if not client.health_check():
    raise RuntimeError("RabbitMQ node is unhealthy")
```

Async variants (requires `pip install rabbitkit[management]`):

```python
queues = await client.list_queues_async()
health = await client.health_check_async()
```

Default configuration uses `http://localhost:15672` with guest/guest.

## Monitoring Dashboard

A lightweight Starlette ASGI app that displays broker health and routes.
Requires `pip install rabbitkit[dashboard]`.

```python
from rabbitkit.dashboard import create_dashboard_app

app = create_dashboard_app(broker)

# Serve with uvicorn:
# uvicorn myapp.dashboard:app --host 0.0.0.0 --port 8080
```

Via the CLI:

```bash
rabbitkit dashboard myapp.main:broker --port 8080
```

With Management API for live queue stats:

```python
from rabbitkit.management import RabbitManagementClient
from rabbitkit.dashboard import create_dashboard_app

mgmt = RabbitManagementClient()
app  = create_dashboard_app(broker, management_client=mgmt)
```

Mount inside an existing FastAPI app:

```python
from fastapi import FastAPI
from rabbitkit.dashboard import create_dashboard_app

api = FastAPI()
api.mount("/rabbit", create_dashboard_app(broker))
```

Endpoints:

| URL               | Description                                       |
|-------------------|---------------------------------------------------|
| `GET /`           | HTML dashboard (routes table, health badge)       |
| `GET /api/health` | JSON health: status, connected, consumer_count    |
| `GET /api/routes` | JSON array of registered routes                   |

## CLI

Run, inspect, and debug brokers from the command line.
Requires `pip install rabbitkit[cli]`.

```bash
# Start a broker (auto-detects sync/async)
rabbitkit run myapp.main:broker

# Hot reload on file changes (requires rabbitkit[reload])
rabbitkit run myapp.main:broker --reload

# Watch extra file types
rabbitkit run myapp.main:broker --reload --reload-ext .yml,.toml

# Run multiple worker processes
rabbitkit run myapp.main:broker --workers 4

# Check broker health (exit 1 if unhealthy)
rabbitkit health check myapp.main:broker

# List all routes in table format
rabbitkit topology list myapp.main:broker

# List routes as JSON
rabbitkit topology list myapp.main:broker --format json

# Interactive Python shell with broker pre-loaded
rabbitkit shell myapp.main:broker
```

### Interactive Shell

`rabbitkit shell` opens a Python REPL with these variables pre-loaded:

| Variable  | Value                        |
|-----------|------------------------------|
| `broker`  | The broker instance          |
| `routes`  | `broker.routes`              |
| `config`  | `broker.config`              |
| `publish` | `broker.publish`             |

Uses IPython if available, falls back to `code.interact`.

```python
# Example shell session
rabbitkit shell myapp.main:broker

In [1]: len(routes)
Out[1]: 3

In [2]: routes[0].queue.name
Out[2]: 'orders'

In [3]: from rabbitkit import MessageEnvelope
   ...: publish(MessageEnvelope(routing_key="orders", body=b'{"test": 1}'))
Out[3]: PublishOutcome(ok=True, ...)
```

## Architecture

```
rabbitkit/
  core/                # Business logic -- ZERO transport imports
  sync/                # pika adapter (AMQP 0-9-1)
  async_/              # aio-pika adapter (AMQP 0-9-1)
  middleware/          # Exception, retry, compression, tracing, dedup,
                       #   circuit breaker, rate_limit, signing, timeout
  serialization/       # JSON, msgspec, pydantic, two-stage pipeline
  di/                  # Depends, Header, Path, Context, resolver
  highload/            # FlowController, BatchPublisher, BatchAcker
  concurrency.py       # SyncWorkerPool, AsyncWorkerPool
  testing/             # TestBroker, TestApp, fixtures
  rpc.py               # RPCClient, AsyncRPCClient
  dlq.py               # DLQInspector
  locking.py           # DistributedLock, RedisLock, LockMiddleware
  management.py        # RabbitManagementClient
  results/             # ResultBackend, RedisResultBackend, ResultMiddleware
  asyncapi/            # AsyncAPI 2.6.0 document generator
  dashboard/           # Starlette ASGI monitoring dashboard
  cli/                 # typer CLI (run, health, topology, shell)
  fastapi.py           # rabbitkit_lifespan
```

The shared core has zero transport dependencies. Sync and async adapters are thin I/O layers. This makes it straightforward to add new transport backends (e.g., AMQP 1.0) without touching business logic.

## Compatibility

- **Python**: >= 3.11 (3.11, 3.12, 3.13)
- **RabbitMQ**: >= 3.12
- **pika**: >= 1.3, < 2.0
- **aio-pika**: >= 9.0, < 10.0

## License

MIT
