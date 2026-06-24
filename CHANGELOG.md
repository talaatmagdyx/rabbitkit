# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Async consumer was not resumed after a broker reconnect (silent stall)**
  - `AsyncTransportImpl.consume()` used `channel.get_queue(name, ensure=False)`, which returns a queue that aio-pika's `RobustChannel` does **not** track for restoration — only `declare_queue` populates the channel's `_queues` restore set. So after a `connect_robust` reconnect (broker restart / network drop) the connection recovered but the consumer subscription was never re-established: the consumer silently stopped receiving while publishes kept working. Reproduced by `examples/header_inspector/chaos_reconnect.py` — 99/100 published but only 44/100 consumed after a restart.
  - Fixed: `consume()` now does `declare_queue(name, passive=True)`, registering the queue (and its consumer) for robust restoration, so the consumer resumes after a reconnect. Verified: 400/400 consumed across a mid-drain restart.

- **`ConnectionConfig.heartbeat` was ignored on the async transport**
  - `make_aio_pika_connect_kwargs()` passed only `url` + `timeout` to `connect_robust`, dropping the configured heartbeat on async (the sync transport already honored it); aio-pika fell back to its negotiated default.
  - Fixed: heartbeat is carried on the connection URL (`?heartbeat=N`), and `reconnect_backoff_base` is mapped to `connect_robust(reconnect_interval=...)`.

- **`Path()` dependency injection never worked — `message.path` was never populated**
  - No broker filled `message.path`, so `Path("name")` always raised `KeyError` in real use (the resolver unit tests pre-set `path` and masked it). There was also no way to *name* a routing-key segment.
  - Fixed: routes may now name a single-word segment with `{name}` (e.g. `routing_key="events.{level}.#"`), which binds to AMQP as `*`; on each delivery the sync, async, and Test brokers extract the named segments into `message.path` (new `core/path.py`). `Path()`, like every DI marker, still requires an explicit `di_resolver=DIResolver()`.

- **AMQP `timestamp` not round-tripped**
  - The async publish path never set the message timestamp (sync did), and **neither** transport surfaced `properties.timestamp` on consume — `message.timestamp` was always `None` for consumers.
  - Fixed: async publish sends `envelope.timestamp`; both transports populate `message.timestamp` on consume (sync converts pika's Unix int to a tz-aware `datetime`).

- **Async per-message TTL (`expiration`) was 1,000,000× too long**
  - `MessageEnvelope.expiration` is milliseconds, but the async publish did `int(expiration) * 1000` and aio-pika's `Message.expiration` is in **seconds** — so `"60000"` (60 s) became 60,000,000 s (~1.9 years), and it disagreed with the sync transport (which passes the ms string straight to pika, correctly).
  - Fixed: async now passes `int(expiration) / 1000` (ms → seconds), matching sync on the wire.

### Testing

- `benchmarks/chaos_suite.py`: the "restart mid-consume" scenario was a **false positive** — its `sleep(0.01)` handler drained the queue before the 1.5 s restart fired, so consumer recovery was never exercised. The handler is now slow enough that the restart lands mid-drain, with a guard (`0 < at_restart < n`) that fails if it doesn't; the "restart mid-publish" resend budget was raised to outlast a full reboot; the port is overridable via `RK_CHAOS_PORT`. Now **6 scenarios** with full sync/async parity — each failure mode (restart mid-consume, transient→retry→DLQ, restart mid-publish) is exercised on **both** transports, all asserting zero loss.
- New `examples/header_inspector/chaos_reconnect.py` — chaos test for the reconnect + consumer-recovery + resend path (100 messages, hard restart + freeze, asserts zero loss).

## [0.7.0] — 2026-06-23

### Fixed

#### Critical Bug Fixes

- **C1 — Sync multi-worker corrupted the connection (pika is not thread-safe)**
  - With `worker_count > 1`, handlers run in a `ThreadPoolExecutor`; their `basic_ack` / `basic_nack` / `basic_reject` / `basic_publish` calls executed on worker threads against the single shared channel, racing the I/O loop and corrupting frames.
  - Fixed: `SyncTransport._run_on_io_thread()` marshals every channel operation onto the connection-owning thread via `connection.add_callback_threadsafe()` and blocks for the result/exception. When already on the owner thread (single-worker or publisher), or when no consume loop is running, the call runs inline to avoid self-deadlock.

- **C2 — Failed retry / result publish acked the source → permanent message loss**
  - `RetryMiddleware._route_to_delay_queue_{sync,async}` ignored the publish outcome and acked the source even when the delay-queue publish failed; the pipeline did the same for result publishing. Since sync/async `publish()` swallow exceptions and return `PublishStatus.ERROR`, a transient failure dropped the message entirely (never retried, never dead-lettered).
  - Fixed: both paths check `outcome.ok` and `nack(requeue=True)` instead of acking when the publish failed, so the broker redelivers.

- **C3 — Subscriber middlewares were never executed**
  - `@subscriber(middlewares=[...])` stored `route_middlewares` on the route, but `HandlerPipeline` never composed them and never called `consume_scope` — so `RetryMiddleware`, `DeduplicationMiddleware`, `CircuitBreakerMiddleware`, `TimeoutMiddleware`, `RateLimitMiddleware`, tracing, etc. silently did nothing when wired the documented way. There was no delayed-retry-with-backoff via the subscriber API.
  - Fixed: `HandlerPipeline._run_consume_sync` / `_run_consume_async` now run each middleware's `on_receive` hook and compose the `consume_scope` / `consume_scope_async` chain outer→inner around the handler (first middleware in the list is outermost). Covered by `tests/unit/core/test_pipeline_middleware.py`.

- **C4 — A route's publish-side middlewares were ignored on result publishing**
  - When a handler returned a result, `_publish_result_*` published it directly, bypassing the route's `publish_scope` middlewares (signing, tracing, etc.).
  - Fixed: `HandlerPipeline` now composes the route's `publish_scope` / `publish_scope_async` chain around result publishing. (Standalone producer publishes via `broker.publish` are not route-scoped; apply publish middlewares manually — a broker-level publish-middleware API is a future enhancement.)

#### High Severity Bug Fixes

- **H1 — Sync consumer had no connection recovery**
  - `SyncBroker.run()` called `start_consuming()` once; pika's `BlockingConnection` does not auto-recover, and `_ensure_connected()` only ran before publishing, so a single connection blip silently killed the consumer (no re-declare, no re-subscribe).
  - Fixed: `run()` wraps the consume loop in a recovery loop that, on connection errors, reconnects (`SyncTransport.reconnect()`), re-declares topology, and re-subscribes all consumers. (Async already recovered via `connect_robust`.)

#### Medium Severity Bug Fixes

- **M1 — `SyncWorkerPool._futures` rebuilt the whole list on every message**
  - `submit()` ran `self._futures = [f for f in self._futures if not f.done()]` per message — O(n) in in-flight tasks under the lock.
  - Fixed: `_futures` is now a `set` with O(1) `add` and a `future.add_done_callback` that discards on completion (mirrors `AsyncWorkerPool`).

## [0.6.1] — 2026-04-15

### Fixed

#### Critical Bug Fixes

- **C1 — Async backpressure `on_blocked="wait"` deadlocked event loop**
  - `FlowController.acquire_async()` previously used `threading.Event.wait()` inside the async path, blocking the event loop for the entire blocked duration.
  - Fixed: rewrote `acquire_async()` with `asyncio.Event` and `asyncio.wait_for()` so the coroutine suspends correctly without blocking.
  - Added `_AsyncTokenBucket` — async-native token-bucket rate limiter using `asyncio.Lock` instead of `threading.Lock`.

- **C2 — `CircuitBreakerMiddleware` silently skipped async handlers when given a sync CB**
  - `publish_scope_async()` / `consume_scope_async()` with only a sync `circuit_breaker=` set would call `cb.call(call_next, envelope)` and return the unawaited coroutine, silently doing nothing.
  - Fixed: both async scope methods now raise `TypeError` immediately with a helpful message pointing to `async_circuit_breaker=` / `async_publish_circuit_breaker=`.
  - **Breaking change**: previously silent misconfiguration now raises `TypeError` at runtime.

- **C3 — RPC `_ensure_consuming()` was not atomic — race with concurrent callers**
  - `AsyncRPCClient._ensure_consuming()` and `RPCClient._ensure_consuming()` performed a check-then-act outside their respective locks, allowing two concurrent callers to both pass the `_consuming` guard and register duplicate consumers.
  - Fixed: the entire body of `_ensure_consuming()` is now inside `async with self._lock:` (async) and `with self._lock:` (sync).

#### High Severity Bug Fixes

- **H1 — Single aio-pika connection caused head-of-line blocking under concurrency**
  - Publisher and consumer shared one connection; slow consumers blocked publishers, and concurrent publishes raced on the single channel.
  - Fixed: `AsyncConnectionPool` now creates separate publisher and consumer connections. Publishers use `AsyncChannelPool` for concurrent channel access.

- **H2 — Channel pool `acquire()` could block forever on exhaustion**
  - `asyncio.Queue.get()` had no timeout; exhausted pools would hang indefinitely, eventually causing cascading timeouts.
  - Fixed: `acquire()` calls `asyncio.wait_for(self._pool.get(), timeout=self._acquire_timeout)`. `PoolConfig.channel_acquire_timeout` (default 10 s) is forwarded to `AsyncChannelPool`.

- **H3 — DLQ never received terminal rejections from retry middleware**
  - Source queues were declared without `x-dead-letter-exchange` / `x-dead-letter-routing-key` arguments, so RabbitMQ discarded nacked/rejected messages instead of routing them to the DLQ.
  - Fixed: `_declare_topology()` in both brokers now uses `dataclasses.replace()` to re-declare source queues with the correct dead-letter arguments when retry is enabled.

- **H4 — `SyncWorkerPool._futures` list was not thread-safe**
  - The `_futures` list was accessed from multiple threads in `submit()`, `pending_count`, and `stop()` without a lock, risking list corruption under high concurrency.
  - Fixed: `threading.Lock` protects all access to `_futures`.

#### Medium Severity Bug Fixes

- **M1 — `FlowController` async rate limiter used `threading.Lock` inside the event loop**
  - `acquire_async()` would call `self._rate_limiter.wait()` which internally used `threading.Lock` and `time.sleep()`, blocking the event loop.
  - Fixed: `_AsyncTokenBucket` with `asyncio.Lock` and `asyncio.sleep()` is used for the async path.

- **M2 — DI generator cleanup exceptions were silently swallowed**
  - When a generator-based dependency's cleanup phase raised an exception, it was caught but never logged, making cleanup failures invisible.
  - Fixed: exceptions during `scope.cleanup()` / `scope.cleanup_async()` are now logged at ERROR level with full traceback.

- **M3 — `RetryConfig` with `len(delays) < max_retries` silently reused last delay**
  - No validation prevented misconfiguration where extra retries would silently reuse the last delay value.
  - Fixed: `RetryConfig.__post_init__` issues `UserWarning` (not `ValueError` — preserving backward compatibility) when delays tuple is shorter than max_retries.

- **M4 — Each new consumer `consume()` call shared the topology channel**
  - All consumers used the same channel, so `set_qos()` calls from one consumer overrode the prefetch of others.
  - Fixed: each `consume()` creates a dedicated channel; `set_qos` is isolated per consumer.

- **M5 — `AsyncRPCClient._futures` dict lacked lock protection**
  - Concurrent `call()` invocations could race when inserting/deleting from `_futures` alongside the message callback.
  - Fixed: all `_futures` operations now hold `self._lock`.

### Added

- **WorkerConfig pool-size validation**: `broker.start()` now emits `RuntimeWarning` when `worker_count > channel_pool_size` to prevent silent deadlocks under load.

### Changed

- `CircuitBreakerMiddleware.publish_scope_async()` and `consume_scope_async()` now raise `TypeError` (instead of silently returning unawaited coroutines) when only a sync circuit breaker is configured. **Breaking change for misconfigured setups.**

---

## [0.6.0] — 2026-04-15

### Added

#### Subscriber Filtering (F1)
- `filter_fn` parameter on `@subscriber()` — reject messages before deserialization
- Filtered messages are nacked with `requeue=False` (no further processing)
- Works with all broker types: `SyncBroker`, `AsyncBroker`, `TestBroker`, `RabbitRouter`
- Zero overhead when `filter_fn=None` (default)

#### Structured Logging (F2)
- `LoggingConfig` — configure structlog integration with per-message context binding
- `configure_structlog()` — one-time setup; call at app startup or via `RabbitConfig.logging`
- Per-message context: `message_id`, `routing_key`, `queue`, `handler` bound via `structlog.contextvars`
- `render_json=False` for development console output; `render_json=True` for production JSON logs
- Context automatically cleared after each message via `finally` block

#### Environment-based Configuration (F3)
- `RabbitSettings` — pydantic-settings model reading from `RABBITMQ_*` environment variables
- Covers all 13 connection parameters: host, port, user, password, vhost, heartbeat, socket_timeout, prefetch_count, confirm_delivery, channel_pool_size, topology_mode
- `to_rabbit_config()` — convert to `RabbitConfig` for broker construction
- Optional dependency: `pip install rabbitkit[settings]` (pydantic-settings)
- Graceful `ImportError` when pydantic-settings not installed

#### RPC Shorthand (F4)
- `AsyncBroker.request()` — one-call async RPC (publish + await reply)
- `SyncBroker.request()` — one-call sync RPC
- Lazily initialises `AsyncRPCClient` / `RPCClient` on first use
- Accepts `routing_key`, `body`, `timeout`, `exchange`, `headers`
- RPC client torn down cleanly in `broker.stop()`

#### Rate Limiting Middleware (F5)
- `RateLimitMiddleware` — token-bucket rate limiter for consume and publish operations
- `RateLimitConfig(max_rate, burst, on_limited)` — configures msgs/sec, burst capacity, and action
- `on_limited="wait"` — blocks until a token is available (default)
- `on_limited="nack"` — nacks the message immediately when rate exceeded
- `on_limited="drop"` — silently drops (acks) the message when rate exceeded
- Thread-safe token bucket; async sleep for `"wait"` in async context

#### Message Signing Middleware (F6)
- `SigningMiddleware` — HMAC-based message signing and verification
- `SigningConfig(secret_key, algorithm, header_name, reject_unsigned, reject_invalid)`
- Supported algorithms: `hmac-sha256` (default), `hmac-sha512`
- Publish: computes HMAC of body and injects signature header
- Consume: verifies signature using `hmac.compare_digest` (timing-safe)
- `reject_unsigned=True` nacks messages with no signature header
- `reject_invalid=True` (default) nacks messages with invalid signature

#### Handler Timeout Middleware (F7)
- `TimeoutMiddleware` — per-message processing deadline enforcement
- `TimeoutConfig(timeout_seconds, on_timeout)` — configures deadline and action on breach
- Async: `asyncio.wait_for` — cancels coroutine precisely at deadline
- Sync: `threading.Thread` join with timeout — handler thread continues but message is settled
- `on_timeout`: `"nack"` (default), `"reject"`, or `"ack"`

#### CLI Tooling (F8)
- `rabbitkit run <app_path>` — start a broker from `module:attribute` path
- `rabbitkit health <app_path>` — print broker health status and exit with appropriate code
- `rabbitkit topology <app_path>` — list registered routes in table or JSON format
- `rabbitkit shell <app_path>` — open interactive Python shell with broker pre-loaded (F17)
- `rabbitkit docs generate <app_path>` — generate AsyncAPI JSON spec to stdout
- Optional dependency: `pip install rabbitkit[cli]` (typer)

#### Hot Reload (F9)
- `rabbitkit run --reload <app_path>` — restart broker subprocess on `.py` file changes
- `--reload-ext` — additional file extensions to watch (e.g. `--reload-ext .yml`)
- `--workers N` — run N broker processes in parallel (multiprocessing)
- Optional dependency: `pip install rabbitkit[reload]` (watchfiles)

#### Distributed Locking Middleware (F14)
- `LockMiddleware` — acquire distributed lock before processing each message
- `DistributedLock` protocol — bring your own lock implementation
- `RedisLock` — SET NX EX implementation with UUID-based safe release
- `key_fn` parameter — custom lock key derivation from `RabbitMessage`
- On lock failure: nacks with `requeue=True` so another consumer can claim it

#### RabbitMQ Management API Client (F15)
- `RabbitManagementClient` — HTTP API client for RabbitMQ Management Plugin (port 15672)
- `ManagementConfig(url, username, password, timeout)` — connection configuration
- Sync operations (stdlib `urllib.request`, no extra deps): `list_queues`, `get_queue`, `purge_queue`, `list_exchanges`, `list_connections`, `list_channels`, `overview`, `health_check`, `delete_queue`
- Async variants via `aiohttp`: `list_queues_async`, `health_check_async`, `overview_async`
- Optional dependency: `pip install rabbitkit[management]` (aiohttp, async only; sync works without it)

#### AsyncAPI Documentation Generation (F10)
- `asyncapi/` package — generate AsyncAPI 2.6.0 specs from broker routes
- `generate_asyncapi_doc()` — returns spec as a JSON-serializable dict
- `generate_asyncapi_json()` — returns spec as a JSON string
- `AsyncAPIGeneratorConfig` — configure title, version, description, server URL
- `schema.py` — JSON Schema extraction from handler type annotations (primitives, dataclasses, Pydantic V2)
- AMQP channel bindings with queue/exchange metadata
- Publish operations for routes with `result_publisher`
- Re-exported in top-level `__init__.py`

#### Result Backends (F11)
- `ResultBackend` protocol — pluggable result storage for handler return values
- `RedisResultBackend` — Redis-based implementation using GET/SET with TTL
- `ResultMiddleware` — consume-scope middleware that stores handler results keyed by correlation_id
- Supports custom serializers and configurable TTL

#### Pydantic Auto-Validation (F12)
- Pipeline `_deserialize_body` now auto-validates decoded dicts against Pydantic models
- When serializer returns a dict and the handler's target type has `model_validate`, auto-validates
- Transparent passthrough for non-Pydantic types, already-constructed models, and bytes

#### Custom Serialization Pipeline (F13)
- `SerializationPipeline` — two-stage serialization (parser + decoder) conforming to Serializer protocol
- `MessageParser` / `MessageDecoder` protocols for composable stages
- `JsonParser` — built-in JSON parser stage
- `PydanticDecoder` — decoder using `model_validate` / `model_dump`
- `DataclassDecoder` — decoder for stdlib dataclasses
- `RawDecoder` — pass-through decoder (no transformation)

#### Monitoring Dashboard (F16)
- `dashboard/` package — ASGI dashboard application for monitoring rabbitkit brokers
- `create_dashboard_app()` — creates a Starlette app with HTML dashboard and JSON API endpoints
- `/` — HTML overview page with health status, route count, and route table
- `/api/health` — JSON health check endpoint
- `/api/routes` — JSON route listing endpoint
- Optional dependency: `pip install rabbitkit[dashboard]` (starlette + uvicorn)
- Re-exported `create_dashboard_app` in top-level `__init__.py`

#### Interactive Shell (F17)
- `cli/commands/shell.py` — `rabbitkit shell` command for interactive Python shell
- Pre-loads broker, routes, config, and publish into the shell namespace
- Uses IPython if available, falls back to stdlib `code.interact`
- Registered as `rabbitkit shell <app_path>` CLI command

### Changed
- Updated `__init__.py` exports with `create_dashboard_app`, `ManagementConfig`, `RabbitManagementClient`
- Added `dashboard` optional dependency group to `pyproject.toml`
- Updated `serialization/__init__.py` re-exports with pipeline classes

## [0.5.0] — 2026-03-10

### Added

#### Production Polish
- `py.typed` marker (PEP 561) — downstream consumers get inline type hints from rabbitkit
- `CLAUDE.md` — project-specific instructions for Claude Code
- GitHub Actions CI workflow — ruff + mypy + pytest across Python 3.11/3.12/3.13

#### Per-Route Prefetch
- `prefetch_count` parameter on `@subscriber()` — override global prefetch per route
- Supported on `SyncBroker`, `AsyncBroker`, `TestBroker`, and `RabbitRouter`
- Falls back to `ConsumerConfig.prefetch_count` when not set

#### Exchange-to-Exchange Bindings
- `bind_exchange()` method on `SyncTransport` and `AsyncTransport`
- Broker `_declare_topology()` now wires `RabbitExchange.bind_to` automatically
- Full protocol contract support (`Transport.bind_exchange`, `AsyncTransport.bind_exchange`)

#### Metrics Middleware
- `MetricsMiddleware` — protocol-based metrics collection for consume and publish operations
- `MetricsCollector` protocol — works with any metrics backend (Prometheus, StatsD, custom)
- `PrometheusCollector` — concrete implementation wrapping `prometheus_client` (optional import)
- Tracks: `rabbitkit_messages_consumed_total`, `rabbitkit_message_processing_seconds`, `rabbitkit_messages_published_total`, `rabbitkit_message_publish_seconds`
- No-op passthrough when collector is None (zero overhead)

### Changed
- Updated `__init__.py` exports with all new v0.5.0 public symbols
- Updated `middleware/__init__.py` re-exports with `MetricsMiddleware`, `MetricsCollector`, `PrometheusCollector`

## [0.4.0] — 2026-03-10

### Added

#### Broker Integration
- `SyncBroker.start(worker_config=)` — wire `SyncWorkerPool` into broker lifecycle
- `AsyncBroker.start(worker_config=)` — wire `AsyncWorkerPool` into broker lifecycle
- Worker pool stops gracefully before consumer cancellation on broker stop
- `prefetch_per_worker` dynamically scales prefetch count: `worker_count × prefetch_per_worker`
- `broker.worker_pool` property for accessing the pool (health checks, monitoring)

#### Health Checks
- `broker_health_check(broker)` — sync health check returning `BrokerHealthResult`
- `broker_health_check_async(broker)` — async variant for async health frameworks
- `HealthStatus` enum: HEALTHY / DEGRADED / UNHEALTHY
- `BrokerHealthResult` dataclass with started, connected, consumer_count, route_count, worker_pool_pending
- DEGRADED when consumers missing or worker pool backlog > 100

#### Stream Queues
- `StreamOffset` — offset specification for stream queue consumers (first/last/next/offset/timestamp)
- `StreamConsumerConfig` — stream-specific consumer configuration with offset and consumer name
- `StreamOffsetType` enum for all offset types
- `to_consume_arguments()` for RabbitMQ `x-stream-offset` / `x-stream-consumer-name`

#### Documentation
- Comprehensive README with full user guide, API reference, and configuration reference
- 615-line documentation covering all features, configuration, and usage patterns

#### Benchmarks
- `benchmarks/bench_pipeline.py` — pipeline throughput benchmarks using in-memory TestBroker
- Raw pipeline, JSON serialization, multiple routes, and result publishing scenarios
- Run via `python -m benchmarks`

### Changed
- Updated `__init__.py` exports with all new v0.4.0 public symbols
- Added `benchmarks/**/*.py` ruff per-file-ignore for T20 (print statements)

## [0.3.0] — 2026-03-10

### Added

#### Resilience
- `CircuitBreakerMiddleware` — wraps consume and publish operations with circuit breaker
- Supports separate consume/publish circuit breakers with sync and async variants
- `CircuitBreakerOpenError` for fail-fast rejection when circuit is open
- No-op passthrough when no circuit breaker is provided (lazy/optional pattern)

#### Dependency Injection
- Generator (yield-based) dependencies via `Depends()` — sync and async generators
- `DependencyScope` for tracking and cleaning up generator lifecycle
- Generators cleaned up in reverse order after handler completes (in `finally` block)
- `ConfigurationError` raised when async generators used in sync context

#### Consumer Concurrency
- `SyncWorkerPool` — ThreadPoolExecutor-based concurrent sync message processing
- `AsyncWorkerPool` — asyncio.Semaphore-based concurrent async message processing
- `WorkerConfig(worker_count=N, prefetch_per_worker=M)` configuration
- Single-worker mode runs inline (no pool overhead)
- Graceful stop with configurable timeout and task cancellation

#### Configuration
- `WorkerConfig` — consumer concurrency configuration dataclass

### Changed
- Updated `__init__.py` exports with all new v0.3.0 public symbols
- Updated `middleware/__init__.py` re-exports with `CircuitBreakerMiddleware`
- Full `mypy --strict` compliance across all 52 source files

## [0.2.0] — 2026-03-10

### Added

#### Observability
- `TracedConsumerMiddleware` — obskit tracing integration with lazy/no-op when obskit absent
- OpenTelemetry semantic attributes for consume and publish spans
- Trace context propagation (inject into outgoing headers, extract from incoming)

#### Middleware
- `DeduplicationMiddleware` — idempotent message processing via Redis SETNX
- Pluggable key extraction: `message_id`, `correlation_id`, `body_hash`, or custom `key_fn`
- Configurable TTL and fallback-on-Redis-error behaviour

#### High-Load Infrastructure
- `FlowController` — publish-side backpressure with connection.blocked handling
- Token-bucket rate limiter with configurable messages-per-second
- In-flight limit tracking with configurable on-blocked behaviour (wait/raise/drop)
- `BackpressureError` for immediate rejection mode
- `BatchPublisher` — buffer outgoing envelopes and flush as a batch
- `BatchAcker` — accumulate delivery tags for multi-ack with `multiple=True`

#### DLQ Inspector
- `DLQInspector` — peek, replay, and purge dead-letter queues
- `peek()` fetches messages as snapshots with requeue
- `replay()` with optional predicate filter and target queue override
- `purge()` delegates to transport for immediate queue drain
- Full async variants for all operations

#### FastAPI Integration
- `rabbitkit_lifespan()` async context manager for FastAPI lifespan
- Supports both sync and async brokers with duck-typed detection
- Proper start/stop ordering: app → broker on start, broker → app on stop

#### Protocol Extensions
- `SupportsBasicGet` / `AsyncSupportsBasicGet` — single-message fetch protocol
- `SupportsPurge` / `AsyncSupportsPurge` — queue purge protocol

#### Configuration
- `BackpressureConfig`, `BatchPublishConfig`, `BatchAckConfig`, `DeduplicationConfig` — new config dataclasses

### Changed
- Updated `__init__.py` exports with all new v0.2.0 public symbols
- Updated `middleware/__init__.py` and `highload/__init__.py` re-exports

## [0.1.0] — 2026-03-10

### Added

#### Core
- Configuration system with composable dataclasses (`RabbitConfig`, `ConnectionConfig`, `SecurityConfig`, `SSLConfig`, `SocketConfig`, `ConsumerConfig`, `PublisherConfig`, `PoolConfig`, `RetryConfig`, `CompressionConfig`)
- Rich message model (`RabbitMessage`) with runtime-aware ack/nack/reject and idempotent settlement
- Topology models (`RabbitExchange`, `RabbitQueue`) with validation and declaration builders
- Error classification engine (`classify_error`) with transient/permanent/unknown severity
- Transport protocol definitions for sync and async adapters
- Route definitions with per-route middleware, serializer, and ack policy overrides
- Subscriber registry with decorator-based handler registration
- Handler pipeline with middleware chain, DI resolution, result publishing, and ack orchestration
- Modular `RabbitRouter` with prefix support, shared defaults, and `include_router`
- `RabbitApp` lifecycle manager with startup/shutdown hooks, signal handling, and state tracking
- `TopologyMode`: AUTO_DECLARE, PASSIVE_ONLY, MANUAL

#### Serialization
- Built-in JSON serializer
- Optional msgspec serializer (`rabbitkit[msgspec]`)
- Optional pydantic support (`rabbitkit[pydantic]`)

#### Dependency Injection
- `Depends()` marker with factory callables and caching
- `Header()`, `Path()`, `Context()` parameter markers
- `DIResolver` with type-hint introspection and `use_cache` control

#### Middleware
- Base middleware protocol (sync + async)
- Exception handler middleware with configurable error callbacks
- Error classifier middleware (transient vs permanent routing)
- Retry middleware with delay queues (TTL + DLX), configurable backoff, jitter, and per-queue isolation
- Compression middleware (gzip built-in, zstd optional via `rabbitkit[compression]`)

#### Sync Transport
- pika connection helpers with SSL, socket tuning, and connection naming
- `SyncTransport` with publisher confirms, topology declaration, consume/cancel
- `SyncBroker` — high-level entry point wiring registry + pipeline + transport
- `SyncChannelPool` and `SyncConnectionPool` for connection reuse

#### Async Transport
- aio-pika connection helpers with `connect_robust` auto-reconnection
- `AsyncTransportImpl` with publisher confirms, topology, consume/cancel
- `AsyncBroker` — async high-level entry point mirroring SyncBroker
- `AsyncChannelPool` and `AsyncConnectionPool` with asyncio.Queue management

#### RPC
- `RPCClient` (sync) using `amq.rabbitmq.reply-to` direct reply-to
- `AsyncRPCClient` with asyncio.Future-based waiting
- Configurable timeout with `RPCTimeoutError`, max pending calls limit

#### Testing
- `TestBroker` — in-memory broker for unit testing (no RabbitMQ required)
- `TestApp` — lifecycle wrapper with context manager support
- Pytest fixtures (`test_broker`, `test_app`)
