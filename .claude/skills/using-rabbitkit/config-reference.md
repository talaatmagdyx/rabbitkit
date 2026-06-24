# rabbitkit configuration reference (complete)

Every config object, every field, every default — sourced from `src/rabbitkit/core/config.py` and the middleware modules. Config is **identical for sync and async**; only the broker class and a few wiring calls differ (see "Sync vs Async" at the end).

## What `RabbitConfig` composes

`RabbitConfig` holds only connection/broker defaults. All sub-configs are optional with sensible defaults.

| Field | Type | Default |
|---|---|---|
| `connection` | `ConnectionConfig` | `ConnectionConfig()` |
| `socket` | `SocketConfig` | `SocketConfig()` |
| `security` | `SecurityConfig` | `SecurityConfig()` |
| `publisher` | `PublisherConfig` | `PublisherConfig()` |
| `consumer` | `ConsumerConfig` | `ConsumerConfig()` |
| `pool` | `PoolConfig` | `PoolConfig()` |
| `topology_mode` | `TopologyMode` | `AUTO_DECLARE` |
| `retry` | `RetryConfig \| None` | `None` (no retry middleware) |
| `compression` | `CompressionConfig \| None` | `None` (no compression) |
| `logging` | `LoggingConfig \| None` | `None` (logging not configured) |

**NOT part of `RabbitConfig`** — passed to the component that uses it:
`WorkerConfig` → `broker.start(worker_config=)` · `MetricsConfig` → metrics middleware · `HealthCheckConfig` → `broker_health_check()` · `DeduplicationConfig`/`RateLimitConfig`/`TimeoutConfig`/`SigningConfig`/`CompressionConfig` → their middleware · `BackpressureConfig` → `FlowController` · `BatchPublishConfig`/`BatchAckConfig` → `BatchPublisher`/`BatchAcker`.

## Connection & transport

### ConnectionConfig
| Field | Default | Notes |
|---|---|---|
| `host` | `"localhost"` | use `127.0.0.1` to avoid IPv6 `::1` vs IPv4-docker resets |
| `port` | `5672` | `5671` for AMQPS |
| `username` / `password` | `"guest"` / `"guest"` | |
| `vhost` | `"/"` | |
| `heartbeat` | `30` | dead-peer detection in ~2 missed beats |
| `socket_timeout` | `10.0` | TCP connect/op timeout |
| `blocked_connection_timeout` | `300.0` | give up if broker holds us blocked (mem/disk alarm) |
| `connection_name` | `None` | shows in the mgmt UI — set it |
| `reconnect_backoff_base` | `1.0` | exponential reconnect backoff base |
| `reconnect_backoff_max` | `30.0` | backoff ceiling |

Also: `ConnectionConfig.from_url("amqp://user:pass@host:port/vhost?heartbeat=30&connection_timeout=10&blocked_connection_timeout=300")` and the `.url` property.

### SocketConfig (best-effort TCP tuning)
| Field | Default |
|---|---|
| `tcp_nodelay` | `True` |
| `tcp_keepidle` | `10` |
| `tcp_keepintvl` | `5` |
| `tcp_keepcnt` | `3` |
| `tcp_sndbuf` | `196608` (192 KB) |
| `tcp_rcvbuf` | `196608` (192 KB) |

### SSLConfig / SecurityConfig
`SSLConfig`: `enabled=False`, `certfile=None`, `keyfile=None`, `ca_certs=None`, `cert_reqs="CERT_REQUIRED"`, `server_hostname=None`.
`SecurityConfig`: `mechanism="PLAIN"`, `ssl=SSLConfig()`.

## Publisher / Consumer / Pool

### PublisherConfig
| Field | Default | Notes |
|---|---|---|
| `exchange` | `""` | default exchange |
| `confirm_delivery` | `True` | publisher confirms — keep on for no-loss |
| `confirm_timeout` | `5.0` | |
| `mandatory` | `False` | return unroutable messages |
| `persistent` | `True` | `delivery_mode=2` — survives broker restart |

### ConsumerConfig
| Field | Default | Notes |
|---|---|---|
| `prefetch_count` | `10` | **async: this is your concurrency knob** |
| `graceful_timeout` | `30.0` | drain time on shutdown |

### PoolConfig
| Field | Default | Notes |
|---|---|---|
| `channel_pool_size` | `10` | publisher channel pool (active) |
| `channel_acquire_timeout` | `10.0` | wait for a pooled channel |
| `publisher_connections` | `1` | **reserved** — one conn per role; scale by processes |
| `consumer_connections` | `1` | **reserved** — same |

## Reliability

### RetryConfig (broker default `RabbitConfig.retry=`, or per-route `retry=`)
| Field | Default | Notes |
|---|---|---|
| `max_retries` | `4` | `>= 0`; warns if `len(delays) < max_retries` |
| `delays` | `(5, 30, 120, 600)` | seconds per attempt (TTL of each delay queue) |
| `retry_header` | `"x-rabbitkit-retry-count"` | |
| `jitter_factor` | `0.1` | ±10% jitter on delays |
| `dead_letter_exchange` | `""` | |
| `per_queue` | `True` | isolated `{queue}.retry.N` + `{queue}.dlq` per source queue |
| `unknown_policy` | `ErrorSeverity.PERMANENT` | how unclassified errors are treated |

`RETRY_DISABLED` (singleton) removes retry from a route entirely — distinct from `RetryConfig(max_retries=0)` (immediate DLQ on a classified error).

### CompressionConfig (→ `CompressionMiddleware`)
`algorithm="gzip"` (`"zstd"` needs `rabbitkit[compression]`), `threshold=1024` (bytes), `level=6`.

### HealthCheckConfig (→ `broker_health_check()`)
`pending_threshold=100`.

## Observability

### LoggingConfig (`RabbitConfig.logging=`)
| Field | Default | Notes |
|---|---|---|
| `render_json` | `False` | `True` = JSON lines for aggregators (prod) |
| `add_log_level` | `True` | |
| `timestamper_fmt` | `"iso"` | |
| `include_caller_info` | `False` | dev convenience |

### MetricsConfig (→ metrics middleware)
`namespace="rabbitkit"`, plus optional name overrides `consumed_counter`/`processing_histogram`/`published_counter`/`publish_histogram` (empty → derived from namespace).

## Concurrency & high-load

### WorkerConfig (→ `broker.start(worker_config=)`)
| Field | Default | Notes |
|---|---|---|
| `worker_count` | `1` | `1` = handler runs inline (fastest for light work) |
| `prefetch_per_worker` | `None` | |
| `stop_timeout` | `30.0` | |

### BackpressureConfig (→ `FlowController`)
`max_in_flight=1000`, `rate_limit=None`, `blocked_timeout=60.0`, `on_blocked="wait"` (`"wait"`/`"raise"`/`"drop"`), `poll_interval_ms=10`.

### BatchPublishConfig / BatchAckConfig
`BatchPublishConfig`: `batch_size=100`, `flush_interval_ms=50`, `max_in_flight=1000`.
`BatchAckConfig`: `batch_size=100`, `flush_interval_ms=200`.

## Middleware configs (in their middleware modules, not core/config.py)

### DeduplicationConfig (→ `DeduplicationMiddleware`, needs Redis)
`key_prefix="rabbitkit:dedup"`, `ttl=86400`, `fallback_on_redis_error=True`, `key_source="message_id"` (`"message_id"`/`"correlation_id"`/`"body_hash"`).

### RateLimitConfig (→ `RateLimitMiddleware`)
`max_rate` (**required**, msgs/s), `burst=1`, `on_limited="wait"` (`"wait"`/`"nack"`/`"drop"`).

### TimeoutConfig (→ `TimeoutMiddleware`)
`timeout_seconds=30.0`. Raises `HandlerTimeoutError` (TRANSIENT → retried). **Sync caveat:** a sync timeout can't kill a running handler thread — it abandons it.

### SigningConfig (→ `SigningMiddleware`)
`secret_key` (**required**), `algorithm="hmac-sha256"` (or `"hmac-sha512"`), `header_name="x-rabbitkit-signature"`, `reject_unsigned=False`, `reject_invalid=True`.

## Enums

- `AckPolicy`: `AUTO` · `MANUAL` · `NACK_ON_ERROR` · `ACK_FIRST`
- `TopologyMode`: `AUTO_DECLARE` (declare on start) · `PASSIVE_ONLY` (verify only) · `MANUAL` (skip)
- `ErrorSeverity`: `TRANSIENT` (retry) · `PERMANENT` (DLQ)
- `ExchangeType`: `DIRECT` · `FANOUT` · `TOPIC` · `HEADERS`
- `QueueType`: `CLASSIC` · `QUORUM` · `STREAM`

## The complete RabbitConfig (every field set) — shared by sync & async

```python
from rabbitkit import (
    RabbitConfig, ConnectionConfig, SocketConfig, SecurityConfig, SSLConfig,
    PublisherConfig, ConsumerConfig, PoolConfig, RetryConfig, CompressionConfig, LoggingConfig,
)
from rabbitkit.core.types import TopologyMode, ErrorSeverity

CONFIG = RabbitConfig(
    connection=ConnectionConfig(
        host="127.0.0.1", port=5672, vhost="/", username="guest", password="guest",
        heartbeat=30, socket_timeout=10.0, blocked_connection_timeout=300.0,
        reconnect_backoff_base=1.0, reconnect_backoff_max=30.0,
        connection_name="my-service@host",
    ),
    socket=SocketConfig(tcp_nodelay=True, tcp_keepidle=10, tcp_keepintvl=5,
                        tcp_keepcnt=3, tcp_sndbuf=196608, tcp_rcvbuf=196608),
    security=SecurityConfig(
        mechanism="PLAIN",
        ssl=SSLConfig(enabled=False, ca_certs=None, certfile=None, keyfile=None,
                      cert_reqs="CERT_REQUIRED", server_hostname=None),
    ),
    publisher=PublisherConfig(confirm_delivery=True, confirm_timeout=5.0,
                              persistent=True, mandatory=False),
    consumer=ConsumerConfig(prefetch_count=64, graceful_timeout=30.0),
    pool=PoolConfig(channel_pool_size=64, channel_acquire_timeout=10.0),
    retry=RetryConfig(max_retries=4, delays=(5, 30, 120, 600), jitter_factor=0.1,
                      per_queue=True, unknown_policy=ErrorSeverity.PERMANENT),
    compression=CompressionConfig(algorithm="zstd", threshold=2048, level=6),
    logging=LoggingConfig(render_json=True, add_log_level=True, timestamper_fmt="iso"),
    topology_mode=TopologyMode.AUTO_DECLARE,   # PASSIVE_ONLY in production
)
```

## Sync vs Async — the only differences

The `CONFIG` above is shared. What changes between transports:

| Concern | Sync (pika) | Async (aio-pika) |
|---|---|---|
| Install | `rabbitkit[sync]` | `rabbitkit[async]` |
| Broker | `from rabbitkit.sync import SyncBroker` | `from rabbitkit.async_ import AsyncBroker` |
| Handler | `def handle(...)` | `async def handle(...)` |
| Publish | `broker.publish(env)` | `await broker.publish(env)` |
| Start/stop | `broker.start(...)` / `broker.stop()` | `await broker.start(...)` / `await broker.stop()` |
| **RetryMiddleware wiring** | `RetryMiddleware(CONFIG.retry, publish_fn=broker.publish)` | `RetryMiddleware(CONFIG.retry, publish_async_fn=broker.publish)` |
| Concurrency | `SyncWorkerPool` / `WorkerConfig(worker_count=N)` (threads) | `prefetch_count` drives it; `AsyncWorkerPool` for semaphore-bound tasks |
| Dedup Redis client | `redis.from_url(...)` | `redis.asyncio.from_url(...)` |
| Long-running consumer | `broker.run(worker_config=...)` (blocks) | `await broker.start(...)` then keep the loop alive, or `await RabbitApp.run_async()` (signal-driven start/wait/stop) |
| Timeout middleware | can't kill the handler thread — abandons it (logs a warning) | cancels the task cleanly |

**Runnable, fully-wired apps** (every config + the full middleware stack) live at
`examples/full_config/sync_app.py` and `examples/full_config/async_app.py`. Read those for the complete consume-side middleware ordering (tracing → exception → circuit-breaker → dedup → retry → timeout → rate-limit).
