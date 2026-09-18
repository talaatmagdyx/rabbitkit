# Roadmap

This page describes what was built in each release and what is planned for future releases. The `[Unreleased]` section of `CHANGELOG.md` contains the most up-to-date view of in-progress work.

---

## Released

### v0.1 — Core

Foundation: configuration, transport, and routing.

- `RabbitConfig` — composable configuration dataclasses, frozen and slot-based
- `AsyncBroker` and `SyncBroker` — async (aio-pika) and sync (pika) broker implementations
- `@subscriber` and `@publisher` decorators — declarative routing and handler registration
- Basic ack policies — `AUTO` (ack on success, nack on exception)
- Topology declaration — exchange and queue declared from `RabbitExchange` / `RabbitQueue` at startup

### v0.2 — Reliability

Production-grade message safety.

- `RetryConfig` — configurable max retries and per-attempt delay sequence
- Dead-letter queue support — automatic DLQ topology declaration and routing
- Publisher confirms — all retry and DLQ publishes confirmed before original ack
- `AckPolicy.MANUAL` — full settlement control in handler code
- `AckPolicy.NACK_ON_ERROR` — explicit nack with requeue on handler exception

### v0.3 — Testing

In-process testing without a running RabbitMQ.

- `TestBroker` — in-memory broker that routes through the real handler pipeline
- In-memory routing — publish and subscribe wired together without transport layer
- pytest fixtures — `test_broker` fixture for use in test functions and conftest

### v0.4 — Production Middleware and Observability

Middleware layer and Kubernetes lifecycle.

- Middleware base class — composable pre/post handler hooks
- `DeduplicationMiddleware` — exactly-once delivery via Redis-backed nonce store
- `RateLimitMiddleware` — per-consumer rate limiting with token bucket
- `CompressionMiddleware` — gzip/zstd/lz4 transparent compression and decompression
- `SigningMiddleware` — HMAC-SHA256 message signing and verification
- `MetricsMiddleware` — Prometheus counters and histograms for handler latency and outcomes
- `TimeoutMiddleware` — per-handler deadline with configurable action on expiry
- Health checks — `broker_health_check()` and `broker_health_check_async()` for liveness and readiness
- Kubernetes lifecycle — graceful shutdown on SIGTERM, drain period, readiness gating

### v0.5 — RabbitMQ Operations

CLI tooling, API ergonomics, and operational observability.

- CLI — `rabbitkit dlq inspect/replay`, `rabbitkit health liveness/readiness`, `rabbitkit topology list/validate/diff/apply`, `rabbitkit routes list/describe`
- `topology validate` / `topology diff` — compare declared topology against live RabbitMQ via management API
- `topology apply` — declare all registered queues and exchanges via AMQP (with `--dry-run`)
- `rabbitkit.aio` — clean alias for `rabbitkit.async_`; both paths are identical and supported
- `rabbitkit.experimental` — groups RPC, locking, signing, streams, result backends, and dashboard under one import namespace
- Simple publish API — `broker.publish(routing_key=..., body=..., headers=...)` without requiring `MessageEnvelope`
- `DeduplicationConfig(mark_policy=)` — `"on_success"` (default, safer for retries) or `"on_start"` (prevents concurrent duplicates)
- Benchmarks — real RabbitMQ (testcontainers) for throughput, latency, failure, resource, sync, lifecycle; result persistence with regression comparison
- Production examples — `publish_message`, `pydantic_validation`, `publisher_confirms`, `kubernetes_worker` (with K8s manifest, health server, SIGTERM drain)
- Dashboard — web UI for live queue and consumer state inspection (experimental)
- Management client — programmatic access to the RabbitMQ Management HTTP API
- Stream queue support — `StreamOffset`, `StreamConsumerConfig`, `StreamOffsetType` for RabbitMQ 3.9+ streams

---

### v0.12 — Reliability & bulk operations

Bulk publishing with per-item outcomes, selected acknowledgement, safe ack
coalescing, reliability profiles, sanitized terminal metadata, bounded
retry-handoff failure handling.

- `publish_many` / `iter_publish` — bounded admission (count, bytes, time), one outcome per input, `UNKNOWN` preserved
- `ack_many` / `nack_many` — per-delivery settlement with stale-channel detection
- `SettlementCoordinator` + `CoalescingAcker` — property-tested safe coalescing
- `standard` / `critical` profiles, `preflight`, `policy_templates`, `put_policy`
- `ErrorSanitizer`, `RetryConfig.delay_queue_type` / `dlq_queue_type` / `handoff`
- Lifecycle gauges and confirm-latency histogram finally emitted

## Planned

### v1.0 — Stable API

API freeze, full documentation, full type coverage, and migration support.

- Stable API freeze — all symbols listed in the stability policy are frozen; semver guarantees apply from this release
- Full documentation — complete docs for all stable APIs, guides for common patterns, and architecture reference
- Full type coverage — `mypy --strict` clean across the entire public API surface
- Migration guide — documented upgrade path from 0.x to 1.0 for any breaking changes introduced during the 0.x stabilization period

### Under consideration (unscheduled)

- **Batch-handler consumption** (plan §8.4) — a handler receiving N
  deliveries at once with per-item success/retry/terminal intents, built on
  the settlement coordinator. Needs prefetch/batch-size coordination so a
  batch can never deadlock behind a smaller prefetch.
- **Pipeline-wired coalescing** — auto-registering every delivery with a
  `CoalescingAcker` so coalescing needs no application code. Blocked on
  guaranteeing channel-wide ownership from inside the pipeline.
- **Auto-stop on exhausted retry handoff** — today `on_handoff_exhausted` is
  a hook; stopping the consumer and closing its channel automatically needs
  supervisor backoff to avoid restart storms.
- **Multi-node quorum fault matrix & benchmarks** for the bulk paths
  (three-node cluster, leader loss, destination unavailable at TTL expiry).

- **SASL `EXTERNAL` authentication (x509 certificate auth)** — for deployments
  that authenticate clients by their TLS client certificate instead of
  username/password. `SecurityConfig(mechanism=...)` currently fails fast for
  anything other than `PLAIN` (deliberately — a silent fallback would be
  worse). Both pika (`pika.credentials.ExternalCredentials`) and aio-pika
  support it, so this is plumbing plus tests, prioritized on demand — open an
  issue if your deployment needs it.

---

## Not on the Roadmap

The following are out of scope for RabbitKit's intended design:

- Multi-broker support (Kafka, NATS, Redis Streams) — use FastStream for multi-broker workloads
- A general-purpose task scheduler — use Celery Beat for cron-style task scheduling
- A result backend as a primary feature — Celery's result backend is more mature; RabbitKit's result backend support remains experimental and scoped to RPC use cases
