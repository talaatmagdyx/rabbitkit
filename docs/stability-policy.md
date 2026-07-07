# Stability Policy

## Where rabbitkit actually is (as of 0.10.0)

rabbitkit is at `0.10.0`; `0.9.0` was the **first published release**
(public beta).
Development happened privately under internal milestone numbers — now the
`0.8.x` entries in `CHANGELOG.md` (each marked with its former number);
none were ever distributed, and the history was renumbered at publication
so the version signals beta maturity honestly.

What that number means in practice:

- **Stable Core** (below) has been through multiple structured
  production-readiness passes (see the `0.8.x` entries in
  `CHANGELOG.md` for the fix record) and is treated as frozen: breaking it
  requires the deprecation cycle described in this document, even before
  `1.0.0`.
- **Advanced Stable** and **Experimental** are explicitly *not* covered by
  that freeze — see their sections below for what guarantee (if any) each
  gets.
- **`1.0.0`** is reserved for the release where Stable Core's freeze
  graduates from policy to full SemVer guarantee. A later major is reserved
  for the point where *all three* tiers are simultaneously under full
  SemVer, if and when Advanced Stable and Experimental features individually
  earn it (see "Promotion criteria" below). Until then, treat the tier a
  symbol is in as the actual guarantee — not the version number alone.

---

## Stable Core

The following symbols will not be removed or changed in a backward-incompatible
way without a deprecation cycle (minimum one minor version of warning, per the
Deprecation Policy below). This is the small, deliberately-limited set the
README teaches — if you only use these, you're on the most-supported path.

| Symbol | Module |
|---|---|
| `RabbitConfig`, `ConnectionConfig`, `ConsumerConfig`, `PublisherConfig`, `RetryConfig`, `SecurityConfig`, `SSLConfig`, `SocketConfig`, `WorkerConfig`, `PoolConfig`, `HealthCheckConfig` | `rabbitkit` |
| `AsyncBroker`, `SyncBroker` | `rabbitkit` |
| `RabbitRouter`, `subscriber`, `publisher` | `rabbitkit` |
| `RabbitMessage`, `MessageEnvelope`, `AckMessage`, `NackMessage`, `RejectMessage` | `rabbitkit` |
| `AckPolicy`, `TopologyMode` | `rabbitkit` |
| `RabbitQueue`, `RabbitExchange` | `rabbitkit` |
| `RabbitApp` | `rabbitkit` |
| `rabbitkit_lifespan` (FastAPI integration) | `rabbitkit` |
| `Depends`, `Header`, `Path`, `Context` | `rabbitkit` |
| `TestBroker`, `TestApp` | `rabbitkit.testing` |
| Exception taxonomy: `ConfigurationError`, `ConfigValidationError`, `TopologyValidationError`, `UnsafeTopologyError`, `MessageTooLargeError`, `BrokerNotStartedError`, `SettlementError`, `DuplicateRouteError`, `MissingDependencyError`, `BackpressureError`, `PublishError` — including their builtin base classes (`ValueError`/`RuntimeError` dual inheritance is part of the contract, not an implementation detail) | `rabbitkit` |
| `broker_health_check`, `broker_health_check_async`, `broker_liveness`, `broker_liveness_async`, `broker_readiness`, `broker_readiness_async` | `rabbitkit` |
| `LoggingConfig`, `configure_structlog` | `rabbitkit` |
| Built-in serializers: JSON, msgspec, Pydantic support | `rabbitkit.serialization.*` |
| Always-on middlewares: retry, deduplication, timeout, compression, metrics, rate limiting | `rabbitkit.middleware.*` |

Stable means:

- The symbol exists at the listed import path.
- The constructor/call signature does not gain required parameters.
- Existing keyword arguments are not removed or renamed.
- Return types do not change in a breaking way.
- Sync and async twins (`SyncBroker`/`AsyncBroker`, etc.) expose the same
  public method names and semantics — checked by a dedicated parity test in
  the test suite, not just convention.

---

## Advanced Stable

Real, production-grade, tested — but adds real complexity, may depend on an
external package outside the core install, or is used deliberately rather
than by default. Breaking changes here get a deprecation cycle like Stable
Core, but the bar for *introducing* new complexity to this tier is lower, and
it is not part of the "understand rabbitkit in 10 minutes" story.

| Feature | Import path | Notes |
|---|---|---|
| Publish-side backpressure (`FlowController`, `BackpressureConfig`) | `rabbitkit` / `rabbitkit.highload.backpressure` | |
| Batch publishing/acking (`BatchPublisher`, `BatchAcker`, `AsyncBatchPublisher`) | `rabbitkit` / `rabbitkit.highload.batch` / `rabbitkit.async_.batch` | Buffering/timing amortization, not wire-level batching — see the high-load guide. |
| DLQ inspection (`DLQInspector`) + the CLI's `dlq` commands | `rabbitkit` | |
| RabbitMQ Management API client (`RabbitManagementClient`) | `rabbitkit` | Async methods require `rabbitkit[management]` (aiohttp). |
| CLI (`rabbitkit run`/`health`/`topology`/`routes`/`shell`) | `rabbitkit[cli]` | |
| `CircuitBreakerMiddleware` | `rabbitkit` | **Requires a `CircuitBreakerProtocol`-compatible implementation (e.g. pybreaker) to do anything** — it's a no-op passthrough without one. Exported at the top level for convenience, but treat the dependency as a real one before adopting it. |

---

## Experimental

The following features live under `rabbitkit.experimental` (or emit a clear
signal when imported from elsewhere). They may change or be removed without
a deprecation cycle, in any release. Do not depend on them in production
unless you're prepared to track changes closely.

| Feature | Import path | Why it's experimental |
|---|---|---|
| RPC (request/reply) | `rabbitkit.experimental` (`RPCClient`, `AsyncRPCClient`) | RPC-over-direct-reply-to has real documented sharp edges: handler exceptions don't propagate as clean RPC errors by default, and the reply-size cap is enforced only after the full reply is buffered. Treat as an escape hatch, not a recommended architecture. |
| Distributed locking (`DistributedLock`, `RedisLock`, `LockMiddleware`) | `rabbitkit.experimental` | `RedisLock` has no TTL auto-renewal — a slow handler can lose the lock mid-work. See the locking guide before using this for anything correctness-critical. |
| Message signing (`SigningMiddleware`) | `rabbitkit.experimental` | The default nonce cache is per-process/in-memory — replay protection is not real across multiple workers/pods without explicitly wiring a shared (Redis) cache. |
| Result backends (`ResultBackend`, `RedisResultBackend`, `ResultMiddleware`) | `rabbitkit.experimental` | This is task-queue-style result correlation, which is deliberately out of scope for a RabbitMQ-first toolkit — see the comparison doc for why rabbitkit doesn't try to be Celery. Use with that tradeoff in mind. |
| Stream queues (`StreamOffset`, `StreamConsumerConfig`) | `rabbitkit.experimental` | Smaller audience, different protocol semantics than classic/quorum queues. |
| Dashboard (`create_dashboard_app`) | `rabbitkit.experimental` | **Unauthenticated by default.** Never bind it to a non-loopback interface without `auth_token=` and a reverse proxy. |

Experimental APIs:

- May change signatures between patch releases.
- May be removed if the design proves unworkable.
- Are not covered by the deprecation policy below.

Feedback on experimental APIs is welcome and actively used to determine
whether they graduate — see the promotion criteria below.

---

## Promotion criteria (Experimental → Advanced Stable / Stable Core)

An experimental feature does not graduate because it's popular. It graduates
when it has demonstrably earned the tier above it:

- **At least 2 minor releases with no breaking changes** to its public shape.
- **A dedicated production-readiness pass** — the kind of structured,
  adversarial review this project already runs internally before shipping
  fixes (see the `CHANGELOG.md` fix history for the standard of evidence
  expected: root cause, fix, regression test, verified-by-reverting).
- **A security review**, if the feature touches authentication, secrets, or
  untrusted input (signing and the dashboard both qualify).
- **No open, known-dangerous default** — e.g. signing cannot graduate while
  its default nonce cache is silently unsafe across multiple workers; that
  has to become a loud, unmissable warning or a safer default first.

Absent all of the above, a feature stays experimental regardless of how long
it's existed or how many users depend on it.

---

## Deprecation Policy

For Stable Core (and Advanced Stable) APIs:

1. A deprecation warning (`DeprecationWarning`) is emitted for at least one
   minor release before removal.
2. The `CHANGELOG.md` entry for the deprecating release documents the
   replacement.
3. The symbol is removed in the following minor release at the earliest.

**Worked example:** `rabbitkit.aio` was deprecated in favor of the canonical
`rabbitkit.async_` import path pre-publication (internal `0.8.1`) — it
still works, but importing it
emits a `DeprecationWarning` pointing at the replacement. It will be removed
no earlier than the next minor release. This is the policy in practice, not
just in theory.

For Experimental APIs, no deprecation cycle is guaranteed. Changes are noted
in `CHANGELOG.md` but may take effect in the same release that introduces
them.

---

## How to Track Changes

- `CHANGELOG.md` in the repository root is the authoritative record of changes per release.
- The `[Unreleased]` section shows what is planned or in progress for the next release.
- Breaking changes are marked with `**Breaking**` in the changelog entry.
- This document is reviewed for accuracy at every release — see the release
  checklist in `CONTRIBUTING.md`.
