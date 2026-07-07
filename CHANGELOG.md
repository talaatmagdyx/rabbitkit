# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.0] — 2026-07-07

> **Upgrade notes (read before deploying):** three behavior changes can
> surface at startup — the quorum-source auto-DLQ now declares as QUORUM
> (an existing classic `{queue}.dlq` will 406 → `ConfigurationError` until
> migrated), `max_message_bytes` defaults to 16 MiB, and
> `prefetch_count=0`/`prefetch_per_worker=0` are rejected. Full migration
> steps: `docs/migration.md` → "0.10.0 — upgrade notes".

### Added

- **`docs/production/scale.md`** — the scale & reliability handbook:
  throughput math for millions of messages/day (measured numbers, async
  and sync), pipelined-confirm batch publishing, prefetch sizing,
  fleet/KEDA scaling, and the full reliability machinery end to end
  (heartbeat negotiation and who services it per mode, both transports'
  auto-reconnect designs, the retry/DLQ/quorum defense-in-depth layers,
  broker alarms and flow control, graceful shutdown under load). Key
  claims executed against a real broker before shipping (batched publish
  measured at ~6.7k msg/s locally). Also `docs/production/patterns.md`
  (the annotated reference consumer/publisher) — see the docs commit for
  the serializer doc-bug it surfaced.

- **Advanced benchmark tier** (`python -m benchmarks.advanced`, nightly) —
  interleaved A/B overhead vs raw aio-pika (median ± CV over 5 reps),
  payload-size sweep, classic-vs-quorum consume A/B, and open-loop paced
  latency measured from the intended send instant (no coordinated
  omission). Results carry an environment fingerprint and upload as
  workflow artifacts. Methodology: `docs/benchmarking.md`.

- **Soak harness** (`python -m benchmarks.soak`, weekly workflow) —
  sustained load with a broker kill every N seconds; verdicts require
  recovery after every bounce, zero loss of confirmed publishes, and
  bounded RSS slope / FD / task counts across all reconnect cycles.
  Produces the long-running-pod evidence (reconnect soak + leak
  detection) that per-push CI cannot.

- **Ten-queues fan-out coverage** — two new real-broker integration tests
  (`test_async_ten_queues_one_broker_isolated_and_concurrent`,
  `test_sync_ten_queues_shared_ten_worker_pool`) covering the "one service,
  10 worker queues" deployment shape at volume: per-queue isolation (no
  cross-queue leaks, no loss), cross-queue handler concurrency on the one
  shared connection, and a 10-thread `WorkerConfig` pool shared across all
  10 sync routes. A matching runnable example,
  `examples/highload/05_ten_queues_high_volume.py`, drives 10 queues with
  3,000 concurrently-published messages and reports per-queue counts and
  end-to-end throughput.

- **`RabbitQueue(consumer_timeout=...)`** — per-queue override of the
  server's consumer ack timeout (`x-consumer-timeout`, ms; RabbitMQ
  >= 3.12, classic/quorum only). RabbitMQ force-closes a consumer's
  channel if a delivery stays unacked past its `consumer_timeout`
  (server default: 30 minutes), and the server never advertises that
  limit to clients — neither in AMQP connection negotiation nor via the
  management API (verified empirically against a live broker) — so a
  long-running handler's only defense is declaring the override at init
  time. Validated (positive; rejected on streams, where per-message ack
  timeouts don't apply), warned about under `passive=True` like the
  other creation-only options, covered by unit tests and a real-broker
  integration test proving the declaration is accepted. Documented in
  `docs/troubleshooting.md` and the production checklist.

- **Typed error taxonomy for validation and API-misuse failures** — five
  new exception classes replace bare builtins at ~50 raise sites, every
  one **dual-inheriting the builtin it replaces** so existing
  `except ValueError` / `except RuntimeError` handlers (and every
  pre-existing test) keep working unchanged; the new types only ADD a
  precise catch point. All exported from `rabbitkit` and
  `rabbitkit.core.errors`:
  - `ConfigValidationError(ConfigurationError, ValueError)` — invalid
    values in config dataclasses (`RetryConfig(max_retries=-1)`, …) and
    AMQP short-string violations (including on `MessageEnvelope`).
  - `TopologyValidationError(ConfigurationError, ValueError)` — invalid
    `RabbitQueue`/`RabbitExchange` declarations (non-durable quorum,
    priorities on a stream, non-positive `consumer_timeout`, …).
  - `MessageTooLargeError(ValueError)` — the publish-time
    `max_message_bytes` guard.
  - `BrokerNotStartedError(RuntimeError)` — broker methods needing a live
    transport called before `start()`.
  - `SettlementError(RuntimeError)` — sync settlement on an async-only
    message or vice versa (defined in `core/message.py` to avoid an
    import cycle; re-exported from `core/errors.py`).
  Deliberately NOT converted: the registry's `TypeError` (genuine
  programming error) and serialization/signing data errors (domain-
  idiomatic; signing already has `InvalidSignatureError`). Locked in by a
  new `TestCustomErrorTaxonomy` suite asserting each raise site, the
  inheritance contract, top-level exports, and that bare-builtin catches
  still work. Documented: a full "Exception taxonomy" section (table of
  every public exception + catch patterns) in the Full Guide's
  Retry & Error Handling chapter, an Exceptions block on the Core API
  reference page, a README failure-modes row, and a Stable Core row in
  the stability policy that explicitly freezes the builtin base classes
  as part of the contract.

### Changed

- **`PublisherConfig.max_message_bytes` now defaults to 16 MiB**
  (previously `0` = guard disabled), mirroring RabbitMQ's own
  server-side `max_message_size` default. Rationale: the server rejects
  an oversized publish anyway, but with a channel exception that kills
  the (pooled) publisher channel and corrupts sibling in-flight
  publishes — the client-side guard converts that into a clean
  `ValueError` before the bytes hit the wire. The server's actual limit
  cannot be discovered at connect time (not in the AMQP tune frame, not
  in the management API), so mirroring the default is the closest
  possible thing: if you raised `max_message_size` in `rabbitmq.conf`,
  set `max_message_bytes` to match; `0` still disables the guard.
  Behavior change is only visible to publishers of >16 MiB messages,
  which the server was already rejecting destructively.

- Dependency ranges widened after full-suite validation on the newest
  releases: `redis >=5,<9` (validated on 8.0.1; rabbitkit touches only
  `set/get/exists/delete/eval`) and `structlog >=23.1,<27` (validated on
  26.1.0).

### Fixed

- **RabbitMQ-architect review remediation** — a three-lens external-style
  review (transport AMQP correctness, retry/DLX topology & delivery
  guarantees, consumer/QoS/lifecycle) surfaced and fixed, in order of
  severity:
  - *Retry loss path*: `RetryMiddleware` with a missing/mismatched publish
    fn ACKED transient failures without publishing (silent drop) — now
    nack+warn, brokers inject their publish fn into user-constructed
    instances, and a `None` publish outcome counts as failure (also in
    `DLQInspector.replay`).
  - *Spoofable retry routing*: `x-rabbitkit-original-queue` is now always
    overwritten at delivery — a producer-set value could steer retries
    into another route's delay ladder or a requeue hot loop.
  - *Sync cross-thread reconnect hijack*: a non-owner thread publishing on
    a dead connection created a new `BlockingConnection` and stole
    ownership (two threads on one pika connection); now a clean ERROR
    outcome, reconnection owned solely by the recovery loop.
  - *Sync shutdown mass-redelivery*: `cancel_consumer` closed the channel
    before the in-flight drain, force-requeuing every unacked delivery
    while workers still ran those handlers; channels are now parked and
    closed after the drain.
  - *`warn_continue` silent-loss modes*: the 406 recovery re-enables
    publisher confirms on the reopened channel (previously reported
    CONFIRMED while fire-and-forget), and a conflict on a declaration
    carrying an injected DLX escalates instead of silently converting
    terminal rejects into discards.
  - *Async shared mandatory channel*: one publish's confirm timeout no
    longer closes the channel under concurrent sibling publishes
    (ref-counted deferred recycle).
  - Plus: idle-death publish retry-once (sync), liveness heartbeat during
    reconnect backoff (no more restart storms during broker outages),
    `AsyncTransportImpl.__aexit__` (real `async with` support), blocked
    hooks + watchdog on the async consumer connection, binding re-apply
    after robust reconnect, retry envelopes no longer preserve
    producer `expiration` (collapsed the backoff ladder), auto-DLQs
    inherit QUORUM from quorum sources, `prefetch_count`/
    `prefetch_per_worker` validated >= 1 (0 meant AMQP *unlimited*),
    `DLQInspector.replay(limit=)`, confirm-timeout clamp removed, and
    confirmed-channel tracking hardened against `id()` reuse.
  - *Adversarial fix verification*: an independent pass attacking the
    fixes themselves refuted one and found residuals, all closed — the
    helper-thread publish dispatch keyed on the non-sticky consume flag
    (a worker publish during the recovery window could still run
    concurrently with the owner rebuilding topology on the new
    connection); reconnect ownership now enforced INSIDE
    `_ensure_connected` via a sticky owner identity (closing the
    publish-guard TOCTOU and every other cross-thread entry point,
    e.g. `DLQInspector.basic_get`); async binding re-apply upgraded to
    bounded retry with backoff; watchdog installs split per connection;
    the direct-reply-to channel closed (not leaked) at cancel.
  - *Docs*: `docs/kubernetes.md` probes rewritten to in-process HTTP
    endpoints (the documented CLI subcommands never existed and exec
    probes structurally cannot see the running broker — deployments from
    the old guide CrashLooped); `docs/retry-and-dlq.md` corrected to the
    real topology (no retry exchange; `{queue}.retry.{attempt}` via the
    default exchange; `x-rabbitkit-retry-count`; `strict_delays`; CLI
    replay continues past failures and resets the counter only with
    `--reset-retry-count`); `docs/rabbitmq-retry-architecture.md` §0
    rewritten against current behavior (retry= auto-wires middleware;
    failed publishes nack; AUTO dead-letters exhausted retries);
    `WorkerConfig.stop_timeout` documented as the fallback it actually is
    (the shutdown budget is `ConsumerConfig.graceful_timeout`);
    active/standby via `single_active_consumer` documented.

- **`RabbitConfig.pool` was silently ignored by `AsyncBroker`** —
  `AsyncBroker.start()` constructed its transport without passing
  `pool_config=`, so the transport always used the default `PoolConfig()`
  (publisher channel pool of 10) no matter what the user configured;
  `PoolConfig(channel_pool_size=...)` tuning had no effect at all on the
  async side (the sync transport has no channel pool, so it was
  unaffected). The M-P5 small-pool warning even read
  `self._config.pool.channel_pool_size` as if it were effective. Found by
  actually running the new ten-queues example with a
  `channel_pool_size=32` config and watching "Channel pool exhausted
  (pool_size=10)" warnings still appear. Fixed by passing
  `pool_config=self._config.pool` through; locked in by a unit test
  asserting the transport receives the exact configured `PoolConfig`.
  With the fix the example's concurrent publish throughput rose ~50%.

- **`RabbitConfig.socket` (SocketConfig) no longer silently ignored by
  `AsyncBroker`** — the same "config that lies" class as the `pool` drop
  above, surfaced by the production-readiness review that followed it:
  nothing under `async_/` ever referenced `SocketConfig`, so TCP tuning
  set on an async broker was a complete no-op. Unlike `pool`, this one
  cannot be plumbed through: aio-pika/aiormq exposes no socket-tuning
  hooks, and applying `setsockopt` to the live socket would be silently
  lost on every `connect_robust` automatic reconnect. `AsyncBroker.start()`
  now emits a `RuntimeWarning` when a non-default `SocketConfig` is set
  (covered by two unit tests: warns on non-default, silent on default),
  and `SocketConfig` is documented as **sync-only** in its docstring
  (which feeds the API docs) and in the retry-architecture guide's
  production config example.

## [0.9.1] — 2026-07-04

First PyPI-visible patch: the 0.9.0 package description referenced images
and doc links by repository-relative paths, which render broken on
pypi.org (release descriptions are immutable, so 0.9.0's page stays
as-is). The README now uses absolute URLs everywhere.

### Fixed

- **Quiet settlement on a dead channel** — when a channel/connection dies
  before a message can be settled (SIGTERM drain, broker restart), the
  pipeline now logs one WARNING ("broker will redeliver it") instead of
  letting the transport error escape as a full ERROR traceback plus a
  secondary failed-settle exception. Detection is name-based
  (`ChannelWrongStateError`, `ChannelInvalidStateError`, …) so `core/`
  stays transport-free.
- **`broker.started` property** — both brokers now expose the typed
  `started` property, so `rabbitkit.health` no longer emits its own
  fall-back-to-private-attribute DeprecationWarning against rabbitkit's
  own brokers.
- **Benchmarks: safety auto-provision opt-out** — real-broker benchmark
  scenarios no longer 406 on preloaded queues (same inequivalent-arg
  collision the chaos suite had); the full nightly suite produces numbers
  again. `psutil` added to dev extras so the resources benchmark stops
  self-skipping.

## [0.9.0] — 2026-07-04

**First published release** (public beta). No earlier version was ever
distributed: the 0.8.x entries below are pre-publication internal
milestones (originally numbered 1.0.0–1.2.0; this release itself was
internally 1.3.0), renumbered so the published 0.9.0 sits at the top of
the version history.

Roadmap release: six features extending the 0.8.3 thesis (*no message is
lost, and operators can see and fix everything*) to places it didn't reach —
the open observability ecosystem, the migration path onto the mandated
quorum topology, retry-wave decorrelation, duplicate-result replay, and the
sync confirm ceiling. Two features shipped as their honest reframings: sync
confirm pipelining runs on a dedicated `SelectConnection` I/O thread
(impossible on `BlockingChannel`), and "exactly-once" ships as the
idempotent-receiver *effect* (wire-level exactly-once does not exist on
RabbitMQ and the docs never claim it).

### Security

- **starlette ≥1.3.1 allowed** — the `dashboard` extra's `<1.0.0` cap forced
  starlette 0.52.1, which carries six known advisories (PYSEC-2026-161/248/249,
  CVE-2026-48817/48818, all fixed by 1.3.1). The pin is now `<2.0.0`.

### Fixed

- CI reliability: unit tests no longer depend on ambient environment state
  (a locally running broker, transitive `prometheus-client`/`httpx`,
  event-loop state left by earlier tests, or wall-clock speed in the
  token-bucket refill test).

### Removed

- **`TracedConsumerMiddleware`** (the obskit integration) — removed
  entirely; rabbitkit is now fully self-contained, with zero org-internal
  packages needed for any feature. obskit was never a required dependency
  (every reference was lazy or duck-typed), and its one functional
  integration is redundant with `OTelTracingMiddleware`, a drop-in
  replacement (same spans, attributes, and propagation). Migration: swap
  the class name and `pip install rabbitkit[otel]`. Duck-typed
  `CircuitBreakerProtocol` compatibility is unaffected — any conforming
  implementation (e.g. pybreaker) works as before.

### Added

- **Hosted documentation** — mkdocs site now deploys to GitHub Pages
  (https://talaatmagdyx.github.io/rabbitkit/) on every push to `main` via a
  new `Docs` workflow (`mkdocs build --strict` gates the deploy). Docs
  homepage now carries the project logo and CLI demo; stale
  `talaatmagdy/rabbitkit` URLs in `mkdocs.yml`/`pyproject.toml` corrected to
  `talaatmagdyx`, and the theme favicon now points at an asset that exists.
- **Python 3.14 support** — added to the CI test matrix and trove
  classifiers (full suite green). Python 3.15 runs as an experimental,
  allowed-to-fail pre-release leg until its C-extension dependencies
  (pydantic-core, msgspec, zstandard) ship cp315 wheels. The `reload`
  extra's watchfiles pin widened to `<2.0.0` (0.x has no 3.14 wheels).

- **`OTelTracingMiddleware`** (`rabbitkit[otel]`) — native OpenTelemetry
  tracing via `opentelemetry-api`: CONSUMER spans with W3C `traceparent`
  extraction from AMQP headers, PRODUCER spans injecting context into a copy
  of the (frozen) envelope, exception recording with ERROR status, OTel
  messaging semantic attributes. No-op with one loud construction warning
  when opentelemetry isn't installed. Removes the org-internal `obskit`
  coupling as the only tracing path — pick one middleware, not both.
- **`HealthWatcher` / `AsyncHealthWatcher`** — opt-in push-style health
  notifications: polls `broker_health_check` and fires
  `on_change(old, new, result)` only after N consecutive identical readings
  (debounce, default 2 — one flapping poll never pages). Callback errors are
  logged, never raised. Optional collector emits a `rabbitkit_health_state`
  gauge (0/1/2). For non-k8s deployments; probes stay primary on k8s.
- **`rabbitkit topology migrate`** — the supported classic→quorum path the
  production profile mandates but 1.2.0 had no route to. Plan mode (default,
  never mutates) emits an ordered runbook + a bindings/arguments rollback
  snapshot; `--execute --strategy drain-cutover` performs the
  shovel-based create-tmp → drain → delete → redeclare-as-quorum → drain-back
  sequence with rails (refuses live consumers without `--force`, verifies
  message counts before every destructive step, checkpoints each of 11 steps
  to a state file for `--resume`); `--strategy bridge` creates `{q}.q2` +
  duplicate bindings and deletes nothing; `--dry-run` prints every management
  call it would make. Detects a missing shovel plugin with a clear error.
  `RabbitManagementClient` gained `put_parameter`/`delete_parameter`/
  `list_shovel_statuses`/`get_queue_bindings`/`declare_queue`/`bind_queue`.
- **`RetryConfig(jitter_mode="sharded")`** — decorrelates retry waves (the
  audit's retry-storm amplifier) without per-message TTL: each tier becomes
  `jitter_shards` sub-queues whose uniform TTLs stagger across
  ±`jitter_factor`; a message picks its shard by a stable md5 hash of its
  message_id (Python's `hash()` is process-salted and would change a
  message's cadence per redelivery). Shard 0 keeps the legacy `{q}.retry.N`
  name and exact TTL — enabling on an existing topology is additive, no
  406s; the default `"off"` topology is byte-identical (regression-tested).
  The roadmap's secondary delayed-exchange backend is deferred: it requires
  custom exchange-type support in the core enums and both transports.
- **`DeduplicationConfig(store_results=True)`** — the idempotent-receiver
  effect on the crash-safe `claim` policy: handler results are stored in a
  versioned envelope alongside the completed mark, and a duplicate delivery
  REPLAYS the stored result (the pipeline re-publishes it byte-identically
  to the result publisher / `reply_to`) instead of just skipping — a
  redelivered RPC request gets the same answer without the handler's side
  effects running twice. Graceful degradations, never errors: non-JSON or
  oversized (`max_result_bytes`) results store the plain mark; legacy values
  and schema-tag mismatches skip without replay (safe rolling upgrade).
- **`SyncBatchPublisher`** — pipelined publisher confirms for sync code,
  built on a private `pika.SelectConnection` owned entirely by one dedicated
  daemon I/O thread (the one-thread-one-connection invariant holds; callers
  enqueue thread-safely and block on their own outcome). Every slot settles
  on every path — ack/nack (incl. `multiple=True` ranges),
  Return-before-Ack with first-settlement-wins, caller timeout → TIMEOUT
  with late-confirm no-op, connection death → ERROR for in-flight and
  queued, bounded-jitter reconnect, drain-then-fail on close. Standalone by
  design (not wired into `SyncBroker.publish`); raises the documented
  ~0.9k msg/s blocking ceiling for callers who adopt it.


## [0.8.3] — 2026-07-04 <small>(internal milestone, formerly 1.2.0)</small>

Production-hardening release implementing the full `PRODUCTION_AUDIT.md`
remediation: all 4 critical, 6 high-risk, and 18 medium findings, the
low-severity list, and the fixes surfaced by re-verifying the audit's own
"resolved-by-design" and "verified strengths" claims against the actual
code. Every known message-loss path in the library's own code is closed;
remaining failure modes are duplicate-or-requeue under at-least-once
delivery (idempotent handlers remain mandatory — see
`docs/production/idempotency.md`).

Several **breaking defaults** — see the inline migration notes: auto-DLX
provisioning for every rejecting route (`SafetyConfig.reject_without_dlx`),
rejection of `RetryConfig(per_queue=False)`, `SecurityConfig.mechanism`
validation, and signing+retry now failing fast at startup.

### Added

- **`ConnectionConfig.credentials_provider`** (audit M13) — credential
  rotation without a redeploy. An optional `() -> (username, password)`
  callable, called at each (re)connect (sync + async), so a rotated secret
  (Vault, short-lived IAM, etc.) is picked up on the next reconnect instead
  of requiring the process to restart with a new frozen config. Falls back to
  the static `username`/`password` when unset.
- **`SafetyConfig.on_topology_conflict="warn_continue"`** (audit M14) — a
  per-conflict warn-and-continue mode for topology drift. When a queue/
  exchange declaration 406s because it already exists with incompatible
  arguments (ops created it, or a prior version, with a different
  type/TTL/DLX), the default `"raise"` fails startup with a typed error;
  `"warn_continue"` logs a warning and continues using the EXISTING
  definition (reopening the broker-closed channel). Unlike
  `TopologyMode.PASSIVE_ONLY` (which skips declaration for *every* queue),
  this still actively declares non-conflicting queues and only tolerates the
  drifted ones — the gap PASSIVE_ONLY couldn't fill. Both transports.
- **`ConsumerConfig.reject_transient_on_redelivery`** (audit M6) — opt-in
  2-strike cap on the transient hot-loop for retry-less AUTO routes. By
  default (False) a transient error nack-requeues unbounded (legitimate
  "wait for the downstream to recover"). When True, a transient error on a
  message the broker has *already redelivered* (`redelivered=True`) is
  rejected to the DLQ instead of requeued again — using the broker's
  redelivered flag, the only per-message redelivery signal available without
  republishing. For a higher cap or delays, use retry or a quorum source
  queue with `x-delivery-limit`. Needs a dead-letter path (the default
  `reject_without_dlx="auto_provision"` gives AUTO routes one).

- **Multi-host / cluster failover via `ConnectionConfig.nodes`** (audit M9) —
  a tuple of additional `"host"` or `"host:port"` cluster nodes. Sync (pika)
  tries them natively via a `ConnectionParameters` list; async cycles
  endpoints on the initial connect (`connect_robust` then pins to the chosen
  node — front with a load balancer / DNS for per-reconnect failover). A dead
  configured primary no longer takes the client down at startup. Malformed
  node entries fail fast at construction.

- **`PublishOutcome.raise_for_status()`** (audit M1) — opt-in exception for the
  publish path. `broker.publish()` still returns a `PublishOutcome` (never
  raises on its own), but code that prefers exceptions can now write
  `broker.publish(...).raise_for_status()`, which raises the new `PublishError`
  (carrying the outcome) on NACKED/TIMEOUT/RETURNED/ERROR and returns the
  outcome otherwise. Guards against silent loss when a caller ignores the
  return value.

- **Queue-depth / consumer-lag metrics via `QueueMetricsPoller`** (audit H5).
  The consume/publish counters only see messages *this process* handles — a
  queue could accumulate millions of messages (consumer fell behind or died)
  while every rabbitkit metric read healthy. `QueueMetricsPoller` polls the
  management API and emits gauges labeled by queue: `queue_messages_ready`
  (backlog), `queue_messages_unacked`, `queue_messages_total`, and
  `queue_consumers` (0 = nothing draining — the DLQ/lag alert signal).
  Sync (`start()`/`stop()` daemon thread) and async (`run_async()` task)
  loops; management errors are swallowed so a transient outage doesn't crash
  the poller. `MetricsCollector` gained `set_gauge`; `PrometheusCollector`
  implements it.


- **`broker_health_check`/`broker_readiness` (+ async variants) accept an
  optional `management_client`** (`RabbitManagementClient`). The existing
  checks are entirely process-local — this process can hold a perfectly
  live connection to one RabbitMQ node while the rest of a partitioned
  cluster is unreachable, and the local checks alone can't see that. When
  given, a failing `.health_check()`/`.health_check_async()` downgrades an
  otherwise-HEALTHY result to DEGRADED (never overrides an already-
  UNHEALTHY local result); `broker_readiness` treats that downgrade as
  not-ready. Fully opt-in — omitting the parameter preserves the original
  process-local-only behavior exactly. The async variants call
  `.health_check_async()`, not the sync method, so readiness/health checks
  never block the event loop on a network round-trip.

### Changed

- **The critical restart-mid-consume chaos scenarios now gate CI** (audit
  M15). `benchmarks/chaos_suite.py` takes a scenario-name filter, and a new
  gating CI step runs the sync+async "restart mid-consume" scenarios with a
  checked exit code — a reconnect/redelivery regression breaks the build
  instead of merging green. The full chaos suite stays best-effort.
- **Async large-body deserialization is offloaded off the event loop** (audit
  M10). Bodies ≥ 256 KiB are now decoded in a worker thread
  (`asyncio.to_thread`) so a multi-MB JSON/msgspec/pydantic parse no longer
  blocks heartbeats, publisher confirms, and other consumers sharing the loop.
  Smaller bodies still decode inline.
- **`PublisherConfig.max_message_bytes`** (audit M10) — opt-in publish-side
  size guard (default 0 = disabled). When set, `broker.publish()` raises
  `ValueError` for a body over the cap, catching the large-message anti-pattern
  before it hits the wire.
- **`WorkerConfig.max_queue_size`** (audit M11) — opt-in bound on the sync
  worker pool's internal work queue (default 0 = unbounded, unchanged). In
  practice prefetch already caps in-flight; this is a defensive ceiling. Also
  added `WorkerConfig.__post_init__` validation (`worker_count >= 1`,
  `max_queue_size >= 0`).
- **Async reconnect interval is now jittered per process** (audit H4). aio-pika's
  `connect_robust` uses a FIXED reconnect interval with no backoff — a fleet
  restarted together (broker bounce under N pods) retried in lockstep every
  `reconnect_backoff_base` seconds, a thundering herd against the recovering
  node. The interval is now randomized per process over
  `[base, base + min(base, backoff_max - base)]` to de-synchronize retries.
  (Exponential backoff still isn't available through `connect_robust`; jitter
  addresses the herd.)
- **Single-worker sync consumers now warn at start** (audit H2). A
  `worker_count=1` sync consumer runs handlers inline on the I/O thread, so a
  handler running longer than ~2× the heartbeat starves heartbeats and the
  broker drops the connection mid-handler (→ redelivery + duplicate side
  effects). `SyncBroker.start()` now emits a `RuntimeWarning` when consuming
  single-worker with heartbeats enabled, pointing at
  `WorkerConfig(worker_count=N)` or a higher heartbeat.
- **Sync confirmed-publish throughput ceiling (~0.9k msg/s) documented**
  (audit H6) in the README, `SyncBroker.publish` docstring, and the batch
  helper — it is RTT-bound per-message on a single channel and does not scale
  with `worker_count`; use `AsyncBroker`/`AsyncBatchPublisher` or more
  processes to drain backlogs. (No behavior change — pika BlockingConnection
  cannot pipeline confirms.)
- **`PublisherConfig.mandatory` / `.persistent` are now honored** by the
  kwargs form of `publish()` (audit M2) — previously dead config. Persistent
  maps to `delivery_mode` (2/1); mandatory sets the envelope flag. Envelope-
  form callers keep full control.
- **`amqps://` URLs now warn and default to port 5671** in
  `ConnectionConfig.from_url` (audit M3). rabbitkit enables TLS via
  `SecurityConfig(ssl=SSLConfig(enabled=True))`, not the URL scheme — so an
  `amqps://` URL without that would previously connect PLAINTEXT silently.
  Now it warns and targets the AMQPS port, so an un-TLS'd connection fails
  fast instead of leaking plaintext.
- **`SSLConfig(cert_reqs="CERT_NONE")` now warns** (audit M2/M13) — disabling
  certificate verification is MITM-able and was previously silent.
- **`SecurityConfig.mechanism` other than `"PLAIN"` now raises** at
  construction (audit M2) — SASL EXTERNAL is not implemented, so accepting it
  was "config that lies." (mTLS for encryption is still available via
  `SSLConfig`.)
- **`RetryConfig.jitter_factor` documented as reserved/no-op** (audit M2) —
  queue-based retry uses a fixed per-queue TTL, so jitter would require
  per-message TTL (head-of-line blocking); the field never affected timing.
- **Removed the misleading `channel_pool_size` warnings** from
  `SyncBroker.start()` (audit M2) — the default publish path uses a single
  dedicated channel, not `SyncChannelPool`, so those warnings pointed at a
  deadlock/throughput knob that has no effect on that path.


- **`mark_policy="on_start"` is now documented as advanced/dangerous**: a
  process crash after the mark but before the handler finishes loses the
  message (the redelivery is skipped as a duplicate). Prefer the default
  `on_success`, or `claim` when concurrent duplicate suppression is needed.

### Fixed

- **`FlowController.acquire_async` no longer silently drops a waiter that
  paid for a rate token, lost the slot race, and woke to a momentarily
  empty bucket.** With `on_blocked="wait"` + `rate_limit` + a contended
  `max_in_flight`, a contender that (1) waited for and consumed a rate
  token, (2) found the slot taken meanwhile, and (3) woke from the slot
  wait to find the token bucket empty again was dropped via a second-token
  demand (`_REASON_RATE_RETRY` → `False`) — returning `False` in ~70ms of
  a 10-second budget. Surfaced as a ~1% failure rate of the contender
  stress test under CPU load (initially misdiagnosed as test flakiness;
  the test was right). The async path now mirrors the sync path exactly:
  after the slot wait it re-loops into the bounded `_REASON_RATE` wait
  instead of dropping — no waiter fails before its deadline. This also
  removes a lock-held policy wait (the old path dispatched the rate
  re-check while holding the async lock, violating the C-5 lock
  discipline) and deletes the now-unused `_REASON_RATE_RETRY` reason.
  Verified: 500 stress rounds under 8-way CPU load — 6 drops before the
  fix, 0 after — plus a deterministic choreographed regression test that
  fails against the old code.
- **Retry delay-queue publishes are now `mandatory`** (audit M4) — a
  runtime-deleted/missing `{queue}.retry.N` queue used to broker-confirm the
  retry publish into the void and ack the source (silent loss). With
  `mandatory=True` the publish RETURNs (outcome not-ok), and the existing
  route-to-delay-queue path nack-requeues instead of acking.
- **`REQUEUED_FOR_RETRY` sentinel no longer leaks into result/RPC publishing**
  (audit M7) — a retried message on a route with a `result_publisher` or
  `reply_to` used to serialize the sentinel as a bogus reply (once per retry
  attempt). The pipeline now skips result-publishing for the sentinel.
- **A MANUAL-policy handler exception no longer stops the whole sync broker**
  (audit M12) — a `AckPolicy.MANUAL` handler that raised without settling used
  to propagate out of the delivery callback and halt `start_consuming()` (one
  bad handler took down the consumer). It is now contained: logged and
  nack-requeued if unsettled, matching the pooled/async paths.
- **`SigningMiddleware` no longer destroys legitimately-redelivered or
  retried messages** (audit H1). Two independent losses: (a) a broker
  redelivery (nack/requeue of an unacked message) tripped the nonce
  replay-check and was rejected → dead-lettered/discarded; now a duplicate
  nonce on a message with `redelivered=True` is allowed (the broker sets that
  flag, an attacker's re-publish arrives as a fresh delivery so replay
  protection still holds). (b) signing + retry is fundamentally incompatible
  — retry re-publishes via the default exchange with a different routing key,
  which the signature covers, so every retried signed message fails
  verification; the brokers now raise `ConfigurationError` at startup when a
  route has both, instead of silently destroying each retried message.
- **`RetryConfig(per_queue=False)` (shared retry mode) now rejected at
  construction** (audit H3). Shared delay queues bake a single
  `x-dead-letter-routing-key` per queue, so with >1 subscriber they either
  406 at startup or dead-letter every queue's failures back to whichever
  queue declared first (orders' failures reappearing on payments). A shared
  delay queue cannot route each message back to its own varying source with
  static broker config, so there is no safe shared topology — `per_queue`
  must be `True` (the default, isolated `<queue>.retry.N`/`<queue>.dlq`).
- **Async batch confirm-timeout no longer corrupts sibling in-flight
  publishes** (audit M17). `_publish_on_channel`'s confirm-timeout handler
  used to close the channel it was passed — but `AsyncBatchPublisher._flush`
  gathers several concurrent calls onto one shared channel, so one publish's
  own timeout closed the channel out from under every sibling publish still
  awaiting its own confirm on it, even ones that would have confirmed
  cleanly a moment later. Closing now happens in the caller instead, after
  that caller's own usage of the channel is fully resolved: the batch flush
  closes the shared channel only once `asyncio.gather` has settled every
  publish in the batch, and the mandatory/pooled-channel publish paths close
  their (non-shared) channel only after their own single call returns.
- **`RetryMiddleware` and result/RPC-reply publishes now honor the broker's
  `FlowController`** (audit M18). Both previously used raw
  `transport.publish`, bypassing publish-side backpressure entirely — a
  broker configured with `max_in_flight`/rate limits to protect itself under
  load gave zero protection to its own retry republishes or handler-result
  publishes. The new `_flow_controlled_internal_publish` (sync + async)
  wraps `FlowController.acquire`/`release` around these paths, and
  deliberately never lets `BackpressureError` escape as an exception — under
  any `on_blocked` policy (including `"raise"`) a blocked/dropped slot
  resolves as a failed `PublishOutcome` instead, since `RetryMiddleware` and
  the pipeline's result-publish only understand that return shape; an
  escaped exception would be misclassified `PERMANENT` by the default error
  classifier and destroy the message instead of nack-requeuing it.

- **`SafetyConfig.reject_without_dlx` — no more silently discarded poison
  messages** (audit C3). Previously a DLQ was auto-provisioned only for
  retry-enabled (or `filter_fn`) routes; a plain `@subscriber(queue="orders")`
  whose handler raised a permanent error (`ValueError`, validation failure,
  or ANY unknown exception type) rejected with `requeue=False` into a queue
  with no DLX — RabbitMQ discarded the message forever with only a log line.
  Now every route that can reject gets a dead-letter path, governed by
  `RabbitConfig(safety=SafetyConfig(reject_without_dlx=...))` — a
  `RejectWithoutDLXPolicy` enum or string:
  ``"auto_provision"`` (default — declares `{queue}.dlq`, same
  default-exchange convention as retry topology), ``"error"`` (startup fails
  with the new `UnsafeTopologyError`, for externally-managed topology), or
  ``"discard"`` (explicit opt-in to loss; warns once per route unless
  `warn_on_discard=False`). Per-route override via
  `@subscriber(reject_without_dlx=...)`. Retry-enabled routes, manually
  configured DLX queues, and `ACK_FIRST` routes (which never reject) are
  unaffected; the policy applies only under `TopologyMode.AUTO_DECLARE`.
  The filter-route special case (H6) is folded into this general mechanism —
  its `RuntimeWarning` is replaced by an INFO log, and
  `warn_filter_without_dlx` was removed.
  **Migration note:** the default changes existing plain queues' declare
  arguments (DLX args injected). Re-declaring an existing queue with
  different arguments 406s at startup with a clear `ConfigurationError` —
  delete/re-create the queue, mirror the args via a broker policy, opt the
  route into `"discard"`/`"error"`, or use `TopologyMode.PASSIVE_ONLY`.

- **`mark_policy="claim"` for `DeduplicationMiddleware`** — a two-state
  dedup policy that blocks concurrent duplicate execution AND is crash-safe.
  Before the handler runs, the key is atomically claimed as ``in-flight``
  with `DeduplicationConfig.processing_timeout` (default 300 s) as its TTL;
  on handler success it is flipped to ``completed`` with the full `ttl`. A
  concurrent copy that sees a live in-flight claim is handled per
  `DeduplicationConfig.on_in_flight`: `"nack_requeue"` (default — the copy
  comes back and retries, so it is not lost if the claiming consumer dies)
  or `"ack_skip"`. A crash mid-handler simply lets the claim expire, after
  which the broker's redelivery re-claims and processes. Keys written by
  `on_success`/`on_start` deployments (value ``"1"``) are read as completed,
  so switching an existing deployment to `claim` is safe. Caveat:
  `processing_timeout` must comfortably exceed the worst-case handler
  duration, or a duplicate can start while the original is still running.
  The duck-typed Redis client needs `.get()` for this policy.
- **`DeduplicationMarkPolicy` enum** (`on_success` / `on_start` / `claim`)
  in `core/types.py`, exported at top level; `DeduplicationConfig.mark_policy`
  accepts the enum or its string value and now fail-fast validates
  `mark_policy`, `on_in_flight`, and `processing_timeout` at construction.


- **Headers-exchange bindings now actually reach the broker** (audit C4,
  silent misrouting). `RabbitQueue.bind_arguments` was never passed by
  either broker — `bind_queue()` had no arguments parameter — so a headers
  binding declared with `{"x-match": "all", "type": "order"}` bound
  argument-less, which RabbitMQ treats as match-everything: the queue
  received the full firehose (or missed selective routing) with zero
  errors. Both transports' `bind_queue()` now accept and forward
  `arguments`, and both brokers pass `queue.bind_arguments`. Registration
  now also fail-fast validates headers routes: binding to a HEADERS
  exchange without `bind_arguments` raises `ConfigurationError`, as does an
  invalid `x-match` value (must be `all`/`any`/`all-with-x`/`any-with-x`;
  absent defaults to `all` per RabbitMQ).
- **`DLQInspector.replay`/`replay_async` no longer ack a DLQ message when the
  republish failed** (audit C2, message loss). Previously the original was
  acked immediately after `transport.publish(...)` — but both transports
  report failures as a *returned* `PublishOutcome`
  (NACKED/TIMEOUT/RETURNED/ERROR), not an exception, so a connection blip or
  unroutable target mid-replay removed the message from the DLQ while the
  republish went nowhere: permanent loss from the recovery tool itself.
  Replay now checks `outcome.ok` before acking; failed republishes are
  nack-requeued (they stay on the DLQ) and reported. Republish envelopes are
  now `mandatory=True`, so an unroutable target surfaces as RETURNED instead
  of being broker-confirmed into the void. The return type is
  `ReplayResult` — an `int` subclass carrying the replayed count (existing
  `count = inspector.replay(...)` callers are unaffected) plus `.failed`
  (left on the DLQ) and `.requeued` (non-matching) counts. New opt-in
  `reset_retry_count=True` strips `x-rabbitkit-retry-count` so a replayed
  message gets a fresh retry ladder — by default a previously max-retried
  message is terminal after one failed attempt and returns to the DLQ.
- **`rabbitkit dlq replay` (CLI) had the same loss window, worse**: it
  published on a non-confirm channel (fire-and-forget) and acked
  immediately. The channel now runs with publisher confirms and
  `mandatory=True`; an unroutable/nacked publish is nack-requeued (message
  stays on the DLQ), reported on stderr, and the command exits non-zero.
- **`DeduplicationMiddleware` `mark_policy="on_success"` no longer writes the
  dedup key before the handler runs** (audit C1, message loss). Previously the
  Redis `SET NX` executed *before* `call_next`, with a `DELETE` rollback on
  exception — so a consumer killed mid-handler (OOM/SIGKILL, where no rollback
  can run) left the key marked, and the broker's redelivery of the unacked
  message was acked-and-skipped as a "duplicate": the message was silently
  lost for the dedup TTL (default 24 h). `on_success` now checks with
  `EXISTS` (no write) before the handler and writes the key only after the
  handler returns successfully. A crash at any point leaves no mark, so the
  redelivery is processed. Trade-off (already documented): concurrent
  deliveries of the same message may both pass the check — standard
  at-least-once semantics. Two behavioural consequences: a failed handler no
  longer triggers a Redis `DELETE` (there is nothing to delete, and deleting
  could erase a concurrent delivery's legitimate mark), and a Redis failure
  while writing the success-mark is logged/metered but never raised — the
  handler's side effects are already committed, and raising would nack →
  redeliver → a guaranteed duplicate execution (applies even with
  `fallback_on_redis_error=False`). The duck-typed Redis client now also
  needs `.exists()` alongside `.set()`/`.delete()`.
- **`DeduplicationMiddleware` local LRU cache is now thread-safe.** The
  optional `local_cache_size` `OrderedDict` was mutated without a lock from
  sync worker-pool daemon threads; concurrent `move_to_end`/`popitem` during
  eviction could raise `KeyError` (classified PERMANENT → message
  dead-lettered/dropped for a bookkeeping race). All cache operations now
  take an internal `threading.Lock`.
- **Retry republish and DLQ replay no longer drop message properties.**
  `_build_retry_envelope`/`_build_replay_envelope` only copied a handful of
  `MessageEnvelope` fields — `priority`, `expiration`, `type`, `app_id`,
  `user_id`, and `reply_to` were silently lost on every retry or DLQ
  replay. A priority-queue message lost its priority on its first retry; an
  RPC request's `reply_to` never survived long enough for a retried/
  replayed reply to route back. `RabbitMessage` gained `priority`/
  `expiration`/`user_id` slots (it already carried `type`/`app_id`/
  `reply_to`), both transports now populate them from the incoming
  delivery, and both envelope builders copy all six through. The async
  transport re-encodes aio-pika's decoded-seconds `expiration` back to the
  ms-string convention used everywhere else, so it round-trips correctly
  regardless of which transport originally received the message.
- **Oversized routing keys/exchange/queue names now fail fast with a clear
  error** instead of an opaque broker connection error at declare/publish
  time. AMQP 0-9-1 encodes these as shortstr (a 1-byte length prefix — 255
  bytes is a hard protocol ceiling). `MessageEnvelope.__post_init__` (every
  publish path funnels through this one construction point) and
  `RabbitQueue`/`RabbitExchange.validate()` now raise `ValueError` for any
  value exceeding `AMQP_SHORTSTR_MAX_BYTES` (255), measured in encoded
  UTF-8 bytes.
- **`RabbitQueue(lazy=True)` now warns it's a no-op on RabbitMQ ≥3.12.**
  `x-queue-mode=lazy` is a classic-queue-v1-era argument; RabbitMQ ≥3.12
  defaults classic queues to CQv2, which already keeps message bodies out
  of memory without it — the argument is silently ignored there. Still has
  effect pre-3.12 or on a queue explicitly downgraded to v1.
- **`rabbitkit topology validate`/`diff --url` no longer accepts arbitrary
  URL schemes.** `_live_resources` passed the CLI-supplied management URL
  straight to `urlopen`, which would happily dispatch `file://`, `ftp://`,
  or any other registered urllib scheme — not just `http(s)://` the
  management API actually uses. The scheme is now validated (`http`/
  `https` only) before any request is made.
- **`rabbitkit dlq replay` gained `--reset-retry-count` / `--retry-count-header`.**
  `DLQInspector.replay(reset_retry_count=True)` already existed at the
  Python API level, but the CLI's `dlq replay` command is a separate,
  hand-rolled raw-pika implementation that never calls `DLQInspector` — so
  an operator using the CLI (the realistic way "replay this stuck,
  max-retried message" gets done) had no way to strip the retry-count
  header at all, and a replayed message carrying `x-rabbitkit-retry-count`
  at its max was instantly terminal on the next failure. `--reset-retry-count`
  strips the header (default `x-rabbitkit-retry-count`, overridable via
  `--retry-count-header` to match a customized `RetryConfig.retry_header`)
  before publishing.
- **`PydanticDecoder` no longer skips validation for a non-dict payload.**
  `decode()` had an `isinstance(data, dict)` guard that skipped
  `model_validate` entirely for a non-dict top-level parse result (a
  producer sending a JSON array/string/number instead of an object) and
  silently handed the handler raw, un-validated data instead. Pydantic's
  own `model_validate` already raises a clean `ValidationError` for a
  non-dict input — the guard only suppressed the exact validation this
  decoder exists to provide over `DataclassDecoder` for untrusted input.
  Removed; `model_validate` now always runs when `target_type` supports it.
- **Log redaction now catches compound secret-bearing key names.**
  `LoggingConfig.redact_keys` matching was exact-string-only: a configured
  key like `token` did NOT match a target key that normalizes to
  `auth_token`, so common compound names (`x-auth-token`, `session-token`,
  `x-secret-key`, `bearer-token`, `api-token`, ...) were logged in the
  clear even though `auth`/`token`/`secret` are each individually in
  `DEFAULT_REDACT_KEYS`. Matching is now word-set based: a target key
  matches when it contains ALL of some configured key's underscore-
  separated words, checked per configured key (not pooled into one flat
  word set — pooling would turn `api_key`'s `key` into a standalone
  matcher and misfire on benign fields like `primary_key`/`cache_key`).
  The documented depth limit (top level + one nested dict) is unchanged.
- **`configure_structlog()` now bridges Python's `warnings` module into
  `logging`/`structlog`.** All 37 of rabbitkit's `warnings.warn()` calls
  (topology drift, retry-without-confirms, unsafe TLS, dashboard auth,
  `lazy=True`, ...) previously wrote straight to `sys.stderr` in their own
  format, completely bypassing whatever log pipeline `LoggingConfig` set
  up — a "loud warning" was only actually loud if something was watching
  raw stderr in dev; in a production JSON-logging deployment (rabbitkit's
  own recommended config) it was invisible unless the application
  separately called `logging.captureWarnings(True)` itself (undocumented).
  New `LoggingConfig.capture_warnings` (default `True`) calls
  `logging.captureWarnings()` during `configure_structlog()`; set `False`
  if your application already manages this itself.


- **Async broker: in-flight handlers abandoned past `graceful_timeout` are
  now cancelled and nacked, not silently left unacked.** This already
  worked for the pooled-worker path (`AsyncWorkerPool.stop()`); the
  *inline* path (no `worker_config`, the default) only logged a count and
  disconnected, same as the sync broker's unavoidable behavior — except the
  async inline path *can* safely cancel the task first. `AsyncBroker` now
  tracks inline in-flight tasks (`_inflight_tasks`) and, on drain-deadline
  timeout, cancels + nacks (`requeue=True`) any still-running ones with
  delivery-tag/message-id logging, matching the pooled path.
- **Sync transport: a publish confirm that never arrives now reports
  `PublishStatus.TIMEOUT`, not `PublishStatus.ERROR`.** `_run_on_io_thread`
  bounds the cross-thread `basic_publish` marshal by `confirm_timeout` and
  raises `TimeoutError` on expiry — but `_publish_on_channel` had no
  dedicated handler for it, so it fell through to the generic
  `except Exception` branch. This contradicted `docs/message-safety.md`'s
  documented contract (`TIMEOUT`: "no confirm arrived within
  `confirm_timeout`") and diverged from the async transport, which already
  correctly maps its equivalent `asyncio.timeout(confirm_timeout)` expiry
  to `TIMEOUT`. A caller checking `status == PublishStatus.TIMEOUT` (the
  documented pattern for this exact scenario) silently never saw it on the
  sync transport.
- **Sync transport: a publish confirm that never arrives no longer hangs
  the calling thread forever.** `pika.BlockingChannel.basic_publish()` has
  no timeout parameter, and when running fully inline (the default
  single-worker / pure-producer path — most sync usage), its confirm-wait
  looped via `process_data_events` with no aggregate time bound;
  `confirm_timeout` only ever bounded the *cross-thread marshal* case
  (`worker_count>1`), not this one. A broker that accepted the TCP
  connection but never sent the confirm frame back (disk full, internally
  wedged) hung the publish call forever regardless of configuration. Now
  bounded via a dedicated one-shot helper thread whenever no consume loop
  can be sharing the connection (pure producer, or nothing has consumed
  yet); on timeout the connection is discarded (never closed from the
  timing-out thread — that would itself be a second thread touching a
  `BlockingConnection`, which pika doesn't support) and transparently
  re-established on the next call, the same recovery path as a genuine
  network failure. One case remains genuinely unsafe to bound and is
  unchanged, documented in `docs/message-safety.md`: a publish from the
  connection's owner thread while that same thread is also actively
  driving `start_consuming()`'s dispatch loop (the default single-worker
  consumer publishing a result/retry from inside a handler) — interrupting
  it risks two threads touching the same connection the instant the
  caller gives up and dispatch resumes. Mitigation: `worker_count>1`
  routes a handler's publish through the already-bounded cross-thread
  path instead.
- **`TracedConsumerMiddleware` now warns when tracing can't actually run.**
  Adding it without `obskit` installed (or with obskit installed but
  tracing not configured) silently made every span a permanent no-op with
  zero signal — easily mistaken for "nothing to trace yet" rather than
  "tracing was never active." Now logs a warning once, at construction,
  naming which of the two conditions applies.
- **New counters: `rabbitkit_messages_redelivered_total` and
  `rabbitkit_reconnects_total`** — the last two observability gaps from
  the audit's H5 finding. `messages_redelivered_total` (labeled by
  `queue`) is incremented by `MetricsMiddleware` whenever the broker flags
  a delivery `redelivered=True` — the signal that handlers are dying or
  timing out before acking (crash loops, heartbeat kills), which the
  success/error consume counters alone can't distinguish from ordinary
  traffic. `reconnects_total` counts transport re-connections: both
  transports gained an `on_reconnect(callback)` hook (sync fires on every
  successful connect after the first; async adapts aio-pika's
  `RobustConnection.reconnect_callbacks` on both the publisher and
  consumer connections), and both brokers wire it to the first route
  `MetricsMiddleware`'s collector at `start()` — reconnects were
  previously logged but never counted, so a flapping broker/network was
  invisible to metrics-based alerting.

### Documentation

- **`BatchAcker`'s usage example no longer shows a real thread-safety bug.**
  The docstring (and `docs/guide/full-guide.md`) wired a raw pika
  `channel.basic_ack` directly as `ack_fn` — but `flush_interval_ms`'s timer
  fires `flush()` from a background `threading.Timer` thread, not pika's
  I/O thread, so a verbatim copy of the old example was a real cross-thread
  violation (the exact thing `_run_on_io_thread` exists elsewhere in this
  codebase to prevent). Both examples now wrap `ack_fn` with
  `connection.add_callback_threadsafe`; noted that the async path
  (`asyncio.create_task` on the same loop aio-pika already runs on) has no
  such hazard.
- **`docs/kubernetes.md`** now covers KEDA queue-depth-based autoscaling
  under the HPA section, with a `ScaledObject`/`TriggerAuthentication`
  example and the rationale for why CPU-based HPA is the wrong signal for a
  queue consumer with a slow downstream dependency.
- **`docs/security.md`** gained a "Least-privilege consumers with
  `TopologyMode.PASSIVE_ONLY`" section explaining the RabbitMQ
  `configure`/`write`/`read` permission model and how `PASSIVE_ONLY` lets a
  consumer run without `configure`, with a dedicated topology-owning
  process/credential declaring the real topology instead.
- **`docs/rabbitmq-retry-architecture.md`**'s graceful-shutdown section now
  spells out what happens when a handler is still running past
  `graceful_timeout`: the async broker cancels the task and nacks its
  message (delivery-tag logged, immediate redelivery); the sync broker has
  no safe way to interrupt a thread-bound handler, so it logs a count and
  disconnects, leaving the message unacked for the broker to notice and
  redeliver on its own. Either way, the abandoned handler may still be
  running its own side effects when the redelivered copy starts — handlers
  must be idempotent under at-least-once delivery regardless of transport.
- **`docs/production/checklist.md` brought current with everything this
  release shipped** — it's the "blessed production profile" and several
  items described pre-1.2.0 behavior: the DLQ item still documented the
  old filter-route-only auto-declare (now `reject_without_dlx`
  auto-provisions for every rejecting route); added
  `reject_transient_on_redelivery`, `raise_for_status()` +
  `CONFIRMED`-vs-`SENT` outcome checking, the sync ~0.9k msg/s
  confirmed-publish ceiling → use-`AsyncBroker` guidance,
  `ConnectionConfig.nodes` cluster failover, `credentials_provider`
  rotation, `PASSIVE_ONLY` least-privilege pointer, `QueueMetricsPoller`
  (without which the "alert on DLQ depth" item had no metric to alert
  on), the new `messages_redelivered_total`/`reconnects_total` alerts,
  and partition-aware readiness via `management_client`.

## [0.8.2] — 2026-07-03 <small>(internal milestone, formerly 1.1.1)</small>

### Added

- **`SyncBroker.pump_idle(time_limit=0.05)`** — a **publish-only** broker (no
  registered routes, or one that never calls `run()`/`start_consuming()`)
  had nothing driving `process_data_events()` on its single shared
  connection: `run()`'s consume loop is what incidentally keeps that
  connection's heartbeats serviced (see `sync/transport.py`'s one-connection
  model), so a broker that only ever calls `publish()` could sit idle long
  enough to get heartbeat-timed-out broker-side, only discovering (and
  reconnecting from) the dead connection on the *next* publish attempt.
  Call `pump_idle()` periodically from your own idle loop, on the same
  thread that called `start()`, to reconnect proactively if the connection
  died, service pending heartbeat frames, and refresh the liveness
  heartbeat (`health.broker_liveness`) even though no message was
  delivered. Backed by a new `SyncTransport.ensure_connected()` (a
  no-op-if-already-connected public wrapper for the existing
  `_ensure_connected()`, unlike `reconnect()` which unconditionally tears
  down and rebuilds). The async broker needs no equivalent: `AsyncBroker`
  already establishes both the publisher and consumer connections eagerly
  via `aio_pika.connect_robust()`, which runs its own heartbeat-sending and
  reconnection logic as an independent asyncio task — no manual pump is
  possible or necessary there. Covered by new unit tests (no-op before
  `start()`; calls `ensure_connected()` before `pump()`, in that order;
  refreshes `last_heartbeat`). Regression-verified by reverting and
  confirming all 6 new tests fail (`AttributeError` on the now-missing
  `pump_idle`/`ensure_connected`).

- **Loop Engineering Review implementation** — a full documentation and
  reliability-testing pass following the project's own strategic review
  (`LOOP_ENGINEERING_REVIEW.md`, not tracked in git). Highlights:
  - **README rewritten from 2,290 to ~240 lines**, cut down to the Stable
    Core story only (install, minimal consumer/publisher, retry+DLQ,
    `TestBroker`, FastAPI). Every advanced/experimental feature (RPC,
    locking, signing, results, streams, the dashboard, the management
    client, batch publishing/backpressure, the CLI, AsyncAPI generation)
    moved to `docs/guide/full-guide.md`'s existing numbered sections, with
    the tier each belongs to stated explicitly instead of read flat as
    equally "core." This also caught a real bug in the old README: its
    Quick Start FastAPI example imported from `rabbitkit.integrations.fastapi`,
    a module that has never existed — the correct path is `rabbitkit.fastapi`.
  - **`docs/stability-policy.md` rewritten** from a 2-tier (Stable/Experimental)
    policy that still described the project as "pre-1.0" while shipping
    `1.1.0`, to a 3-tier taxonomy (Stable Core / Advanced Stable /
    Experimental) that retroactively formalizes the `1.1.0` freeze for
    Stable Core specifically, states explicit promotion criteria for an
    Experimental feature to graduate, and demotes `CircuitBreakerMiddleware`
    to Advanced Stable (it's a no-op without an external `obskit`-compatible
    circuit breaker — that dependency was previously undocumented next to a
    "stable" label).
  - **New docs**: `docs/production/idempotency.md` (the "at-least-once
    delivery means your handler can run more than once" contract, with a
    concrete before/after example), `docs/production/checklist.md`
    (a public, scannable pre-production checklist), `docs/troubleshooting.md`
    (symptom → cause → fix), `docs/migration.md` (breaking-change log for
    Stable/Advanced Stable APIs, seeded with the `rabbitkit.aio` →
    `rabbitkit.async_` rename), and a new "Sync vs. async: two different
    connection models" section in `docs/guide/full-guide.md` explaining why
    `pump_idle()` exists on `SyncBroker` but has no `AsyncBroker` equivalent.
  - **`docs/security.md` gained a safe-defaults table** (Feature | Safe by
    default? | What you must configure) consolidating guidance that was
    previously scattered across docstrings and README prose.
  - **Corrected stale/incorrect examples found while cross-checking
    `docs/guide/full-guide.md` against more recently-fixed behavior**: the
    Message Signing example used non-existent `SigningConfig` kwargs
    (`key=`/`algorithm="sha256"` instead of the real `secret_key=`/
    `"hmac-sha256"`); the nonce-cache description still described the L4-era
    vulnerable behavior (evict-oldest-10%) instead of the fixed
    reject-when-full-of-live-entries behavior; the Retry section didn't
    mention retry-count header clamping, the auto-install-middleware
    behavior, or the quorum-queue `x-delivery-limit` backstop; the
    Deduplication section didn't mention the `RetryMiddleware`-composition
    sentinel; the Health Checks section didn't mention the `blocked` field.
  - **New tests**: `tests/unit/test_public_api.py::TestSyncAsyncBrokerAPIParity`
    (asserts `SyncBroker`/`AsyncBroker`'s public method surface stays in
    sync, with an explicit, comment-justified allowlist for the two real
    asymmetries — `pump_idle`/`request_shutdown`); `tests/unit/test_readme_examples.py`
    (extracts every ```` ```python ```` block from README.md and checks
    syntax validity + that every `from rabbitkit... import X` resolves to a
    real symbol, plus fully executes the `TestBroker` example end-to-end —
    verified this actually catches a regression by re-introducing the
    `rabbitkit.integrations.fastapi` bug and confirming the test fails);
    two new real-broker integration tests in `tests/integration/test_real_rabbitmq.py`:
    a quorum-queue `x-delivery-limit` test proving the broker-side backstop
    dead-letters an endlessly-`nack(requeue=True)`'d message with **no**
    app-level `retry=` configured at all, and `DLQInspector.replay()`
    coverage against a real broker (previously only unit-tested against a
    mock transport) including its `predicate=` filter.
  - **CI**: a new "Docs examples" step in `ci.yml` running
    `test_readme_examples.py` as its own visible checkpoint (excluded from
    the coverage-gated unit run so a failure reads unambiguously as "the
    README is wrong," not "some unit test broke"); a new nightly
    `dependency-matrix.yml` workflow testing both the pinned-minimum and
    latest-resolved versions of `pika`/`aio-pika` (2×2 matrix) — building
    this surfaced a real bug (see Fixed, below).
  - **`.gitignore`** already covered the local review doc; no change needed
    there.

### Fixed

- **`DLQInspector.replay()`/`replay_async()` could hang forever when a
  `predicate=` filter rejected any message.** Both methods nacked
  (`requeue=True`) non-matching messages *immediately*, inside the same
  still-running `while True: basic_get(...)` fetch loop. With nothing else
  consuming from the queue, requeuing mid-loop meant the very next
  `basic_get()` call could immediately re-fetch that exact message —
  an infinite cycle for any predicate that ever returns `False`. Existing
  mock-transport unit tests in `tests/unit/test_dlq.py` never caught this
  because the mock doesn't simulate real broker requeue/redelivery timing;
  it only surfaced once a new real-broker integration test
  (`test_dlq_inspector_replay_async_real_transport`, exercising
  `predicate=`) was run against an actual RabbitMQ instance, where it hung
  for 73 seconds before failing with `asyncio.TimeoutError`. Fixed by
  collecting non-matching messages into a list during the fetch loop and
  only nacking them with `requeue=True` after the loop has fully exhausted
  the queue (`basic_get()` returns `None`) — held-but-unsettled messages
  are invisible to further `basic_get()` calls, so this guarantees
  termination regardless of how many messages the predicate rejects.
  Verified against a real broker: both
  `test_dlq_inspector_replay_sync_real_transport` and
  `test_dlq_inspector_replay_async_real_transport` now pass, and the
  existing mock-based `tests/unit/test_dlq.py` suite still passes unchanged
  against the refactored implementation.
- **`aio-pika>=9.0.0` (the documented minimum) is broken on any modern
  Python environment** — `aio-pika==9.0.0` imports `pkg_resources` at
  module load time, which `setuptools>=81` no longer ships (`pkg_resources`
  is being removed entirely). Any fresh install that happens to resolve to
  exactly `9.0.0`, or an old lockfile pinning it, gets a bare
  `ModuleNotFoundError` importing rabbitkit's own async transport — through
  no fault of rabbitkit's code. `9.1.0` and later don't have this problem.
  Found by actually running the new dependency-matrix job against the
  stated minimum rather than only ever testing whatever pip currently
  resolves. Bumped the documented minimum from `aio-pika>=9.0.0,<10.0.0` to
  `aio-pika>=9.1.0,<10.0.0` in `pyproject.toml`, updated the README's
  Compatibility section and `docs/troubleshooting.md` accordingly, and
  corrected the new dependency-matrix job's own "minimum" matrix leg to
  `9.1.0` (it would otherwise permanently red on a problem that has nothing
  to do with rabbitkit).

## [0.8.1] — 2026-07-01 <small>(internal milestone, formerly 1.1.0)</small>

### Added

- **`AsyncBatchPublisher`** — transparent batch publish for `AsyncBroker`. Pass
  `batch_config=BatchPublishConfig(...)` to the broker constructor; all
  `broker.publish()` calls are transparently coalesced into batches. Each batch
  is published on a single pooled channel with confirms gathered concurrently,
  amortising the per-message confirm round-trip at high concurrency. Exported
  from the top-level package.
- **`BatchPublishConfig.flush_workers`** — number of concurrent flush-loop
  workers (default `0` = auto: `min(16, max_in_flight // batch_size)`). Each
  worker holds one channel for its lifetime (eliminates per-batch
  acquire/release overhead). The broker automatically caps the auto-computed
  value at `pool_size // 2` so at least half the pool remains available for
  retry/direct publishes.
- **`PoolConfig.prewarm_channels`** (`bool`, default `False`) — when `True`,
  `connect()` pre-creates all `channel_pool_size` channels concurrently so
  channels are hot before the first `publish()` call, eliminating warmup jitter.
- **Retry fast-exit bypass** — `RetryMiddleware` sets `_bypass=True` when
  `max_retries=0`, skipping the try/except wrapper entirely on the hot path.

### Version integrity (M1)

- **Cut 1.1.0** — this entire section was accumulating under `[Unreleased]`
  above a dated `## [1.0.0] — 2026-06-29`, while nearly every source file
  had post-1.0.0 changes on this branch (new public API —
  `AsyncBatchPublisher`, `BatchPublishConfig.flush_workers`,
  `PoolConfig.prewarm_channels`, `PublishStatus.SENT`,
  `RabbitMessage.disposition` — plus numerous fixes, several of which are
  documented, intentional behavior changes: H10's optional DI markers,
  M4's `PublishStatus.SENT`, M11's `MANUAL` no-longer-auto-acking). A tree
  with real new features and behavior changes still labeled with the
  already-tagged 1.0.0 version is a version-integrity break — installing
  "1.0.0" from this state would not match the tagged 1.0.0 release.
  Bumped `_version.py` and `pyproject.toml` to `1.1.0` (new features +
  behavior changes warrant a minor bump per SemVer, not a patch), and
  moved everything that had accumulated under `[Unreleased]` into this
  `[1.1.0]` section, leaving a fresh empty `[Unreleased]` above it for
  subsequent work.

### Fixed

- **Result-publish failure nack(requeue=True) re-runs the whole handler,
  including any side effects it already performed (L1, low)** — if a
  handler succeeds (e.g. charges a card) but its result publish then
  fails, the message is requeued and, on redelivery, the handler runs
  again from scratch — a duplicate charge, or a hot loop on a sustained
  publish outage. This is the same at-least-once/idempotency tradeoff
  documented extensively in `docs/rabbitmq-retry-architecture.md`
  (handlers must be idempotent under redelivery) — not new, but
  previously silent. A full fix (routing this specific failure through a
  guaranteed-DLQ path, or a transactional outbox) requires either
  broker-retry-topology info not available at this layer or an
  application-level outbox, both out of scope for a low-severity fix.
  Instead: `message.redelivered` is now checked before logging a failed
  result publish — if this message has already been redelivered once and
  the publish is STILL failing, the log escalates from WARNING to ERROR
  with an explicit "verify broker health and handler idempotency" hint,
  so a sustained outage is loud and alertable instead of a stream of
  routine-looking warnings. Settlement behavior (`nack(requeue=True)`) is
  unchanged — escalating to `reject(requeue=False)` here was considered
  but rejected: without a guaranteed DLQ (only present for retry-enabled
  or manually-DLX'd routes — see H6), that would silently discard the
  message on plain routes, trading a loud, recoverable hot loop for a
  silent, unrecoverable loss. Covered by new unit tests (sync + async:
  ERROR logged with `redelivered=True`, WARNING with `redelivered=False`).
  Regression-verified by reverting and confirming both new tests fail.
- **Retry was inert on the config-driven path (C1, critical)** — setting
  `RabbitConfig(retry=...)` or `@subscriber(retry=...)` previously declared the
  delay/DLQ *topology* but never installed `RetryMiddleware`, so transient
  failures `nack(requeue=True)`'d in a hot loop, the delay queues never received
  anything, and `max_retries` was never enforced. Both brokers now auto-install
  `RetryMiddleware` (outermost) on retry-enabled routes, driven by the same
  `retry=` switch. Idempotent: a `RetryMiddleware` you add to `middlewares=[...]`
  yourself is respected and not duplicated. `TestBroker` mirrors this so the
  behaviour is unit-testable without a real broker.
- **Exhausted transient retries hot-looped instead of dead-lettering** — when
  retries were exhausted on a *transient* error, `RetryMiddleware` re-raised a
  terminal-tagged exception that the pipeline's AUTO policy then re-classified as
  transient and `nack(requeue=True)`'d back onto the queue forever. The pipeline
  now honours the `_rabbitkit_terminal` marker and dead-letters terminal failures
  (`reject(requeue=False)` → source-queue DLX → DLQ) for all non-MANUAL policies.
- **Channel pool starvation with batch mode + retry** — `AsyncBatchPublisher`
  workers hold their channels permanently. With the previous auto-formula,
  default config (pool_size=10) spawned 10 workers that consumed all 10 pool
  channels, causing retry middleware's `transport.publish()` calls to block for
  10 s then timeout. The broker now caps auto flush_workers at
  `max(1, pool_size // 2)`, always leaving headroom for non-batch uses.
- **Stale closed-channel reuse** — `_publish_on_channel` closes the channel on
  confirm timeout but returns `PublishOutcome(TIMEOUT)` rather than raising, so
  the old `_flush` never noticed the closure. The worker now checks
  `channel.is_closed` after each flush and re-acquires before the next batch.
- **`RPCClient`/`AsyncRPCClient` were non-functional against a real broker (C2,
  critical)** — the reply consumer on `amq.rabbitmq.reply-to` violated two hard
  AMQP rules: the sync transport registered it with manual-ack
  (`auto_ack=False`) and the async transport issued a `Queue.Declare` against
  it (even passive), both of which the broker rejects for this pseudo-queue.
  `consume()` on both transports now accepts `no_ack`/`declare` kwargs; the RPC
  clients pass `no_ack=True, declare=False`. A second, deeper rule surfaced
  once that was fixed: RabbitMQ requires the request publish and the reply
  consumer to happen on the *same channel*, or it raises `PRECONDITION_FAILED -
  fast reply consumer does not exist`. Both transports now track the channel
  that registered the direct reply-to consumer and transparently route matching
  publishes onto it — `RPCClient`/`AsyncRPCClient` and their callers need no
  changes. Validated end-to-end against a real broker (not a mock) via
  `RPCClient.call()`/`AsyncRPCClient.call()` and `broker.request()`.
- **`broker.publish()` bypassed all publish-side middleware (C3, critical)** —
  `publish_scope`/`publish_scope_async` only ever composed for a route's
  HANDLER-RETURN-VALUE publish (`@publisher`/RPC replies, Contract 5).
  `broker.publish()`, the primary producer API, went straight to the transport
  with zero middleware applied, so e.g. `SigningMiddleware` never signed
  anything sent via direct publish. Both brokers now accept a `middlewares=`
  constructor param applied to every `broker.publish()` call (composed via new
  `HandlerPipeline.compose_broker_publish_sync`/`_async`, cached like the
  existing route-level chains); exposed via `broker.publish_middlewares`.
  Middleware wraps outside flow control and (async) batching, so the
  transformed envelope is what gets rate-limited/batched/sent. Validated
  end-to-end against a real broker with `SigningMiddleware` on both brokers.
- **`CompressionMiddleware` was dead code — never compressed anything (C4,
  critical)** — `transform_envelope()` (the method that actually compresses an
  envelope and sets `content_encoding`) had zero callers anywhere in the
  pipeline: it implemented neither `publish_scope` nor `publish_scope_async`,
  so attaching it to a route or to `broker.publish_middlewares` (C3)
  compressed nothing. `CompressionMiddleware` now implements both hooks,
  delegating to the existing `transform_envelope()` — this wires it into the
  route-level Contract-5 result-publish chain *and* the broker-level direct-
  publish chain (C3) with no other changes needed. The consume-side
  `on_receive`/`on_receive_async` decompression path was already correct
  whenever the middleware was attached to a subscriber; only the publish side
  was inert. Validated end-to-end against a real broker: `broker.publish()`
  actually compresses on the wire, and a subscriber with the middleware
  attached decompresses automatically. Rewrote the prior "roundtrip" test,
  which manually called `transform_envelope()`/`gzip.decompress()` outside the
  pipeline and so never exercised (or caught the absence of) real wiring.
- **Graceful shutdown drained the worker pool before cancelling consumers (C5,
  critical)** — both brokers' `stop()` called `worker_pool.stop()` (which waits
  for in-flight work, up to the full `graceful_timeout`) *before* cancelling
  consumers, so the consumer stayed active for the entire wait. A message
  delivered in that window was submitted to a pool already mid-shutdown: sync
  either raised an uncaught `RuntimeError` from `SyncWorkerPool.submit()` or,
  once `.stop()` had fully returned, silently ran the handler *inline* on the
  pika I/O thread; async's `AsyncWorkerPool.submit()` creates a task
  unconditionally (it never checked `_running`) and would add it to a
  `_tasks` set `.stop()` had already cleared — an orphaned task nothing would
  ever await. Either way the message was never cleanly settled before
  `disconnect()`. `stop()` now cancels all consumers *first* in both brokers,
  so the pool only ever drains work that was already in flight — closing off
  new deliveries before touching the pool at all. Validated with an explicit
  call-order unit test (regression-checked: reverting the order makes it fail)
  and a real-broker integration test that calls `stop()` deliberately early
  under load and confirms every published message is eventually processed —
  none permanently lost, whether by the original broker or a follow-up
  consumer picking up whatever was left queued or abandoned at the deadline.
- **Unroutable `mandatory=True` publishes were reported CONFIRMED, never
  RETURNED (H1, high)** — `PublishStatus.RETURNED` existed on the enum but no
  transport ever produced it. Sync's `basic_publish()` can only raise
  `UnroutableError`/`NackError` when the channel has `confirm_delivery()`
  enabled; on the (default) non-confirm path a `mandatory=True` publish to a
  missing binding was unconditionally reported CONFIRMED. Async needed both
  `publisher_confirms=True` *and* `on_return_raises=True`; the fast path (no
  confirms) and the regular confirm pool (confirms only if
  `confirm_delivery=True`, and even then without `on_return_raises`) could
  each silently resolve a return as success. Sync now upgrades whichever
  channel a `mandatory=True` publish lands on to confirm mode on demand
  (idempotent, tracked per-channel) regardless of the broker's global
  `confirm_delivery` setting — this also covers the RPC direct-reply-to
  channel "for free" since the upgrade is channel-agnostic. Async now routes
  every `mandatory=True` publish (outside of direct-reply-to) through a
  dedicated, always-confirmed channel with `on_return_raises=True`. Both
  transports now map an unroutable return to `PublishStatus.RETURNED` and a
  broker `Basic.Nack` to `PublishStatus.NACKED`, so `PublishOutcome.ok` is
  `False` for either — retry-publish and result-publish paths that key off
  `.ok` automatically treat a lost mandatory publish as a failure with no
  further changes. Known gap: an RPC request that is *also* `mandatory=True`
  (a narrow combination) still uses async's non-upgradable reply-to channel
  and can silently report success on a return; sync has no such gap. Validated
  against a real broker: publishing `mandatory=True` to a nonexistent binding
  on both transports, with `confirm_delivery` both `True` and `False`, always
  returns `RETURNED`, never `CONFIRMED`.
- **Sync worker-pool acks could run inline, cross-thread, on the pika
  connection during shutdown drain (H2, high)** — `SyncTransport.
  _run_on_io_thread()` fell back to running a channel call *inline* whenever
  `not self._consuming`, on the theory that nothing was left to marshal onto.
  But `_consuming` goes `False` the instant the consume loop stops pumping —
  including for the entire window between consumers being cancelled and
  `SyncBroker.stop()`'s worker-pool drain finishing, while worker threads may
  still be mid-handler. A worker thread's ack/nack/reject in that window ran
  directly against the shared pika `BlockingConnection`/`BlockingChannel` from
  a non-owner thread, unsynchronized with other worker threads acking the
  same consumer channel — confirmed to corrupt the AMQP stream under load
  (`StreamLostError` / `IncompatibleProtocolError` on the next real-broker
  round-trip). `_run_on_io_thread` now gates marshaling on a new
  `_ever_consumed` flag (`True` for the connection's whole lifetime once a
  consume loop has run at all, not just while it's actively pumping) instead
  of `_consuming` — a cross-thread call always marshals once a consume loop
  has ever started, and fails fast with `TimeoutError` rather than falling
  back to an unsafe inline call. To keep those marshaled callbacks from
  simply timing out with nothing left pumping, `SyncTransport.pump()` briefly
  drives the connection's I/O loop, and `SyncWorkerPool.stop()` / `SyncBroker.
  _wait_in_flight()` now poll in short slices calling it between waits — both
  require `stop()` to run on the transport's owner thread, matching `SyncBroker.
  run()`'s existing call pattern. Separately audited (not changed):
  `PublishStatus.CONFIRMED` was already positively backed by pika's own
  `basic_publish()` contract in confirm mode (it blocks and asserts a
  `Basic.Ack` internally before returning; `NackError`/`UnroutableError` are
  raised otherwise) — verified this holds unchanged across the cross-thread
  marshal path with a dedicated test. Validated with a real-broker test that
  drives a worker-pool consumer through a SIGTERM-style drain (cancel
  consumers, then drain the pool) while instrumenting the real pika channel's
  `basic_ack` to record the calling thread — every ack lands on the owner
  thread, never a worker thread; reverting the fix reproduces the exact
  stream-corruption failure against a live broker.
- **`SigningMiddleware`'s HMAC covered only the body — routing key, exchange,
  reply_to, content_encoding were unprotected (H3, high)** — the
  replay-protected signature was computed over `timestamp:nonce:body` only.
  An attacker who could not forge the signature could still capture a
  validly-signed message and re-publish it under a different routing key,
  redirect an RPC reply via `reply_to`, or flip `content_encoding` to hit a
  different decompression path — the signature still verified, and a
  different consumer instance's own nonce cache wouldn't catch the replay
  either. The signature now additionally covers `exchange`, `routing_key`,
  `content_encoding`, and `reply_to` (NUL-delimited, so field concatenation
  can't make two different splits collide), computed from the outgoing
  envelope on publish and from the delivered message's broker-reported
  routing metadata on receive — changing any of those fields on a captured
  message now invalidates the signature even with the body, timestamp, and
  nonce unchanged. This is a breaking change to the signature format for
  anyone using `require_freshness=True` (the default): producer and consumer
  must both be upgraded together, or run with `require_freshness=False`
  during rollout. The legacy body-only path (only reachable with
  `require_freshness=False`, kept for interop with signers that predate the
  freshness headers) is unchanged and remains body-only by design — it has no
  replay protection either and should not be used for security-sensitive
  traffic. Documented exactly what is and isn't covered in the module
  docstring and `docs/security.md`.
- **Replay protection was per-process/in-memory by default, with no warning
  (H4, high)** — `SigningMiddleware`'s default `TTLSetNonceCache` is a plain
  in-process dict. In any multi-process/multi-pod deployment (the norm for a
  consumer with more than one replica) or after a restart, a replayed message
  landing on a *different* worker than the original passed the nonce check —
  the module docstring's "works out of the box" was misleading for exactly
  this common case. Added `RedisNonceCache`, a shared nonce store using an
  atomic `SET NX EX` so two processes racing on the same nonce can never both
  "win" — pass it as `SigningConfig(nonce_cache=RedisNonceCache(redis_client))`
  to share replay state across every process verifying signatures for the
  same producer. `SigningMiddleware.__init__` now emits a `RuntimeWarning`
  whenever the default in-memory cache is left in place with
  `require_freshness=True` (the risky combination this finding describes) —
  it can't detect an actual multi-process deployment, so it fires
  unconditionally for that combination rather than silently claiming
  out-of-the-box protection. Also: `max_skew` (which doubles as the nonce
  replay-window TTL) default tightened from 300s to 60s — shrink further for
  payments/high-value traffic; and the nonce is now always a fresh
  `uuid4().hex`, never derived from the caller-supplied `message_id` (which
  may be reused across retries, weakening the seen-set's uniqueness
  guarantee). Not a release blocker on its own, but documented loudly in the
  module docstring, `docs/security.md`, and `docs/guide/full-guide.md` with a
  shared-store recipe — do not rely on the in-memory default for
  multi-process/multi-pod deployments or high-value traffic.
- **Retry-count header was producer-spoofable with no independent cap (H5,
  high)** — `RetryMiddleware._get_retry_count()` read the
  `x-rabbitkit-retry-count` header verbatim from the inbound message with no
  bounds checking. A producer setting it negative reset the effective attempt
  count while also making `_build_retry_envelope()` compute a negative
  attempt number, producing a delay-queue routing key like `orders.retry.-4`
  that was never declared — the retry publish silently targeted a
  non-existent queue on the default exchange and the message was lost rather
  than retried (not merely "resets the counter": it drops the message).
  Setting it absurdly large forced the message straight to the DLQ, skipping
  every retry. `_get_retry_count()` now clamps to `[0, max_retries]`
  regardless of what the header claims, and treats a non-numeric/malformed
  value the same as missing (`0`) rather than raising and crashing the
  pipeline mid-exception-handling. This makes `max_retries` an enforced
  ceiling independent of the header's trustworthiness. Documented (not
  implemented as a default) a broker-enforced backstop on top of this: prefer
  quorum source queues with `x-delivery-limit` — see
  `docs/retry-and-dlq.md`. Validated against a real broker: a spoofed huge
  count dead-letters on the very first delivery (no retry happens at all) and
  a spoofed negative count clamps to 0 and retries through the real,
  declared `retry.1` delay queue rather than being silently dropped.
- **Filter-rejected messages could be silently lost with no DLX (H6, high)**
  — `filter_fn` returning `False` settles the message with
  `nack(requeue=False)`, which relies on a dead-letter-exchange to preserve
  it. A DLX was only ever wired onto the source queue when retry was
  enabled (`RetryRouter` sets it as part of the retry topology); a route
  with `filter_fn` but no retry and no manually-configured
  `dead_letter_exchange` had no DLX at all, so RabbitMQ just discarded the
  rejected message — no error, no trace, one filtered message is enough to
  hit it (no retries needed). Both brokers' `_declare_topology()` now
  auto-declare a `<queue>.dlq` and wire the source queue's DLX to it
  whenever a route has `filter_fn` set, retry is disabled, and no manual
  `dead_letter_exchange` is already set — a route with retry enabled, or one
  that already configured its own DLX, is left untouched (no double-DLQ, no
  override of a user's own routing). A `RuntimeWarning` is emitted noting the
  auto-declared queue name, so the extra topology isn't a surprise.
  Validated against a real broker: a filter that rejects every message on a
  route with no retry now reliably lands the rejected body in the
  auto-declared DLQ instead of vanishing.
- **`on_receive` hooks ran in a flat pre-pass, breaking documented
  outer→inner composition (H7, high)** — every route middleware's
  `on_receive`/`on_receive_async` ran in a single flat loop entirely BEFORE
  `consume_scope` was ever entered. Two concrete problems: (a) an exception
  raised in `on_receive` (e.g. `SigningMiddleware` verification,
  `CompressionMiddleware` decompression) was never seen by any middleware's
  `consume_scope` — not even `RetryMiddleware`'s — so it could never be
  routed through the retry delay-queue mechanism, regardless of whether the
  route had retry configured; (b) the pre-pass ran in the SAME (forward)
  order as `publish_scope`'s outer→inner apply order, so a receive-side
  "undo" (decompress) was checked against a body/metadata state that never
  matched what was actually transformed at publish time — combining
  `SigningMiddleware` + `CompressionMiddleware` always failed verification,
  in either registration order. Fixed (b) by running `on_receive` hooks in
  the REVERSE of `middlewares=[...]`'s registration order — the mathematical
  mirror of `publish_scope`'s composition, so a receive-side undo always
  runs relative to the correct publish-side apply. This alone is not
  sufficient for `SigningMiddleware` + `CompressionMiddleware` specifically:
  because the signature covers `content_encoding` (H3), a field
  `CompressionMiddleware` itself sets, only ONE relative order actually
  works — `middlewares=[CompressionMiddleware, SigningMiddleware]`
  (compression outer, signing inner) — now pinned and documented loudly in
  both middlewares' module docstrings and `HandlerPipeline`'s docstring, with
  the reverse order's predictable failure mode also documented and tested
  rather than being a silent mystery. (a) is intentionally NOT
  architecturally changed — an `on_receive` failure means "this delivery is
  untrustworthy or unreadable," not "the handler failed," so retrying
  wouldn't make a bad signature or corrupt payload become valid; it settles
  per the route's `AckPolicy` using the pipeline's default classifier
  instead, which is now documented as the defined (not accidental) behavior
  and covered by a test asserting a signing failure on a
  `[retry_mw, failing_signing_mw]` route is explicitly NOT routed through
  retry's delay-queue mechanism. Validated against a real broker: the
  canonical compression-then-signing order round-trips correctly end-to-end;
  reverting the on_receive-ordering fix reproduces the original failure
  (message never reaches the handler).
- **Dedup + retry composed into silent message loss (H8, high)** —
  `RetryMiddleware.consume_scope`/`consume_scope_async` swallows a transient
  handler failure (routes it to a delay queue, acks the source) rather than
  raising, by design, so an outer `ExceptionMiddleware` doesn't treat a
  retry-in-progress as terminal. From an OUTER middleware's point of view,
  though, `call_next(message)` then returns normally either way —
  indistinguishable from the handler actually succeeding.
  `DeduplicationMiddleware(mark_policy="on_success")` listed outer of
  `RetryMiddleware` (the order README itself recommended) would mark the
  message processed on the *failed* first attempt; the real retry delivery
  (same dedup key — `message_id` is preserved across the retry envelope) was
  then dropped as a duplicate and never actually processed — silent loss on
  an ordinary retry, no concurrency or edge case required. Added
  `rabbitkit.core.types.REQUEUED_FOR_RETRY`, a sentinel `RetryMiddleware`
  now returns instead of `None` whenever a failure was requeued (delay-queue
  publish, or nack+redeliver if that publish itself failed) rather than
  actually succeeding. `DeduplicationMiddleware` checks for it under both
  `mark_policy` values — `"on_success"` skips marking; `"on_start"`
  retroactively deletes the key it had to mark before the handler ran — so
  the composition is now correct regardless of which of the two is listed
  first. Any custom middleware with similar "mark as done" side effects
  wrapping a route that may contain a `RetryMiddleware` should check for the
  same sentinel. Validated against a real broker: a handler that fails once
  then succeeds is retried and processed exactly once, not dropped;
  reverting the sentinel reproduces the original silent-loss failure.
- **Sync `TimeoutMiddleware` settlement race with the abandoned handler
  thread (H9, high)** — on timeout the spawned worker thread running
  `call_next` is abandoned (daemonized, never joined again) but keeps
  running; if it later calls `message.ack()`/`nack()`/`reject()` that call
  went straight to the pika channel from a non-owner thread — a TOCTOU
  double-settle racing whatever the consumer thread does with the same
  message, plus a channel op from a thread pika never expects, with a risk
  of deadlock if it lands while the consumer thread is blocked in
  `thread.join()` instead of pumping `process_data_events()`. Fixed with a
  capture-and-replay guard installed on the message's settlement fns for
  the duration of `consume_scope`: calls from any thread OTHER than the
  spawned worker (i.e. the consumer thread's own legitimate settlement)
  always pass straight through; calls from the worker thread before a
  timeout is declared are captured and replayed for real on the consumer
  thread once `call_next` returns normally; calls from the worker thread
  AFTER a timeout is declared are discarded by raising an internal
  `_DiscardedSettlement` sentinel (not returning), which is what keeps
  `RabbitMessage.ack()`/`nack()`/`reject()` from marking the message
  settled for a call that never actually touched the channel — a plain
  `return` there would silently block the consumer thread's own later,
  real settlement via the idempotent disposition guard. The real settlement
  fns are deliberately never restored once a timeout fires, since the
  worker thread may still call them arbitrarily later; the guard's
  thread-identity check is what safely routes the consumer thread's own
  subsequent settlement through regardless. Covered by 7 new deterministic
  unit tests (real threads + `threading.Event` synchronization, no timing
  flakiness) asserting in-time handlers settle for real on the consumer
  thread and abandoned-thread settlement after timeout is discarded, not
  double-applied. Validated against a real broker with a route combining
  `TimeoutMiddleware` and a slow handler — no protocol/channel errors.
- **DI `Header`/`Path`/`Context` had no way to express "optional" — a
  missing value raised a bare `KeyError` that gets classified PERMANENT and
  rejected straight to the DLQ (H10, high)** — there was no way to declare
  `Annotated[str | None, Header("x-tenant")] = None`, and even where a
  parameter had its own Python default, that default was silently ignored
  for DI-marker-annotated parameters (the marker was always resolved
  unconditionally). A missing header/path segment/context key was
  therefore indistinguishable from a genuine handler bug, and always
  fatal. Fixed by giving `Header()`/`Path()`/`Context()` an optional
  `default=` kwarg, and by checking the handler's own Python parameter
  default when the marker has none. A new `MissingDependencyError`
  (PERMANENT) replaces the bare `KeyError`, naming both the marker and the
  parameter so a genuinely-required-but-missing value is immediately
  actionable. Fallback order: marker's own `default=`, then the parameter's
  Python default, then `MissingDependencyError`. Covered by unit tests for
  the markers' new `default`/`has_default` and updated equality, the
  resolver's full fallback matrix (marker default / function default /
  both / neither, sync and async, `Header`/`Path`/`Context`), and
  `TestBroker` integration tests proving a handler runs with the default
  when the value is missing and a truly-required-and-missing value rejects
  with the new typed error. Regression-verified by reverting the fallback
  and confirming the optional-value tests fail while the required-missing
  test still passes.
- **Async graceful shutdown from the signal handler was fire-and-forget when
  `AsyncBroker` is used directly, without `RabbitApp` (H11, high)** —
  `_on_signal` only ever did an unawaited `loop.create_task(self.stop())`.
  Nothing joined that task, so whether in-flight messages actually finished
  draining depended on incidental event-loop lifetime: `asyncio.run()`
  cancels outstanding tasks once its coroutine returns, which could cut the
  drain (and whatever handler was still running) short. `RabbitApp.run_async()`
  already awaited `stop_async()` correctly; the broker's own signal path did
  not have an equivalent. Fixed by adding `AsyncBroker.run()` — the
  direct-use equivalent of `RabbitApp.run_async()` — which starts the
  broker, awaits an internal shutdown event, then awaits `stop()` itself, so
  the coroutine does not return until the drain has actually completed.
  `request_shutdown()` triggers the same drain from any context (e.g. a
  failing health check), mirroring `RabbitApp.request_shutdown()`. The
  existing signal handler now sets the shutdown event and skips its old
  fire-and-forget task only while `run()` is actively awaiting it, so a
  signal received under bare `start()` usage (without `run()`) keeps its
  pre-fix best-effort behavior unchanged. `on_app_shutdown` (H-SRE5) keeps
  working unmodified for the single-signal-owner case where `RabbitApp`
  drives shutdown instead. Covered by 4 new unit tests (`run()` doesn't
  return until `stop()` completes; a signal during `run()` doesn't
  double-schedule `stop()`; `request_shutdown()` outside `run()` falls back
  to the old fire-and-forget task; a stale shutdown event from a prior
  start()/stop() cycle is cleared on the next `start()`), plus a real-broker
  integration test with a handler that sleeps past the shutdown request —
  `run()` only returns once the handler has fully completed and acked.
  Regression-verified: reverting `run()` to not await the shutdown event
  makes the integration test fail with the handler still in-flight when the
  coroutine returns; restoring the fix passes again.
- **`AsyncWorkerPool.submit()` orphaned a delivery instead of settling it when
  called while the pool wasn't running (H12, high)** — `submit()` ignored
  `_running` entirely: a delivery callback that fired after `stop()` had
  already cleared `_tasks` (or before `start()`) would still
  `asyncio.create_task(...)` unconditionally, adding a task nothing would
  ever await — an orphaned coroutine racing the event loop's own shutdown,
  with the message never cleanly settled. Fixed: `submit()` now nacks
  (`requeue=True`) and returns instead of scheduling that task whenever
  `_running` is `False`. Separately, a handler still running at the
  `stop_timeout` deadline is *abandoned*, not killed (`SyncWorkerPool`'s
  daemon thread keeps running in the background — Python cannot forcibly
  stop an arbitrary thread; `AsyncWorkerPool` cancels the task, which does
  not guarantee the handler reached its own ack/nack, since `CancelledError`
  is a `BaseException` and skips right past the pipeline's `except
  Exception` handling, leaving the message unsettled). `AsyncWorkerPool.stop()`
  now explicitly nacks (`requeue=True`) any message whose cancelled task
  never settled it, so redelivery is immediate and observable rather than
  depending on the implicit requeue that happens only once the connection
  eventually closes. Both pools now log every abandoned delivery by
  `delivery_tag`/`message_id` (previously just a bare count), and
  `WorkerConfig.stop_timeout`'s docstring documents the contract prominently:
  it must exceed the slowest handler's expected run time and should be a
  few seconds under `terminationGracePeriodSeconds`; handlers must be
  idempotent under at-least-once delivery regardless, since an abandoned
  handler's side effects may still complete after abandonment. Covered by 8
  new unit tests (abandoned message nacked + logged by delivery tag; an
  already-settled message is never double-touched; `submit()` after
  `stop()` nacks instead of orphaning, and is a no-op for an already-settled
  message; the sync pool logs delivery tag/message id for a handler
  submitted via the public `submit()`), plus a real-broker integration test
  with a pre-ack side effect that outlives `stop_timeout`, proving the
  message is redelivered to a follow-up consumer rather than lost. Note:
  that integration test passes with or without the explicit-nack code,
  since `AsyncBroker.stop()` always disconnects the transport right
  afterward and RabbitMQ auto-requeues unacked deliveries on connection
  close regardless — the deterministic unit tests (which mock the nack fn
  directly) are the actual regression proof for that part of the fix; the
  `submit()`-refuse fix (a genuine bug with no such implicit safety net) is
  unit-test-verified the same way.
- **`ResultMiddleware` stringified arbitrary/exception results with
  `json.dumps(default=str)` — a stored exception was indistinguishable from
  a valid result (H13, high)** — `default=str` silently coerces ANY
  non-JSON-native object into a lossy string with no marker that it wasn't
  a real result; a handler that returned an exception instance as data
  (not by raising it) was stored as a plain string identical in shape to a
  legitimate string result. Fixed: the default path is now strict
  (`json.dumps(result)`, no `default=str`) — a genuinely unencodable object
  raises `TypeError` instead of being stringified. An exception instance
  specifically gets an explicit, marked error envelope
  (`{"__rabbitkit_error__": true, "type": ..., "message": ...}`) rather
  than either raising or silently stringifying, since "return an exception
  as data" is a legitimate pattern worth preserving unambiguously. An
  explicit `serializer=` still takes priority over both. Covered by 6 new
  unit tests (`_serialize({"x": object()})` raises `TypeError` and never
  reaches the backend; `_serialize(ValueError("boom"))` decodes to the
  marked envelope, not a bare string; plain dict/bytes results unaffected;
  custom serializer still wins). Regression-verified by reverting to
  `default=str` and confirming all 4 exception/unencodable-object tests
  fail (2 don't raise where they should, 2 decode to the old bare-string
  shape instead of the envelope).
- **stdlib dataclass JSON decode passed untrusted keys straight into the
  constructor (H14, high)** — `target_type(**parsed)` in both
  `JSONSerializer.decode()` and `DataclassDecoder.decode()` had no field
  filtering: an extra key in the payload raised a bare `TypeError` from the
  dataclass constructor (misclassified PERMANENT, straight to the DLQ) —
  meaning a producer adding a new field broke every consumer until every
  one was upgraded — and a wrong-typed field (e.g. `qty: int` receiving the
  string `"3"`) passed through with no error at all, since stdlib
  dataclasses do no runtime type checking on construction. Fixed the extra-
  key case: both decoders now filter the incoming dict to the target
  dataclass's declared `dataclasses.fields()` before constructing, so an
  unknown key is silently dropped instead of raising. Did NOT add type
  coercion/validation for the wrong-typed-field case — that's what
  `PydanticDecoder`/`model_validate` is for; instead this is now
  **documented prominently** (docstrings on `JSONSerializer` and
  `DataclassDecoder`) as intentional, unvalidated behavior, with an
  explicit recommendation to use Pydantic/msgspec for untrusted input. A
  genuinely wrong shape (e.g. a missing required field) still raises, but
  wrapped as a `TypeError` naming the target dataclass instead of a bare
  constructor traceback. Covered by 7 new unit tests across both decode
  paths matching the finding's exact spec (extra key alone; wrong-typed
  field alone; both together in one payload — all succeed per the
  documented contract, not a raw `TypeError`; missing required field still
  raises, now naming the dataclass). Regression-verified by reverting both
  decoders and confirming all 6 extra-key/wrapped-error tests fail with the
  old bare constructor `TypeError`.
- **No `SECURITY.md` / `CONTRIBUTING.md` / `CODE_OF_CONDUCT.md`; CI had no
  dependency/vuln scanning or release automation (H15, high — `SECURITY.md`
  blocks an OSS release)** — added `SECURITY.md` (private vulnerability
  reporting via email, supported-versions table, disclosure timeline),
  `CONTRIBUTING.md` (dev setup, quality gates, testing conventions, PR
  process), `CODE_OF_CONDUCT.md` (Contributor Covenant v2.1), GitHub issue
  templates (`bug_report.yml`, `feature_request.yml`, a `config.yml`
  pointing security reports away from public issues) and a
  `PULL_REQUEST_TEMPLATE.md`, and `.github/dependabot.yml` (weekly pip +
  github-actions updates). Added `.github/workflows/security.yml`:
  `pip-audit --strict` against installed dependencies and a CodeQL Python
  analysis, both on push/PR to `main` and a weekly schedule so a newly
  disclosed CVE in an existing dependency is caught without a code change.
  Added a `build` job to `ci.yml`: builds the sdist + wheel, runs `twine
  check` (the same check PyPI runs at upload time), and installs the wheel
  into a clean venv with an import smoke test — catches "works from the
  repo checkout but the packaged artifact is broken" before a release
  attempt. Added `.github/workflows/release.yml`: builds and publishes to
  PyPI via trusted publishing (OIDC — no long-lived API token secret),
  triggered by pushing a `v*` tag; requires configuring the trusted
  publisher once on PyPI's project settings before first use. Also added a
  `[project.urls]` table to `pyproject.toml` (Homepage/Documentation/
  Repository/Changelog/Issues), previously missing. Validated all new/edited
  workflow and template YAML for syntax, and ran the new `build` job's exact
  steps locally end-to-end (`python -m build`, `twine check dist/*`, install
  the wheel into a clean venv, `import rabbitkit; print(rabbitkit.__version__)`)
  — this caught a real bug: `[project.urls]` was initially placed directly
  above `dependencies = [...]` in `[project]`, which TOML parses as
  `project.urls.dependencies` (a table swallowing the next key), breaking
  the build; fixed by moving `[project.urls]` after `[project.optional-dependencies]`.
- **No SBOM (software bill of materials) generation in CI (§B OSS-readiness
  checklist item, not tied to a numbered finding)** — `security.yml`'s
  `pip-audit` job now also exports a CycloneDX JSON SBOM (`pip-audit
  --format=cyclonedx-json`, reusing the tool already installed for the
  vulnerability audit rather than adding a new dependency) and uploads it
  as a build artifact, generated only after the audit step passes.
- **Delay-queue dead-letter used the source exchange + queue name as the
  redelivery routing key — silently dropped retries for topic-bound queues
  (M5, medium)** — `RetryRouter.get_delay_queue_definitions()` set a delay
  queue's `x-dead-letter-exchange` to the SOURCE queue's real exchange and
  `x-dead-letter-routing-key` to the queue's own NAME. For a source queue
  bound to that exchange via a topic pattern (e.g. `orders.*.created`)
  rather than literally by its own name, that routing key almost never
  matches the binding — the retried message vanished after the delay
  instead of coming back, and no error was ever raised (the delay-queue
  publish itself succeeded; only the *subsequent* dead-letter-on-expiry
  silently failed to route). Fixed by dead-lettering via the **default
  exchange** instead (`x-dead-letter-exchange=""`) — on the default
  exchange a routing key matching a queue's name always delivers directly
  to that queue, completely independent of the queue's real bindings —
  mirroring the same trick the DLQ routing already used. Covered by
  updated/new unit tests on `RetryRouter.get_delay_queue_definitions()`,
  plus a real-broker integration test with a queue bound via
  `routing_key="orders.*.created"` (not its own name) proving a transient
  failure now retries and succeeds instead of silently vanishing.
  Regression-verified by reverting to the source-exchange dead-letter and
  confirming the integration test times out waiting for the retry.
- **No PRECONDITION_FAILED (406) recovery when declaring topology — an
  ops-created queue/exchange with different arguments aborted startup with
  an opaque channel-closed error (M6, medium)** — declaring a queue/exchange
  whose arguments conflict with an existing one of the same name (e.g. an
  ops-created quorum queue where rabbitkit's config declares classic, or a
  different TTL/DLX) closes the channel with AMQP reply code 406; this
  surfaced as a low-level `pika.exceptions.ChannelClosedByBroker` /
  `aio_pika.exceptions.ChannelPreconditionFailed` traceback with no
  indication of which queue/exchange or argument actually conflicted. Both
  transports now catch this specifically and raise a typed
  `ConfigurationError` naming the conflicting queue/exchange and quoting the
  broker's own message, with guidance (delete/reconcile the existing
  object, adjust the rabbitkit definition to match, or use
  `TopologyMode.PASSIVE_ONLY`). Any other reply code is re-raised as-is —
  not this fix's concern. Covered by new unit tests for both transports
  (queue and exchange, 406 → typed error; a non-406 channel closure still
  propagates unchanged) and a real-broker integration test that pre-declares
  a quorum queue via raw pika then starts a broker with a conflicting
  (classic) definition for the same queue name.
- **Async fast-publish path (`confirm_delivery=False`) reported CONFIRMED
  with no actual broker confirm (M4, medium)** — the non-mandatory publish
  path used when `confirm_delivery=False` returned
  `PublishStatus.CONFIRMED` unconditionally, even though that channel has
  `publisher_confirms=False` and nothing was actually acknowledged by the
  broker — contradicting `PublishOutcome.ok`'s own documented contract
  ("True if the broker confirmed the message"). Combined with
  `RetryMiddleware`'s and the pipeline's result-publish step's internal
  republishes (both of which ack/settle the source message as soon as
  their republish reports `outcome.ok`), a publish silently lost in flight
  right after being written to the socket would still report success —
  the source message already settled, a real loss. Fixed: added a new
  `PublishStatus.SENT` value for the fire-and-forget case (both sync and
  async transports now report it honestly instead of `CONFIRMED`); `.ok`
  still treats `SENT` as "did not fail" (not a regression for existing
  fire-and-forget callers), but code that specifically needs a real broker
  ack can now check `status == PublishStatus.CONFIRMED` directly, which
  `.ok` alone could never distinguish. Additionally, starting a broker with
  `confirm_delivery=False` on a route that has retry enabled, or a
  `@publisher()` result forward, now emits a `RuntimeWarning` naming the
  route and explaining the exact risk, so this combination is no longer
  silent. Covered by updated/new unit tests across both transports (SENT
  vs CONFIRMED for the non-mandatory/non-confirmed path; mandatory publishes
  unaffected), new `PublishOutcome`/`PublishStatus` tests, and new
  broker-level tests for both warnings (retry, and result-publisher) with
  and without confirms. A real-broker integration test proves the fast
  path still delivers correctly end-to-end while honestly reporting SENT.
- **Defined ack/nack/reject/retry/dead-letter metrics were never emitted;
  `handler_errors_total`/`handler_duration_seconds` were pure duplicates of
  metrics already emitted under different names (M2, medium)** —
  `MetricsConfig` defined `messages_acked_total`/`nacked_total`/
  `rejected_total`/`retried_total`/`dead_lettered_total`,
  `handler_errors_total`, and `handler_duration_seconds`, but
  `MetricsMiddleware` only ever emitted the 4 consume/publish counters
  (under different names) — an operator alerting on any of the other names
  got a permanently-empty series. Removed `handler_errors_total`/
  `handler_duration_seconds` entirely (they measured exactly what
  `messages_consumed_total{status=error}`/`message_processing_seconds`
  already cover — dead, confusing duplicates, not worth wiring). Actually
  wired the other five: `RabbitMessage` gained a public `disposition`
  property; `HandlerPipeline` now calls a new
  `MetricsMiddleware.record_settlement()` right after a message's
  disposition becomes final (ack/nack/reject — including the filter-rejection
  path), via a local (not module-level) lazy import so `core/` doesn't gain
  a hard dependency on `middleware/`; `RetryMiddleware` gained optional
  `metrics_collector`/`metrics_config` constructor params (wired by both
  real brokers and `TestBroker` from any `MetricsMiddleware` already
  present on the route) and now emits `messages_retried_total` on a
  successful delay-queue republish and `messages_dead_lettered_total` at
  the point a failure is marked terminal (permanent, or retries exhausted).
  A "pending" disposition (e.g. `MANUAL` policy leaving the message
  unsettled — see M11) emits nothing, since there's nothing final to
  report. Covered by new unit tests on `RabbitMessage.disposition`,
  `MetricsMiddleware.record_settlement()`, the pipeline's settlement-metric
  wiring (ack/nack/reject/filter-rejection/MANUAL-pending), and
  `RetryMiddleware`'s retried/dead-lettered emission (sync + async,
  permanent + exhausted, no-collector no-op), plus a `TestBroker`-based
  end-to-end test proving metrics flow through a full `start()`/`publish()`
  cycle, not just direct pipeline calls. Regression-verified by reverting
  M11's fix and confirming the MANUAL-pending metrics test (and others)
  fail. Note: `publish_total`/`publish_failures_total`/
  `publish_confirm_latency_seconds` are the SAME class of dead metric name
  but were not in this finding's scope — left as-is, noted for a future fix.
- **Metric `queue` label used the raw routing key — cardinality explosion
  risk (M3, medium)** — `MetricsMiddleware`'s consume-side counters/
  histogram labeled `queue` with `message.routing_key`, which for a topic
  or `Path()`-parameterized route can embed an unbounded per-message value
  (tenant id, order id, etc.) — one Prometheus time series per distinct
  value ever seen. Fixed: labels now prefer the BOUND queue name (the
  `x-rabbitkit-original-queue` header, set by the broker's `on_message`
  wrapper before any middleware runs) and fall back to `routing_key` only
  when that header is absent (e.g. a message constructed directly in a
  test rather than delivered through a broker/`TestBroker`). Covered by
  new unit tests (bound queue name preferred over routing key, sync +
  async; existing fallback-to-routing_key tests renamed/kept to document
  the fallback case, not the primary one).
- **`MANUAL` ack policy auto-acked an unsettled message on handler success —
  contradicted "handler owns settlement entirely" (M11, medium)** —
  `_ManualStrategy`/`_ManualStrategyAsync.on_success` did `if not
  msg.is_settled: msg.ack()`, silently acking any MANUAL handler that
  returned without calling `ack()`/`nack()`/`reject()` itself — e.g. one
  that intentionally defers settlement to another task/thread. If the
  process crashed before that deferred settlement actually ran, the
  message was already gone (acked) instead of being redelivered — a real
  loss risk contradicting the class's own documented contract. Fixed: on
  success, an unsettled MANUAL message is now left unsettled (logged as a
  warning, not silently ignored, in case it's actually a bug rather than
  an intentional deferral) — the pipeline never touches it. A handler that
  DOES call `ack()`/`nack()`/`reject()` itself is unaffected. Covered by
  new unit tests (sync + async: success-without-settlement stays pending,
  no ack/nack/reject fn called; success-with-explicit-ack is respected;
  existing direct-strategy-call test renamed and re-asserted for the new
  behavior) and a `TestBroker` integration test with a handler that
  intentionally defers settlement. Regression-verified by reverting and
  confirming 3 tests fail with the message incorrectly acked.
- **`MsgspecSerializer`/`JsonParser` had no input-size cap; `JSONSerializer`'s
  existing cap defaulted to off (M7, medium)** — a large uncompressed body
  was fully materialized during JSON parsing with no bound in either
  serializer, and `JSONSerializer(max_parse_bytes=None)`'s default meant the
  cap it already had wasn't actually protecting anyone who didn't
  explicitly opt in. Added the same `max_parse_bytes` cap (checked before
  parsing, raising `ValueError` naming the limit) to `MsgspecSerializer` and
  `JsonParser`, and changed all three built-ins' default from `None`
  (unbounded) to 64 MiB — matching `CompressionMiddleware`'s
  `max_decompressed_size` default — so a sane bound applies out of the box;
  pass `max_parse_bytes=None` to opt out. Covered by new unit tests across
  both newly-capped serializers (default is 64 MiB not None; oversized
  input raises; within-cap still decodes; explicit `None` opts out) plus an
  updated `JSONSerializer` test that previously encoded the old
  unbounded-by-default behavior. A nesting-depth guard was considered but
  not implemented (`json.loads`/msgspec don't expose one without a custom
  decoder hook) — noted as a possible follow-up, not required by this fix.
- **`MsgspecSerializer.content_type` looked authoritative but decode never
  verifies it; a body/type mismatch surfaced as an opaque
  `msgspec.DecodeError` (M10, medium)** — `content_type` is only ever used
  when *publishing* (set on the outgoing AMQP property); `decode()` never
  checks an incoming message's actual `content_type` against it — decode is
  driven solely by the handler's declared parameter type, matching every
  other built-in serializer here (none of them negotiate content-type on
  consume). Documented this explicitly as advisory-only on
  `content_type`'s docstring, and wrapped `msgspec.DecodeError` with a
  clearer message naming the target type and explicitly hinting at a
  content-type mismatch as the likely cause, instead of a raw low-level
  decode error. Covered by new unit tests (invalid JSON for a Struct/dict
  raises the wrapped error naming the type and mentioning content_type;
  wrong-shape-but-valid-JSON also wrapped; a normal decode is unaffected).
- **Dashboard unauthenticated by default, docs steered to `--host 0.0.0.0`,
  and the bearer-token check used a non-constant-time `!=` (M8, medium)** —
  fixed the bearer check to use `hmac.compare_digest` instead of `!=` (a
  plain string comparison short-circuits on the first mismatched byte,
  leaking timing information about the token). Updated the module
  docstring's Quick start example to recommend `--host 127.0.0.1` by
  default, with an explicit note that `0.0.0.0` requires a
  NetworkPolicy/firewall/reverse-proxy in front — the dashboard exposes
  full broker topology (queue/exchange/routing-key names, consumer counts)
  unauthenticated by default. `create_dashboard_app()` itself has no bind
  parameter (binding happens externally via uvicorn), so there was no
  code-level default to change beyond the auth comparison — the host
  guidance fix is documentation-only. Covered by 2 new unit tests (the
  comparison goes through `hmac.compare_digest`, verified via a spy; a
  wrong-length token is still correctly rejected). Regression-verified by
  reverting to `!=` and confirming the spy-based test fails (the plain
  comparison never calls `compare_digest`).
- **`DeduplicationConfig.fallback_on_redis_error=True` (the default) failed
  open silently — only a `logger.warning`, no metric (M9, medium)** — a
  Redis blip during a dedup check/mark caused the message to be processed
  without idempotency enforcement, logged at `WARNING` (routine-noise
  level) with no way to alert on it beyond log scraping. Kept the
  fail-open default (right for most workloads — availability over strict
  dedup) but: upgraded the log to `ERROR` (an idempotency lapse is an
  operational event worth alerting on, not routine noise) with the message
  now explicitly naming `fallback_on_redis_error=False` as the fail-closed
  alternative; added optional `metrics_collector`/`metrics_config`
  constructor params (mirroring `RetryMiddleware`'s M2 pattern) that emit a
  new `dedup_fallback_total` counter on every fallback, across all 4
  internal fallback sites (sync/async mark, sync/async check). Documented
  the fail-open tradeoff and the fail-closed alternative prominently in
  `docs/security.md`. Covered by new unit tests (ERROR not WARNING; metric
  emitted when wired, sync + async; no-op without metrics wired).
- **`ConnectionConfig`/`ManagementConfig` leaked the plaintext password
  through `repr()`; `.url` embedded it too, with no masked alternative
  (L2, low)** — any log line or traceback that reprs a
  `ConnectionConfig`/`RabbitConfig`/`ManagementConfig` (or nested repr via a
  containing object) leaked the broker/management password verbatim, since
  the dataclass-generated `__repr__` includes every field. Added a shared
  `_masked_repr()` helper (masks named secret fields via
  `dataclasses.fields()`) and gave both configs an explicit `__repr__`
  using it; added `ConnectionConfig.safe_url` — a masked-password variant
  of `.url` safe to log. Covered by new unit tests (repr never contains the
  password; `safe_url` masks it; `url` is unchanged for actual connecting).
- **`RedisLock`'s `ttl` has no auto-renewal — a handler exceeding it loses
  the lock mid-work (L3, low, documentation)** — release is correctly
  atomic (Lua compare-and-delete), but nothing extends the TTL while a
  handler is still running, so a slow handler risks concurrent processing
  of the same key. Documented the tradeoff on `RedisLock`'s docstring:
  size `ttl` above the worst-case handler duration, and use the existing
  `fencing_token()` for downstream write safety when in doubt. No
  code/behavior change.
- **`TTLSetNonceCache` evicted live (unexpired) nonces under flood, enabling
  a flood-to-evict-then-replay attack (L4, low)** — at capacity, the cache
  evicted "the oldest 10%" by insertion order regardless of whether those
  entries had actually expired, so an attacker could flood unique nonces to
  evict a target's still-valid nonce, then replay it successfully. Changed
  `seen()` so that, after reclaiming genuinely-expired entries, if the
  cache is still at capacity (i.e. genuinely full of live nonces), the new
  nonce is now rejected rather than evicting anything live. Covered by new
  unit tests including a dedicated exploit-scenario test modeling the
  flood-then-replay attack directly.
- **`RateLimitMiddleware` dropped messages silently — no log or metric on
  nack/drop/wait-deadline-exceeded; per-process scoping undocumented (L5,
  low)** — added `logger.warning` plus optional `metrics_collector`/
  `metrics_config` wiring (new `rate_limit_dropped_total` counter,
  mirroring the M9 pattern) at all three drop paths in both sync and async
  `consume_scope`. Documented that the effective cluster rate is
  `workers × max_rate` since the token bucket is per-process. (The
  originally-reported "sync wait busy-polls at 10ms / can block forever"
  behavior did not reproduce against current code — both sync and async
  already use a bounded sleep-until-next-token wait with a 30s deadline —
  so only the observability and documentation gaps were addressed.)
  Covered by new unit tests asserting the log and metric fire on each drop
  path.
- **`AsyncRPCClient.close()` had no `_closed` guard, unlike the sync
  `RPCClient`; post-close `call()` could re-register a consumer on a
  torn-down transport, and in-flight callers got a bare `CancelledError`
  (L6, low)** — mirrored the sync client's existing pattern: added
  `self._closed`, guarded both `call()` and `_ensure_consuming()`, and
  changed `close()` to resolve pending futures with `RPCClientClosed`
  (via `close_all(...)`) instead of cancelling them. Covered by new unit
  tests achieving parity with the sync client's existing closed-guard
  tests.
- **`ReplyTooLargeError`'s comment overstated the protection it provides
  (L7, low)** — the `max_reply_bytes` check in `_ReplyRouter.resolve()`
  runs after the reply is already fully buffered into memory, so it
  protects the *caller* from holding an oversized result, not the receive
  path from materializing one. Reworded the comment to describe the actual
  scope of protection; no behavior change.
- **Two import paths for the async broker (`rabbitkit.aio` and
  `rabbitkit.async_`) with no top-level broker export (L8, low)** —
  declared `rabbitkit.async_` the canonical path (majority usage across
  the codebase and docs) and turned `rabbitkit.aio` into a deprecated
  alias that emits `DeprecationWarning` on import but still works.
  Exported `AsyncBroker`/`SyncBroker` from the top-level `rabbitkit`
  package (previously absent despite peripheral configs already being
  exported there, and despite `docs/api/brokers.md` already documenting
  the top-level import as "Recommended"). Updated all
  docs/examples using `rabbitkit.aio` to use `rabbitkit.async_` instead.
  Covered by new tests in `tests/unit/test_public_api.py` (top-level
  broker exports resolve to the same class as the canonical submodule;
  importing `rabbitkit.aio` warns; plain `import rabbitkit` does not warn
  about `aio` as a side effect).
- **Top-level `rabbitkit` namespace re-exported 16 experimental-tier
  symbols (signing, dashboard, RPC, locking, streams, results) already
  declared "NOT covered by the stability guarantee" in
  `rabbitkit.experimental` (L9, low)** — removed all 16 from the top-level
  package's imports/`__all__`; they remain fully available via
  `from rabbitkit.experimental import ...`. Updated the doc examples that
  imported these symbols from the top level. Covered by new tests in
  `tests/unit/test_public_api.py` (each symbol absent from top-level
  `__all__` and `hasattr`, still importable from `rabbitkit.experimental`).
- **`RouteDefinition` (a frozen dataclass) had a monkey-patched
  `__setattr__`/`__delattr__` back door letting `route.consumer_tag = x`
  bypass immutability (L10, low)** — the mutable runtime state was already
  correctly split into a separate `RouteRuntimeState` object
  (`route.runtime_state.consumer_tag`); the back door only existed so
  legacy callers could write `route.consumer_tag` directly. Removed the
  monkey-patch entirely — `RouteDefinition` is now genuinely frozen, and
  `consumer_tag` is a true read-only property. Production code already
  wrote exclusively via `route.runtime_state.consumer_tag = ...`, so this
  is not a behavior change for the broker; only test helpers needed
  updating. Regression-verified by reverting and confirming 3 tests fail
  with the old back door silently accepting the write.
- **Auto-DI could silently mis-bind a `Depends`/`Header`/`Path`/`Context`
  parameter to the message body under postponed annotations (L11, low)**
  — `HandlerPipeline._handler_needs_di` (decides whether a handler needs
  the DI resolver at all) used a weaker, 2-attempt hint-resolution
  strategy than `DIResolver` itself (missing the closure-`localns` retry
  attempt); if hint resolution failed, it fell back to raw
  `__annotations__` strings, which have no `__metadata__` to detect a
  marker on — silently deciding "no DI needed" and letting the fallback
  resolver bind the marked parameter to the body instead. Fixed two ways:
  (1) extracted the hint-resolution logic into a single shared
  `get_type_hints_with_fallback()` in `di/resolver.py`, now used by BOTH
  `DIResolver` and the pipeline's detector, so they can never independently
  drift again; (2) `DIResolver.validate_handler()` (already called for
  every handler at registration time) now raises `ConfigurationError` if
  an annotation textually looks like a DI marker call
  (`Depends(`/`Header(`/`Path(`/`Context(`) but couldn't be resolved to a
  real type at all (e.g. a forward reference to a name only reachable
  under `if TYPE_CHECKING:`) — turning a silent wrong-binding into a loud,
  actionable registration-time error instead. Covered by new unit tests
  (unresolvable Depends/Header markers raise; a similarly-unresolvable but
  marker-free annotation does not; the pipeline detector and `DIResolver`
  agree on ordinary resolvable/marker-free cases). Regression-verified by
  reverting and confirming both new "raises" tests fail (with the fix
  removed, the pre-existing "multiple body-like parameters" check happens
  to also raise, but with a different message not naming the DI marker —
  confirming the specific L11 detection path, not just some error, was
  removed).
- **`Depends`'s docstring cited pre-1.0 milestones for a feature that is
  already implemented (L12, low, documentation)** — "Generator dependencies
  deferred to 0.2.0" was stale; generator/async-generator dependencies with
  post-``yield`` teardown are fully implemented (`DependencyScope`).
  Rewrote the docstring to describe current behavior instead of a
  never-updated roadmap note. No code/behavior change.
- **Public router/subscriber/broker params typed `Any` instead of the real
  protocol (L13, low)** — `middlewares: list[Any]`, `serializer: Any`, and
  `include_router(router: Any)` across `core/router.py`,
  `core/registry.py`, `core/route.py` (`route_middlewares`,
  `serializer_override`), `sync/broker.py`, `async_/broker.py`, and
  `testing/broker.py` meant mypy could never catch a caller passing the
  wrong shape (e.g. a plain object instead of a middleware) — `Any` is
  silently compatible with everything. Retyped to
  `list[BaseMiddleware]`/`Serializer[Any]`/`RabbitRouter` respectively
  (`core/`'s imports of `BaseMiddleware`/`RabbitRouter` are
  `TYPE_CHECKING`-only, preserving the zero-runtime-dependency-on-middleware
  convention already used for `core/pipeline.py`'s metrics integration —
  no new runtime import edges). Also tightened `router.py`'s
  `filter_fn: Callable[[Any], bool]` and the two broker classes'
  `filter_fn: Any` to the already-correct `Callable[[RabbitMessage], bool]`
  used by `registry.py`. Pure type-level change (`from __future__ import
  annotations` means none of this is evaluated at runtime) — no behavior
  change, so the existing test suite is unaffected; the "regression test"
  here is `mypy` itself, verified by checking that a deliberately
  wrong-typed `middlewares=[123]` call is now flagged (`list-item` error)
  where it previously type-checked cleanly under `Any`.
- **Liveness heartbeat only advanced on message delivery — a healthy,
  message-idle consumer could flap to "dead", and a pre-first-message wedge
  reported alive forever (L14, low)** — `last_heartbeat` was set exclusively
  inside each broker's `on_message` callback, so `health.broker_liveness`
  (staleness check: `now - last_heartbeat > wedged_timeout`) would trip on a
  perfectly healthy consumer that simply had no traffic for
  `wedged_timeout` (default 60s); and since `last_heartbeat` was never
  initialized until the first message/tick, a broker wedged from the very
  start had no heartbeat to compare against, so `broker_liveness` (treating
  a missing heartbeat as "no signal available") reported it alive
  indefinitely. Fixed by driving the heartbeat from the I/O loop itself,
  not from message delivery: (1) both brokers now set `last_heartbeat` at
  the top of `start()`, closing the pre-first-message gap; (2)
  `SyncTransport` gained an `on_io_tick(callback)` hook fired once per
  `start_consuming()` loop iteration (i.e. once per `process_data_events()`
  call — a real, existing per-second tick already in that loop, not a new
  poll), wired by `SyncBroker.start()` to refresh the heartbeat regardless
  of whether a message was delivered that iteration; (3) `AsyncBroker`
  (aio-pika has no equivalent manual tick to hook) instead runs a periodic
  `asyncio` task on a 5s interval, started in `start()` and cancelled in
  `stop()`, which itself only keeps ticking if the event loop is not
  wedged. Covered by new unit tests (sync: `on_io_tick` fires once per loop
  iteration and a raising callback doesn't break the loop, mirroring the
  existing `on_blocked`/`on_unblocked` defensive pattern; both: heartbeat
  is non-`None` immediately after `start()`; async: the periodic task
  visibly advances `last_heartbeat` on its own and is cleaned up by
  `stop()`). Regression-verified by reverting and confirming all 6 new
  tests fail (`AttributeError` on the now-missing `last_heartbeat`/
  `on_io_tick`/`_heartbeat_task`).
- **`broker_health_check`/`broker_readiness` never consulted the
  blocked/flow-control state — reported HEALTHY/ready while RabbitMQ had
  paused publishes via a memory/disk alarm (L15, low)** — `connection.blocked`
  is a soft, publish-side flow-control notification on an otherwise-open
  connection, so a blocked broker still looked fully `connected` with no
  other visible symptom; the only place this was tracked at all was the
  opt-in `FlowController` (`broker.flow_controller = fc`), which
  `broker_health_check` never looked at. Fixed by: (1) both
  `SyncTransport`/`AsyncTransportImpl` now passively track blocked state
  themselves (`is_blocked` property, updated in `_pika_blocked`/
  `_pika_unblocked`/`_aio_blocked`/`_aio_unblocked` regardless of whether
  any callback — `FlowController` or otherwise — is registered), so the
  signal exists even without opting into `FlowController`; (2)
  `broker_health_check` now checks `broker.flow_controller.is_blocked`
  first, falling back to `transport.is_blocked`, and downgrades an
  otherwise-HEALTHY result to DEGRADED (checked before the
  consumer/worker-pool checks, as the more actionable root cause) with a
  new `BrokerHealthResult.blocked` field; (3) `broker_readiness` now
  explicitly returns `False` when blocked (a blocked connection can't
  publish, so it isn't ready for traffic even though it's still
  "connected" and may have live consumers) — `broker_liveness` is
  correctly left unaffected, since a blocked connection is not itself a
  liveness failure. Fixing an unrelated MagicMock-default gap surfaced by
  this: `tests/unit/test_health.py`'s `_make_broker` helper built its
  default mock transport without setting `is_blocked`, and since
  `MagicMock` auto-creates any accessed attribute as a (truthy) mock, this
  would have spuriously marked every such test broker "blocked" —
  explicitly set `is_blocked=False` on that fixture's default transport.
  Covered by new unit tests (transport `is_blocked` tracks
  blocked/unblocked with zero callbacks registered; health check
  downgrades via transport-only and via `FlowController`; blocked takes
  priority over a simultaneous missing-consumers reason; readiness is
  `False` while liveness stays `True` when blocked). Regression-verified
  by reverting and confirming all 7 new tests fail (wrong status/missing
  `blocked` field/`AttributeError` on the now-missing `is_blocked`).
- **No secret-redaction hook in structured logging (L16, low)** —
  `core/logging.py`'s `configure_structlog()` built a fixed structlog
  processor chain with no scrubbing step, so any log call (rabbitkit's own
  or the caller's handler code) that included a credential-shaped field
  went out verbatim. rabbitkit's own internal logging never included the
  message body or `headers` dict in the first place (only `message_id`,
  `routing_key`, `queue`, `handler` are bound per message) — that was
  already true but undocumented; now stated explicitly in the module
  docstring. For the real gap — user handler code logging its own fields —
  added `LoggingConfig.redact_keys` (a `frozenset[str]`, enabled by default
  with a sensible credential/secret key list in the new
  `DEFAULT_REDACT_KEYS`, also exported at top level) and a new structlog
  processor that redacts matching keys, checked at the top level and one
  level deep inside nested dict values (covers a `headers={...}` field).
  Matching normalizes AMQP-style `x-`-prefixed/hyphenated header names
  (`x-api-key`) to the same form as the Python-style snake_case defaults
  (`api_key`), so both spellings are caught. Pass `redact_keys=None` to
  disable, or a custom `frozenset` to redact different key names. This is
  a best-effort, key-name-based scrubber, not a content/PII scanner —
  documented as such. Covered by new unit tests (key normalization; the
  processor redacts top-level and nested matches, is case-insensitive, and
  leaves non-matching/non-dict values untouched; `configure_structlog`
  wires the processor into the chain by default and omits it when
  disabled; an end-to-end capture confirming a logged `password` field is
  actually redacted vs. left plain when disabled). Regression-verified by
  reverting and confirming the new tests fail (`ImportError` on the
  now-missing `DEFAULT_REDACT_KEYS`/`_redact_processor`/`_normalize_key`).
- **No pre-commit config; no property-based/security test directories (L17,
  low)** — added `.pre-commit-config.yaml` (ruff with `--fix`, plus a local
  mypy `--strict` hook) mirroring the CI `lint` job exactly, so a local
  failure is never a false negative/positive relative to CI. Added two new
  top-level test categories: `tests/security/` — black-box scenario tests
  using only the public middleware API (never internal helpers) for the two
  attack classes rabbitkit explicitly defends against: signing replay
  (`test_signing_replay.py` — capture-and-resend, cross-worker replay via a
  shared nonce cache, post-capture body tampering, and a false-positive
  sanity check) and decompression bombs (`test_compression_bomb.py` — a
  >1000x-amplification gzip/zstd bomb rejected via the streaming size cap,
  plus a legitimate-payload sanity check); and `tests/property/` — hypothesis
  round-trip tests for `JSONSerializer`/`MsgspecSerializer`
  (`encode`/`decode` recovers arbitrary generated dict/list/str/bytes values
  exactly). The underlying protections were already covered by
  implementation-level unit tests (`tests/unit/middleware/test_signing.py`'s
  `TestReplayProtection`, `test_compression.py`'s zip-bomb guard class) —
  these are additive, framed for discoverability by a security reviewer and
  resilient to internal refactors since they only touch the public surface.
  Added `hypothesis` and `pre-commit` to the `dev` extras. Wired both new
  directories into CI (a new "Security & property tests" step in the `test`
  job) and into the `/gates` skill and `CLAUDE.md`'s command list — a test
  directory nobody runs is dead weight. Verified the security tests are not
  vacuous by simulating a broken nonce cache (always reports "unseen") and
  confirming the replay test's core assertion fails without the real
  protection in place.

### Performance

- **`get_nowait()` fast drain** — after the first blocking `get()` the flush
  loop drains all immediately-available items with `get_nowait()` before
  entering the timed straggler wait. At high concurrency the queue is almost
  always non-empty, eliminating coroutine-per-item overhead for the common case.
- **`asyncio.timeout` in straggler wait** — replaces `asyncio.wait_for`
  (avoids the wrapper-task overhead).

## [0.8.0] — 2026-06-29 <small>(internal milestone, formerly 1.0.0)</small>

Code-quality refactors — Strategy patterns, shared dispatchers, typed protocols,
and performance wins. No public API breaks.

### Added

- **AckPolicy Strategy dispatch** — 4 concrete `AckStrategy` implementations
  replace 4 scattered `if/elif` sites; policies are now pluggable (open-closed).
- **FlowController blocked-policy Strategy** — 3 concrete `_BlockedPolicy`
  implementations (Wait/Raise/Drop) replace 9 inline stringly-typed branches.
- **Shared `TopologyDispatcher`** — `core/topology_dispatch.py` extracts the
  identical TopologyMode dispatch from both transports into one place.
- **RPC `_ReplyRouter` + `concurrent.futures.Future`** — deletes the hand-rolled
  `_PendingCall`; `on_reply` logic defined once; sync/async clients share the router.
- **`HealthProvider` Protocol** — `core/protocols.py` defines a typed interface
  for health checks; `health.py` uses a gradual-migration helper (tries public
  property, then private attr, then default with deprecation warning).
- **`QueueInfo` TypedDict** — `management.py` queue methods return typed dicts
  instead of `dict[str, Any]`.
- **`NoOpMiddleware`** (Null Object pattern) — zero-overhead pass-through middleware.
- **Pool `acquire_ctx` context managers** — `@asynccontextmanager` on
  `AsyncChannelPool`, `@contextmanager` on `SyncChannelPool` (prevents leak footgun).
- **Transport context managers** — `__aenter__`/`__aexit__` on `AsyncTransportImpl`,
  `__enter__`/`__exit__` on `SyncTransport`.
- **MkDocs documentation site** — `mkdocs.yml` + 21 auto-generated API reference
  pages via mkdocstrings + 1,529-line user guide.

### Changed

- **`ContextRepo`: `threading.local()` → `contextvars.ContextVar`** — fixes an
  async correctness bug (concurrent coroutines sharing context on the same thread).
- **Pipeline typing** — `HandlerPipeline.__init__` now takes `Serializer | None`,
  `DIResolver | None`, `ContextRepo | None` instead of `Any`.
- **Hot-path imports** — `from rabbitkit.di.resolver import ...` moved to module
  level (was per-message `sys.modules` lookup).
- **Double-validation removed** — `_deserialize_body` no longer calls
  `model_validate` on dicts returned by the serializer (the serializer is
  responsible for returning the final typed object).
- **`BatchAcker` O(1) `max_tag`** — tracks incrementally instead of
  `max(self._tags)` per flush.
- **`MsgspecSerializer` decoder caching** — `Decoder(type=T)` cached per
  `target_type` instead of codegen per `decode` call.
- **`TTLSetNonceCache` `OrderedDict` LRU** — O(1) eviction instead of O(n)
  scan under global lock when full.
- **Async settlement raises** — `ack_async`/`nack_async`/`reject_async` raise
  `RuntimeError` when no fn set (matching the sync contract) instead of silently
  returning.
- **`asyncio.timeout`** for startup hooks (3.11+ idiom, cheaper than `wait_for`).
- **`_serialize_result` strict** — removed `default=str` silent coercion in the
  fallback (consistent with the strict serializer philosophy).

### Removed

- Dashboard dead `management_client`/`metrics_collector` parameters (accepted and
  documented but never referenced inside the function).
- Dead `_release_sha` field in `RedisLock` (declared, never used).
- Dead `publish_fn` parameter in `_compose_publish_*` (unused — the chain threads
  it at call time).
- Stale "0.2.0 placeholder" comments in config dataclasses.
- `TRANSIENT_ERRORS` simplified — removed unreachable subclasses (`ConnectionResetError`,
  `BrokenPipeError`, `ConnectionAbortedError` are all `OSError` subclasses); kept
  `EOFError` (not an `OSError` subclass).

### Fixed

- **`validate_handler` rejected valid `(body, msg: RabbitMessage)` handlers** under
  `from __future__ import annotations` — string annotations weren't recognized as
  `RabbitMessage`. Added `is_rabbit_message_annotation` helper (class or string form).
  Unannotated params are no longer counted as body-like candidates (the fallback
  resolver binds the first unannotated param to the body and the rest to the message).
- **`SyncTransport.start_consuming` delivered zero messages with per-consumer channels** —
  it called `self._channel.start_consuming()` (publisher channel, no consumers under
  the per-consumer-channel design). Rewritten to drive the connection I/O loop directly
  via `process_data_events` with a no-consumers safety break.
- **`_DaemonWorkerPool` was effectively single-threaded** — idle-count semantics were
  inverted so `worker_count>1` ran ~1 worker. Corrected idle accounting and locked
  `_threads`/`_idle_count` to prevent oversubscription.
- **`_run_on_io_thread` zombie callback** — on a 30s I/O-stall the queued callback is
  now cancelled (checked inside `_cb`) so a late drain no longer settles an
  already-redelivered message.
- **HMAC signing replay protection** — added a bounded `TTLSetNonceCache`
  (OrderedDict LRU), `require_freshness=True` default, and skew+nonce enforcement
  whenever freshness headers are present (past and future).
- **Compression zip-bomb guard** — gzip/zstd decompress now uses streaming with a
  running byte counter that aborts at `max_decompressed_size` before materializing
  the full output. zstd contexts isolated per thread via `threading.local`.
- **Management SSRF** — sync uses a no-redirect opener; async passes
  `allow_redirects=False`; both cap response size.
- **`broker_liveness` wedge detection** — heartbeat updated on every delivery;
  liveness fails when `last_heartbeat` goes stale past `wedged_timeout`.
- **Async readiness stale-consumer detection** — `has_open_channels` added to
  `AsyncTransportImpl`; readiness drops pods whose consumer channel died.
- **Channel pool `_created` leak on closed-idle-channel** — `acquire()` now
  decrements `_created` when discarding a closed pooled channel.
- **`BatchPublisher`/`BatchAcker` async path** — guarded by `asyncio.Lock`;
  `close_async` cancels the sync timer and vice versa.
- **Sync `FlowController.acquire("wait")`** — re-loops on slot-race loss (was
  dropping, unlike async).
- **Sync `publish` honors `confirm_timeout`** — bounded `_run_on_io_thread` timeout.
- **Async `blocked_connection_timeout` watchdog** — closes the connection when a
  broker alarm isn't cleared in time.
- **`startup_timeout` bounds hung hooks** — sync: no more `ThreadPoolExecutor`
  blocking `__exit__`; async: sync hooks run via `to_thread` bounded.
- **`DependencyScope.cleanup` isolates per-generator teardown** — one raising
  teardown no longer leaks the rest.
- **Handler returning a `MessageEnvelope` preserves its fields** —
  headers/priority/content_type/... are preserved via `dataclasses.replace`
  instead of dropping all but `body`.
- **`RabbitSettings.blocked_connection_timeout` default 60s** (was 300); deploy
  manifest env vars renamed to `RABBITMQ_*`.
- **`AsyncBroker.on_app_shutdown` callback** — prevents the `RabbitApp`+broker
  double-install signal-handler hang.
- **Sync `stop_consuming` marshals via `add_callback_threadsafe`** when called
  cross-thread instead of calling pika unsafely.
- **Publish-side middleware chain cached per route** — parity with the consume cache.
- **`ConnectionConfig.url` URL-escapes credentials**; `guest/guest` warns when host
  is non-local; `RabbitSettings.password` is `SecretStr`; `RPCClient` gains
  `max_reply_bytes`; `JSONSerializer(max_parse_bytes=...)` caps input size;
  `ManagementConfig` warns on off-localhost `guest`; `start_metrics_server` defaults
  to `127.0.0.1`; dashboard supports an optional `auth_token`.
- **CI integration job is now gating** (chaos stays best-effort).

## [0.7.3] — 2026-06-28

Production hardening (consolidated from 0.7.1-0.7.3). release. The real-RabbitMQ integration suite (21 tests via
testcontainers) passes against a live broker, including reconnect-resume after
a connection drop, heartbeat wedge detection, and sync SIGTERM graceful drain.
A soak-test script validates zero-loss continuous publish+consume.

### Added

- `tests/integration/test_resilience_scenarios.py` — four live-broker integration scenarios.
- `benchmarks/soak_test.py` — continuous-load soak test.
- `Dockerfile` (multi-stage, non-root), `deploy/consumer.yaml` (k8s manifest with
  probes, PDB, preStop), README "Running in Kubernetes" section.
- `broker_liveness` / `broker_readiness` (liveness vs readiness split for k8s).
- `metrics_app()` (ASGI app serving `/metrics`) and `start_metrics_server(port)`.
- DI marker public exports (`Depends`, `Header`, `Path`, `Context`, `ContextRepo`,
  `DIResolver`, `DependencyScope`, `ConfigurationError`).
- `TestBroker` real settlement + injectable publish outcome (`assert_acked`,
  `assert_nacked`, `assert_rejected`, `fail_next_publish`).
- CI integration job with `rabbitmq:3-management` service container.

### Changed

- Trove classifier → `Development Status :: 5 - Production/Stable`.
- `ConfigurationError` unified in `rabbitkit.core.errors`.
- `AppState` canonical home → `core/types.py`.
- `RabbitConfig` / `RabbitQueue` / `RabbitExchange` are frozen dataclasses.
- `blocked_connection_timeout` default 60s (was 300).
- `RetryConfig.strict_delays=True` by default.
- `filter_fn` on `RabbitRouter.subscriber()`.
- Bounded graceful shutdown (`ConsumerConfig.graceful_timeout`).

### Fixed

- DI generator teardown leak in the auto-DI path — `DependencyScope` now created
  whenever the effective resolver is non-None; resolution + handler invocation
  wrapped in a single try/finally.
- Ack-failure propagation — `_disposition` set only after the transport call
  succeeds; a failed settlement raises and leaves `is_settled == False`.
- Async consumer not resumed after broker reconnect — `consume()` now uses
  `declare_queue(name, passive=True)` for robust restoration.
- `ConnectionConfig.heartbeat` ignored on the async transport — heartbeat now
  carried on the connection URL (`?heartbeat=N`).
- `Path()` DI never worked — `message.path` now populated on each delivery.
- DI markers did nothing without an explicit `DIResolver` — pipeline now
  auto-detects markers and uses a resolver automatically.
- AMQP `timestamp` not round-tripped — async publish now sends `envelope.timestamp`;
  both transports populate `message.timestamp` on consume.
- Async per-message TTL was 1,000,000× too long — async now passes
  `int(expiration) / 1000` (ms → seconds), matching sync.
- Sync multi-worker connection corruption — `_run_on_io_thread()` marshals every
  channel operation onto the connection-owning thread.
- Failed retry/result publish acked the source → permanent message loss — now
  checks `outcome.ok` and `nack(requeue=True)` when the publish failed.
- Subscriber middlewares were never executed — `_run_consume_sync`/`_run_consume_async`
  now run each middleware's `on_receive` hook and compose the `consume_scope` chain.
- Sync consumer had no connection recovery — `run()` wraps the consume loop in a
  recovery loop that reconnects, re-declares topology, and re-subscribes.
- `SyncWorkerPool._futures` was not thread-safe — now a `set` with
  `threading.Lock` and `add_done_callback`.
- Backpressure `on_blocked="wait"` deadlocked the event loop — rewrote with
  `asyncio.Event` and `asyncio.wait_for()`.
- `CircuitBreakerMiddleware` silently skipped async handlers with a sync CB —
  now raises `TypeError` immediately.
- RPC `_ensure_consuming()` was not atomic — entire body now inside the lock.
- Single aio-pika connection caused head-of-line blocking — `AsyncConnectionPool`
  now creates separate publisher and consumer connections.
- Channel pool `acquire()` could block forever — now uses
  `asyncio.wait_for(timeout=channel_acquire_timeout)`.
- DLQ never received terminal rejections — source queues now re-declared with
  dead-letter arguments when retry is enabled.

## [0.6.1] — 2026-04-15

### Fixed

- Async backpressure `on_blocked="wait"` deadlocked event loop — rewrote with
  `asyncio.Event` and `asyncio.wait_for()`; added `_AsyncTokenBucket`.
- `CircuitBreakerMiddleware` silently skipped async handlers when given a sync CB —
  now raises `TypeError` (**breaking change** for misconfigured setups).
- RPC `_ensure_consuming()` was not atomic — race with concurrent callers — entire
  body now inside the lock.
- Single aio-pika connection caused head-of-line blocking — separate publisher and
  consumer connections.
- Channel pool `acquire()` could block forever on exhaustion — now uses
  `asyncio.wait_for(timeout=channel_acquire_timeout)`.
- DLQ never received terminal rejections from retry middleware — source queues
  now re-declared with dead-letter arguments when retry is enabled.
- `SyncWorkerPool._futures` list was not thread-safe — now a `set` with
  `threading.Lock`.
- `FlowController` async rate limiter used `threading.Lock` inside the event loop —
  `_AsyncTokenBucket` with `asyncio.Lock` and `asyncio.sleep()`.
- DI generator cleanup exceptions were silently swallowed — now logged at ERROR.
- `RetryConfig` with `len(delays) < max_retries` silently reused last delay — now
  issues `UserWarning`.
- Each new consumer `consume()` call shared the topology channel — now creates a
  dedicated channel; `set_qos` is isolated per consumer.
- `AsyncRPCClient._futures` dict lacked lock protection — all operations now hold
  `self._lock`.
- `SyncWorkerPool._futures` rebuilt the whole list on every message — now a `set`
  with O(1) `add` and `add_done_callback`.

### Added

- `WorkerConfig` pool-size validation — `broker.start()` emits `RuntimeWarning`
  when `worker_count > channel_pool_size`.

## [0.6.0] — 2026-04-15

### Added

- **Subscriber filtering** — `filter_fn` parameter on `@subscriber()`.
- **Structured logging** — `LoggingConfig` + `configure_structlog()`.
- **Environment-based configuration** — `RabbitSettings` reading `RABBITMQ_*` env vars.
- **RPC shorthand** — `broker.request()` one-call RPC.
- **Rate limiting middleware** — `RateLimitMiddleware` with token-bucket.
- **Message signing middleware** — `SigningMiddleware` with HMAC.
- **Handler timeout middleware** — `TimeoutMiddleware`.
- **CLI tooling** — `rabbitkit run/health/topology/shell`.
- **Hot reload** — `rabbitkit run --reload` via watchfiles.
- **Distributed locking** — `LockMiddleware` + `RedisLock`.
- **RabbitMQ management API client** — `RabbitManagementClient`.
- **AsyncAPI documentation generation** — `generate_asyncapi_doc()`.
- **Result backends** — `ResultMiddleware` + `RedisResultBackend`.
- **Pydantic auto-validation** — body type hints trigger automatic `model_validate()`.
- **Custom serialization pipeline** — `SerializationPipeline` with pluggable
  parser/decoder stages.
- **Monitoring dashboard** — `create_dashboard_app()`.
- **Interactive shell** — `rabbitkit shell`.
- **Per-route prefetch** — `prefetch_count` on `@subscriber()`.
- **Exchange-to-exchange bindings** — `RabbitExchange.bind_to`.
- **Metrics middleware** — `MetricsMiddleware` + `PrometheusCollector`.

## [0.5.0] — 2026-03-10

### Added

- Production polish — logging, health checks, metrics.
- Per-route prefetch override.
- Exchange-to-exchange bindings.
- Metrics middleware with Prometheus collector.

## [0.4.0] — 2026-03-10

### Added

- Broker integration (`SyncBroker`, `AsyncBroker`).
- Health checks (`broker_health_check`).
- Stream queues (`StreamOffset`, `StreamConsumerConfig`).
- Documentation and benchmarks.

## [0.3.0] — 2026-03-10

### Added

- Resilience — `RetryMiddleware` with delay queues, error classification.
- Dependency injection — `Depends()`, `Header()`, `Path()`, `Context()`.
- Consumer concurrency — `WorkerConfig`, `SyncWorkerPool`, `AsyncWorkerPool`.
- Configuration — frozen dataclasses, `RabbitConfig`, composable config.

## [0.2.0] — 2026-03-10

### Added

- Observability — structured logging, tracing middleware.
- Middleware — `ExceptionMiddleware`, `RetryMiddleware`, `CompressionMiddleware`.
- High-load infrastructure — `FlowController`, `BatchPublisher`, `BatchAcker`,
  `WorkerPool`.
- DLQ inspector — peek, replay, purge.
- FastAPI integration — `rabbitkit_lifespan`.
- Protocol extensions — result publishing, publisher confirms.
- Configuration — `PoolConfig`, `RetryConfig`, `CompressionConfig`.

## [0.1.0] — 2026-03-10

### Added

- Core — `HandlerPipeline`, `SubscriberRegistry`, `RouteDefinition`.
- Serialization — JSON serializer, two-stage pipeline.
- Dependency injection — `DIResolver`, `DependencyScope`.
- Middleware — `BaseMiddleware` with `consume_scope`/`publish_scope`.
- Sync transport — pika-based `SyncTransport`.
- Async transport — aio-pika-based `AsyncTransportImpl`.
- RPC — `RPCClient`, `AsyncRPCClient`.
- Testing — `TestBroker`, `TestApp`.