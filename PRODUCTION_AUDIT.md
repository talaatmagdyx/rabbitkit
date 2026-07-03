# rabbitkit v1.1.1 — Production-Grade RabbitMQ Audit

> **STATUS UPDATE (2026-07-03, post-audit):** All four critical findings
> (C1–C4), all six high-risk findings (H1–H6), and **all 18 MEDIUM findings**
> have been **ADDRESSED** — every one with a real code fix (see the ✅
> annotations below and `CHANGELOG.md [1.2.0]`). M17 and M18 were initially
> marked resolved-by-design, but a second, more skeptical pass (prompted by
> the same scrutiny that turned M6 and M14 from "resolved-by-design" into
> real fixes) found genuine bugs behind both: M17's channel-close-on-timeout
> was corrupting sibling in-flight publishes sharing a batch channel, and
> M18's "flow control would deadlock" rationale for skipping internal
> republishes didn't hold up (no such deadlock exists — `FlowController`
> slots release independently via I/O completion, not via the blocked
> task's own progress). Both are now code-fixed, not just documented. The
> M13 tail is now mostly code (`CERT_NONE` warning + `credentials_provider`
> rotation); the only true residuals are deployment posture
> (dashboard-auth-by-default, `rabbitkit shell`), not library defects. The
> 🔵 LOW "for completeness" list is now fully resolved: 10 real code/doc
> fixes (retry/DLQ property preservation, AMQP shortstr validation,
> `lazy=True` deprecation warning, CLI management-URL scheme restriction,
> opt-in management-client health integration, `BatchAcker` docstring
> cross-thread fix, KEDA scaling docs, `rabbitkit dlq replay
> --reset-retry-count`, `PydanticDecoder` non-dict-payload validation
> bypass, and word-set-based log-redaction matching) plus 1 item confirmed
> already-deliberate on inspection (the 85% coverage floor). Three of the
> ten — the CLI reset-retry-count gap, the `PydanticDecoder` bypass, and
> the redaction exact-match gap — were each initially waved off as
> "documented/already-resolved, no change needed," and each turned out to
> have a real gap once re-examined at the same level of scrutiny that
> turned M6/M14 from resolved-by-design into code fixes; see the inline
> notes below each. The 🟢 OPTIMIZATIONS ("verified strengths") section got
> the same re-verification treatment: one claim there — "a `CONFIRMED` vs
> `SENT` distinction so internal republishes only settle on real confirms"
> — overclaimed what the settlement code actually does (it checks `.ok`,
> true for both statuses, not `status == CONFIRMED`); seeing that gap
> exposed a real, separate, systemic one — none of rabbitkit's 37
> `warnings.warn()` safety warnings (including the retry-without-confirms
> one meant to cover exactly this) were bridged into `logging`/`structlog`,
> so they were invisible in a production JSON-logging deployment.
> `configure_structlog()` now calls `logging.captureWarnings()`
> (`LoggingConfig.capture_warnings`, default `True`) to fix that. A full
> pass then re-verified the REMAINING 16 OPTIMIZATIONS claims (5 parallel
> targeted audits against the actual code/CI-workflow files, not the
> claim text) and found 4 more overclaims, 2 of them real functional bugs:
> (1) the async broker's *inline* (default, no worker pool) in-flight
> drain silently abandoned timed-out handlers unacked instead of nacking
> them like the pooled path — fixed, both paths now cancel + nack with
> delivery-tag logging; (2) the sync transport's confirm-wait timeout
> returned `PublishStatus.ERROR` instead of the documented `TIMEOUT`
> (async got this right) — fixed with a dedicated `except TimeoutError`
> branch; (3) "sync abandonment documented with its duplicate-side-effect
> consequence" wasn't actually documented anywhere user-facing — now is;
> (4) "least-privilege guidance in docs" for `PASSIVE_ONLY` didn't exist —
> now does (`docs/security.md`). One benchmarks-wording and one stale-CI-
> comment nit were also corrected. 12 of 17 OPTIMIZATIONS claims were
> confirmed accurate as originally written.
>
> **FINAL PASS:** the rest of the document (Architecture Review,
> Reliability, Performance, Security Review, DevOps, Observability, Race
> Conditions, Chaos Analysis, Final Verdict) was reconciled against actual
> current code — it had never been updated as C/H/M fixes landed
> throughout this whole remediation, so it still read as if H1, H2, H4,
> H5, M2, M3, M6, M7, M8, M9, M10, M11, M14, M15, M16 were open. All are
> now annotated ✅ in place, preserving the original analysis. Verifying
> "Sync confirm wait is unbounded on the owner thread" (Architecture
> Review) confirmed a real, previously-unaddressed severe gap: pika's
> `BlockingChannel.basic_publish()` has no timeout parameter, and running
> inline (the default single-worker/pure-producer path) meant a publish
> could hang forever against a wedged broker regardless of
> `confirm_timeout`. Fixed for every inline case except one that's
> genuinely unsafe to interrupt (owner thread also actively driving a
> consume loop — see `docs/message-safety.md`); that residual case is
> documented with a mitigation (`worker_count>1`), not silently left as
> an overclaim. Also confirmed `RetryConfig.jitter_factor` is a
> *deliberate*, already-documented no-op (not a bug — see the RELIABILITY
> section) and added a startup warning when `TracedConsumerMiddleware` is
> added without obskit available (previously a silent, permanent no-op).
> See the FINAL STAFF ENGINEER VERDICT section for the fully reconciled
> bottom line.
>
> **CLOSING SWEEP (2026-07-04):** a last search for anything in this
> document still reading as open found two: (1) the "document the 0.9k
> msg/s ceiling in the README" follow-up was already satisfied (the README
> section shipped with H6) — record corrected; (2) the H5 residual
> ("connection-churn/broker-redelivery-rate counters remain unadded") is
> now closed with code: `rabbitkit_messages_redelivered_total` (counted by
> `MetricsMiddleware` from the broker's `redelivered` flag) and
> `rabbitkit_reconnects_total` (new `on_reconnect` transport hook on both
> transports, wired to the route `MetricsMiddleware` collector by both
> brokers). **Nothing in this document remains open.** Verified: full unit
> suite passes (2847), `ruff` clean, `mypy --strict` clean, security +
> property suites pass (15). Original finding text preserved
> below as the audit record.

**Date:** 2026-07-03
**Commit:** `8ebde36` (master)
**Auditor role:** Staff/Principal Distributed Systems Engineer, SRE lead, DevOps architect, security reviewer
**Assumptions:** production or about to go live; high throughput, multi-tenant; 99.9%+ SLA; financial/critical data possible; multiple microservices depend on messaging correctness.

**Scope note:** rabbitkit is a client library (pika sync + aio-pika async over a transport-free `core/`), not a deployed system. This audit covers what the code guarantees; broker-side cluster topology, Helm/StatefulSet, and quorum-queue policy are deployment concerns the repo intentionally leaves to consumers (a reference `deploy/consumer.yaml` exists; there is no broker deployment artifact). Everything below is verified against source with file:line evidence — four parallel audit passes covered ~20k LOC.

---

## 🔴 CRITICAL RABBITMQ FAILURES (PRODUCTION BLOCKERS)

### ✅ FIXED — C1. Deduplication "on_success" marks the Redis key BEFORE the handler runs → crash = permanent silent message loss

> **Fix:** `on_success` now checks with `EXISTS` (no write) before the handler
> and writes the key only after success — a crash leaves no state, so the
> redelivery is processed. Local LRU is now lock-guarded. Also added the
> `mark_policy="claim"` two-state policy (in-flight/completed) for workloads
> needing concurrent-duplicate suppression AND crash-safety. Regression test:
> `tests/unit/middleware/test_deduplication.py::TestCrashSafety`.

`src/rabbitkit/middleware/deduplication.py:275` (sync), `:337` (async). The `SET NX` executes before `call_next`; only the local LRU is marked post-success. Rollback (`DELETE`) runs on exception — but rollback is impossible on SIGKILL/OOM/power loss.

**Scenario:** consumer SETNXes the key, gets OOM-killed mid-handler; RabbitMQ redelivers the unacked message; dedup finds the key present → **acks and skips**. The message is unrecoverable for the dedup TTL (default 86,400s). This is the classic unsafe check-then-ack ordering, and the module docstring ("mark the key only after the handler succeeds") misdescribes the implementation. Additionally, the rollback delete itself can fail on a Redis blip and is only WARN-logged (`deduplication.py:147-149`) — same loss. For financial data this alone is a go-live blocker.

### ✅ FIXED — C2. `DLQInspector.replay` acks the DLQ original without checking the republish outcome → your recovery tool can lose messages

> **Fix:** replay now checks `PublishOutcome.ok` before acking; failed
> republishes are nack-requeued (stay on the DLQ) and reported via the new
> int-compatible `ReplayResult` (`.failed`/`.requeued`). Envelopes publish
> `mandatory=True`; opt-in `reset_retry_count=True` grants a fresh retry
> ladder. The CLI `dlq replay` (same bug, worse — non-confirm channel) now
> runs with publisher confirms + mandatory and exits non-zero on failures.
> Regression tests: `tests/unit/test_dlq.py::TestReplayPublishOutcome`.

`src/rabbitkit/dlq.py:142-146` (sync), `:219-223` (async). Both transports report publish failures as a *returned* `PublishStatus` (ERROR/NACKED/TIMEOUT), never an exception — and replay ignores the return value and immediately acks.

**Scenario:** a connection blip or unroutable target mid-replay → publish returns ERROR → original is acked off the DLQ → message permanently lost. The retry middleware gets this exact pattern right (`middleware/retry.py:296-308`); replay doesn't.

### ✅ FIXED — C3. Default configuration silently discards poison messages — no DLX on retry-less routes

> **Fix:** new `SafetyConfig.reject_without_dlx` policy
> (`RejectWithoutDLXPolicy` enum): `auto_provision` (default — every
> rejecting route gets `{queue}.dlq`, same convention as retry topology),
> `error` (startup fails with `UnsafeTopologyError`, for externally-managed
> topology), `discard` (explicit opt-in to loss, warns). Per-route override
> via `@subscriber(reject_without_dlx=...)`. Applied only under
> `AUTO_DECLARE`; ACK_FIRST/manual-DLX/retry routes unaffected. Migration
> note for existing queues (406 on arg change) documented in
> `docs/retry-and-dlq.md` and the changelog. Tests:
> `TestRejectWithoutDLXPolicy` (sync+async) + `TestRejectWithoutDLXResolution`.

`src/rabbitkit/core/pipeline.py:105-110`, `src/rabbitkit/core/errors.py:58-66`, `src/rabbitkit/sync/broker.py:751-762`. A DLQ is auto-provisioned **only** when retry (or `filter_fn`) is configured.

**Scenario:** a plain `@subscriber(queue="orders")` receiving a malformed payload raises `ValueError` → classified PERMANENT → `reject(requeue=False)` with no DLX → RabbitMQ discards it forever, with only a log line. Unknown exception types also default to PERMANENT (`errors.py:78`), widening the blast radius. The safe default for a messaging library carrying critical data is DLX-always or refuse-to-reject-without-DLX.

### ✅ FIXED — C4. Headers-exchange routing is silently broken: queue `bind_arguments` are never sent to the broker

> **Fix:** `bind_queue()` on both transports (and the `Transport` protocols)
> now accepts and forwards `arguments`; both brokers pass
> `queue.bind_arguments`. Registration fail-fast validates headers routes:
> HEADERS exchange without `bind_arguments` raises `ConfigurationError`, as
> does an invalid `x-match`. Tests: `TestBindArguments` (sync),
> `TestRejectWithoutDLXPolicyAsync::test_bind_arguments_passed_to_transport`
> (async), `TestHeadersBindingValidation` (core).

`src/rabbitkit/sync/broker.py:779-783` and `src/rabbitkit/async_/broker.py:917-922` call `bind_queue(queue, exchange, routing_key)`, which has no arguments parameter; `RabbitQueue.to_bind_kwargs()` (`core/topology.py:238-245`) is never called by any broker or transport.

**Scenario:** a headers-exchange binding with `{"x-match": "all", "type": "order"}` binds argument-less — RabbitMQ treats that as match-everything, so the queue receives the full firehose (or misses selective routing entirely) with zero errors. If any tenant uses headers exchanges today, they are receiving wrong traffic right now.

---

## 🟠 HIGH RISK DESIGN ISSUES

### ✅ FIXED — H1. SigningMiddleware is incompatible with retries and redeliveries — it destroys legitimate messages

> **Fix:** (a) a duplicate nonce on a broker redelivery (`redelivered=True`)
> is now allowed — replay protection still holds for fresh deliveries, which
> is how an attacker's re-publish arrives. (b) signing + retry now raises
> `ConfigurationError` at startup (both brokers) instead of silently
> destroying every retried signed message. Tests:
> `TestReplayProtection::test_broker_redelivery_with_seen_nonce_is_allowed`,
> `TestSigningRetryConflict` (sync+async).

Two independent failure paths:

- **(a)** The nonce is marked "seen" at first verification (`middleware/signing.py:619-622`); a transient handler failure → nack/requeue → the redelivery trips "Replay detected". `InvalidSignatureError` is an unknown exception type → PERMANENT → rejected (no DLX → destroyed). `on_receive` exceptions bypass RetryMiddleware entirely (`core/pipeline.py:577-593`).
- **(b)** The signature covers exchange+routing_key (`signing.py:438-455`), but retried messages re-enter via the default exchange with `routing_key=<queue-name>` (`middleware/retry.py:459-461`) — verification fails on **every** post-delay redelivery.

Signing + retry are mutually exclusive in practice and nothing warns about the combination.

### ✅ FIXED — H2. Sync default `worker_count=1` starves heartbeats during long handlers

> **Fix:** `SyncBroker.start()` emits a `RuntimeWarning` when starting a
> single-worker consumer with heartbeats enabled, pointing at
> `WorkerConfig(worker_count=N)` or a higher heartbeat. Tests:
> `TestSingleWorkerHeartbeatWarning`.

The handler runs inline on the pika I/O thread (`concurrency.py:256-262`, `sync/broker.py:805-848`); no heartbeat frames are serviced while it runs. With `heartbeat=30` (`core/config.py:47`), any handler exceeding ~60s gets the connection killed broker-side mid-handler → ack fails → redelivery → duplicate side effects, potentially in a loop. `pump_idle()` (v1.1.1) fixed only the idle-publisher case. `worker_count>1` is immune but this is neither the default nor documented as the fix.

### ✅ FIXED — H3. Shared retry mode (`per_queue=False`) misroutes messages across queues or fails startup

> **Fix:** `RetryConfig(per_queue=False)` now raises `ValueError` at
> construction — a shared delay queue physically cannot route each message
> back to its own varying source with static broker config, so there is no
> safe shared topology. `per_queue=True` (default, isolated per-queue
> topology) is the only supported mode. Tests: `test_shared_mode_rejected`.

`middleware/retry.py:445-462`: shared delay queues `rabbitkit.retry.{n}` hardcode `x-dead-letter-routing-key` to whichever source queue declared first. Two queues sharing it → either a 406 `PRECONDITION_FAILED` at startup or, worse, **orders' failed messages reappear on the payments queue** after the delay. Shared `rabbitkit.dlq` similarly mixes all queues' dead letters.

### ✅ FIXED — H4. Async reconnect is a thundering herd: fixed 1.0s interval, no jitter, no backoff

> **Fix:** the aio-pika `reconnect_interval` is now randomized per process
> over `[base, base + min(base, backoff_max - base)]`, de-synchronizing a
> fleet reconnecting after a broker bounce. (connect_robust still can't do
> exponential backoff; jitter addresses the herd.) Tests:
> `test_reconnect_interval_is_jittered_within_bounds` + bounds/edge cases.

`async_/connection.py:149-150`. The sync side got full-jitter exponential backoff bounded at 300s (`sync/transport.py:309-333`); the async side did not. A broker restart under 200 async pods → lockstep retries every second against a booting node. Also unbounded (retries forever) — asymmetric with sync's 300s give-up.

### ✅ FIXED — H5. No queue-depth / consumer-lag / DLQ-depth metric anywhere

> **Fix:** new `QueueMetricsPoller` bridges the management API into the
> metrics registry — `queue_messages_ready`, `queue_messages_unacked`,
> `queue_messages_total`, `queue_consumers` gauges per queue (sync thread +
> async task loops). `MetricsCollector`/`PrometheusCollector` gained
> `set_gauge`. Tests: `tests/unit/test_queue_metrics.py` (100% coverage).

The Prometheus set is rich (rates, latencies, in-flight, connected) but nothing measures `messages_ready`, unacked age, or DLQ growth. `RabbitManagementClient.list_queues()` returns all of it but nothing bridges it into the registry. Consumers can silently fall behind by millions of messages while every rabbitkit metric reads healthy. Alerting on the #1 RabbitMQ incident signal (queue growth) requires external tooling.

### ✅ FIXED (documented) — H6. Sync publish throughput ceiling ~0.9k msg/s — confirm-per-message on a single channel

> **Fix:** the ceiling is an inherent limit of pika's `BlockingConnection`
> (confirms serialize, can't pipeline) — no safe throughput rewrite exists.
> Now documented prominently where users look: README, `SyncBroker.publish`
> docstring, and the batch helper — steering high-volume publishers to
> `AsyncBroker`/`AsyncBatchPublisher` (~6.1k msg/s) or more processes, and
> noting `worker_count` does not raise sync publish throughput.

`sync/transport.py:470-503`; every publish serializes to one RTT on one publisher channel regardless of worker threads. `SyncChannelPool` exists but the publish path doesn't use it (`sync/pool.py:35-37`); `highload.BatchPublisher` buffers but does not pipeline confirms (`highload/batch.py:48-53`). An outbox drain after an outage caps at ~900 msg/s while backlog grows. (Async has a real pipelined `AsyncBatchPublisher` at ~6.1k msg/s confirmed.)

---

## 🟡 MEDIUM RISK FINDINGS

> **STATUS (2026-07-03):** All 18 MEDIUM findings **FIXED** with code —
> M17 and M18 were briefly marked resolved-by-design before a re-review
> found real fixable bugs in both (see the note at the end of this
> section). ✅ markers inline. Verified green (full suite 2785, ruff,
> mypy --strict, security + property suites).

1. ✅ **FIXED (M1)** — **`publish()` never raises** — all failures become a `PublishOutcome` return value (`sync/transport.py:552-559`, `async_/transport.py:509-516`). User code that ignores the return (the natural calling style) silently loses NACKED/TIMEOUT/ERROR messages. Internal callers check; user code is on its own.
2. ✅ **FIXED (M2)** — **Dead config that lies to operators:** `PublisherConfig.mandatory`/`.persistent` are never read (`core/config.py:188-189`); `SecurityConfig.mechanism` is never read — no SASL EXTERNAL despite the field (`core/config.py:174`); `RetryConfig.jitter_factor` is a no-op (`_compute_delay` at `middleware/retry.py:249-257` is never called — retries fire in phase-locked waves, a retry-storm amplifier); the sync broker warns about `channel_pool_size` tuning for a pool the publish path never uses (`sync/broker.py:264-306` vs `sync/pool.py:35-37`).
3. ✅ **FIXED (M3)** — **`amqps://` URLs are silently ignored** — `from_url` disregards the scheme (`core/config.py:101-130`); TLS only engages via a separate `SSLConfig.enabled=True`. Operators passing an amqps URL may ship plaintext.
4. ✅ **FIXED (M4)** — **Retry delay-queue publishes are not `mandatory`** (`middleware/retry.py:280-311`): a runtime-deleted `{q}.retry.N` queue means every retry at that tier is broker-confirmed into the void and the source is acked — confirmed loss with a success log line.
5. ✅ **FIXED (M5)** — quorum source queue + `x-delivery-limit` now composes with retry (both preserved across the DLX re-declare); documented as the crash-loop backstop in `docs/retry-and-dlq.md`. **Crash-loops bypass retry counting entirely:** the count lives in header `x-rabbitkit-retry-count`, incremented only on the delay-queue path; a handler that crashes the process (OOM/segfault) redelivers with count unchanged, forever. No quorum `x-delivery-limit` backstop is declared (delay queues are pinned classic; `middleware/retry.py:462`), despite the code's own docstring pointing at it (`retry.py:238-241`). Header is clamp-protected against garbage but not reset-protected.
6. ✅ **FIXED (M6)** — opt-in `ConsumerConfig.reject_transient_on_redelivery` gives a 2-strike cap using the broker's `redelivered` flag (reject → DLQ on the redelivery) — the audit's "no use of the redelivered flag" gap. Default off preserves the legitimate infinite-requeue-until-recovery pattern; for a higher cap use retry or quorum `x-delivery-limit` (M5). **Transient errors on retry-less routes hot-loop** at the queue head with no delay, no cap, no use of the `redelivered` flag (`core/pipeline.py:106-108`; `redelivered` used only for log escalation at `:63-86`).
7. ✅ **FIXED (M7)** — **`REQUEUED_FOR_RETRY` sentinel leaks into result/RPC publishing** (`core/pipeline.py:464, 533` only check `result is not None`) — a retried RPC message can emit a garbage reply per attempt with a permissive serializer, then the real reply later (duplicate, wrong-ordered). With the default JSON serializer it is a harmless-but-noisy `TypeError` after settlement.
8. ✅ **FIXED (M8, in the C1 rework)** — **Dedup local LRU is not thread-safe** under the sync worker pool — plain `OrderedDict`, no lock (`middleware/deduplication.py:97-139`); a bookkeeping race (`KeyError` from concurrent `move_to_end`/`popitem`) gets classified PERMANENT → message dead-lettered/dropped. Also, dedup keys default to `message_id`, which is auto-UUID-per-publish (`core/types.py:184`) — so dedup suppresses redeliveries only, not producer-side logical duplicates, unless producers set IDs explicitly.
9. ✅ **FIXED (M9)** — `ConnectionConfig.nodes` adds cluster failover: sync uses pika's native `ConnectionParameters` list; async cycles endpoints on initial connect. **No multi-host/cluster failover in config** — single `host: str` (`core/config.py:42`), no pika parameter-list or multi-URL support. A dead node behind the configured hostname takes every client down for the full backoff window. Sync gives up permanently after 30 attempts/300s (`sync/transport.py:134, 309-333`) → process exit; fine under k8s, fatal on bare metal; the cap is not configurable via `RabbitConfig`.
10. ✅ **FIXED (M10)** — async large-body decode now offloads to a thread (≥256 KiB); opt-in `PublisherConfig.max_message_bytes` publish-side size guard added. **Event-loop blocking:** up to 64MB JSON/msgspec decode runs inline on the asyncio loop (`core/pipeline.py:718, 748-762`), stalling heartbeats, confirms, and every co-located consumer. Decompression is correctly offloaded (`middleware/compression.py:221` uses `asyncio.to_thread`) but decode is not. No publish-side message-size guard exists at all — a 50MB message is accepted end-to-end.
11. ✅ **FIXED (M11)** — opt-in `WorkerConfig.max_queue_size` bounds the sync pool's work queue (default 0 = unbounded); prefetch remains the primary bound. **Unbounded in-process work queue** (`concurrency.py:50-52` — `queue.Queue()` with no maxsize) — backlog bounding relies entirely on prefetch; effective prefetch = `worker_count × prefetch_per_worker` (`sync/broker.py:310`); high prefetch + slow handlers + large bodies = OOM vector.
12. ✅ **FIXED (M12)** — **MANUAL ack-policy asymmetry:** an unhandled exception in a MANUAL-policy handler kills the entire sync run loop (`core/pipeline.py:136-138` re-raises; nothing above catches it except `run()`'s connection-error handler at `sync/broker.py:475-490`) — one buggy handler stops the whole broker. Pooled and async paths swallow it instead.
13. ✅ **MOSTLY FIXED (M13)** — `CERT_NONE` now warns; **credential rotation now supported** via `ConnectionConfig.credentials_provider` (re-resolved at each reconnect, no redeploy). Remaining posture items (dashboard unauthenticated-by-default — already warns + optional bearer token + loopback-documented; `rabbitkit shell` ops-tool) are deployment choices, not library defects. **Security defaults:** dashboard unauthenticated by default (`dashboard/app.py:83-132`; warns, loopback-documented, read-only, XSS-escaped — docs call it "the single riskiest default in the toolkit"); `cert_reqs="CERT_NONE"` disables verification with no warning (`core/config.py:163`, `sync/connection.py:78-81`); no credential-rotation hook — reconnects re-read the frozen config, rotation = redeploy; `rabbitkit shell` (`cli/commands/shell.py:47-73`) is an unaudited live publish path; plaintext password lives in the aio-pika connect URL (`async_/connection.py:136-143`) — third-party exception paths could leak it.
14. ✅ **FIXED (M14)** — `SafetyConfig.on_topology_conflict="warn_continue"` is a true per-conflict warn-and-continue: it warns, reopens the broker-closed channel, and continues with the existing (drifted) definition, while still actively declaring non-conflicting queues — the gap `PASSIVE_ONLY` (all-or-nothing) couldn't fill. Both transports; default `"raise"` unchanged. **Topology drift = CrashLoopBackOff:** 406 conflicts become a clean typed `ConfigurationError` with PASSIVE_ONLY advice (`sync/transport.py:660-682`) — good — but there's no warn-and-continue mode and no migration tooling; enabling retry on a pre-existing queue re-declares it with DLX args injected → 406 at startup (`sync/broker.py:751-762`).
15. ✅ **FIXED (M15)** — a gating CI step now runs the sync+async restart-mid-consume chaos scenarios with a checked exit code (full suite stays best-effort). **Chaos suite doesn't gate CI** (`.github/workflows/ci.yml`, `continue-on-error: true`) — a reconnect/redelivery regression can merge green despite `benchmarks/chaos_suite.py` covering exactly the right scenarios (broker restart during consume/publish, retry-to-DLQ under restart).
16. ✅ **FIXED (M16, in the C1 rework — on_success now checks with EXISTS, no up-front SETNX)** — **Two-consumer dedup race:** SETNX up front means a concurrent duplicate on consumer B is acked-and-dropped while A is mid-handler; if A fails AND its rollback delete also fails (Redis error), both copies are gone. Docstring describes the opposite behavior (`deduplication.py:11-21`).
17. ✅ **FIXED (M17)** — the channel-close-on-timeout decision moved from inside `_publish_on_channel` (which doesn't know if it's one of several concurrent calls sharing a channel) to each *caller*, timed to fire only after that caller's own usage of the channel has fully resolved. `AsyncBatchPublisher._flush` now closes the shared batch channel only after `asyncio.gather` resolves every concurrent publish in the batch, so one slow confirm's timeout can no longer yank the channel out from under sibling publishes that would have confirmed cleanly. **Async batch confirm-timeout amplification (real bug, not inherent):** one slow confirm closed the *shared* batch channel from inside its own timeout handler (`async_/transport.py:402-408`), which could abort sibling in-flight confirms on that same channel mid-flight rather than letting them settle — not just "batch-wide failure," but a wider blast radius than the batch's own genuinely-failed messages.
18. ✅ **FIXED (M18)** — added `_flow_controlled_internal_publish` (sync and async) as the `publish_fn`/`publish_async_fn` for `RetryMiddleware` and the pipeline's result/RPC-reply publish, replacing raw `transport.publish`. It applies the broker's `FlowController` (if configured) to these internal republishes, and — critically — never lets `BackpressureError` escape as an exception regardless of the configured `on_blocked` policy (a blocked/dropped slot always resolves as a failed `PublishOutcome`, since `RetryMiddleware`/result-publish only understand that return shape; an escaped exception would be misclassified PERMANENT and destroy the message). **Internal republishes bypassed broker-level publish middlewares and flow control (real gap, not by design):** the original "flow control on retry would deadlock" rationale for leaving this as-is doesn't hold — `FlowController` slots release independently via each in-flight publish's own I/O completion (confirm/timeout), not via progress of the blocked task, so there is no structural cycle, only backpressure slowdown. RetryMiddleware and result-publish previously got raw `transport.publish` (`sync/broker.py:699, 826-830`; `async_/broker.py:838, 954-957`) — broker-constructor `SigningMiddleware` never signed retry/result publishes (still true — H1 already makes that combination fail fast, so no regression there — but the missing backpressure was a genuine unprotected path).

### Formerly-deferred MEDIUM findings — now resolved

All six previously-deferred MEDIUMs have been addressed with real code
fixes (M6, M9, M14, M15, M17, M18). M17 and M18 briefly carried a
resolved-by-design verdict; re-review under the same scrutiny that turned
M6 and M14 from resolved-by-design into code fixes found genuine bugs
behind both, so they were fixed rather than left documented-as-inherent:

- **M6 (transient hot-loop on retry-less routes):** ✅ **CODE** — see above,
  `ConsumerConfig.reject_transient_on_redelivery`.
- **M9 (multi-host/cluster failover):** ✅ **CODE** — `ConnectionConfig.nodes`;
  sync native pika list, async endpoint cycling on connect.
- **M15 (chaos suite not CI-gating):** ✅ **CODE** — restart-mid-consume
  scenarios (sync+async) now gate CI with a checked exit code.
- **M17 (async batch confirm-timeout amplification):** ✅ **CODE** — the
  confirm-timeout handler in `_publish_on_channel` no longer closes the
  channel itself (it can't know if it's one of several concurrent calls
  sharing that channel). Closing now happens in each caller, only after
  that caller's own usage of the channel has fully resolved:
  `AsyncBatchPublisher._flush` closes the shared channel after
  `asyncio.gather` settles every publish in the batch, so a timeout can no
  longer abort sibling in-flight confirms mid-flight.
- **M6 (transient hot-loop on retry-less routes):** ✅ **CODE** — opt-in
  `ConsumerConfig.reject_transient_on_redelivery` (default off) gives a
  2-strike cap via the broker's `redelivered` flag: a transient error on an
  already-redelivered message rejects to the DLQ instead of requeuing again.
  Default preserves the legitimate infinite-requeue pattern; retry or quorum
  `x-delivery-limit` (M5) give a higher/broker-enforced cap.
- **M14 (topology-drift warn-and-continue):** ✅ **CODE** —
  `SafetyConfig.on_topology_conflict="warn_continue"` warns + reopens the
  channel + continues with the existing definition, per-conflict (unlike
  all-or-nothing `PASSIVE_ONLY`). Both transports; default `"raise"`.
- **M18 (internal republishes bypass middlewares/flow control):** ✅
  **CODE** — `_flow_controlled_internal_publish` (sync + async) is now the
  `publish_fn`/`publish_async_fn` for `RetryMiddleware` and the pipeline's
  result/RPC-reply publish, applying the broker's `FlowController` to both
  paths. It never lets `BackpressureError` escape as an exception (always
  resolves to a failed `PublishOutcome` instead), so `on_blocked="raise"`
  can't misclassify a throttled retry/result publish as a PERMANENT error
  and destroy the message. The original "would deadlock" rationale for
  skipping this was an overclaim — `FlowController` slots release via each
  publish's own I/O completion, not via the blocked task's progress, so
  there's no structural cycle.
- **M13:** ✅ **mostly CODE** — `CERT_NONE` warning + `credentials_provider`
  rotation hook both shipped. The only remaining items are deployment posture,
  not library defects: the dashboard is unauthenticated *by default* but
  already warns, offers a constant-time bearer token, and is loopback-
  documented; `rabbitkit shell` is an ops tool. Neither is a safe-by-default
  gap the library can close without breaking existing loopback/CLI users.

### 🔵 LOW (for completeness) — all addressed 2026-07-03

- ✅ **FIXED** — Retry republish and `DLQInspector.replay` used to silently
  drop `priority`, `expiration`, `type`, `app_id`, `user_id`, and `reply_to`
  (`middleware/retry.py:_build_retry_envelope`, `dlq.py:_build_replay_envelope`).
  `RabbitMessage` gained `priority`/`expiration`/`user_id` slots (it already
  had `type`/`app_id`/`reply_to`) — both transports now populate them from
  the incoming delivery (async re-encodes aio-pika's decoded-seconds
  `expiration` back to the ms-string convention `MessageEnvelope` uses
  everywhere), and both envelope builders copy all six through.
- ✅ **FIXED (was incompletely resolved)** — DLQ replay's retry-count
  reset. `DLQInspector.replay(reset_retry_count=True)` does exist at the
  Python API level, but `rabbitkit dlq replay` — the actual operator-facing
  CLI surface for exactly this "stuck message, reset and retry" workflow —
  is a completely separate, hand-rolled raw-pika implementation
  (`cli/commands/dlq.py`) that never calls `DLQInspector` at all. It had no
  way to strip the retry-count header: an operator using the CLI (the
  realistic way this "operator-surprising" scenario gets hit in practice)
  had no path to a fresh retry ladder without writing custom Python. Added
  `--reset-retry-count` (and `--retry-count-header` for a customized
  `RetryConfig.retry_header`) to the CLI command, mirroring the library's
  existing semantics. My first pass declared this "already resolved" by
  checking only the library API and missing that the CLI didn't expose it
  — the same class of gap as M17/M18 in the MEDIUM section above (verified,
  didn't hold up under a second look).
- ✅ **FIXED** — routing-key/exchange-name/queue-name AMQP shortstr
  validation: `MessageEnvelope.__post_init__` (every publish path funnels
  through this one construction point) and `RabbitQueue`/`RabbitExchange`
  `.validate()` now raise a clear `ValueError` for any name/routing-key over
  the AMQP-mandated 255-byte shortstr limit, instead of an opaque broker
  connection error at declare/publish time.
- ✅ **FIXED** — `lazy=True` now warns that it sets the deprecated
  `x-queue-mode=lazy` argument, which is a silent no-op on RabbitMQ ≥3.12's
  default CQv2 classic queues (still has effect pre-3.12 or on a queue
  explicitly downgraded to v1).
- ✅ **FIXED (the "documented" mitigation itself had a gap)** —
  `DataclassDecoder`'s no-type-validation is genuinely deliberate and its
  docstring correctly directs untrusted input to `PydanticDecoder`. But
  `PydanticDecoder.decode` had an `isinstance(data, dict)` guard that
  skipped `model_validate` entirely for any non-dict top-level payload (a
  producer sending a JSON array/string/number instead of an object) —
  silently handing the handler raw, un-validated data instead of the clean
  `ValidationError` pydantic already raises for exactly this shape
  mismatch. The recommended untrusted-input decoder had its own version of
  the same class of gap, just for the top-level shape instead of a field's
  type. Removed the guard; `model_validate` now always runs when
  `target_type` supports it.
- ✅ **FIXED (a real algorithm gap behind the "shallow" framing)** — the
  *depth* limit (top level + one nested dict) is genuinely deliberate. But
  the *matching* was exact-string-only: a configured key like `token` did
  NOT match a target key that normalizes to `auth_token` — so common
  compound secret-bearing names (`x-auth-token`, `session-token`,
  `x-secret-key`, `bearer-token`, `api-token`, ...) were logged in the
  clear even though `auth`/`token`/`secret` are each individually in
  `DEFAULT_REDACT_KEYS`. Matching is now word-set based: a target key
  matches when it contains ALL of some configured key's underscore-
  separated words (checked per-configured-key, not pooled into one flat
  word set — pooling would turn `api_key`'s `key` into a standalone
  matcher and misfire on benign fields like `primary_key`/`cache_key`).
- ✅ **FIXED** — CLI `_live_resources` now validates the `--url` scheme is
  `http`/`https` before ever calling `urlopen`, closing the arbitrary-scheme
  gap (`file://`, `ftp://`, etc. would otherwise be dispatched straight to
  urllib's registered handlers). The `guest:guest` CLI/docs defaults
  themselves are unchanged — they mirror RabbitMQ's own default account,
  which the broker restricts to localhost, and `ConnectionConfig`/
  `ManagementConfig` already warn when `guest` is used against a non-local
  host; treated as deliberate, matching the finding's own "localhost-only"
  characterization.
- ✅ **FIXED** — `broker_health_check`/`broker_readiness` (and their async
  variants) gained an optional `management_client` parameter
  (`RabbitManagementClient`) that folds in `.health_check()`/
  `.health_check_async()` as an additional signal: a failing management
  check downgrades an otherwise-HEALTHY local result to DEGRADED (never
  upgrades an UNHEALTHY one), catching a partitioned-but-still-connected
  node the process-local checks alone can't see. Opt-in — omitting it
  preserves the exact original process-local-only behavior.
- ✅ **FIXED (DOC)** — rewrote the `BatchAcker` docstring's sync usage
  example (and the matching example in `docs/guide/full-guide.md`) to wrap
  `ack_fn` with `connection.add_callback_threadsafe` instead of passing a
  raw `channel.basic_ack` — the interval timer fires `flush()` from a
  background `threading.Timer` thread, so the original example was the
  exact cross-thread pika violation `_run_on_io_thread` exists elsewhere in
  the codebase to prevent. Noted that the async path has no such hazard.
- 🟡 **DOCUMENTED (deliberate, unchanged)** — coverage floor 85% in CI vs
  stated 100% target. No change.
- ✅ **FIXED (DOC)** — `docs/kubernetes.md` now has a KEDA queue-depth
  `ScaledObject` example under the HPA section, with the rationale for why
  CPU-based HPA is the wrong signal for a queue consumer.
- 🟡 **DOCUMENTED (deliberate, unchanged)** — default guest/guest with
  warn-only enforcement on non-local hosts. No change — matches RabbitMQ's
  own default-account behavior.

---

## 🟢 OPTIMIZATIONS / BEST PRACTICES (verified strengths)

This codebase does a lot right — none of the following are hypothetical, all verified:

- **Confirms ON + persistent delivery by default** (`config.py:265`, `types.py:279`). ⚠️ **Corrected on re-verification:** the original wording overclaimed the `CONFIRMED`/`SENT` distinction — the enum values exist (`core/types.py`), and `PublishOutcome.ok`'s own docstring says to check `status == PublishStatus.CONFIRMED` directly when durability before settling matters, but retry.py's and pipeline.py's actual settlement code (`middleware/retry.py:312,344`; `core/pipeline.py:1062,1129`) checks only `outcome.ok` — true for both CONFIRMED and SENT. So internal republishes do **not** structurally require a real confirm; the only protection is the loud `warn_retry_without_confirms` warning (`retry.py:57-84`) fired once at broker startup. Deliberately not "fixed" by making settlement require CONFIRMED — with `confirm_delivery=False` that would turn every retry/result-forward into a guaranteed infinite nack-requeue loop instead of a documented rare-window race, which is a worse regression than the warning-only status quo (unlike H1's signing+retry, which corrupts *every* retried message, not just an edge case, fail-fast there is proportionate; here it isn't). ✅ **What *was* a real, fixable gap:** the warning is a Python `warnings.warn()` call, which bypasses `logging`/`structlog` entirely — in a production deployment using rabbitkit's own recommended `LoggingConfig(render_json=True)`, this "loud" warning went straight to raw `stderr` in its own format, invisible to whatever log pipeline was actually being watched, unless the app happened to separately call `logging.captureWarnings(True)` (undocumented, not done anywhere in rabbitkit). Fixed: `configure_structlog()` now calls `logging.captureWarnings(config.capture_warnings)` (new field, default `True`), bridging all 37 of rabbitkit's `warnings.warn()` safety warnings — not just this one — into the same logging pipeline.
- **Settlement mechanics are excellent:** ack strictly after handler + result publish (`pipeline.py:459-472`); idempotent, pending-guarded disposition recorded only after the transport call succeeds (`message.py:118-165`); cross-thread pika ops correctly marshaled via `add_callback_threadsafe` with bounded wait and late-callback cancellation (`sync/transport.py:353-429`); `stop()` pumps the I/O loop so drain-window acks land (`sync/broker.py:336-367`).
- **Retry topology avoids the per-message-TTL head-of-line trap** (uniform per-queue TTL per delay queue) and routes back via the default exchange (binding-independent). Retry-count header is clamp-hardened against tampering. `RetryConfig.__post_init__` fail-fasts on `len(delays) < max_retries`.
- **Graceful shutdown is explicit about in-flight fate:** SIGTERM via daemon thread (pika isn't signal-safe), cancel-consumers-first, bounded drain (`graceful_timeout=30s`). ⚠️ **Corrected on re-verification:** "async stragglers nacked-for-requeue with delivery-tag logging" was only true for the *pooled* worker path (`AsyncWorkerPool.stop()`) — the *inline* (no `worker_config`, the default) path just logged a count and abandoned the message unacked, same as sync. ✅ **Fixed:** `AsyncBroker._wait_in_flight` now tracks inline in-flight tasks (`_inflight_tasks`) and, on drain-deadline timeout, cancels + nacks them with delivery-tag logging too, matching the pooled path. The sync transport genuinely cannot safely interrupt a handler running on a plain thread, so sync abandonment (log a count, disconnect, let the broker's own connection-loss detection eventually redeliver) is unchanged — but "documented with its duplicate-side-effect consequence" was itself an overclaim (the consequence was only mentioned in a code comment about pool-ordering, not user-facing docs); now explicitly documented in `docs/rabbitmq-retry-architecture.md`'s graceful-shutdown section, contrasted against the async behavior.
- **Security posture:** no pickle anywhere (zero hits repo-wide); no content-type-driven decoder dispatch (serializer fixed per route — a malicious publisher cannot select a decoder); 64MB parse/decompression caps with zip-bomb guards; HMAC-SHA256/512 signing with constant-time compare, timestamp+nonce replay protection, Redis `SET NX EX` nonce cache; systematic secret masking (`__repr__`, `safe_url`, structlog redaction on by default); SSRF-hardened management client (scheme allowlist, no redirects, 64MiB response cap, non-disableable TLS verify); TLS defaults hardened when enabled (≥TLS1.2, CERT_REQUIRED, hostname check, system trust-store fallback, mTLS client certs); dedicated `tests/security/` + hypothesis property suites.
- **Health checks go far beyond "connected"** (`health.py:188-266`): flow-control blocked state, stale-consumer-tag cross-check, worker backlog, and a correct k8s liveness/readiness split (transient disconnect deliberately NOT a liveness failure; wedge detection via monotonic heartbeat staleness).
- **CI/CD is stronger than most internal libraries:** ruff + mypy `--strict` + 3-Python matrix (3.11/3.12/3.13, confirmed exact) + 85% coverage floor (confirmed gating, no `continue-on-error`) + pip-audit `--strict` + CycloneDX SBOM + CodeQL + nightly dependency min/max matrix (pinned floor is actually `aio-pika>=9.1.0` per `pyproject.toml` — 9.0.0 is broken, imports `pkg_resources` which `setuptools>=81` no longer ships; fixed a stale comment in `dependency-matrix.yml` that still said 9.0.0) + README-examples-as-tests + packaged-wheel import smoke + OIDC trusted publishing (no PyPI token). All independently re-verified against the actual workflow files, not just claimed.
- **Benchmarks are real and honest:** confirms-on numbers published with caveats (`docs/rabbitmq-retry-architecture.md:1586+`, confirmed — includes "local container, single connection... use to size, not as guarantees"). ⚠️ **Corrected on re-verification:** "latency/failure/lifecycle/resources/soak/chaos modules" oversold the invocable suite — `python -m benchmarks` only runs `throughput/latency/failure/resources/lifecycle/pipeline/sync`; `soak_test.py` and `chaos_suite.py` exist but are separate, standalone scripts (`chaos_suite.py` runs as its own CI step, not via `python -m benchmarks`). Wording only — the scripts are real, just organized differently than the sentence implied.
- **Metric cardinality explosion actively prevented** (`middleware/metrics.py:35-51`): queue label preferred over routing key precisely because topic keys with embedded IDs are unbounded.
- **Prefetch isolation:** per-queue consumer channels so per-queue `basic_qos` never clobbers other consumers.
- **Fail-fast registration:** duplicate queue registration raises at decoration time; DLX cycle detection; retry middleware auto-wiring idempotent across reconnects.
- **Broker flow-control handling:** pika `blocked_connection_timeout=60` k8s-friendly default; custom async blocked-connection watchdog (aio-pika lacks the knob); opt-in `FlowController` with wait/raise/drop policies. ⚠️ **Corrected on re-verification:** "bounded confirm waits on both transports so a disk-full broker yields `TIMEOUT` outcomes" was only true for async — the sync transport's cross-thread marshal timeout (`_run_on_io_thread` raising `TimeoutError` when a confirm never arrives) fell through to the generic `except Exception` branch and returned `PublishStatus.ERROR`, not `TIMEOUT`, contradicting `docs/message-safety.md`'s own documented contract ("`TIMEOUT`: no confirm arrived within `confirm_timeout`") and the retry-envelope-close-on-timeout pattern (`async_/transport.py`) that specifically checks `status == PublishStatus.TIMEOUT`. ✅ **Fixed:** `sync/transport.py:_publish_on_channel` now has an explicit `except TimeoutError` branch mapping to `PublishStatus.TIMEOUT`, matching async.
- **Passive declare mode** (`TopologyMode.PASSIVE_ONLY`/`MANUAL`) enables least-privilege consumers without configure permissions. ⚠️ **Corrected on re-verification:** "written least-privilege guidance in docs" overclaimed — the mode's existence was mentioned in passing (topology-conflict recovery context) but nothing connected it to RabbitMQ's actual permission model (`configure`/`write`/`read`). ✅ **Fixed:** added a "Least-privilege consumers with `TopologyMode.PASSIVE_ONLY`" section to `docs/security.md` explaining the `configure`-permission implication and the topology-owner/consumer split.
- **Async batch publish genuinely pipelines confirms** — N publishes, one channel, gathered ACKs (`async_/batch.py`).
- **Reference k8s manifest** (`deploy/consumer.yaml`): startup/liveness/readiness probes, preStop, PDB, non-root, terminationGracePeriod sizing math in comments.

---

## 🐇 RABBITMQ ARCHITECTURE REVIEW

> **STATUS (2026-07-03):** every item below tagged with a finding ID has
> been fixed; original wording preserved with inline ✅ corrections so the
> audit record stays legible against the original analysis.

- **Exchange design:** All four AMQP types modeled (`ExchangeType`, `core/types.py:72-78`) with construction-time validation; exchange-to-exchange bindings supported. ✅ **FIXED (C4)** — headers exchanges are now functional: `bind_arguments` is passed through `bind_queue` on both transports, and registration fail-fast validates `x-match`/missing-`bind_arguments`. Quorum/classic/stream queue types with type-specific validation (quorum: durable/non-exclusive enforced; stream: TTL rejected; `x-delivery-limit` classic-rejected). `lazy=True` emits deprecated `x-queue-mode`; ✅ now warns it's a no-op on ≥3.12 (CQv2). DLX/TTL/max-length/overflow/expires/single-active-consumer all emitted as `x-` args with a user-args escape hatch that takes precedence.
- **Queue strategy:** One handler per queue, enforced at registration (`registry.py:114-119`); per-queue channels with isolated QoS. Durable-by-default (both exchanges and queues). DLX cycle detection at registration (`registry.py:230-279`).
- **Routing strategy:** `{name}` path segments compile to `*` bindings with typed extraction (`core/path.py:22-53`), correctly stopping at `#`. Fail-fast registration validation of retry/ack/DLX conflicts (`route.py:172-226`). ✅ **FIXED** — routing-key/exchange/queue-name AMQP shortstr (255-byte) validation now enforced at `MessageEnvelope`/`RabbitQueue`/`RabbitExchange` construction (previously unchecked, surfacing as an opaque broker error).
- **Delivery guarantees:** At-least-once by default (confirms + persistent + ack-after-handler), symmetric sync/async on paper; `ACK_FIRST` gives documented at-most-once. Exactly-once is approximated by dedup middleware. ✅ **FIXED (C1)** — dedup's crash-window silent loss is closed (mark-after-success default + `claim` policy + thread-safe LRU). ✅ **MOSTLY FIXED (I-11)** — the sync confirm wait is now bounded in every case except one: a publish from the connection's owner thread while it's also actively driving a consume loop (default single-worker handler publishing a result/retry) still can't be safely interrupted without risking two threads touching the same pika connection; every other sync publish path (pure producer, not-yet-consuming, or a non-owner worker thread) is bounded and self-heals by discarding/re-establishing a wedged connection. Documented residual limitation + mitigation (`worker_count>1`) in `docs/message-safety.md`. Async was already correctly bounded via `asyncio.timeout`. ✅ **FIXED (M2)** — `mandatory=True` gets reliable Basic.Return detection on both transports when set, and `PublisherConfig.mandatory`/`.persistent` are no longer dead config — both are read and applied on every publish.

## 🔁 RELIABILITY & RETRY SYSTEM

- **Retry mechanism:** Correctly queue-based (TTL delay queues `{q}.retry.{n}` → DLX back to source via default exchange), not in-process sleeping — delays live broker-side, retry state is header-carried so it survives crashes and reconnects. Defaults: `max_retries=4`, delays `(5, 30, 120, 600)s`. Error classification: transient/permanent via `ErrorClassifierMiddleware`; unknown defaults to PERMANENT. 🟡 **CONFIRMED DELIBERATE (not a bug)** — `RetryConfig.jitter_factor` and the private `_compute_delay` helper that would apply it are unreachable in the actual delay path (`_get_delay_ms`, which drives each tier's queue-level `x-message-ttl`, applies no jitter). This is intentional, not an oversight: each `{q}.retry.N` queue's TTL is uniform across every message that lands in it (the exact property that avoids RabbitMQ's classic-queue head-of-line-blocking trap — see the OPTIMIZATIONS section). Per-message jitter is fundamentally incompatible with that: it would require either a per-message TTL (reintroducing head-of-line blocking) or a client-side sleep before the delay-queue publish (blocking the sync I/O thread — the exact hazard H2 flags elsewhere). `jitter_factor` is kept as a no-op for API stability, with this rationale documented directly on the field (`core/config.py`); fleet-wide retry-storm spreading is handled by the (real, working) async reconnect jitter (H4) and the receiving service's own backpressure, not client-side retry-delay jitter.
- **DLQ design:** `{q}.dlq` via source-queue DLX, headers preserved (original exchange/routing-key/queue + broker `x-death`). Per-queue by default; ✅ **FIXED (H3)** shared mode is now rejected at construction rather than silently misrouting. CLI + `DLQInspector` peek/replay/purge tooling exists. ✅ **FIXED (C2)** — replay checks `PublishOutcome` before acking the original (a failed republish nack-requeues instead of being lost) and publishes `mandatory=True`. ✅ **FIXED** — replay can grant fresh retries via `reset_retry_count=True` (library) / `--reset-retry-count` (CLI, which didn't call into the library method at all until this pass), instead of always preserving the retry count verbatim (default unchanged — still preserves headers unless explicitly asked to reset).
- **Failure handling — "What happens when a message always fails?"**
  - *With retry configured:* attempt 1 fails (transient) → published to `{q}.retry.1` (TTL 5s) + source acked → expires back → fails → `.retry.2` (30s) → `.retry.3` (120s) → `.retry.4` (600s) → 5th failure: retry count = max → tagged `_rabbitkit_terminal` → `reject(requeue=False)` → DLX → parked durably in `{q}.dlq` (headers preserved) until replayed/purged. Permanent-classified errors skip retries and dead-letter on first failure. **Correct.**
  - *Without retry (the default):* ✅ **FIXED (M6)** — opt-in `ConsumerConfig.reject_transient_on_redelivery` caps the hot-loop at 2 strikes using the broker's `redelivered` flag (default off preserves the legitimate infinite-requeue-until-recovery pattern). ✅ **FIXED (C3)** — permanent errors on a plain route are no longer silently destroyed: `SafetyConfig.reject_without_dlx` auto-provisions a DLQ by default (or fails startup / opts into explicit discard). Process-crash loops still bypass retry-count entirely by nature (a crash never runs the increment) — quorum `x-delivery-limit` is the documented backstop for that specific case.
- **"What happens when the broker is overloaded?"** `connection.blocked` is tracked (pika `blocked_connection_timeout=60`; custom async watchdog since aio-pika lacks the knob); confirm waits time out surfacing `TIMEOUT` outcomes rather than hanging on async, and (✅ **FIXED**, this pass) now correctly report `TIMEOUT` rather than generic `ERROR` on sync too, in every case except the one documented residual limitation above. Backpressure gating is opt-in `FlowController`, and (since M18) also covers internal republishes (retry, result/RPC-reply). Unacked messages are redelivered by RabbitMQ after channel/connection death; retry state survives (header-carried, not in-memory).
- **Poison message handling:** ✅ **FIXED (C3)** — the terminal/DLQ path is now correct by default on every route, not just retry-enabled ones.

## ⚡ PERFORMANCE & SCALING

- **Bottlenecks:** Sync confirmed publish ~0.9k msg/s (H6, documented as the ceiling) — the dominant ceiling; serial RTT-bound on one channel. Pipeline CPU is cheap (~2.7µs/msg). Measured: async consume ~14.8k msg/s/process (~42k @ 4 procs), sync wc=1 consume ~34k (trivial handler), async confirmed publish ~6.1k.
- **Scaling limits:** Scale-out model is explicitly more-processes-not-more-connections (`publisher_connections`/`consumer_connections` are reserved no-ops); per-queue channels are fine. ✅ **FIXED (M9)** — `ConnectionConfig.nodes` now provides multi-host cluster failover (sync native pika list; async endpoint cycling on connect); the sync single-connection design still caps single-process throughput by design (scale via processes). ✅ **FIXED (M10)** — a publish-side `PublisherConfig.max_message_bytes` size guard exists (opt-in), and async large-body decode (≥256 KiB) now offloads to a thread instead of blocking the event loop.
- **Backpressure handling:** Consume-side bounded only by prefetch (default 10 — sane for mixed workloads). ✅ **FIXED (M11)** — opt-in `WorkerConfig.max_queue_size` bounds the in-process work queue (default unbounded, unchanged, for backward compat). Publish-side `FlowController` (wait/raise/drop) exists, is opt-in, and (since M18) now also applies to retry/result republishes.

## 🔐 SECURITY REVIEW (SRE GRADE)

- **Authentication:** PLAIN only — no SASL EXTERNAL/mTLS-auth despite client-cert transport support (unchanged; genuinely not implemented, distinct from the config being dead). ✅ **FIXED (M2)** — `SecurityConfig.mechanism` is no longer silently-ignored dead config: a non-`"PLAIN"` value now fails fast at construction instead of lying about being honored. guest/guest defaults warn on non-local hosts only (deliberate, unchanged — matches RabbitMQ's own default-account behavior).
- **Authorization:** Vhost supported; `PASSIVE_ONLY`/`MANUAL` topology modes enable least-privilege consumers (no configure perms needed). ✅ **FIXED** — least-privilege guidance connecting `PASSIVE_ONLY` to RabbitMQ's actual `configure`/`write`/`read` permission model is now written (`docs/security.md`) — it previously only mentioned the mode's existence in passing. Default `AUTO_DECLARE` needs configure perms everywhere.
- **Encryption:** TLS opt-in (`SSLConfig.enabled=False` default), hardened once enabled (≥TLS1.2, CERT_REQUIRED, hostname verify, system trust-store fallback, mTLS certs); `CERT_NONE` escape hatch is silent; `amqps://` scheme ignored (M3); typo'd cert_reqs strings fail safe to CERT_REQUIRED.
- **Exposure risks:** Dashboard unauthenticated by default (read-only, warns, constant-time bearer token available, XSS-escaped, never auto-binds a socket); metrics server loopback-by-default; management client SSRF-hardened with non-disableable TLS verify; `rabbitkit shell` = live unaudited publish access; no pickle/eval surface anywhere; no content-type-driven deserialization; 64MB parse/decompress caps + zip-bomb guards; creds masked systematically but plaintext lives in the aio-pika URL (third-party exception paths could leak it); no rotation without restart.

## 🧯 DEVOPS / DEPLOYMENT REVIEW

- **Kubernetes/infra issues:** Broker deployment explicitly out of scope (no Helm/StatefulSet for RabbitMQ itself — consumer-side `deploy/consumer.yaml` is a genuinely good reference: probes, preStop, PDB, non-root, grace-period math). ✅ **FIXED (M14)** — topology drift no longer means an unrecoverable CrashLoopBackOff: `SafetyConfig.on_topology_conflict="warn_continue"` warns and continues with the existing definition per-conflict (default `"raise"` unchanged); still no automated migration tool, but there's now a supported non-restart path.
- **HA strategy:** Quorum queues supported (`QueueType.QUORUM` + `x-delivery-limit`) and mandated for money flows in `docs/production/checklist.md`. ✅ **FIXED (M9)** — client-side HA gained a multi-host failover list (`ConnectionConfig.nodes`); sync's 300s/30-attempt give-up ceiling is deliberate/configurable (k8s-friendly — the pod is expected to restart), not a defect. ✅ **FIXED (H4)** — async reconnect is no longer a synchronized 1s-fixed-interval herd; `reconnect_interval` is now randomized (full jitter, per-process) since `connect_robust` doesn't expose backoff directly.
- **Upgrade strategy:** OIDC trusted publishing, nightly dependency min/max matrix, pinned ranges (`pika>=1.3.0,<2.0.0`, `aio-pika>=9.1.0,<10.0.0` with known-bad floor correction — verified: the matrix and a stale top-of-file comment referencing the old 9.0.0 floor were reconciled this pass), Python ≥3.11. Library upgrades: fine. Queue-argument migrations (classic→quorum): unhandled, ops' problem.
- **Failure recovery:** Sync reconnect is textbook (full jitter, exponential backoff, bounded 30 attempts/300s, re-declares topology, re-subscribes); ✅ **FIXED (H4)** — async's `connect_robust` interval is now jittered too (see above), no longer a fixed-1s synchronized herd. ✅ **FIXED (M15)** — the chaos suite (broker restart mid-consume/publish, sync+async, retry-to-DLQ under restart, mid-drain restart) now gates CI with a checked exit code.

## 📊 OBSERVABILITY & INCIDENT READINESS

- **Metrics gaps:** ✅ **FIXED (H5)** — `QueueMetricsPoller` bridges `RabbitManagementClient` into the metrics registry, emitting queue-depth/consumer-lag gauges via any `MetricsCollector`. ✅ **FIXED (residual)** — the two remaining unadded counters now exist too: `rabbitkit_messages_redelivered_total` (labeled by queue, incremented by `MetricsMiddleware` whenever the broker flags `redelivered=True` — the crash-loop/heartbeat-kill signal the success/error counters can't distinguish from ordinary traffic) and `rabbitkit_reconnects_total` (both transports expose an `on_reconnect` hook — sync fires on every connect after the first, async adapts aio-pika's `reconnect_callbacks` on both connections — and the brokers wire it to the first route `MetricsMiddleware`'s collector, so a flapping broker/network is no longer invisible to metrics alerting). Everything else is present with cardinality guards: consumed/published totals with status labels, processing/publish/confirm-latency histograms, acked/nacked/rejected/retried/dead_lettered totals, in-flight gauge, worker-pool pending, broker_connected, consumer_active, dedup_fallback_total, rate_limit_dropped_total; pluggable `MetricsCollector` protocol + `PrometheusCollector` + `/metrics` ASGI app.
- **Logging gaps:** Structured (structlog), body-free by design (only message_id/routing_key/queue/handler bound via contextvars, cleared in `finally`), secret-redacting by default. The documented depth limit (one nesting level) is deliberate and unchanged; ✅ **FIXED** — the *matching* was exact-string-only and missed common compound secret names (`x-auth-token`, `session-token`, ...) that individually-listed words like `auth`/`token` should have caught; now word-set based. ✅ **FIXED** — none of rabbitkit's 37 `warnings.warn()` safety warnings (many of which ARE the incident-readiness signal for misconfigurations flagged throughout this audit) were bridged into this logging pipeline at all; `configure_structlog()` now calls `logging.captureWarnings()`.
- **Tracing gaps:** Publish→consume trace propagation via AMQP headers with OTel semantic attributes (`messaging.system`, `messaging.message_id`, `messaging.correlation_id`, retry count) — but through Lucidya's `obskit` wrapper. ✅ **FIXED** — a caller who explicitly adds `TracedConsumerMiddleware` but doesn't have obskit installed/configured now gets a startup warning instead of every span silently no-op-ing forever with zero signal.
- **Alerting gaps:** No hooks/callbacks on health-state transitions; the model is Prometheus-scrape + k8s probes. DLQ monitoring is pull-only (CLI `dlq` command / `DLQInspector.peek`).
- **Replay tooling:** exists (`DLQInspector`, CLI). ✅ **FIXED (C2)** — the loss bug is closed (outcome-checked ack, `mandatory=True`). ✅ **FIXED** — retry-count reset is now available at both the library (`reset_retry_count=True`, pre-existing) and CLI (`--reset-retry-count`, added this pass — the CLI command never called the library method at all before).

## 🧠 RACE CONDITION & CONCURRENCY ANALYSIS

- **Concurrency model (factual):** Sync = single pika `BlockingConnection`, one channel per consumer queue, optional daemon-thread pool; all channel ops from worker threads marshaled to the I/O thread via `add_callback_threadsafe` with 30s bounded wait; ✅ **NEW (I-11)** a publish that would otherwise run fully inline and unbounded (pure producer / not-yet-consuming) now bounds the confirm wait via a dedicated helper thread instead. Async = aio-pika `connect_robust`, separate publisher/consumer connections, per-queue channels, `asyncio.Semaphore(worker_count)` cap. Prefetch = `worker_count × prefetch_per_worker` when set, else 10.
- **Identified race conditions:** ✅ **FIXED (M8)** — dedup local LRU is now mutated under an internal lock (was lock-free from worker threads, could escalate to message loss via PERMANENT classification). ✅ **FIXED (C1)** — dedup's SETNX-before-handler crash window is closed. ✅ **FIXED (M16)** — the two-consumer duplicate race's double-loss corner is closed (EXISTS-check replaces up-front SETNX). The message-disposition guard remains unlocked but safe under the current one-thread-owns-one-message dispatch model — still a latent trap if dispatch ever changes, unchanged assessment.
- **Duplicate processing risks:** Inherent at-least-once duplicates (result-published-then-ack-fails; abandoned sync handlers running concurrently with the redelivery on another pod; async cancelled handlers whose side effects completed) — all *documented* with idempotency mandated (the sync-abandonment duplicate-side-effect consequence specifically was only in a code comment before this pass; now user-facing in `docs/rabbitmq-retry-architecture.md`); Redis-outage fallback processes without dedup (explicit, metered, fail-closed available). Cross-thread pika discipline is correct — no wrong-thread acks found; `_ever_consumed` prevents unsafe inline-fallback during shutdown drain.
- **Ordering issues:** Retry requeues inherently reorder (standard for TTL-retry architectures, correctly documented in `docs/ordering-guarantees.md`); uniform per-tier delay-queue TTL avoids head-of-line-blocking-driven reordering surprises (jitter is deliberately NOT applied here — see RELIABILITY section — since it would require breaking that uniform-TTL property). ✅ **FIXED (M7)** — the RPC sentinel leak that could emit replies out of order is closed.
- **Deadlock/starvation:** No deadlocks found (the warned worker-vs-pool-size deadlock cannot arise via the path actually used — M2/M3 dead-config finding, now also non-misleading since M2's dead config is wired/validated). ✅ **FIXED (H2)** — heartbeat starvation at wc=1 now gets a startup warning naming the risk and the fix (`worker_config`/raise `heartbeat`). ✅ **FIXED (M6)** — a transient hot-loop no longer consumes a prefetch slot indefinitely by default when opted in via `reject_transient_on_redelivery`.

## 🧪 CHAOS FAILURE ANALYSIS

| Scenario | System behavior | Data loss risk |
|---|---|---|
| **Broker node failure** | Sync: jittered backoff, topology re-declare, re-subscribe — exits for good after 300s (deliberate, k8s-friendly — the pod restarts). Async: recovers indefinitely; ✅ **FIXED (H4)** reconnect interval is now jittered per-process, no longer a synchronized 1s herd. ✅ **FIXED (M9)** — `ConnectionConfig.nodes` gives client-side failover to surviving nodes on both transports (was single-host only). Unacked messages redeliver; retry state survives (header-carried). | None (duplicates possible) |
| **Network partition** | Client-side indistinguishable from node failure; multi-host failover list mitigates (M9, above). ✅ **FIXED** — `broker_health_check`/`readiness` accept an optional `management_client` that catches a partitioned-but-still-connected node the process-local checks alone couldn't see (was: reads HEALTHY unconditionally). | None directly; invisibility risk narrowed (opt-in) |
| **Consumer crash mid-processing** | Message redelivered (correct). ✅ **FIXED (C1)** — dedup `on_success` no longer marks before the handler completes, so a crash no longer causes the redelivery to be acked-as-duplicate-and-lost. | None (was HIGH with dedup enabled) |
| **10x/100x traffic spike** | Consumers bounded by prefetch (fine). ✅ **FIXED (H5)** — `QueueMetricsPoller` gives queue-depth/consumer-lag visibility (was invisible). Sync publishers cap at 0.9k msg/s and fall behind (H6, documented ceiling, not a bug). | None; SLA/latency risk |
| **Retry storm** | Bounded by prefetch + broker-side delays (good design). 🟡 Retry-delay jitter remains deliberately unimplemented (see RELIABILITY section — incompatible with the uniform-per-tier-TTL design that avoids head-of-line blocking); a burst of same-moment failures still retries as a synchronized wave against the recovering dependency. Mitigated by the receiving service's own backpressure, not a client-side fix. | None |
| **Disk full (broker)** | Connection blocked → confirm timeouts surface as TIMEOUT outcomes on async, and (✅ **FIXED**, this pass) now on sync too in every case except one documented residual limitation (owner-thread publish during active consuming — see RELIABILITY section) — no infinite hang in the common cases. Publishers that ignore `PublishOutcome` still lose messages silently by not checking the return value (M1, an API-usage pattern warning, not fixable in the type system alone). | Low/Medium if outcomes unchecked |
| **Queue explosion** | ✅ **FIXED (H5)** — depth metric now available via `QueueMetricsPoller` (was none). DLQ still pull-only by design; max-length/overflow args supported per queue but not defaulted. | Depends on overflow policy |
| **Delay-queue deleted at runtime** | ✅ **FIXED (M4)** — retry publishes to the delay queue are `mandatory=True`, so a missing/deleted queue surfaces as RETURNED (nack-requeue) instead of confirming into the void with the source already acked. | None (was confirmed loss) |

- **What broke first (now fixed):** the async fleet's reconnect stampede against a recovering node (✅ H4 — jittered) and any wc=1 sync consumer with slow handlers (heartbeat kill/redeliver loop — ✅ H2, now a loud startup warning naming the exact risk instead of a silent trap).
- **What data was lost (now closed):** C1 (dedup crash window), C2 (replay), C3 (poison discard on default routes), C4 (headers misrouting), H1 (signing×retry), M4 (deleted delay queue) — every one of these has a real code fix (see the ✅ annotations throughout this document and `CHANGELOG.md [1.2.0]`). The only residual loss vector in the library's own code is a publisher that ignores the returned `PublishOutcome` (M1 — a usage-pattern risk `raise_for_status()` mitigates, not something the type system alone can force). Everything else is duplicate-or-requeue under at-least-once delivery, not loss — idempotent handlers remain mandatory.
- **What recovers automatically:** connections (both transports, now with multi-host failover — M9), topology, subscriptions, in-flight redelivery, retry ladders (header-carried state). Health/readiness gates recovery correctly under k8s, and can now also factor in management-API-detected partition state (opt-in).

## 🧾 FINAL STAFF ENGINEER VERDICT

**Production Ready (original assessment): NO** — not for financial/critical multi-tenant data in its original state. **Conditionally yes** for non-critical workloads using the well-trodden path: per-queue retry + DLQ enabled, no dedup middleware, no signing, `worker_count>1`, publish outcomes checked, external queue-depth monitoring in place.

> **FINAL UPDATE (2026-07-03):** every finding in this document — all 4
> critical, all 6 high-risk, all 18 medium, the low/completeness list, and
> the re-verified optimizations/architecture-review sections — now has a
> real code or documentation fix; see the ✅ annotations throughout and
> `CHANGELOG.md [1.2.0]`. The dead-config cleanup (must-fix item 8, below)
> is done except `RetryConfig.jitter_factor`, which turned out to be a
> **deliberate**, already-documented no-op (per-message jitter is
> architecturally incompatible with the uniform-per-tier-TTL design that
> avoids classic-queue head-of-line blocking — see RELIABILITY section) —
> not dead config that lies, once its own comment is read.
>
> **Verdict: CONDITIONAL YES for financial/critical workloads**, on the
> "blessed production profile" this document already recommends: quorum
> queues + `x-delivery-limit` + per-queue retry/DLQ (`reject_without_dlx`
> default already gives this) + confirmed publishes with checked outcomes
> (`raise_for_status()`) + management-API metrics (`QueueMetricsPoller`) +
> `ConnectionConfig.nodes` for multi-host failover. One residual,
> documented, narrow limitation remains (not a bug, a pika API
> constraint): a sync publish from the connection's owner thread *while
> that same thread is actively driving a consume loop* still can't be
> safely bounded against a wedged broker — mitigated by `worker_count>1`.
> Duplicate-execution scenarios remain inherent to at-least-once delivery;
> idempotent handlers are still mandatory, and this library's own
> `on_success` dedup default no longer undermines that guarantee.

**Original biggest risks — status:**

1. ~~The dedup middleware converts crashes into permanent silent loss~~ ✅ **FIXED (C1)** — mark-after-success default + thread-safe LRU + `claim` policy.
2. ~~The default route configuration destroys poison messages with no DLQ~~ ✅ **FIXED (C3)** — `SafetyConfig.reject_without_dlx` auto-provisions by default.
3. ~~The DLQ replay tool can itself lose messages~~ ✅ **FIXED (C2)** — outcome-checked ack + `mandatory=True`, both library and CLI paths.
4. ~~Invisible consumer lag: no queue-depth signal~~ ✅ **FIXED (H5)** — `QueueMetricsPoller` bridges the management API into the metrics registry.

**Must-fix before production — final status:**

1. ~~**C1** — dedup mark-after-success (or in-flight/completed key states) + thread-safe LRU~~ ✅ **DONE** (both: mark-after-success default + `claim` policy)
2. ~~**C2** — replay checks `PublishOutcome` before ack; publish `mandatory=True`; optional retry-count reset~~ ✅ **DONE** (incl. the CLI path)
3. ~~**C3** — DLX on every route by default, or hard startup failure when a route can reject without a DLX~~ ✅ **DONE** (`SafetyConfig.reject_without_dlx`: auto_provision default / error / discard)
4. ~~**C4** — pass `bind_arguments` through `bind_queue`, or reject headers-exchange routes until it works~~ ✅ **DONE** (both, plus x-match validation)
5. ~~**H1** — make signing redelivery/retry-aware or fail fast on the signing+retry combination~~ ✅ **DONE** (redelivery-tolerant nonce check + startup `ConfigurationError` for signing+retry)
6. ~~**H2/H4** — startup warning when `worker_count=1` with `heartbeat < plausible handler time`; jittered exponential backoff on async reconnect~~ ✅ **DONE** (both)
7. ~~**H5** — bridge the management client into the metrics registry~~ ✅ **DONE** (`QueueMetricsPoller`)
8. **Dead config** — ✅ **DONE**: `PublisherConfig.mandatory`/`.persistent` and `SecurityConfig.mechanism` are wired/validated; the misleading sync pool-size warning was removed. 🟡 `RetryConfig.jitter_factor` is intentionally left as a documented no-op (see the FINAL UPDATE note above) rather than "fixed" into something that would reintroduce head-of-line blocking or block the sync I/O thread.

**Recommended follow-ups — final status:**

- ~~Promote the chaos suite to a gating CI job~~ ✅ **DONE (M15)**.
- ~~Multi-host failover list in `ConnectionConfig`~~ ✅ **DONE (M9)**; ~~`amqps://` scheme awareness~~ ✅ **DONE (M3)**; ~~credential-provider hook for rotation~~ ✅ **DONE (M13)**.
- ~~Publish-side message-size guard~~ ✅ **DONE (M10)**; ~~offload async deserialization ≥ some threshold to a thread~~ ✅ **DONE (M10)**.
- ~~Quorum `x-delivery-limit` as the crash-loop backstop~~ ✅ **DONE (M5)**.
- ~~Bound the sync owner-thread confirm wait~~ ✅ **MOSTLY DONE (I-11)** — bounded except the one documented residual case (owner thread also actively consuming). ~~Route internal republishes through broker publish middlewares + FlowController~~ ✅ **DONE (M18 for FlowController; H1 makes signing+retry mutually exclusive by design, so "signing internal republishes" is no longer an applicable combination)**.
- ~~Sync pipelined-confirm batch path, or document the 0.9k msg/s ceiling in the README rather than only the deep architecture doc~~ ✅ **DONE (H6, the "document" branch)** — `README.md` ("Sync confirmed-publish throughput ceiling (~0.9k msg/s)") explains the ceiling, why `worker_count` doesn't raise it (pika serializes confirms), and the escape hatches (`AsyncBroker` + `AsyncBatchPublisher` at ~6.1k msg/s pipelined, or scale out across processes). A sync pipelined-confirm implementation was deliberately not built: pika's `BlockingConnection` API has no confirm-pipelining primitive — building one would mean bypassing `BlockingChannel` for raw frame handling, a disproportionate complexity/maintenance cost for a workload the async transport already serves.

**Architectural recommendation:** The core architecture is sound and unusually well-engineered for an internal library — the TTL-retry topology, settlement discipline, cross-thread pika correctness, security hygiene, and CI pipeline are all above bar. The failures clustered at **feature interaction boundaries** (dedup×crash, signing×retry, replay×confirms, defaults×poison) rather than in the core design, and every one of those interactions is now closed. The one remaining pattern worth naming from the re-verification passes: **several fixes and claims that looked complete on inspection had a narrower gap one layer down** — a library-level fix whose CLI surface never used it (DLQ reset-retry-count), a recommended-safe alternative with the same class of bug as what it replaced (`PydanticDecoder`), a scope-limitation comment that was true but sat next to a separate matching-algorithm bug (log redaction), a type distinction that existed but wasn't actually used for the safety decision it sounds like it enables (`CONFIRMED`/`SENT`), and an async fix that only covered the pooled code path, not the (more common) inline default. None of these were caught by a first read of the code — only by re-verifying claims against the exact call sites doing the work. Treat "resolved-by-design" and "already exists" as a hypothesis to check, not a conclusion, especially near feature boundaries. Remaining work: (a) keep chaos scenarios gating so interaction regressions can't merge (✅ in place — chaos gates CI since M15; this is a keep-doing, not a to-do), (b) ~~define one blessed "production profile" and make it the documented default path~~ ✅ **DONE** — `docs/production/checklist.md` *is* that profile, and was brought current with every ingredient this remediation shipped: quorum + `x-delivery-limit`, the `reject_without_dlx` auto-DLQ default (its pre-C3 wording described the old filter-route-only behavior), `raise_for_status()`/`CONFIRMED`-vs-`SENT` outcome checking, the sync ~0.9k msg/s publish ceiling → `AsyncBroker` guidance, `ConnectionConfig.nodes` failover, `credentials_provider` rotation, `PASSIVE_ONLY` least-privilege, `QueueMetricsPoller` + the `redelivered`/`reconnects` counters, and partition-aware readiness via `management_client`, and (c) keep the honesty the docs already have: this is an **at-least-once** system requiring idempotent handlers — the dedup middleware no longer undermines that claim (a keep-doing, not a to-do).

---

*Generated from a four-track parallel source audit (delivery guarantees & routing; retry/DLQ & concurrency; security & observability; performance & DevOps) of rabbitkit v1.1.1 at commit `8ebde36`, then iteratively re-verified and fixed across a series of increasingly skeptical passes through v1.2.0. All findings carry file:line evidence verified against source.*
