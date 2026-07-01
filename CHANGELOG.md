# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] — 2026-07-01

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

## [1.0.0] — 2026-06-29

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