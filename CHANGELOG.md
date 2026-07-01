# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Fixed

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