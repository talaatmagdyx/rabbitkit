# Concurrency model

Which thread (or event loop) may call what, for every component. The
rules below are invariants the code enforces or relies on — violating
them is undefined behavior even where it happens to work today.

One framing rule up front: **`core/` owns no threads.** All concurrency
in rabbitkit comes from the transport layer (`sync/`, `async_/`) and the
explicitly-threaded helpers. The pipeline, middleware chain, DI resolver,
and serializers run on whatever thread or task the transport invokes
them from.

## Quick reference

| Component | Runs on | Safe to call from |
|---|---|---|
| `SyncBroker.start()/run()/stop()` / transport ops | the **owner thread** (the thread that called `start()`) | owner thread only |
| Sync handlers (default, `worker_count=1`) | the owner/I-O thread, inline | n/a (invoked by the broker) |
| Sync handlers (`worker_count=1` with `ConsumerConfig.processing_timeout`) | one dedicated `SyncWorkerPool` daemon thread (same order, one at a time) | n/a |
| Sync handlers (`worker_count>1`) | `SyncWorkerPool` daemon threads | n/a |
| Sync `processing_timeout` checks and settlement of an expired delivery | the owner/I-O thread (consume-loop tick, and the `stop()` drain) | internal |
| Sync re-subscription of a broker-cancelled consumer, `on_consumer_cancelled` / `on_consumer_resumed` hooks | the owner/I-O thread (pika's cancel callback, consume-loop tick) | internal |
| `message.ack()/nack()/reject()` (sync) | any handler thread — marshalled to the I/O thread internally | the thread running that handler |
| `SyncBroker.pump_idle()` | — | owner thread only |
| `broker.publish()` (sync) | — | owner thread only |
| `SyncBatchPublisher.publish()` | dedicated `SelectConnection` I/O thread inside | **any thread** (this is its purpose) |
| Startup/shutdown hooks (sync, with timeout) | a fresh single-worker executor thread per hook | n/a |
| `AsyncBroker` — everything | one asyncio event loop | that loop only |
| Async handlers | tasks on the broker's loop — up to `prefetch_count` per queue run concurrently unless `worker_count >= 2` (then the pool size bounds them); `worker_count=1` does **not** serialize them | n/a |
| Large-body decode (≥ 256 KiB, async) | `asyncio.to_thread` worker | internal |
| `HealthWatcher` | its own daemon poller thread | constructed anywhere; `stop()` from anywhere |
| `AsyncHealthWatcher` | an asyncio task on the broker's loop | that loop |
| `broker_liveness()` / probe reads | — | **any thread** (read-only, see benign races) |
| `TestBroker` | the calling thread, synchronously | single-threaded use |

## Sync side (pika)

### The owner-thread invariant

`SyncTransport` records the thread that starts consuming
(`_owner_ident`) and **every transport operation must come from that
thread**: publishing, topology declaration, `pump_idle()`, `stop()`.
pika's `BlockingConnection` is not thread-safe; rabbitkit does not try
to make it so — it makes the boundary explicit instead.

**Why there's no `threading.Lock` around reconnect.** The obvious-looking
fix for "don't let two threads reconnect at once" is a lock around the
reconnect call. rabbitkit deliberately doesn't have one — the sticky
owner-thread-identity check inside `_ensure_connected()` is strictly
*stronger*: a non-owner thread reaching a dead connection gets a clean,
immediate `ConnectionError` (reconnection is the owner thread's job,
period), rather than blocking on a lock only to then either reconnect
redundantly or touch a connection object it doesn't own. A lock would
also invite a second bug class a pure identity check can't have: a
non-owner thread that queues up on the lock, waits through a slow
reconnect, and then proceeds to use the (still not thread-safe)
`BlockingConnection` from the wrong thread once the lock releases — the
identity check forecloses that path entirely instead of just
serializing it. If you're reviewing this code expecting a lock here,
this is why one was deliberately not added.

### Handlers and settlement

- Default (`worker_count=1`): handlers run **inline on the I/O thread**.
  Simple, ordered — but a handler slower than the heartbeat interval
  starves heartbeats (the broker warns about this at startup).
- `worker_count>1`: handlers run on `SyncWorkerPool` **daemon** threads
  (daemon so a hung handler cannot block process exit; the graceful-drain
  deadline, not thread teardown, is what bounds shutdown).
- Settlement from a worker thread is safe **because it is marshalled**:
  `ack()/nack()/reject()` internally hop to the I/O thread via pika's
  `add_callback_threadsafe` and block for the result (bounded by an
  I/O-stall timeout). Handlers never touch the channel directly.

### Publish byte budget (`max_in_flight_bytes`)

With `PublisherConfig.max_in_flight_bytes` set, every sync transport publish
reserves its body's bytes before anything else and holds them until its
outcome is known. A worker thread's publish reserves them on the worker
thread and then waits for the I/O thread to run it, so its bytes are held
while it waits. That creates one hazard the async broker cannot have: if the
I/O (owner) thread itself publishes (an I/O-tick hook, the deadline path
settling an expired delivery through retry or DLQ) while workers hold the
budget, and it simply slept until there was room, nothing would run the
workers' publishes and both sides would wait until the admission timeout.

So the waiters differ by thread:

- **Worker threads** (and any thread when no consume loop has run on the
  connection) sleep on the limiter's condition until they are granted or
  `PoolConfig.channel_acquire_timeout` expires.
- **The owner thread of a connection that has run a consume loop** never
  sleeps there: between admission checks it drives
  `process_data_events(time_limit<=10 ms)`, which runs the marshalled worker
  publishes; when they finish and release their bytes it is admitted in its
  turn (the queue is FIFO for every thread). A connection error raised while
  it pumps makes its publish `NOT_SENT` (`not_connected`); the consume loop
  then recovers as usual.

pika runs marshalled callbacks only from the outermost `process_data_events`
on the stack. An owner-thread publish made from *inside* a pika callback, an
inline handler (`worker_count=1` without `processing_timeout`), therefore
cannot run them while it waits. That case has no worker threads publishing
on the connection, so nothing it could wait on is held by a worker.

### Processing deadline (`processing_timeout`)

A thread cannot be killed, so the sync deadline works by keeping the I/O
thread free and letting it settle on the handler's behalf:

- With `ConsumerConfig.processing_timeout` set, handlers never run inline:
  `worker_count>1` uses the pool as usual, and `worker_count=1` gets a
  one-thread `SyncWorkerPool`, so deliveries keep their order and run one at
  a time, exactly like the inline path, but off the I/O thread. That also
  means heartbeats keep flowing during a long handler and the M5 limit below
  does not apply.
- The deadline starts when a worker starts the handler. The I/O thread
  checks deadlines on every consume-loop tick (`process_data_events` waits at
  most `min(1 s, processing_timeout / 4)` while a deadline is set) and during
  the `stop()` drain. An expired delivery is settled there, through the
  pipeline's failure path.
- Settlement is claimed atomically per delivery (a lock in a
  `DeliveryClaim`): the first of the handler and the deadline to settle owns
  it; the other's ack/nack/reject never reaches the channel (on the handler
  thread it raises an internal `BaseException`, `LateSettlementError`, which
  the broker catches around the handler run). A late handler's internal
  publishes (result, retry and DLQ copies) are refused through a
  thread-local check.
- The timed-out handler thread keeps running and keeps its worker until it
  returns, so concurrency never exceeds `worker_count`; a handler that never
  returns holds its worker forever.
- The cost: handlers no longer run on the thread that called `start()`.
  Thread-local state set up there, and owner-thread-only calls such as
  `broker.request()`, are not available to them, as with any worker pool.

### Broker-cancelled consumers

pika dispatches a server `basic.cancel` to the consumer channel's cancel
callback inside `process_data_events`, i.e. on the owner thread. The consume
loop then probes the queue (a passive declare on a throwaway channel) and
re-subscribes with the same tag, also on the owner thread, after every tick
whose backoff has elapsed. Nothing reconnects from there: a dead connection
is left to `run()`'s recovery loop.

### Startup/shutdown hooks

With a timeout, each hook runs on a **fresh single-worker executor**.
Per-hook isolation is deliberate: Python cannot kill a thread, so a hook
that hangs past its timeout occupies its worker forever — a shared pool
would make every subsequent hook (including SIGTERM shutdown hooks) time
out spuriously. The hung worker lingers until process exit; the timeout
bounds the *caller*, not the hook.

### Single-worker confirm waits are unbounded (M5)

With `worker_count=1` (and no `processing_timeout`), a handler publishing
with confirms runs ON the connection's owner thread — `confirm_timeout` cannot be enforced there
(pika's `BlockingChannel` offers no owner-thread way to bound the confirm
wait), so a broker that never confirms wedges the consumer with a live
heartbeat. A startup `RuntimeWarning` calls this out. With
`worker_count > 1`, handler publishes marshal through the bounded
cross-thread path instead — but note each marshaled publish executes inside
`process_data_events` on the I/O thread, so a slow confirm delays delivery
and settlement for ALL queues for up to one confirm round-trip.

### `SyncBatchPublisher`

The exception to "sync = single-threaded": it owns a dedicated
`SelectConnection` I/O thread and its `publish()` is **explicitly
thread-safe** — N caller threads may publish concurrently; confirms are
serviced on the internal thread and each caller blocks only on its own
outcome.

## Async side (aio-pika)

**Everything belongs to one event loop.** The broker, its channels, its
tasks, and every public coroutine must be awaited on the loop where
`await broker.start()` ran. There is no cross-loop or cross-thread
support. From another thread, hand work to the broker's loop explicitly:

```python
asyncio.run_coroutine_threadsafe(broker.publish(routing_key="q", body=b"x"), loop)
```

Two places work intentionally leaves the loop:

- **Large-body decode**: bodies ≥ 256 KiB are deserialized in
  `asyncio.to_thread` so a multi-MB JSON/Pydantic/msgspec parse doesn't
  stall the loop. Below that, inline decode is faster than the hop.
- **`AsyncWorkerPool`** is tasks, not threads — it bounds *concurrency*,
  not CPU parallelism. CPU-heavy handlers need `to_thread`/process pools
  of your own.

**Why there's no `asyncio.Lock` around reconnection either.**
`AsyncConnectionPool` does hold an `asyncio.Lock`, but it scopes to
guarding *first-time* connection creation only (so two concurrent
callers racing to establish the initial connection don't each create
one). Ongoing reconnection — after that first connect — runs inside
aio-pika's own `connect_robust()` task, one per connection; rabbitkit only
sets its policy from aio-pika's close hook (backoff delay, budget, the
next endpoint and fresh credentials, giving up on a permanent failure; see
`async_/reconnect.py`). A rabbitkit-level lock around that would serialize
nothing real and just add latency for no safety gain.

## Health checks and probes

Liveness/readiness functions (`broker_liveness`,
`broker_health_check`) are designed to be called from **any thread** —
a K8s probe served by an HTTP server thread is the expected caller.
They only *read* broker attributes.

`HealthWatcher` polls from its own daemon thread; `AsyncHealthWatcher`
is an asyncio task on the broker's loop and awaits its checks so it
never blocks the loop.

## Benign races (deliberate, not bugs)

- **`last_heartbeat`**: written by the I/O thread / event loop (a blind
  `time.monotonic()` store of an immutable float — single-op atomic on
  every supported runtime, including free-threaded CPython), read by
  probe threads. No read-modify-write exists, so no update can be lost;
  the consumer is a staleness check with a tolerance of tens of seconds
  against a race window of nanoseconds. A lock here would put hot-path
  cost on the one thread whose responsiveness the heartbeat exists to
  prove.
- **`_in_flight`** is mutated only under its `Condition` (thread lock on
  the sync side, `asyncio.Condition` on the async side) — reads for
  health reporting may be slightly stale, which is fine for a gauge.

## Rules of thumb

1. One broker, one thread (sync) or one loop (async). Want to publish
   from many threads? `SyncBatchPublisher`, or one broker per thread.
2. Never share a broker between an asyncio loop and threads without
   `run_coroutine_threadsafe`.
3. Settlement is always safe from the handler's own context — and only
   from there.
4. If a component needs to be called from anywhere, its docstring says
   so explicitly (`SyncBatchPublisher.publish`, the health probes).
   Absence of that sentence means the owner-thread/loop rule applies.
