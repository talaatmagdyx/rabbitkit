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
| Sync handlers (`worker_count>1`) | `SyncWorkerPool` daemon threads | n/a |
| `message.ack()/nack()/reject()` (sync) | any handler thread — marshalled to the I/O thread internally | the thread running that handler |
| `SyncBroker.pump_idle()` | — | owner thread only |
| `broker.publish()` (sync) | — | owner thread only |
| `SyncBatchPublisher.publish()` | dedicated `SelectConnection` I/O thread inside | **any thread** (this is its purpose) |
| Startup/shutdown hooks (sync, with timeout) | a fresh single-worker executor thread per hook | n/a |
| `AsyncBroker` — everything | one asyncio event loop | that loop only |
| Async handlers | tasks on the broker's loop | n/a |
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

### Startup/shutdown hooks

With a timeout, each hook runs on a **fresh single-worker executor**.
Per-hook isolation is deliberate: Python cannot kill a thread, so a hook
that hangs past its timeout occupies its worker forever — a shared pool
would make every subsequent hook (including SIGTERM shutdown hooks) time
out spuriously. The hung worker lingers until process exit; the timeout
bounds the *caller*, not the hook.

### Single-worker confirm waits are unbounded (M5)

With `worker_count=1`, a handler publishing with confirms runs ON the
connection's owner thread — `confirm_timeout` cannot be enforced there
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
one). Ongoing reconnection — after that first connect — is owned
entirely by aio-pika's own `connect_robust()` machinery internally; a
rabbitkit-level lock wrapped around a process rabbitkit doesn't control
would serialize nothing real and just add latency for no safety gain.

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
