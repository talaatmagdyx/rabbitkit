# Producer outbox

`broker.publish()` reports a message as safe only when the broker confirms it.
Until then the message lives in the producer's memory, so a producer process
that crashes, is killed, or loses its pod between "I decided to publish" and
`CONFIRMED` loses it. The optional outbox closes that window: the message is
written to a local SQLite file first, and a relay publishes it and deletes it
only after the broker confirms it.

The outbox is opt-in and has no runtime dependency beyond the standard
library (`sqlite3`). Nothing changes for code that does not use it.

## Design

One durable store, two thin relays.

- **Store** (`rabbitkit.core.outbox.OutboxStore`, transport-free). One SQLite
  file, opened with `journal_mode=WAL` and `synchronous=FULL`. `enqueue()`
  writes the whole `MessageEnvelope` (routing key, exchange, body, every
  property, every header, `mandatory`, `message_id`) in one transaction and
  returns after the commit. The `message_id` is fixed at that moment (a
  missing one gets a uuid4), so every republish of the entry carries the same
  id and consumers can deduplicate. Tables are prefixed `rabbitkit_outbox`,
  so the store can share a file with application tables.
- **Bounds.** `OutboxConfig.max_entries` and `max_bytes` cap what the file
  holds (pending and failed entries both count). An enqueue past either bound
  raises `OutboxFullError` and writes nothing: the outbox never grows without
  limit, and the caller sees backpressure at the line that produced it.
  The counts are kept by SQLite triggers in the same transaction as the
  write, so the check is O(1) and exact.
- **Relay.** Each pass takes up to `max_in_flight` due entries in enqueue
  order, publishes them through the broker's normal `publish()` (confirms,
  `mandatory`, publish middlewares, reserved-header stripping all apply), and
  records every outcome in one transaction:

  | Outcome | Entry |
  |---|---|
  | `CONFIRMED` | deleted |
  | `NOT_SENT`, `UNKNOWN`, `TIMEOUT`, `BrokerNotStartedError` | stays pending, same `message_id`; the whole relay backs off (exponential, capped, with jitter) because these mean the broker is unreachable, not that the message is bad. Retried without limit |
  | `RETURNED`, `NACKED`, `ERROR`, any other exception | stays pending with its own backoff; after `max_refusals` such outcomes it moves to the **failed** state |
  | `SENT` (confirms off), `MessageTooLargeError` | failed at once: retrying cannot make it confirmed |

  Failed entries are kept in the same file, counted (`outbox_failed_entries`
  gauge, `outbox_failed_total` counter), logged at ERROR, and can be listed,
  replayed (moved back to pending) or purged explicitly. Nothing is dropped
  by the library.
- **Crash recovery.** The store has no "in flight" state. An entry is pending
  until its `CONFIRMED` outcome is committed, so a relay that dies mid-pass
  leaves its entries pending and the next relay on the same file publishes
  them again. That is the at-least-once guarantee, and the duplicates it
  allows: at most `max_in_flight` entries per crash (published, not yet
  recorded), plus any entry whose earlier attempt was `UNKNOWN`/`TIMEOUT` but
  did reach the broker.
- **Async** (`AsyncOutbox`): every SQLite call runs in `asyncio.to_thread`,
  so the event loop never blocks on disk. The relay is one task; a pass
  publishes its entries concurrently.
- **Sync** (`SyncOutbox`): no thread of its own. `SyncBroker` may only be
  used by the thread that started it (see
  [Concurrency model](concurrency-model.md)), so the relay runs on the
  thread that calls `relay_once()` / `run()`, which must be the broker's
  owner thread. `enqueue()` is safe from any thread.
- **Transactional enqueue**: `store.enqueue_in(conn, envelope)` writes the
  entry through the application's own `sqlite3` connection, inside the
  application's transaction, so the business row and the outbox row commit
  or roll back together.

What the store does not try to be: a multi-writer queue (run one relay per
file), a cross-host store (keep the file on local disk; SQLite's WAL mode
does not work on network filesystems), or a replacement for consumer
idempotency.

## Guarantees and limits

| Situation | Result |
|---|---|
| `enqueue()` returned | The entry is committed to the SQLite WAL with `synchronous=FULL`: it survives a process crash and, on storage that honours `fsync`, an OS crash or power loss |
| Process crash before `enqueue()` returned | The entry may or may not be stored. Treat the enqueue as not done |
| Relay crash or `kill -9` mid-pass | Nothing lost; up to `max_in_flight` entries published again on restart (duplicates, same `message_id`) |
| Broker down | Entries stay pending; the relay backs off up to `backoff_max` and delivers them once the broker is back |
| Unroutable (`mandatory=True` and no binding), nacked, refused | Retried `max_refusals` times with backoff, then failed: inspectable and replayable, never deleted |
| `stop(timeout)` | Keeps relaying until nothing is due or the timeout expires; whatever is left stays pending in the file |
| Outbox full | `OutboxFullError`; nothing written |

Not covered:

- **Loss or corruption of the disk** holding the file. The outbox moves the
  durability boundary from process memory to local storage; it does not
  replicate. Use a persistent volume when pods move.
- **macOS power loss with the default settings.** macOS `fsync` does not flush
  the drive cache; set `OutboxConfig(full_fsync=True)` (SQLite's
  `fullfsync`) when that matters. It is slower.
- **Storage that lies about `fsync`** (some virtual disks, write caches
  without battery backup).
- **Strict ordering.** Entries are taken in enqueue order, but the async
  relay publishes up to `max_in_flight` of them concurrently, and a refused
  entry (`RETURNED`, `NACKED`, `ERROR`) backs off while later entries
  overtake it. With `max_in_flight=1`, order holds through broker outages
  (the relay retries the same oldest entry) but not past a refusal. Consumer
  side reordering (requeues, retries, several consumers) is described in
  [Ordering guarantees](ordering-guarantees.md).
- **Duplicates.** At-least-once, as above. Consumers deduplicate on
  `message_id` (see [Idempotency](production/idempotency.md)).
- **Messages without `mandatory=True`** that no queue is bound for are
  confirmed and dropped by RabbitMQ, as with a plain publish. Set
  `mandatory=True` on outbox envelopes so an unroutable message is
  `RETURNED` and ends up failed instead.

## Using it (async)

```python
from rabbitkit import AsyncBroker, AsyncOutbox, MessageEnvelope, OutboxConfig, RabbitConfig


async def main(config: RabbitConfig) -> None:
    broker = AsyncBroker(config)
    await broker.start()
    outbox = AsyncOutbox(broker, "/var/lib/orders/outbox.db", OutboxConfig(max_entries=50_000))
    await outbox.start()  # also resumes whatever a crashed process left pending
    try:
        entry_id = await outbox.enqueue(
            MessageEnvelope(
                exchange="orders",
                routing_key="order.created",
                body=b'{"order_id": 42}',
                message_id="order-42-created",  # stable id: consumers deduplicate on it
                mandatory=True,
            )
        )
        # enqueue() returned: the message survives a crash of this process
    finally:
        await outbox.stop(timeout=10)  # drain first ...
        await broker.stop()  # ... then the broker
```

`enqueue()` is the only call on the request path; it never touches the
broker. `OutboxFullError` (an `OutboxError`) is the backpressure signal:
answer 503/429 or slow down.
`enqueue_many()` writes a batch in one transaction (one `fsync`).

The relay starts with `start()` and needs nothing else. It wakes as soon as
`enqueue()` commits; `poll_interval` only matters for entries written
through another connection (transactional enqueue) or whose backoff ends.

## Using it (sync)

`SyncBroker` is bound to the thread that started it, and so is its relay.

A publish-only process can relay on its main thread:

```python
from rabbitkit import MessageEnvelope, RabbitConfig, SyncBroker, SyncOutbox


def run_relay(config: RabbitConfig) -> None:
    broker = SyncBroker(config)
    broker.start()
    outbox = SyncOutbox(broker, "/var/lib/orders/outbox.db")
    try:
        outbox.enqueue(MessageEnvelope(routing_key="orders", body=b"...", mandatory=True))
        outbox.drain(timeout=10)  # or outbox.run() until outbox.request_stop()
    finally:
        outbox.close()
        broker.stop()
```

A process whose main thread runs a consumer (`broker.run()`) gives the relay
its own thread and its own publish-only broker, started on that thread;
handlers and other threads only call `enqueue()`:

```python
import threading

from rabbitkit import RabbitConfig, SyncBroker, SyncOutbox


def start_relay_thread(config: RabbitConfig, path: str) -> tuple[SyncOutbox, threading.Thread]:
    relay_broker = SyncBroker(config)
    outbox = SyncOutbox(relay_broker, path)

    def relay() -> None:
        relay_broker.start()  # this thread owns the relay's connection
        try:
            outbox.run()  # returns after request_stop() and a bounded drain
        finally:
            relay_broker.stop()

    thread = threading.Thread(target=relay, name="outbox-relay", daemon=True)
    thread.start()
    return outbox, thread  # on shutdown: outbox.request_stop(); thread.join()
```

`relay_once()`, `drain()` and `run()` publish one entry at a time (pika's
`BlockingConnection` has one publish in flight) and stop a pass at the first
transient outcome, so an unreachable broker costs one `confirm_timeout`, not
one per entry. `drain(timeout)` checks its deadline between publishes, so it
can overrun by one publish.

## Transactional enqueue

When the business data lives in the same SQLite file, write it and the
outbox entry in one transaction:

```python
from rabbitkit import AsyncOutbox, MessageEnvelope


def place_order(outbox: AsyncOutbox, order_id: int) -> None:
    conn = outbox.store.connect()  # WAL + synchronous=FULL, Python's default transactions
    try:
        with conn:  # one transaction: both rows, or neither
            conn.execute("INSERT INTO orders (id) VALUES (?)", (order_id,))
            outbox.store.enqueue_in(
                conn,
                MessageEnvelope(routing_key="orders", body=b"...", message_id=f"order-{order_id}", mandatory=True),
            )
    finally:
        conn.close()
```

This is blocking SQLite code: from async code run it with
`asyncio.to_thread`. `enqueue_in()` does not commit; the entry exists when your transaction
commits and vanishes if it rolls back. It refuses a connection to another
file or one whose `synchronous` is below `FULL`. The relay picks the entry up
at its next poll; call `outbox.notify()` after the commit (on the outbox's
event loop) to relay it at once. The bound is checked inside your
transaction: over a bound, the outbox row is removed again and
`OutboxFullError` is raised, and your transaction decides what happens to
the rest.

Business data in another database (PostgreSQL, MySQL) cannot share a
transaction with this SQLite file. Keep the outbox table in that database
instead and relay it with `publish_many` and a stable `message_id`; the
recipe is in [Bulk operations](bulk-operations.md#transactional-outbox-and-inbox).

## Operating it

Failed entries never leave the file on their own:

```python
from rabbitkit import AsyncOutbox


async def triage(outbox: AsyncOutbox) -> None:
    for entry in await outbox.list_entries():  # failed, oldest first; page with after_id=
        print(entry.id, entry.envelope.message_id, entry.last_status, entry.last_reason, entry.last_error)
    await outbox.replay_failed()  # all of them (or ids=[...]): back to pending, fresh refusal budget
    # await outbox.purge_failed(ids=[...])  # the only way to discard; logged at WARNING
```

Metrics (through the collector you pass, or the broker's first
`MetricsMiddleware` collector):

| Metric | Type | Meaning |
|---|---|---|
| `rabbitkit_outbox_pending` | gauge | Entries waiting to be published and confirmed |
| `rabbitkit_outbox_failed_entries` | gauge | Entries in the failed state |
| `rabbitkit_outbox_bytes` | gauge | Bytes held, counted against `max_bytes` |
| `rabbitkit_outbox_enqueued_total` | counter | Entries written (a transactional enqueue is counted when written, even if its transaction later rolls back) |
| `rabbitkit_outbox_relayed_total` | counter | Entries confirmed and deleted |
| `rabbitkit_outbox_publish_retries_total{status,reason}` | counter | Publishes that left the entry pending |
| `rabbitkit_outbox_failed_total{status,reason}` | counter | Entries moved to failed |
| `rabbitkit_outbox_full_total{reason}` | counter | Enqueues refused (`max_entries` / `max_bytes`) |

Alert on `outbox_pending` growing for minutes, on `outbox_failed_entries > 0`
and on any `outbox_full_total` increase. Logs go to the `rabbitkit.outbox`
logger: an ERROR per entry moved to failed, a WARNING per refusal that will
be retried and per pass that found the broker unreachable, each with
`outbox_id`, `message_id`, `publish_status`, `publish_reason` as `extra`
fields.

## Configuration

`OutboxConfig` (frozen, validated):

| Field | Default | Meaning |
|---|---|---|
| `max_entries` | 100,000 | Pending plus failed entries the file may hold |
| `max_bytes` | 256 MiB | Body plus encoded properties the file may hold |
| `max_in_flight` | 100 | Entries per relay pass (concurrent on async); also the most duplicates one relay crash can cause |
| `max_refusals` | 5 | `RETURNED` / `NACKED` / `ERROR` / exception outcomes before an entry fails |
| `backoff_initial`, `backoff_max`, `jitter` | 0.5 s, 30 s, 0.2 | Exponential backoff for refused entries and for the relay after a transient outcome |
| `poll_interval` | 1 s | Idle relay poll |
| `publish_timeout` | `None` | `timeout=` for each async publish (`None`: the broker's own bounds) |
| `drain_timeout` | 10 s | Default for `AsyncOutbox.stop()` and the drain at the end of `SyncOutbox.run()` |
| `busy_timeout` | 5 s | SQLite wait for another connection's write lock |
| `full_fsync` | `False` | `F_FULLFSYNC` on macOS (power-loss durability there) |

The outbox requires publisher confirms on the broker it publishes through:
`start()` / the sync relay raise `ConfigValidationError` when
`PublisherConfig.confirm_delivery` is `False`.

Cost: every `enqueue()` call and every relay pass is one SQLite transaction
with an `fsync`. Batch with `enqueue_many()` when you produce many messages
at once.
