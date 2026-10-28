# Streams

A RabbitMQ stream is an append-only log. Consuming does not remove
messages: every consumer reads the log from a position (an *offset*) it
chooses, any number of times, and messages leave only when retention
removes them. rabbitkit consumes streams over AMQP 0.9.1 through ordinary
routes, on `AsyncBroker` and `SyncBroker`.

```python
from rabbitkit import AsyncBroker, QueueType, RabbitMessage, RabbitQueue, StreamConsumerConfig

broker = AsyncBroker()
offsets = BillingOffsets(db)  # your OffsetStore over your own storage (below)

events = RabbitQueue(
    "events",
    queue_type=QueueType.STREAM,
    max_age="7D",                     # retention: drop segments older than 7 days
    max_length_bytes=20_000_000_000,  # ... or once the stream exceeds 20 GB
)


@broker.subscriber(events, stream=StreamConsumerConfig(offset="first", offset_store=offsets))
async def project(body: dict, message: RabbitMessage) -> None:
    print(message.stream_offset, body)
```

The first run reads from the oldest retained message. Each processed offset
is committed to your offset store; a restart resumes right after the last
committed one, and a reconnect resumes right after the last processed one.

The `rabbitmq_stream` plugin is **not** needed: AMQP 0.9.1 consumers read
streams with the default plugins (verified on RabbitMQ 3.13 and 4.1).

## Streams or quorum queues?

| You need | Use |
|---|---|
| Work distribution: each message handled once by one of N workers, then gone | Quorum queue |
| Retries with delays, a DLQ, per-message redelivery | Quorum queue (streams have none of these) |
| Several independent consumers each reading **every** message (fan-out without one queue per consumer) | Stream |
| Replay: re-read history after a bug fix, rebuild a projection, backfill a new service | Stream |
| A large backlog kept for hours or days, read at the consumer's own pace | Stream |
| Strict per-message ordering for one consumer | Either; a stream with one consumer per route keeps log order |

Both are replicated and durable. A stream is a log you read; a queue is a
work list you drain.

## Declaring a stream

`RabbitQueue(queue_type=QueueType.STREAM, ...)` with the retention options:

| Option | Argument | Meaning |
|---|---|---|
| `max_age` | `x-max-age` | Drop segments older than this: a positive integer and a unit, `Y`, `M`, `D`, `h`, `m` or `s` (`"7D"`, `"12h"`) |
| `max_length_bytes` | `x-max-length-bytes` | Drop the oldest segments once the stream is larger than this |
| `stream_max_segment_size_bytes` | `x-stream-max-segment-size-bytes` | Segment file size (broker default 500 MB). Retention deletes whole segments, so smaller segments make it finer-grained |

Without retention a stream grows until the disk fills. Set at least one of
`max_age` / `max_length_bytes`.

Refused at construction (`TopologyValidationError`), because RabbitMQ
refuses them on a stream (406 `invalid arg`): `durable=False`, `exclusive`,
`max_priority`, `message_ttl`, `consumer_timeout`, `lazy`, `max_length`
(use `max_length_bytes`), `overflow`, `expires`, `delivery_limit`,
`single_active_consumer`, `dead_letter_exchange` and
`dead_letter_routing_key`. `max_age` and `stream_max_segment_size_bytes`
are refused on classic and quorum queues.

Under `TopologyMode.PASSIVE_ONLY` keep `queue_type=QueueType.STREAM` on the
route's queue anyway: that is how rabbitkit knows to consume it as a stream.

## Where a consumer starts

`subscriber(stream_offset=...)`, or `StreamConsumerConfig(offset=...)`:

| Value | Starts at |
|---|---|
| `"next"` (default) | Messages published after the consumer subscribes |
| `"first"` | The oldest message still retained |
| `"last"` | The last chunk written (the most recent messages) |
| `42` (an `int`) | Offset 42 (or the oldest retained, if 42 was already removed) |
| a `datetime` | The first chunk published at or after that time. A naive datetime is UTC. Chunk granularity: a few messages published just before it can be delivered too |

`StreamOffset.first()` / `.last()` / `.next()` / `.offset(n)` /
`.timestamp(dt)` build the same values explicitly.

The start position applies only when the offset store has nothing for this
consumer. Once an offset is stored, the consumer resumes at `stored + 1`
whatever `offset` says. To replay from the start, delete the stored offset
for that key in your store, or use a new `consumer_name`.

`message.stream_offset` is each delivery's offset, from the
`x-stream-offset` header RabbitMQ sets on every stream delivery (a
publisher-supplied header of that name is overwritten by the broker). It is
`None` for messages that did not come from a stream.

## Offsets: tracking, committing, resuming

AMQP 0.9.1 has no server-side offset storage (that is a stream-protocol
feature), so rabbitkit tracks the position on the client:

1. Every delivery's offset is recorded as it arrives.
2. The **processed** offset advances once a delivery and every delivery
   before it are settled. Handlers that finish out of order (a worker pool,
   concurrent async handlers) never move it past a message still in
   progress.
3. The processed offset is **committed** to the `OffsetStore` every
   `commit_every` messages (default 100) or after `commit_interval` seconds
   (default 5, checked when a message is settled), and once more by
   `stop()`.

| `StreamConsumerConfig` field | Default | |
|---|---|---|
| `offset` | `"next"` | Start position when nothing is stored |
| `consumer_name` | queue name | The key the offset is stored under, also sent as `x-stream-consumer-name`. Set it when two routes or services share a store and a stream |
| `offset_store` | `InMemoryOffsetStore()` | Where offsets are committed |
| `commit_every` | `100` | Commit after this many newly processed messages |
| `commit_interval` | `5.0` | Also commit after this many seconds; `None` for count only |

Stores:

- `InMemoryOffsetStore` (default): survives reconnects, not restarts. A
  restarted process starts at `offset` again.
- Your own, for restarts: rabbitkit is a RabbitMQ library and ships no
  storage. Any thread-safe object with `load(key) -> int | None` and
  `save(key, offset) -> None` (the `OffsetStore` protocol) works. Keep it in
  the storage your service already uses, ideally in the same transaction as
  the handler's writes, so the offset and the effect commit together.

```python
class BillingOffsets:
    """OffsetStore over a table the service already owns."""

    def __init__(self, db) -> None:
        self.db = db

    def load(self, key: str) -> int | None:
        row = self.db.execute("SELECT offset FROM stream_offsets WHERE key = %s", (key,)).fetchone()
        return None if row is None else row[0]

    def save(self, key: str, offset: int) -> None:
        self.db.execute(
            "INSERT INTO stream_offsets (key, offset) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET offset = EXCLUDED.offset",
            (key, offset),
        )
```

`AsyncBroker` runs commits to any store other than `InMemoryOffsetStore` in
a worker thread, so a commit never blocks the event loop; `SyncBroker`
commits on the thread that settled the message. A store that raises is
logged; the next due commit retries.

### At-least-once, and the replay window

Delivery is **at-least-once**:

- **Graceful stop** (`stop()`, `run()` on SIGTERM): the final commit
  records the last processed offset. The next start resumes right after
  it: no duplicate, no gap.
- **Crash** (kill -9, OOM): the next start resumes after the last
  *committed* offset. Messages processed since then are delivered again: at
  most `commit_every` messages, or `commit_interval` seconds' worth.
  Handlers must be idempotent (see
  [Idempotency](reliable-messaging.md#idempotency)); a smaller
  `commit_every` narrows the window at the cost of more store writes.
- **Reconnect** (broker restart, network blip): the consumer re-subscribes
  at the last *processed* offset + 1, never at `"next"` (which would skip
  everything published during the outage) and never at `"first"` again.
  Deliveries that were in flight when the channel died are delivered again
  (at most the route's prefetch).
- **Before the first message is processed**, a `"next"` or `"last"`
  consumer knows no offset. A reconnect in that window re-subscribes from
  the time it first subscribed, minus 5 seconds for clock skew between
  client and broker: nothing published after subscribing is skipped, and a
  few older messages may be delivered. For `"last"`, the chunk that was
  "last" at subscription time is not delivered again if it is older than
  that (the window is short: a non-empty stream delivers it at once).

## Settlement, failures and retry

A stream consumer must use manual acknowledgements and a prefetch; RabbitMQ
refuses a stream consumer without them. rabbitkit always sets both (the
route's `prefetch_count`, else `ConsumerConfig.prefetch_count`).

An ack on a stream only returns prefetch credit: it does not remove the
message. RabbitMQ also ignores the difference between ack, nack and reject:
**a nacked or rejected stream message is not redelivered and not
dead-lettered** (verified: `nack(requeue=True)` and `reject` both move on).
So on a stream route:

| Handler outcome | What happens |
|---|---|
| Returns | Ack; the offset advances |
| Raises (any category, `AUTO` / `NACK_ON_ERROR`), or its result publish is not confirmed | The message is **skipped**: nack/reject as usual, a WARNING `Stream route ...: the message at offset N failed and is not retried`, and the offset advances past it. The message stays in the stream; replay it from that offset if needed |
| `AckPolicy.MANUAL` | The handler settles; the offset advances when it does |
| Cancelled by `stop()`, or handed back by the worker pool at `stop()` | Not counted as processed: the next start delivers it again |

Refused at registration (`ConfigurationError`):

- **Retry / DLQ**, set on the route (`retry=RetryConfig(...)`) or inherited
  from the broker's default `RetryConfig`. Retry is built on nack/reject
  plus a dead-letter exchange, and a stream has neither. Pass
  `retry=RETRY_DISABLED` on stream routes of a broker that has a default
  retry. To retry, do it inside the handler, or publish failed messages to
  a quorum queue that has retry.
- **`AckPolicy.ACK_FIRST`**: it would mark the message processed before the
  handler runs, so a crash would skip it, for no benefit.
- Stream options (`stream=` / `stream_offset=`) on a classic or quorum
  queue.

The safety DLX (`SafetyConfig.reject_without_dlx`) does not apply to stream
routes: a rejected stream message is not discarded, it stays in the log, so
no `<queue>.dlq` is provisioned (RabbitMQ would refuse the
`x-dead-letter-exchange` argument anyway).

## Limits

- **Single active consumer** and **server-side offset tracking** are
  stream-protocol features, not available over AMQP 0.9.1. Several
  instances of the same route each read the whole stream; to split work
  across instances, use a quorum queue (or partition across several
  streams).
- **Stream filtering** (`x-stream-filter`) and super streams are not
  supported.
- Settlement that bypasses `RabbitMessage` (a raw channel ack, the
  `CoalescingAcker`) is not seen by the tracker: the offset does not advance
  and those messages are replayed on the next start (duplicates, never
  loss).
- The CRITICAL reliability profile requires quorum queues and reports a
  stream route as a violation.
- `TestBroker` deliveries carry no stream offset: handlers run, offsets are
  not tracked.

## API

See [API: Streams](api/streams.md): `StreamOffset`, `StreamOffsetType`,
`StreamConsumerConfig`, `OffsetStore`, `InMemoryOffsetStore`. All are exported from `rabbitkit` and
`rabbitkit.streams`. Importing `StreamOffset`, `StreamOffsetType` or
`StreamConsumerConfig` from `rabbitkit.experimental` still works but emits a
`DeprecationWarning`.
