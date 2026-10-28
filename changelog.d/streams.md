### Added

- **RabbitMQ streams are a supported feature on both brokers** (closes the
  0.20.0 known limit "Streams remain experimental"). Consumed over AMQP
  0.9.1; the `rabbitmq_stream` plugin is not needed. See `docs/streams.md`.
  - `subscriber(..., stream_offset=...)` on a `QueueType.STREAM` queue:
    `"first"`, `"last"`, `"next"` (default), an int offset or a `datetime`;
    or `stream=StreamConsumerConfig(offset=..., offset_store=...,
    commit_every=100, commit_interval=5.0, consumer_name=...)` for the full
    configuration. On `AsyncBroker`, `SyncBroker`, `RabbitRouter`,
    `TestBroker`.
  - `RabbitMessage.stream_offset`: each delivery's offset (from the
    broker-set `x-stream-offset` header).
  - Client-side offset tracking (AMQP 0.9.1 has no server-side offset
    storage): the processed offset advances over the contiguous prefix of
    settled deliveries (out-of-order handlers never move it past an
    unfinished message) and is committed to an `OffsetStore` every
    `commit_every` messages / `commit_interval` seconds and at `stop()`. A
    restart resumes at the committed offset + 1 (a crash replays at most the
    messages processed since the last commit: at-least-once).
  - Offset stores: `OffsetStore` protocol (`load` / `save`),
    `InMemoryOffsetStore` (default; process lifetime) and `SQLiteOffsetStore`
    (stdlib `sqlite3`, WAL, `synchronous=FULL`; durable across crashes).
    `AsyncBroker` commits to a non-memory store in a worker thread.
  - Reconnect: the consume arguments are updated in place as messages are
    processed, so aio-pika's robust restore, the consumer supervisor's
    re-subscribe after a broker cancel, and `SyncBroker.run()`'s recovery all
    resume at the processed offset + 1, never at `"next"` (which skipped
    everything published during the outage) or at `"first"` again. Before the
    first delivery a `"next"`/`"last"` consumer re-subscribes from its
    subscription time minus 5 s.
  - `RabbitQueue` stream retention options: `max_age` (`x-max-age`, format
    validated) and `stream_max_segment_size_bytes`
    (`x-stream-max-segment-size-bytes`), with `max_length_bytes`.
  - Exported from `rabbitkit` and `rabbitkit.streams`: `StreamOffset`,
    `StreamOffsetType`, `StreamConsumerConfig`, `OffsetStore`,
    `InMemoryOffsetStore`, `SQLiteOffsetStore` (Advanced Stable tier).
  - `consume(..., arguments=...)` on both transports (and the transport
    protocols): `basic.consume` arguments; the brokers pass it only for
    stream routes.
- Real-broker tests `tests/integration/test_streams_foundation.py` (async and
  sync, RabbitMQ 3.13 and 4.1): "first" sees every message in order with its
  offset (a publisher-set `x-stream-offset` header is overwritten by the
  broker); "next" sees only new messages; a timestamp start; stop after K and
  restart with `SQLiteOffsetStore` resumes at K with no gap or duplicate; a
  crash replays exactly the uncommitted messages (at most `commit_every`); a
  connection kill mid-stream resumes without skipping and not from "first";
  a reconnect before the first delivery does not skip; a failed message is
  skipped, not redelivered; retry / ACK_FIRST are refused at registration.

### Changed (behaviour)

- **Stream routes refuse retry and `ACK_FIRST` at registration**
  (`ConfigurationError`), including a `RetryConfig` inherited from the
  broker (pass `retry=RETRY_DISABLED` on the stream route). RabbitMQ neither
  redelivers nor dead-letters a nacked or rejected stream message, so retry
  could not work; before this change such a route failed at startup instead,
  when the retry topology declared `x-dead-letter-exchange` on the stream
  (406).
- **Stream routes get no safety DLQ.** `SafetyConfig.reject_without_dlx`
  (default `auto_provision`) no longer adds `x-dead-letter-exchange` to a
  stream or declares `<stream>.dlq`; RabbitMQ refused the argument (406), so
  every stream route under the default safety config failed at startup. A
  rejected stream message is not discarded, it stays in the stream.
- **A handler failure on a stream route is skipped**, logged as a WARNING
  with its offset (`Stream route ...: the message at offset N failed and is
  not retried`), and counted; the consumer moves on. This is what RabbitMQ
  already did with the nack/reject; it is now explicit.
- **A stream route resumes after a reconnect** from the last processed
  offset instead of the broker default `"next"`, which skipped what was
  published during the outage.
- `RabbitQueue(queue_type=STREAM)` now raises `TopologyValidationError` for
  `dead_letter_exchange`, `dead_letter_routing_key`, `max_length`,
  `overflow`, `expires`, `delivery_limit` and `single_active_consumer`, and
  for a non-positive `max_length_bytes`. RabbitMQ 3.13 and 4.1 refuse each
  on a stream with 406 `invalid arg`, so these declarations already failed,
  at startup.
### Fixed

- `StreamOffset.timestamp(dt).to_consume_arguments()` returns the
  `datetime` (sent as an AMQP timestamp) instead of `int(dt.timestamp())`.
  RabbitMQ reads an integer `x-stream-offset` as a numeric offset, so the
  old value started from the wrong place (verified: it delivered nothing). A
  naive `datetime` is taken as UTC. `StreamOffset(...)` now validates its
  value at construction (an `OFFSET` needs a non-negative int, a `TIMESTAMP`
  a datetime, the relative types no value).

### Deprecated

- `from rabbitkit.experimental import StreamOffset, StreamOffsetType,
  StreamConsumerConfig` still works but emits a `DeprecationWarning`; import
  them from `rabbitkit` (or `rabbitkit.streams`). The aliases are removed no
  earlier than the next minor release. They are no longer in
  `rabbitkit.experimental.__all__`.

### Documentation

- New `docs/streams.md` (in the nav): when to choose a stream over a quorum
  queue, declaring and retention, start offsets, offset tracking and stores,
  the at-least-once replay window, reconnect behaviour, settlement and
  failure semantics, limits (no single active consumer or server-side
  offsets over AMQP 0.9.1, no filtering, CRITICAL requires quorum).
- `docs/reliable-messaging.md`: a "Streams" section with the stream delivery
  semantics.
- `docs/stability-policy.md`: streams moved from Experimental to Advanced
  Stable. `docs/api/streams.md`, the Full Guide's stream section and the
  README updated.
