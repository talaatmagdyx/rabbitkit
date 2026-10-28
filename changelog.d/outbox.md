### Added

- **Producer outbox (opt-in): a producer crash no longer loses messages that
  were not yet confirmed.** Closes the 0.20.0 known limit. `AsyncOutbox` /
  `SyncOutbox` (`rabbitkit.outbox`) over one transport-free
  `OutboxStore` (`rabbitkit.core.outbox`, stdlib `sqlite3`, no new
  dependency):
  - `enqueue(envelope)` commits the whole `MessageEnvelope` (body, every
    property and header with its type, `mandatory`, a `message_id` fixed at
    enqueue time) to a SQLite file in WAL mode with `synchronous=FULL` and
    returns after the commit. `enqueue_many()` writes a batch in one
    transaction.
  - Bounded: `OutboxConfig.max_entries` / `max_bytes` (pending and failed
    together, kept exact by triggers); beyond them `OutboxFullError`
    (`limit="max_entries"` / `"max_bytes"`) and nothing is written.
  - The relay publishes due entries in enqueue order through the broker's
    normal `publish()` (confirms, `mandatory`, publish middlewares apply),
    `max_in_flight` per pass, and records each pass in one transaction. An
    entry is deleted only after `CONFIRMED`. `NOT_SENT` / `UNKNOWN` /
    `TIMEOUT` (and `BrokerNotStartedError`) leave it pending and back the
    relay off (capped exponential, jitter), without limit. `RETURNED` /
    `NACKED` / `ERROR` / a publish exception: per-entry backoff, then the
    failed state after `max_refusals`. `SENT` and `MessageTooLargeError`:
    failed at once. Failed entries are listed (`list_entries`), replayed
    (`replay_failed`) or purged (`purge_failed`, the only way to discard
    one), never dropped.
  - Crash recovery: no in-flight state is stored, so a new relay on the same
    file republishes everything not yet recorded as confirmed: at-least-once,
    at most `max_in_flight` duplicates per relay crash, same `message_id`.
  - `AsyncOutbox`: `start()`, `stop(timeout)` (drains until nothing is due or
    the timeout, then cancels; nothing is lost), every SQLite call in
    `asyncio.to_thread`. `SyncOutbox`: no thread of its own; `relay_once()`,
    `drain(timeout)`, `run()` / `request_stop()` on the broker's owner
    thread, `enqueue()` from any thread.
  - Transactional enqueue: `store.enqueue_in(conn, envelope)` inside the
    application's own SQLite transaction (`store.connect()` returns a
    correctly configured connection).
  - Both relays require publisher confirms (`ConfigValidationError`
    otherwise).
  - Metrics (bounded labels, in `metric_inventory()`):
    `rabbitkit_outbox_pending`, `_failed_entries`, `_bytes` (gauges),
    `_enqueued_total`, `_relayed_total`,
    `_publish_retries_total{status,reason}`, `_failed_total{status,reason}`,
    `_full_total{reason}`; logs on the `rabbitkit.outbox` logger with
    `outbox_id` / `message_id` / `publish_status` / `publish_reason`.
  - New public names: `AsyncOutbox`, `SyncOutbox`, `OutboxStore`,
    `OutboxConfig`, `OutboxEntry`, `OutboxEntryState`, `OutboxStats`,
    `OutboxPassReport`, `OutboxError`, `OutboxFullError`,
    `OutboxClosedError`; `MetricsConfig.outbox_*` names.
  - Real-broker tests (`tests/integration/test_outbox_foundation.py`): a
    relay process SIGKILLed mid-flight and restarted on the same file
    delivers all 1,500 messages, duplicates within `max_in_flight`; the same
    for a sync relay; entries stay pending through `rabbitmqctl stop_app`
    and are delivered after `start_app`; an unroutable mandatory message
    fails, is kept, and is delivered after a replay.

### Documentation

- New `docs/outbox.md` (design, guarantees and what they do not cover,
  async/sync/dedicated-thread usage, transactional enqueue, operations,
  configuration) and `docs/api/outbox.md`.
- `docs/reliable-messaging.md`: the producer-side loss window and the
  durability scope now point to the outbox; `docs/observability.md` lists
  the outbox metrics; `docs/bulk-operations.md` links the built-in outbox
  from the outbox recipe; README section.
