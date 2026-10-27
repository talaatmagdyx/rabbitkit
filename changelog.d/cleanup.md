<!-- Cleanup stream (rk2/cleanup, wave 3). Merge into CHANGELOG.md [Unreleased]. README change below. -->

### Changed (behaviour)

- **SyncBroker publish outcomes follow the async contract, and nothing is
  republished once frames may have left.** The sync transport used to
  reconnect and publish once more after any connection-class error, even
  when the first copy had reached the broker (a duplicate reported
  `CONFIRMED`); from inside a handler it did so on the I/O thread, leaving
  the consume loop on a new connection without its consumers (reproduced:
  no redelivery afterwards). Now, on `SyncBroker` / `SyncTransport.publish`:
  - `NOT_SENT`: no connection could be (re)established, or the publishing
    thread may not reconnect (a worker thread, or a handler running inside
    the consume loop) (`not_connected`; this used to raise the reconnect
    error out of `publish()` after the bounded reconnect loop); the channel
    was closed before `basic_publish` or confirm mode could not be enabled
    (`channel_unavailable`); a worker-thread publish the I/O loop never
    picked up within `confirm_timeout` (`admission_timeout`; was `TIMEOUT`).
  - `UNKNOWN`: connection (`connection_lost`) or channel (`channel_closed`)
    lost after `basic_publish` (was `ERROR`). Republish with the same
    `message_id` if needed.
  - `ERROR` with `reason`, `reply_code`, `reply_text`: `exchange_not_found`
    (404), `access_refused` (403), `precondition_failed` (406), sent once
    (a 404 used to be published twice); `invalid_envelope`; `publish_error`
    for anything unclassified. `RETURNED` carries `312 NO_ROUTE`, `NACKED`
    and `TIMEOUT` (`confirm_timeout`) carry their reasons.
  - A publish-only connection the broker closed while idle is detected
    before sending (a zero-time read of the socket) and replaced, so the
    first publish after a quiet period is still `CONFIRMED` (M4) without a
    republish.
- `RPCClient.call()` / `AsyncRPCClient.call()` raise `PublishError` at once
  when the request was definitely not delivered (`NOT_SENT`, `RETURNED`,
  `NACKED`, `ERROR`) instead of waiting for the reply timeout
  (`RPCTimeoutError`). `UNKNOWN` / `TIMEOUT` still wait for the reply.
- **The CRITICAL profile covers the connection.** `validate_profile` /
  `preflight` report `connection.heartbeat <= 0` as an error and
  `apply_profile` / `critical_config` raise `ConfigValidationError` for
  `heartbeat=0`. A `reconnect_budget_seconds` below `reconnect_backoff_max`
  and enabled TLS with `cert_reqs="CERT_NONE"` are warnings. The reconnect
  budget stays unlimited by default and TLS is not required (rationale in
  `docs/reliable-messaging.md`).
- **The CRITICAL profile turns on `SafetyConfig.dead_letter_with_metadata`**
  (see Added) and `validate_profile` reports it as an error when off: a
  hand-built CRITICAL config that was compliant reports one more violation.
  Under CRITICAL a delivery over `max_body_bytes` therefore reaches the DLQ
  as a metadata-stamped copy (`x-rabbitkit-error-category: invalid_message`,
  no `x-death`), superseding changelog.d/profile.md's "The DLQ copy carries
  `x-death` only" for that profile.
- `rabbitkit_messages_dead_lettered_total` counts completed dead-letters
  only: a confirmed DLQ copy, or the dead-lettering reject the pipeline
  actually sent. It used to be recorded when `RetryMiddleware` handed a
  terminal failure back, so a delivery requeued because its DLQ was missing
  was counted on every redelivery.

### Added

- `async with AsyncBroker(config) as broker:` starts the broker with
  `install_signal_handlers=False` (the surrounding code owns SIGINT/SIGTERM,
  as with `rabbitkit_lifespan`) and stops it when the block exits, also on
  an exception or cancellation. Standalone processes keep using
  `await broker.run()`.
- `SafetyConfig.dead_letter_with_metadata` (G-17; default `False`, async
  broker only; `SyncBroker` logs that it ignores it): failures that never
  reach `RetryMiddleware` (an `on_receive` failure such as a bad gzip body
  or signature, a body over `ConsumerConfig.max_body_bytes`, a failure on a
  route without retry, a transient failure on redelivery) get a confirmed,
  `mandatory` DLQ copy with the same failure metadata as a retry terminal
  copy (wire body and encoding, every property and header, stable
  `message_id`, `x-rabbitkit-error-category` / `-error-class` /
  `-error-message` (sanitized) / `-failure-category` / `-attempts` /
  `-first-failed-at` / `-last-failed-at` / `-original-queue` /
  `-original-exchange` / `-original-routing-key` / `-original-message-id` /
  `-dead-lettered-at` and the retry count) before the delivery is acked. If
  the copy is not confirmed, the existing path runs (reject into the DLX
  after the DLQ check, reject for a quorum at-least-once source, requeue
  with backoff for a missing DLQ): a delivery is never acked without a
  confirmed copy. Only a DLQ reached through the default exchange (the retry
  DLQ or the `auto_provision` safety DLQ) is addressed. Such a DLQ entry has
  no `x-death`. Handler-requested and `filter_fn` rejects are not copied.
- `rabbitkit.core.failure_copy` (the retry/DLQ copy builder, shared by
  `RetryMiddleware` and the new `rabbitkit.core.dead_letter.DeadLetterCopier`).

### Fixed

- **A broker refusal of a sync publish replaced the whole connection.**
  After a 404/403/406 closed the publisher channel, the next publish built a
  new connection and left the old one open with its consumer channels while
  the consume loop moved on. The publisher channel is now reopened on the
  same connection (marshalled to the I/O thread for worker threads).
- `AsyncTransportImpl` recorded a binding again on every `bind_queue` /
  `bind_exchange` call, so the post-reconnect re-apply list grew without
  bound; each binding is recorded once.
- Flaky real-broker tests: `test_reconnect_during_drain_does_not_resubscribe_a_cancelled_consumer`
  (and `test_prefetch_is_reapplied_after_reconnect`) failed about 3 runs in
  10 with "connections were never seen dropping". Not a product bug: a
  connection killed through the management API is restored within 2-5 ms
  (the first reconnect attempt after a loss is immediate), often before the
  killing HTTP call returned, so polling `is_connected()` every 20 ms after
  the kill missed it. The tests now count `on_reconnect` callbacks
  registered before the kill; `test_is_connected_is_false_while_reconnecting`
  watches from before the kill. 10/10 consecutive passes.

### Documentation

- `docs/reliable-messaging.md`: the SyncBroker publish caveat is replaced by
  its (now matching) outcome list; CRITICAL connection rows and their
  rationale; `dead_letter_with_metadata` in the settlement table and the
  CRITICAL table; what `messages_dead_lettered_total` counts; `async with`.
- `docs/retry-and-dlq.md` ("Failures that never reach RetryMiddleware"),
  `docs/message-safety.md` (sync no-republish, idle-connection row, G-17
  rows), `docs/production/scale.md`, `docs/production/patterns.md` (idle
  sync publish wording).
- Documentation drift (G-28) fixed in `docs/guide/full-guide.md`,
  `docs/guide/getting-started.md`, `docs/guide/overview.md`, `docs/index.md`,
  `docs/ordering-guarantees.md`, `docs/retry-and-dlq.md`,
  `docs/message-safety.md`, `docs/security.md`, `docs/production/patterns.md`,
  `docs/production/checklist.md` (log redaction is opt-in via
  `RabbitConfig.logging`, not on by default), `docs/bulk-operations.md`,
  `docs/rabbitmq-retry-architecture.md`, and the `rabbitkit.streams`
  docstring (no `subscriber(stream_config=)`; streams stay experimental).

## README

In "## Observability", the first sentence ends "... error type) with
secret redaction on by default." Replace "with secret redaction on by
default." with "and redact secrets once logging is configured
(`RabbitConfig(logging=LoggingConfig())`; redaction is opt-in)."

`PRODUCTION_AUDIT.md` (untracked since bd8c339, local-only in the main
checkout) also claims "structlog redaction on by default" (two places); it is
not on this branch and was not edited.
