# Profile / envelope stream (rk2/profile) — changelog entries for [Unreleased]

Merge these into `CHANGELOG.md` under `## [Unreleased]`, in the matching
sections. README.md was edited directly on this branch (this stream is the
only one allowed to): reliability-profile paragraph, one "What happens when
things fail?" row, one middleware row, two documentation links.

### Added

- **Event metadata (G-20, ADR-M5), optional and generic.**
  `rabbitkit.EventMetadata` (frozen, `core/event_metadata.py`) maps
  `message_id` (AMQP `message_id`), `idempotency_key` (header
  `idempotency-key`), `event_type` (`type`), `event_version` (header
  `event-version`, int >= 1), `source` (`app_id`), `occurred_at` (header
  `occurred-at`, RFC 3339 UTC), `correlation_id` and optional `tenant_id`
  (header `tenant-id`). Trace context stays with `OTelTracingMiddleware`.
  Construction validates and raises the new `rabbitkit.EventMetadataError`
  (an `InvalidMessageError`, so `INVALID_MESSAGE`; names the bad fields,
  never their values). `EventMetadata.create()` fills in `message_id`,
  `correlation_id` (= `message_id`) and `occurred_at`; `to_envelope()` /
  `apply_to()` build a `MessageEnvelope` (no change to `MessageEnvelope`),
  refuse to overwrite different values and set the AMQP `timestamp` when
  missing; `from_message()` / `from_envelope()` read and validate;
  `check_supported_version()` applies a supported-versions policy.
- `rabbitkit.EventMetadataMiddleware` (opt-in): validates deliveries before
  the handler (missing/invalid metadata or an unsupported `event_version` /
  event type → `EventMetadataError`: dead-lettered, never retried; with
  retry the DLQ copy carries `x-rabbitkit-error-category: invalid_message`),
  and validates envelopes before publish (`validate_publish=False` to skip
  on a route whose results are not events), setting `timestamp` when
  missing. Retry, DLQ and replay copies keep the metadata (tested).
- `ConsumerConfig.max_body_bytes` (§15.6, default `None` = no cap): a
  delivery whose wire body is larger is `INVALID_MESSAGE` and rejected
  without requeue (the source's DLX moves it to the DLQ; the async broker
  checks the DLQ exists first) before the filter, any middleware, decode or
  the handler runs, under every `AckPolicy`. New
  `rabbitkit.InboundMessageTooLargeError`. The DLQ copy carries `x-death`
  only, not rabbitkit's failure headers.
- `PublisherConfig.strip_reserved_headers` (G-19, default `False`):
  `broker.publish()` (sync and async, and `publish_many` / `iter_publish`)
  drops rabbitkit's retry/DLQ/replay bookkeeping headers
  (`x-rabbitkit-retry-count`, `-first-failed-at`, `-original-*`,
  `-error-*`, `-replay-*`, ...), `x-death` and a custom
  `RetryConfig.retry_header` from application publishes, logging one
  WARNING per header name. Retry/DLQ copies and DLQ replays bypass
  `broker.publish()` and keep them. Handler-result publishes are not
  stripped. `rabbitkit.core.reserved_headers` (`RESERVED_HEADERS`,
  `strip_reserved_headers()`, `ReservedHeaderStripper`).
- `rabbitkit.core.profiles.CRITICAL_MAX_IN_FLIGHT_BYTES` (64 MiB).
- `rabbitkit.core.signals` (`is_loop_signal_handler`,
  `is_default_signal_handler`, `loop_signal_handler_is`): the signal
  ownership helpers `AsyncBroker` used privately, now shared with
  `RabbitApp`.

### Changed (behaviour)

- **The CRITICAL reliability profile enforces the reliable-messaging
  contract (ADR-M1/M4).** `apply_profile(..., "critical")` /
  `critical_config()` now also set `publisher.max_in_flight_bytes`
  (64 MiB unless set), `publisher.strip_reserved_headers=True`, and on the
  retry config `delay_queue_dead_letter_strategy="at-least-once"`,
  `terminal_action="dead_letter"`, `trust_retry_header=False` (besides
  quorum delay queues and DLQ, as before). They now **raise
  `ConfigValidationError` when `consumer.processing_timeout` is unset**
  (it has no safe default): `critical_config()` with no base, or a base
  without a deadline, used to succeed and now raises; pass
  `RabbitConfig(consumer=ConsumerConfig(processing_timeout=...))`. New
  pinned contradictions: `terminal_action="reject"`,
  `delay_queue_dead_letter_strategy="at-most-once"`,
  `reject_transient_on_redelivery=False`, `manual_settle_on_error=False`.
  `validate_profile(..., "critical")` / `preflight()` report all of these
  and, per route: `AckPolicy.ACK_FIRST`, `RETRY_DISABLED`, a route
  `RetryConfig` missing the retry requirements, and (warning) a quorum
  source queue not declared `x-dead-letter-strategy=at-least-once`; with a
  management client, `preflight()` verifies that on the broker instead and
  drops the warning. A previously compliant CRITICAL config therefore now
  reports violations until it adopts the new settings. Event-metadata
  validation is not forced. STANDARD is unchanged. Connection
  reconnect/backoff settings are not covered yet (TODO for the connection
  stream).

### Fixed

- **`RabbitApp.run_async()` reset the application's signal handlers
  (G-24).** `loop.remove_signal_handler()` restores `SIG_DFL`, not the
  previous handler, so on exit an application's SIGTERM handler and
  `asyncio.run()`'s SIGINT handler were lost. It now captures and restores
  the previous Python-level handler, leaves a signal alone (WARNING) when an
  asyncio handler is already installed, and does not remove a handler
  installed by someone else while the app ran — the same rules as
  `AsyncBroker`.

### Documentation

- New `docs/reliable-messaging.md` (in the mkdocs nav under Production):
  the contract as implemented — publish outcome table with reasons,
  consumer settlement table, failure taxonomy, retry/DLQ guarantees,
  remaining loss windows, durability scope, idempotency and the
  application-owned durable inbox, ordering, shutdown and signals, and the
  CRITICAL profile's requirement table. Gaps are marked "Not implemented"
  (producer outbox, exhausted handoff → DLQ, connection settings in the
  profile) and the SyncBroker's retry-once-after-dead-connection publish is
  called out.
- New `docs/event-metadata.md` (nav: User Guide); API entries in
  `docs/api/messages.md` and `docs/api/middleware.md`.
- `docs/bulk-operations.md`: CRITICAL table and example
  (`processing_timeout`); `docs/retry-and-dlq.md`: stripping reserved
  headers; `docs/message-safety.md`: two guarantee rows.
- `examples/bulk_operations/05_reliability_profile_preflight.py` passes a
  `processing_timeout` to `critical_config()`.
