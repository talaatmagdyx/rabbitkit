# Retry / DLQ stream (rk2/retry) — changelog entries for [Unreleased]

Merge these into `CHANGELOG.md` under `[Unreleased]`. No README changes are
needed (the README has no failure-taxonomy or replay-authorization section;
`docs/retry-and-dlq.md` carries the details).

### Added

- **Failure taxonomy** (`rabbitkit.ErrorCategory`): `RETRYABLE`,
  `PERMANENT`, `INVALID_MESSAGE`, `UNKNOWN_OUTCOME`, `INFRASTRUCTURE`,
  `SHUTDOWN`, each with the `ErrorSeverity` it implies
  (`ErrorCategory.severity`). `ClassifiedError` gains a `category` field;
  when omitted (or contradicting `severity`) it is derived from `severity`,
  so existing code that builds a `ClassifiedError` from a severity alone
  stays consistent. `classify_error` evaluates: shutdown signals
  (`CancelledError`, `KeyboardInterrupt`, `SystemExit` → `SHUTDOWN`), then a
  category the exception declares itself (`rabbitkit_error_category` on the
  class or instance), then predicates (which may now return an
  `ErrorCategory` as well as `True`/`False`/`None`), then registered
  infrastructure errors, the transient and permanent tuples, and
  `unknown_policy`. The unclassified default stays `PERMANENT`.
- Typed exceptions `rabbitkit.InvalidMessageError` (a `ValueError`;
  `INVALID_MESSAGE`, never retried) and `rabbitkit.InfrastructureError`
  (`INFRASTRUCTURE`, retried). `MissingDependencyError` declares
  `INVALID_MESSAGE`.
- `rabbitkit.core.errors.register_infrastructure_errors(*types)` /
  `infrastructure_errors()`: the transports register their client's
  connection and channel errors (aiormq `AMQPConnectionError`,
  `ChannelClosed`, `ChannelInvalidStateError`; pika's `StreamLostError`,
  `AMQPConnectionError`, `ConnectionClosedByBroker`, `ChannelWrongStateError`,
  `ChannelClosedByBroker`, `AMQPChannelError`) when a broker is constructed,
  so `core/` stays transport-free. New helpers
  `rabbitkit.async_.connection.get_infrastructure_errors()` and
  `rabbitkit.sync.connection.get_infrastructure_errors()`.
- `RabbitMessage.wire_body` / `wire_content_encoding`: the body and encoding
  exactly as delivered, before any `on_receive` hook rewrote them.
- `RetryConfig.trust_retry_header` (default `True`, the previous behaviour).
  `False` counts attempts only from the broker's `x-death` history, so a
  producer that sets `x-rabbitkit-retry-count` cannot skip retries. The
  header is still written.
- **DLQ authorization.** `DLQInspector(..., authorizer=fn)`: `fn` receives a
  `rabbitkit.ReplayRequest` (operation `replay`/`purge`, actor,
  `actor_explicit`, queue, target, `reset_retry_count`, `max_replays`,
  `limit`, predicate) before every replay and purge; `False` or an
  exception raises the new `rabbitkit.DLQOperationDeniedError` (a
  `PermissionError`) before anything is read, and writes a `DENIED` record
  to the `rabbitkit.dlq.audit` logger. `purge` / `purge_async` take an
  optional `actor` and are audited. CLI: `rabbitkit dlq replay
  --require-actor` (env `RABBITKIT_REPLAY_REQUIRE_ACTOR`) refuses (exit 2,
  audited) a replay without `--actor`.

### Changed (behaviour)

- **`x-rabbitkit-error-category` carries the failure category**, not the
  severity: a transient failure now reads `retryable` (was `transient`), and
  the new values `invalid_message`, `unknown_outcome` and `infrastructure`
  appear; `permanent` is unchanged. Update any triage script or alert that
  matched `transient`.
- `rabbitkit_messages_retried_total` and `rabbitkit_messages_dead_lettered_total`
  gain a bounded `category` label (the six values above). Aggregations by
  `queue` are unaffected; a selector on the exact label set needs the new
  label.
- **pika and aiormq connection/channel errors raised inside a handler are
  retried** (`INFRASTRUCTURE`, transient). pika's are not `OSError`
  subclasses and aiormq's `ChannelClosed` / `ChannelInvalidStateError`
  aren't either, so they used to classify `PERMANENT` and go straight to the
  DLQ. A 403/404/406 `ChannelClosed` raised in a handler is retried too,
  bounded by `max_retries`.
- **Body-decode and `on_receive` failures are `INVALID_MESSAGE` and always
  terminal.** The pipeline tags them; a declared category wins over
  predicates and `unknown_policy`, so a decode error is no longer retried by
  a predicate that returns `True` for it, nor under
  `unknown_policy=TRANSIENT` when a custom decoder raises something that is
  not a `ValueError`.
- The pipeline's "Handler failed" log line gains `category=`.
- A quorum source declared with `dead_letter_strategy="at-least-once"` is no
  longer probed for its DLQ before a dead-lettering settlement: the broker
  keeps the message until the DLQ exists, so it is rejected once and the
  handler is not re-run. Other sources keep the requeue-with-backoff.
- `ExceptionMiddleware` re-raises a terminal exception whose DLQ copy was
  attempted and not confirmed (`_rabbitkit_dead_letter_failed`), even with
  `swallow_permanent=True` or a matching fallback handler. Its log lines are
  sanitized (they printed raw exception text).

### Fixed

- **Compression + retry dead-lettered every retried message** (G-07, repro
  S3). `CompressionMiddleware` decompresses `message.body` in place and
  keeps `content_encoding`; `RetryMiddleware` republished that decoded body
  under `content_encoding=gzip`, so attempt 2 failed to decompress and went
  to the DLQ. The terminal DLQ copy had the same defect (undecodable). Retry
  and DLQ copies are now built from `wire_body` / `wire_content_encoding`.
  Verified on a real broker: the compressed message succeeds on attempt 2,
  and its DLQ copy decompresses. `SigningMiddleware` stays incompatible with
  retry (the signature also covers exchange and routing key).
- **A failed DLQ handoff could be acked away.** When `RetryMiddleware`'s
  confirmed DLQ publish failed, an outer `ExceptionMiddleware(swallow_permanent=True)`
  (or a fallback handler) swallowed the exception and the pipeline acked a
  message that existed nowhere else. It is now handed back to the pipeline.
- **A cancelled handler left its dedup claim in flight.**
  `DeduplicationMiddleware(mark_policy="claim")` released the claim on
  `except Exception`, which `CancelledError` skips: after a shutdown or
  `processing_timeout` cancel, the requeued redelivery was treated as an
  in-flight duplicate for up to `processing_timeout` (300 s by default). The
  claim is now released on cancellation too (bounded to 1 s); the
  cancellation still propagates.
- Two real-broker tests that disagreed with the merged behaviour:
  `test_replay_preserves_properties` (now checks the original headers plus
  the replay audit headers) and
  `test_quorum_at_least_once_keeps_message_when_dlq_missing` (now asserts no
  loss for both strategies, the declared strategy, bounded redelivery, and
  that the at-most-once message reaches the restored DLQ with metadata).

### Documentation

- `docs/retry-and-dlq.md`: failure taxonomy (replaces the stale
  `PermanentError` example, which never existed), wire-body copies, the
  composed DLQ-missing behaviour per source type, `trust_retry_header`,
  replay authorization.
