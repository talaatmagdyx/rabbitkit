<!-- Transport/publisher stream (rk2/transport). Merge into CHANGELOG.md [Unreleased]. -->

### Changed (behaviour)

- **Publish outcomes say whether the message may have been sent.**
  `PublishStatus` gains `NOT_SENT` (the message definitely never left the
  client) and `UNKNOWN` (its frames may have reached the broker, but no
  confirm, return or nack was seen). On the async broker:
  - `NOT_SENT`: publisher closed, no free channel within
    `PoolConfig.channel_acquire_timeout` (`admission_timeout`), byte budget
    full, no connection / the publisher connection still recovering
    (`not_connected`), a channel could not be opened
    (`channel_unavailable`), a `FlowController` drop
    (`backpressure_dropped`, also on the sync broker; it was `ERROR`).
  - `UNKNOWN`: connection or channel lost after the publish frame
    (`connection_lost`), a sibling's refusal closed a shared channel
    (`channel_closed`), unconfirmed at shutdown (`shutdown`). These used to
    be `ERROR`.
  - `ERROR` is left for a publish the broker refused (`exchange_not_found`,
    `access_refused`, `precondition_failed`, with `reply_code` /
    `reply_text`), invalid input (`invalid_envelope`), and unclassified
    failures (`publish_error`).
  - `TIMEOUT` is unchanged (`confirm_timeout`, or `publish_timeout` when the
    new per-call timeout expired first).

  Code that matched `ERROR` for connection failures should match
  `UNKNOWN` / `NOT_SENT` (or check `not outcome.ok`, which is unchanged
  for every failure). `ok` is still `True` only for `CONFIRMED` and `SENT`.
  On the sync transport, a non-owner thread publishing on a dead connection
  now gets `NOT_SENT` (`not_connected`) instead of `ERROR`; the sync
  transport's other outcomes are unchanged.
- **No hidden republish after a connection error (async).** A confirmed
  publish that hit a connection-class error waited for recovery and
  published again; if the first copy had reached the broker, the message
  was delivered twice and reported `CONFIRMED`. It is now reported
  `UNKNOWN` and never republished. Republish yourself with the same
  `message_id` if needed. Failures before the frame (no channel) are still
  healed by rebuilding the publisher connection and retrying the acquire,
  which cannot duplicate anything. A publish that finds the publisher
  connection mid-reconnect now waits (up to 5 s or its timeout) before
  sending instead of after failing.
- **A missing exchange is an error, sent once (async).** A 404 on publish
  was treated as a connection error: retried once (two publishes, two
  closed channels) and then reported as a generic `ERROR`. It is now
  `ERROR` with `reason="exchange_not_found"` and `reply_code=404`, not
  retried; the broken channel is discarded.
- **Pool exhaustion waits and reports, it does not rebuild (async).** When
  every publisher channel was busy for `channel_acquire_timeout`, the
  publisher connection was rebuilt, closing every in-use channel and
  failing healthy in-flight publishes. Now the caller gets `NOT_SENT`
  (`admission_timeout`) and the connection is untouched. Only a failure to
  open a channel rebuilds it.
- **`AsyncBroker.stop()` shuts the publisher down.** After the consumer
  drain, publish admission closes: a later `publish()` raises
  `BrokerNotStartedError` (it used to reconnect silently and publish). The
  batch queue is flushed and in-flight confirms are awaited within what is
  left of the stop timeout; whatever remains is reported, never dropped:
  still queued or waiting for a channel → `NOT_SENT` (`publisher_closed`),
  sent but unconfirmed → `UNKNOWN` (`shutdown`). Then the broker
  disconnects.
- **`AsyncBatchPublisher` outcomes and bounds.** Callers still queued at
  `stop()` get `NOT_SENT` instead of a bare `RuntimeError`; a publish that
  raises mid-flush is `UNKNOWN` instead of an exception at the caller.
  Every wait is bounded: queue admission by `channel_acquire_timeout`
  (`NOT_SENT`), the whole call by the per-call `timeout` or admission +
  flush interval + confirm timeout (still queued → withdrawn, `NOT_SENT`;
  being flushed → `TIMEOUT`). Flush workers retry channel acquisition for
  at most `channel_acquire_timeout` and then fail the batch `NOT_SENT`
  (`channel_unavailable`) instead of retrying forever.
  `AsyncBatchPublisher.stop()` takes `timeout` (default `0.0`, the old
  "don't wait" behaviour).
- **`PoolConfig` is validated:** `channel_pool_size >= 1` (0 made the pool
  unbounded) and `channel_acquire_timeout > 0`; invalid values raise
  `ConfigValidationError`. `publisher_connections` / `consumer_connections`
  are documented as reserved and ignored.
- `AsyncChannelPool.release()` ignores a channel it does not own (released
  twice, or checked out before `close_all()` / a connection rebuild); it
  used to decrement the pool's counter anyway, so after a rebuild the pool
  could open more channels than `channel_pool_size`.
- `AsyncChannelPool.acquire()` raises `ChannelPoolExhaustedError` (a
  `TimeoutError` subclass, so existing `except TimeoutError` still works)
  on exhaustion and `ChannelPoolClosedError` once the pool is closed.

### Added

- `PublishStatus.NOT_SENT`, `PublishStatus.UNKNOWN`; `PublishOutcome.reason`
  (bounded code), `reply_code`, `reply_text` (default `None`) and
  `PublishOutcome.is_unknown` (`UNKNOWN` or `TIMEOUT`); `PublishReason`
  (the reason codes, exported from `rabbitkit`).
- `AsyncBroker.publish(..., timeout=None)`: bounds admission (flow
  control, byte budget, a free channel), send and confirm. Expiry before
  sending is `NOT_SENT` (`admission_timeout`), after sending `TIMEOUT`
  (`publish_timeout`). Async only: the sync transport cannot bound an
  owner-thread publish (see `docs/message-safety.md`).
- `publish(..., message_id=None)` for the keyword form on `AsyncBroker`
  and `SyncBroker`, so the same logical message can be republished after
  an unknown outcome and deduplicated downstream. Passing it together with
  an envelope raises `ValueError`.
- `PublisherConfig.max_in_flight_bytes` (default `None`, no bound): caps
  the body bytes of async publishes between admission and their outcome,
  shared by `publish()` and the batch publisher. A publish waits (bounded,
  cancellable, no polling) and then returns `NOT_SENT`
  (`admission_timeout`); a body larger than the whole budget is `NOT_SENT`
  (`exceeds_max_in_flight_bytes`). Must be `>= max_message_bytes` when
  that is set.
- `AsyncTransportImpl.close_publishing(timeout)`,
  `AsyncTransportImpl.publishes_in_flight`,
  `AsyncConnectionPool.close_publisher_channels()`,
  `discard_publisher_channel()` / `discard_mandatory_channel()`,
  `AsyncChannelPool.discard()` / `owns()` / `in_use_count` / `closed`,
  `InFlightByteLimiter` (transport internals used by the above).

### Fixed

- **Batch mode reported an unroutable mandatory publish as `CONFIRMED`**
  (and so did a mandatory publish on the direct reply-to channel). Those
  channels resolve the confirm with the returned message; it is now
  `RETURNED` with `reply_code=312`, `reply_text="NO_ROUTE"` on every path.
- **A cancelled confirmed publish leaked its pool channel.** After
  `channel_pool_size` cancellations every publish stalled for
  `channel_acquire_timeout` and then rebuilt the connection (reproduced: 3
  cancels, next publish took 3.02 s). The channel is now released on every
  path; one cancelled mid-publish is discarded (its confirm state is
  unknown) and its slot freed at once. Its bytes are released too.
- A channel slot freed by a discarded or closed channel now wakes a
  publish waiting for a channel; it used to wait until its timeout.
- The rebuilt publisher channel pool lost the channel metric hooks, so
  `channels_opened_total` / `channel_rebuilds_total` stopped counting after
  the first rebuild.
- `publish_many` / `iter_publish` (async) dropped an item when a publish
  raised `CancelledError` without the task being cancelled (aio-pika closes
  channels that way); it is now `UNKNOWN` (`cancelled`). A task that queued
  its outcome just as the overall deadline abandoned it could yield two
  items for one index; every index now yields exactly one item, and an
  invariant check reports any admitted index without an outcome as
  `UNKNOWN` (`no_outcome`) with an ERROR log.

### Documentation

- `docs/message-safety.md`: the publish outcome table (when to publish
  again), admission and timeouts, publisher shutdown, and new guarantee
  rows. `docs/production/checklist.md`: handling unknown outcomes.
  `docs/api/messages.md`: `PublishReason`.

### README (apply when merging; README.md is not edited on this branch)

In "2. Publish with confirms", replace

> A publish can be `CONFIRMED`, `SENT`, `RETURNED`, `NACKED`, `TIMEOUT`, or
> `ERROR`. Application code can treat those as different states instead of
> assuming "publish called" means "message safe".

with

> A publish can be `CONFIRMED`, `SENT`, `RETURNED`, `NACKED`, `NOT_SENT`
> (definitely never left the client: safe to publish again), `UNKNOWN` or
> `TIMEOUT` (may have reached the broker: publish again only with the same
> `message_id`), or `ERROR`. `outcome.reason` says why. Application code can
> treat those as different states instead of assuming "publish called" means
> "message safe".
