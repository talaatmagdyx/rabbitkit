### Changed (behaviour)

- **`SyncBroker` enforces `ConsumerConfig.processing_timeout`.** It used to
  ignore it and log a warning. Python cannot interrupt a thread, so the
  semantics are: when a handler run passes its deadline, the I/O thread
  settles the delivery through the pipeline's normal failure path with
  `HandlerTimeoutError` (`UNKNOWN_OUTCOME`, transient: first delivery
  nack-requeued, a redelivery rejected into the DLQ, logged with
  `outcome_unknown=True`), exactly as `AsyncBroker` settles its cancelled
  handler task. The handler thread is not stopped: it keeps its worker until
  it returns, so no more than `worker_count` handlers ever run; when it
  returns, its result is logged and discarded. Settlement is claimed once per
  delivery, so the late handler's ack/nack/reject never reaches the channel
  (a second settlement of a settled tag would close it), and its
  handler-result, retry and DLQ publishes are refused (`NOT_SENT`, reason
  `cancelled`). The deadline starts when the handler starts and is checked
  on every consume-loop tick (at most `min(1 s, processing_timeout / 4)`
  apart) and during the `stop()` drain.
  **Observable change:** with `processing_timeout` set, sync handlers no
  longer run inline on the I/O thread. `worker_count > 1` uses the worker
  pool as before; `worker_count = 1` now runs handlers on one dedicated
  handler thread (same order, one at a time), chosen over refusing to start
  because the CRITICAL profile requires `processing_timeout` and inline
  sync consumers under it were valid configurations. Handlers that depend on
  running on the thread that called `start()` (thread-local state,
  `broker.request()`) need the deadline off. Unchanged when
  `processing_timeout` is `None` (the default). A handler that never returns
  holds its worker for good; with one worker the route then stops taking
  deliveries (they stay prefetched, unacked).
- **`SyncBroker` reports a consumer the broker cancelled as not consuming.**
  Before, a server `basic.cancel` (queue deleted, quorum leader moved,
  operator action) left the route reported as consuming while nothing was
  delivered, and it never subscribed again. `broker_health_check()` now
  reports such a route as not consuming (DEGRADED) and `broker_readiness()`
  fails until the consumer is back.

### Added

- **Broker-cancelled consumers on `SyncBroker` (G-26 parity).** The sync
  transport registers pika's `BlockingChannel.add_on_cancel_callback` on every
  consumer channel. On a server `basic.cancel` (dispatched by pika on the I/O
  thread) it logs a WARNING, sets `RouteRuntimeState.consumer_cancelled`,
  counts `consumer_cancelled_total{queue}`, sets `route_consuming{queue}` to 0
  and calls `SyncBroker.on_consumer_cancelled(queue)`. The consume loop then
  probes the queue with a passive declare (backoff 1 s, doubling, capped at
  30 s) and re-subscribes with the same consumer tag once it exists, on the
  I/O thread (never from a worker thread, never re-creating the queue,
  reusing the consumer channel or opening a fresh one if the broker closed
  it), then calls `SyncBroker.on_consumer_resumed(queue)`. A reconnect that
  re-subscribes a cancelled route also reports it resumed. New:
  `SyncBroker.on_consumer_cancelled` / `on_consumer_resumed`
  (`Callable[[str], None] | None`, as on `AsyncBroker`), and on
  `SyncTransport` `on_consumer_cancelled(cb)`, `on_consumer_resumed(cb)`,
  `cancelled_consumers`; module `rabbitkit.sync.consumers`
  (`SyncConsumerSupervisor`).
- **`rabbitkit_publish_in_flight` on `SyncBroker` (G-22).**
  `SyncTransport.publishes_in_flight` counts `publish()` calls between entry
  and outcome, including worker-thread publishes waiting for the I/O thread;
  the broker samples it with the other gauges (I/O-loop tick, throttled to
  5 s) and now also from `pump_idle()`, so a publish-only sync broker samples
  its gauges too. `publish_in_flight_bytes` stays async-only (the byte budget
  is enforced by `AsyncBroker` only) and so does `publish_channels_in_use`
  (the sync broker has one publisher channel, not a pool); the metric
  inventory says so.
- `SyncTransport.poll_interval` (default 1.0 s): the consume loop's longest
  `process_data_events` wait. `SyncWorkerPool(config, threaded=True)` runs a
  single worker on its own thread. `HandlerPipeline.settle_expired_sync()`.
  Module `rabbitkit.sync.deadlines` (`SyncDeadlines`, `DeliveryClaim`,
  `LateSettlementError`).

### Fixed

- The settlement metric (`messages_acked/nacked/rejected_total`) is counted
  once per delivery when the sync processing deadline and the late handler
  both report it.

### Documentation

- `docs/reliable-messaging.md`: new "Processing deadline" and
  "Broker-cancelled consumers" sections; the settlement table row for
  `processing_timeout` no longer says "async only" and now states what both
  brokers do (no retry copy: the deadline wraps the retry middleware); two
  verified failure scenarios added (sync and async); the "enforced by
  `AsyncBroker` only" note now covers only `max_in_flight_bytes`.
- `docs/concurrency-model.md`: which thread runs sync handlers under a
  deadline, settles expired deliveries and re-subscribes cancelled
  consumers; the claim protocol; the M5 note applies only without a deadline.
- `docs/observability.md`: `publish_in_flight` is no longer async-only;
  `consumer_cancelled_total` / `route_consuming` cover the sync broker;
  sync sampling caveat (gauges hold their last value while the I/O thread is
  blocked inside a publish).
- `docs/message-safety.md`, `docs/ack-policy.md`,
  `docs/runbooks/consumer-failures.md`, `ConsumerConfig` / `WorkerConfig` /
  `TimeoutMiddleware` docstrings: the sync caveats these changes make untrue
  are removed or rewritten.
