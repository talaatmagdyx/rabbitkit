# Consumer stream — CHANGELOG [Unreleased] entries

Merge these into `CHANGELOG.md` under `## [Unreleased]`, in the matching sections.

### Changed (behaviour)

- **Async settlement is claimed before the network write (G-15).**
  `RabbitMessage.ack_async()` / `nack_async()` / `reject_async()` set the
  disposition before awaiting the transport. Before, the guard set it only
  after the await, so two coroutines (a concurrent ack and nack, or `stop()`
  nacking a handler cancelled mid-ack) could both pass it and send two frames
  for one delivery tag. RabbitMQ answers the second with
  `PRECONDITION_FAILED - unknown delivery tag`, closes the channel and
  redelivers everything unacked on it (reproduced on a real broker: messages
  were handled twice). Now one frame is sent and the channel stays open.
  **Observable change:** when the write raises or is cancelled, the message
  is no longer left `pending`: `disposition` keeps the claimed value,
  `is_settled` is `True`, and the new `settlement_outcome_unknown` is `True`.
  A later settlement call on that message is a no-op (it could contradict a
  frame that landed); if the frame was lost, the broker redelivers when the
  channel closes. The exception still propagates. The settlement metric
  (`messages_acked_total` etc.) is not emitted for such a message. The sync
  methods (`ack()`, `nack()`, `reject()`) are unchanged.
- **`AsyncBroker.start(install_signal_handlers=True)` no longer clobbers the
  application's signal handlers (G-24).** The default stays `True`. The
  Python-level SIGINT/SIGTERM handler in place is captured with
  `signal.getsignal` and restored by `stop()` (before, `stop()` reset it to
  the default, so e.g. `asyncio.run()`'s SIGINT handler or an application's
  SIGTERM handler was lost); replacing a custom one logs a WARNING. A handler
  registered with `loop.add_signal_handler` (the application's, or
  `RabbitApp.run_async()`'s) cannot be read back through asyncio's public
  API, so the broker now leaves that signal alone and logs a WARNING; that
  handler must call `broker.request_shutdown()` or `await broker.stop()`.
  `stop()` also no longer removes a handler the application installed after
  `start()`. Embedding applications should pass
  `install_signal_handlers=False` (see `docs/kubernetes.md`).
- **`rabbitkit_lifespan` starts an `AsyncBroker` with
  `install_signal_handlers=False`** (G-24). The ASGI server owns
  SIGINT/SIGTERM and runs the lifespan teardown on them; with the broker's
  handlers installed over the server's, SIGTERM stopped only the broker and
  the server kept running.
- **`AsyncBroker.stop()` hands not-started worker-pool deliveries back at
  once.** With `WorkerConfig(worker_count > 1)`, deliveries that were
  prefetched into the pool but were still waiting for a worker slot are now
  cancelled before their handler starts and nacked with `requeue=True` at the
  beginning of `stop()`. Before, `stop()` ran them during the grace period
  (and cut them off at the deadline). In-flight handlers keep the
  `graceful_timeout` grace. Verified on a real broker: 2 in flight, 4 queued
  → the 4 are back on the queue while the 2 still run.
- **Per-route `prefetch_count` is validated at registration (CONSUMER-005).**
  `@broker.subscriber(..., prefetch_count=0)` silently fell back to the
  global value, and a negative value was sent to the broker. It now raises
  `ConfigurationError` unless it is `None` (inherit) or an `int >= 1`.

### Added

- **Broker-cancelled consumers are detected, reported and re-subscribed
  (G-26).** RabbitMQ cancels a consumer itself (`basic.cancel`) when its
  queue is deleted, a quorum leader moves or an operator closes it; aiormq
  < 7 only drops the tag, so rabbitkit kept reporting the route as consuming
  and never subscribed again. The async transport now detects it (aiormq's
  consumer-cancel callback on aiormq 7, otherwise by checking the channel's
  consumer table every second), logs a WARNING, and re-subscribes with the
  same tag once a passive declare finds the queue again (backoff 1 s,
  doubling, capped at 30 s). It never re-creates the queue. New:
  `RouteRuntimeState.consumer_cancelled` (health and readiness count the
  route as not consuming while it is `True`), `AsyncBroker.on_consumer_cancelled`
  and `AsyncBroker.on_consumer_resumed` hooks (`Callable[[str], None]`, called
  with the queue name), and on `AsyncTransportImpl`
  `on_consumer_cancelled(cb)`, `on_consumer_resumed(cb)` and
  `cancelled_consumers`. aio-pika 10 also re-subscribes such consumers;
  whichever runs first wins. Verified on a real broker with aio-pika 10.2.1
  (with and without aio-pika's own recovery) and 9.6.2.
- `RabbitMessage.settlement_outcome_unknown` (see Changed).

### Fixed

- **A reconnect during `AsyncBroker.stop()`'s drain could re-subscribe a
  cancelled consumer (CONSUMER-007).** `cancel_consumer()` cancelled through
  a fresh `get_queue()` wrapper, so aio-pika's robust-restore record stayed
  on the original queue object and a reconnect restored the consumer; it then
  took new deliveries during shutdown. Reproduced on a real broker (connection
  kill during the drain → consumer count back to 1). The cancel now goes
  through the queue object the consumer was started on, the restore record is
  dropped even if the cancel itself fails (aio-pika 9 forgets it only after
  `CancelOk`), and a delivery that still reaches a cancelled consumer is
  requeued and that consumer cancelled again instead of running the handler.

### Documentation

- `docs/message-safety.md`: one settlement frame per delivery and the unknown
  outcome; failure settlement verified on a real broker (AUTO transient,
  MANUAL raise, processing timeout); inline handlers are cancelled by aiormq
  when their channel closes, pool handlers are not (and are processed twice);
  the worker pool is shared by all routes; the `stop()` sequence;
  broker-cancelled consumers and their limitation; the MANUAL example uses
  the async settlement methods.
- `docs/kubernetes.md`: who owns SIGTERM (`broker.run()` standalone,
  `install_signal_handlers=False` when embedded), what `start()` does to
  existing handlers, and `terminationGracePeriodSeconds` vs
  `graceful_timeout` (the defaults, 30 s and 30 s, leave no margin); the
  `rabbitkit_lifespan` example passes `broker=` (it passed the broker as the
  `app` argument).

## README

No README change required.
