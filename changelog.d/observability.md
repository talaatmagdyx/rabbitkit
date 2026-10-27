<!-- Observability stream (rk2/observability): G-22, G-23, PUB-017, CON-005, §19, §20, ADR-M6. Merge into CHANGELOG.md [Unreleased]. -->

### Added

- **Broker metrics (`rabbitkit.middleware.BrokerMetrics`, G-22).** Both
  brokers now emit, through the first `MetricsMiddleware` with a collector
  (publish middlewares first, then any route):
  `connection_up{role}`, `connections_active`, `channels_active`,
  `connection_failures_total{role,reason}` (bounded reasons: a lost
  connection, a failed reconnect attempt, a terminal give-up),
  `reconnect_attempts_total{role}` (failed attempts),
  `connection_blocked` (`connection.blocked` / `unblocked`),
  `consumer_cancelled_total{queue}` and `route_consuming{queue}` (broker
  cancels, async), `publish_routing_failures_total{exchange,reason}`
  (`returned` / `exchange_not_found`, every transport publish including
  retry and DLQ copies), `publish_in_flight`, `publish_in_flight_bytes`
  (when `max_in_flight_bytes` is set) and `publish_channels_in_use`.
  `role` is `publisher`/`consumer` (async) or `shared` (sync). Gauges that
  change per publish are sampled every 5 s, never on the hot path.
- `rabbitkit_messages_failed_total{queue,category}`: handler failures by
  `ErrorCategory` (emitted by `MetricsMiddleware`).
- `PrometheusCollector(registry=...)`: register in an application's own
  `CollectorRegistry` (default: the global `REGISTRY`, as before). A second
  collector on the same registry reuses the existing metrics instead of
  raising "Duplicated timeseries"; a conflicting registration still raises.
- `metric_inventory()` / `MetricSpec` / `bounded_label_values()` /
  `connection_failure_reason()` in `rabbitkit.middleware.metrics` (the first
  two also from `rabbitkit.middleware`): the code's list of every metric,
  type and label set. `docs/observability.md` is generated from it and a unit
  test keeps them in sync; another drives every emission path and checks each
  label against the bounded allow-list.
- Transport hooks: `AsyncTransportImpl.on_connection_state(cb(role, up, error))`,
  `on_publish_outcome(cb(outcome, seconds))`, `connection_states()`,
  `channels_active`, `publish_channels_in_use`;
  `AsyncConnectionPool(on_connection_state=...)`, `channel_counts()`;
  `ReconnectPolicy(on_lost=..., on_restored=...)`;
  `SyncTransport.on_connection_state`, `on_reconnect_attempt(cb(attempt, error))`,
  `on_publish_outcome`, `channels_active`; `SyncTransport.reconnect(cause=None)`.
  `TerminalConnectionError.role` (`""` by default).
- `OTelTracingMiddleware(consumer_context="parent" | "link")`: `"link"`
  starts the CONSUMER span from the consumer's own context with a span link
  to the producer (OpenTelemetry messaging conventions for decoupled
  consumption). `"parent"` (default) is the previous behaviour.
- `LoggingConfig.override_existing` (default `False`), `configure_structlog(...,
  override_existing=True)` (returns whether it configured) and
  `structlog_configured_elsewhere()`.
- **Alert rules and runbooks (G-23).** `deploy/prometheus/rabbitkit-alerts.yaml`
  (plain Prometheus rules; wrap in a `PrometheusRule` for the Operator):
  PublisherConfirmFailures, PublisherUnknownOutcomes,
  PublisherUnroutableMessages, RabbitMQBrokerUnavailable,
  RabbitMQConnectionFailures, RabbitMQConnectionTerminalFailure,
  RabbitMQConnectionBlocked, ConsumerProcessingFailures,
  ConsumerNotConsuming, ConsumerBacklogGrowing, ConsumerUnackedHigh,
  DeadLetterQueueGrowing, RetryStormDetected, and broker-side (needs the
  `rabbitmq_prometheus` plugin) RabbitMQDiskAlarm, RabbitMQMemoryAlarm,
  RabbitMQQuorumUnhealthy. Thresholds are placeholders to tune from measured
  workloads. Behaviour tests in `deploy/prometheus/tests/` run with
  `promtool test rules`. Every alert links a runbook under `docs/runbooks/`.

### Changed (behaviour)

- **`rabbitkit_messages_published_total` gains a bounded `reason` label**
  (`PublishReason` code; `none` for a success, `exception` when the call
  raised, `other` for an unknown code). Aggregations by `exchange`/`status`
  are unaffected; a selector on the exact old label set needs the new label.
- **`rabbitkit_publish_confirm_latency_seconds` measures send-to-confirm**,
  timed by the transport from handing the frame to the client library until
  the broker's confirm (async: the `exchange.publish()` await; sync: pika's
  `basic_publish()` call). It used to time the whole `broker.publish()` call
  (admission, channel wait, middleware). It now also covers retry and DLQ
  copies, and is emitted whenever any collector is configured. Under
  `TestBroker` / a custom publish function it still times the whole call.
- **`rabbitkit_broker_connected` follows drops and reconnects** (it was only
  set at `start()`/`stop()` and stayed 1 through an outage). It is 1 only
  while every connection is up.
- **Connection and channel metrics no longer need a `MetricsMiddleware` on a
  route**: a publish-only broker with one in `middlewares=[...]` gets
  `reconnects_total`, `channels_opened_total`, `channel_rebuilds_total` and
  the new ones. They are wired before the first connect, so the initial
  connections and channels are counted (before, counting started after
  topology declaration): expect `channels_opened_total` to be a few higher
  right after start.
- **The broker no longer replaces an application's structlog configuration.**
  With `RabbitConfig(logging=LoggingConfig(...))`, `start()` used to call
  `structlog.configure()` unconditionally, clobbering e.g. obskit's setup in
  an embedding application. Now, if structlog was configured by someone else,
  the broker keeps it and only attaches rabbitkit's value redaction to the
  `rabbitkit.*` loggers. `LoggingConfig(override_existing=True)` restores the
  old behaviour. A direct `configure_structlog()` call is unchanged.
- **The PRODUCER span records the publish outcome.** `broker.publish()`
  returns failures instead of raising, so the span used to look successful
  for NOT_SENT / UNKNOWN / TIMEOUT / RETURNED / NACKED / ERROR. It now gets
  `messaging.rabbitmq.publish.status`, `messaging.rabbitmq.publish.reason`,
  `error.type` and an `ERROR` status for every outcome that is not `ok`.

### Fixed

- `docs/observability.md` told users to alert on
  `messages_published_total{status="failure"}`, a value that was never
  emitted (G-28); and it claimed the first connection predates the metrics
  wiring.

### Documentation

- `docs/observability.md` rewritten: the generated metric inventory, bounded
  label values, metric notes, the trace model, logging, the alert table with
  runbook links. New `docs/runbooks/` (10 runbooks + index) in the mkdocs nav.

### Tests

- `tests/integration/test_observability_foundation.py` (real broker):
  publish counters by status and reason, routing failures and confirm latency
  through a real `PrometheusCollector`; sync routing failures and connection
  state; `connection_up` / `broker_connected` drop and recover on a
  connection kill; failed reconnect attempts per role while the broker is
  down; `channels_active` bounded under concurrent publishing;
  `connection_blocked` on a disk alarm; a trace id propagating producer →
  RabbitMQ → consumer and through a retry copy; `promtool check rules` and
  `promtool test rules` via the `prom/prometheus` image (skipped without
  Docker).

## README

No README change required. (Optional: in the metrics section, mention
`PrometheusCollector(registry=...)` and `deploy/prometheus/rabbitkit-alerts.yaml`.)
