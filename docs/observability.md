# Observability reference

What rabbitkit emits — metrics, traces and logs — and the alert rules and
runbooks built on them. The metric table below is generated from
`rabbitkit.middleware.metrics.metric_inventory()` (the code's own list), and a
unit test fails if the two drift apart.

All names use the default `namespace="rabbitkit"` (`MetricsConfig.namespace`);
a custom namespace replaces the `rabbitkit_` prefix. Nothing is emitted unless
you pass a collector to a `MetricsMiddleware` (or to `QueueMetricsPoller` for
the queue-depth gauges); rabbitkit never talks to Prometheus or StatsD on its
own.

```python
from rabbitkit.middleware.metrics import MetricsMiddleware, PrometheusCollector

collector = PrometheusCollector()                      # global prometheus_client.REGISTRY
collector = PrometheusCollector(registry=my_registry)  # or the application's own registry
broker = AsyncBroker(config, middlewares=[MetricsMiddleware(collector)])
```

## Who emits what

- **`MetricsMiddleware`** on a route measures handler runs and settlement; on
  the broker's `middlewares=[...]` it measures `broker.publish()` calls.
- **The broker itself** (`BrokerMetrics`) emits the connection, channel,
  flow-control, consumer-cancel and transport-publish metrics. It uses the
  first `MetricsMiddleware` with a collector it finds: the broker's publish
  middlewares first, then any route. A **publish-only** broker therefore gets
  them too (before, they needed a `MetricsMiddleware` on a route). They are
  wired before the first connect, so the first connection and channels count.
- **`RetryMiddleware`**, **`DeduplicationMiddleware`**, **`RateLimitMiddleware`**
  emit through the collector of the route's `MetricsMiddleware`.
- **`QueueMetricsPoller`** reads queue depth from the management API.

Gauges that change on every publish (publishes and bytes in flight, channels
in use, open channels) are **sampled**, not updated on the hot path: at start
and then every 5 seconds (the async broker's heartbeat task; the sync
broker's I/O loop, or `pump_idle()` on a publish-only sync broker, throttled
to once per 5 seconds). The sync broker samples on its I/O thread, so while
that thread is itself blocked inside a publish (a connection blocked by a
broker alarm) its gauges keep their last value until it returns. Connection
state also updates the moment a connection drops or comes back.

## Metric inventory

<!-- metric-inventory:start -->
| Metric | Type | Labels | Emitted by | Meaning |
|---|---|---|---|---|
| `rabbitkit_messages_consumed_total` | counter | `queue`, `status` | MetricsMiddleware | Handler executions; status success or error. |
| `rabbitkit_messages_failed_total` | counter | `queue`, `category` | MetricsMiddleware | Handler failures by ErrorCategory. |
| `rabbitkit_message_processing_seconds` | histogram | `queue` | MetricsMiddleware | Handler duration. |
| `rabbitkit_in_flight_messages` | gauge | `queue` | MetricsMiddleware | Handlers running now. |
| `rabbitkit_messages_redelivered_total` | counter | `queue` | MetricsMiddleware | Deliveries the broker flagged redelivered. |
| `rabbitkit_messages_acked_total` | counter | `queue` | MetricsMiddleware | Messages acked by the pipeline. |
| `rabbitkit_messages_nacked_total` | counter | `queue` | MetricsMiddleware | Messages nacked by the pipeline. |
| `rabbitkit_messages_rejected_total` | counter | `queue` | MetricsMiddleware | Messages rejected by the pipeline. |
| `rabbitkit_settlement_outcome_unknown_total` | counter | `queue` | MetricsMiddleware | Settlements whose frame may not have reached the broker (redelivery possible). |
| `rabbitkit_messages_retried_total` | counter | `queue`, `category` | RetryMiddleware | Failures handed to a retry delay queue. |
| `rabbitkit_messages_dead_lettered_total` | counter | `queue`, `category` | RetryMiddleware | Terminal failures sent to the DLQ. |
| `rabbitkit_retry_handoff_failures_total` | counter | `queue` | RetryMiddleware | Retry/DLQ copy publishes that failed. |
| `rabbitkit_retry_handoff_paused` | gauge | `queue` | RetryMiddleware | 1 while a route's retry handoff is backing off. |
| `rabbitkit_dedup_fallback_total` | counter | `queue` | DeduplicationMiddleware | Messages processed despite a Redis error. |
| `rabbitkit_rate_limit_dropped_total` | counter | `reason` | RateLimitMiddleware | Messages settled without running the handler. |
| `rabbitkit_consumer_cancelled_total` | counter | `queue` | broker (BrokerMetrics) | Consumers the broker cancelled (basic.cancel). |
| `rabbitkit_route_consuming` | gauge | `queue` | broker (BrokerMetrics) | 1 while the route's consumer is subscribed. |
| `rabbitkit_consumer_active` | gauge | — | broker (BrokerMetrics) | Routes registered while started. |
| `rabbitkit_worker_pool_pending` | gauge | — | broker (BrokerMetrics) | Deliveries waiting for a worker. |
| `rabbitkit_messages_published_total` | counter | `exchange`, `status`, `reason` | MetricsMiddleware | broker.publish() outcomes by PublishStatus and PublishReason. |
| `rabbitkit_message_publish_seconds` | histogram | `exchange` | MetricsMiddleware | Whole publish call, including admission. |
| `rabbitkit_publish_confirm_latency_seconds` | histogram | `exchange` | broker (BrokerMetrics) | Send to broker confirm for CONFIRMED publishes (transport-timed). |
| `rabbitkit_publish_routing_failures_total` | counter | `exchange`, `reason` | broker (BrokerMetrics) | Unroutable publishes: returned or exchange_not_found (all transport publishes). |
| `rabbitkit_publish_in_flight` | gauge | — | broker (BrokerMetrics) | Publishes between admission and outcome (sampled). |
| `rabbitkit_publish_in_flight_bytes` | gauge | — | broker (BrokerMetrics) | Bytes held by max_in_flight_bytes (sampled; only when set). |
| `rabbitkit_publish_channels_in_use` | gauge | — | broker (BrokerMetrics) | Async publisher channels checked out (sampled; the sync broker has one publisher channel). |
| `rabbitkit_bulk_publish_items_total` | counter | `status`, `reason` | broker bulk API | publish_many/iter_publish item outcomes. |
| `rabbitkit_bulk_publish_batch_size` | histogram | — | broker bulk API | publish_many input sizes. |
| `rabbitkit_settlement_items_total` | counter | `action`, `status` | broker bulk API | ack_many/nack_many item outcomes. |
| `rabbitkit_broker_connected` | gauge | — | broker (BrokerMetrics) | 1 while every connection is up; follows reconnects. |
| `rabbitkit_connection_up` | gauge | `role` | broker (BrokerMetrics) | 1 while the connection of this role is up. |
| `rabbitkit_connections_active` | gauge | — | broker (BrokerMetrics) | Connections up. |
| `rabbitkit_channels_active` | gauge | — | broker (BrokerMetrics) | Channels held open (sampled). |
| `rabbitkit_channels_opened_total` | counter | — | broker (BrokerMetrics) | Channels created. |
| `rabbitkit_channel_rebuilds_total` | counter | — | broker (BrokerMetrics) | Channels created to replace a lost one. |
| `rabbitkit_reconnects_total` | counter | — | broker (BrokerMetrics) | Successful reconnects (one per connection). |
| `rabbitkit_reconnect_attempts_total` | counter | `role` | broker (BrokerMetrics) | Failed reconnect attempts. |
| `rabbitkit_connection_failures_total` | counter | `role`, `reason` | broker (BrokerMetrics) | Connections lost, reconnect attempts failed, terminal give-ups. |
| `rabbitkit_connection_blocked` | gauge | — | broker (BrokerMetrics) | 1 while RabbitMQ blocks the connection (memory/disk alarm). |
| `rabbitkit_connection_blocked_reason` | gauge | `reason` | broker (BrokerMetrics) | 1 per alarm (disk, memory, other) the active connection.blocked reason names. |
| `rabbitkit_settlement_pending` | gauge | — | coalescing acker | Registered deliveries not yet settled. |
| `rabbitkit_settlement_ack_ready` | gauge | — | coalescing acker | Deliveries approved for ack, not yet sent. |
| `rabbitkit_settlement_frontier` | gauge | — | coalescing acker | Highest tag a cumulative ack could cover. |
| `rabbitkit_settlement_gap_count` | gauge | — | coalescing acker | Blocking deliveries below an ack-ready one. |
| `rabbitkit_settlement_oldest_pending_age_seconds` | gauge | — | coalescing acker | Age of the oldest unsettled delivery. |
| `rabbitkit_settlement_coalescing_ratio` | gauge | — | coalescing acker | Deliveries settled per frame. |
| `rabbitkit_settlement_coalesced_total` | counter | — | coalescing acker | Tags settled through a cumulative ack. |
| `rabbitkit_queue_messages_ready` | gauge | `queue` | QueueMetricsPoller | Messages ready (backlog). |
| `rabbitkit_queue_messages_unacked` | gauge | `queue` | QueueMetricsPoller | Messages delivered, not yet acked. |
| `rabbitkit_queue_messages_total` | gauge | `queue` | QueueMetricsPoller | Ready plus unacked. |
| `rabbitkit_queue_consumers` | gauge | `queue` | QueueMetricsPoller | Consumers attached to the queue. |
| `rabbitkit_queue_quorum_members` | gauge | `queue` | QueueMetricsPoller | Quorum queues: Raft members configured. |
| `rabbitkit_queue_quorum_online` | gauge | `queue` | QueueMetricsPoller | Quorum queues: members online (majority = available). |
| `rabbitkit_health_state` | gauge | — | HealthWatcher | Debounced health: 0 healthy, 1 degraded, 2 unhealthy. |
<!-- metric-inventory:end -->

`bulk_publish_admission_wait_seconds` is declared on `MetricsConfig` but not
emitted. `HealthWatcher`'s gauge name is a constructor argument
(`gauge_name`); the table shows its default.

### Bounded labels

Every label is either an enumerated value set or a declared topology name.
Message ids, routing keys and tenant ids are never labels
(`bounded_label_values()` is the allow-list; a unit test drives every
emission path and checks each label against it).

| Label | Values |
|---|---|
| `queue` | the bound queue name (`x-rabbitkit-original-queue`), never the routing key |
| `exchange` | the exchange name, `default` for `""` |
| `status` | `PublishStatus` values (`confirmed`, `sent`, `returned`, `nacked`, `timeout`, `not_sent`, `unknown`, `error`); `success`/`error` on `messages_consumed_total`; the bulk and settlement item statuses |
| `reason` | `PublishReason` codes, plus `none` (no reason: a success), `exception` (the publish call raised) and `other` (a reason outside `PublishReason`); connection failure reasons (below); bulk reason codes; rate-limit reasons; `disk` / `memory` / `other` on `connection_blocked_reason` |
| `category` | `ErrorCategory`: `retryable`, `permanent`, `invalid_message`, `unknown_outcome`, `infrastructure`, `shutdown` |
| `role` | `publisher`, `consumer` (async, two connections), `shared` (sync, one connection), `connection` (a terminal failure without a role) |
| `action` | `ack`, `nack`, `reject` |

`connection_failures_total{reason}`: `connection_refused`, `connection_reset`,
`timeout`, `dns_failure`, `tls_error`, `broker_closed` (the broker closed the
connection), `connection_lost` (any other drop), `other`, and the terminal
`authentication_failed`, `vhost_access_refused`, `tls_verification_failed`,
`tls_handshake_rejected`, `reconnect_budget_exhausted`.

### Notes on specific metrics

- **`messages_published_total{exchange,status,reason}`** counts
  `broker.publish()` outcomes by the real `PublishOutcome.status` and its
  bounded `reason`. `broker.publish()` returns failures instead of raising,
  so a failed publish is never counted as a success. A raised exception is
  `status="error", reason="exception"`. The `reason` label is new: a selector
  on the exact old label set needs it; aggregations by `status` are
  unaffected.
- **`publish_confirm_latency_seconds`** is timed by the transport, from
  handing the frame to the client library until the broker's confirm, for
  `CONFIRMED` publishes. It excludes admission (byte budget, a free channel,
  waiting for a reconnect), serialization and middleware. Async: the
  `exchange.publish()` await (frame write plus confirm). Sync: the pika
  `basic_publish()` call (it blocks for the confirm). It covers every
  transport publish, including retry and dead-letter copies. Only under
  `TestBroker` or a custom publish function, where no transport reports it,
  does `MetricsMiddleware` still time the whole call. `message_publish_seconds`
  remains the whole `broker.publish()` call.
- **`publish_routing_failures_total`** counts every transport publish RabbitMQ
  could not route: `returned` (`mandatory=True`, no binding) and
  `exchange_not_found` (404). It needs no `MetricsMiddleware` in the publish
  chain: any collector on the broker is enough.
- **`publish_in_flight`** counts transport `publish()` calls between
  admission and their outcome (batch-publisher items are not included), on
  both brokers. Async: including the wait for a pooled channel and the byte
  budget. Sync: including a worker thread's wait for the I/O thread to pick
  its publish up; the sync broker publishes on one channel, one publish at a
  time, so a value above 1 means publishes are queued behind the I/O thread.
  **`publish_in_flight_bytes`** is the `PublisherConfig.max_in_flight_bytes`
  budget in use, on both brokers (async: publish and the batch publisher;
  sync: every transport publish, including worker-thread publishes waiting
  for the I/O thread); it is only emitted when that budget is set.
  **`publish_channels_in_use`** is async only: the sync broker has a single
  publisher channel, not a pool.
- **`broker_connected`** is 1 only while every connection is up and follows
  drops and reconnects (it used to be set only at `start()`/`stop()`);
  `connection_up{role}` and `connections_active` break it down.
- **`channels_active`** is the number of open channels: async, the publisher
  and mandatory pools' channels plus the topology, fast-publish and one per
  consumed queue (bounded by `2 × channel_pool_size + 2 + routes`); sync, the
  publisher channel plus one per consumed queue.
- **`reconnect_attempts_total`** counts FAILED reconnect attempts;
  `reconnects_total` counts successful ones (one per connection).
- **`connection_blocked`** is 1 between RabbitMQ's `connection.blocked` and
  `connection.unblocked` (a memory or disk alarm).
  **`connection_blocked_reason{reason}`** says which: RabbitMQ's blocked
  reason is `"low on disk"` or `"low on memory"` (both alarms name both), so
  `reason="disk"` / `"memory"` is 1 while that alarm blocks the connection,
  and `other` covers any other text. All three are 0 while unblocked. This is
  the client-side form of the broker's disk and memory alarm metrics; no
  `rabbitmq_prometheus` plugin is needed. `transport.blocked_reason` has the
  broker's text. The async transport reads the reason from aiormq, which
  otherwise only logs it (rabbitkit wraps aiormq's private
  `connection.blocked` handler to keep it; on an aiormq without that handler
  the reason is `other`). Limits: RabbitMQ blocks a connection when it next
  publishes, and sends the reason once, so an alarm raised while the
  connection is already blocked is not announced again. The sync transport
  sees `connection.blocked` only when pika runs its callbacks (the consume
  loop, `pump_idle()`, a publish without confirms); a publish waiting for a
  confirm on a blocked connection times out first.
- **`consumer_cancelled_total` / `route_consuming`**: RabbitMQ cancelled a
  consumer (`basic.cancel`); both brokers report it at once and re-subscribe
  when the queue exists again (the sync broker on its I/O thread, from the
  consume loop).
- **`messages_failed_total{queue,category}`** classifies handler failures with
  the failure taxonomy (the same classification as the pipeline's
  "Handler failed" log line); `messages_retried_total` and
  `messages_dead_lettered_total` carry the same `category`.

## Queue depth (management-API bridge)

Emitted by `QueueMetricsPoller`, which periodically polls the RabbitMQ
management API and re-exposes the result as gauges through the same
`MetricsCollector` — because the metrics above only ever see traffic *this
process* handles. A queue can silently accumulate millions of messages
while every in-process counter stays green, because the consumer fell behind
or died; queue depth lives on the broker, not in your process.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rabbitkit_queue_messages_ready` | Gauge | `queue` | Backlog depth (ready to be delivered). |
| `rabbitkit_queue_messages_unacked` | Gauge | `queue` | Delivered but not yet acked. |
| `rabbitkit_queue_messages_total` | Gauge | `queue` | `ready + unacked`. |
| `rabbitkit_queue_consumers` | Gauge | `queue` | Consumer count on that queue — `0` means nothing is draining it. |
| `rabbitkit_queue_quorum_members` | Gauge | `queue` | Quorum queues only: Raft members of the queue. |
| `rabbitkit_queue_quorum_online` | Gauge | `queue` | Quorum queues only: members online. The queue works while more than half are online. |

The quorum gauges count the `members` and `online` node lists the management
API reports for every quorum queue (verified on RabbitMQ 3.13 and 4.1; on a
3-node cluster `online` drops as soon as a member's node stops). They count
nodes and never label by them, so the only label stays `queue`.

```python
from rabbitkit import RabbitManagementClient, QueueMetricsPoller

poller = QueueMetricsPoller(
    management_client=RabbitManagementClient(...),
    collector=collector,   # same MetricsCollector as MetricsMiddleware
    interval=15.0,
)
poller.start()  # background daemon thread (sync); poller.start_async() for async brokers
```

Alert on `queue_messages_ready` growth, `queue_consumers == 0` and quorum
members offline: exactly the signals the in-process counters above cannot
provide on their own.

## Bulk operations, settlement & retry handoff

Alerting guidance:

- **`bulk_publish_items_total{status="unknown"}` sustained above ~0** — the
  broker may hold messages you have not accounted for. Reconcile by
  `message_id` + `attempt_id`; do not blindly replay the batch.
- **`bulk_publish_items_total{status="not_sent",reason="admission_timeout"}`**
  — the producer is outrunning `max_in_flight` / `max_buffer_bytes`. Raise
  the bounds deliberately or slow the source.
- **`retry_handoff_failures_total` rising / `retry_handoff_paused == 1`** —
  the delay-queue topology is unreachable (deleted queue, DLX policy
  mismatch, connection flapping). Messages are being nack-requeued, not
  lost, but nothing is progressing.
- **`settlement_items_total{status="stale"}`** — deferred acks are
  outliving their channel (reconnects under MANUAL/batch-commit consumers).
  Shorten the batch window or ack sooner.
- **`settlement_oldest_pending_age_seconds` rising with a flat
  `settlement_frontier`** — one slow handler is stranding the whole
  cumulative-ack prefix. Safe (nothing is acked early), but unacked messages
  accumulate; alert before `max_pending` is reached.

## Async reconnects

`AsyncBroker` emits `rabbitkit_reconnects_total` again (it was removed in
0.14 on a measurement that said aio-pika never reports a broker-closed
connection; that measurement was wrong). The counter is driven by
`transport.on_reconnect`, which fires:

- once per AMQP connection that reconnects in place (publisher and consumer:
  two per outage), *after* aio-pika has restored the consumers, so a restored
  channel can already be delivering when it runs (which is why
  `CoalescingAckerGroup.reset()` keeps live channels' ledgers; see
  [bulk operations](bulk-operations.md));
- when rabbitkit replaces a connection object (a rebuilt publisher
  connection, a lazy re-create).

The transport also exposes the rest of the reconnect lifecycle as hooks, for
your own metrics and logs:

| Hook | Fires | Argument |
|---|---|---|
| `transport.on_reconnect_attempt(cb)` | after every failed reconnect attempt of a connection that had been up | `ReconnectAttempt(role, attempt, delay, elapsed, endpoint, error)`: `role` is `"publisher"`/`"consumer"`, `delay` the backoff before the next attempt |
| `transport.on_reconnect(cb)` | a reconnect succeeded | none |
| `transport.on_connection_failed(cb)` | once, when a connection stops reconnecting for good (permanent failure or spent budget) | the `TerminalConnectionError` (its `role` names the connection); also kept in `transport.connection_failure` |
| `transport.on_connection_state(cb)` | a connection came up (created or reconnected) or a live one dropped | `(role, up, error)` |
| `transport.on_publish_outcome(cb)` | every publish handed to aio-pika (not `NOT_SENT`) | `(PublishOutcome, seconds)`: `seconds` is send to broker answer, `None` without one |
| `transport.on_blocked(cb)` / `on_unblocked(cb)` | RabbitMQ blocked / unblocked the connection | none |
| `transport.on_consumer_cancelled(cb)` / `on_consumer_resumed(cb)` | the broker cancelled a consumer / it consumes again | the queue name |

The broker wires all of these to `BrokerMetrics` when metrics are on. The
sync transport has `on_connection_state` (role `shared`), `on_reconnect_attempt`
(`(attempt, error)`), `on_publish_outcome`, `on_blocked` / `on_unblocked`,
`on_consumer_cancelled` / `on_consumer_resumed` (called on the I/O thread),
`cancelled_consumers` and `publishes_in_flight`.

Reconnect attempts back off exponentially with full jitter
(`reconnect_backoff_base` doubling to `reconnect_backoff_max`), unlimited by
default; `ConnectionConfig.reconnect_budget_attempts` /
`reconnect_budget_seconds` bound an outage. A terminal failure turns
`broker_health_check`, `broker_readiness` and `broker_liveness` false and is
logged at CRITICAL.

- `transport.is_connected()` follows aio-pika's `connected` event, so it is
  False while a `RobustConnection` reconnects (before 0.19 it read
  `is_closed`, which stays False during a reconnect, and a readiness probe
  kept a disconnected pod in rotation).
- Do not hang correctness-critical wiring off `on_reconnect` on async:
  `CoalescingAcker` does not need it, because a rebuilt channel is a new
  object and therefore gets a fresh ledger.

## Traces

`OTelTracingMiddleware` speaks the OpenTelemetry API directly
(`pip install rabbitkit[otel]`) and propagates W3C `traceparent` /
`tracestate` in the AMQP headers.

- **PRODUCER span** `<exchange> send` around `broker.publish()`; the current
  context is injected into a copy of the envelope's headers. The span records
  the returned `PublishOutcome`: `messaging.rabbitmq.publish.status` always,
  `messaging.rabbitmq.publish.reason` when set, and for every outcome that is
  not `ok` (`NOT_SENT`, `UNKNOWN`, `TIMEOUT`, `RETURNED`, `NACKED`, `ERROR`) an
  `ERROR` span status and `error.type` (the reason, or the status). A raised
  exception is recorded as before.
- **CONSUMER span** `<queue> receive` around the handler, from the context in
  the headers. `OTelTracingMiddleware(consumer_context="parent")` (default)
  makes the producer's span its parent: one trace from producer to consumer.
  `consumer_context="link"` starts the consumer span from the consumer's own
  current context (a new trace when there is none) with a span link to the
  producer, as the OpenTelemetry messaging conventions suggest when
  consumption is decoupled from production (batch processing, long queues,
  replays).
- **Retry and dead-letter copies** are published by `RetryMiddleware` with
  the failed message's headers, so they keep the ORIGINAL producer's trace
  context: every attempt's CONSUMER span belongs to the producer's trace (in
  `link` mode, links to it). They are internal publishes and do not get a
  PRODUCER span of their own.
- Verified on a real broker: a trace id published by one broker reaches the
  consumer's span through RabbitMQ
  (`tests/integration/test_observability_foundation.py`).

## Logs

- rabbitkit's log events never contain message bodies, raw headers or
  credentials. Per-message context is `message_id`, `routing_key`, `queue`,
  `handler`; failure lines add bounded fields (`category`, `reason`, publish
  `status`, connection `role`, `endpoint` as `host:port`).
- `RabbitConfig(logging=LoggingConfig(...))` makes the broker configure
  structlog at `start()`. If the application already configured structlog
  (for example through obskit), the broker now **keeps** that configuration
  and only attaches rabbitkit's value redaction to its own `rabbitkit.*`
  loggers; `LoggingConfig(override_existing=True)` restores the old
  replace-it behaviour. Calling `configure_structlog()` yourself still
  configures structlog globally (`override_existing=False` makes it keep an
  existing configuration).

## Alerts and runbooks

[`deploy/prometheus/rabbitkit-alerts.yaml`](https://github.com/talaatmagdyx/rabbitkit/blob/main/deploy/prometheus/rabbitkit-alerts.yaml)
is a plain Prometheus rules file (wrap its `groups:` in a `PrometheusRule`
for the Prometheus Operator). It is checked with `promtool check rules` and
behaviour-tested with `promtool test rules`
(`deploy/prometheus/tests/rabbitkit-alerts.test.yaml`, a positive and a
negative case per alert). Every threshold is a reasoned default and every
alert carries a one-line `rationale` annotation saying where its number
comes from; [Tuning the alerts](#tuning-the-alerts) says how to fit them to
your SLOs.

Every group except the last needs only rabbitkit's own metrics (plus a
`QueueMetricsPoller` for the queue rules). The alerts carry
`source: rabbitkit`.

| Alert | Fires on | Runbook |
|---|---|---|
| `PublisherConfirmFailures` | `messages_published_total{status=~"nacked\|error"}` above 0, for 5m | [publisher confirm failures](runbooks/publisher-confirm-timeout.md) |
| `PublisherUnknownOutcomes` | over 1% of publishes `unknown`/`timeout`, for 10m | [publisher confirm failures](runbooks/publisher-confirm-timeout.md) |
| `PublisherConfirmLatencyHigh` | confirm latency p99 over 250 ms, for 10m | [publisher confirm failures](runbooks/publisher-confirm-timeout.md) |
| `PublisherUnroutableMessages` | `publish_routing_failures_total` above 0, for 5m | [unroutable](runbooks/unroutable.md) |
| `RabbitMQBrokerUnavailable` | `broker_connected == 0` for 2m | [broker unavailable](runbooks/broker-unavailable.md) |
| `RabbitMQConnectionFailures` | over 10 failed reconnect attempts in 5m, for 5m | [connection storm](runbooks/connection-storm.md) |
| `RabbitMQConnectionTerminalFailure` | a terminal `connection_failures_total` reason | [connection storm](runbooks/connection-storm.md) |
| `RabbitMQDiskAlarm` | `connection_blocked_reason{reason="disk"}` for most of 2m, for 1m | [flow control](runbooks/flow-control.md) |
| `RabbitMQMemoryAlarm` | `connection_blocked_reason{reason="memory"}` for most of 2m, for 1m | [flow control](runbooks/flow-control.md) |
| `RabbitMQConnectionBlocked` | `connection_blocked` for most of 2m, for 1m, when not a disk or memory alarm | [flow control](runbooks/flow-control.md) |
| `ConsumerProcessingFailures` | over 5% of handler runs raise, for 10m (at least 1 run/min) | [consumer failures](runbooks/consumer-failures.md) |
| `ConsumerNotConsuming` | `route_consuming == 0` while connected, for 5m | [consumer failures](runbooks/consumer-failures.md) |
| `ConsumerBacklogGrowing` | backlog drain time over 10 min and growing, for 15m | [consumer backlog](runbooks/consumer-backlog.md) |
| `ConsumerUnackedHigh` | unacked deliveries while no handler finishes, for 10m | [consumer backlog](runbooks/consumer-backlog.md) |
| `DeadLetterQueueGrowing` | `messages_dead_lettered_total` or a `*.dlq` backlog rising, for 5m | [DLQ growth](runbooks/dlq-growth.md) |
| `RetryStormDetected` | over 0.5 retries per handler run (at least 1 run/min), or `retry_handoff_paused == 1`, for 10m | [retry storm](runbooks/retry-storm.md) |
| `RabbitMQQuorumMajorityLost` | `queue_quorum_online <= queue_quorum_members / 2`, for 1m | [quorum unavailable](runbooks/quorum-unavailable.md) |
| `RabbitMQQuorumUnhealthy` | `queue_quorum_online < queue_quorum_members` (majority kept), for 5m | [quorum unavailable](runbooks/quorum-unavailable.md) |

The `rabbitkit-recording` group records the ratios the alerts compare
(`job_exchange:rabbitkit_publish_unknown_ratio:rate5m`,
`job_exchange:rabbitkit_publish_confirm_latency_seconds:p99_5m`,
`job_queue:rabbitkit_consume_error_ratio:rate5m`,
`job_queue:rabbitkit_retry_ratio:rate5m`, ...); they are also the series to
put on a dashboard.

**Alternative: the broker's `rabbitmq_prometheus` plugin.** The last group,
`rabbitmq-broker-plugin`, has `RabbitMQDiskAlarm`, `RabbitMQMemoryAlarm` and
a cluster-level `RabbitMQQuorumUnhealthy` on the plugin's own metrics, with
`source: rabbitmq_prometheus`. Load it only when Prometheus scrapes the
plugin; it sees an alarm before any client publishes, the client-side rules
see what actually blocks your services. With both loaded, route or inhibit
on `source`. Verify the metric names against your RabbitMQ version.

## Tuning the alerts

The defaults come from three places: the documented contract (a `NACKED`
publish, an unroutable publish and a dead-lettered message never happen in a
healthy system, so their threshold is any), `ConnectionConfig` defaults
(`reconnect_backoff_base=1`, `reconnect_backoff_max=30`,
`blocked_connection_timeout=60`, `heartbeat=30`) and the measured numbers in
[benchmarking](benchmarking.md) and
[reliable messaging](reliable-messaging.md#verified-failure-scenarios)
(quorum end-to-end p99 27.5 ms at 2,778 msg/s, a 200,000-message burst
drained in 5–7.5 s, quorum loss recovered 10–18 s after the nodes return,
silent-partition detection about 5 × `heartbeat`). Those numbers come from
a single-node broker on one machine; on a 3-node cluster sharing that
machine the confirm p99 was 185 ms and the end-to-end p99 800 ms (see
`docs/benchmarking.md`). Measure your own cluster before paging on anything
tighter.

Most thresholds are ratios or times, so they do not depend on traffic.
What to change, and how:

| Knob | Default | Derive it from |
|---|---|---|
| `PublisherUnknownOutcomes` ratio | 1% | Your publish success SLO, as a burn rate. For "99.9% of publishes confirmed" over 30 days, a ratio above `14.4 × 0.001` held for an hour spends 2% of the month's error budget: the usual fast-burn page. |
| `PublisherConfirmLatencyHigh` p99 | 250 ms | Your publish latency SLO, and at most a fraction of `PublisherConfig.confirm_timeout` (5 s): when p99 nears the timeout, publishes start resolving `TIMEOUT`. Multi-node quorum clusters add Raft round trips; measure p99 there first. |
| `RabbitMQBrokerUnavailable` `for:` | 2m | Your availability SLO's tolerance for a broker restart. Keep it above the time a restart takes to recover (backoff caps at `reconnect_backoff_max`); a silent partition is only detected after about 5 × `heartbeat`, before this timer starts. |
| `RabbitMQConnectionFailures` count | > 10 per 5m | `reconnect_backoff_base` / `reconnect_backoff_max`: full-jitter backoff averages `min(max, base × 2ⁿ) / 2` per attempt, about 8 attempts in the first 60 s with the defaults and 60 / (max / 2) a minute after that. Set the count above what one restart costs. |
| Blocked / alarm alerts | most of 2m, for 1m | `blocked_connection_timeout`: keep the averaging window above it, because the forced reconnect it triggers unblocks the gauge until the next publish. |
| `ConsumerProcessingFailures` ratio | 5% | The handler error budget; each failure is re-run by retries, so the ratio is also extra load. |
| `ConsumerBacklogGrowing` drain time | 600 s | Your end-to-end latency SLO: a backlog that takes longer than the SLO to drain is already violating it. |
| `ConsumerUnackedHigh` window | 10m | Longer than your slowest legitimate handler run (and its timeout). |
| `RetryStormDetected` ratio | 0.5 retries per run | `RetryConfig.max_retries`: with N retries a permanently failing input produces up to N retries per new message, so lower the ratio for a large N. |
| `RabbitMQQuorumUnhealthy` `for:` | 5m | How long one node of a rolling restart takes to rejoin. |
| Minimum traffic guards | 1 handler run/min, 0.1 confirms/s | Below these a ratio or a p99 rests on a handful of samples. Lower them for low-volume queues, or alert on counts instead. |

Example: a service with a "95% of messages processed within 30 s" objective
sets the drain-time threshold to 30 and `for: 5m`, and keeps the rest.
Another that publishes 5 messages an hour drops both minimum-traffic guards
and alerts on `increase(...[1h]) > 0` for the error counters instead of
ratios.

## High-cardinality routing keys / queue names

Every `queue` label above uses the bound queue name specifically to avoid
cardinality blowups from routing keys (see [Bounded labels](#bounded-labels)).
The same caution applies one level up, at topology-creation time: if your
application dynamically declares a **new queue per request/session/tenant**
(rather than a small, fixed set of queues bound at startup), every metric
above that carries a `queue` label — plus `QueueMetricsPoller`'s gauges —
creates one new time series per queue, forever (Prometheus/StatsD backends
don't reclaim series for queues that no longer exist without external
cleanup). Prefer a bounded, small number of long-lived queues with a
routing-key or header dimension for per-tenant/per-session distinction
instead — see [`docs/production/scale.md`](production/scale.md) for the
throughput tradeoffs of that shape.

## See also

- [`docs/kubernetes.md`](kubernetes.md) — wiring `/metrics` behind a
  `ServiceMonitor`/`PodMonitor`, and how these signals relate to
  liveness/readiness probes.
- [`docs/production/checklist.md`](production/checklist.md) — the
  production readiness checklist this page backs.
- [`docs/rabbitmq-retry-architecture.md`](rabbitmq-retry-architecture.md) —
  how the retry/DLQ metrics fit into the broader retry design.
- [Runbooks](runbooks/index.md).
