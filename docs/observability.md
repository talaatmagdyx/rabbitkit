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
broker's I/O loop, throttled to once per 5 seconds). Connection state also
updates the moment a connection drops or comes back.

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
| `rabbitkit_publish_in_flight` | gauge | — | broker (BrokerMetrics) | Async publishes between admission and outcome (sampled). |
| `rabbitkit_publish_in_flight_bytes` | gauge | — | broker (BrokerMetrics) | Bytes held by max_in_flight_bytes (sampled; only when set). |
| `rabbitkit_publish_channels_in_use` | gauge | — | broker (BrokerMetrics) | Async publisher channels checked out (sampled). |
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
| `reason` | `PublishReason` codes, plus `none` (no reason: a success), `exception` (the publish call raised) and `other` (a reason outside `PublishReason`); connection failure reasons (below); bulk reason codes; rate-limit reasons |
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
- **`publish_in_flight`** counts async `publish()` calls between admission
  and their outcome (batch-publisher items are not included).
  **`publish_in_flight_bytes`** is the `PublisherConfig.max_in_flight_bytes`
  budget in use (publish and batch); it is only emitted when that budget is
  set.
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
- **`consumer_cancelled_total` / `route_consuming`**: RabbitMQ cancelled a
  consumer (`basic.cancel`); the async broker re-subscribes when the queue
  exists again. The sync broker samples `route_consuming` but cannot detect a
  broker-initiated cancel.
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

```python
from rabbitkit import RabbitManagementClient, QueueMetricsPoller

poller = QueueMetricsPoller(
    management_client=RabbitManagementClient(...),
    collector=collector,   # same MetricsCollector as MetricsMiddleware
    interval=15.0,
)
poller.start()  # background daemon thread (sync); poller.start_async() for async brokers
```

Alert on `queue_messages_ready` growth and `queue_consumers == 0` — those are
exactly the signals the in-process counters above cannot provide on their own.

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
(`(attempt, error)`), `on_publish_outcome`, `on_blocked` / `on_unblocked`.

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
(`deploy/prometheus/tests/rabbitkit-alerts.test.yaml`). **Every threshold is a
placeholder** to be tuned from measured workloads.

| Alert | Fires on | Runbook |
|---|---|---|
| `PublisherConfirmFailures` | `messages_published_total{status=~"nacked\|error"}` rising | [publisher confirm failures](runbooks/publisher-confirm-timeout.md) |
| `PublisherUnknownOutcomes` | `messages_published_total{status=~"unknown\|timeout"}` rising | [publisher confirm failures](runbooks/publisher-confirm-timeout.md) |
| `PublisherUnroutableMessages` | `publish_routing_failures_total` rising | [unroutable](runbooks/unroutable.md) |
| `RabbitMQBrokerUnavailable` | `broker_connected == 0` for 2m | [broker unavailable](runbooks/broker-unavailable.md) |
| `RabbitMQConnectionFailures` | > 5 failed reconnect attempts in 10m, for 10m | [connection storm](runbooks/connection-storm.md) |
| `RabbitMQConnectionTerminalFailure` | a terminal `connection_failures_total` reason | [connection storm](runbooks/connection-storm.md) |
| `RabbitMQConnectionBlocked` | `connection_blocked == 1` for 1m | [flow control](runbooks/flow-control.md) |
| `ConsumerProcessingFailures` | > 5% of handler runs raise, for 10m | [consumer failures](runbooks/consumer-failures.md) |
| `ConsumerNotConsuming` | `route_consuming == 0` while connected, for 5m | [consumer failures](runbooks/consumer-failures.md) |
| `ConsumerBacklogGrowing` | `queue_messages_ready` > 1000 and growing, for 15m | [consumer backlog](runbooks/consumer-backlog.md) |
| `ConsumerUnackedHigh` | `queue_messages_unacked` > 5000 for 15m | [consumer backlog](runbooks/consumer-backlog.md) |
| `DeadLetterQueueGrowing` | `messages_dead_lettered_total` or a `*.dlq` backlog rising | [DLQ growth](runbooks/dlq-growth.md) |
| `RetryStormDetected` | retried / consumed > 50%, or `retry_handoff_paused == 1` | [retry storm](runbooks/retry-storm.md) |
| `RabbitMQDiskAlarm` | broker metric `rabbitmq_alarms_free_disk_space_watermark == 1` | [flow control](runbooks/flow-control.md) |
| `RabbitMQMemoryAlarm` | broker metric `rabbitmq_alarms_memory_used_watermark == 1` | [flow control](runbooks/flow-control.md) |
| `RabbitMQQuorumUnhealthy` | broker metrics: fewer nodes than expected, or Raft term churn | [quorum unavailable](runbooks/quorum-unavailable.md) |

The last three use the **broker's** `rabbitmq_prometheus` plugin metrics, not
rabbitkit's: a client only sees `connection.blocked`, not which alarm caused
it, and cannot see Raft state. Verify those metric names against your
RabbitMQ version.

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
