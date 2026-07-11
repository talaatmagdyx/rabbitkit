# Observability reference

The single source of truth for every metric rabbitkit actually emits — name,
labels, meaning, and where it comes from. This page exists because
`MetricsConfig` defines a few more properties than are wired to an actual
emission point (see [Defined but not emitted](#defined-but-not-emitted)
below) — checking source before building a dashboard/alert on a name is
easy to get wrong otherwise.

All names below use the default `namespace="rabbitkit"` (`MetricsConfig.namespace`);
a custom namespace replaces the `rabbitkit_` prefix. Nothing here is emitted
unless you construct a collector and pass it to `MetricsMiddleware` (or
`QueueMetricsPoller` for the queue-depth gauges) — rabbitkit never talks to
Prometheus/StatsD on its own.

```python
from rabbitkit.middleware.metrics import MetricsMiddleware, PrometheusCollector

collector = PrometheusCollector()  # requires: pip install prometheus-client
broker = SyncBroker(middlewares=[MetricsMiddleware(collector)])
```

## Consume-side

Emitted by `MetricsMiddleware.consume_scope`/`consume_scope_async`, which wrap
handler execution — and by `HandlerPipeline`, which calls
`MetricsMiddleware.record_settlement()` once a message's final disposition
(ack/nack/reject) is known (settlement happens *after* the wrapped handler
call returns, so `MetricsMiddleware` itself can't see it directly).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rabbitkit_messages_consumed_total` | Counter | `queue`, `status` (`success`\|`error`) | Handler ran without raising vs. raised. |
| `rabbitkit_message_processing_seconds` | Histogram | `queue` | Handler execution duration. |
| `rabbitkit_messages_acked_total` | Counter | `queue` | Message settled with `ack`. |
| `rabbitkit_messages_nacked_total` | Counter | `queue` | Message settled with `nack`. |
| `rabbitkit_messages_rejected_total` | Counter | `queue` | Message settled with `reject`. |
| `rabbitkit_messages_redelivered_total` | Counter | `queue` | Broker flagged the delivery `redelivered=True`. A sustained rise means handlers are dying/timing out before acking (crash loops, heartbeat kills, connection churn) — the ack/nack/reject counters alone can't distinguish that from normal traffic. |

The `queue` label is always the **bound queue name**
(`x-rabbitkit-original-queue`, set by the broker before any middleware
runs), never the raw routing key — a topic/`Path()` routing key can embed
an unbounded per-message value (tenant id, order id, ...), which would
otherwise explode your metrics backend's cardinality. See
[High-cardinality routing keys](#high-cardinality-routing-keys-queue-names)
below for the same caution applied to dynamically-created queues.

## Publish-side

Emitted by `MetricsMiddleware.publish_scope`/`publish_scope_async`, wrapping
`broker.publish()`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rabbitkit_messages_published_total` | Counter | `exchange`, `status` | The real `PublishOutcome.status` value: `confirmed`\|`sent`\|`nacked`\|`timeout`\|`returned`\|`error`. A raised exception escaping the publish call itself is also labeled `error`. Before 0.11.0 this label was hardcoded to `success`/`error` based on whether the call *raised* — since `broker.publish()` never raises for NACKED/TIMEOUT/RETURNED/ERROR (it returns them as a `PublishOutcome` instead), every one of those was miscounted as `success`. If your dashboards predate 0.11.0, re-check any alert built on this metric's `status` label. |
| `rabbitkit_message_publish_seconds` | Histogram | `exchange` | Publish call duration (including the confirm wait, when `confirm_delivery=True`). |

For "why did this specific publish fail," see `PublishOutcome.classification`
(a `ClassifiedError` with `severity`/`reason`, populated via the same
`classify_error()` the consume/retry path uses) rather than trying to infer
it from the `status` label alone.

## Connection & channel churn

Emitted directly by the broker (`SyncBroker`/`AsyncBroker`, not
`MetricsMiddleware`) via callbacks registered on the transport at `start()` —
wired to the collector of the first route carrying a `MetricsMiddleware`, so
these are a no-op if no route has one. No labels on any of these three.

| Metric | Type | Meaning |
|---|---|---|
| `rabbitkit_reconnects_total` | Counter | Every re-connection *after* the first successful connect. Reconnects were previously logged but never counted, so a flapping broker/network was invisible to metrics-based alerting. |
| `rabbitkit_channels_opened_total` | Counter | Every new low-level channel either transport creates — the publisher/topology channel, a per-queue consumer channel, an async channel-pool slot, or a dedicated fast/mandatory/reply-to channel. A steady climb with no matching traffic growth signals a channel leak. |
| `rabbitkit_channel_rebuilds_total` | Counter | The subset of `channels_opened_total` that **replaces** a channel lost to a reconnect, a 406 topology-drift close, or a mandatory/fast-channel recycle — as opposed to an ordinary first-ever open or async channel-pool growth. Isolates "something upstream actually failed" from routine churn. |

Caveat shared by all three: the very first channel/connection opened during a
broker's initial `start()` predates the metrics wiring (the collector is
only discovered *after* the transport has already connected), so these track
churn from "the broker finished starting" onward — not a from-zero census.

## Retry / DLQ / dedup / rate-limit

| Metric | Type | Labels | Emitted by | Meaning |
|---|---|---|---|---|
| `rabbitkit_messages_retried_total` | Counter | `queue` | `RetryMiddleware` | A transient failure was routed to a delay queue for retry. |
| `rabbitkit_messages_dead_lettered_total` | Counter | `queue` | `RetryMiddleware` | A message was committed to being dead-lettered — either a permanent error (first attempt) or retries exhausted. Recorded at the decision point, not at the eventual `reject()` call, so it's known *why* even though the actual reject happens later in the pipeline. |
| `rabbitkit_dedup_fallback_total` | Counter | `queue` | `DeduplicationMiddleware` | A Redis error forced processing to continue WITHOUT idempotency enforcement for that message (`DeduplicationConfig.fallback_on_redis_error`). Treat any non-zero rate as "idempotency was not guaranteed here" and alert accordingly. |
| `rabbitkit_rate_limit_dropped_total` | Counter | `reason` (`nack`\|`drop`\|`wait_deadline_exceeded`) | `RateLimitMiddleware` | A message was settled without the handler running, because of rate-limit backpressure. |

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

## Defined but not emitted

`MetricsConfig` also defines `publish_total` (a differently-named alias —
`published_total`/`MESSAGES_PUBLISHED_TOTAL` is the one actually used),
`publish_failures_total`, `publish_confirm_latency_seconds`,
`in_flight_messages`, `worker_pool_pending`, `broker_connected`, and
`consumer_active`. None of these currently have an emission site anywhere
in rabbitkit — they resolve to a name string like every other property, but
nothing ever calls `inc_counter`/`observe_histogram`/`set_gauge` for them.
Don't build a dashboard panel or alert on any of these; if you need the
signal, wire your own via a custom middleware in the meantime, or open an
issue.

## High-cardinality routing keys / queue names

Every `queue` label above uses the bound queue name specifically to avoid
cardinality blowups from routing keys (see [Consume-side](#consume-side)).
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
