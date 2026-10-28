# Consumer backlog

**Alerts:**

- `ConsumerBacklogGrowing` — the ready backlog would take more than 10
  minutes to drain at the current delivery rate
  (`rabbitkit_queue_messages_ready / rate(rabbitkit_messages_consumed_total)`),
  and it has grown for 15 minutes. A drain time, not a depth, so the same
  threshold fits a 10 msg/s queue and a 10,000 msg/s one; a queue that
  delivers nothing has an infinite drain time.
- `ConsumerUnackedHigh` — `rabbitkit_queue_messages_unacked > 0` while no
  handler run on the queue has finished for 10 minutes: handlers are stuck.

Both need a `QueueMetricsPoller` (they are broker-side numbers read through the
management API) and, for the delivery rate, a `MetricsMiddleware` on the
consuming routes.

## What it means

- Backlog growing: producers are faster than consumers, or consumers are not
  consuming (`rabbitkit_queue_consumers == 0`, `rabbitkit_route_consuming == 0`).
- Unacked with no completions: deliveries sit at consumers and nothing
  finishes: a deadlock, or a downstream call with no timeout. Unacked
  messages are redelivered when a consumer's channel closes. (A high but
  turning-over unacked count is normal: it tops out at prefetch × consumers.)

## Triage

1. Consumers attached? `rabbitkit_queue_consumers{queue="..."}` and
   `rabbitkit_route_consuming`. Zero: see [consumer failures](consumer-failures.md).
2. Throughput: `sum by (queue) (rate(rabbitkit_messages_consumed_total[5m]))`
   vs the publish rate; handler time:
   `histogram_quantile(0.99, sum by (le, queue) (rate(rabbitkit_message_processing_seconds_bucket[5m])))`.
3. Stuck handlers: `rabbitkit_in_flight_messages`, `rabbitkit_worker_pool_pending`,
   `rabbitkit_settlement_oldest_pending_age_seconds`.
4. Retries feeding the queue: `rabbitkit_messages_retried_total` (see
   [retry storm](retry-storm.md)).

## Mitigation

- Scale consumers (more replicas or `WorkerConfig.worker_count`) if handlers
  are healthy but too slow.
- Fix the slow dependency if handler time rose.
- Lower `prefetch_count` if unacked is high while consumers are idle.
- Do not purge a backlog without a decision from the data owner.
