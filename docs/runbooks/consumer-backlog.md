# Consumer backlog

**Alerts:**

- `ConsumerBacklogGrowing` — `rabbitkit_queue_messages_ready` high and growing.
- `ConsumerUnackedHigh` — `rabbitkit_queue_messages_unacked` high.

Both need a `QueueMetricsPoller` (they are broker-side numbers read through the
management API).

## What it means

- Ready growing: producers are faster than consumers, or consumers are not
  consuming (`rabbitkit_queue_consumers == 0`, `rabbitkit_route_consuming == 0`).
- Unacked high: deliveries sit at consumers without an ack: handlers are slow
  or stuck, or prefetch × consumers is larger than the handler throughput
  needs. Unacked messages are redelivered when a consumer's channel closes.

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
