# Consumer failures

**Alerts:**

- `ConsumerProcessingFailures` — more than 5% of handler runs raise.
- `ConsumerNotConsuming` — a route's consumer is not subscribed while the broker is connected.

## What it means

`rabbitkit_messages_consumed_total{status="error"}` counts handler runs that
raised. `rabbitkit_messages_failed_total{queue,category}` says why, using the
failure taxonomy:

| category | meaning | what rabbitkit does |
|---|---|---|
| `retryable` | transient (timeouts, connection errors) | retried |
| `infrastructure` | a dependency or the AMQP client is unavailable | retried |
| `unknown_outcome` | the processing deadline expired; side effects may have happened | retried (handlers must be idempotent) |
| `permanent` | a retry cannot succeed (bug, business rule) | dead-lettered |
| `invalid_message` | undecodable or invalid message | dead-lettered |
| `shutdown` | cancelled by shutdown | requeued |

`ConsumerNotConsuming`: `rabbitkit_route_consuming == 0`. Usually RabbitMQ
cancelled the consumer (`basic.cancel`: queue deleted, quorum leader moved,
operator action); `rabbitkit_consumer_cancelled_total{queue}` counts those.
The async broker re-subscribes once the queue exists again; it never
re-creates the queue.

## Triage

1. `sum by (queue, category) (rate(rabbitkit_messages_failed_total[5m]))`.
2. Logs: `Handler failed ... category=... reason=...` (no payloads).
3. `permanent` / `invalid_message` spike after a deploy: a producer or a
   consumer changed the message contract.
4. `infrastructure` / `retryable`: a downstream dependency is failing.
5. Not consuming: does the queue exist (`rabbitmqctl list_queues name`)? Was
   it deleted, or is the quorum queue without a leader
   ([quorum unavailable](quorum-unavailable.md))?

## Mitigation

- Roll back a bad deploy; dead-lettered messages can be replayed after the
  fix ([DLQ growth](dlq-growth.md)).
- Dependency outage: retries are bounded by `max_retries`; consider pausing
  the consumer instead of burning retries.
- Deleted queue: re-create it (re-deploy the owner); the consumer resumes on
  its own.
