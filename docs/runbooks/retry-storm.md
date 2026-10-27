# Retry storm

**Alert:** `RetryStormDetected` — more than half of the handler runs on a
queue are retried, or `rabbitkit_retry_handoff_paused == 1`.

## What it means

- A high retry ratio means most messages fail transiently: a dependency is
  down or overloaded. Retries add load to it.
- `retry_handoff_paused == 1`: the retry publish to the delay queue fails
  (`rabbitkit_retry_handoff_failures_total`), so the handoff backs off and
  messages are requeued on the source queue instead of moving forward.

## Triage

1. `sum by (queue, category) (rate(rabbitkit_messages_retried_total[5m]))`:
   `infrastructure` / `retryable` / `unknown_outcome`.
2. Handoff failures: `rate(rabbitkit_retry_handoff_failures_total[5m])` and
   `rabbitkit_publish_routing_failures_total` for the retry exchanges; check
   the delay queues exist (`rabbitmqctl list_queues name | grep retry`).
3. The retry delay queues' depth (`rabbitkit_queue_messages_ready`).

## Mitigation

- Fix or shed load from the failing dependency; consider pausing consumers.
- Retry topology missing: re-deploy the owning service so it re-declares it.
- Do not raise `max_retries` during a storm: it multiplies the load.
