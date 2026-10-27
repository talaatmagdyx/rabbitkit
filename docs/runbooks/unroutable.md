# Unroutable publishes

**Alert:** `PublisherUnroutableMessages` — `rabbitkit_publish_routing_failures_total` rising.

## What it means

RabbitMQ could not route a publish:

- `reason="returned"`: a `mandatory=True` publish matched no binding; the
  broker returned it (`312 NO_ROUTE`). The message was not stored anywhere.
- `reason="exchange_not_found"`: the exchange does not exist (`404`); the
  broker closed the channel and the message was not stored.

The counter covers every transport publish, including retry and dead-letter
copies, so a missing delay queue binding or DLQ shows up here too. A
non-mandatory publish that matches nothing is dropped silently by RabbitMQ
and cannot be counted by the client; use `mandatory=True` (or an alternate
exchange) where losing it matters.

## Triage

1. `sum by (exchange, reason) (rate(rabbitkit_publish_routing_failures_total[5m]))`.
2. Check the topology: `rabbitmqctl list_bindings source_name destination_name routing_key`
   and `rabbitmqctl list_exchanges name`. Was a queue or binding deleted, or
   did a deploy change a routing key?
3. Retry/DLQ exchanges failing: the retry topology was not declared (or was
   deleted); see [retry storm](retry-storm.md).
4. Logs: WARNING `Publish returned as unroutable ... exchange=... routing_key=...`.

## Mitigation

- Restore the missing binding/queue/exchange (re-deploying the owning
  service re-declares it under `TopologyMode.AUTO_DECLARE`).
- Returned messages are reported as `RETURNED` to the caller, never as
  success: the producer decides whether to republish after the fix.
