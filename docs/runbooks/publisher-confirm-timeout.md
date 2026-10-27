# Publisher confirm failures and unknown outcomes

**Alerts:**

- `PublisherConfirmFailures` — `messages_published_total{status=~"nacked|error"}` rising.
- `PublisherUnknownOutcomes` — `messages_published_total{status=~"unknown|timeout"}` rising.

## What it means

| Status | Meaning | Safe to publish again? |
|---|---|---|
| `nacked` | RabbitMQ refused the message (internal error, overflow `reject-publish`) | yes |
| `error` | refused (`exchange_not_found`, `access_refused`, `precondition_failed`), invalid envelope, or unclassified | after fixing the cause |
| `timeout` | no confirm within `confirm_timeout` / the per-call timeout | only with the same `message_id` |
| `unknown` | the connection or channel was lost after the frame left | only with the same `message_id` |

`reason` on the same counter is the bounded `PublishReason` code. A `TIMEOUT`
or `UNKNOWN` message may be on the broker: republishing without the same
`message_id` can duplicate it; not republishing can lose it.

## Triage

1. `sum by (exchange, status, reason) (rate(rabbitkit_messages_published_total[5m]))`.
2. Confirm latency: `histogram_quantile(0.99, sum by (le, exchange) (rate(rabbitkit_publish_confirm_latency_seconds_bucket[5m])))`.
   It is timed from handing the frame to the client library to the broker's
   answer, so a rise is the broker (disk fsync on quorum/durable queues,
   overload) or the network, not rabbitkit's admission.
3. Correlate with `rabbitkit_connection_blocked`, `rabbitkit_connection_failures_total`
   and the broker's queue length limits (`x-overflow: reject-publish` nacks).
4. `nacked` with `reject-publish`: the target queue is full; see
   [consumer backlog](consumer-backlog.md).

## Mitigation

- Make sure producers republish `UNKNOWN`/`TIMEOUT` outcomes with the same
  `message_id` and consumers deduplicate (see `docs/production/idempotency.md`).
- Raise `PublisherConfig.confirm_timeout` only if measured confirm
  latency legitimately exceeds it.
- Persistent `error` with `access_refused`: fix the user's permissions;
  `precondition_failed`: the publish properties conflict with the queue.
