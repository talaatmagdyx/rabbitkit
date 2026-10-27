# DLQ growth

**Alert:** `DeadLetterQueueGrowing` — `rabbitkit_messages_dead_lettered_total`
rising, or a `*.dlq` queue's ready count growing.

## What it means

Messages failed terminally (`permanent`, `invalid_message`, retries
exhausted, retry age exceeded) and were copied to the dead-letter queue
(`<queue>.dlq`, or `rabbitkit.dlq` in shared mode) with failure metadata
headers (`x-rabbitkit-error-category`, `x-rabbitkit-failure-category`, ...).
Nothing is lost, but every message needs a decision.

## Triage

1. `sum by (queue, category) (increase(rabbitkit_messages_dead_lettered_total[1h]))`.
2. Inspect without consuming: `rabbitkit dlq inspect <queue>.dlq` (or
   `DLQInspector`), group by `x-rabbitkit-error-category` and error code.
3. One category dominating after a deploy: a contract or code regression.

## Mitigation

- Fix the cause, then replay: `rabbitkit dlq replay <queue>.dlq --actor <you>`
  (replays are audited; `--require-actor` enforces a named actor).
- Messages that can never succeed: export them for the data owner, then purge
  with an explicit actor.
- Never replay before fixing the cause: it only refills the DLQ.
