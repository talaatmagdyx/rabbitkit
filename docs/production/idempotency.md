# The idempotency contract

**rabbitkit's "safe retries" promise means retries and dead-lettering are
implemented correctly — it does not mean your handler can safely have
side effects that aren't idempotent.** This page explains exactly where
that line is, because it's easy to assume more than rabbitkit actually
guarantees.

## The short version

RabbitMQ (and therefore rabbitkit) gives you **at-least-once delivery**.
That means a message can be delivered — and your handler can run — more
than once for the same logical event. This is not a bug and not something
rabbitkit can remove; it's an inherent property of any messaging system
that refuses to silently drop messages on a crash or a lost acknowledgement.

**Your handler must be safe to run more than once with the same input.**
If it charges a card, sends an email, or writes a non-idempotent database
row, rabbitkit's retry/DLQ correctness does not protect you from a
duplicate side effect.

## Where this actually bites, concretely

### 1. A successful handler, then a failed result-publish

```python
@broker.publisher(exchange="notifications", routing_key="order.confirmed")
@broker.subscriber(queue="orders")
def handle_order(body: bytes) -> bytes:
    charge_card(body)                 # (1) side effect: real, already happened
    return b'{"status": "confirmed"}' # (2) published to "notifications"
```

If step (1) succeeds but the publish of the return value in step (2) fails
(a transient broker hiccup, a full channel pool, a connection drop), the
*source* message is **not** acked — it's nacked and requeued, exactly as
you'd want for at-least-once safety around the publish. But that means
`handle_order` runs again on redelivery, and **`charge_card(body)` runs a
second time.**

This is not a theoretical edge case — it's the direct, structural
consequence of "don't ack a message whose result we couldn't confirm was
published." rabbitkit escalates this from a quiet `WARNING` to a loud
`ERROR` in the logs when it detects the message has already been
redelivered once and is still failing to publish its result, specifically
so a sustained outage doesn't hide in routine-looking log noise — but the
handler still reruns. There is no way to fix this generically inside the
library, because only your handler knows what "already done" means for
`charge_card`.

### 2. Ordinary redelivery after a crash or a lost ack

Even with no result-publish involved: if your process crashes (or is
`SIGKILL`ed by Kubernetes past `terminationGracePeriodSeconds`) *after* a
handler's side effect completes but *before* the ack reaches the broker,
RabbitMQ has no way to know the work was done — it redelivers the message.
Same outcome: your handler runs again.

### 3. Retry after a transient failure

This is the case `RetryConfig` is explicitly for, and it's usually fine —
but only if the handler is written to be re-run safely. A handler that
partially completes (e.g. it debited an internal ledger, then failed before
crediting the destination) and gets retried from the top can leave your
data in a state neither "not done" nor "done" describes correctly.

## What "idempotent" means here, precisely

Your handler is idempotent (for rabbitkit's purposes) if:

> Running it twice, three times, or a hundred times with the same message
> body produces the same *observable outcome* as running it once — no
> duplicate charges, no duplicate rows, no duplicate emails, no
> double-decremented inventory.

It does **not** mean the handler must be free of side effects. It means the
side effects must be safe to repeat.

## How to make a handler idempotent

Pick whichever of these fits the side effect:

- **Idempotency keys.** Generate (or receive) a stable key per logical
  operation (e.g. `order_id`, or a UUID you control) and have the
  downstream system (payment processor, database, email provider) dedupe
  on that key. Most payment APIs support this natively (Stripe's
  `Idempotency-Key` header, for example) — use it.
- **Upsert instead of insert.** `INSERT ... ON CONFLICT DO NOTHING` /
  `ON CONFLICT DO UPDATE` instead of a bare `INSERT` that fails or
  duplicates on a second attempt.
- **Check-then-act inside a transaction.** Look up whether the operation
  already happened (by the same key) before performing it, inside the same
  transaction that records it as done, so there's no window for a
  concurrent duplicate.
- **A dedup table / `DeduplicationMiddleware`.** rabbitkit ships a
  Redis-backed `DeduplicationMiddleware` that skips a message it's already
  seen (by `message_id`, `correlation_id`, or a body hash) within a TTL
  window. This *reduces* duplicate processing — it does not eliminate it
  (Redis can evict, lag, or be temporarily down; `fallback_on_redis_error`
  defaults to *processing anyway* rather than blocking on a Redis outage).
  Treat it as a second layer, not the source of truth. Application-level
  idempotency (the previous three bullets) is the real guarantee.
- **Publish before the irreversible side effect, not after.** If you have
  the choice, structure the handler as "record intent" → "check if intent
  was already fulfilled" → "fulfill" → "record fulfillment," so a crash at
  any point leaves you with an idempotency key to check against on
  redelivery, instead of an ambiguous partial state.
- **A transactional outbox**, for the strictest cases: write the side
  effect and the "I did this" record in the same local database
  transaction, and have a separate process publish from the outbox. This
  is real architectural weight — reach for it only when the two options
  above genuinely aren't enough (e.g. no idempotency-key support
  downstream, and the side effect is high-value).

## What rabbitkit does and doesn't do for you

| | rabbitkit's behavior |
|---|---|
| A transient handler exception | Classified, retried with backoff via the delay-queue topology, dead-lettered on exhaustion. Correct and tested end-to-end. |
| A spoofed/corrupted retry-count header | Clamped to `[0, max_retries]` — can't be used to force infinite retries or skip to the DLQ early. |
| A failed result-publish after a successful handler | Nacks and requeues the *source* message (never falsely acks a lost result) — but that means the handler reruns. Escalates to `ERROR` logging on a repeat failure so it's not silent. **Does not make your side effect idempotent for you.** |
| A crash between side effect and ack | Redelivered, like any at-least-once system. Same requirement applies. |
| Deduplication | `DeduplicationMiddleware` reduces duplicate processing within a TTL window. It is a mitigation, not a substitute for idempotent handlers. |

## The one-sentence version to remember

**If your handler's side effect can't be repeated safely, at-least-once
delivery — which is what "safe retries" is built on — will eventually
repeat it. Design for that, don't fight it.**
