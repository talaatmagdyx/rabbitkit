# two_stage_chain — receive → operate → publish on; next queue: operate → ACK

```
seed ──> readings.raw ──> convert() ──> readings.celsius ──> store() ──> ack
         [stage 1: F→C + anomaly flag]  [stage 2: MANUAL ack after persist]
```

One self-contained script (`chain.py`): seeds 10 sensor readings, runs both
stages on one `SyncBroker`, verifies the result, exits.

**The two lessons this example isolates:**

1. **Stage 1 — forward safely.** `@publisher` publishes the handler's return
   value to the next queue *with confirms*, and the source message is acked
   only after that publish is broker-confirmed. A lost forward nack-requeues
   the source; nothing silently vanishes between queues. Transient failures
   in the operation walk a 2s/10s/30s retry ladder, then dead-letter with
   triage headers.

2. **Stage 2 — `AckPolicy.MANUAL`: tie the ack to the side effect.** The
   message is acked by your code, only after the operation (persisting to a
   store) actually succeeds:
   - store briefly down → `msg.nack(requeue=True)` → redelivered, retried
   - malformed payload → `msg.reject(requeue=False)` → dead-lettered
   - stored durably → `msg.ack()`

   The script proves it: one reading's first store attempt fails on purpose,
   gets nacked, is redelivered, stores, and is acked — the final store holds
   every reading exactly once (the store write is keyed, so a redelivered
   duplicate overwrites instead of double-inserting).

**Production decisions carried (explicitly, not by default-reliance):**

| Decision | Why |
|----------|-----|
| `PublisherConfig(confirm_delivery=True, persistent=True)` | Every publish resolves only on broker ack; messages survive a restart. (These are rabbitkit's defaults — set explicitly so the choice is visible.) |
| `mandatory=True` on seeds + `assert outcome.ok` | `publish()` never raises on delivery failure — you branch on the `PublishOutcome`; unroutable → `RETURNED`, never a false confirm |
| Quorum queues, `delivery_limit=6` | Replicated, and the **broker-enforced** cap on stage 2's nack/requeue loop — without it, a permanently-down store would redeliver forever |
| Per-route `retry=` on stage 1 only | Stage 2 has **no retry middleware on purpose**: under MANUAL ack your code owns settlement; its transient path is the explicit nack+requeue, bounded by `delivery_limit` |
| `worker_count=4` | Required, not tuning: stage 1 publishes with confirms from its handler — unbounded on a single sync worker (rabbitkit warns) |
| Env-driven connection, `connection_name` | No hardcoded prod credentials; identifiable in the management UI |

## Run

```bash
docker run -d --rm -p 5672:5672 rabbitmq:3.13-management-alpine
python examples/two_stage_chain/chain.py
```

Expected output ends with:

```
[verified] 10 readings: raw -> convert -> celsius -> store -> ack in 0.0s;
sensor-7 was nacked once, redelivered (2 deliveries), stored exactly once.
```

For the full production hardening of this same shape (quorum queues, retry
ladder, DLQ triage, metrics, probes, graceful drain), see
[`examples/production_pipeline/`](../production_pipeline/).
