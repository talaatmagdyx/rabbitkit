# production_pipeline — the production checklist, executable (sync / pika)

A realistic consume **and** publish worker on **`SyncBroker`** with every
production decision made explicitly: an order processor that consumes
`order.created`, validates and enriches it, and republishes
`order.processed` — surviving bad payloads, flaky downstreams, broker
bounces, and SIGTERM without losing a message.

This is the runnable companion to
[`docs/production/checklist.md`](../../docs/production/checklist.md). Each
decision in the code is tagged `[P#]` and justified inline.

## Files

| File | What it shows |
|------|---------------|
| `app.py` | The worker: quorum queues + delivery-limit backstop, retry ladder + DLQ, confirmed result publishing, Pydantic validation, optional Redis dedup, Prometheus metrics, liveness/readiness split, graceful SIGTERM drain, env config with credential hygiene |
| `producer.py` | The publish contract: `publish()` never raises — branch on `PublishOutcome` (CONFIRMED / RETURNED / NACKED / TIMEOUT / ERROR), use `outcome.classification` for the why, `mandatory=True` to catch unroutable messages |
| `test_pipeline.py` | The business contract under `TestBroker` — validation, fee math, settlement, idempotency — no RabbitMQ needed |

## Run it

```bash
docker run -d --rm -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management-alpine

# Terminal 1 — the worker (owns ALL topology declaration)
python examples/production_pipeline/app.py

# Terminal 2 — seed traffic
python examples/production_pipeline/producer.py

# The contract tests (no broker needed)
pytest examples/production_pipeline/test_pipeline.py -v
```

## What happens to each seeded order

| Order | Path |
|-------|------|
| `ord-1xxx` (valid) | processed → `order.processed` published (confirmed) → recorded by the sink |
| `ord-2001` (`simulate=transient`) | raises `ConnectionError` → TRANSIENT → retry ladder (2s, 10s, 30s) → still failing → dead-lettered |
| `ord-3001` (`simulate=permanent`) | raises `ValueError` → PERMANENT → dead-lettered immediately, **no retries** |
| routing key `order.nope` | nothing bound → broker returns it → producer sees `RETURNED`, not a false confirm |

Inspect the dead letters — every DLQ'd message carries triage headers
(`x-rabbitkit-error-type`, `x-rabbitkit-error-message`,
`x-rabbitkit-first-failed-at`, `x-rabbitkit-last-failed-at`, plus the
original exchange/routing-key/queue):

```bash
rabbitkit dlq peek pp.orders.incoming.dlq
```

## The decisions, in one table

| # | Decision | Why (the one-line version) |
|---|----------|----------------------------|
| P1 | Quorum queues, `delivery_limit=6` | Replicated + a **broker-enforced** poison backstop independent of any client-side logic |
| P2 | `RetryConfig(max_retries=3, delays=…)` | Transient ≠ permanent: network errors walk a delay ladder; `ValueError`/validation go straight to the DLQ |
| P3 | `confirm_delivery=True, persistent=True`; results via `@publisher` | The source message is only acked after the result publish is **broker-confirmed** — a lost result nack-requeues the source instead of vanishing |
| P4 | Pydantic-annotated handler body | Malformed payloads are rejected as PERMANENT before business logic runs (needs no `from __future__ import annotations` in the module — see the note in `app.py`) |
| P5 | Idempotent handler (+ optional Redis dedup via `REDIS_URL`) | At-least-once delivery **will** rerun handlers; dedup narrows the window, handler design closes it — [the idempotency contract](../../docs/production/idempotency.md) |
| P6 | `MetricsMiddleware` + Prometheus on `:9100` | Publish outcomes by real status, retry/DLQ counters, reconnect + channel-churn counters — [the metric reference](../../docs/observability.md) |
| P7 | Liveness ≠ readiness on `:8080` | A broker outage must flip **readiness** (stop traffic), never **liveness** (don't restart the fleet mid-outage) — served from a plain stdlib HTTP thread; rabbitkit's health functions are safe to call from any thread |
| P8 | `broker.run(...)` — never bare `start()` | `run()` blocks, **reconnects after connection drops** (pika has no built-in recovery), and doesn't return until the SIGTERM drain fully completes — no messages abandoned mid-handler |
| P9 | Env config, `safe_url` logging, `connection_name` + `client_properties` | No credentials in code or logs; every connection identifiable in the management UI in an incident |
| P10 | `prefetch_count` bounded + `WorkerConfig(worker_count=4)` | Backpressure lives in RabbitMQ, not this process's memory. `worker_count > 1` is **required** here, not a tuning knob: handlers publish with confirms, and on a single worker that confirm wait runs on the connection's own I/O thread and cannot be time-bounded (rabbitkit emits a `RuntimeWarning` at startup if you try). Multiple workers also keep slow handlers from starving heartbeats |

## Sync-specific honesty

- **Throughput ceiling:** sync confirmed publishing tops out around ~0.9k
  msg/s per process (pika serializes confirms on one channel; `worker_count`
  does not raise it). If you need more, the same config and decorators run
  on `AsyncBroker` (~6k msg/s measured with batching) — see the sync-vs-async
  section of the [full guide](../../docs/guide/full-guide.md) and
  [`docs/production/scale.md`](../../docs/production/scale.md).
- **Owner-thread rule:** every transport call must come from the thread that
  called `start()`/`run()` — worker-thread settlement is marshalled for you,
  but don't wire your own cross-thread `broker.stop()` (see
  [`docs/concurrency-model.md`](../../docs/concurrency-model.md)).
- **Publish-only sync processes** (like a long-lived version of
  `producer.py`) must call `broker.pump_idle()` between publishes or the
  idle connection misses heartbeats.

## What this example deliberately does NOT do

- **No `ACK_FIRST`** — at-most-once is the wrong default for anything that
  matters; every route here is at-least-once + idempotent.
- **No test-publish in the readiness probe** — a known anti-pattern (probe
  traffic + side effects every few seconds); the passive checks are richer.
- **No unbounded anything** — prefetch, retries, redeliveries
  (`delivery_limit`), graceful drain, and publish confirm waits all have
  explicit bounds.

Scaling this shape (workers, batching, flow control, sync-vs-async
tradeoffs): [`docs/production/scale.md`](../../docs/production/scale.md).
Kubernetes manifests with correctly-wired probes and `preStop`:
[`examples/kubernetes_worker/`](../kubernetes_worker/) and
[`docs/kubernetes.md`](../../docs/kubernetes.md).
