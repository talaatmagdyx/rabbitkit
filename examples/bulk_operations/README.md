# Bulk operations & reliability profiles (0.12)

Runnable companions to [`docs/bulk-operations.md`](../../docs/bulk-operations.md).
Every example expects RabbitMQ on `localhost:5672` (`05` also needs the
management plugin on `15672`); `06` needs no broker at all.

| File | Shows |
|---|---|
| `01_publish_many_async.py` | `AsyncBroker.publish_many` with bounded in-flight/bytes/deadline; UNROUTABLE and INVALID items reported per input; what to do with UNKNOWN vs NOT_SENT |
| `02_publish_many_sync.py` | `SyncBroker.publish_many` + streaming `iter_publish` over a paged generator; `raise_for_status()` |
| `03_selected_ack_batch_commit.py` | MANUAL handlers parked until a batch DB commit; `ack_many` for the exact successful subset, `nack_many(requeue=False)` for the rest |
| `04_coalescing_acker.py` | `CoalescingAcker` with 4 concurrent workers finishing out of order — cumulative acks only through a completed prefix |
| `05_reliability_profile_preflight.py` | `critical_config`, contradiction detection, `validate_profile`, `policy_templates`, `broker.preflight` against the management API |
| `06_retry_handoff_and_sanitizer.py` | Sanitized DLQ triage headers (`error_detail`) and bounded retry-handoff backoff / EXHAUSTED hook — no broker needed |
| `07_transactional_outbox_inbox.py` | Outbox → `publish_many` → inbox with SQLite: stable ids, mark-only-confirmed, duplicate absorbed, ack after commit |

```bash
docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
python examples/bulk_operations/01_publish_many_async.py
```

## Which one do I need?

- Publishing a page of rows and want to know exactly what happened to each: `01` / `02`.
- Acking after a batch database commit: `03`. Crash-safe end to end: `07`.
- Wire-level ack coalescing without ever acking a still-running sibling: `04`.
- Proving a critical deployment meets the profile (and seeing what cannot be proven): `05`.
- Understanding what lands on your DLQ headers and what happens when the retry queue is gone: `06`.

There is no "publish by id" or "fetch by id" example because RabbitMQ has no
such operation: queues are not queryable stores. The selection ("pluck")
always happens in application code — `01`, `02` and `07` show that step.

## Numbers

`python -m benchmarks.bench_bulk` measures every path above against a real
broker. On an Apple Silicon laptop (2,000 × 1 KiB, confirms on, median of 3):

| | single | bulk |
|---|---|---|
| async publish | 932 msg/s (`publish` loop) | 4,548 msg/s (`publish_many`, defaults) · 9,862 msg/s (+ batch publisher) |
| sync publish | 833 msg/s | 1,015 msg/s (`publish_many` — sequential by design; you gain outcomes, not speed) |
| ack | 15,308 msg/s, 1 frame/msg (`ack_async` each) | `ack_many`: 11,808 msg/s, 1 frame/msg, 20 API calls · `CoalescingAcker`: 15,670 msg/s, **0.01 frame/msg** |

Full table, environment and interpretation: [docs/benchmarking.md](../../docs/benchmarking.md#tier-2b-bulk-vs-single-python--m-benchmarksbench_bulk).
