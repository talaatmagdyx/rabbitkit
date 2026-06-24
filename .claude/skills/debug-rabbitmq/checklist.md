# rabbitkit triage checklist

Match the symptom; run the check; apply the fix. All commands assume the broker is importable as `myapp.main:broker`.

| Symptom | Check | Likely cause → fix |
|---|---|---|
| **Won't connect** (`AMQPConnectionError`, refused, reset) | `rabbitkit health check ...`; `RabbitManagementClient().health_check()` | Wrong host/port/vhost/creds in `ConnectionConfig`; broker down; TLS mismatch in `SSLConfig`. For aio-pika "connection reset", use `127.0.0.1` not `localhost` (IPv6 ::1 vs IPv4 docker map). |
| **Queue not found on start** | `rabbitkit topology list ...` | `TopologyMode.PASSIVE_ONLY`/`MANUAL` doesn't create queues — switch to `AUTO_DECLARE`, or declare topology out of band. |
| **Handler never fires** | `topology list` shows the route? message arriving? | Wrong `queue`/`routing_key`/`exchange` binding; a `filter_fn` nacking it; consuming a different vhost. |
| **Messages stuck "unacked"** | mgmt `get_queue(q)` → `messages_unacknowledged` high | Handler hung or `prefetch_count` too high with slow work; `AckPolicy.MANUAL` handler that forgot to ack; add `TimeoutMiddleware`. |
| **Endless redelivery / retry loop** | `peek` a `*.retry.*` queue; inspect retry-count header | Error mis-classified as TRANSIENT; reduce `max_retries` or fix classification; a permanent error should hit the DLQ, not loop. |
| **Piling up in `*.dlq`** | `DLQInspector.peek(q, limit=10)` — read `x-error`/headers | Real permanent failures (bad payload, validation). Fix the producer or handler, then `replay(predicate=..., target_queue=...)` the recoverable ones; `purge` only true garbage. |
| **Pydantic model arrives as raw dict** | grep handler module for `from __future__ import annotations` | Remove future-annotations from that module — the pipeline reads raw `inspect.signature`. |
| **Return value not published** | check decorator order | `@publisher` must be inner, `@subscriber` outer. |
| **Low throughput** | measure drain rate, not produce rate | Raise `prefetch_count`; use `SyncWorkerPool`/`AsyncWorkerPool` (`worker_count>1`); for publish, `BatchPublisher` + `FlowController`; confirm you're not gated by a single producer. See README High-Load. |
| **Publisher blocks / `connection.blocked`** | broker memory/disk alarm? | Broker resource alarm — `FlowController(on_blocked="wait"|"raise"|"drop")` to control behavior; fix broker resources. |
| **Lost messages on restart** | queues durable? messages persistent? confirms on? | `RabbitQueue(durable=True)` + `PublisherConfig(persistent=True, confirm_delivery=True)`; use quorum queues for stronger guarantees. |
