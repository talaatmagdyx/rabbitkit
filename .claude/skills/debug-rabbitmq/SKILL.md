---
name: debug-rabbitmq
description: Diagnose rabbitkit / RabbitMQ problems — connection failures, messages stuck retrying or piling in a DLQ, low throughput, unacked messages — using rabbitkit's own tools (the CLI health/topology/shell, DLQInspector, management client, dashboard, structured logs). Use when a rabbitkit broker won't connect, consume, or drain, or when investigating a dead-letter queue.
argument-hint: "[symptom, e.g. 'connection refused' | 'stuck in DLQ' | 'slow']"
allowed-tools: Bash(rabbitkit *), Read, Grep
---

Triage a rabbitkit/RabbitMQ issue. Work the symptom in `$ARGUMENTS` against the checklist — read `${CLAUDE_SKILL_DIR}/checklist.md` for the full step-by-step table, then apply the matching row.

## The tools rabbitkit gives you (prefer these over guessing)

- **Health:** `rabbitkit health check myapp.main:broker` (exit 1 if unhealthy) — or `broker_health_check()` / `_async` in code.
- **Topology:** `rabbitkit topology list myapp.main:broker [--format json]` — confirm the routes/queues the broker *thinks* it has.
- **Live REPL:** `rabbitkit shell myapp.main:broker` — `broker`, `routes`, `config`, `publish` preloaded; publish a probe `MessageEnvelope` and watch it.
- **DLQ:** `DLQInspector(transport)` → `.peek(q, limit=)` (non-destructive), `.replay(q, predicate=, target_queue=)`, `.purge(q)` (+ `_async` variants). Peek first, always.
- **Broker truth:** `RabbitManagementClient` → `list_queues()` (`messages`, `messages_unacknowledged`), `get_queue()`, `overview()`, `health_check()`. This is the broker's view, not the app's.
- **Dashboard:** `create_dashboard_app(broker, management_client=...)` → `/api/health`, `/api/routes`.
- **Logs:** `LoggingConfig(render_json=...)` — every in-handler log line carries `message_id`, `routing_key`, `queue`, `handler`. Grep those.

## First moves

1. Reproduce the broker's *own* view with the management client (`list_queues` → `messages` vs `messages_unacknowledged`) before trusting app logs.
2. `localhost` vs `127.0.0.1`: aio-pika resolving `localhost` to IPv6 `::1` against an IPv4-only `-p` docker mapping shows as "connection reset by peer". Try `127.0.0.1`.
3. Don't `purge` a DLQ to "fix" it — `peek` to learn *why* messages landed there (`x-error` / retry-count headers), then `replay` the recoverable ones.

Report findings with evidence (the command run + its output), not assertions.
