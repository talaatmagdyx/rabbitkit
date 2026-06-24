---
paths:
  - "src/rabbitkit/core/**"
---

# Core layer rules

`core/` is the shared, transport-free heart of rabbitkit. Anything here is imported by both the sync (pika) and async (aio-pika) sides.

- **ZERO transport imports.** `core/` must never import `pika` or `aio_pika`, directly or transitively. Transport-specific code belongs in `sync/` or `async_/`.
- Config dataclasses: `@dataclass(frozen=True, slots=True)` — immutable by convention, composable.
  - `RabbitConfig` only composes connection/broker defaults; throughput configs go to their components directly. `WorkerConfig` is NOT part of `RabbitConfig` — it's passed to `broker.start(worker_config=)`.
- `core/types.py` is the **single canonical location** for ALL enums and data types. Don't redefine enums elsewhere.
- `core/topology.py` — Exchange/Queue models with validation; expose `to_declare_kwargs()` for the transports.
- `core/route.py` — `RouteDefinition` validates at registration time (fail fast).
- `core/pipeline.py` — middleware chain composes outer→inner; on publish/retry failure, **nack** (never silently ack).
