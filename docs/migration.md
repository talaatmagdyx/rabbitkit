# Migration guide

Breaking changes and deprecations to Stable Core / Advanced Stable APIs,
one entry per change, in the order they happened. See
[`docs/stability-policy.md`](stability-policy.md) for the deprecation
policy this follows. Experimental API changes are not tracked here — see
`CHANGELOG.md` for those.

> **Version numbering note:** entries marked *(internal 0.8.x)* predate
> the first published release (**0.9.0**, the public beta). No earlier
> version was ever distributed, so these entries only matter if you
> tracked the repository before publication.

## 0.10.0 — upgrade notes

Three behavior changes land in 0.10.0. All are safety improvements, and
two can surface loudly the first time you restart a service after
upgrading — read this **before** rolling it out.

### 1. Quorum sources now get a QUORUM auto-DLQ — existing classic DLQs 406 at startup

**What changed:** the auto-declared `{queue}.dlq` (retry and
safety-provision paths) inherits `queue_type=QUORUM` when the source queue
is quorum. Previously it was always classic — meaning the very messages
you chose replication for (dead-lettered failures, stored indefinitely)
sat on unreplicated single-node storage.

**Who is affected:** any existing deployment with a quorum source queue
under `TopologyMode.AUTO_DECLARE`. The broker already has a **classic**
`{queue}.dlq`; the upgraded service re-declares it as quorum → `406
PRECONDITION_FAILED` → `ConfigurationError` **at startup**. Loud and
before any message is touched — but it will block the deploy if you
haven't prepared.

**How to migrate (pick one):**

1. *Recommended:* drain and migrate the DLQ before deploying —
   `rabbitkit dlq replay <queue>.dlq <target>` (or requeue to the source),
   delete the empty classic DLQ, deploy; the upgraded service declares it
   quorum.
2. Declare topology externally (policy/provisioner) and run the service
   with `TopologyMode.PASSIVE_ONLY` — the service then never re-declares
   anything (this is the production-recommended split regardless; see
   [Production patterns §4](production/patterns.md)).
3. To keep the classic DLQ deliberately, declare it yourself: a manually
   configured `dead_letter_exchange`/queue is respected as-is.

### 2. `PublisherConfig.max_message_bytes` defaults to 16 MiB (was: disabled)

Publishes with a body over 16 MiB now raise `MessageTooLargeError` (a
`ValueError`) client-side. The server was already rejecting them — with a
channel-killing exception that corrupted sibling in-flight publishes — so
only deployments that **raised** `max_message_size` in `rabbitmq.conf`
need action: set `PublisherConfig(max_message_bytes=<your server limit>)`
(or `0` to disable the guard).

### 3. `prefetch_count=0` / `prefetch_per_worker=0` now raise at construction

`0` meant AMQP *unlimited* prefetch — the whole queue backlog buffered in
process memory. If you really want unbounded (you don't), that spelling no
longer exists; set an explicitly large value. `ConfigValidationError` (a
`ValueError`) is raised where the config is constructed.

Also in 0.10.0, not breaking but worth knowing: a `RetryMiddleware`
without a usable publish fn now **nack-requeues** transient failures
(previously they were acked and silently lost); producer-set
`x-rabbitkit-original-queue` headers are ignored (always overwritten);
per-message `expiration` no longer survives a retry republish.

---

## `rabbitkit.aio` → `rabbitkit.async_` (internal 0.8.1)

**What changed:** `rabbitkit.async_` is now the canonical import path for
the async broker and transport. `rabbitkit.aio` still works, but importing
it now emits a `DeprecationWarning`.

**Why:** both paths existed with no documented canonical answer. Usage
across the codebase and documentation was already overwhelmingly
`rabbitkit.async_` (roughly 4:1), so that became the canonical path rather
than picking one arbitrarily.

**Before:**

```python
from rabbitkit.aio import AsyncBroker
```

**After:**

```python
from rabbitkit.async_ import AsyncBroker
# or, equivalently, from the top level:
from rabbitkit import AsyncBroker
```

**Timeline:** deprecated pre-publication (internal `0.8.1`). Per the deprecation policy, it will be
removed no earlier than the following minor release. If you see the
`DeprecationWarning`, update the import now — there's no behavior
difference, `rabbitkit.aio` re-exports the exact same class.

**How to check if you're affected:**

```bash
grep -rn "from rabbitkit.aio\|from rabbitkit import aio\|import rabbitkit.aio" .
```

---

## Template for future entries

When a Stable Core or Advanced Stable API changes in a way that requires
user action, add an entry here following this shape:

```markdown
## <old symbol/path> → <new symbol/path> (<version>)

**What changed:** ...
**Why:** ...
**Before:** <code>
**After:** <code>
**Timeline:** deprecated in `X.Y.Z`; removed no earlier than the following minor release.
**How to check if you're affected:** <a grep/search command, if applicable>
```

## 0.9.0 — `TracedConsumerMiddleware` removed

The obskit-based tracing middleware is removed; rabbitkit is now fully
self-contained (zero org-internal packages for any feature). Migrate to
[`OTelTracingMiddleware`](api/middleware.md#oteltracingmiddleware)
(`pip install rabbitkit[otel]`) — a drop-in replacement: same span names,
semantic attributes, and W3C header propagation, on the standard
OpenTelemetry API. The duck-typed `CircuitBreakerProtocol` compatibility
with obskit's circuit breaker is unaffected (any compatible implementation,
e.g. pybreaker, works and always did).
