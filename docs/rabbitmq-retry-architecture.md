# RabbitMQ Retry Architecture — Production Design Document

**Service:** `order-service` (reference domain)
**Library:** `rabbitkit` (sync pika + async aio-pika)
**Audience:** backend engineers, SRE/on-call, platform, future maintainers
**Status:** authoritative design + implementation guide
**Delivery guarantee assumed:** **at-least-once**. We do **not** claim exactly-once. Every consumer is designed for duplicates, crashes, downstream outages, and replay.

> **Reading note.** Code blocks use the *real* rabbitkit API (verified against the source, v0.6.x). Where the library does not do something automatically, this document says so explicitly — those are the places teams get burned.

---

## 0. Four rabbitkit truths that shape everything below

Read these first. They are the non-obvious facts that determine whether your retry system works or silently loses messages.

1. **`retry=RetryConfig(...)` on a subscriber does BOTH: declares topology
   AND installs the retry middleware.** The broker declares the delay
   queues + DLQ, re-declares the source queue with dead-letter arguments,
   and auto-wires a `RetryMiddleware` with its own confirmed publish
   function into the route at `start()`. You do **not** add
   `RetryMiddleware` manually — one switch, both halves. (If you DO
   construct your own `RetryMiddleware` in `middlewares=[...]`, the broker
   injects its publish fn into it at start when you left it unset, so even
   manual wiring can no longer create the publish-less loss path this
   section used to warn about.)

2. **A failed, unverified, or impossible retry publish nacks — never
   acks.** The retry republish is `mandatory=True` and confirm-gated; the
   source message is acked only after the outcome reports OK. A publish fn
   that is missing, mismatched (sync fn on an async route), or returns
   `None` results in `nack(requeue=True)` plus a `RuntimeWarning` — a hot
   loop you will see, not a silent drop.

3. **Only four retry headers are set for you:** `x-rabbitkit-retry-count`
   (clamped to `[0, max_retries]` — a producer cannot spoof it),
   `x-rabbitkit-original-exchange`, `x-rabbitkit-original-routing-key`,
   and `x-rabbitkit-original-queue` (**always overwritten** at delivery
   with the real source queue — a producer-set value is never trusted for
   retry routing). The richer forensic headers in §9 (`x-first-failure-at`,
   `x-error-type`, `x-trace-id`, `x-tenant-id`, `x-idempotency-key`, …)
   are **your** responsibility — add them via the producer or a small
   enrichment middleware.

4. **`AckPolicy.AUTO` dead-letters exhausted retries correctly.**
   `RetryMiddleware` acks every non-terminal retry itself and marks
   terminal failures (retry budget exhausted, or permanent classification)
   so the pipeline rejects them with `requeue=False` → the source queue's
   DLX → the DLQ — under `AUTO` as well as `NACK_ON_ERROR`. The historical
   claim that AUTO hot-loops exhausted-transient failures is stale; the
   terminal marker closed it. What remains true: on **retry-less** routes,
   AUTO nack-requeues transient failures indefinitely by design (opt into
   `ConsumerConfig(reject_transient_on_redelivery=True)` for a 2-strike
   cap). Classification is TYPE-based (§7): custom transient errors must
   subclass `OSError`/`TimeoutError`, or register a predicate — unknown
   errors default to PERMANENT and skip retries.

Everything else is detail.

---

## 1. Executive Summary

We process business events (`orders.created`) from RabbitMQ. Handlers fail. The retry architecture decides what happens next so that **valid work eventually completes**, **invalid work is quarantined for humans**, and **the system never melts down under failure**.

**Why retries are needed.** Most failures are transient: a database failover, a 503 from a downstream API, a momentary network blip. Dropping or instantly failing those messages turns a 5-second hiccup into lost orders.

**Why naive retries are dangerous.**
- **Immediate `requeue=True` loops** put the failed message straight back at (or near) the head of the queue. If the downstream is down, the same message fails again in microseconds — a **retry storm** that pins CPU, starves healthy messages, and amplifies the outage. This is the single most common self-inflicted RabbitMQ incident.
- **`time.sleep()` inside the consumer** blocks the consumer thread/event loop, collapses throughput, holds a delivery unacked (counts against prefetch), and breaks heartbeats.
- **Retrying every exception** retries un-retryable errors (bad schema, auth failure) forever, filling queues with messages that can never succeed.

**Why TTL + DLX delay queues are preferred.** Instead of sleeping, we *move* the message to a **delay queue** with a per-attempt TTL and a dead-letter exchange (DLX) pointing back to the main exchange. The message sits idle (no consumer attached), costs nothing, and RabbitMQ re-delivers it to the main queue when the TTL expires. The consumer is never blocked. Backoff is enforced by the broker, not by sleeping code. **Jitter** (±10%) spreads re-deliveries so a thundering herd doesn't all retry at the same instant.

**Why idempotency is mandatory.** At-least-once means the same message *will* be delivered more than once (consumer crash after side-effect but before ack; redelivery after reconnect; replay). If processing a message twice corrupts data (double charge, double inventory decrement), you have a bug, not a retry. Idempotency is the price of admission, not an optimization.

**Why the DLQ is an operational workflow, not an error bin.** A message in the DLQ represents *work that did not happen* — a customer impact. The DLQ is a queue you **triage, classify, fix the root cause for, and replay** with intent and audit trail. A DLQ nobody watches is just silent data loss with extra steps.

**Target architecture in one sentence:** *durable topic exchange → durable (preferably quorum) main queue → consumer with a middleware pipeline that classifies errors, retries transient failures through TTL+DLX delay queues with bounded attempts and jitter, dead-letters terminal/poison messages to a per-queue DLQ, and is fully idempotent, observable, and replay-safe.*

---

## 2. System Context

```
                         ┌──────────────────────┐
   producers ───────────▶│  orders.exchange      │ (topic, durable)
   (web, other services) │  routing: orders.*    │
                         └───────────┬──────────┘
                                     │ rk = orders.created
                                     ▼
                         ┌──────────────────────┐
                         │  orders.queue         │ (durable/quorum)
                         │  x-dead-letter → DLQ  │
                         └───────────┬──────────┘
                                     │ consume (prefetch=N, worker pool)
                                     ▼
                         ┌──────────────────────┐
                         │  order-service        │  middleware pipeline:
                         │  consumer pods (K8s)  │  trace→exc→cb→dedupe→
                         └───────────┬──────────┘  retry→timeout→rate→handler
                                     │
              transient failure?     │  permanent / exhausted?
              ┌──────────────────────┴───────────────────────┐
              ▼                                               ▼
   ┌────────────────────┐   TTL expires      ┌────────────────────────┐
   │ orders.queue.retry.N│ ───── DLX ───────▶│ orders.exchange → main │
   │ TTL = delays[N]     │   back to main     └────────────────────────┘
   │ no consumer         │
   └────────────────────┘   after max_retries (or permanent error)
                                     │
                                     ▼
                         ┌──────────────────────┐
                         │  orders.queue.dlq     │ ◀── humans: peek / replay / purge
                         └──────────────────────┘
```

**Message lifecycle (happy + unhappy):**

1. Producer publishes `orders.created` to `orders.exchange` with **publisher confirms** + persistent delivery.
2. Broker routes to `orders.queue`.
3. Consumer pulls (bounded by prefetch), runs the pipeline.
4. **Success** → ack. Done.
5. **Transient failure**, attempt `n < max_retries` → `RetryMiddleware` publishes a copy to `orders.queue.retry.n` (TTL `delays[n]`, no consumer), then acks the source. TTL expires → DLX → back to `orders.exchange` → `orders.queue`. `x-rabbitkit-retry-count` incremented.
6. **Permanent failure** *or* **retries exhausted** → message is dead-lettered to `orders.queue.dlq` (via the source queue's `x-dead-letter-routing-key`, triggered by a terminal nack/reject).
7. **DLQ** → operator triages, fixes root cause, replays in controlled batches.

Reference names:

| Thing | Name |
|---|---|
| Exchange | `orders.exchange` (topic, durable) |
| Routing key | `orders.created` |
| Main queue | `orders.queue` |
| Retry queues | `orders.queue.retry.0..3` (declared by `RetryRouter`) |
| DLQ | `orders.queue.dlq` |

> rabbitkit's `RetryRouter` generates this topology from your `RetryConfig`. The names are deterministic: with `per_queue=True`, delay queues are `{source_queue}.retry.{attempt}` (→ `orders.queue.retry.0..3`) and the DLQ is `{source_queue}.dlq` (→ `orders.queue.dlq`). With `per_queue=False` they collapse to shared `rabbitkit.retry.{attempt}` / `rabbitkit.dlq` (not recommended — see §3).

---

## 3. RabbitMQ Retry Strategy

**Mechanism:** TTL + DLX delayed retry, bounded attempts, exponential-ish delays, jitter, per-queue isolation, terminal → DLQ.

**Policy (this is also the rabbitkit default — `RetryConfig()` already matches it):**

```python
from rabbitkit import RetryConfig
from rabbitkit.core.types import ErrorSeverity

RETRY = RetryConfig(
    max_retries=4,                       # 4 retry attempts after the first delivery
    delays=(5, 30, 120, 600),            # seconds: 5s → 30s → 2m → 10m
    jitter_factor=0.1,                   # ±10% applied to each delay
    unknown_policy=ErrorSeverity.PERMANENT,  # unclassified error → DO NOT retry (safe default)
    per_queue=True,                      # each queue gets its own retry/DLQ topology
    retry_header="x-rabbitkit-retry-count",
    dead_letter_exchange="",             # "" = default exchange → routes by DLQ name
)
```

Total worst-case retained lifetime before DLQ ≈ `5 + 30 + 120 + 600 = 755s` (~12.5 min) plus processing time, before a message is declared dead. Tune `delays` to your SLO (see §23).

**Why this is safer than the alternatives:**

| Anti-pattern | Failure mode | This design |
|---|---|---|
| `requeue=True` loop | Retry storm; same msg reprocessed thousands of times/sec; healthy msgs starved | Message parked in a *separate* delay queue with no consumer; main queue keeps flowing |
| `sleep()` in consumer | Blocks consumer; throughput → 0; unacked piles up; heartbeat loss | No sleeping; broker holds the delay via TTL |
| Only `x-death` (no custom header) | `x-death` semantics differ across queue types/versions and are easy to misread; hard to cap attempts reliably | Explicit `x-rabbitkit-retry-count` we own and increment |
| Retry **all** exceptions | Un-retryable errors retried forever; DLQ never reached | Error **classification**: only TRANSIENT retries; unknown defaults to PERMANENT |
| One global retry queue shared across services | One noisy service's retries delay everyone; blast radius = whole platform | `per_queue=True`: isolated topology per queue |

**Delay-queue mechanic (why it works):** a delay queue has `x-message-ttl=delays[n]`, `x-dead-letter-exchange` = main exchange, `x-dead-letter-routing-key` = original routing key, and **no consumer**. A message published there is invisible until its TTL elapses; RabbitMQ then dead-letters it back to the main queue. Because the queue is per-attempt, all messages in `retry.0` share the same TTL — no head-of-line blocking from TTL ordering (a classic single-delay-queue bug where a short-TTL message is stuck behind a long-TTL one at the head).

---

## 4. Queue Topology

```
orders.exchange            type=topic   durable=true   auto_delete=false
  └── binding rk=orders.created ──▶ orders.queue

orders.queue               durable=true   type=quorum (recommended)
  arguments:
    x-dead-letter-exchange   = ""                      # default exchange
    x-dead-letter-routing-key= orders.queue.dlq        # terminal nack/reject → DLQ
    x-delivery-limit         = 20      # quorum-queue poison guard (belt & suspenders)
    # optional: x-max-length / x-overflow=reject-publish-dlx for hard caps

orders.queue.retry.0       durable=true   NO CONSUMER
  arguments:
    x-message-ttl            = 5000                     # delays[0]
    x-dead-letter-exchange   = orders.exchange
    x-dead-letter-routing-key= orders.created           # back to main

orders.queue.retry.1  (TTL 30000)  → DLX orders.exchange → orders.created
orders.queue.retry.2  (TTL 120000) → DLX orders.exchange → orders.created
orders.queue.retry.3  (TTL 600000) → DLX orders.exchange → orders.created

orders.queue.dlq           durable=true   NO auto-consumer
  arguments:
    # no DLX (terminal). optional x-message-ttl for retention, e.g. 14 days
    x-message-ttl            = 1209600000               # 14d retention (optional)
```

**Design choices and trade-offs:**

- **Durable exchanges + queues, persistent messages (`delivery_mode=2`).** Survive broker restart. Cost: fsync latency on publish. Non-negotiable for business events.
- **Quorum queues (recommended) vs classic.** Quorum = Raft-replicated, safer under node failure, has native `x-delivery-limit` poison protection. Cost: more memory/disk, higher publish latency, no lazy mode. Use quorum for the **main** and **DLQ**. Delay queues can stay classic (they hold transient data; replication is less critical) — but keep them durable.
- **Lazy queues** (classic only): page to disk, good for deep backlogs / DLQs that grow large. Do **not** combine with quorum (quorum manages memory itself).
- **Single Active Consumer (SAC):** set on the main queue **only if you need per-queue ordering** with multiple consumer pods (one pod consumes at a time, others stand by for failover). This caps throughput to one consumer — see §17 for ordering vs throughput.
- **`x-max-length` + `x-overflow=reject-publish-dlx`:** a hard backpressure valve. When the queue hits the cap, new publishes are dead-lettered instead of accepted. Prevents unbounded memory growth at the cost of shedding load — only enable with an explicit overflow plan.
- **DLQ has no DLX.** It is terminal by design. Add a TTL only if you have a documented retention policy and an external archive (don't silently expire customer-impacting messages).

---

## 5. rabbitkit Configuration (production-grade)

```python
# config.py
from __future__ import annotations

from rabbitkit import (
    RabbitConfig, ConnectionConfig, SocketConfig, SecurityConfig,
    PublisherConfig, ConsumerConfig, PoolConfig, RetryConfig,
    CompressionConfig, LoggingConfig,
)
from rabbitkit.core.config import SSLConfig
from rabbitkit.core.types import ErrorSeverity, TopologyMode


def build_config(env: str) -> RabbitConfig:
    return RabbitConfig(
        connection=ConnectionConfig(
            host="rabbitmq.internal",
            port=5671 if env == "production" else 5672,   # 5671 = AMQPS
            vhost="/orders",
            username="order-service",                     # least-privilege user (§27)
            password="...",                               # from secret manager, never literal
            heartbeat=30,             # detect dead peers in ~60s (2 missed beats). Lower = faster
                                      #   detection but more chatter; 30 is a good default.
            socket_timeout=10.0,      # TCP connect/op timeout. Fail fast on a black-hole network.
            blocked_connection_timeout=60.0,  # if broker says connection.blocked (memory/disk
                                      #   alarm), give up after 60s instead of hanging forever.
            reconnect_backoff_base=1.0,   # first reconnect after 1s
            reconnect_backoff_max=30.0,   # cap exponential backoff at 30s
            connection_name=f"order-service@{env}",  # shows in mgmt UI — priceless during incidents
        ),
        socket=SocketConfig(          # SYNC-ONLY: applied by SyncBroker (pika tcp_options);
            tcp_nodelay=True,         #   AsyncBroker warns and ignores it (aio-pika manages
            tcp_keepidle=10, tcp_keepintvl=5, tcp_keepcnt=3,  # its own sockets across reconnects)
        ),
        security=SecurityConfig(
            mechanism="PLAIN",
            ssl=SSLConfig(
                enabled=(env == "production"),
                ca_certs="/etc/rabbitmq/certs/ca.pem",
                certfile="/etc/rabbitmq/certs/client.pem",   # mTLS (optional but recommended)
                keyfile="/etc/rabbitmq/certs/client.key",
                cert_reqs="CERT_REQUIRED",
                server_hostname="rabbitmq.internal",         # verify cert hostname
            ),
        ),
        publisher=PublisherConfig(
            confirm_delivery=True,    # REQUIRED. Without confirms, a publish that the broker
                                      #   never persisted looks successful → silent loss.
            confirm_timeout=5.0,      # how long to wait for the broker ack before raising
            persistent=True,          # delivery_mode=2; survive broker restart
            mandatory=False,          # see §16 — True surfaces unroutable msgs, costs a return path
        ),
        consumer=ConsumerConfig(
            prefetch_count=20,        # tune to workload (§14/§17). IO-bound: higher. CPU/slow: lower.
            graceful_timeout=30.0,    # drain window on shutdown
        ),
        pool=PoolConfig(
            channel_pool_size=20,     # >= worker_count, or publishes from handlers block (§14)
            publisher_connections=1,  # separate publisher/consumer connections (anti HOL-blocking)
            consumer_connections=1,
            channel_acquire_timeout=10.0,
        ),
        retry=RetryConfig(            # broker default; per-route can override or disable
            max_retries=4, delays=(5, 30, 120, 600),
            jitter_factor=0.1, per_queue=True,
            unknown_policy=ErrorSeverity.PERMANENT,
        ),
        compression=CompressionConfig(algorithm="zstd", threshold=2048, level=6),
        logging=LoggingConfig(),      # structured JSON logs
        # Topology mode is the big environment-dependent lever:
        topology_mode=(
            TopologyMode.AUTO_DECLARE if env in ("dev", "staging")
            else TopologyMode.PASSIVE_ONLY   # production: assert topology exists, don't mutate it
        ),
    )
```

**`TopologyMode` is the most important environment difference:**

- **`AUTO_DECLARE`** (dev/staging): the broker declares exchanges/queues/bindings on start. Fast iteration. **Dangerous in production** — a code change silently mutates broker topology, and two app versions can fight over arguments (declaring a queue with different args than it has = `PRECONDITION_FAILED`, consumer won't start).
- **`PASSIVE_ONLY`** (production, common): the broker asserts the topology exists (passive declare) but never creates/changes it. Topology is owned by a controlled process (Terraform, a migration job, or platform team). Mismatch = loud failure at startup, not silent drift.
- **`MANUAL`** (production, mature platforms): rabbitkit touches no topology at all. Everything is provisioned out-of-band. Maximum control, requires discipline.

> **Why not AUTO_DECLARE in prod?** See §34 and the FMEA (topology drift). The one-line rule: *application code should not have write access to production topology.*

---

## 6. Consumer Implementation (async)

Use the **async broker** in production: aio-pika `connect_robust` gives automatic connection recovery, real publisher confirms, and per-queue channels. (The sync broker is fine for simple workers — its recovery loop was added recently — but async is the default recommendation.)

```python
# models.py
from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel, Field

class OrderCreated(BaseModel):
    order_id: str = Field(min_length=1)
    tenant_id: str
    amount_cents: int = Field(ge=0)
    currency: str = Field(pattern="^[A-Z]{3}$")
    created_at: datetime
    event_version: int = 1
```

```python
# errors.py
#
# CRITICAL: rabbitkit's classifier is TYPE-BASED (see §7). It checks isinstance
# against built-in tuples. An exception that matches NEITHER tuple falls through
# to RetryConfig.unknown_policy (PERMANENT) and is NOT retried. So custom
# "transient" errors MUST subclass a recognized transient base, or they will
# silently go straight to the DLQ.
#
#   TRANSIENT_ERRORS = (ConnectionResetError, BrokenPipeError,
#                       ConnectionAbortedError, TimeoutError, EOFError, OSError)
#   PERMANENT_ERRORS = (json.JSONDecodeError, KeyError, ValueError,
#                       TypeError, UnicodeDecodeError, AttributeError)

class TransientError(OSError):       # OSError ∈ TRANSIENT_ERRORS → retried
    """Retry me: downstream blip, timeout, lock contention."""

class PermanentError(ValueError):    # ValueError ∈ PERMANENT_ERRORS → straight to DLQ
    """Do not retry: bad data, business invariant violated."""

class DownstreamUnavailable(TransientError): ...   # → transient
class InvalidTenant(PermanentError): ...           # → permanent
class DuplicateOrder(PermanentError): ...          # non-retryable duplicate → permanent
```

```python
# broker.py
from __future__ import annotations
import redis.asyncio as aioredis

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.serialization.pipeline import SerializationPipeline, JsonParser, PydanticDecoder
from rabbitkit.di.resolver import DIResolver
from rabbitkit.middleware.retry import RetryMiddleware
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware
from rabbitkit.middleware.timeout import TimeoutMiddleware, TimeoutConfig
from rabbitkit.middleware.exception import ExceptionMiddleware
from rabbitkit.middleware.otel import OTelTracingMiddleware
from rabbitkit.core.config import DeduplicationConfig

from .config import build_config
# NOTE: there is no classifier object to import — RetryMiddleware classifies by
# exception TYPE + RetryConfig.unknown_policy (see §7). Custom errors carry their
# severity by subclassing the built-in transient/permanent base classes.

config = build_config(env="production")

broker = AsyncBroker(
    config,
    serializer=SerializationPipeline(JsonParser(), PydanticDecoder()),  # → Pydantic models
    di_resolver=DIResolver(),
)

redis = aioredis.from_url("redis://redis.internal:6379/0")

# ── Middlewares (instances are shared across messages; construct once) ──
trace_mw  = OTelTracingMiddleware(service_name="order-service")
exc_mw    = ExceptionMiddleware(swallow_permanent=False)  # let terminal errors surface → DLQ
cb_mw     = CircuitBreakerMiddleware(async_circuit_breaker=...)  # any CircuitBreakerProtocol impl (§12)
dedupe_mw = DeduplicationMiddleware(
    redis, DeduplicationConfig(key_prefix="orders:dedup", ttl=86400, key_source="message_id"),
    key_fn=lambda m: m.headers.get("x-idempotency-key") or m.message_id,  # business key first
)
# CRITICAL: publish_async_fn=broker.publish — this is what makes retries actually publish AND
# what makes the "failed publish → nack not ack" safety net work (it needs a real PublishOutcome).
retry_mw   = RetryMiddleware(config.retry, publish_async_fn=broker.publish)
timeout_mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=15.0))

ORDER_MIDDLEWARES = [trace_mw, exc_mw, cb_mw, dedupe_mw, retry_mw, timeout_mw]  # order matters (§15)
```

```python
# handlers/orders.py
from __future__ import annotations
from typing import Annotated

from rabbitkit.core.types import AckPolicy
from rabbitkit.di.depends import Depends

from .broker import broker, config, ORDER_MIDDLEWARES
from .models import OrderCreated
from .errors import DownstreamUnavailable, InvalidTenant, DuplicateOrder
from .services import OrderService, get_order_service

@broker.subscriber(
    queue="orders.queue",
    exchange="orders.exchange",
    routing_key="orders.created",
    ack_policy=AckPolicy.NACK_ON_ERROR,  # REQUIRED for retry+DLQ: terminal failures
                                         # nack(requeue=False) → dead-letter to DLQ.
                                         # AUTO would requeue=True on exhausted-transient → hot loop (§8).
    retry=config.retry,                  # declares delay queues + DLQ topology (NOT behavior!)
    middlewares=ORDER_MIDDLEWARES,       # RetryMiddleware here is what actually retries
    name="order_created",
    description="Create an order from an orders.created event.",
)
async def handle_order_created(
    event: OrderCreated,                                   # body → validated Pydantic model
    svc: Annotated[OrderService, Depends(get_order_service)],
) -> None:
    # idempotency at the business layer (dedupe middleware is a fast-path, NOT the source of truth)
    if await svc.already_processed(event.order_id, event.event_version):
        return  # safe no-op; ack

    try:
        await svc.create_order(event)        # opens its own DB tx; outbox for follow-up events (§13)
    except TimeoutError as e:                # downstream slow
        raise DownstreamUnavailable(str(e)) from e
    except svc.UnknownTenant as e:
        raise InvalidTenant(str(e)) from e   # PERMANENT → straight to DLQ, no retries
    except svc.AlreadyExists as e:
        raise DuplicateOrder(str(e)) from e  # PERMANENT (business decided dupes are terminal)
```

Notes:
- **`AckPolicy.NACK_ON_ERROR`** = ack on success, `nack(requeue=False)` on any unhandled exception. `RetryMiddleware` acks the source on each *non-terminal* retry (routing a copy to the delay queue), so the only exceptions that reach the pipeline are **terminal** (permanent or retries exhausted) — and `NACK_ON_ERROR` dead-letters those to the DLQ. **Do not use `AckPolicy.AUTO` here:** AUTO's exception path re-classifies and does `nack(requeue=True)` for transient errors, so an *exhausted-transient* message (downstream down for the whole retry window) gets requeued instantly and hot-loops — the exact storm this design prevents. See §8.
- **Pydantic validation failure** raises inside deserialization → classified PERMANENT → DLQ. No retries on malformed data.
- **Transient vs permanent** is expressed by **exception type**, consumed by the classifier in §7.

---

## 7. Error Classification

The classifier is the brain of the retry system. **In rabbitkit today, classification is purely TYPE-BASED.** `RetryMiddleware` constructs its classifier internally as `ErrorClassifierMiddleware(unknown_policy=config.unknown_policy)` — it forwards only `unknown_policy` and **does not expose a way to inject predicates**. Evaluation order inside `classify_error`:

1. `isinstance(exc, TRANSIENT_ERRORS)` → **transient** (retry)
2. `isinstance(exc, PERMANENT_ERRORS)` → **permanent** (DLQ)
3. otherwise → `RetryConfig.unknown_policy` (default **PERMANENT**)

```python
TRANSIENT_ERRORS = (ConnectionResetError, BrokenPipeError, ConnectionAbortedError,
                    TimeoutError, EOFError, OSError)   # OSError is the broad transient base
PERMANENT_ERRORS = (json.JSONDecodeError, KeyError, ValueError,
                    TypeError, UnicodeDecodeError, AttributeError)
```

Two consequences you must design around:
- **`HandlerTimeoutError` ⊂ `TimeoutError` ⊂ transient** → handler timeouts retry automatically. ✅
- **Pydantic v2 `ValidationError` ⊂ `ValueError` ⊂ permanent** → schema/validation failures go straight to DLQ, no retries. ✅
- A bare `Exception` or a third-party error (e.g. `httpx.HTTPStatusError` for a 503) matches **neither** tuple → it becomes PERMANENT and is **not retried**. To make it retry you must give it a transient *type*. The supported mechanism is **exception mapping at the handler boundary** (catch the downstream error, re-raise as a transient/permanent base subclass — exactly what §6's handler does).

> **Library limitation (be honest with your team).** Predicate-based classification (e.g. "any HTTP 5xx → transient") exists in the lower-level `classify_error()` / `ErrorClassifierMiddleware(predicates=...)` API, but **`RetryMiddleware` does not thread predicates through** in this version. Until that's added, do classification by *type* (map at the handler boundary) — do **not** write a predicate-based classifier and expect retry to honor it; it won't.

**Taxonomy:**

| Class | Examples | Action |
|---|---|---|
| **Transient** | network timeout, DB connection timeout, HTTP 502/503/504, Redis blip, AMQP interruption, downstream rate-limit (429), lock contention, `HandlerTimeoutError` | retry with backoff |
| **Permanent** | invalid schema, missing field, unauthorized tenant, unsupported event type, business invariant violation, non-retryable duplicate, invalid signature, malformed JSON | straight to DLQ |
| **Ambiguous** | unknown exceptions, unexpected DB integrity errors, third-party 400, serialization edge cases | `unknown_policy` decides |

The **working** pattern is exception mapping at the handler boundary — translate downstream/library errors into your transient/permanent base classes (§6) *before* they reach `RetryMiddleware`. This is the only mechanism `RetryMiddleware` actually honors.

```python
# error_mapping.py — call this at the edges of your handler / service layer.
from __future__ import annotations
import httpx

from .errors import DownstreamUnavailable, PermanentError   # OSError/ValueError subclasses

def map_http_error(exc: httpx.HTTPStatusError) -> Exception:
    """Translate an HTTP error into a rabbitkit-classifiable exception by TYPE."""
    status = exc.response.status_code
    if status in (429, 502, 503, 504):
        return DownstreamUnavailable(f"HTTP {status}")  # ⊂ OSError → TRANSIENT → retried
    return PermanentError(f"HTTP {status}")             # ⊂ ValueError → PERMANENT → DLQ

# usage inside a handler:
#   try:
#       resp = await client.post(...); resp.raise_for_status()
#   except httpx.HTTPStatusError as e:
#       raise map_http_error(e) from e
```

`unknown_policy` is set on the `RetryConfig` (broker-wide, or per-route via `@subscriber(retry=RetryConfig(unknown_policy=...))`):

```python
from rabbitkit import RetryConfig
from rabbitkit.core.types import ErrorSeverity

# critical route: never retry surprises
ORDERS_RETRY = RetryConfig(max_retries=4, delays=(5, 30, 120, 600),
                           unknown_policy=ErrorSeverity.PERMANENT)

# low-criticality, infra-flaky stream: surprises are usually transient
METRICS_RETRY = RetryConfig(max_retries=2, delays=(5, 30),
                            unknown_policy=ErrorSeverity.TRANSIENT)
```

**Why `unknown_policy=PERMANENT` by default in critical systems.** An unknown exception is, by definition, one you did not anticipate. Retrying it 4× over 12 minutes rarely helps and often *amplifies* a novel failure (e.g. a code bug that throws on every message → 5× the load, 5× the logs). Sending it to the DLQ stops the bleeding, preserves the message, and forces a human to look. **Fail visible, not loud-and-infinite.**

**When `unknown_policy=TRANSIENT` is acceptable:** low-criticality, high-volume streams where the dominant failure mode is genuinely transient infra (e.g. metrics ingestion behind a flaky collector) and a DLQ flood would be noisier than a few extra retries. Even then, cap `max_retries` low (1–2) and alert on retry rate.

**Avoiding infinite retries:** three independent guards — (1) `max_retries` in `RetryConfig`, (2) `x-rabbitkit-retry-count` we own and check, (3) quorum-queue `x-delivery-limit` as a broker-level backstop if anything bypasses the middleware.

**Error metadata on headers:** attach `x-error-type`, `x-error-message` (truncated), `x-error-stack-hash` to the retry/DLQ envelope (see §9). **Preserve `message_id` and `correlation_id`** across retries — rabbitkit copies the body and original routing metadata; ensure your producer set `correlation_id`, and never regenerate `message_id` on retry (it's the dedupe key).

---

## 8. Ack / Nack / Reject Semantics

| Action | Meaning | Use when | Danger |
|---|---|---|---|
| **ack** | done; remove from queue | success; safe duplicate skipped; permanent msg *after* its failure is recorded | acking before the side-effect is durable = loss on crash |
| **nack, requeue=false** | not processed; don't put back | let DLX/DLQ topology handle it (terminal) | — |
| **nack, requeue=true** | put back now | *only* short-lived local contention (e.g. optimistic-lock retry within ms) | blind use = **hot loop / retry storm** |
| **reject, requeue=false** | invalid; discard (→ DLX) | bad signature, unauthorized source, poison | — |

**Policy mapping in rabbitkit:**

- **`AckPolicy.NACK_ON_ERROR`** — success→ack, any unhandled exception→`nack(requeue=False)` → dead-letter to DLQ. **This is the correct policy for retry+DLQ business workflows.** Because `RetryMiddleware` settles (acks) every non-terminal retry itself, the only exceptions reaching this handler are terminal — and they route straight to the DLQ with no requeue.
- **`AckPolicy.AUTO`** — success→ack; on exception the pipeline **re-classifies** and does `nack(requeue=True)` for transient or `reject(requeue=False)` for permanent. **Dangerous on retry-enabled routes:** an exhausted-transient terminal error is still "transient" by type, so AUTO requeues it with `requeue=True` → instant reprocess → terminal again → **hot loop** for the entire duration of a downstream outage. Use AUTO only on routes *without* retry where you explicitly want transient errors to requeue-in-place (rare). For anything with a DLQ, prefer `NACK_ON_ERROR`.
- **`AckPolicy.ACK_FIRST`** — **at-most-once.** Acks *before* the handler runs. If the handler crashes, the message is gone. **Only** for fire-and-forget analytics where loss is acceptable. **Never** for orders/payments.
- **`AckPolicy.MANUAL`** — you call `message.ack()/nack()/reject()` yourself. Use when the ack must coincide with a transaction boundary you control. Requires discipline: ack exactly once, never after the connection may have dropped (delivery tag is channel-scoped).

```python
# CORRECT: manual ack tied to a committed transaction
@broker.subscriber(queue="orders.queue", ack_policy=AckPolicy.MANUAL, middlewares=[...])
async def handle(event: OrderCreated, msg: RabbitMessage, svc=Depends(get_order_service)) -> None:
    try:
        await svc.create_order(event)   # commits inside
        await msg.ack_async()           # ack ONLY after commit succeeded
    except TransientError:
        await msg.nack_async(requeue=False)   # → retry topology / DLX
    except PermanentError:
        await msg.reject_async(requeue=False) # → DLQ

# WRONG: ack before the work is durable (loses the message if create_order crashes after ack)
async def bad(event, msg, svc):
    await msg.ack_async()               # ❌ acked too early
    await svc.create_order(event)       # crash here = order lost forever

# WRONG: requeue=true on a downstream outage (hot loop)
async def also_bad(event, msg):
    try: ...
    except DownstreamUnavailable:
        await msg.nack_async(requeue=True)  # ❌ instantly redelivered; storms while downstream is down
```

---

## 9. Retry Header Design

rabbitkit auto-sets: `x-rabbitkit-retry-count`, `x-rabbitkit-original-exchange`, `x-rabbitkit-original-routing-key`, `x-rabbitkit-original-queue`. Everything else below you add yourself (producer or enrichment middleware).

| Header | Purpose | PII risk | Notes |
|---|---|---|---|
| `x-rabbitkit-retry-count` | attempt counter | none | **auto** |
| `x-rabbitkit-original-exchange` / `-routing-key` / `-original-queue` | restore routing on replay | none | **auto** |
| `x-correlation-id` | tie to originating request | low | propagate end-to-end |
| `x-trace-id` / `traceparent` | distributed trace link | none | W3C trace context |
| `x-tenant-id` | per-tenant triage/replay | **maybe** — opaque id only, never names/emails | |
| `x-idempotency-key` | business dedupe key | depends — hash, don't embed PII | dedupe key source |
| `x-first-failure-at` / `x-last-failure-at` | failure window | none | ISO-8601 UTC |
| `x-error-type` | exception class name | none | for DLQ grouping |
| `x-error-message` | short reason | **maybe** — truncate, scrub | cap ~256 chars |
| `x-error-stack-hash` | dedupe identical failures | none | hash, **not** the stack |

**Rules:**
- **Never put a full stack trace in a header.** Headers are sent with every redelivery (size/bandwidth), are visible in the management UI (access control), and bloat the message. Log the stack with the `x-error-stack-hash` as the join key; keep only the hash on the wire.
- **No PII in headers.** Headers are the least-protected part of the message and the most-exposed (mgmt UI, dashboards, logs). Use opaque ids and hashes.
- **Message size constraints.** Keep total headers well under a few KB. Large headers hurt throughput and can hit frame limits. Truncate `x-error-message`.
- **Preserve context across retries.** Always copy `message_id` and `correlation_id` unchanged. Losing `message_id` breaks idempotency; losing `correlation_id` breaks incident forensics.

```python
# middleware-style enrichment (publish-side) to add forensic headers on retry/DLQ
# (sketch — implement as a BaseMiddleware that wraps publish_scope_async)
def enrich_failure_headers(env, exc, retry_count):
    now = datetime.now(UTC).isoformat()
    env.headers.setdefault("x-first-failure-at", now)
    env.headers["x-last-failure-at"] = now
    env.headers["x-error-type"] = type(exc).__name__
    env.headers["x-error-message"] = str(exc)[:256]
    env.headers["x-error-stack-hash"] = hashlib.sha256(traceback.format_exc().encode()).hexdigest()[:16]
    return env
```

---

## 10. DLQ Design — an operational product

**Naming:** `per_queue=True` → `<queue>.dlq` (e.g. `orders.queue.dlq`). One DLQ per source queue keeps blast radius and triage scoped.

**Lifecycle of a DLQ message:** *land → inspect → classify → root-cause → fix → dry-run → replay in batches → audit.* Never *land → purge*.

**Operations with `DLQInspector`** (constructed from the broker's transport):

```python
# dlq_tools.py
from rabbitkit.dlq import DLQInspector
from rabbitkit.core.message import RabbitMessage

inspector = DLQInspector(broker._transport)   # or the async transport

# peek (non-destructive-ish: basic_get + nack requeue=true; MAY reorder — see caveat)
msgs: list[RabbitMessage] = await inspector.peek_async("orders.queue.dlq", limit=50)

# replay ONLY timeout failures, back to the original queue
def only_timeouts(m: RabbitMessage) -> bool:
    return m.headers.get("x-error-type") == "HandlerTimeoutError"
n = await inspector.replay_async("orders.queue.dlq", predicate=only_timeouts)

# replay only one tenant's messages to a quarantine queue (not back into the hot path)
def tenant_42(m: RabbitMessage) -> bool:
    return m.headers.get("x-tenant-id") == "42"
n = await inspector.replay_async(
    "orders.queue.dlq", predicate=tenant_42, target_queue="orders.queue.quarantine",
)

# purge — TEST/staging only; production requires change approval + audit (§31)
await inspector.purge_async("orders.queue.dlq")   # ⚠️ irreversible
```

**Caveats baked into the library (be honest about them):**
- `peek` uses `basic_get` + `nack(requeue=true)`, which **can change message order**. Treat peek as sampling, not a stable snapshot. For stable inspection, prefer the management API to read counts and a shadow/quarantine queue for content.
- `DLQInspector` has **no built-in dry-run, throttling, or batching**. Build controlled replay yourself (§31) — replay-all into a degraded system is how you turn one incident into two.

**Replay targets:**
- **Original queue** — after the root cause is fixed and deployed. The common case.
- **Quarantine queue** — when you want to re-examine without re-entering the hot path.
- **Parking-lot queue** — long-term hold for messages that need product/data decisions, not code fixes.

**Permissions & audit:** the DLQ user that can `purge`/`delete` must be separate from the service user (§27). Every replay/purge is logged with operator, count, predicate, and reason (§31).

---

## 11. Poison Message Handling

A **poison message** is one that fails *deterministically* — it will never succeed no matter how many times you retry (corrupt payload, a data shape your code can't handle, an event referencing a deleted entity).

**Detection signals:**
- A single `message_id` exhausts all retries and lands in DLQ.
- One `x-error-stack-hash` / `x-error-type` dominates the DLQ.
- DLQ **growth rate** spikes (the real alert — see §22).
- Validation-error rate spikes right after a producer deploy (schema drift).
- Consumer pods crash-loop (a message that kills the process, not just the handler).

**Handling:**
1. **Bounded retries + classification** already route most poison straight to DLQ (PERMANENT) or after `max_retries`.
2. **Quorum `x-delivery-limit`** is the backstop for messages that crash the consumer *before* the middleware can classify them — RabbitMQ dead-letters after N delivery attempts.
3. **Fingerprinting:** group DLQ messages by `x-error-stack-hash` to find the dominant poison class fast.
4. **Quarantine + manual triage:** replay poison to a quarantine queue, not back to main.
5. **Runbook escalation:** crash-loop → roll back the deploy (§24.4 / §24.11), then triage the DLQ.

**Never** let a poison message sit on the main queue with `requeue=true` — that is a crash loop or a hot loop depending on whether it kills the process or just the handler.

---

## 12. Idempotency and Deduplication

**The rule: every retried consumer MUST be idempotent.** At-least-once guarantees you *will* reprocess. Dedupe middleware reduces duplicates; it does **not** eliminate them (Redis can evict, lag, or be down). Business idempotency is the real guarantee.

**Idempotency keys, in priority order:**
1. `x-idempotency-key` (explicit business key) — best.
2. Business natural key (`order_id` + `event_version`).
3. `correlation_id`.
4. `message_id` (last resort; survives retries because rabbitkit preserves it).

**Two layers:**

**Layer 1 — fast-path dedupe (Redis, `DeduplicationMiddleware`):**
```python
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
from rabbitkit.core.config import DeduplicationConfig

dedupe_mw = DeduplicationMiddleware(
    redis,
    DeduplicationConfig(
        key_prefix="orders:dedup",
        ttl=86400,                       # 24h: ≥ max retention (755s) × safety margin × clock skew
        key_source="message_id",         # or correlation_id / body_hash
        fallback_on_redis_error=True,    # Redis down → PROCESS anyway (Layer 2 catches dupes)
    ),
    key_fn=lambda m: m.headers.get("x-idempotency-key") or m.message_id,
)
```
- **TTL selection:** must exceed the longest possible redelivery window (sum of delays + processing + replay horizon). Too short → a legitimately-delayed retry is treated as new and double-processed. 24h is a safe default for the 12.5-min policy.
- **`fallback_on_redis_error=True`:** when Redis is unavailable, *process the message* and rely on Layer 2. The alternative (block all processing on Redis) makes Redis a hard dependency for your message pipeline — usually worse. Alert on the fallback (§22).
- **`body_hash` risks:** two semantically-identical events with different non-meaningful fields (timestamps, ordering of JSON keys) hash differently → dedupe misses. And a retried message with enriched headers but identical body hashes the same → fine. Prefer an explicit key over body hashing.

**Layer 2 — business idempotency (database, the source of truth):**
```sql
-- processed_messages: the inbox/dedupe table (see §13)
CREATE TABLE processed_messages (
    idempotency_key TEXT PRIMARY KEY,         -- order_id + event_version, or message_id
    order_id        TEXT NOT NULL,
    event_version   INT  NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    result_hash     TEXT
);
```
The `INSERT ... ON CONFLICT DO NOTHING` on this table inside the same transaction as the business change is what makes processing **actually** exactly-once-effecting, even though delivery is at-least-once.

**The exactly-once myth.** There is no exactly-once *delivery* over a network. There is only at-least-once delivery + idempotent processing = **effectively-once**. Design for that. Anyone promising exactly-once is hiding a dedupe table somewhere.

---

## 13. Transaction Boundaries

**Safe processing pattern (the spine of the consumer):**

```
1. Receive message
2. Validate (schema/business)                  → invalid? PermanentError → DLQ
3. Check idempotency (Redis fast-path)          → seen? ack, return
4. BEGIN DB transaction
5.   Apply business change
6.   INSERT processed_messages (idempotency_key) ON CONFLICT DO NOTHING
        → 0 rows affected = concurrent duplicate → ROLLBACK, ack, return
7.   INSERT into outbox(event)                   -- follow-up events, same tx
8. COMMIT
9. (outbox relay publishes follow-up events with confirms, separately)
10. ack
```

**Why this order:**
- **Idempotency insert inside the tx** makes the dedupe atomic with the business change. No window where the change commits but the dedupe record doesn't.
- **Transactional outbox** (step 7): never publish a follow-up event inside the consumer and then commit — if publish succeeds and commit fails, you emitted an event for work that didn't happen. Write the event to an `outbox` table in the *same* transaction; a separate relay publishes it with confirms and marks it sent. This decouples "did the work" from "told the world."
- **Inbox table** = `processed_messages`; it is the consumer-side dedupe.

**The two unavoidable failure windows (name them so on-call understands):**
- **Publish-after-commit risk:** if you publish *then* ack and crash between, the message redelivers and you publish again → duplicate downstream event. Outbox + idempotent consumers downstream handle it.
- **Ack-before-commit risk:** if you ack *then* commit and crash between, the work is lost (acked but not done). **Always commit before ack.**
- **Crash after DB commit, before ack:** the message redelivers; step 3/6 (idempotency) makes the reprocess a no-op. This is the *designed-for* case and proves why idempotency is mandatory.

```sql
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_id  TEXT NOT NULL,
    routing_key   TEXT NOT NULL,
    payload       JSONB NOT NULL,
    headers       JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at       TIMESTAMPTZ,
    attempts      INT NOT NULL DEFAULT 0
);
CREATE INDEX outbox_unsent ON outbox (created_at) WHERE sent_at IS NULL;
```

---

## 14. Backpressure and Flow Control

**Sources of backpressure:**
- RabbitMQ `connection.blocked` (broker memory/disk alarm) → publishes block.
- Publisher confirms outstanding (slow broker).
- Consumer `prefetch` (how many unacked deliveries you hold).
- Worker concurrency (how many you process at once).
- Rate limits (yours or a downstream's).
- Downstream latency (the real bottleneck most of the time).
- Retry/DLQ queue growth (a symptom — watch it).

**Levers in rabbitkit:**

```python
from rabbitkit.highload.backpressure import FlowController
from rabbitkit.core.config import BackpressureConfig
from rabbitkit.middleware.rate_limit import RateLimitMiddleware, RateLimitConfig
from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware
from rabbitkit.core.config import WorkerConfig

# publisher-side flow control (bound in-flight publishes; respect connection.blocked)
flow = FlowController(BackpressureConfig(max_in_flight=1000, on_blocked="wait", blocked_timeout=60.0))

# consumer rate limit (protect a fragile downstream)
rate_mw = RateLimitMiddleware(RateLimitConfig(max_rate=200, burst=50, on_limited="wait"))

# worker pool (concurrency) — pass to start()
await broker.start(worker_config=WorkerConfig(worker_count=8, prefetch_per_worker=5))
```

**Tuning guidance:**

| Workload | prefetch | worker_count | Why |
|---|---|---|---|
| CPU-bound (parsing, crypto) | low (1–2 × cores) | ≈ cores | More concurrency than cores just thrashes; prefetch high wastes memory |
| IO-bound (DB/HTTP calls) | higher (50–200) | high (async) | Hide latency with concurrency; async excels here |
| Slow downstream API | **low** | **low** + `RateLimitMiddleware` | High prefetch on a slow downstream = huge unacked backlog + redelivery storms on restart |
| High-throughput publish | n/a | n/a | `FlowController` + batching + confirms; bound in-flight |
| Burst traffic | moderate prefetch + HPA on queue depth | autoscale | Let queue absorb the burst; scale consumers, don't drop prefetch limits |
| Noisy tenant | `RateLimitMiddleware` keyed by tenant; or separate queue | isolate | One tenant must not starve others |

**Critical interaction:** `channel_pool_size` must be `>= worker_count`. Handlers that publish (retry, outbox, results) acquire a channel; if the pool is smaller than the worker count, concurrent publishers block on `channel_acquire_timeout` and you get latency cliffs or deadlock-like stalls. The broker warns about this at start — heed it.

**`connection.blocked`:** when the broker raises a memory/disk alarm, it stops accepting publishes. `FlowController(on_blocked="wait")` parks publishers (up to `blocked_timeout`) instead of erroring. Alert on blocked duration (§22) — it means the broker is in trouble.

---

## 15. Middleware Ordering

Middlewares wrap **outermost-first**: the first item in `middlewares=[...]` is the outermost layer; the handler is innermost.

**Recommended consume order (outer → inner):**

```python
ORDER_MIDDLEWARES = [
    trace_mw,      # 1. Tracing      — outermost: one span covers everything, incl. retries
    exc_mw,        # 2. Exception    — catch terminal errors, decide swallow vs surface
    cb_mw,         # 3. CircuitBreaker — fail fast BEFORE dedupe/handler when downstream is dead
    dedupe_mw,     # 4. Deduplication — skip duplicates before doing real work
    retry_mw,      # 5. Retry        — classify + route transient failures to delay queues
    timeout_mw,    # 6. Timeout      — bound handler time; HandlerTimeoutError is transient → retried
    rate_mw,       # 7. RateLimit    — closest to handler so it paces actual execution
    # compression decode happens via on_receive; signing verify via on_receive (see below)
]
```

**Reasoning:**
- **Tracing outermost** so the span includes dedupe hits, retries, and timeouts — you want the full picture in the trace.
- **Circuit breaker above retry/handler:** when the downstream is hard-down, the breaker rejects *immediately* (cheap), and that rejection is classified transient → retried later. If the breaker were *inside* retry, you'd burn all retry attempts hammering a dead downstream.
- **Dedupe before the handler** so duplicates cost a Redis GET, not a full transaction.
- **Timeout *inside* retry, not outside.** You want a timeout to **count as one failed attempt that gets retried**. If timeout wrapped retry, a slow message would be killed once with no backoff/retry. Inside retry: `HandlerTimeoutError` → classified transient → delay queue → try again later. (Caveat: the **sync** `TimeoutMiddleware` can't actually kill the handler thread — it abandons it and raises. Real cancellation needs **async** handlers + `asyncio.wait_for`. Another reason to prefer async.)
- **Rate limit closest to the handler** so it throttles real execution, not cheap rejections.

**Signing vs compression ordering (important):**
- If the signature must cover the **bytes on the wire** (tamper-evidence of the actual payload incl. compression): **compress, then sign** (publish: compress → sign; receive: verify → decompress).
- If the signature must cover the **logical payload** (so re-compression doesn't invalidate it): **sign, then compress** (publish: sign over raw → compress; receive: decompress → verify).
- Default recommendation: **sign the logical payload** (sign before compress). It survives transport-layer re-encoding and is what most consumers reason about. Document the choice; mismatched assumptions across services = every message rejected.

---

## 16. Publisher Guarantees

```python
# Producer with confirms; check the outcome.
from rabbitkit.core.types import MessageEnvelope, PublishStatus

outcome = await broker.publish(MessageEnvelope(
    exchange="orders.exchange",
    routing_key="orders.created",
    body=payload,                         # already serialized
    headers={"x-correlation-id": cid, "x-idempotency-key": order_id},
    correlation_id=cid,
    delivery_mode=2,                      # persistent
    mandatory=True,                       # surface unroutable (see below)
))
if not outcome.ok:                        # ok == (status == CONFIRMED)
    if outcome.status == PublishStatus.RETURNED:
        log.error("unroutable message", routing_key="orders.created")  # no queue bound!
    elif outcome.status == PublishStatus.TIMEOUT:
        # broker didn't confirm in confirm_timeout — DO NOT assume lost or delivered.
        # Re-publish is safe ONLY because consumers are idempotent. Otherwise reconcile.
        raise PublishConfirmTimeout(cid)
    else:
        raise PublishFailed(outcome.status, outcome.error)
```

**Guarantees and when to use them:**
- **Durable exchanges + queues + persistent messages:** survive restart. Always for business events.
- **Publisher confirms (`confirm_delivery=True`):** the broker acks once the message is safely enqueued (and persisted, for persistent msgs to durable queues). Without confirms, "publish returned" ≠ "broker has it." **Always on** for anything you can't afford to lose. rabbitkit returns a `PublishOutcome`; `.ok` is true only on `CONFIRMED`.
- **`mandatory=True`:** if no queue is bound for the routing key, the broker *returns* the message (status `RETURNED`) instead of silently dropping it. Use when an unroutable message is a bug you must catch. Cost: a return path and handling code. For high-volume non-critical streams where you accept drops, leave it `False`.
- **Confirm timeout:** the dangerous gray zone. A timeout means *unknown* — the broker may or may not have the message. Safe only if consumers are idempotent (then re-publish freely). Otherwise you need a reconciliation/outbox to resolve.

**Batching trade-offs:**
- **Acceptable:** high-throughput, loss-tolerant or idempotent-downstream streams where you batch and confirm the batch.
- **Dangerous:** rabbitkit's `BatchPublisher` buffer is **unbounded** — a stall in flushing grows memory without limit, and an un-flushed buffer on crash is lost. **Never** batch critical messages without (a) bounding the buffer yourself and (b) per-batch confirms. For orders, prefer the outbox + single confirmed publishes.

---

## 17. Consumer Concurrency

| Lever | Effect |
|---|---|
| `prefetch_count` | max unacked deliveries held by a consumer; the throughput/fairness knob |
| `worker_count` (`WorkerConfig`) | concurrent handler executions (`AsyncWorkerPool` semaphore / `SyncWorkerPool` threads) |
| Single Active Consumer | one consumer at a time per queue → ordering + failover, capped throughput |
| horizontal scaling | more pods = more consumers = more parallelism (until downstream or queue is the bottleneck) |

**Examples:**

```python
# (a) low-latency API enrichment — IO-bound, moderate concurrency
await broker.start(worker_config=WorkerConfig(worker_count=16))   # prefetch ~50, async handlers

# (b) high-throughput event processing — many pods, fair dispatch
await broker.start(worker_config=WorkerConfig(worker_count=32, prefetch_per_worker=10))

# (c) strict ordering per order_id — Single Active Consumer + worker_count=1
#     declare queue with x-single-active-consumer=true; one pod processes, others stand by.
#     Throughput = single consumer. For per-key ordering at scale, partition by hashing
#     order_id to multiple queues (orders.queue.0..N) instead.

# (d) long-running jobs — LOW prefetch so a slow job doesn't hoard deliveries
await broker.start(worker_config=WorkerConfig(worker_count=4, prefetch_per_worker=1))
```

**Ordering reality:** RabbitMQ preserves order *within a single queue to a single consumer*. The moment you have multiple consumers or a worker pool, ordering is gone. If you need per-entity ordering, either SAC (one consumer, slow) or **partition by key into N queues** (ordering within a partition, parallelism across partitions). Don't pretend a worker pool preserves order.

**Hot partitioning:** if one key (one big tenant) dominates, partitioning by `hash(key)` can put all its load on one partition. Use a partitioning scheme that spreads the heavy keys, or give big tenants dedicated queues.

**Fair dispatch:** with `prefetch` set, RabbitMQ round-robins only up to each consumer's unacked limit, so a slow consumer doesn't get flooded while a fast one idles. Prefetch is your fairness control.

**Kubernetes replicas:** scale pods on **queue depth / consumer lag**, not just CPU (a consumer waiting on a slow DB is idle-CPU but deeply backlogged). See §18.

---

## 18. Kubernetes and Deployment

```yaml
# deployment.yaml (excerpt)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 3
  selector: { matchLabels: { app: order-service } }
  template:
    metadata: { labels: { app: order-service } }
    spec:
      terminationGracePeriodSeconds: 45      # > consumer.graceful_timeout (30s) + drain margin
      containers:
        - name: order-service
          image: registry/order-service:1.4.2
          envFrom:
            - configMapRef: { name: order-service-config }
            - secretRef:    { name: order-service-secrets }   # RABBITMQ_PASSWORD, etc.
          resources:
            requests: { cpu: "250m", memory: "256Mi" }
            limits:   { cpu: "1",    memory: "512Mi" }
          lifecycle:
            preStop:
              exec:
                # stop taking new work, let in-flight drain before SIGTERM path completes
                command: ["/bin/sh", "-c", "kill -SIGTERM 1; sleep 35"]
          startupProbe:                       # don't kill a slow-connecting pod
            httpGet: { path: /health/startup, port: 8080 }
            failureThreshold: 30
            periodSeconds: 2
          readinessProbe:                     # remove from "ready" if broker connection is down
            httpGet: { path: /health/ready, port: 8080 }
            periodSeconds: 5
          livenessProbe:                      # restart only on true deadlock (be conservative)
            httpGet: { path: /health/live, port: 8080 }
            periodSeconds: 10
            failureThreshold: 6
```

**Graceful shutdown (the part everyone gets wrong):**
- On SIGTERM: **stop consuming** (cancel consumers), let in-flight handlers finish within `graceful_timeout`, **commit + ack**, then close connections. rabbitkit's `broker.stop()` cancels consumers and drains the worker pool — call it from your signal handler / FastAPI lifespan.
- `terminationGracePeriodSeconds` must exceed `graceful_timeout` + drain margin, or Kubernetes SIGKILLs you mid-message → redelivery (safe because idempotent, but noisy).
- `preStop` sleep gives the load balancer / service mesh time to stop routing before the process exits.
- **What happens if a handler is still running when `graceful_timeout` elapses** differs by transport: the **async** broker explicitly cancels the still-running handler task and nacks its message with `requeue=True` (delivery tag logged), so the broker redelivers it immediately. The **sync** broker has no way to safely interrupt a handler running on a plain Python thread — it logs a count and disconnects anyway, leaving that message **unacked and abandoned**; the broker only redelivers it once it notices the connection is gone (typically fast, but not instant, and not delivery-tag-logged the way the async path is). Either way, **that handler execution may still be running its own side effects** (DB writes, external API calls) after the process has moved on — the same message ends up processed again on redelivery. Handlers MUST be idempotent under at-least-once delivery regardless of transport; this is just the mechanism by which the "duplicate execution" case actually gets triggered during a shutdown that races a slow handler.

**Health probes for a consumer (not an HTTP server):**
- **startup:** broker connected + topology asserted.
- **readiness:** broker connected + consumers active (use `broker_health_check` → `BrokerHealthResult`). Not-ready → drain, don't kill.
- **liveness:** only fail on genuine deadlock (e.g. event loop stuck). A too-aggressive liveness probe restarts a pod that's merely waiting on a slow downstream → cascading restarts.

```python
# health endpoints from rabbitkit's health check
from rabbitkit.health import broker_health_check_async
from rabbitkit.core.config import HealthCheckConfig

async def ready():
    r = await broker_health_check_async(broker, HealthCheckConfig(pending_threshold=100))
    return (200 if r.status == "healthy" else 503), r.__dict__
```

**Autoscaling:** prefer **KEDA** with a RabbitMQ scaler on **queue depth** (and/or consumer lag), with CPU as a secondary signal. Set sane `min`/`max` replicas and a stabilization window so a transient spike doesn't thrash. Never autoscale consumers past what the **downstream** can take — more consumers on a saturated DB just moves the queue from RabbitMQ to the DB connection pool.

---

## 19. Helm Values

```yaml
# values.yaml
image:
  repository: registry/order-service
  tag: "1.4.2"

replicaCount: 3

rabbitmq:
  host: rabbitmq.internal
  port: 5671
  vhost: /orders
  usernameSecret: { name: order-service-secrets, key: RABBITMQ_USER }
  passwordSecret: { name: order-service-secrets, key: RABBITMQ_PASSWORD }
  tls:
    enabled: true
    caSecret: rabbitmq-ca

consumer:
  prefetch: 20
  workerCount: 8
  gracefulTimeout: 30

retry:
  maxRetries: 4
  delays: "5,30,120,600"
  jitterFactor: 0.1
  unknownPolicy: PERMANENT

topologyMode: PASSIVE_ONLY      # production

logging:
  json: true
  level: INFO

dashboards:
  enabled: true
  auth: oidc                    # never expose unauthenticated

resources:
  requests: { cpu: 250m, memory: 256Mi }
  limits:   { cpu: 1,    memory: 512Mi }

autoscaling:                    # KEDA
  enabled: true
  minReplicas: 3
  maxReplicas: 20
  queueDepthTarget: 1000        # scale up when orders.queue > 1000 ready msgs/replica
  cooldownSeconds: 120
```

These map to env vars (`RABBITMQ_*`) consumed by `RabbitSettings.to_rabbit_config()` (§28).

---

## 20. RabbitMQ Policies

Set via policy (not per-queue args) so they're centrally managed and overridable by ops:

```bash
# Quorum + delivery limit (poison guard) for main + DLQ
rabbitmqctl set_policy orders-quorum "^orders\.queue$" \
  '{"queue-type":"quorum","delivery-limit":20,"dead-letter-exchange":"","dead-letter-routing-key":"orders.queue.dlq"}' \
  --apply-to queues --priority 10

# DLQ retention (optional) + length cap
rabbitmqctl set_policy orders-dlq "^orders\.queue\.dlq$" \
  '{"queue-type":"quorum","message-ttl":1209600000,"max-length":1000000,"overflow":"reject-publish"}' \
  --apply-to queues --priority 10

# Classic delay queues: lazy mode for deep backlogs (do NOT apply to quorum)
rabbitmqctl set_policy orders-retry-lazy "^orders\.queue\.retry\." \
  '{"queue-mode":"lazy"}' --apply-to queues --priority 5
```

**Recommendations and when *not* to:**
- **Quorum queues:** main + DLQ. **Not** for delay queues (extra replication overhead for transient data; classic+durable is fine) and **not** for very-high-churn ephemeral queues.
- **`delivery-limit`:** quorum-only poison backstop. **Not** a replacement for app-level classification — it's a safety net for crashes before classification.
- **`max-length` + `overflow`:** hard cap. Use `reject-publish-dlx` to dead-letter overflow, or `reject-publish` to push back on producers. **Not** with `drop-head` for business data (silently drops oldest).
- **Lazy queues:** classic only, good for big DLQs/backlogs. **Not** with quorum.
- **`max-priority`:** only if you genuinely need priority lanes — it adds overhead and complicates ordering reasoning. Usually a separate high-priority queue is clearer.
- **Federation/Shovel:** only for cross-cluster/cross-DC topologies. Don't introduce for a single cluster.

---

## 21. Observability

**Structured logs (one event per message outcome, JSON):**
```json
{"ts":"...","level":"info","event":"message_processed","message_id":"...","correlation_id":"...",
 "trace_id":"...","queue":"orders.queue","routing_key":"orders.created","tenant_id":"42",
 "handler":"order_created","retry_count":1,"outcome":"retried","error_type":"DownstreamUnavailable",
 "error_hash":"9f3a...","processing_time_ms":182}
```
Fields: `message_id`, `correlation_id`, `trace_id`, `queue`, `routing_key`, `tenant_id`, `handler`, `retry_count`, `error_type`, `error_hash`, `processing_time_ms`, `outcome` (`ok|retried|dlq|duplicate`). rabbitkit's `LoggingConfig` + structlog binds the per-message context.

**Metrics (Prometheus via `MetricsMiddleware` + `PrometheusCollector`):**

| Metric | Type | Labels | Why |
|---|---|---|---|
| `rabbitkit_messages_consumed_total` | counter | queue, status | throughput + error rate |
| `rabbitkit_message_processing_seconds` | histogram | queue | latency SLI |
| `rabbitkit_messages_published_total` | counter | exchange, status | publish success rate |
| `rabbitkit_message_publish_seconds` | histogram | exchange | confirm latency |
| `messages_retried_total` | counter | queue, attempt | retry health |
| `messages_dlq_total` | counter | queue, error_type | the alert that matters |
| `queue_depth` (from mgmt API) | gauge | queue | backlog / lag |
| `dedupe_hits_total` / `dedupe_fallback_total` | counter | — | dupe rate + Redis health |
| `circuit_breaker_open_total` | counter | name | downstream health |
| `timeout_total`, `rate_limited_total` | counter | queue | saturation |
| `connection_blocked_seconds`, `reconnect_total` | counter/gauge | — | broker/connection health |

Scrape `queue_depth`, `messages_ready`, `messages_unacknowledged`, `messages_dlq` from `RabbitManagementClient` (§30) — these come from the broker, not the app, and survive app outages.

**Tracing (OpenTelemetry via `OTelTracingMiddleware`):** consume span + publish span, trace context propagated through headers (`traceparent`), child spans for DB and external HTTP. A single trace shows: receive → dedupe → handler → DB tx → outbox publish, *including* retries (because tracing is outermost). This is what lets you answer "why is this order stuck?" in one click.

**Dashboards:** service overview (throughput/latency/error), queue health (depth/age/consumers), retry health (retry rate, attempt distribution), **DLQ triage** (depth, growth rate, top `error_type`), downstream dependency health (breaker state, downstream latency), saturation (prefetch utilization, worker pool pending, connection blocked), SLO dashboard (§23).

---

## 22. Alerts

Every alert: condition, threshold, duration, impact, owner, runbook, first action.

| Sev | Alert | Condition | For | Impact | Runbook | First action |
|---|---|---|---|---|---|---|
| **Crit** | DLQ growing fast | `rate(messages_dlq_total[5m]) > 1/s` | 5m | work failing en masse | §24.1 | check top `error_type`; suspect recent deploy |
| **Crit** | Main queue age > SLO | `queue oldest msg age > 120s` | 5m | SLO breach, customer waiting | §24.3 | check consumers up; scale; check downstream |
| **Crit** | No consumers | `consumers == 0 AND messages_ready > 0` | 1m | nothing processing | §24.4 | check pods; rollback if crash-loop |
| **Crit** | Consumer crash loop | `rate(pod_restarts[10m]) > 3` | 10m | poison killing process | §24.4 | rollback deploy; quarantine poison |
| **Crit** | Connection blocked | `connection_blocked_seconds > 60` | 1m | publishes stalling; broker alarm | §24.5 | check broker memory/disk |
| **Crit** | Broker node unhealthy | mgmt `health_check == false` | 1m | platform | platform RB | page platform |
| **Warn** | Retry queue growth | `sum(retry depth) rising 15m` | 15m | downstream degraded | §24.2 | check breaker + downstream |
| **Warn** | High retry rate | `rate(messages_retried_total[10m]) > baseline×3` | 10m | partial degradation | §24.7 | identify error_type |
| **Warn** | Publish confirm latency | `p99 publish_seconds > 1s` | 10m | producer slowdown | §24.13 | check broker load |
| **Warn** | Processing latency high | `p99 processing_seconds > SLO×0.8` | 10m | approaching breach | §24.3 | check downstream |
| **Warn** | Circuit breaker open | `circuit_breaker_open_total increasing` | 5m | downstream down | §24.7 | confirm downstream outage |
| **Warn** | Redis dedupe unavailable | `dedupe_fallback_total increasing` | 5m | dupe risk ↑ (Layer 2 holds) | §24.6 | restore Redis |
| **Warn** | Permanent error spike | `rate(messages_dlq_total{error_type=~"Invalid.*"}[10m]) high` | 10m | schema drift | §24.1 | check producer deploy |
| **Info** | Replay started/finished | event | — | audit | §31 | none |
| **Info** | Purge completed | event | — | audit | §24.10 | none |
| **Info** | Topology drift detected | passive declare mismatch | — | config risk | §24.12 | reconcile |

**The one alert you cannot skip:** DLQ growth rate. A growing DLQ is the universal "customers are being harmed right now" signal.

---

## 23. SLOs and SLIs

**SLOs (example — set yours from product):**
- 99% of messages processed within **2 minutes** (end-to-end).
- 99.9% of *valid* messages eventually processed within **30 minutes** (covers full retry horizon + recovery).
- DLQ rate < **0.1%** of total volume.
- Publish success (confirmed) > **99.95%**.
- Consumer availability > **99.9%**.

**SLIs:**
- end-to-end latency = `produced_at` → `processed_at` (needs a producer timestamp header).
- queue age (oldest ready message) — the lag SLI.
- retry success ratio = retried-then-succeeded / retried.
- DLQ ratio = dlq / consumed.
- duplicate suppression ratio = dedupe_hits / consumed.
- publish confirm success ratio.

**Error budget:** the 30-min/99.9% SLO sets your retry budget — `delays` must sum to well under 30 min so a fully-retried message still meets it. The 0.1% DLQ budget is what you spend when you ship a bug; blow through it and you freeze deploys until the DLQ is drained and root-caused.

---

## 24. Runbooks

Each: symptoms → likely causes → immediate mitigation → diagnostics → safe remediation → escalation → prevention. Condensed; expand in your wiki.

**24.1 DLQ is growing**
- *Symptoms:* DLQ depth/rate alert; customer reports of missing orders.
- *Causes:* bad deploy (code throws), schema drift from a producer, downstream hard-down past retry budget, poison batch.
- *Mitigate:* if tied to a deploy → **roll back** (§24.11). If producer schema drift → coordinate producer rollback.
- *Diagnose:* `inspector.peek_async("orders.queue.dlq", 50)`; group by `x-error-type` / `x-error-stack-hash`; correlate timestamp with deploys.
- *Remediate:* fix root cause, deploy, then controlled replay (§31).
- *Escalate:* service owner → producer team if schema.
- *Prevent:* contract tests (§25), canary deploys, alert on DLQ rate.

**24.2 Retry queue is growing**
- *Causes:* downstream degraded (not down) → everything transient-fails and retries.
- *Mitigate:* check circuit breaker; if downstream is the cause, let backoff work; consider pausing consumers if retries amplify load.
- *Diagnose:* mgmt API depth of `retry.*`; breaker open count; downstream latency.
- *Prevent:* circuit breaker + rate limit toward the downstream.

**24.3 Main queue backing up**
- *Causes:* consumers down/insufficient; slow downstream; prefetch too low; poison slowing handlers.
- *Mitigate:* scale consumers (HPA/manual); verify pods ready; check downstream.
- *Diagnose:* consumer count, queue age, processing p99, pod readiness.
- *Prevent:* KEDA on queue depth; capacity plan (§32).

**24.4 Consumer pods crash-looping**
- *Causes:* poison message killing the process; bad deploy; missing config/secret.
- *Mitigate:* **roll back** immediately. If poison: temporarily lower prefetch to 1; rely on quorum `delivery-limit` to dead-letter the killer; or move it to quarantine via mgmt.
- *Diagnose:* `kubectl logs --previous`; find the `message_id` that precedes each crash.
- *Prevent:* validate before doing work; never deserialize untrusted data into code paths that can crash the process.

**24.5 RabbitMQ `connection.blocked`**
- *Causes:* broker memory or disk alarm.
- *Mitigate:* free disk/memory on broker; reduce publish rate (`FlowController`); platform team.
- *Diagnose:* mgmt `overview()` (alarms, mem/disk); `list_connections()` for blocked state.
- *Prevent:* broker capacity + alerts on mem/disk watermarks.

**24.6 Redis dedupe unavailable**
- *Causes:* Redis down/failover.
- *Mitigate:* with `fallback_on_redis_error=True`, processing continues (Layer 2 DB idempotency protects against dupes). Restore Redis.
- *Diagnose:* `dedupe_fallback_total` rising; Redis health.
- *Prevent:* Redis HA; the DB unique constraint is the real guarantee.

**24.7 External dependency down**
- *Mitigate:* circuit breaker opens → fast transient failures → retried later. Don't disable retries; do consider pausing if retries worsen the downstream.
- *Prevent:* breaker + bulkhead + rate limit per downstream.

**24.8 Poison messages detected** — see §11; quarantine, fingerprint, triage; roll back if deploy-related.

**24.9 Replay DLQ safely** — see §31 (the full procedure).

**24.10 Purge test DLQ** — staging only: `inspector.purge_async(...)`. **Production purge requires change ticket + two-person approval + audit log.** Prefer replay/quarantine over purge always.

**24.11 Roll back bad deployment**
- `kubectl rollout undo deployment/order-service`; verify consumers reconnect and DLQ rate drops; then triage the DLQ accumulated during the bad window.

**24.12 Topology mismatch (`PRECONDITION_FAILED`)**
- *Causes:* code declares a queue with args differing from what exists (AUTO_DECLARE leak, or platform changed args).
- *Mitigate:* in prod you should be `PASSIVE_ONLY` — failure is at startup, loud. Reconcile the actual topology to the intended (migration job), or fix the code's declared args. Never delete a queue with messages to "fix" args without draining.

**24.13 Publisher confirms timing out**
- *Causes:* broker overloaded; network; disk slow.
- *Mitigate:* reduce publish rate; check broker load; treat TIMEOUT as unknown (idempotent re-publish or reconcile).
- *Prevent:* broker capacity; bounded in-flight via `FlowController`.

---

## 25. Testing Strategy

**Unit (rabbitkit `TestBroker` — in-memory, no RabbitMQ):**
```python
# tests/test_orders.py
import pytest
from rabbitkit.testing import TestBroker

@pytest.fixture
def broker():
    b = TestBroker(serializer=..., di_resolver=...)
    register_order_handlers(b)     # same decorators as prod
    b.start()
    yield b
    b.stop()

def test_success_acks(broker):
    broker.publish("orders.queue", b'{"order_id":"o1","tenant_id":"t","amount_cents":100,"currency":"USD","created_at":"2026-01-01T00:00:00Z"}')
    msg = broker.consumed_messages[-1]
    msg._ack_fn.assert_called_once()           # acked
    msg._nack_fn.assert_not_called()

def test_validation_error_no_retry(broker):
    # Route uses AckPolicy.NACK_ON_ERROR (§6); ValidationError ⊂ ValueError → PERMANENT.
    broker.publish("orders.queue", b'{"bad":"data"}')
    msg = broker.consumed_messages[-1]
    # permanent → nack(requeue=False) → DLX → DLQ, never retried.
    # NOTE: message.nack() calls _nack_fn(requeue) POSITIONALLY, so assert positionally:
    msg._ack_fn.assert_not_called()
    msg._nack_fn.assert_called_once_with(False)
```

```python
# tests/test_retry.py — transient retries, permanent → DLQ
def test_transient_routes_to_delay_queue(broker_with_retry):
    # handler raises DownstreamUnavailable; RetryMiddleware publishes to delay queue + acks source
    ...
    assert any("retry" in e.routing_key for e in broker.published_messages)

def test_retry_publish_failure_nacks_not_acks(broker):
    # publish_fn returns PublishStatus.ERROR → message NACKed (requeue), never acked (data-loss guard)
    ...
```

**Integration (real RabbitMQ via testcontainers):** TTL delay behavior, DLX routing, `x-rabbitkit-retry-count` increments, DLQ after exhaustion, publisher confirms, consumer reconnect (kill connection → recovers), graceful shutdown (in-flight drained). rabbitkit already has these patterns under `tests/integration/`.

**Contract tests:** message schema (Pydantic), routing keys, headers, **AsyncAPI** doc matches the running broker (§29). Run in CI against producer + consumer to catch schema drift before it hits the DLQ.

**Chaos:** RabbitMQ restart; network partition; Redis outage (verify fallback); downstream 503 storm (verify retries + breaker); slow consumer (verify backpressure); **consumer crash after DB commit before ack** (verify idempotent reprocess); publish confirm timeout; replay storm (verify throttled replay holds).

**Load (§24/§32):** sustained throughput; latency under load; **retry-storm simulation** (force transient failures, confirm delay queues absorb without melting the main queue); DLQ replay load; backpressure behavior at the prefetch/worker limits.

---

## 26. Failure Mode and Effects Analysis (FMEA)

S/P scale 1–5 (5=worst/most-likely).

| Failure mode | Cause | Effect | Detection | Mitigation | S | P | Recovery |
|---|---|---|---|---|---|---|---|
| Retry storm | `requeue=true` on outage | CPU pinned, healthy msgs starved | retry rate, CPU | TTL+DLX delays, jitter, never blind requeue | 5 | 3 | drain delay queues; fix code |
| Poison message | corrupt payload / code bug | crash loop or DLQ flood | crash rate, DLQ error_type | classify PERMANENT; `delivery-limit`; quarantine | 4 | 3 | rollback; quarantine; fix |
| DLQ ignored | no alert/owner | silent customer loss | DLQ growth alert | alert on DLQ rate; ownership | 5 | 2 | replay after fix |
| Redis unavailable | Redis outage | dupe risk ↑ | dedupe_fallback metric | `fallback_on_redis_error`; DB unique constraint | 3 | 2 | restore Redis |
| Connection blocked | broker mem/disk alarm | publishes stall | blocked_seconds | broker capacity; FlowController | 4 | 2 | free resources |
| Publish confirm timeout | broker overload | unknown delivery | publish_seconds p99 | idempotent re-publish; outbox | 3 | 2 | reconcile |
| Crash after side-effect, before ack | pod killed | redelivery | normal | idempotency (Layer 2) | 2 | 4 | auto (no-op reprocess) |
| Duplicate processing | at-least-once | double side-effect | invariant checks | idempotency mandatory | 5 | 4 | DB constraint blocks |
| Topology drift | AUTO_DECLARE / arg change | consumer won't start | startup PRECONDITION_FAILED | PASSIVE_ONLY in prod; topology as code | 4 | 2 | reconcile topology |
| Message too large | unbounded payload/headers | frame errors, mem | publish errors | size limits; no stacks in headers | 3 | 2 | reject oversize |
| Downstream outage | dependency down | mass transient fail | breaker open | breaker + retry + rate limit | 4 | 3 | auto recover |
| Bad deployment | regression | DLQ/crash spike | DLQ rate, crash rate | canary; fast rollback | 5 | 3 | rollback |
| Clock skew | NTP drift | wrong TTL/dedupe expiry | dedupe anomalies | NTP everywhere; generous dedupe TTL | 3 | 1 | fix NTP |
| Invalid TLS cert | expiry/misconfig | can't connect | startup failure, conn errors | cert monitoring + rotation | 4 | 2 | rotate cert |
| Secret rotation failure | bad creds rolled | auth failures, no consume | conn errors at deploy | rotate with overlap; canary | 4 | 2 | restore prev secret |

---

## 27. Security

- **TLS (AMQPS, port 5671):** encrypt in transit. mTLS (client certs) for service auth where supported.
- **SASL:** `mechanism="PLAIN"` over TLS (PLAIN without TLS = plaintext creds — never).
- **Least-privilege users, per service:** `order-service` user has configure/write/read **only** on `^orders\.` resources in vhost `/orders`. One user per service — never a shared `guest`/admin. The **DLQ-management** user (purge/delete) is separate from the runtime user.
- **vhost isolation:** each domain/team gets its own vhost; blast radius is contained.
- **Secrets:** from a secret manager (K8s secrets backed by Vault/cloud KMS), never in images, env literals, or git. Rotate with overlap.
- **Message signing (`SigningMiddleware`):** HMAC over the payload; `reject_unsigned=True` / `reject_invalid=True` on trust boundaries. Invalid signature → PERMANENT → DLQ (don't retry an attacker).
- **No PII in headers** (§9). Consider field-level encryption for sensitive bodies; the broker is not your encryption boundary.
- **Audit logging:** every DLQ replay/purge and every topology change logged with actor, time, scope, reason.
- **Management API + dashboard:** behind authn/authz (OIDC), network-restricted (internal only), read-mostly. The dashboard exposes routes/health — still gate it.
- **Network policies:** consumers reach only RabbitMQ, Redis, their DB, and required downstreams. Deny-by-default.

---

## 28. Environment Configuration

`RabbitSettings` (pydantic-settings, `rabbitkit[settings]`) reads `RABBITMQ_*` and builds a `RabbitConfig` via `.to_rabbit_config()`.

```bash
# .env.dev
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_VHOST=/orders
RABBITMQ_USER=order-service
RABBITMQ_PASSWORD=devpass
RABBITMQ_PREFETCH_COUNT=10
RABBITMQ_RETRY_MAX_RETRIES=4
RABBITMQ_RETRY_DELAYS=5,30,120,600
RABBITMQ_TOPOLOGY_MODE=AUTO_DECLARE     # dev: declare freely
RABBITMQ_SSL_ENABLED=false

# .env.staging  (mirror prod as closely as possible)
RABBITMQ_HOST=rabbitmq.staging.internal
RABBITMQ_TOPOLOGY_MODE=AUTO_DECLARE     # staging may still declare
RABBITMQ_SSL_ENABLED=true

# .env.production
RABBITMQ_HOST=rabbitmq.internal
RABBITMQ_PORT=5671
RABBITMQ_VHOST=/orders
RABBITMQ_USER=order-service             # from secret manager
RABBITMQ_PASSWORD=                      # injected as a secret, not in the file
RABBITMQ_PREFETCH_COUNT=20
RABBITMQ_RETRY_MAX_RETRIES=4
RABBITMQ_RETRY_DELAYS=5,30,120,600
RABBITMQ_TOPOLOGY_MODE=PASSIVE_ONLY     # prod: assert, don't mutate
RABBITMQ_SSL_ENABLED=true
RABBITMQ_SSL_CA_CERTS=/etc/rabbitmq/certs/ca.pem
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT=300
RABBITMQ_HEARTBEAT=30
```

**Must differ by environment:** host/port, credentials, TLS (off in dev / on in prod), `TOPOLOGY_MODE` (AUTO_DECLARE → PASSIVE_ONLY), prefetch/worker sizing, dashboard auth. **Keep identical:** retry policy, error classification, queue/exchange names, schema — so staging actually validates prod behavior.

---

## 29. AsyncAPI Documentation

```python
# generate from the live broker routes
from rabbitkit.asyncapi import generate_asyncapi_json, AsyncAPIGeneratorConfig

spec = generate_asyncapi_json(
    broker.routes,
    AsyncAPIGeneratorConfig(
        title="order-service", version="1.4.2",
        description="Order ingestion + retry pipeline",
        server_url="rabbitmq.internal:5671", server_description="prod",
    ),
)
# write to docs/asyncapi.json in CI; publish to your API catalog
```

**Why it pays off:** contract review (producers see the exact schema/headers/routing they must emit), schema governance (diff the spec in CI; a breaking change fails the build before it floods the DLQ), onboarding (new engineers read one file), incident debugging (what *should* be on this queue?), and cross-team integration (the spec is the contract). Generate it in CI and fail on undocumented routes.

---

## 30. Management and Dashboard

```python
# management_tools.py
from rabbitkit.management import RabbitManagementClient, ManagementConfig

mgmt = RabbitManagementClient(ManagementConfig(
    url="https://rabbitmq.internal:15671", username="observer", password="...", timeout=10.0,
))

for q in mgmt.list_queues(vhost="/orders"):
    print(q["name"], q["messages_ready"], q["messages_unacknowledged"])

dlq = mgmt.get_queue("orders.queue.dlq", vhost="/orders")
if dlq["messages"] > 0:
    alert(f"DLQ depth = {dlq['messages']}")

assert mgmt.health_check()      # broker reachable + healthy
```

```python
# fastapi_app.py — mount dashboard + lifespan-managed broker
from contextlib import asynccontextmanager
from fastapi import FastAPI
from starlette.routing import Mount
from rabbitkit.fastapi import rabbitkit_lifespan
from rabbitkit.dashboard import create_dashboard_app
from .broker import broker

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with rabbitkit_lifespan(broker=broker):   # starts/stops broker with the app
        yield

app = FastAPI(lifespan=lifespan)
app.mount("/_rabbit", create_dashboard_app(broker, management_client=mgmt))  # behind OIDC + netpol

@app.get("/health/ready")
async def ready():
    from rabbitkit.health import broker_health_check_async
    r = await broker_health_check_async(broker)
    return {"status": r.status, "connected": r.connected, "consumers": r.consumer_count}
```

Restrict the dashboard: authenticated (OIDC), internal network only, read-mostly. It exposes routes + health, which is useful to attackers mapping your system.

---

## 31. DLQ Replay Safety

Replay is a deploy-grade operation. The library gives you `replay`/`replay_async` with a predicate; **it does not throttle or dry-run** — you wrap it.

**Procedure:**
1. **Root cause identified and fixed.** Never replay into the same bug.
2. **Deploy the fix.** Confirm the live error rate is normal.
3. **Sample** the DLQ (`peek_async`, group by `error_type`/`stack_hash`) to confirm these messages are now processable.
4. **Dry-run predicate:** run the predicate over a peeked sample; log what *would* replay. No publishing.
5. **Replay a small batch** (e.g. 50), monitor `messages_consumed_total{status=...}`, DLQ re-entry, downstream latency.
6. **Replay in controlled batches** with a sleep between batches (throttle). **Stop on error-rate spike.**
7. **Audit:** log operator, time, queue, predicate, count, reason. Notify stakeholders.

```python
# predicate-based, throttled, abortable replay (you own the loop; the lib only does one pass)
async def safe_replay(inspector, dlq: str, predicate, *, batch=50, pause=2.0, max_total=5000):
    replayed = 0
    while replayed < max_total:
        sample = await inspector.peek_async(dlq, limit=batch)
        if not sample:
            break
        # dry-run visibility before acting
        matching = [m for m in sample if predicate(m)]
        log.info("replay_batch_preview", dlq=dlq, batch=len(sample), matching=len(matching))
        n = await inspector.replay_async(dlq, predicate=predicate)   # replays matching this pass
        replayed += n
        if await error_rate_too_high():     # your metric check — abort on spike
            log.warning("replay_aborted_error_spike", replayed=replayed); break
        await asyncio.sleep(pause)           # throttle to protect downstream
    log.info("replay_done", dlq=dlq, replayed=replayed)   # AUDIT
    return replayed
```

**Never:** replay-all blindly into a degraded system; replay before the fix is live; purge production DLQ to "clean up" (that's deleting evidence of customer harm).

---

## 32. Capacity Planning

**Consumers:**
```
required_consumers = ceil( incoming_rate(msg/s) × avg_processing_time(s) / target_utilization )
# e.g. 500 msg/s × 0.05s / 0.7 ≈ 36 concurrent handlers
#   → 5 pods × worker_count=8 ≈ 40 (headroom for spikes + retries)
```
Add headroom for retry traffic: retried messages re-enter the main queue, so effective load ≈ `incoming × (1 + retry_rate × avg_attempts)`.

**Queue storage:**
```
queue_storage ≈ avg_message_size × retained_messages × replication_factor
# quorum replication_factor = number of replicas (typically 3)
# retained_messages = steady-state depth + worst-case backlog during an outage
```

**Retry queue load:** at retry rate `r`, delay queues hold ≈ `incoming × r × (sum(delays)/avg_delay)` messages in flight — size broker memory/disk for the *worst* transient-outage scenario (everything retrying for the full horizon).

**DLQ retention:** `dlq_storage ≈ dlq_rate × avg_size × retention_days × 86400 × replication`. Pick retention from compliance + realistic triage SLA (e.g. 14 days).

**Redis dedupe memory:** `≈ key_count × (key_size + overhead) ≈ peak_msg_rate × ttl × ~100 bytes`. For 500 msg/s × 86400s ttl ≈ 43M keys × ~100B ≈ ~4GB — **tune TTL down or shard Redis** if this is large; the DB constraint is the real guarantee, so dedupe TTL can be shorter than you fear (just ≥ retry horizon × safety).

**Broker nodes:** 3-node quorum cluster minimum for HA; size CPU/RAM/disk for peak depth + replication + connection count; keep disk free above the broker's disk alarm watermark with wide margin (a triggered disk alarm = `connection.blocked` = incident).

---

## 33. Recommended Defaults

| Setting | Small (low traffic, low risk) | Medium (business-critical) | High-throughput critical |
|---|---|---|---|
| prefetch | 5 | 20 | 50–200 (IO-bound) |
| worker_count | 2 | 8 | 16–32 |
| channel_pool_size | ≥ worker | ≥ worker | ≥ worker |
| max_retries | 3 | 4 | 4–5 |
| delays (s) | 10,60,300 | 5,30,120,600 | 5,30,120,600,1800 |
| dedupe TTL | 6h | 24h | 24–48h |
| DLQ retention | 7d | 14d | 30d |
| queue type | classic durable | quorum | quorum |
| alert: DLQ rate | >5/min | >1/s | >1/s + per-tenant |
| alert: queue age | >5m | >2m | >2m (SLO-driven) |
| autoscaling | off / CPU | KEDA queue depth | KEDA queue depth + lag |

---

## 34. Anti-Patterns (do **not**)

- **Retry forever** — always cap (`max_retries` + `x-delivery-limit`).
- **`requeue=true` for all exceptions** — the #1 retry-storm cause.
- **`sleep()` in the consumer** — blocks the consumer; use TTL+DLX.
- **No DLQ** — failures vanish; you can't replay what you didn't keep.
- **No idempotency** — at-least-once *will* double-process; corruption follows.
- **No `correlation_id`** — incidents become unsolvable.
- **No publisher confirms** — silent message loss on the publish side.
- **No alert on DLQ growth** — silent customer harm.
- **Purge the DLQ to "fix" it** — deleting evidence of lost work.
- **Retry validation/auth errors** — they never succeed; straight to DLQ.
- **Share one retry/DLQ across unrelated workloads** — one noisy service hurts all.
- **Stack traces or PII in headers** — size, exposure, and privacy violations.
- **Rely only on `x-death`** — own your `x-rabbitkit-retry-count`.
- **`ACK_FIRST` for business workflows** — at-most-once = silent loss on crash.
- **Scale consumers without checking downstream limits** — you just move the queue into the DB pool.
- **High prefetch for slow jobs** — huge unacked backlog + redelivery storm on restart.
- **One RabbitMQ user for everything** — no isolation, no least privilege, no audit.
- **AUTO_DECLARE in production** — app code mutating prod topology = drift + `PRECONDITION_FAILED` outages.
- **(rabbitkit-specific) `AckPolicy.AUTO` on a retry+DLQ route** — AUTO's exception path does `nack(requeue=True)` for transient errors, so an exhausted-transient message hot-loops at full speed during a downstream outage instead of dead-lettering. Use `AckPolicy.NACK_ON_ERROR` (§0/§8).
- **(rabbitkit-specific) custom transient errors that don't subclass `OSError`** (or permanent that don't subclass `ValueError`) — classification is type-based; an unrecognized type falls to `unknown_policy=PERMANENT` and silently skips retries (§7).
- **(rabbitkit-specific) `retry=RetryConfig` without `RetryMiddleware` in `middlewares`** — topology exists but nothing retries; transient failures don't back off.
- **(rabbitkit-specific) `RetryMiddleware` without `publish_async_fn=broker.publish`** — retries can't publish; messages nack-loop.
- **(rabbitkit-specific) `BatchPublisher` for critical messages** — unbounded buffer, lost on crash; use the outbox + confirmed singles.

---

## 35. Implementation Plan (phased)

- **Phase 0 — Discovery:** inventory current queues, schemas, and ad-hoc retry behavior. Map producers/consumers. Identify non-idempotent handlers (the riskiest finding).
- **Phase 1 — Foundations:** structured logging + `correlation_id` propagation; publisher confirms on all critical publishes; declare a DLQ per critical queue (even before retries). *Ship value immediately: nothing is silently lost anymore.*
- **Phase 2 — Retry:** add `RetryConfig` (topology) + `RetryMiddleware` (behavior, wired with `publish_async_fn`); build the error classifier; add retry/forensic headers. Validate TTL+DLX in staging with chaos.
- **Phase 3 — Integrity:** idempotency (Redis dedupe + DB `processed_messages`/outbox); timeout middleware; circuit breaker per downstream.
- **Phase 4 — Operate:** dashboards, metrics, alerts, AsyncAPI in CI, runbooks written and linked from alerts.
- **Phase 5 — Prove:** load test (incl. retry-storm sim), chaos drills (RabbitMQ/Redis/downstream), a **replay fire-drill** in staging (so on-call has done it before the real one).
- **Phase 6 — Rollout:** canary one consumer pod; watch DLQ rate, latency, retry rate; ramp; keep the rollback one command away.

---

## 36. Code Deliverables (map)

The snippets above are the production templates. Suggested file layout:

| File | Contents | Section |
|---|---|---|
| `config.py` | `build_config(env)` → `RabbitConfig` (+ `RabbitSettings` for env) | §5, §28 |
| `broker.py` | `AsyncBroker`, serializer, DI, middleware instances (incl. wired `RetryMiddleware`) | §6, §12, §15 |
| `models.py` | Pydantic event models | §6 |
| `errors.py` | Transient/Permanent exception hierarchy | §6 |
| `error_mapping.py` | map downstream errors → transient/permanent base classes (type-based; §7) | §7 |
| `middleware.py` | custom enrichment/signing middleware if any | §9, §15 |
| `handlers/orders.py` | `@broker.subscriber` handlers | §6 |
| `dlq_tools.py` | `DLQInspector` peek/replay/purge + `safe_replay` | §10, §31 |
| `management_tools.py` | `RabbitManagementClient` checks | §30 |
| `fastapi_app.py` | lifespan + dashboard mount + health | §18, §30 |
| `tests/test_orders.py` | handler success/validation (TestBroker) | §25 |
| `tests/test_retry.py` | transient retry, permanent→DLQ, publish-failure→nack | §25 |
| `tests/test_dlq.py` | peek/replay predicate, throttled replay | §25, §31 |

Build async-first. Use sync only for simple, single-purpose workers where async adds no value.

---

## 37. Final Recommendation

**Recommended architecture:** async `AsyncBroker` → durable topic exchange → quorum main queue with dead-letter args → middleware pipeline (`trace → exception → circuit-breaker → dedupe → retry → timeout → rate-limit`) → TTL+DLX delay queues (`delays=5,30,120,600`, jitter 0.1, `unknown_policy=PERMANENT`, `per_queue=True`) → per-queue DLQ as a triaged operational workflow. Idempotency enforced in the database (inbox + outbox), not just Redis. `PASSIVE_ONLY` topology in production. Full metrics/traces/alerts, with DLQ-growth as the keystone alert.

**Most important risks:** (1) non-idempotent handlers under at-least-once — the silent data-corruption risk; (2) misconfigured retry wiring (topology without middleware, or middleware without `publish_async_fn`) — silent no-retry or nack-loops; (3) an unwatched DLQ — silent customer harm; (4) AUTO_DECLARE topology drift in prod; (5) retry storms from blind `requeue=true`.

**Minimum production checklist:**
- [ ] Every critical publish uses confirms; outcome checked.
- [ ] Every critical queue has a DLQ with dead-letter args.
- [ ] `RetryMiddleware` is in `middlewares=[]` **and** wired with `publish_async_fn=broker.publish`.
- [ ] Retry+DLQ routes use `AckPolicy.NACK_ON_ERROR` (not `AUTO`).
- [ ] Error severity carried by exception type (transient ⊂ `OSError`, permanent ⊂ `ValueError`); `unknown_policy=PERMANENT`.
- [ ] Handlers idempotent at the DB (inbox/outbox), not just Redis.
- [ ] `correlation_id` propagated; structured JSON logs; traces wired.
- [ ] Metrics + alerts live; **DLQ-growth alert** has an owner and a runbook.
- [ ] `PASSIVE_ONLY` topology in prod; topology provisioned as code.
- [ ] Least-privilege per-service user over TLS; secrets from a manager.
- [ ] Graceful shutdown verified (drain + commit-before-ack); probes correct.
- [ ] Replay procedure rehearsed in staging.

**The 10 things you must not skip:**
1. Idempotency at the database.
2. Publisher confirms (and check the outcome).
3. A DLQ per critical queue, with an alert and an owner.
4. Error classification (don't retry permanent errors).
5. Bounded retries + jitter via TTL+DLX (never blind requeue, never sleep).
6. Wire `RetryMiddleware` correctly: `middlewares=[]` + `publish_async_fn=broker.publish` + `AckPolicy.NACK_ON_ERROR`.
7. `correlation_id` + structured logs + tracing.
8. Graceful shutdown: commit before ack; drain in-flight.
9. `PASSIVE_ONLY` topology in production.
10. A rehearsed, throttled, audited DLQ replay procedure.

**Operational ownership model:**
- **Service team** owns: handlers, classifier, schemas, retry policy, DLQ triage + replay, service dashboards/alerts, runbooks 24.1–24.4/24.7–24.13.
- **Platform/SRE** owns: RabbitMQ cluster health, policies, capacity, broker alerts (24.5, node health), TLS/cert rotation, vhost/user provisioning, topology-as-code pipeline.
- **On-call (service)** is first responder for DLQ/queue-age/crash-loop alerts; escalates broker/platform issues to SRE.
- **Producers** own their schema contract (validated via AsyncAPI in CI); a producer-side breaking change that floods a consumer DLQ is a producer incident.

---

# 38. Performance, Connection Loss, and Data-Loss Prevention

> **Measured baselines (this codebase, local container, single connection).** These are real numbers from `benchmarks/`, not aspirations. Use them to size, not as guarantees — your hardware/broker/network differ.
> - **Consumer:** ~14.8k msg/s per process; scales near-linearly with processes (2 → ~27k, 4 → ~42k). Concurrency is driven by **prefetch**, not `worker_count`.
> - **Publisher:** ~6.1k msg/s per process (publisher-confirm + broker-write bound; raw aio-pika ceiling ~8k/connection). Scales across producer processes.
> - **Pipeline CPU:** ~2.7µs/msg (negligible vs broker I/O). The lever for "more" is **horizontal** (more pods), not bigger `worker_count` or more connections in one event loop (both measured flat).

## 38.1 The Data-Loss Model

RabbitMQ does **not** give exactly-once processing. There is no such thing over a network. The honest guarantee target:

- **At-least-once delivery** (the floor).
- **No acknowledged-message loss** (an acked message's work is durably done).
- **No silently-lost publish** (every critical publish is confirmed or retried).
- **Duplicate-safe processing** (idempotent handlers).
- **Recoverable publish failures** (durable outbox + resend).
- **Recoverable consumer failures** (commit-before-ack + inbox dedup).
- **Auditable DLQ + replay** (every dead-letter and replay is observable and logged).

**Five places messages die:**

| # | Loss class | Cause | Defense |
|---|---|---|---|
| 1 | **Publisher-side** | publish without confirms; crash before resend | confirms + durable outbox (§38.2) |
| 2 | **Broker-side** | non-durable queue/exchange or non-persistent message lost on restart | durable topology + persistent + quorum (§38.4) |
| 3 | **Consumer-side** | ack before the work is durable | commit-before-ack (§38.3) |
| 4 | **Application-side** | non-idempotent handler corrupts on redelivery | inbox/dedup + DB constraints (§38.3) |
| 5 | **Operational** | human purges/replays the DLQ wrongly | change-gated, audited, throttled replay (§38.14) |

State it plainly to the team:
- Confirms off → the publisher can believe a message was sent when the broker never durably accepted it.
- Non-persistent messages → lost on broker restart.
- Non-durable queues → the **queue itself** disappears on broker restart.
- Ack before committing business state → **data loss**.
- Commit business state then crash before ack → **duplicate** (redelivery). ⇒ **the system must be idempotent.**

## 38.2 Publisher-Side Data Safety

**Requirements:** confirms enabled, `persistent=True`, `mandatory=True` for must-route messages, durable exchanges + queues, resend-on-failure from a **durable outbox**, never drop from memory before confirm, treat confirm-timeout as **unknown**, and make resend safe (duplicates happen — consumers dedup).

Per RabbitMQ's reliability guidance: a publisher recovering from a connection/channel failure should **retransmit any message for which it did not receive a confirm**, and because this can create duplicates, consumers must deduplicate or be idempotent.

```python
# config.py — safe publisher
from rabbitkit import PublisherConfig
PublisherConfig(confirm_delivery=True, confirm_timeout=5.0, persistent=True, mandatory=True)
```

```python
# check the outcome — never trust "publish returned"
outcome = await broker.publish(envelope)        # PublishOutcome
if outcome.status == PublishStatus.CONFIRMED:   # outcome.ok
    mark_published(envelope)
elif outcome.status == PublishStatus.RETURNED:  # unroutable (mandatory=True)
    alert_unroutable(envelope)                  # a routing bug — do not silently drop
elif outcome.status == PublishStatus.TIMEOUT:
    pass            # UNKNOWN — leave PENDING, resend later (idempotent consumers absorb dupes)
else:               # NACKED / ERROR
    pass            # leave PENDING, retry with backoff
```

> **rabbitkit notes:** On the **async** path confirms are always on (aio-pika opens channels with publisher confirms by default) — exactly what a safety-first design wants. `confirm_delivery=False` is **not** honored on async (no fire-and-forget fast path); that's acceptable here since §38 wants confirms always on. The data-integrity fix (C2) ensures a failed retry/result publish **nacks instead of acking**, so a publish failure never silently drops the source message.

**Outbox table (the source of truth for outbound events):**

```sql
CREATE TABLE outbox_messages (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    routing_key     TEXT NOT NULL,
    exchange        TEXT NOT NULL DEFAULT '',
    payload         BYTEA NOT NULL,
    headers         JSONB NOT NULL DEFAULT '{}',          -- incl. message_id, idempotency_key
    status          TEXT NOT NULL DEFAULT 'PENDING',      -- PENDING | PUBLISHED | FAILED
    publish_attempts INT  NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at     TIMESTAMPTZ
);
CREATE INDEX outbox_due ON outbox_messages (next_attempt_at) WHERE status = 'PENDING';
```

**Outbox relay loop** (separate from the request path; the message is written to `outbox` in the *same DB transaction* as the business change, then this loop publishes it):

```python
async def outbox_relay(db, broker) -> None:
    while True:
        rows = await db.fetch_due_pending(limit=200)          # status=PENDING, next_attempt_at<=now
        for row in rows:
            env = MessageEnvelope(
                exchange=row.exchange, routing_key=row.routing_key, body=row.payload,
                headers=row.headers, message_id=row.headers["message_id"],
                correlation_id=row.headers.get("correlation_id"), delivery_mode=2,
            )
            outcome = await broker.publish(env)
            if outcome.ok:
                await db.mark_published(row.id)                 # -> PUBLISHED, published_at=now
            else:
                # TIMEOUT/NACK/ERROR -> leave PENDING, back off; preserve message_id (dedupe key)
                await db.bump_attempt(row.id, error=str(outcome.status),
                                      next_attempt_at=backoff(row.publish_attempts))
            metrics.inc("publish_attempt_total")
            metrics.inc("publish_confirmed_total" if outcome.ok else "publish_unconfirmed_total")
        await asyncio.sleep(0.2)
```

Rules: read PENDING → publish with confirms → CONFIRMED ⇒ PUBLISHED; TIMEOUT/lost ⇒ leave PENDING and retry with backoff; **never** delete a row before a confirm; keep `message_id` stable across resends.

## 38.3 Consumer-Side Data Safety

Use server-side acks (`AckPolicy.AUTO` or `MANUAL`; **never `ACK_FIRST`** for business workflows — see §8). **Do not ack until the DB transaction is committed.** Redelivery is normal ⇒ handlers must be idempotent; external side effects must use idempotency keys with the external system.

**Safe sequence:** receive → validate → extract `message_id`/idempotency key → check inbox → BEGIN → mutate → insert processed record → COMMIT → ack → emit follow-up events **via the outbox** (not inline).

```sql
CREATE TABLE processed_messages (
    message_id     TEXT NOT NULL,
    consumer_name  TEXT NOT NULL,
    aggregate_id   TEXT,
    correlation_id TEXT,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at   TIMESTAMPTZ,
    status         TEXT NOT NULL,            -- PROCESSING | DONE
    payload_hash   TEXT,
    result_hash    TEXT,
    PRIMARY KEY (message_id, consumer_name)  -- the dedupe guarantee
);
```

**Crash windows (the whole point of commit-before-ack):**

| Case | Sequence | Result |
|---|---|---|
| **A** | crash **before** commit and before ack | redelivered → reprocessed → **safe** |
| **B** | crash **after** commit, before ack | redelivered → inbox detects dupe → ack the dupe → **safe** |
| **C** | **ack before commit**, crash | message gone, work not done → **UNSAFE — never do this** |
| **D** | publish follow-up **before** commit | downstream sees an event for state that may not exist → **UNSAFE — use the outbox** |

This maps directly to rabbitkit: with `AckPolicy.AUTO`/`NACK_ON_ERROR` the pipeline acks only after the handler returns successfully; do your commit *inside* the handler before it returns.

## 38.4 Broker-Side Durability

Require: durable exchanges + queues, persistent messages (`delivery_mode=2`), **quorum queues** for critical workloads, a multi-node cluster, disk + memory alarms, queue-length limits, DLQ retention, definition/config backups, and **topology as code**.

- **Quorum queues** (Raft-replicated, per RabbitMQ docs) — the safer default for replicated, highly-available queues. Use for **orders, payments, inventory, billing** and anything where losing a node must not lose messages. (Also gives `x-delivery-limit` poison protection — §11.)
- **Classic queues** — acceptable for low-value telemetry, ephemeral notifications, non-critical analytics, where throughput beats replicated durability.

## 38.5 Connection-Loss Scenarios

For each: what happens · loss? · dup? · mitigation · alert · runbook.

**Publisher:**

| # | Scenario | Loss? | Dup? | Mitigation |
|---|---|---|---|---|
| P1 | conn lost before publish reaches broker | no (still PENDING) | no | outbox resend |
| P2 | broker got it, confirm never returned | no | **yes on resend** | treat TIMEOUT as unknown; resend; consumer dedup |
| P3 | accepted but unroutable | yes (dropped) unless mandatory | no | `mandatory=True` → RETURNED → alert |
| P4 | broker blocked (mem/disk alarm) | no | no | FlowController `on_blocked` (§38.7); shed load |
| P5 | confirm timeout | unknown | maybe | leave PENDING; resend |
| P6 | channel closed (protocol error) | no (PENDING) | maybe | reconnect; resend unconfirmed |
| P7 | TLS handshake fails post cert-rotation | no (can't publish) | no | cert monitoring; rotate with overlap |
| P8 | DNS → dead node | no | no | reconnect/backoff; healthy endpoints |
| P9 | K8s network blip | no | maybe | reconnect; outbox |
| P10 | rolling broker restart | no (if quorum+persistent) | maybe | quorum queues; reconnect |

**Consumer:**

| # | Scenario | Loss? | Dup? | Mitigation |
|---|---|---|---|---|
| C1 | conn lost mid-processing (unacked) | no | yes | redelivery + idempotency |
| C2 | crash after receive, before ack | no | yes | redelivery + idempotency |
| C3 | crash after commit, before ack | no | yes | inbox dedup (Case B) |
| C4 | crash after ack, before side effect | **yes** | no | side effect **inside** tx / via outbox, before ack |
| C5 | duplicate after reconnect | no | yes | inbox dedup |
| C6 | Redis dedupe conn lost | no | maybe ↑ | `fallback_on_redis_error=True` + DB constraint (§38.13) |
| C7 | graceful shutdown exceeds timeout | no | maybe | drain window ≥ handler timeout (§38.12) |
| C8 | SIGKILL | no | yes | idempotency; unacked redeliver |
| C9 | channel closed `PRECONDITION_FAILED` | no | no | `PASSIVE_ONLY` topology; reconcile (§24.12) |
| C10 | consumer cancelled (queue delete/redeclare) | maybe | maybe | topology ownership; alert on cancel |

The only true data-loss rows are **P3 without `mandatory`** and **C4 (ack-before-side-effect)** — both are eliminated by the rules in §38.18.

## 38.6 Reconnect Strategy

Defaults (these are rabbitkit's `ConnectionConfig` defaults): `heartbeat=30`, `socket_timeout=10`, `blocked_connection_timeout=300`, `reconnect_backoff_base=1`, `reconnect_backoff_max=30`; exponential backoff **with jitter**; separate publisher/consumer connections; `connection_name=f"{service}@{env}/{pod}/{version}"` (priceless in the mgmt UI during incidents); reconnect metrics + logs; alert on a sustained reconnect loop.

**After reconnect, do not assume any prior channel state survived:** reopen connection → reopen channel → re-enable publisher confirms → re-declare or passively verify topology → re-bind/re-subscribe consumers → resume consuming → resend only *unconfirmed* outbox rows → emit a reconnect event → alert if the loop continues.

> **rabbitkit:** async uses aio-pika `connect_robust` (automatic recovery + consumer re-establishment). Sync recovers via `SyncBroker.run()`'s loop (reconnect → re-declare topology → re-subscribe — fix H1). Production should run `PASSIVE_ONLY` so a post-reconnect topology mismatch fails loudly instead of drifting.

## 38.7 Backpressure and Flow Control

When the broker raises a memory/disk alarm it **blocks publishers**. The app must slow or stop publishing — do **not** keep accepting unlimited HTTP requests and buffering in memory (lost on pod restart). Push backpressure to the edge: return **429/503**, buffer only in the **durable outbox**.

```python
from rabbitkit import FlowController, BackpressureConfig

# important data — wait (bounded) for capacity rather than drop
flow = FlowController(BackpressureConfig(max_in_flight=1000, rate_limit=5000,
                                         blocked_timeout=60.0, on_blocked="wait"))

# API edge that must fail fast — raise so the handler can return 503
api_flow = FlowController(BackpressureConfig(max_in_flight=1000, on_blocked="raise"))
```

- `on_blocked="wait"` — bounded wait; for important data.
- `on_blocked="raise"` — fail fast; for API edges (translate to 503/429).
- `on_blocked="drop"` — **never** for business-critical events.

Watch: `connection.blocked`/`unblocked` duration, mem/disk alarms, confirm latency, queue depth **and age**, consumer lag, retry-queue depth, DLQ depth.

## 38.8 Performance Tuning

**Publisher:** per-message confirms are safe but serialize on the round-trip — *pipeline* by publishing concurrently (confirms then overlap; measured ~equal to no-confirm at concurrency). Use pooled channels; compress above a threshold; avoid huge messages (claim-check, §38.10); avoid unroutable messages; monitor confirm latency. *(Measured: concurrency and channel-pool size barely move single-process publish — it's broker-write bound. Scale across processes.)*

**Consumer:** tune **prefetch** (the real async concurrency knob); `worker_count` for sync thread pools; async I/O over blocking; size DB/Redis pools and downstream concurrency to match; separate queues by workload class (slow vs fast — avoid head-of-line blocking); SAC only when ordering matters. *(Measured: pipeline CPU ~2.7µs/msg; the bound is aio-pika + ack round-trip on one loop. `worker_count`'s semaphore only **caps** async concurrency that prefetch already provides — a low value can reduce throughput.)*

**Queues:** per-source retry queues (never one global); quorum for critical durability; classic (optionally lazy) for high-throughput non-critical or deep backlogs; set `max-length`/overflow intentionally; **alert on queue age, not just depth**.

**Retry:** jitter, capped attempts, exponential/staged backoff via TTL+DLX; never `requeue=True` loops, never sleep in the consumer; circuit-break failing dependencies; rate-limit DLQ replay.

**Sync vs async — pick by handler shape, not by habit.** Measured on a real broker, per process, with a *trivial* handler (`benchmarks/load_test_sync.py` / `load_test_worker_pool.py` / `load_test_publisher.py`):

| Path | msg/s (per process) | Concurrency model | Notes |
|---|---|---|---|
| **Sync consumer, `worker_count=1`** | **~34k** | serial (one msg at a time) | lowest per-message overhead — inline ack, no event loop, no marshaling |
| **Async consumer** | **~14.8k** (→ ~42k at 4 procs) | concurrent via **prefetch** | scales near-linearly across processes |
| Sync consumer, `worker_count=4–8` | ~8–9k | thread pool | **slower than `wc=1`** for light handlers — pays the ack-marshaling + GIL tax; only worth it for *blocking* handlers needing thread parallelism |
| **Async publisher** | **~6.1k** | pipelined confirms | concurrent publishes |
| Sync publisher (blocking) | **~0.9k** | serial confirm round-trips | ~7× slower; avoid for volume |

**The caveat that flips the ranking:** the ~34k sync `wc=1` number is a *trivial-handler ceiling*. Sync `wc=1` is single-threaded, so for real work its ceiling is `1 / handler_latency` — a 5 ms DB/HTTP handler caps it at **~200 msg/s**, while async runs hundreds concurrently (prefetch-bound) → thousands/s. Decision rule:

- **I/O-bound handlers (DB, HTTP, Redis) → async.** Concurrency wins by orders of magnitude.
- **CPU-light / trivial high-throughput consumers → sync `worker_count=1`** for the lowest per-message overhead.
- **Sync `worker_count>1` → only for blocking/CPU handlers** that genuinely need thread parallelism (else it is *slower* than `wc=1`).
- **Publishing at volume → async** (pipelined confirms), or multiple sync producer threads/processes; never the single-threaded blocking publisher for throughput.
- **Default for a high-throughput service: async** — best all-rounder (concurrent consume + pipelined publish + horizontal scaling).

## 38.9 Prefetch and Worker-Count Sizing

```
consumer_concurrency   = replicas × worker_count
max_unacked_messages   = replicas × consumer_connections × prefetch_count
required_concurrency   = incoming_rate × avg_processing_time / target_utilization
```

Example: `500 msg/s × 0.05s / 0.70 = 35.7` → start with **6 pods × worker_count 8 = 48** concurrency, `prefetch_count` ~8–16 per worker group (tune by handler latency).

- Too-low prefetch starves throughput; too-high wastes memory and skews fairness.
- Slow jobs → low prefetch; fast I/O → raise carefully.
- Strict ordering → low concurrency or partitioned queues.
- Downstream-limited → concurrency must respect the downstream's limit.

> **rabbitkit caveat:** for the **async** broker, raw concurrency is driven by **prefetch + pods**, not `worker_count` (its semaphore caps, it does not multiply). The formula's `worker_count` term maps cleanly to the **sync** thread-pool broker and to per-pod parallelism; for async, treat `worker_count` as a *cap* and size with prefetch × pods.

## 38.10 Message Size and Payload Design

Keep messages small; compress only above a threshold (`CompressionConfig.threshold`); for large blobs use the **claim-check** pattern — store the blob in object storage, send a reference + checksum in the message. Always include `content_type`, `schema_version`/`event_version`, and an **idempotency key**. Large messages cut throughput and raise memory/disk pressure (worse in retry/DLQ where they linger). **No large stack traces or PII in headers** — store diagnostics externally keyed by `x-error-stack-hash` (§9).

## 38.11 Batch Publishing and Batch Ack

- **Batch publishing** — good for analytics, high-throughput non-interactive pipelines, buffered outbox flushing. **Dangerous** for payment/low-latency/user-facing flows or memory-only buffering. **rabbitkit reality:** `BatchPublisher` is **buffering/timing only** — its flush loops per-message publish, so it does **not** reduce confirm round-trips or give wire-level throughput. Use it for ergonomics, not as a speed lever; for safety-critical outbound use the durable outbox.
- **Batch ack** — good for high-volume idempotent processing where duplicates are safe. **Dangerous** when per-message results aren't independently tracked, a later failure makes the batch ambiguous, or ordering/side effects are complex. Only batch-ack when duplicate safety is **proven**.

## 38.12 Graceful Shutdown

Avoid loss on deploy/scale-down:

1. readiness → false (stop new HTTP traffic), 2. stop consuming new messages, 3. wait for in-flight handlers, 4. finish or nack uncompleted, 5. flush the outbox relay, 6. wait for publisher confirms, 7. close channels, 8. close connections, 9. exit.

```
SIGTERM ─▶ readiness=false ─▶ cancel consumers ─▶ drain in-flight (≤ graceful_timeout)
        ─▶ commit+ack done work ─▶ flush outbox + await confirms ─▶ close channels/conns ─▶ exit
        (Kubernetes SIGKILL only if terminationGracePeriodSeconds is exceeded)
```

Kubernetes: `terminationGracePeriodSeconds ≥ max handler timeout + buffer`; `preStop` marks not-ready; align with `ConsumerConfig.graceful_timeout`; **liveness must not kill pods during a temporary broker outage** (use readiness for that). rabbitkit's `broker.stop()` cancels consumers and drains the worker pool — call it from your SIGTERM handler / FastAPI lifespan.

## 38.13 Redis / Dedup Failure Policy

`DeduplicationMiddleware` is a **fast-path optimization, not the correctness boundary.**

- `fallback_on_redis_error=True` — availability first: process on Redis failure; duplicates possible ⇒ **DB idempotency must catch them**. (Recommended; alert on `dedupe_fallback_total`.)
- `fallback_on_redis_error=False` — safety first: fail closed (retry) when Redis is down; grows the retry queue.

**For financial/order flows, DB-level idempotency (`processed_messages` unique constraint) is mandatory.** Never depend on Redis dedupe alone for correctness.

## 38.14 DLQ Replay Performance and Safety

DLQ replay **is production traffic** — it can overload consumers and downstreams and create duplicate side effects. Controls (build on `DLQInspector`, which has no built-in throttle — see §31's `safe_replay`): batch size, rate limit, **dry-run**, predicate, **audit log (operator, reason, correlation id, count)**, stop-on-spike (DLQ/retry rate or downstream unhealthy), replay to a **quarantine** queue first for risky fixes, and prefer low-traffic windows.

## 38.15 Monitoring for No-Data-Loss Confidence

**Publisher:** `publish_attempt_total`, `publish_confirmed_total`, `publish_unconfirmed_total`, `publish_confirm_timeout_total`, `publish_returned_unroutable_total`, `publish_reconnect_total`, `outbox_pending_count`, `outbox_oldest_pending_age_seconds`, `outbox_retry_total`, `broker_connection_blocked_total`.

**Consumer:** `consumed_total`, `ack_total`, `nack_total`, `reject_total`, `redelivered_total`, `duplicate_detected_total`, `idempotency_conflict_total`, `handler_success/failure_total`, `retry_scheduled_total`, `dlq_published_total`, `processing_duration_seconds`, `in_flight_messages`.

**Broker (from the mgmt API):** `queue_depth`, `queue_oldest_message_age`, `unacked_messages`, `ready_messages`, publish/deliver/ack rates, `disk_free`, `memory_used`, `connection_blocked`, `channel_count`, `consumer_count`.

**Alert on:** outbox oldest-pending-age > 2m; unconfirmed publishes rising; confirmed publish rate → 0; connection blocked > 60s; redelivery spike; duplicate spike; DLQ growth; retry-queue growth; queue-age SLO breach; `consumer_count == 0`; **no acks while depth grows** (a stuck consumer). The keystone success metric is **publish_confirmed**, never publish_attempt.

## 38.16 Performance Load Tests

Run and record each (rabbitkit ships `benchmarks/load_test_worker_pool.py` for consumer drain and `benchmarks/load_test_publisher.py` for publish):

1. baseline throughput · 2. publisher-confirm throughput · 3. batch-publisher throughput · 4. consumer throughput × prefetch · 5. × worker_count · 6. broker restart during publish · 7. broker restart during consume · 8. network drop before confirm · 9. consumer crash after commit before ack · 10. Redis outage during dedupe · 11. downstream 503 storm · 12. retry-queue storm · 13. DLQ replay storm · 14. broker memory alarm / `connection.blocked` · 15. K8s rolling deploy under load.

Per test record: msg/s, p50/p95/p99 latency, confirm latency, retry rate, DLQ rate, duplicate rate, CPU, memory, broker disk/network I/O, queue age, consumer lag.

## 38.17 Performance Acceptance Criteria

- No **confirmed** message lost during broker restart.
- No **committed** business operation lost during consumer crash.
- Duplicate rate observable and safely handled.
- Confirm timeout → outbox resend (not lost, not assumed-sent).
- Queue age within SLO at peak.
- Survives broker pod restart and consumer rolling deploy.
- DLQ replay at the configured rate does not breach downstream limits.
- Redis outage causes no data corruption.
- **No unbounded in-memory buffering** anywhere.
- All critical queues durable; all critical messages persistent; all critical publishers confirmed; all critical consumers idempotent.

## 38.18 Hard Rules

1. No ack before the durable side effect is committed.
2. No direct publish of business events without confirms.
3. No memory-only queue/buffer for critical outbound events.
4. No infinite retry.
5. No `requeue=True` loop for arbitrary exceptions.
6. No sleeping inside consumers for retry delay.
7. No DLQ without an alert.
8. No DLQ replay without a rate limit.
9. No business-critical consumer without idempotency.
10. No production topology auto-mutation without ownership (`PASSIVE_ONLY`/`MANUAL`).
11. No large payloads in RabbitMQ unless explicitly approved (claim-check).
12. No stack traces or PII in headers.
13. No single RabbitMQ user shared across services.
14. No `ACK_FIRST` for important workflows.
15. No batch-ack unless duplicate safety is proven.
16. Confirm timeout is **unknown** — never treated as success or failure blindly.
17. No manual queue delete/purge without change approval.
18. No scaling consumers beyond downstream capacity.
19. No retrying permanent validation errors.
20. Success is measured by **publish confirms**, never publish attempts.

---

# 39. High-Throughput Reference Architecture & Plan (sync **and** async)

> The lever for scale is **process count**, not `worker_count` or extra connections in one runtime — measured: consume scales near-linearly across processes (1 → ~14.8k, 4 → ~42k msg/s) while in-loop connection count and async `worker_count` are flat-or-worse. Both runtimes scale the same way: **stateless pods + autoscale on queue depth.** The runtime you pick (sync vs async) is decided by **handler shape**, and it does **not** change the topology, the reliability invariants, or the publisher design — those are shared.

## 39.1 Pick the runtime by handler shape (measured)

| Handler shape | Runtime | Per-pod | Why |
|---|---|---|---|
| **I/O-bound** (DB/HTTP/Redis awaits) | **async** | ~14.8k (trivial); for real work = prefetch concurrent | concurrency is free on one loop; sync would serialize |
| **CPU-trivial, very high volume** | **sync `worker_count=1`** | ~34k, **serial** | lowest per-message overhead — no loop, no marshaling |
| **Blocking libs / CPU-bound** (no async client) | **sync `worker_count=N`** | ~8–9k | thread pool gives parallelism; pays the ack-marshaling + GIL tax |
| **Mixed fleet** | **async default**, sync only where forced | — | one runtime to operate; reach for sync only when a handler can't be async |

Rule of thumb: **default async.** Use **sync `wc=1`** only when handlers are CPU-trivial and you want max raw throughput. Use **sync `wc=N`** only when a handler must call a blocking library and you need thread parallelism (and accept it's ~4× slower per pod than `wc=1`).

## 39.2 Shared foundation (identical for sync and async)

These do not change with the runtime — get them right once:

- **Topology:** durable exchanges + **quorum** queues (critical) / classic-lazy (bulk), per-source retry (TTL+DLX) + `.dlq`, `TopologyMode.PASSIVE_ONLY` in prod, provisioned as code.
- **Workload-class isolation:** separate queues **and** separate deployments for fast vs slow vs bulk — a slow handler must never share a queue/pod with a fast one (head-of-line blocking is the #1 throughput killer).
- **Reliability invariants** (§38.18): confirms with outcome checked; **commit-before-ack** (`AckPolicy.NACK_ON_ERROR`, never `ACK_FIRST`); **DB idempotency** (`processed_messages` unique key); bounded retry + jitter (never `requeue=True`, never sleep); DLQ-per-queue with a growth alert + throttled replay; backpressure to durable storage only.
- **Observability:** the no-data-loss metrics (§38.15), queue-**age** SLO, KEDA scale signal = queue depth.
- **Scaling:** KEDA `rabbitmq` trigger on `QueueLength`, `maxReplicas` capped to downstream capacity, scale on **backlog not CPU**.

## 39.3 Async consumer plan (default)

```python
broker = AsyncBroker(config, serializer=SerializationPipeline(JsonParser(), PydanticDecoder()),
                     di_resolver=DIResolver())
retry_mw = RetryMiddleware(config.retry, publish_async_fn=broker.publish)

@broker.subscriber(queue="orders.fast", ack_policy=AckPolicy.NACK_ON_ERROR,
                   middlewares=[trace_mw, exc_mw, retry_mw, timeout_mw], prefetch_count=200)
async def handle(event: OrderCreated, svc: Annotated[Svc, Depends(get_svc)]) -> None: ...

await broker.start()   # worker_count left at 1 for async — prefetch drives concurrency
```

- **Concurrency knob = `prefetch_count`** (IO-bound 100–300; mixed 32–64; slow 1–8). Leave `worker_count=1` (its semaphore only *caps*).
- **Sizing:** `pods = ceil(incoming_rate × handler_latency / target_util / prefetch)`. Then KEDA to absorb bursts.
- **Scales near-linearly across pods** (measured 1→4 = ~14.8k→~42k).

## 39.4 Sync consumer plan

```python
broker = SyncBroker(config, serializer=..., di_resolver=DIResolver())
@broker.subscriber(queue="reports.bulk", ack_policy=AckPolicy.NACK_ON_ERROR,
                   middlewares=[trace_mw, exc_mw, RetryMiddleware(config.retry, publish_fn=broker.publish)])
def handle(event: ReportRequested, svc: Annotated[Svc, Depends(get_svc)]) -> None: ...

# worker_count=1 path (recommended for CPU-light): run() = start + reconnect/recovery
broker.run()                                              # ONE thread: connect + consume + recover

# worker_count=N path (blocking handlers): start() takes the worker_config, then consume
# on the SAME thread. NOTE: run()'s recovery loop is not available with wc>1 today —
# add a reconnect loop around start_consuming() yourself if you need wc>1 + recovery.
broker.start(worker_config=WorkerConfig(worker_count=8))  # channel_pool_size >= worker_count
broker._transport.start_consuming()
```

- **CPU-trivial / fast handlers → `worker_count=1`** via `broker.run()` (gets the reconnect/recovery loop), scale by pods. One pod ≈ ~34k (trivial); for real work, throughput per pod ≈ `1 / handler_latency` (serial), so **add pods** to hit the rate. Fastest per-pod path when handlers are light.
- **Blocking-library handlers → `worker_count=N`** (thread pool) via `start()`. Expect ~8–9k/pod (the C1 ack-marshaling routes every ack through the I/O thread — correct, but a tax). Size `N` to the concurrency the handler needs; **`channel_pool_size ≥ worker_count`**.
- **Critical:** `connect()` and `start_consuming()` must run on the **same thread**. `broker.run()` guarantees this; if you drive `start()` + `start_consuming()` manually, do both on one thread (splitting them with `worker_count=1` deadlocks the ack marshaling).
- **Known gap:** `run()` (the recovery loop) does not accept `worker_config`, so `worker_count>1` + automatic reconnect-recovery isn't wired today — wrap `start_consuming()` in your own reconnect loop if you need both.
- **Sizing (blocking handler):** `pods × worker_count = ceil(incoming_rate × handler_latency / target_util)`.

## 39.5 Publisher plan (both runtimes — decouple it)

Publishing is **broker-write bound** and the runtimes differ sharply: **async pipelines confirms (~6.1k/pod); the sync blocking publisher is serial (~0.9k/pod).** So:

- **Always publish through the transactional outbox + a relay** (§38.2) — write the event in the same DB tx as the business change; a separate relay publishes with confirms and marks `PUBLISHED`.
- **Make the relay async even if the consumer is sync.** The relay is a standalone component; there's no reason to inherit the sync publisher's serial penalty. ~6.1k/relay-pod, scale relay pods.
- If you must publish synchronously at volume, run **multiple sync producer threads/processes** (each ~0.9k) — but prefer the async relay.

## 39.6 Capacity cookbook

| Need | Async | Sync |
|---|---|---|
| 10k/s, light handlers | 1–2 pods, prefetch 100 | 1 pod `wc=1` (trivial) → few pods for real work |
| 50k/s, light | ~4–6 pods (linear) | partitioned queues + `wc=1` pods |
| 50k/s, 20ms I/O handler | `58 concurrent` → ~2 pods × prefetch 64 (+KEDA) | **bad fit** — sync would need ~1000 threads; use async |
| CPU-bound transform | thread offload or process pool | `wc=N` sized to cores, scale pods |
| Publish 30k/s | ~5–8 async relay pods | async relay (don't use sync blocking) |
| Strict per-key order | partition (hash→queue), 1 consumer/partition | same; or SAC (caps to 1) |

## 39.7 Phased rollout plan

- **Phase 0 — Baseline & classify.** Inventory handlers; tag each I/O-bound / CPU-trivial / blocking. Pick runtime per workload class (§39.1). Stand up topology-as-code (quorum, per-queue retry/DLQ), `PASSIVE_ONLY`.
- **Phase 1 — Reliability floor (before chasing speed).** Confirms + outcome checks; outbox + relay; commit-before-ack; DB idempotency; bounded retry + DLQ alert. *Nothing is "high throughput" if it loses messages.*
- **Phase 2 — Per-class deployments.** Separate fast/slow/bulk queues + deployments. Async for I/O classes; sync `wc=1` for any CPU-trivial firehose. `channel_pool_size ≥ worker_count` on sync `wc>1`.
- **Phase 3 — Tune to the formula.** Set prefetch (async) / worker_count (sync) and pod count from §39.3/39.4 sizing. Run `benchmarks/load_test_*` against a staging broker to confirm per-pod numbers on your hardware.
- **Phase 4 — Autoscale.** KEDA on queue depth, `min/max` per class, `max` capped to downstream limits. Alert on queue **age**, DLQ growth, no-acks-while-depth-grows.
- **Phase 5 — Prove under failure.** Run the §38.16 load+chaos suite: broker restart, consumer crash-after-commit, Redis outage, downstream 503 storm, rolling deploy under load. Acceptance = §38.17 (no confirmed-message loss, no committed-work loss, bounded duplicates).
- **Phase 6 — Production ramp.** Canary one class, watch queue-age + DLQ + duplicate rate, ramp pods, keep rollback one command away.

**The one-line plan:** *reliability floor first → split by workload class → async for I/O & publishing, sync `wc=1` for CPU-trivial firehoses → tune prefetch/pods to the formula → autoscale on depth → prove it under chaos → ramp.*

## 39.8 Hitting 10k msg/s per worker

**10k msg/s = a 100µs per-message budget.** What fits in that budget decides whether one worker can do it. Measured per process on a real broker (`benchmarks/load_test_handler_latency.py`):

| Per-message work | Async (1 worker, prefetch 300) | Sync `worker_count=1` |
|---|---|---|
| trivial | **~13.1k** ✅ | **~35.9k** ✅ |
| **1 ms I/O** (DB/HTTP/Redis) | **~13.7k** ✅ | **~0.6k** ❌ |
| 100 µs **CPU** | ~6.0k ❌ | ~7.3k ❌ |

Two facts the numbers prove:
- **I/O time is FREE on async, FATAL on sync.** Async overlaps the 1 ms waits across the prefetch window, so one worker still does ~13.7k. Sync `wc=1` is serial → 1 ms/msg ≈ 600/s. This is the whole game.
- **CPU time counts against both** (one GIL): keep per-message CPU **≤ ~50 µs** to clear 10k on a single worker.

**Async → 10k/worker for I/O-bound *and* trivial handlers (the common case):**

```python
broker = AsyncBroker(config)                              # worker_count stays 1
@broker.subscriber(queue="orders", prefetch_count=300,   # prefetch IS the concurrency
                   ack_policy=AckPolicy.NACK_ON_ERROR, middlewares=[...])
async def handle(event: OrderCreated, svc: Annotated[Svc, Depends(get_svc)]) -> None:
    await svc.persist(event)                              # awaited I/O overlaps across in-flight msgs
await broker.start()
```

Requirements to actually hold 10k on one async worker:
1. **Handler is fully async** — a single blocking call (sync DB driver, `time.sleep`, heavy CPU) freezes the loop and collapses it to serial speed.
2. **`prefetch_count` ≥ ~`throughput × latency`, with headroom** — at 1 ms latency, 10k/s needs ≥ 10 in-flight; use **100–300** for jitter (measured ~13.7k at 300).
3. **Downstream sustains it** — size DB/HTTP/Redis pools for the in-flight concurrency; at 10k/s the bottleneck moves from the broker to your dependency.
4. **CPU/message < ~50 µs** — offload heavy CPU to a process pool.

**Sync → 10k/worker only for CPU-trivial handlers:**

```python
broker = SyncBroker(config)
@broker.subscriber(queue="events", prefetch_count=300, ack_policy=AckPolicy.NACK_ON_ERROR)
def handle(event: Evt) -> None:
    ...                                                   # < ~50 µs CPU, NO I/O (serial!)
broker.run()                                              # worker_count=1, ~36k for trivial
```

Sync **cannot** do 10k/worker when the handler does I/O (serial → ~600/s for 1 ms). If you're stuck on sync + I/O handlers: (a) move that workload to **async** (right answer); (b) use `worker_count=N` — a blocking-I/O library can approach 10k because the GIL releases during socket I/O, but at ~8–9k/pod base + the marshaling tax, so size `N` and scale pods; (c) scale pods (sync `wc=1` at ~0.6k/pod → ~17 pods for 10k — don't; use async).

**Decision in one line:** *async + high prefetch for I/O-bound (10k/worker easily); sync `wc=1` only for CPU-trivial firehoses; CPU-heavy (any runtime) caps ~6–7k/worker on one GIL → scale processes.*

---

*Design for duplicates. Design for crashes. Design for downstream outages. Design for replay. Design for the on-call engineer at 3am, and for the team that inherits this in two years. At-least-once is the floor; idempotency is how you build a reliable system on top of it.*
