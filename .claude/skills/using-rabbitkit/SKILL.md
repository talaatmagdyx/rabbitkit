---
name: using-rabbitkit
description: How to use the rabbitkit library to publish and consume RabbitMQ messages — sync (pika) and async (aio-pika) brokers, RabbitConfig composition, @subscriber/@publisher routing, ack policies, retry/DLQ, dependency injection, and TestBroker testing. Use when writing or reviewing code that uses rabbitkit, or when asked how to set up a publisher, consumer, retry, or configuration.
argument-hint: "[topic, e.g. retry | di | testing | config]"
---

Authoritative usage guide for the **rabbitkit** library. The full API reference lives in `@README.md`; the canonical end-to-end configurations are `examples/full_config/sync_app.py` and `examples/full_config/async_app.py`. This skill is the fast path plus the non-obvious gotchas.

## Minimal consumer + publisher

```python
# SYNC (pip install rabbitkit[sync])
from rabbitkit import RabbitConfig
from rabbitkit.sync import SyncBroker

broker = SyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")          # queue auto-declared (AUTO_DECLARE default)
def handle(body: bytes) -> None:
    ...

broker.start()                               # connect + consume (blocks)
```

```python
# ASYNC (pip install rabbitkit[async]) — same API, awaitable
from rabbitkit.async_ import AsyncBroker
broker = AsyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
async def handle(body: bytes) -> None: ...

await broker.start()
```

Publish (from a handler, or a separate producer once the broker is connected):

```python
from rabbitkit import MessageEnvelope
broker.publish(MessageEnvelope(routing_key="orders", body=b"{}", exchange=""))
# async: await broker.publish(MessageEnvelope(...))
```

Config is composed from frozen dataclasses: `RabbitConfig(connection=ConnectionConfig(...), publisher=PublisherConfig(...), consumer=ConsumerConfig(...), retry=RetryConfig(...), topology_mode=..., ...)`. From env: `RabbitSettings().to_rabbit_config()` (needs `rabbitkit[settings]`). See README "Configuration Reference".

## Gotchas that bite (read before debugging)

- **Does a missing queue get created?** Yes, under the default `TopologyMode.AUTO_DECLARE` the `@subscriber` queue/exchange/bindings are declared on `start()`. `PASSIVE_ONLY` only *verifies* they exist (fails if absent); `MANUAL` skips topology entirely.
- **Pydantic body validation** (`def handle(order: Order)`) only fires if the handler's module does **NOT** have `from __future__ import annotations` — the pipeline reads raw `inspect.signature` annotations. With future-annotations you get a raw dict.
- **Decorator order:** `@publisher` is inner (applied first), `@subscriber` is outer (applied second). Reverse order silently won't publish the return value.
- **AckPolicy:** `AUTO` (default) acks on success, classifies the exception on failure (transient→retry/requeue, permanent→DLQ). `MANUAL` → you call `msg.ack()/nack()/reject()`. `NACK_ON_ERROR` → nack(requeue=False) on error. `ACK_FIRST` → at-most-once (acks before the handler runs).
- **`RetryConfig(max_retries=0)` ≠ `RETRY_DISABLED`.** `max_retries=0` means retry-owned terminal semantics (immediate DLQ on a classified error). `RETRY_DISABLED` removes the retry middleware from the route entirely.
- **Middleware ordering:** the *outermost* listed middleware runs first on receive. Recommended outer→inner: tracing → exception → circuit-breaker → dedup → retry → compression. For per-route `middlewares=[...]`, put `retry` before `timeout` so a timeout gets retried.
- **Errors → severity:** validation/`ValueError`/`KeyError`/`TypeError` = PERMANENT (straight to DLQ); network/`TimeoutError`/`ConnectionReset` = TRANSIENT (retried). `unknown_policy` (default PERMANENT) decides the rest.

## Where to look it up (README sections)

Routing & routers · DI (`Depends`/`Header`/`Path`/`Context`) · Middleware (retry, compression, dedup, circuit breaker, rate-limit, signing, timeout, locking) · Retry & delay-queue topology · RPC · DLQ Inspector · Streams · Health/FastAPI/lifecycle · High-load (FlowController, BatchPublisher, WorkerPool) · CLI · Dashboard · Management API.

## Testing — no broker required

```python
from rabbitkit.testing import TestBroker
b = TestBroker()
@b.subscriber(queue="orders")
def h(body: bytes) -> None: ...
b.start(); b.publish("orders", b"{}")
h.mock.assert_called_once()         # every handler gets a .mock
```
Assertions: `b.published_messages`, `b.consumed_messages`, `b.routes`, `b.declared_queues/exchanges`. Async: `await b.publish_async(...)`.

If `$ARGUMENTS` names a topic, focus the answer on it; otherwise give the relevant section for the user's task.
