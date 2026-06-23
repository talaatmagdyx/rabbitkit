# Full configuration — every knob + the full middleware stack (sync & async)

Two reference apps that wire **everything** rabbitkit offers, so you can copy the
shape and delete what you don't need:

- `async_app.py` — `AsyncBroker` (aio-pika)
- `sync_app.py` — `SyncBroker` (pika)

They're identical except for the runtime differences (broker class, `async def`
handler, `RetryMiddleware` `publish_async_fn` vs `publish_fn`, async vs sync Redis).

## What's configured

**`RabbitConfig`** — every sub-config: `ConnectionConfig` (heartbeat, socket /
blocked-connection timeouts, reconnect backoff, connection name), `SocketConfig`
(TCP keepalives), `SecurityConfig` + `SSLConfig` (AMQPS), `PublisherConfig`
(confirms, persistent, mandatory, confirm timeout), `ConsumerConfig` (prefetch,
graceful timeout), `PoolConfig`, `RetryConfig` (TTL+DLX delays, jitter,
`unknown_policy`), `CompressionConfig`, `LoggingConfig`, `TopologyMode`.

**Middleware stack** (outer → inner): tracing → exception → circuit breaker →
deduplication → **retry** → timeout → rate limit. Plus a Pydantic serializer and
DI resolver.

**Consumer** uses `AckPolicy.NACK_ON_ERROR` + `retry=RetryConfig` (declares the
delay/DLQ topology) + `RetryMiddleware` wired with the broker's publish fn
(what actually performs the retry — and makes the failed-publish-nacks safety net work).

## Run

```bash
docker run -d --rm -p 5672:5672 rabbitmq:3.13-alpine
python examples/full_config/async_app.py    # or sync_app.py
```

Verified output (both): `[publisher] confirmed` then `[handler] processed order o1`.

## Notes / gotchas baked in

- **No `from __future__ import annotations`** — the pipeline reads the body
  parameter's annotation via `inspect.signature`, so it must be a real type for
  Pydantic decoding. (Stringized annotations → the body arrives as a raw dict.)
- **Redis/obskit are optional at runtime here** — Redis dedup falls back to
  processing on connection error (`fallback_on_redis_error=True`); the circuit
  breaker is passed `None` (pass an obskit `CircuitBreaker` to activate it).
- **Sync `TimeoutMiddleware`** raises `HandlerTimeoutError` but cannot kill a
  running handler thread (it abandons it); only async truly cancels.
- For a long-running sync consumer use `broker.run()` (it adds the reconnect /
  re-subscribe recovery loop); the demo uses `process_data_events` for a quick smoke.
- Set `topology_mode=PASSIVE_ONLY` in production.
