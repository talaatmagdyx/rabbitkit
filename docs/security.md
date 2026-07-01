# Security Notes

Found a vulnerability in rabbitkit itself (not just a hardening question)?
See [SECURITY.md](../SECURITY.md) for how to report it privately.

## Message Bodies

Do not log message bodies by default. Bodies may contain PII, credentials, or sensitive business data. Enable body logging only in development environments with explicit opt-in.

## Message Size Limits

Set a maximum incoming message size to prevent memory exhaustion from oversized payloads. RabbitMQ enforces `max-frame-size` at the connection level; apply additional validation in your handler.

When using `CompressionMiddleware`, configure a maximum decompressed size to prevent decompression bombs:

```python
from rabbitkit import CompressionConfig
from rabbitkit.middleware.compression import CompressionMiddleware

mw = CompressionMiddleware(config=CompressionConfig(max_decompressed_size=10 * 1024 * 1024))  # 10 MB
```

## TLS for Broker Connections

Always use TLS in production. Use `SSLConfig` to configure mutual TLS:

```python
from rabbitkit import RabbitConfig, ConnectionConfig, SSLConfig

config = RabbitConfig(
    connection=ConnectionConfig(host="rabbitmq.internal", port=5671),
    ssl=SSLConfig(
        ca_certs="/etc/ssl/certs/ca.crt",
        certfile="/etc/ssl/certs/service.crt",
        keyfile="/etc/ssl/private/service.key",
    ),
)
```

## Credentials

Use separate RabbitMQ credentials per service. Do not share a single admin account across services. Apply the principle of least privilege: each service should only have permission to publish/consume on its own exchanges and queues.

## Header Validation

Headers are untrusted input. Validate and sanitize all routing headers before acting on them. Do not use header values as file paths, SQL queries, or shell arguments.

## Routing Key Trust

Do not let external callers control routing keys directly. A caller that controls the routing key can route messages to unintended queues. Always validate or override the routing key server-side.

## HMAC Signing

`SigningMiddleware` signs and verifies messages using HMAC-SHA256 (or SHA-512). It uses `hmac.compare_digest` for constant-time comparison to prevent timing attacks. Configure it with a secret that is at least 32 bytes:

```python
from rabbitkit.experimental import SigningConfig
from rabbitkit.middleware.signing import SigningMiddleware

mw = SigningMiddleware(config=SigningConfig(secret_key="a-long-random-secret-at-least-32-bytes"))
```

The default (`require_freshness=True`) signature covers `timestamp`, `nonce`,
`exchange`, `routing_key`, `content_encoding`, and `reply_to`, in addition to
the body — not just the body. Without this, an attacker who cannot forge the
signature could still capture a validly-signed message and re-publish it
under a different routing key, redirect an RPC reply via `reply_to`, or flip
`content_encoding` to hit a different decompression path, all while the
signature still verified. Headers other than the signature/timestamp/nonce
triplet itself are **not** covered — do not use freeform headers for
security-critical routing decisions. The legacy body-only signature path
(only reachable with `require_freshness=False`, kept for interop with
producers that predate the freshness headers) has none of these protections;
avoid it for security-sensitive traffic.

Rotate the signing secret periodically. Store it in a secrets manager, not in environment variables that may be logged.

### Shared nonce store for multi-process/multi-pod deployments

`SigningMiddleware`'s default nonce cache (`TTLSetNonceCache`) is a
per-process, in-memory dict. In any deployment with more than one consumer
process — pods behind a Deployment, multiple worker processes, or a restart —
a nonce recorded by one process is invisible to every other one, and a
replayed message that lands on a different process passes the nonce check.
`SigningMiddleware` emits a `RuntimeWarning` at construction time for exactly
this reason whenever `require_freshness=True` and no explicit `nonce_cache`
is supplied.

Use `RedisNonceCache` to share the seen-set across every process:

```python
import redis
from rabbitkit.middleware.signing import RedisNonceCache, SigningConfig

cache = RedisNonceCache(redis.Redis(host="redis", port=6379))
config = SigningConfig(secret_key="shared-secret", nonce_cache=cache)
```

It records each nonce with an atomic `SET NX EX`, so two processes racing on
the same nonce can never both pass. For payments or other high-value traffic,
also use a tight `max_skew` (default 60s) to shrink the replay window, and
always pair it with a shared `nonce_cache` — the in-memory default is not
sufficient once there is more than one consumer process.

## Replay Attack Prevention

Use `DeduplicationMiddleware` with a TTL that covers your maximum message delay. This prevents replay attacks where an attacker retransmits a previously captured message:

```python
from rabbitkit import DeduplicationConfig
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
import redis

mw = DeduplicationMiddleware(
    redis_client=redis.Redis(),
    config=DeduplicationConfig(key_source="message_id", ttl=3600),
)
```

**Fail-open by default (M9):** `DeduplicationConfig.fallback_on_redis_error`
defaults to `True` — if Redis is unreachable, messages are processed
*without* idempotency enforcement rather than blocking the consumer (an
ERROR-level log and, if you wire a `MetricsCollector`, a
`dedup_fallback_total` counter are emitted every time this happens, so it's
observable, not silent). This is the right default for most workloads
(availability over strict dedup), but for traffic where a duplicate is
unacceptable — payments, anything non-idempotent at the business layer — set
`fallback_on_redis_error=False` to fail closed (re-raise the Redis error)
instead.

Set `mark_policy="on_start"` if you need to prevent concurrent duplicate processing at the cost of retry safety.
