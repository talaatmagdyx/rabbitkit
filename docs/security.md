# Security Notes

Found a vulnerability in rabbitkit itself (not just a hardening question)?
See [SECURITY.md](https://github.com/talaatmagdy/rabbitkit/blob/main/SECURITY.md)
for how to report it privately.

## Safe defaults at a glance

A scannable summary — each row links to the section with the full story.

| Feature | Safe by default? | What you must configure for production |
|---|---|---|
| TLS (`SSLConfig`) | Yes, when enabled: `CERT_REQUIRED`, hostname verification, TLS ≥ 1.2 | You must still explicitly enable it (`SSLConfig(enabled=True)`) and point it at real certs — see [TLS for Broker Connections](#tls-for-broker-connections). |
| Default credentials | No — rabbitkit warns if you use `guest`/`guest` against a non-local host, but does not block it | Use dedicated, least-privilege credentials per service; never ship the default in production — see [Credentials](#credentials). |
| Config `repr()` / `.url` | Yes — passwords are masked in `repr()` and in `.safe_url` | Use `.safe_url` (not `.url`) anywhere you might log a config object; `.url` still contains the plaintext password by necessity (it's what actually connects). |
| Message signing (`SigningMiddleware`) | Partially — HMAC comparison is constant-time and the signature covers routing metadata, not just the body | The default nonce cache is **per-process** — real replay protection across multiple workers/pods requires wiring `RedisNonceCache` yourself. You'll get a `RuntimeWarning` if you don't — don't ignore it. See [Shared nonce store](#shared-nonce-store-for-multiprocessmultipod-deployments). |
| Deduplication (`DeduplicationMiddleware`) | Fails open by default (`fallback_on_redis_error=True`) — availability over strict dedup | For payments or anything non-idempotent at the business layer, set `fallback_on_redis_error=False` to fail closed. Either way, see [the idempotency contract](production/idempotency.md) — dedup is a mitigation, not a substitute for idempotent handlers. |
| Management API client (`RabbitManagementClient`) | Yes — rejects non-`http(s)` schemes, doesn't follow redirects, caps response size | Never construct `ManagementConfig.url` from user-controllable input, even though the client defends against SSRF-class abuse itself. |
| Distributed locking (`RedisLock`) | Release is atomic (compare-and-delete) | `ttl` has **no auto-renewal** — a slow handler can lose the lock mid-work. Size `ttl` well above worst-case handler time; use `fencing_token()` for downstream writes that must not double-apply. |
| Monitoring dashboard (`create_dashboard_app`) | **No — unauthenticated by default** | Always pass `auth_token=`, bind to loopback only, and put it behind a reverse proxy for anything beyond local debugging. This is the single riskiest default in the toolkit if ignored — see [`docs/stability-policy.md`](stability-policy.md)'s Experimental section. |
| Result backends (`ResultBackend`) | N/A — no auth built in, it's a Redis key-value store under the hood | Treat correlation IDs as sensitive if the stored result is; the TTL you set is the only cleanup mechanism. |

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

### Credential rotation

For short-lived / rotated secrets (Vault, IAM), pass a `credentials_provider`
instead of static `username`/`password`. It is called at every (re)connect,
so a rotated secret is picked up on the next reconnect with no redeploy:

```python
from rabbitkit import RabbitConfig, ConnectionConfig

config = RabbitConfig(
    connection=ConnectionConfig(
        host="rabbit",
        credentials_provider=lambda: vault.read("rabbitmq/creds"),  # -> (user, password)
    )
)
```

The provider must be quick and non-blocking (it runs on the connect path).
Cache the secret and refresh it out-of-band; the reconnect just reads the
current value.

### Least-privilege consumers with `TopologyMode.PASSIVE_ONLY`

The default `TopologyMode.AUTO_DECLARE` issues `queue.declare`/
`exchange.declare`/`queue.bind` for every route at startup — which requires
the connecting user to hold RabbitMQ's **configure** permission on those
resources, in addition to **write** (publish) and **read** (consume).
Granting `configure` to every consumer service means any of them could
redeclare, rebind, or delete topology it doesn't own.

`TopologyMode.PASSIVE_ONLY` only ever *checks* that a queue/exchange already
exists (`passive=True` declares, which RabbitMQ permits with the read/write
permissions alone) — it never creates or mutates topology. A dedicated
topology-owning process/deploy step declares the real topology once (with a
`configure`-scoped credential), and every consumer/producer service instead
connects with a credential scoped to just `write`/`read` on its own
queues/exchanges (via RabbitMQ's per-vhost `set_permissions <user> <conf>
<write> <read>` regex patterns — an empty or narrowly-scoped `configure`
pattern):

```python
from rabbitkit import RabbitConfig, ConnectionConfig
from rabbitkit.core.types import TopologyMode

config = RabbitConfig(
    connection=ConnectionConfig(host="rabbit", username="orders-consumer"),
    topology_mode=TopologyMode.PASSIVE_ONLY,
)
```

If the expected topology doesn't exist, `PASSIVE_ONLY` fails fast at startup
with a clear error instead of silently trying (and being denied permission)
to create it. `TopologyMode.MANUAL` goes further — it skips topology
declaration/checking entirely, for callers that manage topology completely
out-of-band (e.g. via `rabbitmqadmin`/Terraform).

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

If you need to prevent concurrent duplicate processing, use
`mark_policy="claim"`: it takes an atomic in-flight claim (expiring after
`processing_timeout`) before the handler and flips it to completed on
success, so concurrent copies are requeued rather than double-executed and a
crash mid-handler never loses the message — set `processing_timeout`
comfortably above your worst-case handler duration. Avoid
`mark_policy="on_start"` unless you explicitly accept that a crash after the
mark but before the handler finishes **loses the message** (the redelivery is
skipped as a duplicate).
