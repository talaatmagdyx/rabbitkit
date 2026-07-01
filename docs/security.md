# Security Notes

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

`SigningMiddleware` signs and verifies message bodies using HMAC-SHA256. It uses `hmac.compare_digest` for constant-time comparison to prevent timing attacks. Configure it with a secret that is at least 32 bytes:

```python
from rabbitkit import SigningConfig
from rabbitkit.middleware.signing import SigningMiddleware

mw = SigningMiddleware(config=SigningConfig(secret="a-long-random-secret-at-least-32-bytes"))
```

Rotate the signing secret periodically. Store it in a secrets manager, not in environment variables that may be logged.

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

Set `mark_policy="on_start"` if you need to prevent concurrent duplicate processing at the cost of retry safety.
