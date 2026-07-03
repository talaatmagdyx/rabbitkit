# Middleware

## BaseMiddleware (base class)

::: rabbitkit.middleware.base.BaseMiddleware
::: rabbitkit.middleware.base.NoOpMiddleware

## RetryMiddleware

::: rabbitkit.middleware.retry.RetryMiddleware
::: rabbitkit.middleware.retry.RetryRouter

## CompressionMiddleware

::: rabbitkit.middleware.compression.CompressionMiddleware

## SigningMiddleware

::: rabbitkit.middleware.signing.SigningMiddleware
::: rabbitkit.middleware.signing.SigningConfig
::: rabbitkit.middleware.signing.InvalidSignatureError

## RateLimitMiddleware

::: rabbitkit.middleware.rate_limit.RateLimitMiddleware
::: rabbitkit.middleware.rate_limit.RateLimitConfig

## DeduplicationMiddleware

`DeduplicationConfig.mark_policy` controls when the dedup key is recorded
(accepts a `DeduplicationMarkPolicy` enum member or its string value):

| Value | Prevents concurrent duplicates | Crash-safe | Behaviour |
|---|---|---|---|
| `"on_success"` (default) | No | Yes | Key checked (no write) before the handler, stored only after it succeeds. A consumer killed mid-handler leaves no mark, so the redelivery is processed. Concurrent duplicates may both process (at-least-once). Use for most consumers. |
| `"claim"` | Yes | Yes¹ | Two-state: atomic `in-flight` claim (TTL = `processing_timeout`) before the handler, flipped to `completed` (full `ttl`) on success. A concurrent copy that sees a live claim is nack-requeued (default `on_in_flight="nack_requeue"`; `"ack_skip"` available) so it retries if the claiming consumer dies. A crash lets the claim expire. Use for sensitive side effects. |
| `"on_start"` | Yes | **No — can lose messages** | Key stored before the handler runs. If the process crashes after marking but before the handler finishes, the redelivery is skipped as a duplicate and the message is lost. Use only when duplicate execution is worse than message loss. |

¹ Provided `processing_timeout` comfortably exceeds the worst-case handler
duration — a handler that outlives its claim lets a duplicate start while it
is still running.

Deduplication is not a replacement for idempotent business logic: RabbitMQ is
an at-least-once system, and a handler can still run twice in some failure
scenarios (e.g. side effect completes but the ack fails, or a claim expires
under a slow handler). Keep a business-level guard — unique DB constraint,
`payment_id`/`transaction_id`, outbox or `processed_events` table — for
anything non-idempotent.

::: rabbitkit.middleware.deduplication.DeduplicationMiddleware

## CircuitBreakerMiddleware

::: rabbitkit.middleware.circuit_breaker.CircuitBreakerMiddleware
::: rabbitkit.middleware.circuit_breaker.CircuitBreakerOpenError

## TimeoutMiddleware

::: rabbitkit.middleware.timeout.TimeoutMiddleware

## MetricsMiddleware

::: rabbitkit.middleware.metrics.MetricsMiddleware
::: rabbitkit.middleware.metrics.MetricsCollector
::: rabbitkit.middleware.metrics.PrometheusCollector
::: rabbitkit.middleware.metrics.metrics_app
::: rabbitkit.middleware.metrics.start_metrics_server

## ExceptionMiddleware

::: rabbitkit.middleware.exception.ExceptionMiddleware

## TracedConsumerMiddleware

::: rabbitkit.middleware.tracing.TracedConsumerMiddleware
