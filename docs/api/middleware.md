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

`DeduplicationConfig.mark_policy` controls when the dedup key is recorded:

| Value | Behaviour |
|---|---|
| `"on_success"` (default) | Key stored after handler succeeds. Safer for retries. |
| `"on_start"` | Key stored before handler runs. Prevents concurrent dual-execution. |

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
