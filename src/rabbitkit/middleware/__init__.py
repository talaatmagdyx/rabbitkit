"""Middleware module — composable consume/publish processing."""

from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware, CircuitBreakerOpenError
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
from rabbitkit.middleware.metrics import MetricsCollector, MetricsMiddleware, PrometheusCollector
from rabbitkit.middleware.rate_limit import RateLimitConfig, RateLimitMiddleware
from rabbitkit.middleware.signing import InvalidSignatureError, SigningConfig, SigningMiddleware
from rabbitkit.middleware.timeout import HandlerTimeoutError, TimeoutConfig, TimeoutMiddleware
from rabbitkit.middleware.otel import OTelTracingMiddleware
from rabbitkit.middleware.tracing import TracedConsumerMiddleware

__all__ = [
    "CircuitBreakerMiddleware",
    "CircuitBreakerOpenError",
    "DeduplicationMiddleware",
    "HandlerTimeoutError",
    "InvalidSignatureError",
    "MetricsCollector",
    "MetricsMiddleware",
    "PrometheusCollector",
    "RateLimitConfig",
    "RateLimitMiddleware",
    "SigningConfig",
    "SigningMiddleware",
    "TimeoutConfig",
    "TimeoutMiddleware",
    "OTelTracingMiddleware",
    "TracedConsumerMiddleware",
]
