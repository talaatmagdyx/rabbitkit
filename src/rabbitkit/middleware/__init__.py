"""Middleware module — composable consume/publish processing."""

from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware, CircuitBreakerOpenError
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
from rabbitkit.middleware.event_metadata import EventMetadataMiddleware
from rabbitkit.middleware.metrics import (
    BrokerMetrics,
    MetricsCollector,
    MetricsMiddleware,
    MetricSpec,
    PrometheusCollector,
    metric_inventory,
)
from rabbitkit.middleware.otel import OTelTracingMiddleware
from rabbitkit.middleware.rate_limit import RateLimitConfig, RateLimitMiddleware
from rabbitkit.middleware.signing import InvalidSignatureError, SigningConfig, SigningMiddleware
from rabbitkit.middleware.timeout import HandlerTimeoutError, TimeoutConfig, TimeoutMiddleware

__all__ = [
    "BrokerMetrics",
    "CircuitBreakerMiddleware",
    "CircuitBreakerOpenError",
    "DeduplicationMiddleware",
    "EventMetadataMiddleware",
    "HandlerTimeoutError",
    "InvalidSignatureError",
    "MetricSpec",
    "MetricsCollector",
    "MetricsMiddleware",
    "OTelTracingMiddleware",
    "PrometheusCollector",
    "RateLimitConfig",
    "RateLimitMiddleware",
    "SigningConfig",
    "SigningMiddleware",
    "TimeoutConfig",
    "TimeoutMiddleware",
    "metric_inventory",
]
