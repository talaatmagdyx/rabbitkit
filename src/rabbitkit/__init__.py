"""rabbitkit — Production-grade RabbitMQ toolkit."""

from rabbitkit import experimental
from rabbitkit._version import __version__
from rabbitkit.async_.batch import AsyncBatchPublisher
from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.asyncapi import AsyncAPIGeneratorConfig, generate_asyncapi_doc, generate_asyncapi_json
from rabbitkit.concurrency import AsyncWorkerPool, SyncWorkerPool
from rabbitkit.core.app import AppState, RabbitApp
from rabbitkit.core.config import (
    RETRY_DISABLED,
    BackpressureConfig,
    BatchAckConfig,
    BatchPublishConfig,
    CompressionConfig,
    ConnectionConfig,
    ConsumerConfig,
    DeduplicationConfig,
    HealthCheckConfig,
    MetricsConfig,
    PoolConfig,
    PublisherConfig,
    RabbitConfig,
    RetryConfig,
    RetryDisabled,
    SafetyConfig,
    SecurityConfig,
    SocketConfig,
    SSLConfig,
    WorkerConfig,
)
from rabbitkit.core.errors import (
    BackpressureError,
    ConfigurationError,
    MissingDependencyError,
    UnsafeTopologyError,
)
from rabbitkit.core.logging import DEFAULT_REDACT_KEYS, LoggingConfig, configure_structlog
from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage
from rabbitkit.core.router import RabbitRouter
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import (
    AckPolicy,
    ClassifiedError,
    DeduplicationMarkPolicy,
    ErrorSeverity,
    ExchangeType,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    QueueType,
    RejectWithoutDLXPolicy,
    TopologyMode,
)
from rabbitkit.di import Context, ContextRepo, Depends, DIResolver, Header, Path
from rabbitkit.di.resolver import DependencyScope
from rabbitkit.dlq import DLQInspector, ReplayResult
from rabbitkit.fastapi import rabbitkit_lifespan
from rabbitkit.health import (
    BrokerHealthResult,
    HealthStatus,
    broker_health_check,
    broker_health_check_async,
    broker_liveness,
    broker_liveness_async,
    broker_readiness,
    broker_readiness_async,
)
from rabbitkit.highload.backpressure import FlowController
from rabbitkit.highload.batch import BatchAcker, BatchPublisher
from rabbitkit.management import ManagementConfig, RabbitManagementClient
from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware, CircuitBreakerOpenError
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
from rabbitkit.middleware.metrics import (
    MetricsCollector,
    MetricsMiddleware,
    PrometheusCollector,
    metrics_app,
    start_metrics_server,
)
from rabbitkit.middleware.rate_limit import RateLimitConfig, RateLimitMiddleware
from rabbitkit.middleware.tracing import TracedConsumerMiddleware
from rabbitkit.queue_metrics import QueueMetricsPoller
from rabbitkit.serialization.pipeline import (
    DataclassDecoder,
    JsonParser,
    MessageDecoder,
    MessageParser,
    PydanticDecoder,
    RawDecoder,
    SerializationPipeline,
)
from rabbitkit.sync.broker import SyncBroker

__all__ = [
    "DEFAULT_REDACT_KEYS",
    "RETRY_DISABLED",
    "AckMessage",
    "AckPolicy",
    "AppState",
    "AsyncAPIGeneratorConfig",
    "AsyncBatchPublisher",
    "AsyncBroker",
    "AsyncWorkerPool",
    "BackpressureConfig",
    "BackpressureError",
    "BatchAckConfig",
    "BatchAcker",
    "BatchPublishConfig",
    "BatchPublisher",
    "BrokerHealthResult",
    "CircuitBreakerMiddleware",
    "CircuitBreakerOpenError",
    "ClassifiedError",
    "CompressionConfig",
    "ConfigurationError",
    "ConnectionConfig",
    "ConsumerConfig",
    "Context",
    "ContextRepo",
    "DIResolver",
    "DLQInspector",
    "DataclassDecoder",
    "DeduplicationConfig",
    "DeduplicationMarkPolicy",
    "DeduplicationMiddleware",
    "DependencyScope",
    "Depends",
    "ErrorSeverity",
    "ExchangeType",
    "FlowController",
    "Header",
    "HealthCheckConfig",
    "HealthStatus",
    "JsonParser",
    "LoggingConfig",
    "ManagementConfig",
    "MessageDecoder",
    "MessageEnvelope",
    "MessageParser",
    "MetricsCollector",
    "MetricsConfig",
    "MetricsMiddleware",
    "MissingDependencyError",
    "NackMessage",
    "Path",
    "PoolConfig",
    "PrometheusCollector",
    "PublishOutcome",
    "PublishStatus",
    "PublisherConfig",
    "PydanticDecoder",
    "QueueMetricsPoller",
    "QueueType",
    "RabbitApp",
    "RabbitConfig",
    "RabbitExchange",
    "RabbitManagementClient",
    "RabbitMessage",
    "RabbitQueue",
    "RabbitRouter",
    "RateLimitConfig",
    "RateLimitMiddleware",
    "RawDecoder",
    "RejectMessage",
    "RejectWithoutDLXPolicy",
    "ReplayResult",
    "RetryConfig",
    "RetryDisabled",
    "SSLConfig",
    "SafetyConfig",
    "SecurityConfig",
    "SerializationPipeline",
    "SocketConfig",
    "SyncBroker",
    "SyncWorkerPool",
    "TopologyMode",
    "TracedConsumerMiddleware",
    "UnsafeTopologyError",
    "WorkerConfig",
    "__version__",
    "broker_health_check",
    "broker_health_check_async",
    "broker_liveness",
    "broker_liveness_async",
    "broker_readiness",
    "broker_readiness_async",
    "configure_structlog",
    "experimental",
    "generate_asyncapi_doc",
    "generate_asyncapi_json",
    "metrics_app",
    "rabbitkit_lifespan",
    "start_metrics_server",
]
