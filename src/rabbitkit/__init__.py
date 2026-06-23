"""rabbitkit — Production-grade RabbitMQ toolkit."""

from rabbitkit._version import __version__
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
    SecurityConfig,
    SocketConfig,
    SSLConfig,
    WorkerConfig,
)
from rabbitkit.core.errors import BackpressureError
from rabbitkit.core.logging import LoggingConfig, configure_structlog
from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage
from rabbitkit.core.router import RabbitRouter
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import (
    AckPolicy,
    ClassifiedError,
    ErrorSeverity,
    ExchangeType,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    QueueType,
    TopologyMode,
)
from rabbitkit.dashboard import create_dashboard_app
from rabbitkit.di.resolver import DependencyScope
from rabbitkit.dlq import DLQInspector
from rabbitkit.fastapi import rabbitkit_lifespan
from rabbitkit.health import BrokerHealthResult, HealthStatus, broker_health_check, broker_health_check_async
from rabbitkit.highload.backpressure import FlowController
from rabbitkit.highload.batch import BatchAcker, BatchPublisher
from rabbitkit.locking import DistributedLock, LockMiddleware, RedisLock
from rabbitkit.management import ManagementConfig, RabbitManagementClient
from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware, CircuitBreakerOpenError
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
from rabbitkit.middleware.metrics import MetricsCollector, MetricsMiddleware, PrometheusCollector
from rabbitkit.middleware.rate_limit import RateLimitConfig, RateLimitMiddleware
from rabbitkit.middleware.signing import InvalidSignatureError, SigningConfig, SigningMiddleware
from rabbitkit.middleware.tracing import TracedConsumerMiddleware
from rabbitkit.results.backend import RedisResultBackend, ResultBackend
from rabbitkit.results.middleware import ResultMiddleware
from rabbitkit.rpc import AsyncRPCClient, RPCClient, RPCTimeoutError
from rabbitkit.serialization.pipeline import (
    DataclassDecoder,
    JsonParser,
    MessageDecoder,
    MessageParser,
    PydanticDecoder,
    RawDecoder,
    SerializationPipeline,
)
from rabbitkit.streams import StreamConsumerConfig, StreamOffset, StreamOffsetType

__all__ = [
    "RETRY_DISABLED",
    "AckMessage",
    "AckPolicy",
    "AppState",
    "AsyncAPIGeneratorConfig",
    "AsyncRPCClient",
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
    "ConnectionConfig",
    "ConsumerConfig",
    "DLQInspector",
    "DataclassDecoder",
    "DeduplicationConfig",
    "DeduplicationMiddleware",
    "DependencyScope",
    "DistributedLock",
    "ErrorSeverity",
    "ExchangeType",
    "FlowController",
    "HealthCheckConfig",
    "HealthStatus",
    "InvalidSignatureError",
    "JsonParser",
    "LockMiddleware",
    "LoggingConfig",
    "ManagementConfig",
    "MessageDecoder",
    "MessageEnvelope",
    "MessageParser",
    "MetricsCollector",
    "MetricsConfig",
    "MetricsMiddleware",
    "NackMessage",
    "PoolConfig",
    "PrometheusCollector",
    "PublishOutcome",
    "PublishStatus",
    "PublisherConfig",
    "PydanticDecoder",
    "QueueType",
    "RPCClient",
    "RPCTimeoutError",
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
    "RedisLock",
    "RedisResultBackend",
    "RejectMessage",
    "ResultBackend",
    "ResultMiddleware",
    "RetryConfig",
    "RetryDisabled",
    "SSLConfig",
    "SecurityConfig",
    "SerializationPipeline",
    "SigningConfig",
    "SigningMiddleware",
    "SocketConfig",
    "StreamConsumerConfig",
    "StreamOffset",
    "StreamOffsetType",
    "SyncWorkerPool",
    "TopologyMode",
    "TracedConsumerMiddleware",
    "WorkerConfig",
    "__version__",
    "broker_health_check",
    "broker_health_check_async",
    "configure_structlog",
    "create_dashboard_app",
    "generate_asyncapi_doc",
    "generate_asyncapi_json",
    "rabbitkit_lifespan",
]
