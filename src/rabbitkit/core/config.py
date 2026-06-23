"""Focused configuration objects — composable, immutable, validated.

Split into focused dataclasses. Each has clear responsibility.
RabbitConfig only composes connection/broker defaults.
Throughput/batching config objects are accepted by their respective components.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlparse

from rabbitkit.core.logging import LoggingConfig
from rabbitkit.core.types import ErrorSeverity, TopologyMode

# ── Connection ─────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class ConnectionConfig:
    """Core connection parameters."""

    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    vhost: str = "/"
    heartbeat: int = 30
    socket_timeout: float = 10.0
    blocked_connection_timeout: float = 300.0
    connection_name: str | None = None
    reconnect_backoff_base: float = 1.0
    reconnect_backoff_max: float = 30.0

    @property
    def url(self) -> str:
        """Build AMQP URL from config fields."""
        vhost = self.vhost
        if vhost == "/":
            vhost = "%2F"
        return f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/{vhost}"

    @classmethod
    def from_url(cls, url: str) -> ConnectionConfig:
        """Parse an AMQP URL into a ConnectionConfig.

        Supports: amqp://user:pass@host:port/vhost?heartbeat=30&connection_timeout=10
        """
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        vhost = parsed.path.lstrip("/") if parsed.path and parsed.path != "/" else "/"
        if vhost == "%2F" or vhost == "":
            vhost = "/"

        kwargs: dict[str, str | int | float | None] = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5672,
            "username": parsed.username or "guest",
            "password": parsed.password or "guest",
            "vhost": vhost,
        }

        if "heartbeat" in query:
            kwargs["heartbeat"] = int(query["heartbeat"][0])
        if "connection_timeout" in query:
            kwargs["socket_timeout"] = float(query["connection_timeout"][0])
        if "blocked_connection_timeout" in query:
            kwargs["blocked_connection_timeout"] = float(query["blocked_connection_timeout"][0])

        return cls(**kwargs)  # type: ignore[arg-type]


# ── TCP/Socket ─────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SocketConfig:
    """Low-level TCP tuning.

    Applied best-effort — not all options are universally guaranteed
    depending on OS and backend internals.
    """

    tcp_nodelay: bool = True
    tcp_keepidle: int = 10
    tcp_keepintvl: int = 5
    tcp_keepcnt: int = 3
    tcp_sndbuf: int = 196608  # 192KB
    tcp_rcvbuf: int = 196608  # 192KB


# ── SSL/TLS ────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SSLConfig:
    """TLS/SSL configuration."""

    enabled: bool = False
    certfile: str | None = None
    keyfile: str | None = None
    ca_certs: str | None = None
    cert_reqs: str = "CERT_REQUIRED"
    server_hostname: str | None = None


# ── Security ───────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SecurityConfig:
    """SASL + authentication configuration."""

    mechanism: str = "PLAIN"
    ssl: SSLConfig = field(default_factory=SSLConfig)


# ── Publisher ──────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PublisherConfig:
    """Publisher behavior tuning."""

    exchange: str = ""
    confirm_delivery: bool = True
    confirm_timeout: float = 5.0
    mandatory: bool = False
    persistent: bool = True


# ── Consumer ───────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class ConsumerConfig:
    """Consumer behavior tuning."""

    prefetch_count: int = 10
    graceful_timeout: float = 30.0


# ── Pool ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PoolConfig:
    """Connection and channel pool sizing.

    ``channel_pool_size`` (publisher channel pool) and ``channel_acquire_timeout``
    are active. ``publisher_connections`` / ``consumer_connections`` are
    **reserved**: the transport currently uses one connection per role, because
    multiple connections sharing a single event loop showed no throughput benefit
    in benchmarks (the loop is the bound). Scale throughput by running more
    processes/pods, not more connections per process.
    """

    channel_pool_size: int = 10
    publisher_connections: int = 1   # reserved — see class docstring
    consumer_connections: int = 1    # reserved — see class docstring
    channel_acquire_timeout: float = 10.0  # seconds to wait for a pooled channel


# ── Retry ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class RetryConfig:
    """Retry with delay queues configuration.

    Accepted by RetryMiddleware. Can be set as broker default
    (RabbitConfig.retry) or per-route override (route.retry_override).

    ``delays`` must have at least ``max_retries`` entries.  Extra retries
    beyond the length of ``delays`` would silently reuse the last delay,
    which is almost always a misconfiguration.
    """

    max_retries: int = 4
    delays: tuple[int, ...] = (5, 30, 120, 600)
    retry_header: str = "x-rabbitkit-retry-count"
    jitter_factor: float = 0.1
    dead_letter_exchange: str = ""
    per_queue: bool = True
    unknown_policy: ErrorSeverity = ErrorSeverity.PERMANENT

    def __post_init__(self) -> None:
        import warnings
        if self.max_retries < 0:
            raise ValueError(f"RetryConfig.max_retries must be >= 0, got {self.max_retries}")
        if self.delays and len(self.delays) < self.max_retries:
            warnings.warn(
                f"RetryConfig.delays has {len(self.delays)} entries but max_retries={self.max_retries}. "
                "Retries beyond the last delay entry will reuse the last delay value. "
                "Consider providing at least max_retries delay values for explicit control.",
                UserWarning,
                stacklevel=2,
            )


# ── Compression ────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class CompressionConfig:
    """Compression configuration.

    Accepted by CompressionMiddleware.
    """

    algorithm: str = "gzip"
    threshold: int = 1024
    level: int = 6


# ── Metrics ───────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class MetricsConfig:
    """Metrics naming configuration.

    ``namespace`` sets the metric name prefix (default "rabbitkit").
    Individual name fields override the derived default when non-empty.
    """

    namespace: str = "rabbitkit"
    consumed_counter: str = ""
    processing_histogram: str = ""
    published_counter: str = ""
    publish_histogram: str = ""

    @property
    def consumed_total(self) -> str:
        return self.consumed_counter or f"{self.namespace}_messages_consumed_total"

    @property
    def processing_seconds(self) -> str:
        return self.processing_histogram or f"{self.namespace}_message_processing_seconds"

    @property
    def published_total(self) -> str:
        return self.published_counter or f"{self.namespace}_messages_published_total"

    @property
    def publish_seconds(self) -> str:
        return self.publish_histogram or f"{self.namespace}_message_publish_seconds"


# ── Health Check ──────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class HealthCheckConfig:
    """Thresholds for broker_health_check()."""

    pending_threshold: int = 100


# ── Sentinel ──────────────────────────────────────────────────────────────


class RetryDisabled:
    """Typed singleton — explicitly disables retry on a route.

    Distinct from RetryConfig(max_retries=0) which means 'retry-owned
    terminal semantics with zero retry attempts (immediate DLQ on any
    classified error).'
    """

    _instance: RetryDisabled | None = None

    def __new__(cls) -> RetryDisabled:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "RETRY_DISABLED"

    def __bool__(self) -> bool:
        return False


RETRY_DISABLED = RetryDisabled()


# ── Deduplication (0.2.0 — placeholder) ──────────────────────────────────


@dataclass(frozen=True, slots=True)
class DeduplicationConfig:
    """Deduplication configuration. Used in 0.2.0."""

    key_prefix: str = "rabbitkit:dedup"
    ttl: int = 86400
    fallback_on_redis_error: bool = True
    key_source: str = "message_id"


# ── Backpressure (0.2.0 — placeholder) ───────────────────────────────────


@dataclass(frozen=True, slots=True)
class BackpressureConfig:
    """Backpressure configuration. Used in 0.2.0."""

    max_in_flight: int = 1000
    rate_limit: int | None = None
    blocked_timeout: float = 60.0
    on_blocked: str = "wait"
    poll_interval_ms: int = 10


# ── Batch (0.2.0 — placeholder) ──────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class BatchPublishConfig:
    """Batch publish configuration. Used in 0.2.0."""

    batch_size: int = 100
    flush_interval_ms: int = 50
    max_in_flight: int = 1000


@dataclass(frozen=True, slots=True)
class BatchAckConfig:
    """Batch ack configuration. Used in 0.2.0."""

    batch_size: int = 100
    flush_interval_ms: int = 200


# ── Worker (0.2.0 — placeholder) ─────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Consumer concurrency configuration.

    Accepted by broker.start(), NOT part of RabbitConfig. Added in 0.2.0.
    """

    worker_count: int = 1
    prefetch_per_worker: int | None = None
    stop_timeout: float = 30.0


# ── Top-Level Config ─────────────────────────────────────────────────────


@dataclass(slots=True)
class RabbitConfig:
    """Top-level config — composes focused config objects.

    Only connection/broker defaults. Throughput/batching configs
    are accepted by their respective components directly.
    """

    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    socket: SocketConfig = field(default_factory=SocketConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    publisher: PublisherConfig = field(default_factory=PublisherConfig)
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)
    pool: PoolConfig = field(default_factory=PoolConfig)
    topology_mode: TopologyMode = TopologyMode.AUTO_DECLARE
    retry: RetryConfig | None = None
    compression: CompressionConfig | None = None
    logging: LoggingConfig | None = None
