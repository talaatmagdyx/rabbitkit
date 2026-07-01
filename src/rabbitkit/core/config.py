"""Focused configuration objects — composable, immutable, validated.

Split into focused dataclasses. Each has clear responsibility.
RabbitConfig only composes connection/broker defaults.
Throughput/batching config objects are accepted by their respective components.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from urllib.parse import parse_qs, quote, unquote, urlparse

from rabbitkit.core.logging import LoggingConfig
from rabbitkit.core.types import ErrorSeverity, TopologyMode

# ── Connection ─────────────────────────────────────────────────────────────


def _masked_repr(obj: object, *, secret_fields: tuple[str, ...] = ("password",)) -> str:
    """L2: generic ``__repr__`` for a config dataclass that masks *secret_fields*.

    The default dataclass-generated ``__repr__`` includes every field
    verbatim — any log line or traceback that reprs a config object (or logs
    it directly, which falls back to ``__repr__``) leaks the plaintext
    password. Iterates ``dataclasses.fields()`` generically so a field added
    later is still included (just not masked unless also listed).
    """
    import dataclasses

    parts = []
    for f in dataclasses.fields(obj):  # type: ignore[arg-type]
        value = "'***'" if f.name in secret_fields else repr(getattr(obj, f.name))
        parts.append(f"{f.name}={value}")
    return f"{type(obj).__name__}({', '.join(parts)})"


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
    # k8s-friendly default: fail fast on a blocked connection rather than
    # appearing healthy for 5 minutes while publishing stalls.
    blocked_connection_timeout: float = 60.0
    connection_name: str | None = None
    reconnect_backoff_base: float = 1.0
    reconnect_backoff_max: float = 30.0

    def __repr__(self) -> str:
        return _masked_repr(self)

    @property
    def url(self) -> str:
        """Build AMQP URL from config fields.

        Username, password and vhost are URL-encoded so credentials containing
        reserved characters (``@:/#`` etc.) cannot corrupt the host/port parse
        (mirrors the encoding done in ``async_/connection.py``).

        SECURITY (L2): this embeds the plaintext password (``user:pass@host``)
        — never log or repr it. Use :attr:`safe_url` for logging/display.
        """
        if self.vhost == "/":
            vhost = "%2F"
        else:
            vhost = quote(self.vhost, safe="")
        user = quote(self.username, safe="")
        pwd = quote(self.password, safe="")
        return f"amqp://{user}:{pwd}@{self.host}:{self.port}/{vhost}"

    @property
    def safe_url(self) -> str:
        """Like :attr:`url` but with the password masked (L2) — safe to log."""
        if self.vhost == "/":
            vhost = "%2F"
        else:
            vhost = quote(self.vhost, safe="")
        user = quote(self.username, safe="")
        return f"amqp://{user}:***@{self.host}:{self.port}/{vhost}"

    def __post_init__(self) -> None:
        # Surface the insecure default-credentials-against-non-local-host mistake
        # once at construction (not per-connection). The default is kept for dev
        # convenience; this only warns.
        if self.username == "guest" and self.host not in {"localhost", "127.0.0.1", "::1"}:
            warnings.warn(
                "ConnectionConfig uses default 'guest' credentials against non-local "
                f"host {self.host!r}; set explicit username/password for production.",
                UserWarning,
                stacklevel=2,
            )

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
            # Percent-decode credentials so an encoded AMQP URL round-trips
            # without double-encoding (e.g. user%40 -> user@).
            "username": unquote(parsed.username) if parsed.username else "guest",
            "password": unquote(parsed.password) if parsed.password else "guest",
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
    publisher_connections: int = 1  # reserved — see class docstring
    consumer_connections: int = 1  # reserved — see class docstring
    channel_acquire_timeout: float = 10.0  # seconds to wait for a pooled channel
    prewarm_channels: bool = False  # pre-create all pool channels on connect() to eliminate warmup jitter


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
    strict_delays: bool = True

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError(f"RetryConfig.max_retries must be >= 0, got {self.max_retries}")
        if len(self.delays) < self.max_retries:
            msg = (
                f"RetryConfig.delays has {len(self.delays)} entries but max_retries={self.max_retries}. "
                "Retries beyond the last delay entry will reuse the last delay value, "
                "which is almost always a misconfiguration. Provide at least max_retries "
                "delay values, or set strict_delays=False to allow the flat-tail behavior."
            )
            if self.strict_delays:
                raise ValueError(msg)
            import warnings

            warnings.warn(msg, UserWarning, stacklevel=2)


# NOTE: ``RetryConfig`` no longer imports ``warnings`` at module top-level; the
# warning is only emitted on the non-strict path. Strict (default) raises so
# misconfiguration fails fast per the project's fail-fast philosophy.


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
    def messages_acked_total(self) -> str:
        return f"{self.namespace}_messages_acked_total"

    @property
    def messages_nacked_total(self) -> str:
        return f"{self.namespace}_messages_nacked_total"

    @property
    def messages_rejected_total(self) -> str:
        return f"{self.namespace}_messages_rejected_total"

    @property
    def messages_retried_total(self) -> str:
        return f"{self.namespace}_messages_retried_total"

    @property
    def messages_dead_lettered_total(self) -> str:
        return f"{self.namespace}_messages_dead_lettered_total"

    @property
    def dedup_fallback_total(self) -> str:
        """M9: incremented every time DeduplicationMiddleware falls back to
        processing a message despite a Redis error (idempotency is not
        enforced for that message) — see
        ``DeduplicationConfig.fallback_on_redis_error``."""
        return f"{self.namespace}_dedup_fallback_total"

    @property
    def rate_limit_dropped_total(self) -> str:
        """L5: incremented every time RateLimitMiddleware settles a message
        without calling the handler — nack/drop policy, or the "wait" policy's
        deadline elapsing with no token acquired. Labeled by ``reason``
        (``nack``/``drop``/``wait_deadline_exceeded``)."""
        return f"{self.namespace}_rate_limit_dropped_total"

    @property
    def published_total(self) -> str:
        return self.published_counter or f"{self.namespace}_messages_published_total"

    @property
    def publish_total(self) -> str:
        return self.published_counter or f"{self.namespace}_publish_total"

    @property
    def publish_failures_total(self) -> str:
        return f"{self.namespace}_publish_failures_total"

    @property
    def publish_confirm_latency_seconds(self) -> str:
        return f"{self.namespace}_publish_confirm_latency_seconds"

    @property
    def publish_seconds(self) -> str:
        return self.publish_histogram or f"{self.namespace}_message_publish_seconds"

    @property
    def in_flight_messages(self) -> str:
        return f"{self.namespace}_in_flight_messages"

    @property
    def worker_pool_pending(self) -> str:
        return f"{self.namespace}_worker_pool_pending"

    @property
    def broker_connected(self) -> str:
        return f"{self.namespace}_broker_connected"

    @property
    def consumer_active(self) -> str:
        return f"{self.namespace}_consumer_active"


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


# ── Deduplication (active) ──────────────────────────────────


@dataclass(frozen=True, slots=True)
class DeduplicationConfig:
    """Deduplication configuration. Active."""

    key_prefix: str = "rabbitkit:dedup"
    ttl: int = 86400
    fallback_on_redis_error: bool = True
    key_source: str = "message_id"
    mark_policy: str = "on_success"
    local_cache_size: int = 0  # 0 = disabled; >0 = in-process LRU capacity (short-circuits Redis for known duplicates)


# ── Backpressure (active) ───────────────────────────────────


@dataclass(frozen=True, slots=True)
class BackpressureConfig:
    """Backpressure configuration. Active."""

    max_in_flight: int = 1000
    rate_limit: int | None = None
    blocked_timeout: float = 60.0
    on_blocked: str = "wait"
    poll_interval_ms: int = 10


# ── Batch (active) ──────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class BatchPublishConfig:
    """Batch publish configuration. Active."""

    batch_size: int = 100
    flush_interval_ms: int = 50
    max_in_flight: int = 1000
    flush_workers: int = 0  # 0 = auto (min(16, max_in_flight // batch_size)); >0 = explicit count

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError(f"BatchPublishConfig.batch_size must be > 0, got {self.batch_size}")
        if self.flush_interval_ms < 0:
            raise ValueError(
                f"BatchPublishConfig.flush_interval_ms must be >= 0, got {self.flush_interval_ms}"
            )
        if self.max_in_flight <= 0:
            raise ValueError(
                f"BatchPublishConfig.max_in_flight must be > 0, got {self.max_in_flight}"
            )
        if self.flush_workers < 0:
            raise ValueError(
                f"BatchPublishConfig.flush_workers must be >= 0, got {self.flush_workers}"
            )


@dataclass(frozen=True, slots=True)
class BatchAckConfig:
    """Batch ack configuration. Active."""

    batch_size: int = 100
    flush_interval_ms: int = 200


# ── Worker (active) ─────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Consumer concurrency configuration.

    Accepted by broker.start(), NOT part of RabbitConfig. Added in 0.2.0.

    ``stop_timeout`` (H12): the drain deadline given to a multi-worker pool's
    ``stop()`` — it must exceed your slowest handler's expected run time, and
    should be a few seconds *less* than ``terminationGracePeriodSeconds`` (k8s)
    so the graceful drain always has a chance to finish before SIGKILL. A
    handler still running past this deadline is **abandoned, not killed**:
    the sync pool's daemon thread keeps running in the background (it is
    never forcibly stopped — Python cannot interrupt an arbitrary thread),
    and the async pool cancels the task (which does not guarantee the
    handler reaches its own ack/nack — ``CancelledError`` is a
    ``BaseException`` and is not caught by the pipeline's exception
    handling). Either way the abandoned delivery is logged by delivery
    tag/message id, and — for the async pool — nacked for redelivery
    immediately rather than relying on the implicit requeue that happens
    when the connection eventually closes. Because the original handler may
    still complete its side effects after abandonment, **handlers must be
    idempotent under at-least-once delivery** regardless of ``stop_timeout``.
    """

    worker_count: int = 1
    prefetch_per_worker: int | None = None
    stop_timeout: float = 30.0


# ── Top-Level Config ─────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class RabbitConfig:
    """Top-level config — composes focused config objects.

    Frozen + slots (per project convention). Brokers that need to apply
    per-route overrides (e.g. prefetch) hold a private ``dataclasses.replace``
    copy rather than mutating the caller's object.

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
