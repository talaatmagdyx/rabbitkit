"""Focused configuration objects — composable, immutable, validated.

Split into focused dataclasses. Each has clear responsibility.
RabbitConfig only composes connection/broker defaults.
Throughput/batching config objects are accepted by their respective components.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
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
    # M13: credential rotation. When set, called at each (re)connect to fetch
    # fresh (username, password) — e.g. from Vault/short-lived secrets — so a
    # rotated credential is picked up on the next reconnect WITHOUT a redeploy
    # (the frozen username/password fields are the static fallback). Excluded
    # from the AMQP URL/repr paths; only the connection builders call it.
    credentials_provider: Callable[[], tuple[str, str]] | None = None
    # M9: additional cluster nodes for failover, each "host" or "host:port".
    # The primary (host/port above) is tried first, then each node in order.
    # All share the same credentials/vhost/TLS/heartbeat. Sync (pika) tries
    # them natively via a ConnectionParameters list; async cycles endpoints on
    # initial connect (connect_robust then pins to the chosen node — put a
    # load balancer / DNS round-robin in front for per-reconnect failover).
    nodes: tuple[str, ...] = ()

    def resolve_credentials(self) -> tuple[str, str]:
        """Return ``(username, password)`` — from ``credentials_provider`` if
        set (M13: called at each (re)connect so rotated secrets are picked up
        without a redeploy), else the static ``username``/``password`` fields."""
        if self.credentials_provider is not None:
            return self.credentials_provider()
        return self.username, self.password

    def cluster_endpoints(self) -> list[tuple[str, int]]:
        """Primary host:port plus any failover nodes, in connect-attempt order."""
        endpoints = [(self.host, self.port)]
        for node in self.nodes:
            if ":" in node:
                host, _, port = node.partition(":")
                endpoints.append((host, int(port)))
            else:
                endpoints.append((node, self.port))
        return endpoints

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
        # M9: fail fast on a malformed node entry rather than at connect time.
        for node in self.nodes:
            _, sep, port = node.partition(":")
            if sep and not port.isdigit():
                raise ValueError(
                    f"ConnectionConfig.nodes entry {node!r} has a non-numeric port; "
                    "use 'host' or 'host:port'."
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

        # M3: an amqps:// URL implies TLS, but rabbitkit enables TLS via
        # SecurityConfig(ssl=SSLConfig(enabled=True)), NOT the URL scheme —
        # ConnectionConfig carries no TLS state. Silently ignoring the scheme
        # would let an operator ship PLAINTEXT while believing TLS is on. We
        # default the port to the AMQPS port (5671) and warn loudly, so a
        # not-actually-encrypted connection fails fast against a TLS-only port
        # instead of leaking plaintext to a plaintext listener.
        is_amqps = parsed.scheme == "amqps"
        default_port = 5671 if is_amqps else 5672
        if is_amqps:
            import warnings

            warnings.warn(
                "amqps:// URL parsed, but rabbitkit does not enable TLS from the URL "
                "scheme — you must pass SecurityConfig(ssl=SSLConfig(enabled=True)) to "
                "RabbitConfig. Port defaulted to 5671; without TLS enabled this "
                "connection will fail against a TLS-only listener (not silently send "
                "plaintext).",
                RuntimeWarning,
                stacklevel=2,
            )

        kwargs: dict[str, str | int | float | None] = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or default_port,
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

    **Sync-only.** ``SyncBroker`` applies these via pika's ``tcp_options``;
    ``AsyncBroker`` emits a ``RuntimeWarning`` and ignores a non-default
    value — aio-pika/aiormq exposes no socket-tuning hooks, and per-socket
    tuning would be silently lost on every automatic reconnect. Tune the
    async side via ``ConnectionConfig`` (heartbeat, timeouts) or at the
    OS level.
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
    """SASL + authentication configuration.

    Only ``mechanism="PLAIN"`` is implemented (username/password). ``EXTERNAL``
    (x509 client-cert auth) is not wired into either transport, so accepting
    it silently would be "config that lies" — it is rejected at construction
    (M2). mTLS is still supported for transport *encryption* via
    ``SSLConfig(certfile=..., keyfile=...)``; it just isn't an auth mechanism.
    """

    mechanism: str = "PLAIN"
    ssl: SSLConfig = field(default_factory=SSLConfig)

    def __post_init__(self) -> None:
        if self.mechanism != "PLAIN":
            raise ValueError(
                f"SecurityConfig.mechanism={self.mechanism!r} is not supported — only "
                "'PLAIN' (username/password) is implemented. SASL EXTERNAL/x509-auth is "
                "not wired into the transports. For TLS client certs (encryption, not "
                "auth), use SSLConfig(certfile=..., keyfile=...)."
            )


# ── Publisher ──────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PublisherConfig:
    """Publisher behavior tuning."""

    exchange: str = ""
    confirm_delivery: bool = True
    confirm_timeout: float = 5.0
    mandatory: bool = False
    persistent: bool = True
    # M10: reject oversized message bodies at publish time (bytes),
    # enforced by broker.publish() → ValueError. Default mirrors RabbitMQ's
    # own server-side `max_message_size` default (16 MiB): the server would
    # reject a larger message anyway — but with a channel exception that
    # kills the (possibly pooled) publisher channel, corrupting sibling
    # in-flight publishes. Neither AMQP connection negotiation nor the
    # management API exposes the server's actual limit, so it cannot be
    # discovered at connect time — if you raised `max_message_size` in
    # rabbitmq.conf, raise this to match; 0 disables the client-side guard
    # entirely. Large messages are a RabbitMQ anti-pattern (memory pressure,
    # head-of-line blocking, slow recovery) — consider a tighter cap
    # (e.g. 1_048_576 for 1 MiB) and storing payloads externally.
    max_message_bytes: int = 16_777_216


# ── Consumer ───────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class ConsumerConfig:
    """Consumer behavior tuning."""

    prefetch_count: int = 10
    graceful_timeout: float = 30.0
    # M6: bound the transient hot-loop on retry-less AUTO routes. By default
    # (False), a transient error nack-requeues with no cap — legitimate
    # "wait for the downstream to recover" behavior, but a footgun when the
    # failure is really permanent. When True, a transient error on a message
    # the broker has ALREADY redelivered (redelivered=True) is rejected to the
    # DLQ instead of requeued again — a 2-strike cap using the broker's
    # redelivered flag (the only per-message redelivery signal available
    # without republishing; classic-queue requeues can't carry a count). For
    # a higher cap or delays, use retry (delay ladder) or a quorum source
    # queue with x-delivery-limit. Requires a dead-letter path — the default
    # reject_without_dlx="auto_provision" (C3) gives AUTO routes one.
    reject_transient_on_redelivery: bool = False


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
    # RESERVED / no-op (M2): queue-based retry uses a fixed per-queue
    # x-message-ttl, so jitter would require per-message TTL — which
    # reintroduces head-of-line blocking on classic queues. Kept for API
    # stability; does not affect delay timing. Spread retries across a fleet
    # via the per-process reconnect jitter, not this.
    jitter_factor: float = 0.1
    # F4: "off" (default — single delay queue per tier, exact legacy topology)
    # or "sharded" — each tier becomes jitter_shards sub-queues with uniform
    # TTLs staggered across ±jitter_factor; a message picks its shard by a
    # STABLE hash of its message_id, decorrelating retry waves across the
    # fleet WITHOUT per-message TTL (which would reintroduce classic-queue
    # head-of-line blocking). Shard 0 keeps the legacy queue name/TTL, so
    # enabling this on an existing topology is additive (no 406s).
    jitter_mode: str = "off"
    jitter_shards: int = 3
    dead_letter_exchange: str = ""
    per_queue: bool = True
    unknown_policy: ErrorSeverity = ErrorSeverity.PERMANENT
    strict_delays: bool = True

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError(f"RetryConfig.max_retries must be >= 0, got {self.max_retries}")
        if self.jitter_mode not in ("off", "sharded"):
            raise ValueError(
                f"RetryConfig.jitter_mode must be 'off' or 'sharded', got {self.jitter_mode!r}"
            )
        if self.jitter_mode == "sharded":
            if self.jitter_shards < 2:
                raise ValueError(
                    f"RetryConfig.jitter_shards must be >= 2 with jitter_mode='sharded', "
                    f"got {self.jitter_shards}"
                )
            if not (0 < self.jitter_factor < 1):
                raise ValueError(
                    "RetryConfig.jitter_factor must be in (0, 1) with jitter_mode='sharded' "
                    f"(it sets the TTL spread), got {self.jitter_factor}"
                )
        if not self.per_queue:
            # H3: shared delay queues (rabbitkit.retry.N) bake a single
            # x-dead-letter-routing-key into each queue at declare time. That
            # key can only point at ONE source queue, so with >1 subscriber
            # they either 406 at startup (conflicting args) or silently
            # dead-letter every queue's failures back to whichever queue
            # declared first — cross-queue misdelivery (orders' failures
            # reappearing on payments). A shared delay queue physically
            # cannot route each message back to its own varying source with
            # static broker config, so there is no safe shared topology.
            raise ValueError(
                "RetryConfig(per_queue=False) is unsafe and unsupported: shared "
                "delay queues misroute failed messages across source queues (or "
                "406 at startup). Use per_queue=True (the default), which gives "
                "each queue isolated '<queue>.retry.N'/'<queue>.dlq' topology."
            )
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
    def messages_redelivered_total(self) -> str:
        """Incremented (labeled by ``queue``) for every consumed message the
        broker flagged ``redelivered=True`` — the broker-redelivery-rate
        signal. A sustained rise means handlers are dying/timing out before
        acking (crash loops, heartbeat kills, connection churn), which the
        success/error consume counters alone can't distinguish from ordinary
        traffic."""
        return f"{self.namespace}_messages_redelivered_total"

    @property
    def reconnects_total(self) -> str:
        """Incremented on every transport re-connection after the first
        successful connect — the connection-churn signal. Reconnects were
        previously logged but never counted, so a flapping broker/network
        was invisible to metrics-based alerting."""
        return f"{self.namespace}_reconnects_total"

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

    # ── Broker-side gauges (H5: polled from the management API) ──
    # Bridged by QueueMetricsPoller — the #1 RabbitMQ incident signal
    # (queue growth / consumer lag) that the consume/publish counters cannot
    # see. All labeled by {queue}.

    @property
    def queue_messages_ready(self) -> str:
        """Messages ready for delivery (backlog depth)."""
        return f"{self.namespace}_queue_messages_ready"

    @property
    def queue_messages_unacked(self) -> str:
        """Messages delivered but not yet acked (in-flight at consumers)."""
        return f"{self.namespace}_queue_messages_unacked"

    @property
    def queue_messages_total(self) -> str:
        """Total messages in the queue (ready + unacked)."""
        return f"{self.namespace}_queue_messages_total"

    @property
    def queue_consumers(self) -> str:
        """Number of consumers attached to the queue (0 = nothing draining)."""
        return f"{self.namespace}_queue_consumers"


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
    """Deduplication configuration. Active.

    ``mark_policy`` accepts a :class:`~rabbitkit.core.types.DeduplicationMarkPolicy`
    member or its string value:

    - ``"on_success"`` (default) — crash-safe; concurrent duplicates may both run.
    - ``"on_start"`` — blocks concurrent duplicates but a crash mid-handler
      LOSES the message. Advanced/dangerous.
    - ``"claim"`` — in-flight claim (``processing_timeout``) before the
      handler, "completed" (``ttl``) after success. Blocks concurrent
      duplicates and survives crashes; ``processing_timeout`` must
      comfortably exceed the worst-case handler duration or a duplicate can
      start while the original is still running.
    """

    key_prefix: str = "rabbitkit:dedup"
    ttl: int = 86400
    fallback_on_redis_error: bool = True
    key_source: str = "message_id"
    mark_policy: str = "on_success"
    local_cache_size: int = 0  # 0 = disabled; >0 = in-process LRU capacity (short-circuits Redis for known duplicates)
    processing_timeout: int = 300  # claim only: in-flight claim TTL (seconds)
    # F5 (idempotent receiver): with mark_policy="claim", store the handler's
    # JSON-serializable result alongside the completed mark; a duplicate
    # delivery then REPLAYS the stored result (the pipeline re-publishes it to
    # the route's result publisher / reply_to, byte-identical) instead of just
    # skipping. Results that aren't JSON-serializable or exceed
    # max_result_bytes degrade gracefully to plain skip-without-replay.
    # This is the idempotent-receiver EFFECT — wire-level exactly-once does
    # not exist on RabbitMQ and this does not claim otherwise.
    store_results: bool = False
    max_result_bytes: int = 65536
    on_in_flight: str = "nack_requeue"  # claim only: "nack_requeue" (retry-safe) | "ack_skip"

    def __post_init__(self) -> None:
        if self.store_results and self.mark_policy != "claim":
            raise ValueError(
                "DeduplicationConfig.store_results=True requires mark_policy='claim' "
                f"(got {self.mark_policy!r}) — result replay is only crash-safe on the "
                "claim state machine."
            )
        if self.max_result_bytes < 1:
            raise ValueError(
                f"DeduplicationConfig.max_result_bytes must be >= 1, got {self.max_result_bytes}"
            )
        if self.mark_policy not in ("on_success", "on_start", "claim"):
            raise ValueError(
                f"DeduplicationConfig.mark_policy must be one of "
                f"'on_success', 'on_start', 'claim'; got {self.mark_policy!r}"
            )
        if self.on_in_flight not in ("nack_requeue", "ack_skip"):
            raise ValueError(
                f"DeduplicationConfig.on_in_flight must be 'nack_requeue' or "
                f"'ack_skip'; got {self.on_in_flight!r}"
            )
        if self.processing_timeout <= 0:
            raise ValueError(
                f"DeduplicationConfig.processing_timeout must be > 0, got {self.processing_timeout}"
            )


# ── Safety (active) ─────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SafetyConfig:
    """Message-safety policies. Active.

    ``reject_without_dlx`` — what to do when a route can
    ``reject(requeue=False)`` but its queue has no dead-letter exchange
    (RabbitMQ silently discards such rejects). Accepts a
    :class:`~rabbitkit.core.types.RejectWithoutDLXPolicy` member or its
    string value:

    - ``"auto_provision"`` (default): declare ``{queue}{dlq_suffix}`` and
      wire the source queue's DLX to it — poison messages are preserved.
    - ``"error"``: fail startup with ``UnsafeTopologyError``. For teams that
      manage topology externally.
    - ``"discard"``: explicitly allow RabbitMQ to discard rejected messages
      (warns once per route unless ``warn_on_discard=False``).

    Only applied under ``TopologyMode.AUTO_DECLARE``; per-route override via
    ``@subscriber(reject_without_dlx=...)``.
    """

    reject_without_dlx: str = "auto_provision"
    dlq_suffix: str = ".dlq"
    warn_on_discard: bool = True
    # M14: what to do when declaring a queue/exchange 406s because it already
    # exists with incompatible arguments (drift — e.g. ops created it, or a
    # prior rabbitkit version, with a different type/TTL/DLX).
    # - "raise" (default): fail startup with a typed ConfigurationError. Safe
    #   default — surfaces the drift instead of silently ignoring your config.
    # - "warn_continue": log a warning and CONTINUE using the EXISTING
    #   definition (rabbitkit's declaration is NOT applied). Unlike
    #   TopologyMode.PASSIVE_ONLY (which skips declaration for EVERY queue),
    #   this still actively declares non-conflicting queues and only tolerates
    #   the ones that drifted — the per-conflict warn-and-continue mode.
    on_topology_conflict: str = "raise"

    def __post_init__(self) -> None:
        if self.reject_without_dlx not in ("auto_provision", "error", "discard"):
            raise ValueError(
                f"SafetyConfig.reject_without_dlx must be one of "
                f"'auto_provision', 'error', 'discard'; got {self.reject_without_dlx!r}"
            )
        if not self.dlq_suffix:
            raise ValueError("SafetyConfig.dlq_suffix must be non-empty")
        if self.on_topology_conflict not in ("raise", "warn_continue"):
            raise ValueError(
                f"SafetyConfig.on_topology_conflict must be 'raise' or 'warn_continue'; "
                f"got {self.on_topology_conflict!r}"
            )


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
    # M11: bound the sync worker pool's internal work queue. 0 = unbounded
    # (default, unchanged). In practice the broker's prefetch already caps
    # in-flight messages (effective prefetch = worker_count x
    # prefetch_per_worker), so this is a defensive ceiling — set it >= your
    # effective prefetch so it only trips if prefetch isn't being honored.
    # When the queue is full, submit() blocks (backpressure); keep it above
    # prefetch so that never happens on the I/O thread.
    max_queue_size: int = 0

    def __post_init__(self) -> None:
        if self.worker_count < 1:
            raise ValueError(f"WorkerConfig.worker_count must be >= 1, got {self.worker_count}")
        if self.max_queue_size < 0:
            raise ValueError(f"WorkerConfig.max_queue_size must be >= 0, got {self.max_queue_size}")


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
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    retry: RetryConfig | None = None
    compression: CompressionConfig | None = None
    logging: LoggingConfig | None = None
