"""Configuration: Full RabbitConfig composition.

Shows every configuration knob available in RabbitConfig.
All config objects are immutable frozen dataclasses.

Run:
    python examples/config/01_rabbit_config.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

from rabbitkit import (
    CompressionConfig,
    ConnectionConfig,
    ConsumerConfig,
    PoolConfig,
    PublisherConfig,
    RabbitConfig,
    RetryConfig,
    TopologyMode,
)
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.config import (
    SecurityConfig,
    SocketConfig,
    SSLConfig,
    WorkerConfig,
)

# ── Minimal config (defaults) ─────────────────────────────────────────────────
minimal = RabbitConfig()
print(f"Minimal: {minimal.connection.host}:{minimal.connection.port}")


# ── Full production config ────────────────────────────────────────────────────
production_config = RabbitConfig(
    # Core connection
    connection=ConnectionConfig(
        host="rabbitmq.prod.internal",
        port=5672,
        username="myapp",
        password="super-secret",
        vhost="/production",
        heartbeat=30,
        socket_timeout=10.0,
        blocked_connection_timeout=60.0,
        connection_name="order-service",
        reconnect_backoff_base=1.0,  # 1s initial backoff
        reconnect_backoff_max=30.0,  # 30s max backoff
    ),
    # TCP tuning
    socket=SocketConfig(
        tcp_nodelay=True,
        tcp_keepidle=10,
        tcp_keepintvl=5,
        tcp_keepcnt=3,
        tcp_sndbuf=196608,  # 192KB send buffer
        tcp_rcvbuf=196608,  # 192KB recv buffer
    ),
    # TLS
    security=SecurityConfig(
        mechanism="PLAIN",
        ssl=SSLConfig(
            enabled=True,
            certfile="/certs/client.pem",
            keyfile="/certs/client.key",
            ca_certs="/certs/ca.pem",
            cert_reqs="CERT_REQUIRED",
            server_hostname="rabbitmq.prod.internal",
        ),
    ),
    # Publishing
    publisher=PublisherConfig(
        confirm_delivery=True,  # publisher confirms (at-least-once)
        confirm_timeout=5.0,
        persistent=True,  # delivery_mode=2 (durable messages)
        mandatory=False,
    ),
    # Consuming
    consumer=ConsumerConfig(
        prefetch_count=20,  # fetch 20 messages ahead
        graceful_timeout=30.0,  # wait 30s for in-flight on shutdown
    ),
    # Channel pooling
    pool=PoolConfig(
        channel_pool_size=10,
        publisher_connections=1,
        consumer_connections=1,
    ),
    # Topology
    topology_mode=TopologyMode.AUTO_DECLARE,
    # Default retry for all subscribers
    retry=RetryConfig(
        max_retries=4,
        delays=(5, 30, 120, 600),
        jitter_factor=0.1,
        dead_letter_exchange="",
        per_queue=True,
    ),
    # Default compression for all messages
    compression=CompressionConfig(
        algorithm="gzip",
        threshold=1024,  # compress bodies >= 1KB
        level=6,
    ),
)


# ── Connection from URL ────────────────────────────────────────────────────────
url_config = RabbitConfig(
    connection=ConnectionConfig.from_url(
        "amqp://user:pass@rabbitmq.prod.internal:5672/production?heartbeat=60&connection_name=worker"
    )
)
print(f"From URL: host={url_config.connection.host}, vhost={url_config.connection.vhost!r}")


# ── Per-environment configs ───────────────────────────────────────────────────


def get_config(env: str) -> RabbitConfig:
    """Return environment-specific config."""
    base_conn = ConnectionConfig(
        host="localhost" if env == "dev" else f"rabbitmq.{env}.internal",
        username="guest" if env == "dev" else "myapp",
        password="guest" if env == "dev" else "from-secrets-manager",
    )

    retry = None if env == "dev" else RetryConfig(max_retries=3, delays=(5, 30, 120))

    return RabbitConfig(
        connection=base_conn,
        retry=retry,
        topology_mode=TopologyMode.AUTO_DECLARE,
    )


dev_config = get_config("dev")
stg_config = get_config("staging")
print(f"Dev:     {dev_config.connection.host}")
print(f"Staging: {stg_config.connection.host}")
print(f"Retry:   dev={dev_config.retry is None}, staging={stg_config.retry is not None}")


# ── WorkerConfig (separate from RabbitConfig) ─────────────────────────────────
# WorkerConfig is NOT part of RabbitConfig — passed to broker.start()
worker_config = WorkerConfig(
    worker_count=4,
    prefetch_per_worker=5,  # total prefetch = 4x5 = 20
)

broker = AsyncBroker(production_config)
# await broker.start(worker_config=worker_config)

print("\nConfiguration objects created successfully.")
print(f"Production: host={production_config.connection.host}")
print(f"  retry: max_retries={production_config.retry.max_retries if production_config.retry else 'none'}")
print(f"  compression: {production_config.compression.algorithm if production_config.compression else 'none'}")
