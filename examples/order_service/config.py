"""Environment-aware RabbitConfig (docs §5). No connection happens here — the
broker connects lazily on start()."""

from __future__ import annotations

from rabbitkit import (
    ConnectionConfig,
    ConsumerConfig,
    PoolConfig,
    PublisherConfig,
    RabbitConfig,
    RetryConfig,
)
from rabbitkit.core.types import ErrorSeverity, TopologyMode

# Matches the rabbitkit default policy; spelled out for clarity.
ORDERS_RETRY = RetryConfig(
    max_retries=4,
    delays=(5, 30, 120, 600),
    jitter_factor=0.1,
    per_queue=True,
    unknown_policy=ErrorSeverity.PERMANENT,
)


def build_config(env: str) -> RabbitConfig:
    return RabbitConfig(
        connection=ConnectionConfig(
            host="rabbitmq.internal" if env == "production" else "localhost",
            vhost="/orders",
            heartbeat=30,
            socket_timeout=10.0,
            blocked_connection_timeout=60.0,
            reconnect_backoff_base=1.0,
            reconnect_backoff_max=30.0,
            connection_name=f"order-service@{env}",
        ),
        publisher=PublisherConfig(confirm_delivery=True, persistent=True, confirm_timeout=5.0),
        consumer=ConsumerConfig(prefetch_count=20, graceful_timeout=30.0),
        pool=PoolConfig(channel_pool_size=20, channel_acquire_timeout=10.0),
        retry=ORDERS_RETRY,
        # Application code should not mutate production topology (docs §5/§34).
        topology_mode=(TopologyMode.AUTO_DECLARE if env in ("dev", "staging") else TopologyMode.PASSIVE_ONLY),
    )
