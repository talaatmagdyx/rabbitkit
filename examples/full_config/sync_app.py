"""Fully-configured SYNC rabbitkit app — every config knob + the full middleware stack.

The sync (pika) twin of async_app.py. Same config + middleware; the differences are
SyncBroker, a sync handler, RetryMiddleware wired with `publish_fn` (not async), and
a sync Redis client. Run against a real broker:

    docker run -d --rm -p 5672:5672 rabbitmq:3.13-alpine
    python examples/full_config/sync_app.py

NOTE: no `from __future__ import annotations` (real annotations required for Pydantic
body decoding — see async_app.py).
"""

import time
from datetime import datetime
from typing import Annotated

import redis
from pydantic import BaseModel, Field

from rabbitkit import (
    CompressionConfig,
    ConnectionConfig,
    ConsumerConfig,
    DeduplicationConfig,
    LoggingConfig,
    MessageEnvelope,
    PoolConfig,
    PublisherConfig,
    RabbitConfig,
    RateLimitConfig,
    RetryConfig,
    SecurityConfig,
    SocketConfig,
    SSLConfig,
    WorkerConfig,
)
from rabbitkit.core.types import AckPolicy, ErrorSeverity, TopologyMode
from rabbitkit.di.depends import Depends
from rabbitkit.di.resolver import DIResolver
from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
from rabbitkit.middleware.exception import ExceptionMiddleware
from rabbitkit.middleware.rate_limit import RateLimitMiddleware
from rabbitkit.middleware.retry import RetryMiddleware
from rabbitkit.middleware.timeout import TimeoutConfig, TimeoutMiddleware
from rabbitkit.middleware.tracing import TracedConsumerMiddleware
from rabbitkit.serialization.pipeline import JsonParser, PydanticDecoder, SerializationPipeline
from rabbitkit.sync.broker import SyncBroker

# ── 1. FULL CONFIG — identical to the async app ──────────────────────────────
CONFIG = RabbitConfig(
    connection=ConnectionConfig(
        host="localhost", port=5672, vhost="/", username="guest", password="guest",
        heartbeat=30, socket_timeout=10.0, blocked_connection_timeout=300.0,
        reconnect_backoff_base=1.0, reconnect_backoff_max=30.0,
        connection_name="full-config-sync@dev",
    ),
    socket=SocketConfig(tcp_nodelay=True, tcp_keepidle=10, tcp_keepintvl=5,
                        tcp_keepcnt=3, tcp_sndbuf=196608, tcp_rcvbuf=196608),
    security=SecurityConfig(
        mechanism="PLAIN",
        ssl=SSLConfig(enabled=False, ca_certs=None, certfile=None, keyfile=None,
                      cert_reqs="CERT_REQUIRED", server_hostname=None),
    ),
    publisher=PublisherConfig(confirm_delivery=True, confirm_timeout=5.0,
                              persistent=True, mandatory=False),
    consumer=ConsumerConfig(prefetch_count=64, graceful_timeout=30.0),
    pool=PoolConfig(channel_pool_size=64, publisher_connections=1,
                    consumer_connections=1, channel_acquire_timeout=10.0),
    retry=RetryConfig(max_retries=4, delays=(5, 30, 120, 600), jitter_factor=0.1,
                      per_queue=True, unknown_policy=ErrorSeverity.PERMANENT),
    compression=CompressionConfig(algorithm="zstd", threshold=2048, level=6),
    logging=LoggingConfig(render_json=True, add_log_level=True, timestamper_fmt="iso"),
    topology_mode=TopologyMode.AUTO_DECLARE,   # PASSIVE_ONLY in production
)


# ── 2. Model + service ───────────────────────────────────────────────────────
class OrderCreated(BaseModel):
    order_id: str = Field(min_length=1)
    amount_cents: int = Field(ge=0)
    created_at: datetime


class OrderService:
    def handle(self, event: OrderCreated) -> None:
        print(f"[handler] processed order {event.order_id} ({event.amount_cents}c)")


def get_service() -> OrderService:
    return OrderService()


# ── 3. Broker + serializer + DI ──────────────────────────────────────────────
broker = SyncBroker(
    CONFIG,
    serializer=SerializationPipeline(JsonParser(), PydanticDecoder()),
    di_resolver=DIResolver(),
)
redis_client = redis.from_url("redis://localhost:6379/0")   # lazy; connects on first use


# ── 4. Full consume-side middleware stack (sync variants) ────────────────────
MIDDLEWARES = [
    TracedConsumerMiddleware(service_name="full-config-sync"),
    ExceptionMiddleware(swallow_permanent=False),
    CircuitBreakerMiddleware(circuit_breaker=None),                 # pass an obskit CircuitBreaker here
    DeduplicationMiddleware(redis_client, DeduplicationConfig(
        key_prefix="full:dedup", ttl=86400, key_source="message_id",
        fallback_on_redis_error=True)),
    RetryMiddleware(CONFIG.retry, publish_fn=broker.publish),       # sync: publish_fn (not async)
    TimeoutMiddleware(TimeoutConfig(timeout_seconds=15.0)),         # NOTE: sync timeout can't kill a
                                                                   # running handler thread — it abandons it
    RateLimitMiddleware(RateLimitConfig(max_rate=5000, burst=500, on_limited="wait")),
]


# ── 5. Consumer ──────────────────────────────────────────────────────────────
@broker.subscriber(
    queue="orders.queue", exchange="orders.exchange", routing_key="orders.created",
    ack_policy=AckPolicy.NACK_ON_ERROR,
    retry=CONFIG.retry,
    middlewares=MIDDLEWARES,
)
def handle(event: OrderCreated, svc: Annotated[OrderService, Depends(get_service)]) -> None:
    svc.handle(event)


# ── 6. Demo: confirmed publish + drain (smoke; for a long-running consumer use broker.run()) ──
def main() -> None:
    broker.start(worker_config=WorkerConfig(worker_count=1))   # wc=1: handler inline, fastest for light work
    outcome = broker.publish(MessageEnvelope(
        exchange="orders.exchange", routing_key="orders.created",
        body=b'{"order_id":"o1","amount_cents":100,"created_at":"2026-01-01T00:00:00Z"}'))
    print(f"[publisher] {outcome.status.value}")
    conn = broker._transport._connection
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        conn.process_data_events(time_limit=0.2)
    broker.stop()


if __name__ == "__main__":
    main()
