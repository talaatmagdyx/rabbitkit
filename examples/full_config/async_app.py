"""Fully-configured ASYNC rabbitkit app — every config knob + the full middleware stack.

A REFERENCE wiring: every RabbitConfig sub-config and every consume-side middleware,
with honest notes on what each needs at runtime. Run against a real broker:

    docker run -d --rm -p 5672:5672 rabbitmq:3.13-alpine
    python examples/full_config/async_app.py

NOTE: this module deliberately does NOT use `from __future__ import annotations` —
the pipeline reads the body parameter's annotation via inspect.signature, so it
must be a real type (not a stringized one) for Pydantic decoding to work.
"""

import asyncio
from datetime import datetime
from typing import Annotated

import redis.asyncio as aioredis
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
from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.types import AckPolicy, ErrorSeverity, TopologyMode
from rabbitkit.di.depends import Depends
from rabbitkit.di.resolver import DIResolver
from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
from rabbitkit.middleware.exception import ExceptionMiddleware
from rabbitkit.middleware.otel import OTelTracingMiddleware
from rabbitkit.middleware.rate_limit import RateLimitMiddleware
from rabbitkit.middleware.retry import RetryMiddleware
from rabbitkit.middleware.timeout import TimeoutConfig, TimeoutMiddleware
from rabbitkit.serialization.pipeline import JsonParser, PydanticDecoder, SerializationPipeline

# ── 1. FULL CONFIG — every sub-config, every field ───────────────────────────
CONFIG = RabbitConfig(
    connection=ConnectionConfig(
        host="localhost",
        port=5672,
        vhost="/",
        username="guest",
        password="guest",
        heartbeat=30,  # detect a dead peer in ~2 missed beats
        socket_timeout=10.0,  # TCP connect/op timeout — fail fast
        blocked_connection_timeout=60.0,  # give up if the broker holds us blocked (mem/disk alarm)
        reconnect_backoff_base=1.0,
        reconnect_backoff_max=30.0,  # exponential reconnect backoff
        connection_name="full-config-async@dev",  # shows in the mgmt UI — priceless in incidents
    ),
    socket=SocketConfig(
        tcp_nodelay=True, tcp_keepidle=10, tcp_keepintvl=5, tcp_keepcnt=3, tcp_sndbuf=196608, tcp_rcvbuf=196608
    ),
    security=SecurityConfig(
        mechanism="PLAIN",
        ssl=SSLConfig(
            enabled=False,  # set True + certs for AMQPS (port 5671) in prod
            ca_certs=None,
            certfile=None,
            keyfile=None,
            cert_reqs="CERT_REQUIRED",
            server_hostname=None,
        ),
    ),
    publisher=PublisherConfig(confirm_delivery=True, confirm_timeout=5.0, persistent=True, mandatory=False),
    consumer=ConsumerConfig(prefetch_count=64, graceful_timeout=30.0),  # prefetch = async concurrency
    pool=PoolConfig(
        channel_pool_size=64, publisher_connections=1, consumer_connections=1, channel_acquire_timeout=10.0
    ),
    retry=RetryConfig(
        max_retries=4,
        delays=(5, 30, 120, 600),
        jitter_factor=0.1,
        per_queue=True,
        unknown_policy=ErrorSeverity.PERMANENT,
    ),
    compression=CompressionConfig(algorithm="zstd", threshold=2048, level=6),
    logging=LoggingConfig(render_json=True, add_log_level=True, timestamper_fmt="iso"),
    topology_mode=TopologyMode.AUTO_DECLARE,  # use PASSIVE_ONLY in production
)


# ── 2. Model + service (DI factory MUST be module-level) ─────────────────────
class OrderCreated(BaseModel):
    order_id: str = Field(min_length=1)
    amount_cents: int = Field(ge=0)
    created_at: datetime


class OrderService:
    def handle(self, event: OrderCreated) -> None:
        print(f"[handler] processed order {event.order_id} ({event.amount_cents}c)")


def get_service() -> OrderService:
    return OrderService()


# ── 3. Broker + Pydantic serializer + DI ─────────────────────────────────────
broker = AsyncBroker(
    CONFIG,
    serializer=SerializationPipeline(JsonParser(), PydanticDecoder()),
    di_resolver=DIResolver(),
)
redis = aioredis.from_url("redis://localhost:6379/0")  # constructed lazily; connects on first use


# ── 4. Full consume-side middleware stack (OUTER → INNER) ────────────────────
MIDDLEWARES = [
    OTelTracingMiddleware(service_name="full-config-async"),  # outermost: span covers retries
    ExceptionMiddleware(swallow_permanent=False),  # let terminal errors reach the DLQ
    CircuitBreakerMiddleware(async_circuit_breaker=None),  # pass an obskit CircuitBreaker here
    DeduplicationMiddleware(
        redis,
        DeduplicationConfig(  # Redis fast-path idempotency
            key_prefix="full:dedup", ttl=86400, key_source="message_id", fallback_on_redis_error=True
        ),
    ),  # Redis down → process anyway (DB is truth)
    RetryMiddleware(CONFIG.retry, publish_async_fn=broker.publish),  # REQUIRED: wire publish_async_fn
    TimeoutMiddleware(TimeoutConfig(timeout_seconds=15.0)),  # → HandlerTimeoutError (transient)
    RateLimitMiddleware(RateLimitConfig(max_rate=5000, burst=500, on_limited="wait")),
]


# ── 5. Consumer ──────────────────────────────────────────────────────────────
@broker.subscriber(
    queue="fullcfg-async.orders",
    exchange="fullcfg-async.exchange",
    routing_key="orders.created",
    ack_policy=AckPolicy.NACK_ON_ERROR,  # terminal → nack(requeue=False) → DLQ (NOT AUTO's requeue=True)
    retry=CONFIG.retry,  # declares the delay-queue + DLQ topology
    middlewares=MIDDLEWARES,  # RetryMiddleware here is what actually retries
)
async def handle(event: OrderCreated, svc: Annotated[OrderService, Depends(get_service)]) -> None:
    svc.handle(event)


# ── 6. Demo: confirmed publish + drain ───────────────────────────────────────
async def main() -> None:
    await broker.start(worker_config=WorkerConfig(worker_count=1))  # async: prefetch drives concurrency
    outcome = await broker.publish(
        MessageEnvelope(
            exchange="fullcfg-async.exchange",
            routing_key="orders.created",
            body=b'{"order_id":"o1","amount_cents":100,"created_at":"2026-01-01T00:00:00Z"}',
        )
    )
    print(f"[publisher] {outcome.status.value}")
    await asyncio.sleep(1.0)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
