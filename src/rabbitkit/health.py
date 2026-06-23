"""Health check utilities for rabbitkit brokers.

Provides callables suitable for use with obskit's health check system
or any monitoring framework.

Usage::

    from rabbitkit.health import broker_health_check, BrokerStatus

    # Standalone
    status = broker_health_check(broker)
    print(status.status, status.details)

    # With obskit health router
    from obskit.health import build_health_router, HealthCheck
    router = build_health_router(
        checks=[HealthCheck(name="rabbitmq", check=broker_health_check(broker))]
    )
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from typing import Any

from rabbitkit.core.config import HealthCheckConfig

logger = logging.getLogger(__name__)


class HealthStatus(str, enum.Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True, slots=True)
class BrokerHealthResult:
    """Result of a broker health check."""

    status: HealthStatus
    started: bool = False
    connected: bool = False
    consumer_count: int = 0
    route_count: int = 0
    worker_pool_pending: int = 0
    details: dict[str, Any] = field(default_factory=dict)


def broker_health_check(
    broker: Any,
    config: HealthCheckConfig | None = None,
) -> BrokerHealthResult:
    """Check broker health status (sync).

    Returns:
        BrokerHealthResult with status:
        - HEALTHY: started, connected, all consumers active
        - DEGRADED: started but some consumers missing or pool backlog high
        - UNHEALTHY: not started or not connected
    """
    cfg = config or HealthCheckConfig()
    # Not started
    started = getattr(broker, "_started", False)
    if not started:
        return BrokerHealthResult(
            status=HealthStatus.UNHEALTHY,
            started=False,
            details={"reason": "broker not started"},
        )

    # Check transport connectivity
    transport = getattr(broker, "_transport", None)
    connected = False
    if transport is not None:
        connected = bool(getattr(transport, "is_connected", lambda: False)())

    if not connected:
        return BrokerHealthResult(
            status=HealthStatus.UNHEALTHY,
            started=True,
            connected=False,
            details={"reason": "transport not connected"},
        )

    # Check routes and consumers
    routes = getattr(broker, "routes", [])
    route_count = len(routes)
    consumer_count = sum(1 for r in routes if getattr(r, "consumer_tag", None))

    # Check worker pool
    pool = getattr(broker, "_worker_pool", None)
    worker_pool_pending = 0
    if pool is not None:
        worker_pool_pending = getattr(pool, "pending_count", 0)

    # Determine status
    if consumer_count < route_count:
        status = HealthStatus.DEGRADED
        details: dict[str, Any] = {"reason": f"only {consumer_count}/{route_count} consumers active"}
    elif worker_pool_pending > cfg.pending_threshold:
        status = HealthStatus.DEGRADED
        details = {"reason": f"worker pool backlog: {worker_pool_pending}"}
    else:
        status = HealthStatus.HEALTHY
        details = {}

    return BrokerHealthResult(
        status=status,
        started=True,
        connected=connected,
        consumer_count=consumer_count,
        route_count=route_count,
        worker_pool_pending=worker_pool_pending,
        details=details,
    )


async def broker_health_check_async(
    broker: Any,
    config: HealthCheckConfig | None = None,
) -> BrokerHealthResult:
    """Async variant of broker_health_check.

    Same logic as sync -- transport.is_connected() is always sync.
    Provided for consistency with async health check frameworks.
    """
    return broker_health_check(broker, config=config)
