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
import time
import warnings
from dataclasses import dataclass, field
from typing import Any

from rabbitkit.core.config import HealthCheckConfig
from rabbitkit.core.protocols import HealthProvider

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


# ── Public-property / private-attr fallback helper ──────────────────────


class _Missing:
    """Sentinel for "attribute absent" (distinct from a real False/None)."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover — debug only
        return "<missing>"


_MISSING = _Missing()


def _get(broker: Any, public: str, private: str, default: Any = False) -> Any:
    """Try the public property first, then the private attr, then the default.

    This makes the transition from private attributes (``_started``) to typed
    properties (``started``) gradual: brokers can add the ``@property`` when
    ready, and health checks pick it up automatically. When only the private
    attribute exists, a ``DeprecationWarning`` is emitted once per process
    per (public, private) pair so callers know to migrate.
    """
    value = getattr(broker, public, _MISSING)
    if value is not _MISSING:
        return value
    value = getattr(broker, private, _MISSING)
    if value is not _MISSING:
        warnings.warn(
            f"Broker {type(broker).__name__} does not expose the typed "
            f"property {public!r}; falling back to private attribute "
            f"{private!r}. Implement {public!r} on the broker to silence this.",
            DeprecationWarning,
            stacklevel=3,
        )
        return value
    return default


def _get_started(broker: Any) -> bool:
    return bool(_get(broker, "started", "_started", False))


def _get_connected(broker: Any) -> bool:
    """Check transport connectivity via the typed property or private attr."""
    connected = _get(broker, "connected", "_connected", _MISSING)
    if connected is not _MISSING:
        return bool(connected)
    # Fallback: probe the private transport attribute directly.
    transport = getattr(broker, "_transport", None)
    if transport is not None:
        return bool(getattr(transport, "is_connected", lambda: False)())
    return False


def _get_transport(broker: Any) -> Any:
    """Return the transport object, trying public then private names."""
    transport = getattr(broker, "transport", _MISSING)
    if transport is not _MISSING:
        return transport
    return getattr(broker, "_transport", None)


def _get_consumer_count(broker: Any, transport: Any) -> int:
    """Count routes with an active consumer, cross-checked against transport liveness."""
    consumer_count = _get(broker, "consumer_count", "_consumer_count", _MISSING)
    if consumer_count is not _MISSING:
        return int(consumer_count)
    # Fallback: compute from routes list.
    routes = getattr(broker, "routes", [])
    computed: int = sum(1 for r in routes if getattr(r, "consumer_tag", None))
    # M-SRE3: cross-check consumer registration against live transport
    # connectivity. consumer_tag is set at registration time and may remain
    # set even after the channel dies, so a registered-but-disconnected
    # consumer must not be counted as active.
    if transport is not None and not _transport_consumers_alive(transport):
        computed = 0
    return computed


def _get_route_count(broker: Any) -> int:
    route_count = _get(broker, "route_count", "_route_count", _MISSING)
    if route_count is not _MISSING:
        return int(route_count)
    return len(getattr(broker, "routes", []))


def _get_worker_pool_pending(broker: Any) -> int:
    pending = _get(broker, "worker_pool_pending", "_worker_pool_pending", _MISSING)
    if pending is not _MISSING:
        return int(pending)
    # Fallback: probe the private worker pool attribute.
    pool = getattr(broker, "_worker_pool", None)
    if pool is not None:
        return int(getattr(pool, "pending_count", 0))
    return 0


def _get_last_heartbeat(broker: Any) -> float | None:
    hb = _get(broker, "last_heartbeat", "_last_heartbeat", None)
    if hb is None:
        return None
    return float(hb)


def broker_health_check(
    broker: HealthProvider | Any,
    config: HealthCheckConfig | None = None,
) -> BrokerHealthResult:
    """Check broker health status (sync).

    Args:
        broker: A broker implementing :class:`HealthProvider` (typed
            properties) or a legacy broker exposing private attributes
            (``_started``, ``_transport``, ``_worker_pool``).
        config: Optional :class:`HealthCheckConfig` to tune thresholds.

    Returns:
        BrokerHealthResult with status:
        - HEALTHY: started, connected, all consumers active
        - DEGRADED: started but some consumers missing or pool backlog high
        - UNHEALTHY: not started or not connected
    """
    cfg = config or HealthCheckConfig()

    # Not started
    started = _get_started(broker)
    if not started:
        return BrokerHealthResult(
            status=HealthStatus.UNHEALTHY,
            started=False,
            details={"reason": "broker not started"},
        )

    # Check transport connectivity
    transport = _get_transport(broker)
    connected = _get_connected(broker)
    if not connected:
        return BrokerHealthResult(
            status=HealthStatus.UNHEALTHY,
            started=True,
            connected=False,
            details={"reason": "transport not connected"},
        )

    # Check routes and consumers
    route_count = _get_route_count(broker)
    consumer_count = _get_consumer_count(broker, transport)

    # Check worker pool
    worker_pool_pending = _get_worker_pool_pending(broker)

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
    broker: HealthProvider | Any,
    config: HealthCheckConfig | None = None,
) -> BrokerHealthResult:
    """Async variant of broker_health_check.

    Same logic as sync -- transport.is_connected() is always sync.
    Provided for consistency with async health check frameworks.
    """
    return broker_health_check(broker, config=config)


# Transport contract (I-5): a transport MAY expose any of these optional
# attributes/properties to advertise live consumer state. Each is checked
# independently; if any present one reports False, registered consumer_tags
# are treated as stale. When NONE of these exist on the transport, we cannot
# prove the channels are dead, so we fall back to trusting the registered
# consumer_tag (current/backwards-compatible behaviour).
#
#   has_open_channels  -> bool | () -> bool   (e.g. SyncTransport exposes this)
#   is_consuming       -> bool | () -> bool
#   consumers_active   -> bool | () -> bool
_TRANSPORT_LIVENESS_ATTRS: tuple[str, ...] = (
    "has_open_channels",
    "is_consuming",
    "consumers_active",
)


def _transport_consumers_alive(transport: Any) -> bool:
    """Best-effort check that the transport still has live consumers.

    Probes the optional transport-contract attributes
    (:data:`_TRANSPORT_LIVENESS_ATTRS`). Each attribute may be a plain bool
    or a zero-arg callable returning bool. When any present attribute reports
    ``False``, the transport's consumer channels are considered dead and
    this returns ``False``.

    When NONE of the contract attributes exist on the transport, we cannot
    prove the channels are dead, so we trust the registered ``consumer_tag``
    (backwards-compatible behaviour — returns ``True``).
    """
    any_present = False
    for attr in _TRANSPORT_LIVENESS_ATTRS:
        # Use getattr-without-default via a sentinel so a transport that
        # genuinely sets the attribute to False is still detected.
        flag = getattr(transport, attr, _MISSING)
        if flag is _MISSING:
            continue
        any_present = True
        if callable(flag):
            try:
                value = flag()
            except Exception:
                logger.debug("transport %s check raised", attr, exc_info=True)
                continue
        else:
            value = flag
        if value is False:
            return False
    # No richer signal available — trust the registered consumer_tag.
    if not any_present:
        return True
    # At least one signal was present and none reported False.
    return True


def mark_heartbeat(broker: Any) -> None:
    """Record a liveness heartbeat on *broker*.

    Brokers/transports should call this from their consume callback / I/O loop
    (or any other "I made forward progress" signal) so :func:`broker_liveness`
    can detect a wedged broker whose process is alive but is no longer
    draining the network.  Sets ``broker.last_heartbeat`` to the current
    ``time.monotonic()`` value.

    Safe to call when the broker does not yet expose ``last_heartbeat`` — it
    simply sets the attribute.
    """
    broker.last_heartbeat = time.monotonic()


def broker_liveness(broker: HealthProvider | Any, wedged_timeout: float = 60.0) -> bool:
    """Liveness probe — is the broker process alive and not hard-wedged?

    Args:
        broker: A broker implementing :class:`HealthProvider` (typed
            properties) or a legacy broker exposing private attributes
            (``_started``, ``_wedged``, ``last_heartbeat``).
        wedged_timeout: Seconds without a heartbeat before liveness fails.

    Liveness fails when:

    - the broker is not started (``_started``/``started`` False/absent), OR
    - an explicit ``_wedged`` flag is set to ``True`` (transports/brokers may
      set this on a hard fault), OR
    - a ``last_heartbeat`` is present and ``now - last_heartbeat >
      wedged_timeout`` (a heartbeat was recorded via :func:`mark_heartbeat`
      but has gone stale, meaning the I/O loop is wedged).

    A transient broker/transport disconnect is *not* a liveness failure: the
    process is still running and can recover. Use :func:`broker_readiness` to
    decide whether to route traffic.

    When no ``last_heartbeat`` attribute exists, liveness falls back to the
    ``_started`` / ``_wedged`` checks only (backwards compatible).
    """
    started = _get_started(broker)
    if not started:
        return False
    if _get(broker, "wedged", "_wedged", False):
        return False
    last_heartbeat = _get_last_heartbeat(broker)
    if last_heartbeat is not None:
        if time.monotonic() - last_heartbeat > wedged_timeout:
            return False
    return True


def broker_readiness(broker: HealthProvider | Any, config: HealthCheckConfig | None = None) -> bool:
    """Readiness probe — is the broker ready to serve traffic right now?

    Args:
        broker: A broker implementing :class:`HealthProvider` (typed
            properties) or a legacy broker exposing private attributes.
        config: Optional :class:`HealthCheckConfig` to tune thresholds.

    Requires: health check not UNHEALTHY, transport connected, and every
    registered route has an active (live) consumer. Use this for
    load-balancer / ingress gating; use :func:`broker_liveness` for restart
    decisions.
    """
    result = broker_health_check(broker, config=config)
    if result.status == HealthStatus.UNHEALTHY:
        return False
    if not result.connected:
        return False
    # M-SRE3: every route must have a live consumer. The health check already
    # verified transport connectivity above.
    return result.consumer_count == result.route_count


async def broker_liveness_async(broker: HealthProvider | Any, wedged_timeout: float = 60.0) -> bool:
    """Async variant of :func:`broker_liveness`."""
    return broker_liveness(broker, wedged_timeout=wedged_timeout)


async def broker_readiness_async(broker: HealthProvider | Any, config: HealthCheckConfig | None = None) -> bool:
    """Async variant of :func:`broker_readiness`."""
    return broker_readiness(broker, config=config)
