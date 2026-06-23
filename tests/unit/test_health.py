"""Tests for health.py — broker health check utilities."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

from rabbitkit.health import (
    BrokerHealthResult,
    HealthStatus,
    broker_health_check,
    broker_health_check_async,
)

# ── helpers ───────────────────────────────────────────────────────────────


def _make_broker(
    *,
    started: bool = True,
    transport: Any | None = "default",
    connected: bool = True,
    routes: list[Any] | None = None,
    worker_pool: Any | None = "default",
    pool_pending: int = 0,
) -> SimpleNamespace:
    """Build a mock broker with configurable attributes.

    Args:
        started: Whether ``_started`` is True.
        transport: The ``_transport`` object.  Pass ``None`` to omit, or
            ``"default"`` to create a mock transport.
        connected: Value returned by ``transport.is_connected()``.
        routes: List of route objects (each may have ``consumer_tag``).
        worker_pool: The ``_worker_pool`` object.  Pass ``None`` to omit,
            or ``"default"`` to create one from *pool_pending*.
        pool_pending: ``pending_count`` on the worker pool.
    """
    broker = SimpleNamespace(_started=started)

    if transport == "default":
        t = MagicMock()
        t.is_connected.return_value = connected
        broker._transport = t
    elif transport is None:
        pass  # no _transport attribute at all
    else:
        broker._transport = transport

    broker.routes = routes if routes is not None else []

    if worker_pool == "default":
        broker._worker_pool = SimpleNamespace(pending_count=pool_pending)
    elif worker_pool is None:
        pass  # no _worker_pool attribute at all
    else:
        broker._worker_pool = worker_pool

    return broker


def _make_route(*, consumer_tag: str | None = "ctag-1") -> SimpleNamespace:
    """Build a mock route with an optional consumer_tag."""
    return SimpleNamespace(consumer_tag=consumer_tag)


# ── HealthStatus enum ────────────────────────────────────────────────────


class TestHealthStatus:
    def test_enum_values(self) -> None:
        """Enum values are the expected lowercase strings."""
        assert HealthStatus.HEALTHY == "healthy"
        assert HealthStatus.DEGRADED == "degraded"
        assert HealthStatus.UNHEALTHY == "unhealthy"

    def test_enum_is_str(self) -> None:
        """HealthStatus members are also str instances."""
        assert isinstance(HealthStatus.HEALTHY, str)
        assert isinstance(HealthStatus.DEGRADED, str)
        assert isinstance(HealthStatus.UNHEALTHY, str)

    def test_enum_value_attribute(self) -> None:
        """The .value attribute matches the string."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"


# ── BrokerHealthResult dataclass ─────────────────────────────────────────


class TestBrokerHealthResult:
    def test_defaults(self) -> None:
        """Default field values are correct."""
        result = BrokerHealthResult(status=HealthStatus.HEALTHY)
        assert result.started is False
        assert result.connected is False
        assert result.consumer_count == 0
        assert result.route_count == 0
        assert result.worker_pool_pending == 0
        assert result.details == {}

    def test_all_fields(self) -> None:
        """All fields can be set explicitly."""
        result = BrokerHealthResult(
            status=HealthStatus.DEGRADED,
            started=True,
            connected=True,
            consumer_count=3,
            route_count=5,
            worker_pool_pending=42,
            details={"reason": "missing consumers"},
        )
        assert result.status == HealthStatus.DEGRADED
        assert result.started is True
        assert result.connected is True
        assert result.consumer_count == 3
        assert result.route_count == 5
        assert result.worker_pool_pending == 42
        assert result.details == {"reason": "missing consumers"}

    def test_frozen(self) -> None:
        """BrokerHealthResult is immutable (frozen dataclass)."""
        result = BrokerHealthResult(status=HealthStatus.HEALTHY)
        try:
            result.status = HealthStatus.UNHEALTHY  # type: ignore[misc]
        except AttributeError:
            pass
        else:
            raise AssertionError("Expected AttributeError for frozen dataclass")

    def test_equality(self) -> None:
        """Two results with same fields are equal."""
        a = BrokerHealthResult(status=HealthStatus.HEALTHY, started=True, connected=True)
        b = BrokerHealthResult(status=HealthStatus.HEALTHY, started=True, connected=True)
        assert a == b


# ── broker_health_check (sync) ───────────────────────────────────────────


class TestBrokerHealthCheck:
    def test_unhealthy_when_not_started(self) -> None:
        """Broker not started -> UNHEALTHY."""
        broker = _make_broker(started=False)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.UNHEALTHY
        assert result.started is False
        assert result.details["reason"] == "broker not started"

    def test_unhealthy_when_not_connected(self) -> None:
        """Started but transport not connected -> UNHEALTHY."""
        broker = _make_broker(started=True, connected=False)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.UNHEALTHY
        assert result.started is True
        assert result.connected is False
        assert result.details["reason"] == "transport not connected"

    def test_healthy_when_all_good(self) -> None:
        """Started, connected, all consumers active -> HEALTHY."""
        routes = [_make_route(consumer_tag="ctag-1"), _make_route(consumer_tag="ctag-2")]
        broker = _make_broker(started=True, connected=True, routes=routes)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.HEALTHY
        assert result.started is True
        assert result.connected is True
        assert result.consumer_count == 2
        assert result.route_count == 2
        assert result.details == {}

    def test_degraded_missing_consumers(self) -> None:
        """Some routes without consumer_tag -> DEGRADED."""
        routes = [
            _make_route(consumer_tag="ctag-1"),
            _make_route(consumer_tag=None),
            _make_route(consumer_tag=None),
        ]
        broker = _make_broker(started=True, connected=True, routes=routes)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.DEGRADED
        assert result.consumer_count == 1
        assert result.route_count == 3
        assert "1/3" in result.details["reason"]

    def test_degraded_worker_pool_backlog(self) -> None:
        """Pool pending_count > 100 -> DEGRADED."""
        routes = [_make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=150)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.DEGRADED
        assert result.worker_pool_pending == 150
        assert "backlog" in result.details["reason"]

    def test_no_routes(self) -> None:
        """Started, connected, zero routes -> HEALTHY."""
        broker = _make_broker(started=True, connected=True, routes=[])
        result = broker_health_check(broker)

        assert result.status == HealthStatus.HEALTHY
        assert result.consumer_count == 0
        assert result.route_count == 0

    def test_with_worker_pool_no_backlog(self) -> None:
        """Pool exists but pending=0 -> HEALTHY."""
        routes = [_make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=0)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.HEALTHY
        assert result.worker_pool_pending == 0

    def test_result_fields_populated(self) -> None:
        """Check all BrokerHealthResult fields populated correctly."""
        routes = [_make_route(consumer_tag="ctag-1"), _make_route(consumer_tag="ctag-2")]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=5)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.HEALTHY
        assert result.started is True
        assert result.connected is True
        assert result.consumer_count == 2
        assert result.route_count == 2
        assert result.worker_pool_pending == 5
        assert result.details == {}

    def test_no_transport(self) -> None:
        """Started=True but no transport -> UNHEALTHY."""
        broker = _make_broker(started=True, transport=None, connected=False)
        # Remove _transport entirely so getattr returns None
        if hasattr(broker, "_transport"):
            del broker._transport
        result = broker_health_check(broker)

        assert result.status == HealthStatus.UNHEALTHY
        assert result.started is True
        assert result.connected is False
        assert result.details["reason"] == "transport not connected"

    def test_no_worker_pool(self) -> None:
        """No _worker_pool attribute -> pending defaults to 0, still HEALTHY."""
        routes = [_make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes, worker_pool=None)
        if hasattr(broker, "_worker_pool"):
            del broker._worker_pool
        result = broker_health_check(broker)

        assert result.status == HealthStatus.HEALTHY
        assert result.worker_pool_pending == 0

    def test_pool_backlog_at_threshold(self) -> None:
        """Pool pending_count exactly 100 -> HEALTHY (threshold is >100)."""
        routes = [_make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=100)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.HEALTHY
        assert result.worker_pool_pending == 100

    def test_pool_backlog_just_above_threshold(self) -> None:
        """Pool pending_count 101 -> DEGRADED."""
        routes = [_make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=101)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.DEGRADED
        assert "backlog" in result.details["reason"]

    def test_missing_consumers_takes_priority_over_backlog(self) -> None:
        """When both consumers missing and backlog high, consumer check runs first."""
        routes = [_make_route(consumer_tag=None)]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=200)
        result = broker_health_check(broker)

        assert result.status == HealthStatus.DEGRADED
        # Should report missing consumers, not backlog
        assert "consumers active" in result.details["reason"]


# ── broker_health_check_async ────────────────────────────────────────────


class TestBrokerHealthCheckAsync:
    async def test_async_returns_same_result(self) -> None:
        """Async variant returns same result as sync."""
        routes = [_make_route(), _make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes)

        sync_result = broker_health_check(broker)
        async_result = await broker_health_check_async(broker)

        assert sync_result == async_result

    async def test_async_unhealthy(self) -> None:
        """Async check on unhealthy broker."""
        broker = _make_broker(started=False)
        result = await broker_health_check_async(broker)

        assert result.status == HealthStatus.UNHEALTHY
        assert result.started is False

    async def test_async_degraded(self) -> None:
        """Async check on degraded broker."""
        routes = [_make_route(consumer_tag=None)]
        broker = _make_broker(started=True, connected=True, routes=routes)
        result = await broker_health_check_async(broker)

        assert result.status == HealthStatus.DEGRADED


# ── HealthCheckConfig ─────────────────────────────────────────────────────


class TestHealthCheckConfig:
    def test_custom_threshold_triggers_degraded(self) -> None:
        """Pending > custom threshold -> DEGRADED."""
        from rabbitkit.core.config import HealthCheckConfig

        routes = [_make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=50)
        result = broker_health_check(broker, config=HealthCheckConfig(pending_threshold=40))

        assert result.status == HealthStatus.DEGRADED
        assert "backlog" in result.details["reason"]

    def test_custom_threshold_healthy_below(self) -> None:
        """Pending <= custom threshold -> HEALTHY."""
        from rabbitkit.core.config import HealthCheckConfig

        routes = [_make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=50)
        result = broker_health_check(broker, config=HealthCheckConfig(pending_threshold=60))

        assert result.status == HealthStatus.HEALTHY

    def test_default_threshold_is_100(self) -> None:
        """Default threshold is 100 (unchanged from before)."""
        from rabbitkit.core.config import HealthCheckConfig

        assert HealthCheckConfig().pending_threshold == 100

    def test_none_config_uses_default(self) -> None:
        """Passing config=None uses default HealthCheckConfig."""
        routes = [_make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=101)
        result = broker_health_check(broker, config=None)
        assert result.status == HealthStatus.DEGRADED

    async def test_async_respects_custom_threshold(self) -> None:
        """Async variant also accepts HealthCheckConfig."""
        from rabbitkit.core.config import HealthCheckConfig

        routes = [_make_route()]
        broker = _make_broker(started=True, connected=True, routes=routes, pool_pending=30)
        result = await broker_health_check_async(
            broker, config=HealthCheckConfig(pending_threshold=20)
        )
        assert result.status == HealthStatus.DEGRADED
