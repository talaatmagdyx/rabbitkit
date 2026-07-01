"""Tests for health.py — broker health check utilities."""

from __future__ import annotations

import warnings
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.protocols import HealthProvider
from rabbitkit.health import (
    BrokerHealthResult,
    HealthStatus,
    broker_health_check,
    broker_health_check_async,
    broker_liveness,
    broker_liveness_async,
    broker_readiness,
    broker_readiness_async,
    mark_heartbeat,
)

# Legacy brokers (using private attrs like _started) trigger a DeprecationWarning
# from the _get fallback helper. That warning is expected and tested separately;
# suppress it for the bulk of existing tests that exercise the fallback path.
pytestmark = pytest.mark.filterwarnings("ignore:Broker.*does not expose the typed property:DeprecationWarning")

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
        result = await broker_health_check_async(broker, config=HealthCheckConfig(pending_threshold=20))
        assert result.status == HealthStatus.DEGRADED


# ── liveness vs readiness (C-3 / SRE-C3) ────────────────────────────────


class TestLivenessVsReadiness:
    def _broker_with_routes(self, *, connected: bool, routes: list, transport: Any | None = None) -> SimpleNamespace:
        if transport is None:
            t = MagicMock()
            t.is_connected.return_value = connected
            transport = t
        broker = SimpleNamespace(_started=True, _transport=transport, routes=routes)
        broker._worker_pool = SimpleNamespace(pending_count=0)
        return broker

    def test_liveness_true_when_started_even_if_disconnected(self) -> None:
        """Liveness does not fail on a transient disconnect."""
        broker = self._broker_with_routes(connected=False, routes=[_make_route()])
        assert broker_liveness(broker) is True

    def test_liveness_false_when_not_started(self) -> None:
        broker = SimpleNamespace(_started=False)
        assert broker_liveness(broker) is False

    def test_liveness_false_when_wedged(self) -> None:
        broker = SimpleNamespace(_started=True, _wedged=True)
        assert broker_liveness(broker) is False

    def test_readiness_true_when_all_consumers_active(self) -> None:
        routes = [_make_route(consumer_tag="ctag-1"), _make_route(consumer_tag="ctag-2")]
        broker = self._broker_with_routes(connected=True, routes=routes)
        assert broker_readiness(broker) is True

    def test_readiness_false_when_disconnected(self) -> None:
        """A transient disconnect makes the broker not-ready but still live."""
        routes = [_make_route(consumer_tag="ctag-1")]
        broker = self._broker_with_routes(connected=False, routes=routes)
        assert broker_liveness(broker) is True
        assert broker_readiness(broker) is False

    def test_readiness_false_when_missing_consumers(self) -> None:
        routes = [_make_route(consumer_tag="ctag-1"), _make_route(consumer_tag=None)]
        broker = self._broker_with_routes(connected=True, routes=routes)
        assert broker_readiness(broker) is False

    def test_readiness_false_when_no_routes_and_zero_consumers_is_true(self) -> None:
        """Zero routes with zero consumers is trivially ready."""
        broker = self._broker_with_routes(connected=True, routes=[])
        assert broker_readiness(broker) is True

    def test_msre3_stale_consumer_tag_with_dead_channel(self) -> None:
        """consumer_tag stays set after channel dies — readiness must catch it."""
        t = MagicMock()
        t.is_connected.return_value = True  # connection alive
        t.is_consuming.return_value = False  # but channel/consumers gone
        routes = [_make_route(consumer_tag="stale-tag")]  # registered, now dead
        broker = self._broker_with_routes(connected=True, routes=routes, transport=t)

        result = broker_health_check(broker)
        # consumer_count forced to 0 → DEGRADED, and readiness False.
        assert result.consumer_count == 0
        assert result.status == HealthStatus.DEGRADED
        assert broker_readiness(broker) is False
        # Liveness unaffected — process still alive.
        assert broker_liveness(broker) is True

    async def test_async_liveness_and_readiness(self) -> None:
        routes = [_make_route(consumer_tag="ctag-1")]
        broker = self._broker_with_routes(connected=True, routes=routes)
        assert await broker_liveness_async(broker) is True
        assert await broker_readiness_async(broker) is True

    async def test_async_readiness_false_on_disconnect(self) -> None:
        routes = [_make_route(consumer_tag="ctag-1")]
        broker = self._broker_with_routes(connected=False, routes=routes)
        assert await broker_liveness_async(broker) is True
        assert await broker_readiness_async(broker) is False


# ── I-4: liveness heartbeat wedge detection ───────────────────────────────


class TestLivenessHeartbeat:
    """I-4: broker_liveness detects a wedge via a stale last_heartbeat."""

    def test_no_heartbeat_attr_falls_back_to_started(self) -> None:
        """Backwards compat: no last_heartbeat → only _started/_wedged matter."""
        broker = SimpleNamespace(_started=True)
        assert broker_liveness(broker) is True

    def test_fresh_heartbeat_is_live(self) -> None:
        broker = SimpleNamespace(_started=True)
        mark_heartbeat(broker)
        assert hasattr(broker, "last_heartbeat")
        assert broker_liveness(broker, wedged_timeout=60.0) is True

    def test_stale_heartbeat_is_not_live(self) -> None:
        """A heartbeat older than wedged_timeout fails liveness."""
        import time

        broker = SimpleNamespace(_started=True)
        mark_heartbeat(broker)
        # Rewind the heartbeat into the past, beyond the timeout.
        broker.last_heartbeat = time.monotonic() - 120.0
        assert broker_liveness(broker, wedged_timeout=60.0) is False

    def test_stale_within_timeout_is_live(self) -> None:
        """A heartbeat within the timeout window is still live."""
        import time

        broker = SimpleNamespace(_started=True)
        broker.last_heartbeat = time.monotonic() - 30.0
        assert broker_liveness(broker, wedged_timeout=60.0) is True

    def test_not_started_overrides_heartbeat(self) -> None:
        broker = SimpleNamespace(_started=False)
        mark_heartbeat(broker)
        assert broker_liveness(broker) is False

    def test_wedged_flag_overrides_fresh_heartbeat(self) -> None:
        import time

        broker = SimpleNamespace(_started=True, _wedged=True)
        broker.last_heartbeat = time.monotonic()
        assert broker_liveness(broker) is False

    def test_custom_wedged_timeout(self) -> None:
        """A small wedged_timeout catches a slightly-stale heartbeat."""
        import time

        broker = SimpleNamespace(_started=True)
        broker.last_heartbeat = time.monotonic() - 0.05
        assert broker_liveness(broker, wedged_timeout=0.01) is False
        assert broker_liveness(broker, wedged_timeout=1.0) is True

    async def test_async_liveness_stale_heartbeat(self) -> None:
        import time

        broker = SimpleNamespace(_started=True)
        broker.last_heartbeat = time.monotonic() - 120.0
        assert await broker_liveness_async(broker, wedged_timeout=60.0) is False

    def test_mark_heartbeat_sets_monotonic_value(self) -> None:
        import time

        broker = SimpleNamespace(_started=True)
        before = time.monotonic()
        mark_heartbeat(broker)
        after = time.monotonic()
        assert before <= broker.last_heartbeat <= after


# ── I-5: transport-contract consumer liveness ─────────────────────────────


class TestTransportConsumerLiveness:
    """I-5: _transport_consumers_alive honours transport-provided signals."""

    def _broker_with_transport(self, transport: Any, routes: list[Any]) -> SimpleNamespace:
        broker = SimpleNamespace(_started=True, _transport=transport, routes=routes)
        broker._worker_pool = SimpleNamespace(pending_count=0)
        return broker

    def test_has_open_channels_false_drops_consumer_count(self) -> None:
        """A transport exposing has_open_channels=False → readiness drops consumers."""
        transport = SimpleNamespace(
            is_connected=lambda: True,
            has_open_channels=False,
        )
        routes = [_make_route(consumer_tag="ctag-1")]
        broker = self._broker_with_transport(transport, routes)
        result = broker_health_check(broker)
        assert result.consumer_count == 0
        assert result.status == HealthStatus.DEGRADED
        assert broker_readiness(broker) is False

    def test_has_open_channels_true_keeps_consumer_count(self) -> None:
        """A transport exposing has_open_channels=True → consumers counted."""
        transport = SimpleNamespace(
            is_connected=lambda: True,
            has_open_channels=True,
        )
        routes = [_make_route(consumer_tag="ctag-1")]
        broker = self._broker_with_transport(transport, routes)
        result = broker_health_check(broker)
        assert result.consumer_count == 1
        assert result.status == HealthStatus.HEALTHY
        assert broker_readiness(broker) is True

    def test_callable_has_open_channels_false(self) -> None:
        """has_open_channels may be a callable returning bool."""
        transport = SimpleNamespace(
            is_connected=lambda: True,
            has_open_channels=lambda: False,
        )
        broker = self._broker_with_transport(transport, [_make_route(consumer_tag="ctag-1")])
        assert broker_health_check(broker).consumer_count == 0

    def test_callable_is_consuming_false(self) -> None:
        """is_consuming (callable) reporting False drops consumers."""
        transport = SimpleNamespace(
            is_connected=lambda: True,
            is_consuming=lambda: False,
        )
        broker = self._broker_with_transport(transport, [_make_route(consumer_tag="ctag-1")])
        assert broker_health_check(broker).consumer_count == 0

    def test_no_contract_attrs_falls_back_to_consumer_tag(self) -> None:
        """When the transport exposes none of the contract attrs, trust consumer_tag."""
        transport = SimpleNamespace(is_connected=lambda: True)
        broker = self._broker_with_transport(transport, [_make_route(consumer_tag="ctag-1")])
        result = broker_health_check(broker)
        assert result.consumer_count == 1
        assert broker_readiness(broker) is True

    def test_consumers_active_false(self) -> None:
        """consumers_active=False drops consumers."""
        transport = SimpleNamespace(
            is_connected=lambda: True,
            consumers_active=False,
        )
        broker = self._broker_with_transport(transport, [_make_route(consumer_tag="ctag-1")])
        assert broker_health_check(broker).consumer_count == 0

    def test_transport_contract_attr_raises_is_ignored(self) -> None:
        """A contract attr callable that raises is skipped (not treated as False)."""

        def boom() -> bool:
            raise RuntimeError("boom")

        transport = SimpleNamespace(
            is_connected=lambda: True,
            has_open_channels=boom,
        )
        broker = self._broker_with_transport(transport, [_make_route(consumer_tag="ctag-1")])
        # The raising attr is skipped; no other attr present → trust consumer_tag.
        assert broker_health_check(broker).consumer_count == 1

    def test_has_open_channels_property_on_sync_transport(self) -> None:
        """SyncTransport.has_open_channels reflects _consumer_channels (I-5)."""
        from rabbitkit.sync.transport import SyncTransport

        t = SyncTransport()
        # No consumer channels → False.
        assert t.has_open_channels is False

        ch_open = SimpleNamespace(is_open=True)
        ch_closed = SimpleNamespace(is_open=False)
        t._consumer_channels = {"q1": ch_open}
        assert t.has_open_channels is True
        t._consumer_channels = {"q1": ch_open, "q2": ch_closed}
        assert t.has_open_channels is False
        t._consumer_channels = {"q1": ch_closed}
        assert t.has_open_channels is False


# ── R5: HealthProvider protocol + typed-property path ─────────────────────


class _TypedBroker:
    """A broker that fully implements the HealthProvider protocol via properties."""

    def __init__(
        self,
        *,
        started: bool = True,
        connected: bool = True,
        consumer_count: int = 0,
        route_count: int = 0,
        worker_pool_pending: int = 0,
        last_heartbeat: float | None = None,
        wedged: bool = False,
    ) -> None:
        self._started = started
        self._connected = connected
        self._consumer_count = consumer_count
        self._route_count = route_count
        self._worker_pool_pending = worker_pool_pending
        self._last_heartbeat = last_heartbeat
        self._wedged = wedged

    @property
    def started(self) -> bool:
        return self._started

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def consumer_count(self) -> int:
        return self._consumer_count

    @property
    def route_count(self) -> int:
        return self._route_count

    @property
    def worker_pool_pending(self) -> int:
        return self._worker_pool_pending

    @property
    def last_heartbeat(self) -> float | None:
        return self._last_heartbeat

    @property
    def wedged(self) -> bool:
        return self._wedged


class TestHealthProviderProtocol:
    """R5: HealthProvider is a runtime-checkable protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        assert isinstance(_TypedBroker(), HealthProvider)

    def test_plain_namespace_does_not_satisfy_protocol(self) -> None:
        # A SimpleNamespace without the required properties does not satisfy.
        broker = SimpleNamespace(_started=True)
        assert not isinstance(broker, HealthProvider)


class TestTypedBrokerHealthCheck:
    """R5: broker_health_check reads typed properties directly (no fallback)."""

    def test_healthy_typed_broker(self) -> None:
        broker = _TypedBroker(
            started=True,
            connected=True,
            consumer_count=2,
            route_count=2,
            worker_pool_pending=0,
        )
        result = broker_health_check(broker)
        assert result.status == HealthStatus.HEALTHY
        assert result.started is True
        assert result.connected is True
        assert result.consumer_count == 2
        assert result.route_count == 2
        assert result.worker_pool_pending == 0

    def test_unhealthy_not_started_typed(self) -> None:
        broker = _TypedBroker(started=False)
        result = broker_health_check(broker)
        assert result.status == HealthStatus.UNHEALTHY
        assert result.started is False

    def test_unhealthy_not_connected_typed(self) -> None:
        broker = _TypedBroker(started=True, connected=False)
        result = broker_health_check(broker)
        assert result.status == HealthStatus.UNHEALTHY
        assert result.connected is False

    def test_degraded_missing_consumers_typed(self) -> None:
        broker = _TypedBroker(
            started=True,
            connected=True,
            consumer_count=1,
            route_count=3,
            worker_pool_pending=0,
        )
        result = broker_health_check(broker)
        assert result.status == HealthStatus.DEGRADED
        assert "1/3" in result.details["reason"]

    def test_degraded_worker_pool_backlog_typed(self) -> None:
        broker = _TypedBroker(
            started=True,
            connected=True,
            consumer_count=1,
            route_count=1,
            worker_pool_pending=150,
        )
        result = broker_health_check(broker)
        assert result.status == HealthStatus.DEGRADED
        assert "backlog" in result.details["reason"]

    def test_readiness_typed_broker(self) -> None:
        broker = _TypedBroker(
            started=True,
            connected=True,
            consumer_count=2,
            route_count=2,
        )
        assert broker_readiness(broker) is True

    def test_readiness_false_missing_consumers_typed(self) -> None:
        broker = _TypedBroker(
            started=True,
            connected=True,
            consumer_count=1,
            route_count=2,
        )
        assert broker_readiness(broker) is False


class TestTypedBrokerLiveness:
    """R5: broker_liveness reads typed properties directly."""

    def test_liveness_started_and_not_wedged(self) -> None:
        broker = _TypedBroker(started=True, wedged=False)
        assert broker_liveness(broker) is True

    def test_liveness_not_started(self) -> None:
        broker = _TypedBroker(started=False)
        assert broker_liveness(broker) is False

    def test_liveness_wedged(self) -> None:
        broker = _TypedBroker(started=True, wedged=True)
        assert broker_liveness(broker) is False

    def test_liveness_stale_heartbeat(self) -> None:
        import time

        broker = _TypedBroker(
            started=True,
            last_heartbeat=time.monotonic() - 120.0,
        )
        assert broker_liveness(broker, wedged_timeout=60.0) is False

    def test_liveness_fresh_heartbeat(self) -> None:
        import time

        broker = _TypedBroker(
            started=True,
            last_heartbeat=time.monotonic(),
        )
        assert broker_liveness(broker, wedged_timeout=60.0) is True


class TestDeprecationWarning:
    """R5: a legacy broker (private attrs only) emits a DeprecationWarning."""

    def test_legacy_broker_emits_deprecation_warning(self) -> None:
        broker = SimpleNamespace(_started=True, _transport=MagicMock(is_connected=lambda: True), routes=[])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            broker_health_check(broker)
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert len(dep_warnings) >= 1
        assert "does not expose the typed property" in str(dep_warnings[0].message)

    def test_typed_broker_emits_no_deprecation_warning(self) -> None:
        broker = _TypedBroker(
            started=True,
            connected=True,
            consumer_count=0,
            route_count=0,
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            broker_health_check(broker)
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert len(dep_warnings) == 0


class TestBrokerLivenessConnectedFalse:
    def test_returns_false_when_not_connected(self) -> None:
        """broker_liveness returns False when status is not UNHEALTHY but connected=False."""
        from types import SimpleNamespace
        from unittest.mock import patch

        from rabbitkit.health import (
            BrokerHealthResult,
            HealthStatus,
            broker_liveness,
        )

        broker = SimpleNamespace()
        result = BrokerHealthResult(
            status=HealthStatus.HEALTHY,
            started=True,
            connected=False,
            consumer_count=0,
            route_count=0,
        )
        with patch("rabbitkit.health.broker_health_check", return_value=result):
            assert broker_liveness(broker) is False


class TestBrokerReadinessConnectedFalse:
    """Line 368: broker_readiness returns False when the health check result
    has status != UNHEALTHY but connected=False.

    This path is distinct from the ``status == UNHEALTHY`` early-return on
    line 365.  It is reached when ``broker_health_check`` returns a result
    that is HEALTHY or DEGRADED, yet the ``connected`` field is False
    (e.g. a custom mock or a future broker implementation that diverges from
    the reference implementation's invariant).
    """

    def test_readiness_returns_false_when_not_connected_but_not_unhealthy(self) -> None:
        from unittest.mock import patch

        broker = SimpleNamespace()
        # Produce a result where status is HEALTHY but connected is False.
        # This makes the first guard (UNHEALTHY → False) pass, and the second
        # guard (not connected → False) on line 368 fire.
        mocked_result = BrokerHealthResult(
            status=HealthStatus.HEALTHY,
            started=True,
            connected=False,
            consumer_count=0,
            route_count=0,
        )
        with patch("rabbitkit.health.broker_health_check", return_value=mocked_result):
            result = broker_readiness(broker)

        assert result is False
