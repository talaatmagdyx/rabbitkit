"""Health check utilities for rabbitkit brokers.

Provides callables suitable for use with any monitoring or health-check
framework.

Usage::

    from rabbitkit.health import broker_health_check, BrokerStatus

    # Standalone
    status = broker_health_check(broker)
    print(status.status, status.details)

    # With any health-router framework
    register_check(name="rabbitmq", check=lambda: broker_health_check(broker))
"""

from __future__ import annotations

import enum
import logging
import time
import typing
import warnings
from dataclasses import dataclass, field, replace
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
    blocked: bool = False
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


def _get_is_blocked(broker: Any, transport: Any) -> bool:
    """L15: True if the connection is currently blocked by broker-side flow
    control (e.g. a memory/disk alarm). This is orthogonal to
    ``connected`` -- an open connection can be blocked while still
    reporting connected, since ``connection.blocked`` is a soft
    publish-side flow-control notification, not a disconnect.

    Checks the opt-in ``FlowController`` first
    (``broker.flow_controller.is_blocked``); falls back to the transport's
    own passive blocked-state tracking so this is visible even without an
    explicit ``FlowController`` wired in.
    """
    fc = getattr(broker, "flow_controller", None)
    if fc is not None:
        is_blocked = getattr(fc, "is_blocked", _MISSING)
        if is_blocked is not _MISSING:
            return bool(is_blocked)
    if transport is not None:
        is_blocked = getattr(transport, "is_blocked", _MISSING)
        if is_blocked is not _MISSING:
            return bool(is_blocked)
    return False


def _get_last_heartbeat(broker: Any) -> float | None:
    hb = _get(broker, "last_heartbeat", "_last_heartbeat", None)
    if hb is None:
        return None
    return float(hb)


def _local_broker_health_check(
    broker: HealthProvider | Any,
    config: HealthCheckConfig | None = None,
) -> BrokerHealthResult:
    """Process-local health check — see :func:`broker_health_check`.

    Only inspects *this process's* view of the broker (its own connection,
    consumers, worker pool). Cannot detect a network partition where this
    process still holds a live connection to one node while the rest of the
    cluster is unreachable — that requires an independent signal, which
    :func:`broker_health_check`'s optional ``management_client`` provides.
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

    # L15: a connection can be blocked (broker memory/disk alarm pausing
    # publishes) while still reporting connected -- check this before the
    # route/consumer checks below since it's the more actionable root cause.
    blocked = _get_is_blocked(broker, transport)

    # Check routes and consumers
    route_count = _get_route_count(broker)
    consumer_count = _get_consumer_count(broker, transport)

    # Check worker pool
    worker_pool_pending = _get_worker_pool_pending(broker)

    # Determine status
    if blocked:
        status = HealthStatus.DEGRADED
        details: dict[str, Any] = {
            "reason": "connection blocked by broker flow control (memory/disk alarm); publishes will stall"
        }
    elif consumer_count < route_count:
        status = HealthStatus.DEGRADED
        details = {"reason": f"only {consumer_count}/{route_count} consumers active"}
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
        blocked=blocked,
        details=details,
    )


def _apply_management_check(result: BrokerHealthResult, ok: bool) -> BrokerHealthResult:
    if ok or result.status == HealthStatus.UNHEALTHY:
        return result
    return replace(
        result,
        status=HealthStatus.DEGRADED,
        details={
            **result.details,
            "management_check": "failed — this process has a live broker connection, but the "
            "management API reports the node unhealthy (possible cluster partition)",
        },
    )


def broker_health_check(
    broker: HealthProvider | Any,
    config: HealthCheckConfig | None = None,
    management_client: Any = None,
) -> BrokerHealthResult:
    """Check broker health status (sync).

    Args:
        broker: A broker implementing :class:`HealthProvider` (typed
            properties) or a legacy broker exposing private attributes
            (``_started``, ``_transport``, ``_worker_pool``).
        config: Optional :class:`HealthCheckConfig` to tune thresholds.
        management_client: Optional :class:`~rabbitkit.management.RabbitManagementClient`
            (sync ``.health_check()``). When given, its result is folded in
            as an additional signal: this process may hold a perfectly live
            connection to one node while the rest of a partitioned cluster is
            unreachable, which the process-local checks alone cannot detect.
            A failing management check downgrades an otherwise-HEALTHY result
            to DEGRADED (never overrides an UNHEALTHY local result). Omit for
            the original process-local-only behavior.

    Returns:
        BrokerHealthResult with status HEALTHY (started, connected, not
        blocked, all consumers active), DEGRADED (started but connection
        blocked (L15), consumers missing, pool backlog high, or — with
        ``management_client`` — the management API reports the node
        unhealthy), or UNHEALTHY (not started or not connected).
    """
    result = _local_broker_health_check(broker, config=config)
    if management_client is not None and result.status != HealthStatus.UNHEALTHY:
        result = _apply_management_check(result, management_client.health_check())
    return result


async def broker_health_check_async(
    broker: HealthProvider | Any,
    config: HealthCheckConfig | None = None,
    management_client: Any = None,
) -> BrokerHealthResult:
    """Async variant of broker_health_check.

    Same local logic as sync -- transport.is_connected() is always sync.
    ``management_client`` (if given) must expose an async ``.health_check_async()``
    — see :func:`broker_health_check` for the rationale and semantics.
    """
    result = _local_broker_health_check(broker, config=config)
    if management_client is not None and result.status != HealthStatus.UNHEALTHY:
        result = _apply_management_check(result, await management_client.health_check_async())
    return result


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


def broker_readiness(
    broker: HealthProvider | Any,
    config: HealthCheckConfig | None = None,
    management_client: Any = None,
) -> bool:
    """Readiness probe — is the broker ready to serve traffic right now?

    Args:
        broker: A broker implementing :class:`HealthProvider` (typed
            properties) or a legacy broker exposing private attributes.
        config: Optional :class:`HealthCheckConfig` to tune thresholds.
        management_client: Optional :class:`~rabbitkit.management.RabbitManagementClient`
            — see :func:`broker_health_check`. A failing management check
            fails readiness even if this process's own connection looks fine.

    Requires: health check not UNHEALTHY, transport connected, connection
    not blocked by broker flow control (L15), and every registered route
    has an active (live) consumer. Use this for load-balancer / ingress
    gating; use :func:`broker_liveness` for restart decisions.
    """
    result = broker_health_check(broker, config=config, management_client=management_client)
    if result.status == HealthStatus.UNHEALTHY:
        return False
    if not result.connected:
        return False
    # L15: a blocked connection can't publish -- not ready for traffic even
    # though it's technically still "connected" and may still have live
    # consumers.
    if result.blocked:
        return False
    # A failing management check downgrades to DEGRADED rather than
    # UNHEALTHY (this process's own connection may be fine), but a
    # partitioned/unreachable node is still not ready for traffic.
    if "management_check" in result.details:
        return False
    # M-SRE3: every route must have a live consumer. The health check already
    # verified transport connectivity above.
    return result.consumer_count == result.route_count


async def broker_liveness_async(broker: HealthProvider | Any, wedged_timeout: float = 60.0) -> bool:
    """Async variant of :func:`broker_liveness`."""
    return broker_liveness(broker, wedged_timeout=wedged_timeout)


async def broker_readiness_async(
    broker: HealthProvider | Any,
    config: HealthCheckConfig | None = None,
    management_client: Any = None,
) -> bool:
    """Async variant of :func:`broker_readiness`.

    Uses :func:`broker_health_check_async` (``management_client.health_check_async()``)
    rather than delegating to the sync ``broker_readiness`` — the sync
    management check makes a blocking network call, which must not run on
    the event loop.
    """
    result = await broker_health_check_async(broker, config=config, management_client=management_client)
    if result.status == HealthStatus.UNHEALTHY:
        return False
    if not result.connected:
        return False
    if result.blocked:
        return False
    if "management_check" in result.details:
        return False
    return result.consumer_count == result.route_count


# ── Health-transition watcher ────────────────────────────────────────────


class HealthWatcher:
    """Opt-in push-style health notifications (sync, daemon-thread poller).

    Polls :func:`broker_health_check` every *interval* seconds and fires
    ``on_change(old, new, result)`` when the status transitions -- but only
    after *debounce* consecutive identical readings, so a single flapping
    poll never pages anyone. Callback exceptions are logged, never raised,
    and never stall the loop.

    Positioning: for deployments that aren't (only) Kubernetes -- bare
    metal, VMs, direct pager/webhook wiring. On k8s, keep
    :func:`broker_liveness`/:func:`broker_readiness` probes as the primary
    signal; this watcher complements, never replaces, them.

    When *collector* is given (any ``MetricsCollector`` with ``set_gauge``),
    every poll also emits a ``rabbitkit_health_state`` gauge
    (0 healthy / 1 degraded / 2 unhealthy), so Prometheus users get a state
    series without writing a callback.

    *clock* and *sleeper* are injectable for tests (no wall-clock sleeps in
    the test suite -- the 1.2.0 deflaking lesson).
    """

    _GAUGE_VALUES: typing.ClassVar[dict[HealthStatus, int]] = {
        HealthStatus.HEALTHY: 0,
        HealthStatus.DEGRADED: 1,
        HealthStatus.UNHEALTHY: 2,
    }

    def __init__(
        self,
        broker: HealthProvider | Any,
        *,
        interval: float = 10.0,
        on_change: Any = None,
        management_client: Any = None,
        config: HealthCheckConfig | None = None,
        debounce: int = 2,
        collector: Any = None,
        gauge_name: str = "rabbitkit_health_state",
    ) -> None:
        if interval <= 0:
            raise ValueError(f"HealthWatcher interval must be > 0, got {interval}")
        if debounce < 1:
            raise ValueError(f"HealthWatcher debounce must be >= 1, got {debounce}")
        self._broker = broker
        self._interval = interval
        self._on_change = on_change
        self._management_client = management_client
        self._config = config
        self._debounce = debounce
        self._collector = collector
        self._gauge_name = gauge_name

        self._current: HealthStatus | None = None  # last CONFIRMED status
        self._candidate: HealthStatus | None = None
        self._candidate_count = 0
        self._thread: Any = None
        self._stop_event: Any = None

    @property
    def current_status(self) -> HealthStatus | None:
        """Last debounce-confirmed status (None until the first confirmation)."""
        return self._current

    def _tick(self) -> None:
        """One poll: read health, then run the shared debounced state machine."""
        result = broker_health_check(
            self._broker, config=self._config, management_client=self._management_client
        )
        self._apply(result)

    def _apply(self, result: BrokerHealthResult) -> None:
        """Debounced state machine on an already-obtained result (shared with
        the async variant)."""
        if self._collector is not None:
            try:
                self._collector.set_gauge(self._gauge_name, {}, self._GAUGE_VALUES[result.status])
            except Exception:  # pragma: no cover — collector bugs never stall the loop
                logger.exception("HealthWatcher gauge emission raised")

        status = result.status
        if status == self._current:
            # Confirmed state re-observed; reset any half-built candidate.
            self._candidate = None
            self._candidate_count = 0
            return
        if status != self._candidate:
            self._candidate = status
            self._candidate_count = 0
        self._candidate_count += 1
        if self._candidate_count < self._debounce:
            return
        old, self._current = self._current, status
        self._candidate = None
        self._candidate_count = 0
        if self._on_change is not None:
            try:
                self._on_change(old, status, result)
            except Exception:
                logger.exception("HealthWatcher on_change callback raised")

    def start(self) -> None:
        """Start the daemon poller thread. Idempotent."""
        import threading

        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event = threading.Event()
        stop = self._stop_event

        def _loop() -> None:
            while not stop.wait(timeout=self._interval):
                try:
                    self._tick()
                except Exception:  # pragma: no cover — defensive; _tick guards itself
                    logger.exception("HealthWatcher tick raised")

        self._thread = threading.Thread(target=_loop, name="rabbitkit-health-watcher", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the poller (bounded join). Idempotent."""
        if self._stop_event is not None:
            self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None


class AsyncHealthWatcher(HealthWatcher):
    """Async variant of :class:`HealthWatcher` — an asyncio task instead of
    a thread, and the management check (if any) awaited via
    :func:`broker_health_check_async` so it never blocks the event loop."""

    async def _tick_async(self) -> None:
        result = await broker_health_check_async(
            self._broker, config=self._config, management_client=self._management_client
        )
        self._apply(result)

    async def run(self) -> None:
        """Poll forever (cancel the task to stop)."""
        import asyncio

        while True:
            await asyncio.sleep(self._interval)
            try:
                await self._tick_async()
            except Exception:  # pragma: no cover — defensive; _tick guards itself
                logger.exception("HealthWatcher tick raised")
