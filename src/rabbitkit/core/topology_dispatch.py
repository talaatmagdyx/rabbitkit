"""Shared topology-mode dispatch logic.

Both the sync (``sync/transport.py``) and async (``async_/transport.py``)
transports repeated the same ``TopologyMode`` conditional: skip on
``MANUAL``, passive-check on ``PASSIVE_ONLY`` / ``entity.passive``, else
active declare.  That ~150 lines of duplicated decision logic now lives
here.

Design note — why a thin action-returning dispatcher (not callables):
The async transport's declare/get calls are coroutines, but lambdas
cannot ``await``.  Rather than maintain sync/async callable variants,
this dispatcher decides *what to do* (returns a ``TopoAction``) and lets
each transport perform the actual sync/async channel call.  This keeps
the dispatcher sync-only, transport-agnostic, and free of pika/aio-pika
imports (preserving the ``core/`` zero-transport-import invariant).
"""

from __future__ import annotations

from enum import Enum, auto

from rabbitkit.core.errors import ConfigurationError
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import TopologyMode


class TopoAction(Enum):
    """What the transport should do for a topology entity."""

    SKIP = auto()  # TopologyMode.MANUAL — do nothing
    PASSIVE = auto()  # PASSIVE_ONLY or entity.passive — passive existence check
    DECLARE = auto()  # active declaration


class TopologyDispatcher:
    """Resolves ``TopologyMode`` into a concrete ``TopoAction`` per entity.

    The transport computes ``to_declare_kwargs()`` and performs the actual
    (sync or async) channel call based on the returned ``TopoAction``,
    keeping all transport-specific I/O out of this class.
    """

    def __init__(self, mode: TopologyMode) -> None:
        self._mode = mode

    @property
    def mode(self) -> TopologyMode:
        """The topology mode this dispatcher was configured with."""
        return self._mode

    def exchange_action(self, exchange: RabbitExchange) -> TopoAction:
        """Action to take for ``declare_exchange(exchange)``."""
        if self._mode == TopologyMode.MANUAL:
            return TopoAction.SKIP
        if self._mode == TopologyMode.PASSIVE_ONLY or exchange.passive:
            return TopoAction.PASSIVE
        return TopoAction.DECLARE

    def queue_action(self, queue: RabbitQueue) -> TopoAction:
        """Action to take for ``declare_queue(queue)``."""
        if self._mode == TopologyMode.MANUAL:
            return TopoAction.SKIP
        if self._mode == TopologyMode.PASSIVE_ONLY or queue.passive:
            return TopoAction.PASSIVE
        return TopoAction.DECLARE

    def binding_action(self) -> TopoAction:
        """Action to take for ``bind_queue`` / ``bind_exchange``.

        Bindings have no passive variant, and creating one is a WRITE, so
        they are performed only under ``AUTO_DECLARE``. ``PASSIVE_ONLY``
        means the topology is infrastructure-managed: rabbitkit must never
        mutate it (R-D8 — it used to create missing bindings). Whether the
        expected bindings exist can only be checked through the management
        API: see :func:`rabbitkit.core.topology_validation.verify_topology`.
        """
        if self._mode is not TopologyMode.AUTO_DECLARE:
            return TopoAction.SKIP
        return TopoAction.DECLARE


def check_public_topology_write(mode: TopologyMode, operation: str, *, passive: bool = False) -> None:
    """Gate for the brokers' public ``declare_*`` / ``bind_*`` API (16.6).

    Only ``AUTO_DECLARE`` lets the application create topology.
    ``PASSIVE_ONLY`` (infrastructure-managed) refuses every write — a
    ``passive=True`` entity is still allowed, since a passive declare only
    checks existence. ``MANUAL`` refuses too: there rabbitkit touches no
    topology at all, and silently skipping an explicit call would be worse.

    Raises:
        ConfigurationError: the call would write under a mode that forbids it.
    """
    if mode is TopologyMode.AUTO_DECLARE:
        return
    if mode is TopologyMode.PASSIVE_ONLY and passive:
        return
    raise ConfigurationError(
        f"{operation} refused: RabbitConfig.topology_mode={mode.value!r} does not allow rabbitkit to "
        "write topology. Use TopologyMode.AUTO_DECLARE for application-managed topology, or provision it "
        "with your infrastructure tooling (and verify it with broker.verify_topology(management_client=...))."
    )
