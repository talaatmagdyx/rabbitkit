"""Tests for core/topology_dispatch.py — TopologyDispatcher mode resolution."""

from __future__ import annotations

import pytest

from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.topology_dispatch import TopoAction, TopologyDispatcher
from rabbitkit.core.types import ExchangeType, TopologyMode

# ── exchange_action ──────────────────────────────────────────────────────


class TestExchangeAction:
    def test_manual_skips(self) -> None:
        d = TopologyDispatcher(TopologyMode.MANUAL)
        assert d.exchange_action(RabbitExchange(name="x")) is TopoAction.SKIP

    def test_auto_declare_active(self) -> None:
        d = TopologyDispatcher(TopologyMode.AUTO_DECLARE)
        assert d.exchange_action(RabbitExchange(name="x")) is TopoAction.DECLARE

    def test_passive_only_forces_passive(self) -> None:
        d = TopologyDispatcher(TopologyMode.PASSIVE_ONLY)
        assert d.exchange_action(RabbitExchange(name="x")) is TopoAction.PASSIVE

    def test_entity_passive_forces_passive_under_auto(self) -> None:
        d = TopologyDispatcher(TopologyMode.AUTO_DECLARE)
        ex = RabbitExchange(name="x", passive=True)
        assert d.exchange_action(ex) is TopoAction.PASSIVE

    def test_entity_non_passive_declares_under_auto(self) -> None:
        d = TopologyDispatcher(TopologyMode.AUTO_DECLARE)
        ex = RabbitExchange(name="x", passive=False, type=ExchangeType.FANOUT)
        assert d.exchange_action(ex) is TopoAction.DECLARE

    def test_passive_only_overrides_entity_non_passive(self) -> None:
        d = TopologyDispatcher(TopologyMode.PASSIVE_ONLY)
        assert d.exchange_action(RabbitExchange(name="x", passive=False)) is TopoAction.PASSIVE


# ── queue_action ──────────────────────────────────────────────────────────


class TestQueueAction:
    def test_manual_skips(self) -> None:
        d = TopologyDispatcher(TopologyMode.MANUAL)
        assert d.queue_action(RabbitQueue(name="q")) is TopoAction.SKIP

    def test_auto_declare_active(self) -> None:
        d = TopologyDispatcher(TopologyMode.AUTO_DECLARE)
        assert d.queue_action(RabbitQueue(name="q")) is TopoAction.DECLARE

    def test_passive_only_forces_passive(self) -> None:
        d = TopologyDispatcher(TopologyMode.PASSIVE_ONLY)
        assert d.queue_action(RabbitQueue(name="q")) is TopoAction.PASSIVE

    def test_entity_passive_forces_passive_under_auto(self) -> None:
        d = TopologyDispatcher(TopologyMode.AUTO_DECLARE)
        assert d.queue_action(RabbitQueue(name="q", passive=True)) is TopoAction.PASSIVE


# ── binding_action ────────────────────────────────────────────────────────


class TestBindingAction:
    def test_manual_skips(self) -> None:
        d = TopologyDispatcher(TopologyMode.MANUAL)
        assert d.binding_action() is TopoAction.SKIP

    def test_auto_declare_declares(self) -> None:
        assert TopologyDispatcher(TopologyMode.AUTO_DECLARE).binding_action() is TopoAction.DECLARE

    def test_passive_only_never_writes_bindings(self) -> None:
        """R-D8: PASSIVE_ONLY used to create missing bindings."""
        assert TopologyDispatcher(TopologyMode.PASSIVE_ONLY).binding_action() is TopoAction.SKIP


# ── mode property ─────────────────────────────────────────────────────────


class TestModeProperty:
    @pytest.mark.parametrize("mode", list(TopologyMode))
    def test_mode_echoed(self, mode: TopologyMode) -> None:
        assert TopologyDispatcher(mode).mode is mode


# ── public declare API gate (16.6) ────────────────────────────────────────


class TestPublicTopologyWriteGate:
    def test_auto_declare_allows(self) -> None:
        from rabbitkit.core.topology_dispatch import check_public_topology_write

        check_public_topology_write(TopologyMode.AUTO_DECLARE, "declare_queue")

    @pytest.mark.parametrize("mode", [TopologyMode.PASSIVE_ONLY, TopologyMode.MANUAL])
    def test_non_owner_modes_refuse_writes(self, mode: TopologyMode) -> None:
        from rabbitkit.core.errors import ConfigurationError
        from rabbitkit.core.topology_dispatch import check_public_topology_write

        with pytest.raises(ConfigurationError, match="refused"):
            check_public_topology_write(mode, "bind_queue")

    def test_passive_only_allows_passive_existence_checks(self) -> None:
        from rabbitkit.core.errors import ConfigurationError
        from rabbitkit.core.topology_dispatch import check_public_topology_write

        check_public_topology_write(TopologyMode.PASSIVE_ONLY, "declare_queue", passive=True)
        with pytest.raises(ConfigurationError):
            check_public_topology_write(TopologyMode.MANUAL, "declare_queue", passive=True)
