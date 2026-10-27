"""Tests for core/topology_validation.py — read-only topology compatibility
checks for infrastructure-managed (PASSIVE_ONLY) topology (R-D8)."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from rabbitkit.core.config import RetryConfig
from rabbitkit.core.errors import ConfigurationError
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.topology_validation import expected_topology, log_passive_only_limits, verify_topology
from rabbitkit.core.types import ExchangeType, PreflightStatus, QueueType, TopologyMode


class _HTTP404(Exception):
    code = 404


class _Mgmt:
    def __init__(
        self,
        queues: dict[str, dict[str, Any]],
        exchanges: dict[str, dict[str, Any]] | None = None,
        bindings: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        self.queues = queues
        self.exchanges = exchanges or {}
        self.bindings = bindings or {}
        self.calls: list[str] = []

    def get_queue(self, name: str, vhost: str = "/") -> dict[str, Any]:
        self.calls.append(f"GET queue {name}")
        if name not in self.queues:
            raise _HTTP404(name)
        return self.queues[name]

    def get_exchange(self, name: str, vhost: str = "/") -> dict[str, Any]:
        self.calls.append(f"GET exchange {name}")
        if name not in self.exchanges:
            raise _HTTP404(name)
        return self.exchanges[name]

    def get_queue_bindings(self, queue: str, vhost: str = "/") -> list[dict[str, Any]]:
        self.calls.append(f"GET bindings {queue}")
        return self.bindings.get(queue, [])


def _route(queue: RabbitQueue, exchange: RabbitExchange | None = None, retry: Any = None) -> RouteDefinition:
    return RouteDefinition(
        name=f"r-{queue.name}", queue=queue, exchange=exchange, handler=lambda body: None, retry_override=retry
    )


def _statuses(report: Any) -> dict[tuple[str, str, str], PreflightStatus]:
    return {(f.entity, f.name, f.check): f.status for f in report.findings}


def test_type_mismatch_is_detected() -> None:
    """The audit's p5b: a quorum queue in code, provisioned as classic."""
    mgmt = _Mgmt({"q": {"type": "classic", "durable": True, "arguments": {"x-queue-type": "classic"}}})
    report = verify_topology([_route(RabbitQueue("q", queue_type=QueueType.QUORUM))], management_client=mgmt)
    assert _statuses(report)[("queue", "q", "type")] is PreflightStatus.FAILED
    assert not report.ok
    with pytest.raises(ConfigurationError, match="expected quorum, broker has classic"):
        report.raise_for_failures()


def test_matching_queue_and_arguments_verify() -> None:
    q = RabbitQueue("q", queue_type=QueueType.QUORUM, delivery_limit=5, message_ttl=1000)
    mgmt = _Mgmt(
        {
            "q": {
                "type": "quorum",
                "durable": True,
                "arguments": {"x-queue-type": "quorum", "x-delivery-limit": 5},
                "effective_policy_definition": {"message-ttl": 1000},
            }
        }
    )
    report = verify_topology([_route(q)], management_client=mgmt)
    assert report.fully_verified, report.as_dict()


def test_argument_mismatch_and_missing() -> None:
    q = RabbitQueue("q", max_length=10, message_ttl=5)
    mgmt = _Mgmt({"q": {"type": "classic", "durable": True, "arguments": {"x-max-length": 99}}})
    s = _statuses(verify_topology([_route(q)], management_client=mgmt))
    assert s[("queue", "q", "argument:x-max-length")] is PreflightStatus.FAILED
    assert s[("queue", "q", "argument:x-message-ttl")] is PreflightStatus.FAILED


def test_durability_mismatch() -> None:
    mgmt = _Mgmt({"q": {"type": "classic", "durable": False, "arguments": {}}})
    s = _statuses(verify_topology([_route(RabbitQueue("q"))], management_client=mgmt))
    assert s[("queue", "q", "durable")] is PreflightStatus.FAILED


def test_missing_queue_exchange_and_binding() -> None:
    ex = RabbitExchange("ex", type=ExchangeType.TOPIC)
    q = RabbitQueue("q", routing_key="orders.#")
    mgmt = _Mgmt({}, {"ex": {"type": "direct", "durable": True}})
    s = _statuses(verify_topology([_route(q, ex)], management_client=mgmt))
    assert s[("queue", "q", "exists")] is PreflightStatus.FAILED
    assert s[("exchange", "ex", "type")] is PreflightStatus.FAILED
    assert s[("binding", "ex->q", "routing_key=orders.#")] is PreflightStatus.FAILED


def test_present_binding_verifies() -> None:
    ex = RabbitExchange("ex", type=ExchangeType.TOPIC)
    q = RabbitQueue("q", routing_key="orders.#")
    mgmt = _Mgmt(
        {"q": {"type": "classic", "durable": True, "arguments": {}}},
        {"ex": {"type": "topic", "durable": True}},
        {"q": [{"source": "", "routing_key": "q"}, {"source": "ex", "routing_key": "orders.#"}]},
    )
    assert verify_topology([_route(q, ex)], management_client=mgmt).fully_verified


def test_retry_route_needs_dead_letter_path_and_retry_queues() -> None:
    q = RabbitQueue("q")
    retry = RetryConfig(max_retries=1, delays=(1,))
    mgmt = _Mgmt({"q": {"type": "classic", "durable": True, "arguments": {}}, "q.retry.1": {"type": "classic"}})
    s = _statuses(verify_topology([_route(q, retry=retry)], management_client=mgmt))
    assert s[("queue", "q", "dead-letter-path")] is PreflightStatus.FAILED
    assert s[("queue", "q.retry.1", "type")] is PreflightStatus.VERIFIED
    assert s[("queue", "q.dlq", "exists")] is PreflightStatus.FAILED


def test_dead_letter_path_from_policy_counts() -> None:
    q = RabbitQueue("q")
    retry = RetryConfig(max_retries=1, delays=(1,))
    mgmt = _Mgmt(
        {
            "q": {
                "type": "classic",
                "durable": True,
                "arguments": {},
                "effective_policy_definition": {"dead-letter-exchange": "dlx"},
            },
            "q.retry.1": {"type": "classic"},
            "q.dlq": {"type": "classic"},
        }
    )
    assert verify_topology([_route(q, retry=retry)], management_client=mgmt).ok


def test_lookup_error_is_unverified_not_failed() -> None:
    class _Broken(_Mgmt):
        def get_queue(self, name: str, vhost: str = "/") -> dict[str, Any]:
            raise TimeoutError("mgmt down")

    report = verify_topology([_route(RabbitQueue("q"))], management_client=_Broken({}))
    assert report.ok and not report.fully_verified


def test_without_management_client_everything_is_unverified() -> None:
    ex = RabbitExchange("ex")
    report = verify_topology(
        [_route(RabbitQueue("q", routing_key="k"), ex, retry=RetryConfig(max_retries=1, delays=(1,)))]
    )
    assert report.ok and not report.fully_verified
    assert all("management API" in f.detail for f in report.unverified)
    assert {f.entity for f in report.unverified} == {"queue", "exchange", "binding"}


def test_only_reads_are_made() -> None:
    mgmt = _Mgmt({"q": {"type": "classic", "durable": True, "arguments": {}}})
    verify_topology([_route(RabbitQueue("q"), RabbitExchange("ex"))], management_client=mgmt)
    assert all(c.startswith("GET ") for c in mgmt.calls)


def test_expected_topology_collects_retry_auxiliaries() -> None:
    exp = expected_topology([_route(RabbitQueue("q"), retry=RetryConfig(max_retries=2, delays=(1, 2)))])
    assert dict(exp.auxiliary) == {"q.retry.1": "classic", "q.retry.2": "classic", "q.dlq": "classic"}
    assert exp.needs_dead_letter == ("q",)


def test_passive_only_notice(caplog: pytest.LogCaptureFixture) -> None:
    routes = [_route(RabbitQueue("q"))]
    with caplog.at_level(logging.WARNING):
        log_passive_only_limits(TopologyMode.AUTO_DECLARE, routes)
        assert caplog.records == []
        log_passive_only_limits(TopologyMode.PASSIVE_ONLY, routes)
    assert "verify_topology" in caplog.text and "no topology writes" in caplog.text
