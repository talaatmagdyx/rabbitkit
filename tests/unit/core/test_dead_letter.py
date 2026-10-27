"""Tests for core/dead_letter.py — DeadLetterGuard (F17)."""

from __future__ import annotations

from unittest.mock import AsyncMock

from rabbitkit.core.dead_letter import DeadLetterGuard
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitQueue


def _route(queue: RabbitQueue) -> RouteDefinition:
    async def handler(body: bytes) -> None:
        return None

    return RouteDefinition(name="r", queue=queue, exchange=None, handler=handler)


class TestTargets:
    def test_recorded_default_exchange_target_is_verifiable(self) -> None:
        guard = DeadLetterGuard(AsyncMock(return_value=True))
        guard.set_target("orders", "", "orders.dlq")
        assert guard.target_for(_route(RabbitQueue(name="orders"))) == "orders.dlq"

    def test_named_dead_letter_exchange_is_not_verifiable(self) -> None:
        guard = DeadLetterGuard(AsyncMock(return_value=True))
        guard.set_target("orders", "dlx", "orders.dlq")
        assert guard.target_for(_route(RabbitQueue(name="orders"))) is None

    def test_falls_back_to_the_queue_declaration(self) -> None:
        guard = DeadLetterGuard(AsyncMock(return_value=True))
        q = RabbitQueue(name="orders", dead_letter_exchange="", dead_letter_routing_key="orders.dead")
        assert guard.target_for(_route(q)) == "orders.dead"
        assert guard.target_for(_route(RabbitQueue(name="plain"))) is None


class TestCheck:
    async def test_unverifiable_target_never_probes(self) -> None:
        probe = AsyncMock(return_value=False)
        guard = DeadLetterGuard(probe)
        assert await guard.check(_route(RabbitQueue(name="plain"))) is True
        probe.assert_not_awaited()

    async def test_positive_answer_is_cached_for_ttl(self) -> None:
        now = [0.0]
        probe = AsyncMock(return_value=True)
        guard = DeadLetterGuard(probe, ttl=5.0, clock=lambda: now[0])
        guard.set_target("orders", "", "orders.dlq")
        route = _route(RabbitQueue(name="orders"))
        assert await guard.check(route) is True
        now[0] = 4.9
        assert await guard.check(route) is True
        assert probe.await_count == 1
        now[0] = 5.1
        assert await guard.check(route) is True
        assert probe.await_count == 2

    async def test_negative_answer_is_never_cached(self) -> None:
        probe = AsyncMock(side_effect=[False, False, True])
        guard = DeadLetterGuard(probe, clock=lambda: 0.0)
        guard.set_target("orders", "", "orders.dlq")
        route = _route(RabbitQueue(name="orders"))
        assert [await guard.check(route) for _ in range(3)] == [False, False, True]
        assert guard.missing == 2 and guard.probes == 3

    async def test_probe_failure_fails_closed(self) -> None:
        guard = DeadLetterGuard(AsyncMock(side_effect=ConnectionError("down")))
        guard.set_target("orders", "", "orders.dlq")
        assert await guard.check(_route(RabbitQueue(name="orders"))) is False

    async def test_redeclaring_the_target_drops_the_cache(self) -> None:
        probe = AsyncMock(side_effect=[True, False])
        guard = DeadLetterGuard(probe, clock=lambda: 0.0)
        guard.set_target("orders", "", "orders.dlq")
        route = _route(RabbitQueue(name="orders"))
        assert await guard.check(route) is True
        guard.set_target("orders", "", "orders.dlq2")
        assert await guard.check(route) is False


class TestBrokerRetainedDeadLetters:
    async def test_quorum_at_least_once_source_is_not_probed(self) -> None:
        from rabbitkit.core.types import DeadLetterStrategy, QueueType

        probe = AsyncMock(return_value=False)
        guard = DeadLetterGuard(probe)
        guard.set_target("orders", "", "orders.dlq")
        q = RabbitQueue(
            name="orders",
            queue_type=QueueType.QUORUM,
            dead_letter_strategy=DeadLetterStrategy.AT_LEAST_ONCE,
            overflow="reject-publish",
        )
        assert await guard.check(_route(q)) is True
        probe.assert_not_awaited()

    async def test_quorum_at_most_once_source_is_still_probed(self) -> None:
        from rabbitkit.core.types import DeadLetterStrategy, QueueType

        probe = AsyncMock(return_value=False)
        guard = DeadLetterGuard(probe)
        guard.set_target("orders", "", "orders.dlq")
        q = RabbitQueue(
            name="orders", queue_type=QueueType.QUORUM, dead_letter_strategy=DeadLetterStrategy.AT_MOST_ONCE
        )
        assert await guard.check(_route(q)) is False
        probe.assert_awaited_once()
