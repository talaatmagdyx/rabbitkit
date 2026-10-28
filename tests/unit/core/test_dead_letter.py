"""Tests for core/dead_letter.py — DeadLetterGuard."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from rabbitkit.core.dead_letter import DeadLetterCopier, DeadLetterGuard
from rabbitkit.core.errors import mark_invalid_message
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.sanitizer import ErrorSanitizer
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus


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


# ── DeadLetterCopier ──────────────────────────────────────────────


class _Publisher:
    def __init__(self, *results: object) -> None:
        self.results = list(results)
        self.sent: list[MessageEnvelope] = []

    async def __call__(self, envelope: MessageEnvelope) -> PublishOutcome:
        self.sent.append(envelope)
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        assert isinstance(result, PublishStatus)
        return PublishOutcome(status=result, reason="returned" if result is PublishStatus.RETURNED else None)


def _failed_message(**kw: object) -> RabbitMessage:
    params: dict[str, object] = {
        "body": b"\x1f\x8bnot-gzip",
        "headers": {"x-rabbitkit-original-queue": "orders", "x-tenant": "acme"},
        "message_id": "m-1",
        "content_encoding": "gzip",
        "exchange": "events",
        "routing_key": "orders.created",
        "delivery_tag": 7,
    }
    params.update(kw)
    return RabbitMessage(**params)  # type: ignore[arg-type]


class TestDeadLetterCopier:
    async def test_confirmed_copy_carries_failure_metadata_and_the_wire_body(self) -> None:
        publish = _Publisher(PublishStatus.CONFIRMED)
        copier = DeadLetterCopier(publish, lambda route: "orders.dlq")
        exc = ValueError("bad gzip password=hunter2")
        mark_invalid_message(exc)
        msg = _failed_message()

        assert await copier.copy(_route(RabbitQueue(name="orders")), msg, exc, "permanent") is True

        (env,) = publish.sent
        assert env.routing_key == "orders.dlq" and env.exchange == "" and env.mandatory
        assert env.body == b"\x1f\x8bnot-gzip" and env.content_encoding == "gzip"
        assert env.message_id == "m-1"
        h = env.headers
        assert h["x-rabbitkit-error-category"] == "invalid_message"
        assert h["x-rabbitkit-failure-category"] == "permanent"
        assert h["x-rabbitkit-error-class"] == "ValueError"
        assert "hunter2" not in str(h.get("x-rabbitkit-error-message", ""))
        assert h["x-rabbitkit-original-queue"] == "orders"
        assert h["x-rabbitkit-original-exchange"] == "events"
        assert h["x-rabbitkit-original-routing-key"] == "orders.created"
        assert h["x-rabbitkit-original-message-id"] == "m-1"
        assert h["x-rabbitkit-retry-count"] == 0
        assert h["x-rabbitkit-attempts"] == 1
        assert "x-rabbitkit-first-failed-at" in h and "x-rabbitkit-dead-lettered-at" in h
        assert h["x-tenant"] == "acme"
        assert "x-rabbitkit-retry-count" not in msg.headers  # the delivery is not changed
        assert copier.copied == 1

    async def test_transient_on_redelivery_is_exhausted_with_two_attempts(self) -> None:
        publish = _Publisher(PublishStatus.CONFIRMED)
        copier = DeadLetterCopier(publish, lambda route: "orders.dlq", retry_header_for=lambda route: "x-tries")
        msg = _failed_message(redelivered=True, headers={})

        assert await copier.copy(_route(RabbitQueue(name="orders")), msg, TimeoutError(), "transient_on_redelivery")

        h = publish.sent[0].headers
        assert h["x-rabbitkit-failure-category"] == "exhausted"
        assert h["x-rabbitkit-attempts"] == 2
        assert h["x-rabbitkit-original-queue"] == "orders"  # filled from the route
        assert h["x-tries"] == 0

    async def test_no_addressable_dlq_publishes_nothing(self) -> None:
        publish = _Publisher()
        copier = DeadLetterCopier(publish, lambda route: None)
        assert (
            await copier.copy(_route(RabbitQueue(name="orders")), _failed_message(), ValueError(), "permanent") is False
        )
        assert publish.sent == []

    async def test_unconfirmed_copy_is_a_failure(self) -> None:
        for status in (PublishStatus.RETURNED, PublishStatus.UNKNOWN, PublishStatus.SENT, PublishStatus.NOT_SENT):
            copier = DeadLetterCopier(_Publisher(status), lambda route: "orders.dlq")
            assert await copier.copy(_route(RabbitQueue(name="orders")), _failed_message(), ValueError(), "x") is False
            assert copier.failed == 1

    async def test_raising_publish_is_a_failure(self) -> None:
        copier = DeadLetterCopier(_Publisher(ConnectionError("down")), lambda route: "orders.dlq")
        assert await copier.copy(_route(RabbitQueue(name="orders")), _failed_message(), ValueError(), "x") is False
        assert copier.failed == 1

    async def test_cancellation_propagates(self) -> None:
        copier = DeadLetterCopier(_Publisher(asyncio.CancelledError()), lambda route: "orders.dlq")
        with pytest.raises(asyncio.CancelledError):
            await copier.copy(_route(RabbitQueue(name="orders")), _failed_message(), ValueError(), "x")

    async def test_omit_policy_writes_no_error_text(self) -> None:
        publish = _Publisher(PublishStatus.CONFIRMED)
        copier = DeadLetterCopier(
            publish, lambda route: "orders.dlq", sanitizer_for=lambda route: ErrorSanitizer(policy="omit")
        )
        assert await copier.copy(_route(RabbitQueue(name="orders")), _failed_message(), ValueError("x"), "permanent")
        assert "x-rabbitkit-error-message" not in publish.sent[0].headers
