"""0.17: stop trusting publisher-set identifiers as capabilities.

Four findings shared one root cause. `message_id`, `correlation_id` and
`reply_to` are all set by the PUBLISHER, and each was used as a trust-bearing
key into a shared store or as a routing destination, with no binding to the
authenticated producer.
"""

from __future__ import annotations

import contextvars
import time

import pytest

from rabbitkit.concurrency import SyncWorkerPool
from rabbitkit.core.config import DeduplicationConfig, WorkerConfig
from rabbitkit.core.pipeline import _DIRECT_REPLY_PREFIX, _reply_to_permitted


class TestReplyToIsNoLongerAnArbitraryWrite:
    """`reply_to` was used verbatim as a routing key on the default exchange.

    A publisher who could reach ONE queue could have that route's handler
    output delivered into ANY queue in the vhost — including queues they have
    no publish rights to, and (with a dedup result store) someone else's
    stored result.
    """

    def test_no_allowlist_permits_everything(self) -> None:
        """Backward compatible: unset means unchanged behaviour."""
        assert _reply_to_permitted("any.queue.at.all", None) is True

    def test_an_allowlisted_destination_is_permitted(self) -> None:
        assert _reply_to_permitted("replies.orders", ("replies.orders",)) is True

    def test_a_destination_outside_the_allowlist_is_refused(self) -> None:
        assert _reply_to_permitted("victim.private.queue", ("replies.orders",)) is False

    def test_a_prefix_pattern_matches(self) -> None:
        assert _reply_to_permitted("replies.tenant-a.42", ("replies.*",)) is True

    def test_a_prefix_pattern_does_not_over_match(self) -> None:
        assert _reply_to_permitted("other.tenant-a", ("replies.*",)) is False

    def test_the_brokers_private_reply_queue_is_always_permitted(self) -> None:
        """`amq.rabbitmq.reply-to` can only reach the channel that asked, so
        it is safe by construction and must not need allowlisting — otherwise
        turning the allowlist on would break every ordinary RPC caller.
        """
        assert _reply_to_permitted(f"{_DIRECT_REPLY_PREFIX}.abc123", ("replies.only",)) is True

    def test_an_empty_allowlist_still_permits_the_private_queue(self) -> None:
        assert _reply_to_permitted(f"{_DIRECT_REPLY_PREFIX}.x", ()) is True

    def test_an_empty_allowlist_refuses_everything_else(self) -> None:
        assert _reply_to_permitted("anything", ()) is False


class TestRouteCarriesTheAllowlist:
    def test_the_subscriber_decorator_accepts_it(self) -> None:
        from rabbitkit.testing import TestBroker

        broker = TestBroker()

        @broker.subscriber(queue="q-allow", reply_to_allow=("replies.*",))
        def handler(body: bytes) -> None:
            return None

        route = next(r for r in broker.routes if r.queue.name == "q-allow")
        assert route.reply_to_allow == ("replies.*",)

    def test_it_defaults_to_none(self) -> None:
        from rabbitkit.testing import TestBroker

        broker = TestBroker()

        @broker.subscriber(queue="q-default")
        def handler(body: bytes) -> None:
            return None

        route = next(r for r in broker.routes if r.queue.name == "q-default")
        assert route.reply_to_allow is None

    def test_the_unbounded_warning_fires_only_once_per_route(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A busy queue must not flood the log with the same advisory."""
        import logging

        from rabbitkit.core.route import RouteRuntimeState

        state = RouteRuntimeState()
        with caplog.at_level(logging.WARNING):
            for _ in range(5):
                state.warn_unbounded_reply_to("orders", "attacker.queue")
        warnings = [r for r in caplog.records if "one-hop write" in r.getMessage()]
        assert len(warnings) == 1


class TestInFlightRequeueIsThrottled:
    """One duplicate drove ~3,160 redeliveries/second for up to 300s."""

    def test_a_delay_is_configured_by_default(self) -> None:
        assert DeduplicationConfig().in_flight_requeue_delay > 0

    def test_the_old_behaviour_is_still_reachable(self) -> None:
        """Explicit opt-out for anyone who measured and wants it."""
        assert DeduplicationConfig(in_flight_requeue_delay=0).in_flight_requeue_delay == 0

    def test_the_default_bounds_the_redelivery_rate(self) -> None:
        """0.5s per requeue caps a single duplicate at ~2/s rather than
        ~3,160/s — four orders of magnitude off the measured spin."""
        delay = DeduplicationConfig().in_flight_requeue_delay
        assert 1.0 / delay < 10, "the delay must bound the loop to single-digit redeliveries/sec"


class TestSyncWorkerPoolIsolatesMessages:
    """A pooled thread carries ONE context for its whole life, so message A's
    ContextVar writes leaked into message B. With DI's `set_local`, tenant B
    could read tenant A's scope."""

    @pytest.mark.parametrize("worker_count", [1, 2])
    def test_a_contextvar_set_by_one_message_does_not_leak(self, worker_count: int) -> None:
        tenant: contextvars.ContextVar[str] = contextvars.ContextVar("tenant", default="<unset>")
        seen: list[tuple[str, str]] = []

        def work(label: str) -> None:
            if label == "A":
                tenant.set("TENANT-A")
            seen.append((label, tenant.get()))

        pool = SyncWorkerPool(WorkerConfig(worker_count=worker_count))
        pool.start()
        try:
            pool.submit(work, "A")  # type: ignore[arg-type]
            time.sleep(0.15)
            pool.submit(work, "B")  # type: ignore[arg-type]
            time.sleep(0.15)
        finally:
            pool.stop()

        assert len(seen) == 2
        assert seen[0] == ("A", "TENANT-A")
        assert seen[1] == ("B", "<unset>"), "message B must not observe message A's context"

    def test_worker_count_one_runs_inline_and_is_still_isolated(self) -> None:
        """worker_count=1 bypasses the pool and runs on the transport's owner
        thread, which lives for the whole process — so it needed the same
        treatment, not just the pooled path."""
        marker: contextvars.ContextVar[int] = contextvars.ContextVar("marker", default=0)
        observed: list[int] = []

        def work(value: int) -> None:
            observed.append(marker.get())
            marker.set(value)

        pool = SyncWorkerPool(WorkerConfig(worker_count=1))
        pool.start()
        try:
            for value in (1, 2, 3):
                pool.submit(work, value)  # type: ignore[arg-type]
                time.sleep(0.1)
        finally:
            pool.stop()

        assert observed == [0, 0, 0], "each message must start from a clean context"


class TestThePipelineRefusesADisallowedReplyTo:
    """End-to-end: the handler runs, but its result is not published to a
    destination the route did not authorise."""

    def _broker_with(self, allow: tuple[str, ...]) -> tuple[object, list[str]]:
        from rabbitkit.testing import TestBroker

        broker = TestBroker()
        ran: list[str] = []

        @broker.subscriber(queue="rt-guard", reply_to_allow=allow)
        def handler(body: bytes) -> bytes:
            ran.append(body.decode())
            return b"SENSITIVE-RESULT"

        return broker, ran

    def test_a_disallowed_destination_drops_the_result(self) -> None:
        """Asserts on BEHAVIOUR, not the log line: the pipeline logs through
        structlog, which caplog cannot capture."""
        broker, ran = self._broker_with(("replies.only",))
        broker.publish(  # type: ignore[attr-defined]
            "rt-guard", b"req", reply_to="victim.private.queue"
        )
        assert ran == ["req"], "the handler must still run"
        assert not any(
            b"SENSITIVE-RESULT" in getattr(m, "body", b"")
            for m in broker.published_messages  # type: ignore[attr-defined]
        ), "the result must NOT reach a destination the route did not authorise"

    def test_an_allowed_destination_still_receives_the_result(self) -> None:
        broker, ran = self._broker_with(("replies.only",))
        broker.publish("rt-guard", b"req", reply_to="replies.only")  # type: ignore[attr-defined]
        assert ran == ["req"]
        assert any(
            getattr(env, "routing_key", None) == "replies.only"
            for env in broker.published_messages  # type: ignore[attr-defined]
        )
