"""Regression tests for C1 — config-driven retry must actually retry.

Before the fix, ``retry=RetryConfig(...)`` declared the delay/DLQ *topology* but
never installed ``RetryMiddleware`` into the route's middleware chain, so:

- a transient failure ``nack(requeue=True)``'d in a hot loop instead of being
  routed to a delay queue, and the delay queues never received anything;
- ``max_retries`` was never enforced.

These tests drive the behaviour through ``TestBroker``, which now mirrors the
real broker by auto-installing ``RetryMiddleware`` on retry-enabled routes.
"""

from __future__ import annotations

import asyncio

import pytest

from rabbitkit.core.config import RetryConfig
from rabbitkit.middleware.exception import ExceptionMiddleware
from rabbitkit.middleware.retry import RetryMiddleware, retry_middleware_insertion_index
from rabbitkit.middleware.timeout import TimeoutConfig, TimeoutMiddleware
from rabbitkit.testing.broker import TestBroker

RETRY_HEADER = "x-rabbitkit-retry-count"


def _retry_publishes(broker: TestBroker) -> list:
    """Envelopes the retry middleware published to a delay queue."""
    return [e for e in broker.published_messages if ".retry." in e.routing_key]


# ── the middleware is actually installed ─────────────────────────────────────


def test_config_driven_retry_installs_middleware() -> None:
    broker = TestBroker()

    @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=2, delays=(5, 30)))
    def handle(body: bytes) -> None:  # pragma: no cover - not invoked here
        pass

    broker.start()

    route = broker.routes[0]
    retry_mws = [m for m in route.route_middlewares if isinstance(m, RetryMiddleware)]
    assert len(retry_mws) == 1, "config-driven retry must install exactly one RetryMiddleware"


def test_no_retry_config_installs_no_middleware() -> None:
    broker = TestBroker()

    @broker.subscriber(queue="orders")
    def handle(body: bytes) -> None:  # pragma: no cover - not invoked here
        pass

    broker.start()

    route = broker.routes[0]
    assert not any(isinstance(m, RetryMiddleware) for m in route.route_middlewares)


# ── transient failure → delay queue + ack source ─────────────────────────────


def test_transient_failure_routes_to_delay_queue_and_acks() -> None:
    broker = TestBroker()

    @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=2, delays=(5, 30)))
    def handle(body: bytes) -> None:
        raise TimeoutError("db briefly down")  # transient

    broker.start()
    broker.publish("orders", b'{"id": 1}')

    msg = broker.consumed_messages[0]
    # Source is acked (it is now safely in the delay queue), NOT nack-requeued.
    broker.assert_acked(msg)

    retries = _retry_publishes(broker)
    assert len(retries) == 1, "transient failure must be published to a delay queue"
    assert retries[0].routing_key == "orders.retry.1"
    assert retries[0].headers[RETRY_HEADER] == 1


def test_second_attempt_routes_to_second_delay_queue() -> None:
    broker = TestBroker()

    @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=3, delays=(5, 30, 120)))
    def handle(body: bytes) -> None:
        raise TimeoutError("still down")

    broker.start()
    # Simulate a message that already went through the first delay queue.
    broker.publish("orders", b'{"id": 1}', headers={RETRY_HEADER: 1})

    retries = _retry_publishes(broker)
    assert len(retries) == 1
    assert retries[0].routing_key == "orders.retry.2"
    assert retries[0].headers[RETRY_HEADER] == 2
    broker.assert_acked(broker.consumed_messages[0])


# ── exhaustion → dead-letter (NOT hot loop) ──────────────────────────────────


def test_exhausted_transient_dead_letters_not_requeue() -> None:
    """The core regression: an exhausted *transient* error must reject→DLQ,
    NOT nack(requeue=True) into a hot loop."""
    broker = TestBroker()

    @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=1, delays=(5,)))
    def handle(body: bytes) -> None:
        raise TimeoutError("down for good")  # transient, but retries exhausted

    broker.start()
    # retry-count already at max_retries → next failure is terminal.
    broker.publish("orders", b'{"id": 1}', headers={RETRY_HEADER: 1})

    msg = broker.consumed_messages[0]
    broker.assert_rejected(msg, requeue=False)  # → source-queue DLX → DLQ
    assert _retry_publishes(broker) == [], "exhausted retries must not re-publish to a delay queue"


# ── permanent error → immediate dead-letter, no retries ──────────────────────


def test_permanent_error_dead_letters_immediately() -> None:
    broker = TestBroker()

    @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=3, delays=(5, 30, 120)))
    def handle(body: bytes) -> None:
        raise ValueError("malformed order")  # permanent per classify_error

    broker.start()
    broker.publish("orders", b'{"id": 1}')

    msg = broker.consumed_messages[0]
    broker.assert_rejected(msg, requeue=False)
    assert _retry_publishes(broker) == [], "permanent errors must not be retried"


# ── idempotency: no double-wiring ────────────────────────────────────────────


def test_explicit_retry_middleware_not_double_wired() -> None:
    broker = TestBroker()
    user_mw = RetryMiddleware(RetryConfig(max_retries=2, delays=(5, 30)))

    @broker.subscriber(
        queue="orders",
        retry=RetryConfig(max_retries=2, delays=(5, 30)),
        middlewares=[user_mw],
    )
    def handle(body: bytes) -> None:  # pragma: no cover - not invoked here
        pass

    broker.start()

    route = broker.routes[0]
    retry_mws = [m for m in route.route_middlewares if isinstance(m, RetryMiddleware)]
    assert len(retry_mws) == 1, "must not stack a second RetryMiddleware on a user-supplied one"
    assert retry_mws[0] is user_mw


def test_manual_middleware_without_retry_config_warns() -> None:
    """A RetryMiddleware with no retry topology declared must warn loudly
    (its delay-queue publishes would target non-existent queues)."""
    broker = TestBroker()
    user_mw = RetryMiddleware(RetryConfig(max_retries=2, delays=(5, 30)))

    @broker.subscriber(queue="orders", middlewares=[user_mw])  # NOTE: no retry=
    def handle(body: bytes) -> None:  # pragma: no cover - not invoked here
        pass

    with pytest.warns(RuntimeWarning, match="no retry topology was declared"):
        broker.start()


def test_retry_wiring_idempotent_across_restart() -> None:
    broker = TestBroker()

    @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=1, delays=(5,)))
    def handle(body: bytes) -> None:  # pragma: no cover - not invoked here
        pass

    broker.start()
    broker.start()  # second start must not stack another middleware

    route = broker.routes[0]
    assert sum(isinstance(m, RetryMiddleware) for m in route.route_middlewares) == 1


# ── async parity ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_transient_failure_routes_to_delay_queue_async() -> None:
    broker = TestBroker()

    @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=2, delays=(5, 30)))
    async def handle(body: bytes) -> None:
        raise TimeoutError("db briefly down")

    broker.start()
    await broker.publish_async("orders", b'{"id": 1}')

    msg = broker.consumed_messages[0]
    broker.assert_acked(msg)
    retries = _retry_publishes(broker)
    assert len(retries) == 1
    assert retries[0].routing_key == "orders.retry.1"


@pytest.mark.asyncio
async def test_exhausted_transient_dead_letters_async() -> None:
    broker = TestBroker()

    @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=1, delays=(5,)))
    async def handle(body: bytes) -> None:
        raise TimeoutError("down for good")

    broker.start()
    await broker.publish_async("orders", b'{"id": 1}', headers={RETRY_HEADER: 1})

    msg = broker.consumed_messages[0]
    broker.assert_rejected(msg, requeue=False)
    assert _retry_publishes(broker) == []


# ── middleware ordering: retry outer of ordinary middlewares, inner of
# ── ExceptionMiddleware (see retry_middleware_insertion_index docstring) ─────


def test_insertion_index_is_zero_with_no_leading_exception_middleware() -> None:
    assert retry_middleware_insertion_index([]) == 0
    assert retry_middleware_insertion_index([TimeoutMiddleware()]) == 0


def test_insertion_index_skips_leading_exception_middleware() -> None:
    assert retry_middleware_insertion_index([ExceptionMiddleware()]) == 1
    assert retry_middleware_insertion_index([ExceptionMiddleware(), TimeoutMiddleware()]) == 1


def test_insertion_index_stops_at_first_non_exception_middleware() -> None:
    """Only a *leading* run of ExceptionMiddleware counts — one after a
    non-ExceptionMiddleware is not "outermost" and must not be skipped past."""
    assert retry_middleware_insertion_index([TimeoutMiddleware(), ExceptionMiddleware()]) == 0


@pytest.mark.asyncio
async def test_retry_catches_timeout_exception_when_auto_wired() -> None:
    """Retry must be OUTER of TimeoutMiddleware so it can retry a timeout.

    This is the documented composition from middleware/timeout.py
    (``middlewares=[retry_mw, timeout_mw]  # retry outermost``) — a config-driven
    retry= must produce the same effective ordering when a user adds their own
    TimeoutMiddleware via middlewares=[...].
    """
    broker = TestBroker()
    timeout_mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.01))

    @broker.subscriber(
        queue="jobs",
        retry=RetryConfig(max_retries=2, delays=(5, 30)),
        middlewares=[timeout_mw],
    )
    async def run_job(body: bytes) -> None:
        await asyncio.sleep(1.0)  # exceeds the 0.01s timeout

    broker.start()

    route = broker.routes[0]
    assert isinstance(route.route_middlewares[0], RetryMiddleware), "retry must be outer of timeout"
    assert route.route_middlewares[1] is timeout_mw

    await broker.publish_async("jobs", b"{}")

    # HandlerTimeoutError is a TimeoutError subclass → TRANSIENT → retry engages
    # (proves retry actually saw the exception, i.e. it wraps timeout).
    msg = broker.consumed_messages[0]
    broker.assert_acked(msg)
    retries = _retry_publishes(broker)
    assert len(retries) == 1
    assert retries[0].routing_key == "jobs.retry.1"


@pytest.mark.asyncio
async def test_exception_middleware_catches_terminal_exception_from_retry() -> None:
    """ExceptionMiddleware must be OUTER of retry so it sees terminal failures.

    Matches middleware/exception.py: "Outermost middleware. Catches exceptions
    after retry gives up." A config-driven retry= must insert itself INSIDE a
    user-supplied ExceptionMiddleware, not outside it.
    """
    broker = TestBroker()
    exc_mw = ExceptionMiddleware(swallow_permanent=True)

    @broker.subscriber(
        queue="orders",
        retry=RetryConfig(max_retries=1, delays=(5,)),
        middlewares=[exc_mw],
    )
    async def handle(body: bytes) -> None:
        raise ValueError("malformed order")  # permanent -> terminal immediately

    broker.start()

    route = broker.routes[0]
    assert route.route_middlewares[0] is exc_mw, "ExceptionMiddleware must stay outermost"
    assert isinstance(route.route_middlewares[1], RetryMiddleware)

    # ExceptionMiddleware(swallow_permanent=True) swallows the terminal exception
    # and returns None instead of re-raising -> the pipeline sees a normal
    # (non-exception) return and acks, proving the terminal exception reached
    # ExceptionMiddleware rather than being caught only by the AckPolicy.
    await broker.publish_async("orders", b'{"id": 1}')
    msg = broker.consumed_messages[0]
    broker.assert_acked(msg)
