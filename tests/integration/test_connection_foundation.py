"""Real-broker coverage for the connection foundation (G-08, G-10).

* G-08a  a refused login, vhost or TLS verification fails ``start()`` fast with
         a typed error; the same failure after startup stops the reconnect
         loop as a terminal failure instead of retrying forever
* G-08b  reconnects after a drop back off exponentially and honour a budget
* G-08c  ``credentials_provider`` is re-resolved on every in-place reconnect
* G-08d  reconnect attempts, recovery and terminal failure reach the hooks,
         and the async broker counts ``reconnects_total``
* G-10   a ``start()`` that fails part-way leaks no connection, consumer or
         task, and can be retried

The broker here is this module's own: tests stop its AMQP listener and kill
every connection on it, which would break neighbours on the shared one.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest

from tests.integration.conftest import await_consumers, skip_without_docker
from tests.integration.test_tls import _conn as _tls_conn
from tests.integration.test_tls import _security as _tls_security
from tests.integration.test_tls import tls_rabbit  # noqa: F401 — module fixture reused from the TLS suite

pytestmark = pytest.mark.integration

_PW1 = "pw-1"  # a throwaway user on a throwaway broker


@pytest.fixture(scope="module")
def own_rabbit() -> Iterator[dict[str, Any]]:
    skip_without_docker()
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as container:
        host = container.get_container_host_ip()
        yield {
            "host": host,
            "port": int(container.get_exposed_port(5672)),
            "url": f"amqp://guest:guest@{host}:{container.get_exposed_port(5672)}/",
            "container": container,
        }


def _ctl(rabbit: dict[str, Any], *args: str) -> str:
    result = rabbit["container"].get_wrapped_container().exec_run(["rabbitmqctl", *args])
    output = result[1] if isinstance(result, tuple) else result.output
    return output.decode("utf-8", "replace") if isinstance(output, bytes) else str(output)


async def actl(rabbit: dict[str, Any], *args: str) -> str:
    return await asyncio.to_thread(_ctl, rabbit, *args)


async def _new_user(rabbit: dict[str, Any], password: str = _PW1) -> str:
    user = f"rk-{uuid.uuid4().hex[:8]}"
    await actl(rabbit, "add_user", user, password)
    await actl(rabbit, "set_permissions", "-p", "/", user, ".*", ".*", ".*")
    return user


async def _connections_of(rabbit: dict[str, Any], user: str) -> int:
    out = await actl(rabbit, "list_connections", "user", "--no-table-headers")
    return sum(1 for line in out.splitlines() if line.strip() == user)


async def _wait_until(predicate: Any, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not await predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.2)


def _config(
    rabbit: dict[str, Any], user: str, password: str = _PW1, rabbit_kw: dict[str, Any] | None = None, **conn: Any
) -> Any:
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    conn.setdefault("reconnect_backoff_base", 0.1)
    conn.setdefault("reconnect_backoff_max", 0.5)
    return RabbitConfig(
        connection=ConnectionConfig(
            host=rabbit["host"], port=rabbit["port"], username=user, password=password, socket_timeout=5, **conn
        ),
        **(rabbit_kw or {}),
    )


@contextlib.asynccontextmanager
async def _started(broker: Any) -> AsyncIterator[Any]:
    await broker.start(install_signal_handlers=False)
    try:
        yield broker
    finally:
        async with asyncio.timeout(30):
            await broker.stop()


class _Events:
    """Collects the transport's connection hooks with arrival times."""

    def __init__(self, transport: Any) -> None:
        self.attempts: list[tuple[float, Any]] = []
        self.failures: list[BaseException] = []
        self.reconnects = 0
        transport.on_reconnect_attempt(lambda a: self.attempts.append((time.monotonic(), a)))
        transport.on_connection_failed(self.failures.append)
        transport.on_reconnect(self._reconnected)

    def _reconnected(self) -> None:
        self.reconnects += 1


# ── G-08a: permanent failures at startup ─────────────────────────────────


async def test_wrong_password_fails_start_fast(own_rabbit: dict[str, Any]) -> None:
    """S7: start() used to retry bad credentials 30 times (6-12 min)."""
    from rabbitkit import AsyncBroker, AuthenticationFailedError

    user = await _new_user(own_rabbit)
    broker = AsyncBroker(_config(own_rabbit, user, password="wrong"))
    started = time.monotonic()
    async with asyncio.timeout(20):
        with pytest.raises(AuthenticationFailedError) as caught:
            await broker.start(install_signal_handlers=False)
    assert time.monotonic() - started < 5
    assert caught.value.reason == "authentication_failed"
    assert "wrong" not in str(caught.value) and user not in str(caught.value)
    assert isinstance(caught.value, ConnectionError)
    assert not broker.started
    assert await _connections_of(own_rabbit, user) == 0


async def test_unknown_vhost_fails_start_fast(own_rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, VhostAccessRefusedError

    user = await _new_user(own_rabbit)
    broker = AsyncBroker(_config(own_rabbit, user, vhost="no-such-vhost"))
    started = time.monotonic()
    async with asyncio.timeout(20):
        with pytest.raises(VhostAccessRefusedError, match="no-such-vhost"):
            await broker.start(install_signal_handlers=False)
    assert time.monotonic() - started < 5


@pytest.mark.parametrize("case", ["hostname", "unknown_ca"])
async def test_tls_verification_failure_fails_start_fast(
    tls_rabbit: dict[str, Any],  # noqa: F811 — the imported module fixture
    case: str,
) -> None:
    """F22: SSLCertVerificationError is an OSError and was retried like a refused socket."""
    from rabbitkit import AsyncBroker, TLSVerificationError
    from rabbitkit.core.config import RabbitConfig, SecurityConfig, SSLConfig

    if case == "hostname":
        security = _tls_security(tls_rabbit, server_hostname="wrong.example")
    else:  # no ca_certs: the system trust store does not know the test CA
        security = SecurityConfig(ssl=SSLConfig(enabled=True, server_hostname="localhost"))
    broker = AsyncBroker(RabbitConfig(connection=_tls_conn(tls_rabbit, tls_rabbit["tls_port"]), security=security))
    started = time.monotonic()
    async with asyncio.timeout(20):
        with pytest.raises(TLSVerificationError) as caught:
            await broker.start(install_signal_handlers=False)
    assert time.monotonic() - started < 5
    assert caught.value.reason == "tls_verification_failed"


# ── G-08a/d: permanent failures after startup are terminal ───────────────


async def _assert_terminal(rabbit: dict[str, Any], broker: Any, events: _Events, user: str, expected: type) -> None:
    from rabbitkit import broker_liveness, broker_readiness
    from rabbitkit.core.types import MessageEnvelope

    await _wait_until(lambda: _true(bool(events.failures)), 20, "the terminal failure")
    assert len(events.failures) == 1
    assert isinstance(events.failures[0], expected)
    transport = broker._transport
    assert transport.connection_failure is events.failures[0]
    assert not transport.is_connected()
    assert not broker_readiness(broker)
    assert not broker_liveness(broker)
    # The loop stopped: no attempt after the failure, and nothing connected.
    attempts = len(events.attempts)
    await asyncio.sleep(2)
    assert len(events.attempts) == attempts
    assert await _connections_of(rabbit, user) == 0
    # A publish fails without reconnecting (no new connection with the bad credentials).
    started = time.monotonic()
    outcome = await broker.publish(MessageEnvelope(routing_key="anywhere", body=b"x"))
    assert not outcome.ok
    assert time.monotonic() - started < 2
    assert await _connections_of(rabbit, user) == 0


async def _true(value: bool) -> bool:
    return value


async def test_password_changed_after_start_is_terminal(own_rabbit: dict[str, Any]) -> None:
    """Before: aio-pika retried the stale password forever, every interval."""
    from rabbitkit import AsyncBroker, AuthenticationFailedError

    user = await _new_user(own_rabbit)
    queue = f"q-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(_config(own_rabbit, user))

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        pass

    async with asyncio.timeout(60), _started(broker):
        events = _Events(broker._transport)
        await actl(own_rabbit, "change_password", user, "pw-2")
        await actl(own_rabbit, "close_all_connections", "rotate")
        await _assert_terminal(own_rabbit, broker, events, user, AuthenticationFailedError)


async def test_permissions_revoked_after_start_is_terminal(own_rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, VhostAccessRefusedError

    user = await _new_user(own_rabbit)
    broker = AsyncBroker(_config(own_rabbit, user))
    async with asyncio.timeout(60), _started(broker):
        events = _Events(broker._transport)
        await actl(own_rabbit, "clear_permissions", "-p", "/", user)
        await actl(own_rabbit, "close_all_connections", "revoked")
        await _assert_terminal(own_rabbit, broker, events, user, VhostAccessRefusedError)


async def test_terminal_failure_ends_run_with_the_error(own_rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, AuthenticationFailedError

    user = await _new_user(own_rabbit)
    broker = AsyncBroker(_config(own_rabbit, user))
    async with asyncio.timeout(60):
        run = asyncio.create_task(broker.run())
        await _wait_until(lambda: _true(broker.started), 20, "start")
        await actl(own_rabbit, "change_password", user, "pw-2")
        await actl(own_rabbit, "close_all_connections", "rotate")
        with pytest.raises(AuthenticationFailedError):
            await run
    assert not broker.started


# ── G-08c: credential rotation ───────────────────────────────────────────


async def test_rotated_credentials_are_used_on_in_place_reconnect(own_rabbit: dict[str, Any]) -> None:
    """Before: only rabbitkit-built URLs called credentials_provider; aio-pika's
    in-place reconnect reused the first password and never got back in."""
    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.metrics import MetricsMiddleware

    user = await _new_user(own_rabbit)
    secret = {"password": "pw-1"}
    queue = f"q-{uuid.uuid4().hex[:8]}"
    config = RabbitConfig(
        connection=ConnectionConfig(
            host=own_rabbit["host"],
            port=own_rabbit["port"],
            credentials_provider=lambda: (user, secret["password"]),
            reconnect_backoff_base=0.1,
            reconnect_backoff_max=0.5,
            socket_timeout=5,
        )
    )
    collector = _Counter()
    broker = AsyncBroker(config)
    received: asyncio.Queue[bytes] = asyncio.Queue()

    @broker.subscriber(queue=queue, middlewares=[MetricsMiddleware(collector)])
    async def handle(body: bytes) -> None:
        await received.put(body)

    async with asyncio.timeout(90), _started(broker):
        events = _Events(broker._transport)
        await broker.publish(MessageEnvelope(routing_key=queue, body=b"before"))
        assert await received.get() == b"before"

        secret["password"] = "pw-2"
        await actl(own_rabbit, "change_password", user, "pw-2")
        await actl(own_rabbit, "close_all_connections", "rotate")
        await _wait_until(lambda: _true(broker._transport.is_connected()), 30, "the reconnect")
        await await_consumers(own_rabbit["url"], queue, timeout=30)

        outcome = await broker.publish(MessageEnvelope(routing_key=queue, body=b"after"))
        assert outcome.ok, outcome
        assert await received.get() == b"after"
        assert events.failures == []
        assert events.reconnects >= 2  # both connections reconnected in place
        assert collector.counts.get("rabbitkit_reconnects_total", 0) >= 2


class _Counter:
    """Minimal MetricsCollector: records counter increments."""

    def __init__(self) -> None:
        self.counts: dict[str, float] = {}

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        self.counts[name] = self.counts.get(name, 0.0) + value

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        pass

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        pass


@contextlib.asynccontextmanager
async def _amqp_listener_stopped(rabbit: dict[str, Any]) -> AsyncIterator[None]:
    await actl(rabbit, "stop_app")
    try:
        yield
    finally:
        await actl(rabbit, "start_app")
        await actl(rabbit, "await_startup")


async def test_in_place_reconnect_fails_over_to_the_next_node(
    own_rabbit: dict[str, Any], rabbit_container: dict[str, Any]
) -> None:
    """Before: an in-place reconnect stayed pinned to the node of the first
    connect, so with that node down the consumer never came back."""
    from urllib.parse import urlparse

    import aio_pika

    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, SafetyConfig
    from rabbitkit.core.types import MessageEnvelope, TopologyMode

    queue = f"q-{uuid.uuid4().hex[:8]}"
    other = urlparse(rabbit_container["url"])
    for url in (own_rabbit["url"], rabbit_container["url"]):  # the queue exists on both nodes
        conn = await aio_pika.connect(url)
        async with conn:
            await (await conn.channel()).declare_queue(queue, durable=True)

    config = RabbitConfig(
        connection=ConnectionConfig(
            host=own_rabbit["host"],
            port=own_rabbit["port"],
            nodes=(f"{other.hostname}:{other.port}",),
            reconnect_backoff_base=0.1,
            reconnect_backoff_max=0.5,
            socket_timeout=5,
        ),
        topology_mode=TopologyMode.PASSIVE_ONLY,
        safety=SafetyConfig(reject_without_dlx="discard"),
    )
    broker = AsyncBroker(config)
    received: asyncio.Queue[bytes] = asyncio.Queue()

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        await received.put(body)

    async with asyncio.timeout(90), _started(broker):
        events = _Events(broker._transport)
        async with _amqp_listener_stopped(own_rabbit):
            await _wait_until(lambda: _true(broker._transport.is_connected()), 30, "the failover")
            await await_consumers(rabbit_container["url"], queue, timeout=30)
            outcome = await broker.publish(MessageEnvelope(routing_key=queue, body=b"on the other node"))
            assert outcome.ok, outcome
            assert await received.get() == b"on the other node"
        assert events.failures == []
        assert any(a.endpoint == f"{own_rabbit['host']}:{own_rabbit['port']}" for _, a in events.attempts)


# ── G-08b: backoff and budget ────────────────────────────────────────────


async def test_reconnect_backoff_grows_while_broker_is_down(
    own_rabbit: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Before: a fixed interval in [base, 2*base], forever."""
    from rabbitkit import AsyncBroker
    from rabbitkit.async_ import reconnect

    monkeypatch.setattr(reconnect, "_jitter", lambda ceiling: ceiling)  # no jitter: exact delays
    user = await _new_user(own_rabbit)
    broker = AsyncBroker(_config(own_rabbit, user, reconnect_backoff_base=0.2, reconnect_backoff_max=1.6))
    queue = f"q-{uuid.uuid4().hex[:8]}"

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        pass

    async with asyncio.timeout(120), _started(broker):
        events = _Events(broker._transport)
        async with _amqp_listener_stopped(own_rabbit):
            await _wait_until(lambda: _true(_max_attempt(events) >= 6), 40, "six failed attempts")
        await _wait_until(lambda: _true(broker._transport.is_connected()), 40, "the reconnect")
        await await_consumers(own_rabbit["url"], queue, timeout=30)
        assert events.failures == []

    # Per connection: the delay doubles from base to the cap, and aio-pika
    # really waits it before the next attempt.
    for role in ("publisher", "consumer"):
        run = [(t, a) for t, a in events.attempts if a.role == role]
        assert [a.attempt for _, a in run][:6] == [1, 2, 3, 4, 5, 6], run
        assert [a.delay for _, a in run][:6] == pytest.approx([0.2, 0.4, 0.8, 1.6, 1.6, 1.6])
        for (t0, a0), (t1, _a1) in itertools.pairwise(run):
            assert t1 - t0 >= a0.delay * 0.9, (role, t1 - t0, a0.delay)


def _max_attempt(events: _Events) -> int:
    roles = ("publisher", "consumer")
    return min(max((a.attempt for _, a in events.attempts if a.role == r), default=0) for r in roles)


async def test_reconnect_budget_exhausted_is_terminal(own_rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, ReconnectBudgetExhaustedError

    user = await _new_user(own_rabbit)
    broker = AsyncBroker(_config(own_rabbit, user, reconnect_budget_attempts=3))
    async with asyncio.timeout(90), _started(broker):
        events = _Events(broker._transport)
        async with _amqp_listener_stopped(own_rabbit):
            await _wait_until(lambda: _true(bool(events.failures)), 30, "the spent budget")
            assert isinstance(events.failures[0], ReconnectBudgetExhaustedError)
            assert events.failures[0].reason == "reconnect_budget_exhausted"
        # The broker came back, but the connection gave up for good.
        await asyncio.sleep(2)
        assert not broker._transport.is_connected()
        assert await _connections_of(own_rabbit, user) == 0


# ── G-10: a failed start() leaks nothing ─────────────────────────────────


async def test_failed_start_leaks_nothing_and_can_be_retried(own_rabbit: dict[str, Any]) -> None:
    """A consume refused by the broker (exclusive consumer elsewhere) after the
    first route's consumer, the batch publisher and both connections were up.
    Before: both connections, the first consumer and the batch workers stayed
    open, and stop() was a no-op."""
    import aio_pika

    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import BatchPublishConfig, SafetyConfig
    from rabbitkit.core.types import TopologyMode

    user = await _new_user(own_rabbit)
    first, second = f"q-{uuid.uuid4().hex[:8]}", f"q-{uuid.uuid4().hex[:8]}"
    holder = await aio_pika.connect(own_rabbit["url"])
    try:
        channel = await holder.channel()
        for name in (first, second):
            await channel.declare_queue(name, durable=True)
        held = await channel.get_queue(second)
        held_tag = await held.consume(_ignore, exclusive=True)

        broker = AsyncBroker(
            _config(
                own_rabbit,
                user,
                rabbit_kw={
                    "topology_mode": TopologyMode.PASSIVE_ONLY,
                    "safety": SafetyConfig(reject_without_dlx="discard"),
                },
            ),
            batch_config=BatchPublishConfig(batch_size=10, flush_workers=2),
        )

        @broker.subscriber(queue=first)
        async def handle_first(body: bytes) -> None:
            pass

        @broker.subscriber(queue=second)
        async def handle_second(body: bytes) -> None:
            pass

        baseline_tasks = len(asyncio.all_tasks())
        async with asyncio.timeout(90):
            for _ in range(3):
                with pytest.raises(Exception, match="exclusive"):
                    await broker.start(install_signal_handlers=False)
                assert not broker.started
                assert await _connections_of(own_rabbit, user) == 0
                await asyncio.sleep(0.5)  # let cancelled tasks finish unwinding
                assert len(asyncio.all_tasks()) == baseline_tasks, asyncio.all_tasks()
                info = await channel.declare_queue(first, passive=True)
                assert info.declaration_result.consumer_count == 0

            await held.cancel(held_tag)
            async with _started(broker):
                assert broker._transport.is_connected()
            assert await _connections_of(own_rabbit, user) == 0
    finally:
        await holder.close()


async def _ignore(_message: Any) -> None:
    return None
