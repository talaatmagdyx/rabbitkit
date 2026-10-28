"""Real-broker tests for the async publish path.

Each test reproduces a publish-path defect fixed in 0.20.0 against a live
RabbitMQ and asserts what the BROKER holds, not only what the client
reports. Every one of them fails on the pre-fix code.

Holding a confirm open: a few tests need a publish whose frames reached the
broker but whose confirm has not arrived yet. They drop incoming
``Basic.Ack`` frames in aiormq for a moment (``_dropping_confirms``); the
message itself really is on the broker, which is the point.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


def _name(prefix: str) -> str:
    return f"rk-transport-{prefix}-{uuid.uuid4().hex[:8]}"


def _config(url: str, *, connection_name: str | None = None, **kw: Any) -> Any:
    from dataclasses import replace

    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    connection = ConnectionConfig.from_url(url)
    if connection_name is not None:
        connection = replace(connection, connection_name=connection_name)
    return RabbitConfig(connection=connection, **kw)


@contextlib.asynccontextmanager
async def _broker(config: Any, **kwargs: Any) -> AsyncIterator[Any]:
    from rabbitkit.async_.broker import AsyncBroker

    broker = AsyncBroker(config, **kwargs)
    await broker.start(install_signal_handlers=False)
    try:
        yield broker
    finally:
        await broker.stop(timeout=5)


async def _declare(url: str, queue: str) -> None:
    import aio_pika

    conn = await aio_pika.connect(url)
    async with conn:
        ch = await conn.channel()
        await ch.declare_queue(queue, durable=True)


def _ready(url: str, queue: str) -> int | None:
    with QueueProbe(url) as probe:
        return probe.ready(queue)


async def _await_ready(url: str, queue: str, expected: int, timeout: float = 10.0) -> int | None:
    deadline = time.monotonic() + timeout
    seen = None
    while time.monotonic() < deadline:
        seen = await asyncio.to_thread(_ready, url, queue)
        if seen == expected:
            return seen
        await asyncio.sleep(0.05)
    return seen


@contextlib.contextmanager
def _dropping_confirms() -> Iterator[None]:
    """Ignore incoming publisher confirms while active (see module docstring).

    Patches ``Channel._confirm_delivery``, which ``_on_confirm_frame`` looks
    up on every Ack/Nack. Patching ``_on_confirm_frame`` itself only works on
    aiormq 7: aiormq 6 (aio-pika 9.x) binds it into a dispatch table when the
    channel's reader starts, so channels already open ignored the patch."""
    from aiormq.channel import Channel

    original = Channel._confirm_delivery

    def _drop(self: Any, delivery_tag: Any, frame: Any) -> None:
        return None

    Channel._confirm_delivery = _drop  # type: ignore[method-assign]
    try:
        yield
    finally:
        Channel._confirm_delivery = original  # type: ignore[method-assign]


def _kill_connections_named(rabbit: dict[str, Any], connection_name: str) -> int:
    """Force-close only THIS test's connections (the container is shared)."""
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    client = RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))
    killed = 0
    deadline = time.monotonic() + 15
    while killed == 0 and time.monotonic() < deadline:
        for connection in client.list_connections():
            props = connection.get("client_properties") or {}
            if props.get("connection_name") != connection_name:
                continue
            with contextlib.suppress(Exception):
                client.close_connection(str(connection["name"]), reason="rabbitkit test")
                killed += 1
        if killed == 0:
            time.sleep(0.2)  # the management API lists new connections after a moment
    return killed


# ── unroutable mandatory publish, batch mode ─────────────────────────────────


@pytest.mark.asyncio
async def test_batch_mode_reports_an_unroutable_mandatory_publish_as_returned(rabbit: dict[str, Any]) -> None:
    """With a batch publisher, an unroutable mandatory publish was
    reported CONFIRMED (batch channels resolve the return as a result)."""
    from rabbitkit.core.config import BatchPublishConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    async with _broker(
        _config(rabbit["url"]), batch_config=BatchPublishConfig(batch_size=10, flush_interval_ms=10)
    ) as broker:
        outcome = await broker.publish(MessageEnvelope(routing_key=_name("unbound"), body=b"x", mandatory=True))
        routed_queue = _name("routed")
        await _declare(rabbit["url"], routed_queue)
        routed = await broker.publish(MessageEnvelope(routing_key=routed_queue, body=b"y", mandatory=True))

    assert outcome.status is PublishStatus.RETURNED
    assert (outcome.reply_code, outcome.reply_text) == (312, "NO_ROUTE")
    assert routed.status is PublishStatus.CONFIRMED


# ── cancelled publishes release their channel ────────────────────────────────


@pytest.mark.asyncio
async def test_cancelled_publishes_do_not_leak_pool_slots(rabbit: dict[str, Any]) -> None:
    """Each cancelled confirmed publish kept its channel; after pool_size
    cancels the next publish waited channel_acquire_timeout, then rebuilt
    the connection."""
    from rabbitkit.core.config import PoolConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    pool_size = 3
    queue = _name("cancel")
    await _declare(rabbit["url"], queue)
    config = _config(rabbit["url"], pool=PoolConfig(channel_pool_size=pool_size, channel_acquire_timeout=3.0))
    async with _broker(config) as broker:
        await broker.publish(MessageEnvelope(routing_key=queue, body=b"warm"))
        conn_pool = broker._transport._conn_pool
        connection = conn_pool._publisher_connection
        pool = conn_pool._publisher_channel_pool
        for _ in range(pool_size):
            task = asyncio.create_task(broker.publish(MessageEnvelope(routing_key=queue, body=b"x")))
            held = pool.in_use_count
            while pool.in_use_count == held:  # cancel only once it holds a channel
                await asyncio.sleep(0)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        assert pool.in_use_count == 0

        start = time.monotonic()
        outcome = await broker.publish(MessageEnvelope(routing_key=queue, body=b"after"))
        took = time.monotonic() - start

        assert outcome.status is PublishStatus.CONFIRMED
        assert took < 1.0, f"next publish waited {took:.2f}s for a leaked slot"
        assert conn_pool._publisher_connection is connection  # never rebuilt
        assert pool.created_count <= pool_size


# ── pool exhaustion ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pool_exhaustion_is_not_sent_and_never_rebuilds_the_connection(rabbit: dict[str, Any]) -> None:
    """exhaustion used to rebuild the publisher connection, closing
    every in-use channel and failing healthy in-flight publishes."""
    from rabbitkit.core.config import PoolConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    queue = _name("exhaust")
    await _declare(rabbit["url"], queue)
    config = _config(rabbit["url"], pool=PoolConfig(channel_pool_size=1, channel_acquire_timeout=0.3))
    async with _broker(config) as broker:
        conn_pool = broker._transport._conn_pool
        connection = conn_pool._publisher_connection
        busy = await conn_pool.acquire_publisher_channel()  # stands in for an in-flight publish

        outcome = await broker.publish(MessageEnvelope(routing_key=queue, body=b"x"))

        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "admission_timeout")
        assert conn_pool._publisher_connection is connection
        assert not busy.is_closed  # the "in-flight" channel was not torn down
        await busy.default_exchange.publish(_aio_message(b"held"), routing_key=queue)
        await conn_pool.release_publisher_channel(busy)
        assert (await broker.publish(MessageEnvelope(routing_key=queue, body=b"y"))).status is PublishStatus.CONFIRMED

    assert await _await_ready(rabbit["url"], queue, 2) == 2  # "held" + "y"; the NOT_SENT one never left


def _aio_message(body: bytes) -> Any:
    import aio_pika

    return aio_pika.Message(body=body)


# ── unknown outcomes ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_connection_lost_after_send_is_unknown_and_never_republished(rabbit: dict[str, Any]) -> None:
    """a connection error after the frame left was retried once after
    recovery; the message reached the queue twice and the call said
    CONFIRMED."""
    from rabbitkit.core.config import PublisherConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    queue = _name("unknown")
    connection_name = _name("conn")
    await _declare(rabbit["url"], queue)
    # The kill waits for the management API to list the connection (stats lag
    # ~5 s), so the confirm wait must outlast it or the outcome races to TIMEOUT.
    publisher = PublisherConfig(confirm_timeout=60.0)
    async with _broker(_config(rabbit["url"], connection_name=connection_name, publisher=publisher)) as broker:
        # Confirms stay dropped until the outcome is in: a persistent message
        # can be visible on the queue before the broker sends its confirm, and
        # a confirm let through after that point would honestly be CONFIRMED.
        with _dropping_confirms():
            task = asyncio.create_task(broker.publish(MessageEnvelope(routing_key=queue, body=b"x")))
            assert await _await_ready(rabbit["url"], queue, 1) == 1  # the frames reached the broker
            assert await asyncio.to_thread(_kill_connections_named, rabbit, connection_name) >= 1
            outcome = await asyncio.wait_for(task, timeout=10)

        assert outcome.status is PublishStatus.UNKNOWN
        assert outcome.reason == "connection_lost"
        assert outcome.is_unknown
        await asyncio.sleep(1.0)  # give a (wrong) hidden retry time to land
        assert await asyncio.to_thread(_ready, rabbit["url"], queue) == 1


@pytest.mark.asyncio
async def test_missing_exchange_is_an_error_published_once(rabbit: dict[str, Any]) -> None:
    """a 404 was treated as a connection error: retried once (two
    publishes, two channels closed), then reported as a generic ERROR."""
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    queue = _name("after404")
    await _declare(rabbit["url"], queue)
    async with _broker(_config(rabbit["url"])) as broker:
        transport = broker._transport
        sends = 0
        original = transport._publish_on_channel_unguarded

        async def counting(*args: Any, **kwargs: Any) -> Any:
            nonlocal sends
            sends += 1
            return await original(*args, **kwargs)

        transport._publish_on_channel_unguarded = counting
        outcome = await broker.publish(MessageEnvelope(exchange=_name("missing"), routing_key="rk", body=b"x"))
        transport._publish_on_channel_unguarded = original

        assert outcome.status is PublishStatus.ERROR
        assert (outcome.reason, outcome.reply_code) == ("exchange_not_found", 404)
        assert sends == 1
        assert (await broker.publish(MessageEnvelope(routing_key=queue, body=b"ok"))).status is PublishStatus.CONFIRMED


# ── byte budget ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_byte_budget_bounds_in_flight_bytes_against_a_real_broker(rabbit: dict[str, Any]) -> None:
    from rabbitkit.core.config import PoolConfig, PublisherConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    queue = _name("bytes")
    await _declare(rabbit["url"], queue)
    budget = 4096
    config = _config(
        rabbit["url"],
        publisher=PublisherConfig(max_message_bytes=1024, max_in_flight_bytes=budget),
        pool=PoolConfig(channel_pool_size=16, channel_acquire_timeout=10.0),
    )
    async with _broker(config) as broker:
        limiter = broker._transport.byte_limiter
        peak = 0
        original_acquire = limiter.acquire

        async def tracking(n: int, timeout: float | None) -> bool:
            nonlocal peak
            ok = await original_acquire(n, timeout)
            peak = max(peak, limiter.used)
            return bool(ok)

        limiter.acquire = tracking
        outcomes = await asyncio.gather(
            *[broker.publish(MessageEnvelope(routing_key=queue, body=b"x" * 1024)) for _ in range(40)]
        )
        assert all(o.status is PublishStatus.CONFIRMED for o in outcomes)
        assert peak <= budget
        assert limiter.used == 0

        with _dropping_confirms():  # hold the whole budget, then one more must wait and give up
            held = [
                asyncio.create_task(broker.publish(MessageEnvelope(routing_key=queue, body=b"h" * 1024), timeout=1.0))
                for _ in range(4)
            ]
            await asyncio.sleep(0.2)
            assert limiter.used == budget
            blocked = await broker.publish(MessageEnvelope(routing_key=queue, body=b"z"), timeout=0.2)
            assert (blocked.status, blocked.reason) == (PublishStatus.NOT_SENT, "admission_timeout")
            held_outcomes = await asyncio.gather(*held)
        assert all(o.status is PublishStatus.TIMEOUT for o in held_outcomes)
        assert limiter.used == 0  # released on the timeout path too


# ── publisher shutdown ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_after_stop_raises_and_does_not_reconnect(rabbit: dict[str, Any]) -> None:
    """A publish after stop() reconnected silently and was CONFIRMED."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.errors import BrokerNotStartedError
    from rabbitkit.core.types import MessageEnvelope

    queue = _name("afterstop")
    await _declare(rabbit["url"], queue)
    broker = AsyncBroker(_config(rabbit["url"]))
    await broker.start(install_signal_handlers=False)
    await broker.stop()

    with pytest.raises(BrokerNotStartedError):
        await broker.publish(MessageEnvelope(routing_key=queue, body=b"x"))
    assert not broker._transport.is_connected()
    assert await asyncio.to_thread(_ready, rabbit["url"], queue) == 0


@pytest.mark.asyncio
async def test_stop_drains_in_flight_publishes(rabbit: dict[str, Any]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import PoolConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    queue = _name("drain")
    await _declare(rabbit["url"], queue)
    broker = AsyncBroker(_config(rabbit["url"], pool=PoolConfig(channel_pool_size=16)))
    await broker.start(install_signal_handlers=False)
    tasks = [asyncio.create_task(broker.publish(MessageEnvelope(routing_key=queue, body=b"x"))) for _ in range(100)]
    await asyncio.sleep(0)  # all admitted, most still waiting for a channel or a confirm
    await broker.stop(timeout=10)
    outcomes = await asyncio.gather(*tasks)

    assert all(o.status is PublishStatus.CONFIRMED for o in outcomes), {o.status for o in outcomes}
    assert await _await_ready(rabbit["url"], queue, 100) == 100


@pytest.mark.asyncio
async def test_stop_reports_unconfirmed_publishes_unknown_within_the_deadline(rabbit: dict[str, Any]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import PoolConfig, PublisherConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    queue = _name("stopunknown")
    await _declare(rabbit["url"], queue)
    broker = AsyncBroker(
        _config(
            rabbit["url"],
            pool=PoolConfig(channel_pool_size=1, channel_acquire_timeout=30.0),
            publisher=PublisherConfig(confirm_timeout=30.0),
        )
    )
    await broker.start(install_signal_handlers=False)
    with _dropping_confirms():
        on_wire = asyncio.create_task(broker.publish(MessageEnvelope(routing_key=queue, body=b"x")))
        assert await _await_ready(rabbit["url"], queue, 1) == 1
        waiting = asyncio.create_task(broker.publish(MessageEnvelope(routing_key=queue, body=b"y")))
        await asyncio.sleep(0.1)
        start = time.monotonic()
        await broker.stop(timeout=0.5)
        took = time.monotonic() - start
    a, b = await asyncio.gather(on_wire, waiting)

    assert took < 5.0, f"stop() took {took:.1f}s"
    assert (a.status, a.reason) == (PublishStatus.UNKNOWN, "shutdown")
    assert (b.status, b.reason) == (PublishStatus.NOT_SENT, "publisher_closed")
    assert await asyncio.to_thread(_ready, rabbit["url"], queue) == 1  # y never left the client


@pytest.mark.asyncio
async def test_batch_stop_resolves_every_caller(rabbit: dict[str, Any]) -> None:
    """batch callers still queued at stop() got a bare RuntimeError."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import BatchPublishConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    queue = _name("batchstop")
    await _declare(rabbit["url"], queue)
    broker = AsyncBroker(
        _config(rabbit["url"]), batch_config=BatchPublishConfig(batch_size=10, flush_interval_ms=10, flush_workers=1)
    )
    await broker.start(install_signal_handlers=False)
    with _dropping_confirms():
        tasks = [asyncio.create_task(broker.publish(MessageEnvelope(routing_key=queue, body=b"x"))) for _ in range(30)]
        await asyncio.sleep(0.3)
        await broker.stop(timeout=0.5)
    outcomes = await asyncio.gather(*tasks)

    statuses = {o.status for o in outcomes}
    assert statuses <= {PublishStatus.UNKNOWN, PublishStatus.NOT_SENT}, statuses
    unknown = sum(o.status is PublishStatus.UNKNOWN for o in outcomes)
    assert unknown >= 1
    on_broker = await asyncio.to_thread(_ready, rabbit["url"], queue)
    assert on_broker is not None and on_broker <= unknown  # NOT_SENT never reached the broker
