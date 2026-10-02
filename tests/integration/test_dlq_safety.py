"""Real-broker coverage for issues #31-#36, #39 and #40.

Runs against the suite-wide RabbitMQ 3.13 container and a module-scoped 4.1
container: quorum delivery-limit semantics differ by major version, and that
difference is the whole point of #32.

* #31  ``rabbitkit dlq inspect`` shows each message once
* #32  quorum DLQs: what the broker does, what rabbitkit declares, the guard
* #33  replay without a target finds the source queue and terminates
* #34  a raising predicate leaves nothing unacked
* #35  one inspection's channel error doesn't touch another's messages
* #36  replay keeps the original's properties, expiration to the millisecond
* #39  ``is_connected()`` is False while aio-pika reconnects
* #40  a vhost containing ``/`` reaches the right vhost
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe, live_counts, skip_without_docker

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    """The suite-wide 3.13 broker: ``url`` / ``mgmt`` / ``container``."""
    return rabbit_container


@pytest.fixture(scope="module")
def rabbit4() -> Iterator[dict[str, Any]]:
    """A RabbitMQ 4.1 broker, where quorum queues default to a limit of 20."""
    skip_without_docker()
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

    with RabbitMqContainer("rabbitmq:4.1-management-alpine").with_exposed_ports(15672) as container:
        host = container.get_container_host_ip()
        yield {
            "url": f"amqp://guest:guest@{host}:{container.get_exposed_port(5672)}/",
            "mgmt": f"http://{host}:{container.get_exposed_port(15672)}",
            "container": container,
        }


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _mgmt(rabbit: dict[str, Any]) -> Any:
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    return RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))


@contextlib.contextmanager
def _pika(rabbit: dict[str, Any]) -> Iterator[Any]:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(rabbit["url"]))
    try:
        yield conn.channel()
    finally:
        with contextlib.suppress(Exception):
            conn.close()


def _sync_transport(rabbit: dict[str, Any]) -> Any:
    from rabbitkit.core.config import ConnectionConfig
    from rabbitkit.sync.transport import SyncTransport

    transport = SyncTransport(connection_config=ConnectionConfig.from_url(rabbit["url"]))
    transport.connect()
    return transport


async def _async_transport(rabbit: dict[str, Any], **conn_kw: Any) -> Any:
    from rabbitkit.async_.transport import AsyncTransportImpl
    from rabbitkit.core.config import ConnectionConfig

    config = dataclasses.replace(ConnectionConfig.from_url(rabbit["url"]), **conn_kw)
    transport = AsyncTransportImpl(connection_config=config)
    await transport.connect()
    return transport


def _wait_for_stats(rabbit: dict[str, Any], queue: str, timeout: float = 20.0) -> dict[str, Any]:
    """The management record once its first statistics emission has landed."""
    client = _mgmt(rabbit)
    deadline = time.monotonic() + timeout
    while True:
        with contextlib.suppress(Exception):
            info = dict(client.get_queue(queue))
            if "messages" in info:
                return info
        if time.monotonic() > deadline:
            raise AssertionError(f"no statistics for {queue}")
        time.sleep(0.25)


def _ready(rabbit: dict[str, Any], queue: str) -> int | None:
    """Ready count via passive declare. ``rabbitmqctl list_queues`` reports
    quorum queues on 4.1 as 0/0 even when they hold messages."""
    with QueueProbe(rabbit["url"]) as probe:
        return probe.ready(queue)


def _requeue_cycles(ch: Any, queue: str, cycles: int) -> int:
    """basic_get + nack(requeue) up to *cycles* times; how many found a message."""
    survived = 0
    for _ in range(cycles):
        method, _props, _body = ch.basic_get(queue)
        if method is None:
            break
        survived += 1
        ch.basic_nack(method.delivery_tag, requeue=True)
    return survived


# ── #32: the broker behaviour rabbitkit has to work around ─────────────────


@pytest.mark.parametrize("broker", ["rabbit", "rabbit4"])
def test_quorum_requeue_limits_as_measured(broker: str, request: pytest.FixtureRequest) -> None:
    """3.x: no default; -1 drops on the first return. 4.x: default 20; -1 unlimited."""
    rabbit = request.getfixturevalue(broker)
    four = broker == "rabbit4"
    with _pika(rabbit) as ch:
        for args, expected in (({}, 21 if four else 25), ({"x-delivery-limit": -1}, 25 if four else 1)):
            q = _name("limit")
            ch.queue_declare(q, durable=True, arguments={"x-queue-type": "quorum", **args})
            ch.basic_publish("", q, b"x")
            time.sleep(0.2)
            assert _requeue_cycles(ch, q, 25) == expected, (broker, args)
            ch.queue_delete(q)


# ── #32: what rabbitkit declares ───────────────────────────────────────────


def _quorum_retry_broker(rabbit: dict[str, Any], queue: str) -> Any:
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, RetryConfig
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import QueueType
    from rabbitkit.sync.broker import SyncBroker

    broker = SyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(rabbit["url"])))

    @broker.subscriber(
        queue=RabbitQueue(name=queue, queue_type=QueueType.QUORUM), retry=RetryConfig(max_retries=1, delays=(1,))
    )
    def handle(body: bytes) -> None:
        pass

    return broker


def _declare_only(broker: Any) -> None:
    from rabbitkit.sync.transport import SyncTransport

    broker._transport = SyncTransport(connection_config=broker._config.connection)
    broker._transport.connect()
    try:
        broker._declare_topology()
    finally:
        broker._transport.disconnect()


def test_rabbitkit_quorum_dlq_on_4x_survives_repeated_peeks(rabbit4: dict[str, Any]) -> None:
    from rabbitkit.dlq import DLQInspector

    queue = _name("q4")
    _declare_only(_quorum_retry_broker(rabbit4, queue))
    info = _wait_for_stats(rabbit4, f"{queue}.dlq")
    assert info["arguments"].get("x-delivery-limit") == -1

    with _pika(rabbit4) as ch:
        ch.basic_publish("", f"{queue}.dlq", b"failed-order")
    transport = _sync_transport(rabbit4)
    try:
        inspector = DLQInspector(transport, management=_mgmt(rabbit4))
        for _ in range(25):  # more than the 4.x default of 20
            assert [m.body for m in inspector.peek(f"{queue}.dlq")] == [b"failed-order"]
    finally:
        transport.disconnect()
    assert _ready(rabbit4, f"{queue}.dlq") == 1


@pytest.mark.parametrize("consumer_first", [False, True])
def test_a_route_consuming_another_routes_quorum_dlq_starts_on_4x(
    consumer_first: bool, rabbit4: dict[str, Any]
) -> None:
    """Both routes declare orders.dlq. They must agree on x-delivery-limit, or
    the second declaration is a 406 at startup (review finding)."""
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig, RetryConfig
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import QueueType
    from rabbitkit.sync.broker import SyncBroker

    queue = _name("drain")
    broker = SyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(rabbit4["url"])))

    def source() -> None:
        @broker.subscriber(
            queue=RabbitQueue(name=queue, queue_type=QueueType.QUORUM), retry=RetryConfig(max_retries=1, delays=(1,))
        )
        def handle(body: bytes) -> None:
            pass

    def consumer() -> None:
        @broker.subscriber(queue=RabbitQueue(name=f"{queue}.dlq", queue_type=QueueType.QUORUM))
        def drain(body: bytes) -> None:
            pass

    for register in (consumer, source) if consumer_first else (source, consumer):
        register()
    _declare_only(broker)
    _declare_only(broker)  # a restart: everything exists now
    assert _wait_for_stats(rabbit4, f"{queue}.dlq")["arguments"].get("x-delivery-limit") == -1


def test_rabbitkit_quorum_dlq_on_3x_has_no_limit_argument(rabbit: dict[str, Any]) -> None:
    from rabbitkit.dlq import DLQInspector

    queue = _name("q3")
    _declare_only(_quorum_retry_broker(rabbit, queue))
    info = _wait_for_stats(rabbit, f"{queue}.dlq")
    assert "x-delivery-limit" not in info["arguments"]
    transport = _sync_transport(rabbit)
    try:
        DLQInspector(transport, management=_mgmt(rabbit)).peek(f"{queue}.dlq")  # browsable
    finally:
        transport.disconnect()


def test_existing_plain_quorum_dlq_is_left_alone_then_guarded(rabbit4: dict[str, Any]) -> None:
    """An upgrade must not 406 on a DLQ that predates the -1 argument, and
    the inspector must refuse it until the operator applies the policy."""
    from rabbitkit.core.errors import UnsafeToBrowseError
    from rabbitkit.dlq import DLQInspector

    queue = _name("old")
    with _pika(rabbit4) as ch:
        ch.queue_declare(f"{queue}.dlq", durable=True, arguments={"x-queue-type": "quorum"})
        ch.basic_publish("", f"{queue}.dlq", b"kept")
    _declare_only(_quorum_retry_broker(rabbit4, queue))  # no 406

    _wait_for_stats(rabbit4, f"{queue}.dlq")
    transport = _sync_transport(rabbit4)
    try:
        inspector = DLQInspector(transport, management=_mgmt(rabbit4))
        with pytest.raises(UnsafeToBrowseError, match="delivery limit of 20"):
            inspector.peek(f"{queue}.dlq")

        _mgmt(rabbit4).put_policy(
            f"{queue}-unlimited",
            {"pattern": f"^{queue}\\.dlq$", "definition": {"delivery-limit": -1}, "apply-to": "quorum_queues"},
        )
        deadline = time.monotonic() + 20
        while True:
            try:
                assert [m.body for m in inspector.peek(f"{queue}.dlq")] == [b"kept"]
                break
            except UnsafeToBrowseError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)
    finally:
        transport.disconnect()


async def test_async_broker_declares_unlimited_quorum_dlq_on_4x(rabbit4: dict[str, Any]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.async_.transport import AsyncTransportImpl
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import QueueType

    queue = _name("aq4")
    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(rabbit4["url"])))

    @broker.subscriber(queue=RabbitQueue(name=queue, queue_type=QueueType.QUORUM))  # safety DLQ
    async def handle(body: bytes) -> None:
        pass

    broker._transport = AsyncTransportImpl(connection_config=broker._config.connection)
    await broker._transport.connect()
    try:
        await broker._declare_topology()
        await broker._declare_topology()  # second start: exists, not redeclared, no 406
    finally:
        await broker._transport.disconnect()
    info = await asyncio.to_thread(_wait_for_stats, rabbit4, f"{queue}.dlq")
    assert info["arguments"].get("x-delivery-limit") == -1


def test_tripwire_without_management_keeps_the_message(rabbit4: dict[str, Any]) -> None:
    from rabbitkit.core.errors import UnsafeToBrowseError
    from rabbitkit.dlq import DLQInspector

    q = _name("trip")
    with _pika(rabbit4) as ch:
        ch.queue_declare(q, durable=True, arguments={"x-queue-type": "quorum"})
        ch.basic_publish("", q, b"x")
    transport = _sync_transport(rabbit4)
    try:
        inspector = DLQInspector(transport)
        assert len(inspector.peek(q)) == 1  # fresh: no x-delivery-count yet
        with pytest.raises(UnsafeToBrowseError, match="quorum queue"):
            inspector.peek(q)
        with pytest.raises(UnsafeToBrowseError):
            inspector.peek(q)  # refused up front this time
    finally:
        transport.disconnect()
    assert _ready(rabbit4, q) == 1


# ── #31: the CLI shows each message once ───────────────────────────────────


def test_cli_inspect_shows_each_message_once(rabbit: dict[str, Any]) -> None:
    from typer.testing import CliRunner

    from rabbitkit.cli import app

    q = _name("cli")
    with _pika(rabbit) as ch:
        ch.queue_declare(q, durable=True)
        for body in (b"one", b"two", b"three"):
            ch.basic_publish("", q, body)
    result = CliRunner().invoke(app, ["dlq", "inspect", q, "--url", rabbit["url"], "--format", "json"])
    assert result.exit_code == 0, result.output
    assert [m["body_preview"] for m in json.loads(result.output)] == ["one", "two", "three"]
    assert live_counts(rabbit, q) == (3, 0)


def test_cli_inspect_refuses_limited_quorum_queue(rabbit4: dict[str, Any]) -> None:
    from typer.testing import CliRunner

    from rabbitkit.cli import app

    q = _name("cliq")
    with _pika(rabbit4) as ch:
        ch.queue_declare(q, durable=True, arguments={"x-queue-type": "quorum"})
        ch.basic_publish("", q, b"x")
    _wait_for_stats(rabbit4, q)
    mgmt = rabbit4["mgmt"].replace("http://", "http://guest:guest@")
    result = CliRunner().invoke(app, ["dlq", "inspect", q, "--url", rabbit4["url"], "--management-url", mgmt])
    assert result.exit_code == 2
    assert "delivery limit of 20" in result.output
    assert _ready(rabbit4, q) == 1


# ── #33: replay without a target ───────────────────────────────────────────


def _dead_letter_one(rabbit: dict[str, Any], source: str, body: bytes = b"order", **props: Any) -> None:
    """Publish to *source* and reject it into ``<source>.dlq`` via the broker."""
    import pika

    with _pika(rabbit) as ch:
        ch.queue_declare(f"{source}.dlq", durable=True)
        ch.queue_declare(
            source,
            durable=True,
            arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": f"{source}.dlq"},
        )
        ch.basic_publish("", source, body, pika.BasicProperties(**props))
        deadline = time.monotonic() + 10
        while True:
            method, _p, _b = ch.basic_get(source)
            if method is not None:
                break
            assert time.monotonic() < deadline
            time.sleep(0.05)
        ch.basic_reject(method.delivery_tag, requeue=False)
    assert live_counts(rabbit, f"{source}.dlq")[0] == 1


def test_replay_routes_a_broker_dead_lettered_message_to_its_source(rabbit: dict[str, Any]) -> None:
    from rabbitkit.dlq import DLQInspector

    source = _name("src")
    _dead_letter_one(rabbit, source)
    transport = _sync_transport(rabbit)
    try:
        result = DLQInspector(transport).replay(f"{source}.dlq")  # no target, limit=None
    finally:
        transport.disconnect()
    assert int(result) == 1
    assert live_counts(rabbit, source) == (1, 0)
    assert live_counts(rabbit, f"{source}.dlq") == (0, 0)


def test_replay_never_loops_a_message_back_into_the_dlq(rabbit: dict[str, Any]) -> None:
    """A message with no origin anywhere used to be republished into the DLQ
    being drained, forever."""
    from rabbitkit.dlq import DLQInspector

    dlq = _name("orphan.dlq")
    with _pika(rabbit) as ch:
        ch.queue_declare(dlq, durable=True)
        ch.basic_publish("", dlq, b"orphan")
    transport = _sync_transport(rabbit)
    try:
        result = DLQInspector(transport).replay(dlq)
    finally:
        transport.disconnect()
    assert (int(result), result.skipped) == (0, 1)
    assert live_counts(rabbit, dlq) == (1, 0)


# ── #36: replay keeps the original's properties ────────────────────────────


@pytest.mark.parametrize("side", ["sync", "async"])
async def test_replay_preserves_properties(side: str, rabbit: dict[str, Any]) -> None:
    import pika

    from rabbitkit.dlq import DLQInspector

    source = _name(f"props-{side}")
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    # Published straight into the DLQ: dead-lettering itself strips
    # expiration (RabbitMQ moves it to x-death[].original-expiration).
    with _pika(rabbit) as ch:
        ch.queue_declare(source, durable=True)
        ch.queue_declare(f"{source}.dlq", durable=True)
        ch.basic_publish(
            "",
            f"{source}.dlq",
            b"payload",
            pika.BasicProperties(
                headers={"x-rabbitkit-original-queue": source},
                delivery_mode=1,
                expiration="65526",  # int() truncation turned this into 65525
                timestamp=int(ts.timestamp()),
                priority=3,
                correlation_id="corr-1",
            ),
        )
    if side == "sync":
        transport = _sync_transport(rabbit)
        try:
            assert int(DLQInspector(transport).replay(f"{source}.dlq")) == 1
        finally:
            transport.disconnect()
    else:
        transport = await _async_transport(rabbit)
        try:
            assert int(await DLQInspector(transport).replay_async(f"{source}.dlq")) == 1
        finally:
            await transport.disconnect()

    with _pika(rabbit) as ch:
        _method, props, body = ch.basic_get(source, auto_ack=True)
    assert body == b"payload"
    assert props.delivery_mode == 1
    assert props.expiration == "65526"
    assert props.timestamp == int(ts.timestamp())
    assert (props.priority, props.correlation_id) == (3, "corr-1")
    assert props.content_type is None  # was "application/octet-stream"
    if side == "sync":
        assert props.message_id is None  # aiormq always stamps one on async
    assert props.headers == {"x-rabbitkit-original-queue": source}


# ── #34 / #35: held messages and per-inspection channels ───────────────────


async def test_raising_predicate_leaves_nothing_unacked(rabbit: dict[str, Any]) -> None:
    from rabbitkit.dlq import DLQInspector

    q = _name("raise")
    with _pika(rabbit) as ch:
        ch.queue_declare(q, durable=True)
        for i in range(3):
            ch.basic_publish("", q, str(i).encode())
    transport = await _async_transport(rabbit)
    seen: list[bytes] = []

    def predicate(m: Any) -> bool:
        seen.append(m.body)
        if len(seen) == 2:
            raise ValueError("bad predicate")
        return False

    try:
        with pytest.raises(ValueError):
            await DLQInspector(transport).replay_async(q, predicate=predicate, target_queue="nowhere")
        # Nothing stranded on a long-lived channel: all three are ready again
        # while this transport is still connected.
        assert await asyncio.to_thread(live_counts, rabbit, q) == (3, 0)
    finally:
        await transport.disconnect()


async def test_one_inspections_channel_error_spares_another(rabbit: dict[str, Any]) -> None:
    import aio_pika.exceptions

    q = _name("iso")
    with _pika(rabbit) as ch:
        ch.queue_declare(q, durable=True)
        ch.basic_publish("", q, b"held")
    transport = await _async_transport(rabbit)
    try:
        async with transport.inspection_session() as a:
            held = await a.basic_get(q)
            assert held is not None
            with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
                async with transport.inspection_session() as b:
                    await b.basic_get(_name("typo"))  # 404 closes b's channel only
            await held.ack_async()  # a's channel is still alive
        assert await asyncio.to_thread(live_counts, rabbit, q) == (0, 0)
        assert await transport.queue_exists(q) is True
        assert await transport.queue_exists(_name("missing")) is False
    finally:
        await transport.disconnect()


# ── #39: is_connected() during an outage ───────────────────────────────────


async def test_is_connected_is_false_while_reconnecting(rabbit: dict[str, Any]) -> None:
    transport = await _async_transport(rabbit, reconnect_backoff_base=2.0, reconnect_backoff_max=2.0)
    try:
        assert transport.is_connected()
        await asyncio.to_thread(
            rabbit["container"].get_wrapped_container().exec_run,
            ["rabbitmqctl", "close_all_connections", "rabbitkit test outage"],
        )
        deadline = asyncio.get_running_loop().time() + 10
        while transport.is_connected():
            assert asyncio.get_running_loop().time() < deadline, "is_connected() stayed True during the outage"
            await asyncio.sleep(0.05)
        deadline = asyncio.get_running_loop().time() + 30
        while not transport.is_connected():
            assert asyncio.get_running_loop().time() < deadline, "never reconnected"
            await asyncio.sleep(0.1)
    finally:
        await transport.disconnect()


# ── #40: a vhost with a slash in it ────────────────────────────────────────


async def test_vhost_with_a_slash(rabbit: dict[str, Any]) -> None:
    vhost = _name("orders/eu")
    container = rabbit["container"].get_wrapped_container()
    await asyncio.to_thread(container.exec_run, ["rabbitmqctl", "add_vhost", vhost])
    await asyncio.to_thread(
        container.exec_run, ["rabbitmqctl", "set_permissions", "-p", vhost, "guest", ".*", ".*", ".*"]
    )
    from rabbitkit.core.topology import RabbitQueue

    transport = await _async_transport(rabbit, vhost=vhost)
    try:
        await transport.declare_queue(RabbitQueue(name="vh-q"))
    finally:
        await transport.disconnect()
    result = await asyncio.to_thread(container.exec_run, ["rabbitmqctl", "list_queues", "-p", vhost, "name"])
    output = result[1] if isinstance(result, tuple) else result.output
    assert "vh-q" in output.decode()
