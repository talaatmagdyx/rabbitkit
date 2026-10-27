"""Real-broker coverage for topology ownership and quorum dead-lettering.

* R-D8  PASSIVE_ONLY performs no writes (bindings included) and
        ``verify_topology`` detects a classic queue where quorum is declared
* 406   AUTO_DECLARE on an ops-created plain queue names the conflicting arg
* 16.6  public ``declare_*`` / ``bind_queue`` for publish-only services
* 16.2  quorum ``dead_letter_strategy="at-least-once"`` keeps a terminally
        failed message when the DLQ is missing (RabbitMQ 4.1)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import urllib.request
import uuid
from base64 import b64encode
from collections.abc import Iterator
from typing import Any

import pytest

from tests.integration.conftest import await_consumers, skip_without_docker

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


@pytest.fixture(scope="module")
def rabbit4() -> Iterator[dict[str, Any]]:
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


def _config(rabbit: dict[str, Any], **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    return RabbitConfig(connection=ConnectionConfig.from_url(rabbit["url"]), **kw)


def _mgmt_client(rabbit: dict[str, Any]) -> Any:
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    return RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))


def _mgmt_get(rabbit: dict[str, Any], path: str) -> Any:
    req = urllib.request.Request(f"{rabbit['mgmt']}/api{path}")  # noqa: S310
    req.add_header("Authorization", "Basic " + b64encode(b"guest:guest").decode())
    with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
        return json.loads(resp.read())


@contextlib.contextmanager
def _pika(rabbit: dict[str, Any]) -> Iterator[Any]:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(rabbit["url"]))
    try:
        yield conn.channel()
    finally:
        with contextlib.suppress(Exception):
            conn.close()


def _cleanup(rabbit: dict[str, Any], queues: list[str], exchanges: list[str] = []) -> None:  # noqa: B006
    with _pika(rabbit) as ch:
        for q in queues:
            with contextlib.suppress(Exception):
                ch.queue_delete(q)
        for e in exchanges:
            with contextlib.suppress(Exception):
                ch.exchange_delete(e)


# ── R-D8 ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_passive_only_creates_no_binding(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, ExchangeType, RabbitExchange, RabbitQueue, TopologyMode

    ex, q = _name("rk-pex"), _name("rk-pq")
    with _pika(rabbit) as ch:
        ch.exchange_declare(ex, exchange_type="topic", durable=True)
        ch.queue_declare(q, durable=True)
    broker = AsyncBroker(_config(rabbit, topology_mode=TopologyMode.PASSIVE_ONLY))
    broker.subscriber(RabbitQueue(q, routing_key="orders.#"), exchange=RabbitExchange(ex, type=ExchangeType.TOPIC))(
        lambda body: None
    )
    try:
        async with asyncio.timeout(30):
            await broker.start(install_signal_handlers=False)
            report = await broker.verify_topology(_mgmt_client(rabbit))
            await broker.stop()
        bindings = _mgmt_get(rabbit, f"/queues/%2F/{q}/bindings")
        assert [(b["source"], b["routing_key"]) for b in bindings] == [("", q)]  # default binding only
        failed = {(f.entity, f.check) for f in report.failed}
        assert ("binding", "routing_key=orders.#") in failed
    finally:
        _cleanup(rabbit, [q], [ex])


@pytest.mark.asyncio
async def test_verify_topology_detects_classic_where_quorum_declared(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, QueueType, RabbitQueue, TopologyMode

    q = _name("rk-ptype")
    with _pika(rabbit) as ch:
        ch.queue_declare(q, durable=True, arguments={"x-queue-type": "classic"})
    broker = AsyncBroker(_config(rabbit, topology_mode=TopologyMode.PASSIVE_ONLY))
    broker.subscriber(RabbitQueue(q, queue_type=QueueType.QUORUM))(lambda body: None)
    try:
        async with asyncio.timeout(30):
            await broker.start(install_signal_handlers=False)  # passive: existence only
            report = await broker.verify_topology(_mgmt_client(rabbit))
            await broker.stop()
        [failure] = report.failed
        assert (failure.name, failure.check) == (q, "type")
        assert "expected quorum, broker has classic" in failure.detail
        assert _mgmt_get(rabbit, f"/queues/%2F/{q}")["type"] == "classic"  # untouched
    finally:
        _cleanup(rabbit, [q])


@pytest.mark.asyncio
async def test_auto_declare_conflict_names_the_argument(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitQueue
    from rabbitkit.core.errors import ConfigurationError

    q = _name("rk-406")
    with _pika(rabbit) as ch:
        ch.queue_declare(q, durable=True)  # ops-created, no DLX
    broker = AsyncBroker(_config(rabbit))
    broker.subscriber(RabbitQueue(q))(lambda body: None)
    try:
        with pytest.raises(ConfigurationError) as info:
            async with asyncio.timeout(30):
                await broker.start(install_signal_handlers=False)
        with contextlib.suppress(Exception):
            await broker.stop()
        msg = str(info.value)
        assert repr(q) in msg and "'x-dead-letter-exchange'" in msg
        assert "PASSIVE_ONLY" in msg and "reject_without_dlx" in msg
    finally:
        _cleanup(rabbit, [q, f"{q}.dlq"])


@pytest.mark.asyncio
async def test_public_declare_api_for_publish_only_service(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, ExchangeType, RabbitExchange, RabbitQueue, TopologyMode
    from rabbitkit.core.errors import ConfigurationError

    ex, q = _name("rk-pubex"), _name("rk-pubq")
    broker = AsyncBroker(_config(rabbit))
    passive = AsyncBroker(_config(rabbit, topology_mode=TopologyMode.PASSIVE_ONLY))
    try:
        async with asyncio.timeout(30):
            await broker.start(install_signal_handlers=False)
            await broker.declare_exchange(RabbitExchange(ex, type=ExchangeType.TOPIC))
            await broker.declare_queue(RabbitQueue(q))
            await broker.bind_queue(q, ex, "a.#")
            await passive.start(install_signal_handlers=False)
            with pytest.raises(ConfigurationError):
                await passive.bind_queue(q, ex, "b.#")
            await passive.declare_queue(RabbitQueue(q, passive=True))  # existence check is fine
            await passive.stop()
            await broker.stop()
        bindings = {(b["source"], b["routing_key"]) for b in _mgmt_get(rabbit, f"/queues/%2F/{q}/bindings")}
        assert (ex, "a.#") in bindings and (ex, "b.#") not in bindings
        assert "x-dead-letter-exchange" not in (_mgmt_get(rabbit, f"/queues/%2F/{q}")["arguments"] or {})
    finally:
        _cleanup(rabbit, [q], [ex])


# ── 16.2: quorum at-least-once dead-lettering ─────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["at-least-once", "at-most-once"])
async def test_quorum_at_least_once_keeps_message_when_dlq_missing(rabbit4: dict[str, Any], strategy: str) -> None:
    from rabbitkit import AsyncBroker, QueueType, RabbitQueue, RetryConfig

    q = _name("rk-alo")
    source = RabbitQueue(q, queue_type=QueueType.QUORUM, dead_letter_strategy=strategy, overflow="reject-publish")  # type: ignore[arg-type]
    broker = AsyncBroker(_config(rabbit4, retry=RetryConfig(max_retries=1, delays=(1,), delay_queue_type="quorum")))
    calls = {"n": 0}

    @broker.subscriber(source)
    async def handle(body: bytes) -> None:
        calls["n"] += 1
        raise ValueError("permanent")

    try:
        async with asyncio.timeout(60):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit4["url"], q)
            _cleanup(rabbit4, [f"{q}.dlq"])  # the DLQ disappears
            with _pika(rabbit4) as ch:
                ch.confirm_delivery()
                ch.basic_publish("", q, b"{}")
            for _ in range(100):
                if calls["n"]:
                    break
                await asyncio.sleep(0.05)
            await asyncio.sleep(1)
            await broker.stop()
            # rabbitkit's DLQ publish came back unroutable → it fell back to
            # reject → the broker's dead-letter strategy decides.
            dlx = None
            for _ in range(30):  # management stats refresh every ~5s
                info = _mgmt_get(rabbit4, f"/queues/%2F/{q}")
                dlx = info.get("messages_dlx")
                if dlx is not None and (dlx == 1 or strategy == "at-most-once"):
                    break
                await asyncio.sleep(0.5)
        assert calls["n"] == 1
        assert info["arguments"]["x-dead-letter-strategy"] == strategy
        if strategy == "at-least-once":
            assert dlx == 1, info  # retained, waiting for the DLQ to come back
        else:
            assert not dlx  # dropped: the at-most-once data-loss case
    finally:
        _cleanup(rabbit4, [q, f"{q}.dlq", f"{q}.retry.1"])
