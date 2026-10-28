"""Real-broker tests for sync publish outcomes and metadata DLQ copies.

Each test asserts what the BROKER holds (queue counts, the copies on a
queue, their headers), not only what the client reports:

* Sync publish outcomes follow the async contract: a connection lost after
  the frames left is UNKNOWN and never republished; a missing exchange is
  ERROR ``exchange_not_found``, sent once; a connection the broker closed
  while idle is replaced before sending.
* ``async with AsyncBroker(...)`` starts and stops the broker.
* failures that bypass RetryMiddleware reach the DLQ as a confirmed
  copy carrying rabbitkit's failure metadata, not only ``x-death``.
"""

from __future__ import annotations

import contextlib
import threading
import time
import uuid
from dataclasses import replace
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe, wait_for_consumers

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


def _name(prefix: str) -> str:
    return f"rk-cleanup-{prefix}-{uuid.uuid4().hex[:8]}"


def _config(url: str, *, connection_name: str | None = None, **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    connection = ConnectionConfig.from_url(url)
    if connection_name is not None:
        connection = replace(connection, connection_name=connection_name)
    return RabbitConfig(connection=connection, **kw)


def _declare(url: str, queue: str) -> None:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(url))
    try:
        conn.channel().queue_declare(queue=queue, durable=True)
    finally:
        conn.close()


def _ready(url: str, queue: str) -> int | None:
    with QueueProbe(url) as probe:
        return probe.ready(queue)


def _wait_ready(url: str, queue: str, expected: int, timeout: float = 10.0) -> int | None:
    deadline = time.monotonic() + timeout
    seen = _ready(url, queue)
    while seen != expected and time.monotonic() < deadline:
        time.sleep(0.05)
        seen = _ready(url, queue)
    return seen


def _management(rabbit: dict[str, Any]) -> Any:
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    return RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))


def _connections_named(client: Any, connection_name: str) -> list[str]:
    return [
        str(c.get("name"))
        for c in client.list_connections()
        if (c.get("client_properties") or {}).get("connection_name") == connection_name
    ]


def _kill_connection(rabbit: dict[str, Any], connection_name: str) -> None:
    """Force-close this client's connection and wait until the broker forgot it.

    The management API lists a new connection only after its next stats
    refresh, so poll until it is visible first."""
    client = _management(rabbit)
    deadline = time.monotonic() + 15
    names = _connections_named(client, connection_name)
    while not names and time.monotonic() < deadline:
        time.sleep(0.25)
        names = _connections_named(client, connection_name)
    assert names, f"connection {connection_name} never appeared in the management API"
    for name in names:
        client.close_connection(name, reason="rabbitkit test")
    while _connections_named(client, connection_name) and time.monotonic() < deadline:
        time.sleep(0.1)
    time.sleep(0.3)  # the Connection.Close frame has reached the client socket


# ── Sync publish outcomes (contract consistency with async) ────────────────


def test_sync_handler_publish_after_connection_kill_is_unknown_and_not_republished(
    rabbit: dict[str, Any],
) -> None:
    """A handler publishes on a connection the broker has just closed. The
    frames are written, then the close is read: UNKNOWN, never republished
    by the transport (it used to reconnect inside the handler, publish again
    and report CONFIRMED). The application republishes with the same
    message_id on redelivery: exactly one copy on the target queue."""
    from rabbitkit.core.types import PublishStatus
    from rabbitkit.sync.broker import SyncBroker

    source = _name("sync-src")
    target = _name("sync-dst")
    connection_name = _name("sync-conn")
    _declare(rabbit["url"], target)
    broker = SyncBroker(_config(rabbit["url"], connection_name=connection_name))
    kill_done = threading.Event()
    in_handler = threading.Event()
    outcomes: list[Any] = []

    @broker.subscriber(source, reject_without_dlx="discard")
    def handler(body: bytes) -> None:
        if not outcomes:
            in_handler.set()
            assert kill_done.wait(30)
        outcomes.append(broker.publish(routing_key=target, body=b"payload", message_id="order-1"))

    runner = threading.Thread(target=broker.run, daemon=True)
    runner.start()
    try:
        wait_for_consumers(rabbit["url"], source)
        with QueueProbe(rabbit["url"]) as probe:
            probe._channel().basic_publish(exchange="", routing_key=source, body=b"go")
        assert in_handler.wait(20)
        _kill_connection(rabbit, connection_name)
        kill_done.set()

        deadline = time.monotonic() + 30
        while len(outcomes) < 2 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert len(outcomes) >= 2, f"no redelivery after the reconnect: {outcomes}"
        first, second = outcomes[0], outcomes[1]
        assert first.status is PublishStatus.UNKNOWN
        assert first.reason == "connection_lost"
        assert second.status is PublishStatus.CONFIRMED
        assert _wait_ready(rabbit["url"], target, 1) == 1
        time.sleep(0.5)
        assert _ready(rabbit["url"], target) == 1, "the transport published a hidden second copy"
    finally:
        kill_done.set()
        transport = broker._transport
        if transport is not None:
            transport.stop_consuming()
        runner.join(timeout=30)


def test_sync_publish_to_missing_exchange_is_error_and_sent_once(rabbit: dict[str, Any]) -> None:
    """404 on publish: ERROR ``exchange_not_found`` with the broker's reply,
    not retried (the old path reconnected and published a second time). The
    next publish reopens the publisher channel on the SAME connection
    instead of leaving the old connection behind."""
    from rabbitkit.core.types import PublishStatus
    from rabbitkit.sync.broker import SyncBroker

    queue = _name("sync-ok")
    _declare(rabbit["url"], queue)
    broker = SyncBroker(_config(rabbit["url"]))
    broker.start()
    try:
        transport = broker._transport
        assert transport is not None
        broker.publish(routing_key=queue, body=b"warm-up")
        connection = transport._connection
        opened: list[int] = []
        transport.on_channel_opened(lambda: opened.append(1))

        outcome = broker.publish(exchange=_name("missing-x"), routing_key="rk", body=b"x")

        assert outcome.status is PublishStatus.ERROR
        assert outcome.reason == "exchange_not_found"
        assert outcome.reply_code == 404
        assert outcome.reply_text is not None and "no exchange" in outcome.reply_text
        assert opened == [], "the refused publish was retried on a new channel"
        assert transport._connection is connection

        after = broker.publish(routing_key=queue, body=b"after")
        assert after.status is PublishStatus.CONFIRMED
        assert transport._connection is connection, "a 404 replaced the whole connection"
        assert opened == [1]
        assert _wait_ready(rabbit["url"], queue, 2) == 2
    finally:
        broker.stop()


def test_sync_idle_publisher_connection_closed_by_broker_is_replaced_before_sending(
    rabbit: dict[str, Any],
) -> None:
    """A publish-only SyncBroker whose connection the broker closed while it
    sat idle: the dead connection is noticed before anything is sent, so the
    publish is CONFIRMED with exactly one copy (M4 without a republish)."""
    from rabbitkit.core.types import PublishStatus
    from rabbitkit.sync.broker import SyncBroker

    queue = _name("sync-idle")
    connection_name = _name("sync-idle-conn")
    _declare(rabbit["url"], queue)
    broker = SyncBroker(_config(rabbit["url"], connection_name=connection_name))
    broker.start()
    try:
        transport = broker._transport
        assert transport is not None
        reconnects: list[int] = []
        transport.on_reconnect(lambda: reconnects.append(1))
        assert broker.publish(routing_key=queue, body=b"first").status is PublishStatus.CONFIRMED

        _kill_connection(rabbit, connection_name)
        outcome = broker.publish(routing_key=queue, body=b"second", message_id="idle-2")

        assert outcome.status is PublishStatus.CONFIRMED
        assert reconnects == [1]
        assert _wait_ready(rabbit["url"], queue, 2) == 2
        time.sleep(0.5)
        assert _ready(rabbit["url"], queue) == 2
    finally:
        with contextlib.suppress(Exception):
            broker.stop()


# ── async with AsyncBroker ─────────────────────────────────


async def test_async_with_starts_consumes_and_stops(rabbit: dict[str, Any]) -> None:
    import asyncio

    from rabbitkit import AsyncBroker

    queue = _name("ctx")
    received: list[bytes] = []
    broker = AsyncBroker(_config(rabbit["url"]))

    @broker.subscriber(queue, reject_without_dlx="discard")
    async def handle(body: bytes) -> None:
        received.append(body)

    async with asyncio.timeout(30):
        async with broker as entered:
            assert entered is broker and broker.started
            assert (await broker.publish(routing_key=queue, body=b"hello")).ok
            while not received:
                await asyncio.sleep(0.05)
        assert not broker.started
    assert received == [b"hello"]
    assert await asyncio.to_thread(_consumers, rabbit["url"], queue) == 0


def _consumers(url: str, queue: str) -> int | None:
    with QueueProbe(url) as probe:
        return probe.consumers(queue)


# ── DLQ copies with metadata for failures outside RetryMiddleware ────


def _get_one(url: str, queue: str) -> tuple[Any, bytes] | None:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(url))
    try:
        method, props, body = conn.channel().basic_get(queue, auto_ack=True)
        return None if method is None else (props, body)
    finally:
        conn.close()


async def _await_dlq_copy(url: str, dlq: str, timeout: float = 20.0) -> tuple[Any, bytes]:
    import asyncio

    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await asyncio.to_thread(_ready, url, dlq) == 1:
            got = await asyncio.to_thread(_get_one, url, dlq)
            assert got is not None
            return got
        await asyncio.sleep(0.05)
    raise AssertionError(f"{dlq}: no dead-lettered copy within {timeout}s (ready={_ready(url, dlq)})")


def _publish_raw(url: str, queue: str, body: bytes, **props: Any) -> None:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(url))
    try:
        channel = conn.channel()
        channel.confirm_delivery()
        channel.basic_publish("", queue, body, pika.BasicProperties(delivery_mode=2, **props))
    finally:
        conn.close()


async def test_bad_gzip_on_a_retry_route_reaches_the_dlq_with_metadata(rabbit: dict[str, Any]) -> None:
    """A gzip body that does not decompress fails in on_receive, before
    RetryMiddleware: it used to reach the DLQ by a broker reject with only
    x-death. Now the DLQ copy is confirmed with the failure metadata and the
    wire body, and the source is acked."""
    import asyncio

    from rabbitkit import AsyncBroker, RetryConfig
    from rabbitkit.core.config import SafetyConfig
    from rabbitkit.middleware.compression import CompressionMiddleware

    queue = _name("gz")
    calls: list[bytes] = []
    broker = AsyncBroker(
        _config(
            rabbit["url"],
            retry=RetryConfig(max_retries=2, delays=(1, 2)),
            safety=SafetyConfig(dead_letter_with_metadata=True),
        )
    )

    @broker.subscriber(queue, middlewares=[CompressionMiddleware()])
    async def handle(body: bytes) -> None:
        calls.append(body)

    async with asyncio.timeout(60):
        async with broker:
            await asyncio.to_thread(wait_for_consumers, rabbit["url"], queue)
            await asyncio.to_thread(
                _publish_raw, rabbit["url"], queue, b"definitely not gzip", content_encoding="gzip", message_id="gz-1"
            )
            props, body = await _await_dlq_copy(rabbit["url"], f"{queue}.dlq")

    headers = props.headers
    assert body == b"definitely not gzip" and props.content_encoding == "gzip"
    assert props.message_id == "gz-1"
    assert headers["x-rabbitkit-error-category"] == "invalid_message"
    assert headers["x-rabbitkit-failure-category"] == "permanent"
    assert headers["x-rabbitkit-original-queue"] == queue
    assert headers["x-rabbitkit-original-routing-key"] == queue
    assert headers["x-rabbitkit-original-message-id"] == "gz-1"
    assert headers["x-rabbitkit-retry-count"] == 0
    assert "x-rabbitkit-dead-lettered-at" in headers
    assert "x-death" not in headers, "the copy, not a broker dead-letter, reached the DLQ"
    assert calls == []
    assert _ready(rabbit["url"], queue) == 0


async def test_permanent_failure_on_a_route_without_retry_carries_metadata(rabbit: dict[str, Any]) -> None:
    import asyncio

    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import SafetyConfig

    queue = _name("noretry")
    broker = AsyncBroker(_config(rabbit["url"], safety=SafetyConfig(dead_letter_with_metadata=True)))

    @broker.subscriber(queue)
    async def handle(body: bytes) -> None:
        raise ValueError("order 42 is invalid; token=s3cr3t")

    async with asyncio.timeout(60):
        async with broker:
            await asyncio.to_thread(wait_for_consumers, rabbit["url"], queue)
            await asyncio.to_thread(_publish_raw, rabbit["url"], queue, b'{"id": 42}', message_id="p-1")
            props, body = await _await_dlq_copy(rabbit["url"], f"{queue}.dlq")

    headers = props.headers
    assert body == b'{"id": 42}'
    assert headers["x-rabbitkit-error-category"] == "permanent"
    assert headers["x-rabbitkit-error-class"] == "ValueError"
    assert "s3cr3t" not in str(headers.get("x-rabbitkit-error-message", ""))
    assert headers["x-rabbitkit-original-queue"] == queue
    assert headers["x-rabbitkit-attempts"] == 1
    assert "x-death" not in headers
    assert _ready(rabbit["url"], queue) == 0


async def test_without_the_setting_the_dlq_entry_has_only_x_death(rabbit: dict[str, Any]) -> None:
    """The default (old) path: a broker reject into the safety DLX."""
    import asyncio

    from rabbitkit import AsyncBroker

    queue = _name("plain")
    broker = AsyncBroker(_config(rabbit["url"]))

    @broker.subscriber(queue)
    async def handle(body: bytes) -> None:
        raise ValueError("permanent")

    async with asyncio.timeout(60):
        async with broker:
            await asyncio.to_thread(wait_for_consumers, rabbit["url"], queue)
            await asyncio.to_thread(_publish_raw, rabbit["url"], queue, b"{}")
            props, _ = await _await_dlq_copy(rabbit["url"], f"{queue}.dlq")

    assert "x-death" in props.headers
    assert "x-rabbitkit-error-category" not in props.headers


async def test_copy_to_a_missing_dlq_is_never_acked_and_lands_once_restored(rabbit: dict[str, Any]) -> None:
    """The DLQ is deleted under a running consumer: the copy comes back
    RETURNED, the delivery is not acked (requeued after a backoff, since a
    reject would drop it), and once the DLQ exists again the next attempt's
    copy is confirmed: exactly one copy, with metadata."""
    import asyncio

    import pika

    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import SafetyConfig

    queue = _name("missing-dlq")
    dlq = f"{queue}.dlq"
    broker = AsyncBroker(_config(rabbit["url"], safety=SafetyConfig(dead_letter_with_metadata=True)))
    attempts: list[int] = []

    @broker.subscriber(queue)
    async def handle(body: bytes) -> None:
        attempts.append(1)
        raise ValueError("permanent")

    def _delete_dlq() -> None:
        conn = pika.BlockingConnection(pika.URLParameters(rabbit["url"]))
        try:
            conn.channel().queue_delete(dlq)
        finally:
            conn.close()

    async with asyncio.timeout(90):
        async with broker:
            await asyncio.to_thread(wait_for_consumers, rabbit["url"], queue)
            await asyncio.to_thread(_delete_dlq)
            await asyncio.to_thread(_publish_raw, rabbit["url"], queue, b"{}", message_id="keep-1")
            while len(attempts) < 2:  # failed, copy RETURNED, requeued, failed again
                await asyncio.sleep(0.05)
            assert await asyncio.to_thread(_ready, rabbit["url"], dlq) is None  # still missing
            await asyncio.to_thread(_declare, rabbit["url"], dlq)
            props, _ = await _await_dlq_copy(rabbit["url"], dlq, timeout=30)

    assert props.message_id == "keep-1"
    assert props.headers["x-rabbitkit-error-category"] == "permanent"
    await asyncio.sleep(0.5)
    assert _ready(rabbit["url"], dlq) == 0  # the one copy was taken by _await_dlq_copy
    assert _ready(rabbit["url"], queue) == 0
