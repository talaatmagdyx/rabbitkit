"""Real-broker tests: a broker ``basic.nack`` surfaces as NACKED.

a publisher-confirm ``basic.nack`` from RabbitMQ must surface as
``PublishStatus.NACKED`` -- never CONFIRMED, never silently republished.

The reliable way to make a real broker nack a publish is a full queue that
refuses new messages: ``x-max-length=1`` + ``x-overflow=reject-publish``.
Once the queue holds one message, RabbitMQ answers every further publish
routed to it with ``basic.nack`` (the message WAS routed, so it is a nack,
not a ``basic.return``, even with ``mandatory=True``).

Each test asserts what the BROKER holds afterwards, not only what the client
reports: exactly the first message, so nothing was republished behind the
caller's back.
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
    return f"rk-hardening-{prefix}-{uuid.uuid4().hex[:8]}"


def _config(url: str) -> Any:
    from rabbitkit.core.config import ConnectionConfig, PublisherConfig, RabbitConfig

    return RabbitConfig(
        connection=ConnectionConfig.from_url(url),
        publisher=PublisherConfig(confirm_delivery=True, mandatory=True, confirm_timeout=10.0),
    )


@contextlib.contextmanager
def _pika(url: str) -> Iterator[Any]:
    import pika

    connection = pika.BlockingConnection(pika.URLParameters(url))
    try:
        yield connection.channel()
    finally:
        with contextlib.suppress(Exception):
            connection.close()


def _declare_full_queue(url: str, queue: str) -> None:
    """A queue that holds one message and nacks every publish after it."""
    with _pika(url) as channel:
        channel.queue_declare(
            queue=queue,
            durable=True,
            arguments={"x-max-length": 1, "x-overflow": "reject-publish"},
        )


def _drain(url: str, queue: str) -> list[bytes]:
    bodies: list[bytes] = []
    with _pika(url) as channel:
        while True:
            method, _props, body = channel.basic_get(queue=queue, auto_ack=True)
            if method is None:
                return bodies
            bodies.append(body)


def _delete(url: str, queue: str) -> None:
    with _pika(url) as channel:
        channel.queue_delete(queue=queue)


def _settled_ready(url: str, queue: str, *, settle: float = 1.0) -> int | None:
    """Ready count after a quiet period, so a late republish would show up."""
    time.sleep(settle)
    with QueueProbe(url) as probe:
        return probe.ready(queue)


@contextlib.asynccontextmanager
async def _async_broker(url: str) -> AsyncIterator[Any]:
    from rabbitkit.async_.broker import AsyncBroker

    broker = AsyncBroker(_config(url))
    await broker.start(install_signal_handlers=False)
    try:
        yield broker
    finally:
        await broker.stop(timeout=5)


@contextlib.contextmanager
def _sync_broker(url: str) -> Iterator[Any]:
    from rabbitkit.sync.broker import SyncBroker

    broker = SyncBroker(_config(url))
    broker.start()
    try:
        yield broker
    finally:
        broker.stop()


# ── async ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_broker_nack_surfaces_as_nacked_and_is_not_republished(rabbit: dict[str, Any]) -> None:
    from rabbitkit.core.types import MessageEnvelope, PublishReason, PublishStatus

    url = rabbit["url"]
    queue = _name("nack-async")
    await asyncio.to_thread(_declare_full_queue, url, queue)
    try:
        async with _async_broker(url) as broker:
            first = await broker.publish(MessageEnvelope(routing_key=queue, body=b"first", mandatory=True))
            second = await broker.publish(MessageEnvelope(routing_key=queue, body=b"second", mandatory=True))
            # The channel the nack arrived on stays usable.
            third = await broker.publish(MessageEnvelope(routing_key=queue, body=b"third", mandatory=True))

        assert first.status is PublishStatus.CONFIRMED
        for outcome in (second, third):
            assert outcome.status is PublishStatus.NACKED, outcome
            assert outcome.reason == PublishReason.NACKED
            assert not outcome.ok
            assert not outcome.is_unknown
            assert outcome.reply_code is None  # a nack carries no reply code (that is a return)
        assert await asyncio.to_thread(_settled_ready, url, queue) == 1
        assert await asyncio.to_thread(_drain, url, queue) == [b"first"]
    finally:
        await asyncio.to_thread(_delete, url, queue)


@pytest.mark.asyncio
async def test_async_publish_many_reports_a_broker_nack_per_item(rabbit: dict[str, Any]) -> None:
    from rabbitkit.core.types import BulkPublishStatus, MessageEnvelope

    url = rabbit["url"]
    queue = _name("nack-bulk")
    await asyncio.to_thread(_declare_full_queue, url, queue)
    try:
        async with _async_broker(url) as broker:
            first = await broker.publish(MessageEnvelope(routing_key=queue, body=b"first", mandatory=True))
            result = await broker.publish_many(
                [MessageEnvelope(routing_key=queue, body=f"b{i}".encode(), mandatory=True) for i in range(3)]
            )

        assert first.ok
        assert [item.status for item in result.items] == [BulkPublishStatus.NACKED] * 3
        assert await asyncio.to_thread(_drain, url, queue) == [b"first"]
    finally:
        await asyncio.to_thread(_delete, url, queue)


# ── sync ────────────────────────────────────────────────────────────────


def test_sync_broker_nack_surfaces_as_nacked_and_is_not_republished(rabbit: dict[str, Any]) -> None:
    from rabbitkit.core.types import MessageEnvelope, PublishReason, PublishStatus

    url = rabbit["url"]
    queue = _name("nack-sync")
    _declare_full_queue(url, queue)
    try:
        with _sync_broker(url) as broker:
            first = broker.publish(MessageEnvelope(routing_key=queue, body=b"first", mandatory=True))
            second = broker.publish(MessageEnvelope(routing_key=queue, body=b"second", mandatory=True))
            third = broker.publish(MessageEnvelope(routing_key=queue, body=b"third", mandatory=True))

        assert first.status is PublishStatus.CONFIRMED
        for outcome in (second, third):
            assert outcome.status is PublishStatus.NACKED, outcome
            assert outcome.reason == PublishReason.NACKED
            assert not outcome.ok
            assert not outcome.is_unknown
        assert _settled_ready(url, queue) == 1
        assert _drain(url, queue) == [b"first"]
    finally:
        _delete(url, queue)
