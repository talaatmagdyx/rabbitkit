"""Real-broker coverage for the retry/DLQ foundation fixes.

* G-07  a compressed message that fails transiently is retried and
        succeeds on attempt 2 (repro S3 flips); a terminal failure's DLQ
        copy is still decodable
* G-16  an AMQP client error raised by the handler is retried as
        ``infrastructure``; a decode failure is ``invalid_message``
* G-19  with ``trust_retry_header=False`` a producer-set retry count does
        not skip retries; the broker's x-death history still ends them
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe, await_consumers

pytestmark = pytest.mark.integration

PAYLOAD = b"a" * 20_000


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _config(rabbit: dict[str, Any], **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    return RabbitConfig(connection=ConnectionConfig.from_url(rabbit["url"]), **kw)


@contextlib.contextmanager
def _pika(rabbit: dict[str, Any]) -> Iterator[Any]:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(rabbit["url"]))
    try:
        yield conn.channel()
    finally:
        with contextlib.suppress(Exception):
            conn.close()


def _publish(rabbit: dict[str, Any], queue: str, body: bytes = b"{}", **props: Any) -> None:
    import pika

    with _pika(rabbit) as ch:
        ch.confirm_delivery()
        ch.basic_publish("", queue, body, pika.BasicProperties(delivery_mode=2, **props))


def _drain(rabbit: dict[str, Any], queue: str) -> list[tuple[Any, bytes]]:
    out: list[tuple[Any, bytes]] = []
    with _pika(rabbit) as ch:
        while True:
            method, props, body = ch.basic_get(queue, auto_ack=True)
            if method is None:
                return out
            out.append((props, body))


def _delete(rabbit: dict[str, Any], *queues: str) -> None:
    with _pika(rabbit) as ch:
        for q in queues:
            with contextlib.suppress(Exception):
                ch.queue_delete(q)


async def _wait(pred: Any, timeout: float) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.05)
    return bool(pred())


def _retry_queues(q: str, n: int) -> list[str]:
    return [q, f"{q}.dlq", *[f"{q}.retry.{i}" for i in range(1, n + 1)]]


# ── G-07 ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_compressed_message_is_retried_and_succeeds_on_attempt_two(rabbit: dict[str, Any]) -> None:
    """Repro S3: the retry copy used to carry the DECOMPRESSED body under
    content_encoding=gzip, so attempt 2 failed to decode and the message was
    dead-lettered after one handler run."""
    from rabbitkit import AsyncBroker, RetryConfig
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.compression import CompressionMiddleware

    q = _name("rk-gz")
    seen: list[bytes] = []
    broker = AsyncBroker(
        _config(rabbit, retry=RetryConfig(max_retries=2, delays=(1, 1))),
        middlewares=[CompressionMiddleware()],
    )

    @broker.subscriber(q, middlewares=[CompressionMiddleware()])
    async def handle(message: RabbitMessage) -> None:
        seen.append(message.body)
        if len(seen) == 1:
            raise OSError("transient")

    try:
        async with asyncio.timeout(45):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            outcome = await broker.publish(MessageEnvelope(routing_key=q, body=PAYLOAD))
            assert outcome.ok
            assert await _wait(lambda: len(seen) >= 2, 20), seen
            await asyncio.sleep(1.5)  # a third run (or a DLQ landing) would show by now
            await broker.stop()
        assert seen == [PAYLOAD, PAYLOAD]
        with QueueProbe(rabbit["url"]) as probe:
            assert probe.ready(f"{q}.dlq") == 0
            assert probe.ready(q) == 0
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
        _delete(rabbit, *_retry_queues(q, 2))


@pytest.mark.asyncio
async def test_terminal_dlq_copy_of_a_compressed_message_is_decodable(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RetryConfig
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.compression import CompressionMiddleware

    q = _name("rk-gzdlq")
    broker = AsyncBroker(
        _config(rabbit, retry=RetryConfig(max_retries=1, delays=(1,))),
        middlewares=[CompressionMiddleware()],
    )

    @broker.subscriber(q, middlewares=[CompressionMiddleware()])
    async def handle(message: RabbitMessage) -> None:
        assert message.body == PAYLOAD
        raise ValueError("business rule")

    try:
        async with asyncio.timeout(30):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            assert (await broker.publish(MessageEnvelope(routing_key=q, body=PAYLOAD))).ok
            with QueueProbe(rabbit["url"]) as probe:
                assert await _wait(lambda: probe.ready(f"{q}.dlq") == 1, 15)
            await broker.stop()
        [(props, body)] = _drain(rabbit, f"{q}.dlq")
        assert props.content_encoding == "gzip"
        assert gzip.decompress(body) == PAYLOAD
        assert props.headers["x-rabbitkit-failure-category"] == "permanent"
        assert props.headers["x-rabbitkit-error-category"] == "permanent"
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
        _delete(rabbit, *_retry_queues(q, 1))


# ── G-16 ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_amqp_client_error_is_retried_as_infrastructure(rabbit: dict[str, Any]) -> None:
    """aiormq's ChannelClosed is not an OSError: before the taxonomy it
    classified PERMANENT and went straight to the DLQ."""
    import aiormq.exceptions

    from rabbitkit import AsyncBroker, RabbitQueue, RetryConfig

    q = _name("rk-infra")
    broker = AsyncBroker(_config(rabbit, retry=RetryConfig(max_retries=1, delays=(1,))))
    calls = {"n": 0}

    @broker.subscriber(RabbitQueue(q))
    async def handle(body: bytes) -> None:
        calls["n"] += 1
        raise aiormq.exceptions.ChannelInvalidStateError("channel closed")

    try:
        async with asyncio.timeout(30):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            _publish(rabbit, q, message_id="infra-1")
            with QueueProbe(rabbit["url"]) as probe:
                assert await _wait(lambda: probe.ready(f"{q}.dlq") == 1, 15)
            await broker.stop()
        [(props, _)] = _drain(rabbit, f"{q}.dlq")
        assert calls["n"] == 2  # retried once, then exhausted
        assert props.headers["x-rabbitkit-error-category"] == "infrastructure"
        assert props.headers["x-rabbitkit-failure-category"] == "exhausted"
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
        _delete(rabbit, *_retry_queues(q, 1))


@pytest.mark.asyncio
async def test_undecodable_body_is_dead_lettered_as_invalid_message(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitQueue, RetryConfig
    from rabbitkit.core.types import ErrorSeverity
    from rabbitkit.serialization.json import JSONSerializer

    q = _name("rk-invalid")
    # unknown_policy=TRANSIENT would retry an unclassified error; a decode
    # failure is still terminal.
    broker = AsyncBroker(
        _config(rabbit, retry=RetryConfig(max_retries=2, delays=(1, 1), unknown_policy=ErrorSeverity.TRANSIENT)),
        serializer=JSONSerializer(),
    )
    calls = {"n": 0}

    @broker.subscriber(RabbitQueue(q))
    async def handle(body: dict[str, Any]) -> None:
        calls["n"] += 1

    try:
        async with asyncio.timeout(30):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            _publish(rabbit, q, body=b"{not json", message_id="bad-1")
            with QueueProbe(rabbit["url"]) as probe:
                assert await _wait(lambda: probe.ready(f"{q}.dlq") == 1, 15)
            await broker.stop()
        [(props, body)] = _drain(rabbit, f"{q}.dlq")
        assert calls["n"] == 0
        assert body == b"{not json"
        assert props.headers["x-rabbitkit-error-category"] == "invalid_message"
        assert props.headers["x-rabbitkit-attempts"] == 1
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
        _delete(rabbit, *_retry_queues(q, 2))


# ── G-19 ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_producer_set_retry_count_does_not_skip_retries(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitQueue, RetryConfig

    q = _name("rk-forged")
    broker = AsyncBroker(_config(rabbit, retry=RetryConfig(max_retries=2, delays=(1, 1), trust_retry_header=False)))
    calls = {"n": 0}

    @broker.subscriber(RabbitQueue(q))
    async def handle(body: bytes) -> None:
        calls["n"] += 1
        raise TimeoutError("flaky")

    try:
        async with asyncio.timeout(45):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            _publish(rabbit, q, message_id="forged-1", headers={"x-rabbitkit-retry-count": 99})
            with QueueProbe(rabbit["url"]) as probe:
                assert await _wait(lambda: probe.ready(f"{q}.dlq") == 1, 25)
            await broker.stop()
        [(props, _)] = _drain(rabbit, f"{q}.dlq")
        assert calls["n"] == 3  # the full budget: first run + 2 retries
        assert props.headers["x-rabbitkit-failure-category"] == "exhausted"
        assert props.headers["x-rabbitkit-attempts"] == 3
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
        _delete(rabbit, *_retry_queues(q, 2))
