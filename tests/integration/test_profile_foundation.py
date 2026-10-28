"""Real-broker coverage for the CRITICAL reliability profile and the body cap
and reserved-header features it builds on.

* ``critical_config()`` declares a quorum, at-least-once retry chain and a
  quorum DLQ; a transient failure is retried and then succeeds; the routes
  pass ``validate_profile(CRITICAL)``
* a permanent failure's DLQ copy carries the failure metadata and keeps the
  message's AMQP properties and application headers unchanged
* a producer-set ``x-rabbitkit-retry-count`` / ``x-death`` is stripped from
  an application publish, so retries still happen
* an oversized delivery is dead-lettered without running the handler
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe, await_consumers

pytestmark = pytest.mark.integration

OCCURRED = datetime(2026, 10, 27, 12, 0, 0, 250000, tzinfo=UTC)


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _critical(rabbit: dict[str, Any], **consumer: Any) -> Any:
    from rabbitkit import ConnectionConfig, ConsumerConfig, RabbitConfig, RetryConfig, critical_config

    return critical_config(
        RabbitConfig(
            connection=ConnectionConfig.from_url(rabbit["url"]),
            consumer=ConsumerConfig(processing_timeout=10.0, **consumer),
            retry=RetryConfig(max_retries=1, delays=(1,)),
        )
    )


def _source(name: str) -> Any:
    from rabbitkit import DeadLetterStrategy, QueueType, RabbitQueue

    return RabbitQueue(
        name,
        queue_type=QueueType.QUORUM,
        dead_letter_strategy=DeadLetterStrategy.AT_LEAST_ONCE,
        overflow="reject-publish",
    )


@contextlib.contextmanager
def _pika(rabbit: dict[str, Any]) -> Iterator[Any]:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(rabbit["url"]))
    try:
        yield conn.channel()
    finally:
        with contextlib.suppress(Exception):
            conn.close()


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


def _queues(q: str) -> list[str]:
    return [q, f"{q}.dlq", f"{q}.retry.1"]


async def _wait(pred: Any, timeout: float) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.05)
    return bool(pred())


async def _wait_ready(rabbit: dict[str, Any], queue: str, count: int, timeout: float = 20.0) -> None:
    def ready() -> bool:
        with QueueProbe(rabbit["url"]) as probe:
            return probe.ready(queue) == count

    assert await _wait(ready, timeout), f"{queue} never reached {count} ready message(s)"


def _mgmt_get(rabbit: dict[str, Any], path: str) -> Any:
    import json
    import urllib.request
    from base64 import b64encode

    req = urllib.request.Request(f"{rabbit['mgmt']}/api{path}")  # noqa: S310
    req.add_header("Authorization", "Basic " + b64encode(b"guest:guest").decode())
    with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
        return json.loads(resp.read())


# ── ADR-M1/M4: the profile's topology, and a transient failure retried ─────


@pytest.mark.asyncio
async def test_critical_profile_declares_an_at_least_once_quorum_chain_and_retries(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, MessageEnvelope, ReliabilityProfile, validate_profile

    q = _name("rk-crit-retry")
    config = _critical(rabbit)
    broker = AsyncBroker(config)
    runs: list[int] = []

    @broker.subscriber(_source(q))
    async def handle(body: bytes) -> None:
        runs.append(len(runs) + 1)
        if len(runs) == 1:
            raise TimeoutError("flaky dependency")

    assert validate_profile(config, ReliabilityProfile.CRITICAL, routes=broker.routes) == []
    try:
        async with asyncio.timeout(45):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            outcome = await broker.publish(MessageEnvelope(routing_key=q, body=b"{}", mandatory=True))
            assert outcome.ok, outcome
            assert await _wait(lambda: len(runs) >= 2, 20), runs
            await asyncio.sleep(0.5)
            await broker.stop()
        assert runs == [1, 2]
        src = _mgmt_get(rabbit, f"/queues/%2F/{q}")
        delay = _mgmt_get(rabbit, f"/queues/%2F/{q}.retry.1")
        dlq = _mgmt_get(rabbit, f"/queues/%2F/{q}.dlq")
        assert src["type"] == "quorum"
        assert src["arguments"]["x-dead-letter-strategy"] == "at-least-once"
        assert src["arguments"]["x-overflow"] == "reject-publish"
        assert delay["type"] == "quorum"
        assert delay["arguments"]["x-dead-letter-strategy"] == "at-least-once"
        assert dlq["type"] == "quorum"
        with QueueProbe(rabbit["url"]) as probe:
            assert probe.ready(f"{q}.dlq") == 0
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
        _delete(rabbit, *_queues(q))


# ── permanent failure → DLQ copy with failure metadata, properties kept ────


class _OrderRejected(Exception):
    """An unclassified exception: PERMANENT by default."""


@pytest.mark.asyncio
async def test_permanent_failure_dlq_copy_keeps_failure_metadata_and_properties(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, MessageEnvelope

    q = _name("rk-crit-perm")
    broker = AsyncBroker(_critical(rabbit))
    runs = {"n": 0}

    @broker.subscriber(_source(q))
    async def handle(body: bytes) -> None:
        runs["n"] += 1
        raise _OrderRejected("business rule")

    envelope = MessageEnvelope(
        routing_key=q,
        body=b'{"id": 1}',
        message_id=f"order-{uuid.uuid4().hex[:6]}",
        correlation_id="corr-1",
        type="order.created",
        app_id="checkout",
        timestamp=OCCURRED,
        headers={"x-app-key": "order:1"},
        mandatory=True,
    )
    try:
        async with asyncio.timeout(45):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            outcome = await broker.publish(envelope)
            assert outcome.ok, outcome
            await _wait_ready(rabbit, f"{q}.dlq", 1)
            await broker.stop()
        assert runs["n"] == 1  # permanent: never retried
        ((props, body),) = _drain(rabbit, f"{q}.dlq")
        assert body == b'{"id": 1}'
        headers = props.headers
        assert headers["x-rabbitkit-error-category"] == "permanent"
        assert headers["x-rabbitkit-failure-category"] == "permanent"
        assert headers["x-rabbitkit-original-queue"] == q
        assert "x-rabbitkit-dead-lettered-at" in headers
        assert props.delivery_mode == 2
        assert (props.message_id, props.correlation_id) == (envelope.message_id, "corr-1")
        assert (props.type, props.app_id) == ("order.created", "checkout")
        assert props.timestamp is not None
        assert headers["x-app-key"] == "order:1"
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
        _delete(rabbit, *_queues(q))


# ── G-19: a forged retry count cannot skip retries ─────────────────────────


@pytest.mark.asyncio
async def test_forged_retry_headers_are_stripped_and_the_message_is_retried(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, MessageEnvelope, RabbitMessage

    q = _name("rk-crit-forged")
    broker = AsyncBroker(_critical(rabbit))
    first_headers: list[dict[str, Any]] = []
    runs = {"n": 0}

    @broker.subscriber(_source(q))
    async def handle(body: bytes, message: RabbitMessage) -> None:
        runs["n"] += 1
        if runs["n"] == 1:
            first_headers.append(dict(message.headers))
            raise TimeoutError("flaky")

    forged = {
        "x-rabbitkit-retry-count": 99,
        "x-rabbitkit-original-queue": q,
        "x-rabbitkit-first-failed-at": "2000-01-01T00:00:00+00:00",
        "x-death": [{"queue": f"{q}.retry.1", "reason": "expired", "count": 5}],
        "x-app": "kept",
    }
    try:
        async with asyncio.timeout(45):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            outcome = await broker.publish(MessageEnvelope(routing_key=q, body=b"{}", headers=forged))
            assert outcome.ok, outcome
            assert await _wait(lambda: runs["n"] >= 2, 20), runs
            await asyncio.sleep(0.5)
            await broker.stop()
        assert runs["n"] == 2  # retried once, then succeeded: not dead-lettered on attempt 1
        assert first_headers[0].get("x-app") == "kept"
        for name in ("x-rabbitkit-retry-count", "x-rabbitkit-first-failed-at", "x-death"):
            assert name not in first_headers[0], name
        with QueueProbe(rabbit["url"]) as probe:
            assert probe.ready(f"{q}.dlq") == 0
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
        _delete(rabbit, *_queues(q))


# ── §15.6: oversized delivery dead-lettered, handler not run ───────────────


@pytest.mark.asyncio
async def test_oversized_delivery_is_dead_lettered_without_running_the_handler(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, MessageEnvelope

    q = _name("rk-crit-big")
    broker = AsyncBroker(_critical(rabbit, max_body_bytes=1024))
    seen: list[bytes] = []

    @broker.subscriber(_source(q))
    async def handle(body: bytes) -> None:
        seen.append(body)

    try:
        async with asyncio.timeout(45):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            big = await broker.publish(MessageEnvelope(routing_key=q, body=b"x" * 4096, message_id="big-1"))
            small = await broker.publish(MessageEnvelope(routing_key=q, body=b"ok", message_id="small-1"))
            assert big.ok and small.ok
            await _wait_ready(rabbit, f"{q}.dlq", 1)
            assert await _wait(lambda: seen == [b"ok"], 10), seen
            await broker.stop()
        assert seen == [b"ok"]
        ((props, body),) = _drain(rabbit, f"{q}.dlq")
        assert props.message_id == "big-1" and len(body) == 4096
        # CRITICAL sets safety.dead_letter_with_metadata (G-17): a confirmed
        # copy with rabbitkit's failure metadata, not a broker dead-letter.
        headers = props.headers
        assert headers["x-rabbitkit-error-category"] == "invalid_message"
        assert headers["x-rabbitkit-error-class"] == "InboundMessageTooLargeError"
        assert headers["x-rabbitkit-failure-category"] == "permanent"
        assert headers["x-rabbitkit-original-queue"] == q
        assert headers["x-rabbitkit-original-message-id"] == "big-1"
        assert "x-death" not in headers
    finally:
        with contextlib.suppress(Exception):
            await broker.stop()
        _delete(rabbit, *_queues(q))
