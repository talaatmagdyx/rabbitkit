"""Real-broker coverage for the retry / DLQ reliability audit.

* R-D1  handler settlement signals are never retried
* R-D2  a terminal failure reaches the DLQ with the CURRENT attempt's metadata
* R-D3  a failing delay-queue handoff does not hot-loop the handler
* R-D5 / 12.2  retry copies keep timestamp, message id and trace context
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


def _publish(rabbit: dict[str, Any], queue: str, **props: Any) -> None:
    import pika

    with _pika(rabbit) as ch:
        ch.confirm_delivery()
        ch.basic_publish("", queue, b"{}", pika.BasicProperties(delivery_mode=2, **props))


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


# ── R-D1 ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("signal_name", ["ack", "reject"])
async def test_settlement_signals_are_not_retried(rabbit: dict[str, Any], signal_name: str) -> None:
    from rabbitkit import AckMessage, AsyncBroker, RabbitQueue, RejectMessage, RetryConfig
    from rabbitkit.core.types import ErrorSeverity

    q = _name("rk-sig")
    broker = AsyncBroker(
        _config(rabbit, retry=RetryConfig(max_retries=2, delays=(1, 1), unknown_policy=ErrorSeverity.TRANSIENT))
    )
    calls = {"n": 0}

    @broker.subscriber(RabbitQueue(q))
    async def handle(body: bytes) -> None:
        calls["n"] += 1
        raise AckMessage() if signal_name == "ack" else RejectMessage(requeue=False)

    try:
        async with asyncio.timeout(30):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            _publish(rabbit, q, message_id="sig-1")
            await _wait(lambda: calls["n"] >= 1, 10)
            await asyncio.sleep(2.5)  # > every delay tier: a retry would have run by now
            await broker.stop()
        assert calls["n"] == 1
        with QueueProbe(rabbit["url"]) as probe:
            assert probe.ready(q) == 0
            assert probe.ready(f"{q}.dlq") == (0 if signal_name == "ack" else 1)
            assert probe.ready(f"{q}.retry.1") == 0
    finally:
        _delete(rabbit, *_retry_queues(q, 2))


# ── R-D2 + R-D5 + 12.2 ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_first_attempt_permanent_failure_has_terminal_metadata(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitQueue, RetryConfig

    q = _name("rk-perm")
    broker = AsyncBroker(_config(rabbit, retry=RetryConfig(max_retries=2, delays=(1, 1))))
    calls = {"n": 0}

    @broker.subscriber(RabbitQueue(q))
    async def handle(body: bytes) -> None:
        calls["n"] += 1
        raise ValueError("bad payload password=hunter2")

    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        async with asyncio.timeout(30):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            _publish(rabbit, q, message_id="perm-1", correlation_id="corr-1", timestamp=int(stamp.timestamp()))
            with QueueProbe(rabbit["url"]) as probe:
                assert await _wait(lambda: probe.ready(f"{q}.dlq") == 1, 15)
            await broker.stop()
        [(props, _)] = _drain(rabbit, f"{q}.dlq")
        h = props.headers
        assert calls["n"] == 1
        assert h["x-rabbitkit-error-class"] == "ValueError"
        assert h["x-rabbitkit-failure-category"] == "permanent"
        assert h["x-rabbitkit-attempts"] == 1
        assert "hunter2" not in h["x-rabbitkit-error-message"]
        assert h["x-rabbitkit-original-queue"] == q
        assert h["x-rabbitkit-original-routing-key"] == q
        assert h["x-rabbitkit-original-message-id"] == "perm-1"
        assert h["x-rabbitkit-dead-lettered-at"] and h["x-rabbitkit-first-failed-at"]
        assert "x-death" not in h  # moved by rabbitkit's confirmed publish, not the broker DLX
        assert props.message_id == "perm-1" and props.correlation_id == "corr-1"
        assert props.timestamp == int(stamp.timestamp())
        assert _drain(rabbit, q) == []
    finally:
        _delete(rabbit, *_retry_queues(q, 2))


@pytest.mark.asyncio
async def test_identity_and_attempts_survive_every_retry(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitMessage, RabbitQueue, RetryConfig

    q = _name("rk-ident")
    broker = AsyncBroker(_config(rabbit, retry=RetryConfig(max_retries=2, delays=(1, 1))))
    seen: list[RabbitMessage] = []

    @broker.subscriber(RabbitQueue(q))
    async def handle(body: bytes, msg: RabbitMessage) -> None:
        seen.append(msg)
        raise TimeoutError(f"attempt {len(seen)}")

    ts = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp())
    try:
        async with asyncio.timeout(40):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            _publish(
                rabbit,
                q,
                message_id="id-1",
                correlation_id="corr-1",
                timestamp=ts,
                headers={"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "x-idem": "k1"},
            )
            with QueueProbe(rabbit["url"]) as probe:
                assert await _wait(lambda: probe.ready(f"{q}.dlq") == 1, 20)
            await broker.stop()
        assert len(seen) == 3
        for msg in seen:
            assert msg.message_id == "id-1" and msg.correlation_id == "corr-1"
            assert msg.timestamp is not None and int(msg.timestamp.timestamp()) == ts
            assert msg.timestamp.tzinfo is not None  # pamqp 3.2 (aio-pika 9.1) decodes a naive datetime
            assert msg.headers["traceparent"].startswith("00-0af7")
        assert [int(m.headers.get("x-rabbitkit-retry-count", 0)) for m in seen] == [0, 1, 2]
        [(props, _)] = _drain(rabbit, f"{q}.dlq")
        h = props.headers
        assert props.message_id == "id-1" and props.timestamp == ts
        assert h["x-rabbitkit-attempts"] == 3
        assert h["x-rabbitkit-failure-category"] == "exhausted"
        assert h["x-rabbitkit-error-message"] == "attempt 3"  # the FINAL attempt, not the previous one
        assert h["x-rabbitkit-first-failed-at"] == seen[1].headers["x-rabbitkit-first-failed-at"]
    finally:
        _delete(rabbit, *_retry_queues(q, 2))


@pytest.mark.asyncio
async def test_id_less_message_keeps_one_id_across_retries(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitMessage, RabbitQueue, RetryConfig

    q = _name("rk-noid")
    broker = AsyncBroker(_config(rabbit, retry=RetryConfig(max_retries=2, delays=(1, 1))))
    seen: list[RabbitMessage] = []

    @broker.subscriber(RabbitQueue(q))
    async def handle(body: bytes, msg: RabbitMessage) -> None:
        seen.append(msg)
        raise TimeoutError("t")

    try:
        async with asyncio.timeout(40):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            _publish(rabbit, q)  # pika sets no message_id
            with QueueProbe(rabbit["url"]) as probe:
                assert await _wait(lambda: probe.ready(f"{q}.dlq") == 1, 20)
            await broker.stop()
        assert seen[0].message_id is None
        stable = seen[1].message_id
        assert stable and seen[2].message_id == stable
        assert seen[1].headers["x-rabbitkit-original-message-id"] == stable
        [(props, _)] = _drain(rabbit, f"{q}.dlq")
        assert props.message_id == stable
    finally:
        _delete(rabbit, *_retry_queues(q, 2))


# ── R-D3 ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_failed_handoff_is_braked_and_message_kept(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitQueue, RetryConfig

    q = _name("rk-hand")
    broker = AsyncBroker(_config(rabbit, retry=RetryConfig(max_retries=2, delays=(1, 1))))
    calls = {"n": 0}

    @broker.subscriber(RabbitQueue(q))
    async def handle(body: bytes) -> None:
        calls["n"] += 1
        raise TimeoutError("transient")

    try:
        async with asyncio.timeout(40):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            _delete(rabbit, f"{q}.retry.1")  # the delay queue disappears at runtime
            _publish(rabbit, q, message_id="hand-1")
            await asyncio.sleep(5)
            executions = calls["n"]
            await broker.stop()
        # 0.5s initial backoff doubling: ~4 handoffs in 5 s. The old code ran
        # the handler ~930 times per second here.
        assert 1 <= executions <= 8, executions
        with QueueProbe(rabbit["url"]) as probe:
            assert probe.ready(q) == 1  # never acked, never lost
    finally:
        _delete(rabbit, *_retry_queues(q, 2))
