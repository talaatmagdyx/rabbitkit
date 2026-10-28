"""Real-broker coverage for R-D7: DeduplicationMiddleware must never mark a
message processed before its result was published, or a failed result
publish's redelivery is acked as a duplicate and the result is lost."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe, await_consumers

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


class _AsyncRedis:
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    async def set(self, key: str, value: Any, nx: bool = False, ex: int | None = None) -> bool | None:
        if nx and key in self.data:
            return None
        self.data[key] = value
        return True

    async def exists(self, key: str) -> int:
        return int(key in self.data)

    async def get(self, key: str) -> Any:
        return self.data.get(key)

    async def delete(self, key: str) -> int:
        return int(self.data.pop(key, None) is not None)


@pytest.mark.asyncio
@pytest.mark.parametrize(("policy", "store"), [("on_success", False), ("claim", False), ("claim", True)])
async def test_failed_result_publish_is_retried_not_deduplicated(
    rabbit: dict[str, Any], policy: str, store: bool
) -> None:
    import pika

    from rabbitkit import AsyncBroker, DeduplicationConfig, DeduplicationMiddleware, RabbitQueue
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig
    from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
    from rabbitkit.middleware.base import BaseMiddleware

    class FailFirstResult(BaseMiddleware):
        def __init__(self) -> None:
            self.calls = 0

        async def publish_scope_async(self, call_next: Any, envelope: MessageEnvelope) -> Any:
            self.calls += 1
            if self.calls == 1:
                return PublishOutcome(status=PublishStatus.ERROR, exchange="", routing_key=envelope.routing_key)
            return await call_next(envelope)

    src = f"rk-dedup-{uuid.uuid4().hex[:8]}"
    results = f"{src}.results"
    redis = _AsyncRedis()
    dedup = DeduplicationMiddleware(redis, DeduplicationConfig(mark_policy=policy, store_results=store))
    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(rabbit["url"])))
    handled: list[int] = []

    @broker.subscriber(RabbitQueue(src), middlewares=[dedup, FailFirstResult()])
    @broker.publisher(exchange="", routing_key=results)
    async def handle(body: bytes) -> dict[str, bool]:
        handled.append(1)
        return {"ok": True}

    try:
        async with asyncio.timeout(30):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], src)
            await asyncio.to_thread(_declare, rabbit, results)
            await broker.publish(routing_key=src, body=b"{}")
            with QueueProbe(rabbit["url"]) as probe:
                for _ in range(200):
                    if probe.ready(results) == 1:
                        break
                    await asyncio.sleep(0.05)
                assert probe.ready(results) == 1, "the handler's result was lost"
                await asyncio.sleep(0.3)
                assert probe.ready(src) == 0
            await broker.stop()
        # stored result: the redelivery replays it without re-running the handler
        assert len(handled) == (1 if store else 2)
    finally:
        with contextlib.closing(pika.BlockingConnection(pika.URLParameters(rabbit["url"]))) as conn:
            ch = conn.channel()
            for q in (src, results, f"{src}.dlq"):
                with contextlib.suppress(Exception):
                    ch.queue_delete(q)


def _declare(rabbit: dict[str, Any], queue: str) -> None:
    import pika

    with contextlib.closing(pika.BlockingConnection(pika.URLParameters(rabbit["url"]))) as conn:
        conn.channel().queue_declare(queue, durable=True)
