"""Shared fixtures for the order-service example tests."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from examples.order_service.handlers import register_order_handlers
from examples.order_service.services import get_order_service
from rabbitkit.core.message import RabbitMessage
from rabbitkit.di.resolver import DIResolver
from rabbitkit.serialization.pipeline import JsonParser, PydanticDecoder, SerializationPipeline
from rabbitkit.testing import TestBroker


@pytest.fixture(autouse=True)
def _reset_service() -> Iterator[None]:
    """Each test gets a clean in-memory service (module-level singleton)."""
    get_order_service().reset()
    yield
    get_order_service().reset()


@pytest.fixture
def nack_spy(monkeypatch: pytest.MonkeyPatch) -> list[bool]:
    """Record the ``requeue`` value of every async nack (proves requeue=False → DLQ)."""
    calls: list[bool] = []
    original = RabbitMessage.nack_async

    async def spy(self: RabbitMessage, requeue: bool = True) -> None:
        calls.append(requeue)
        await original(self, requeue)

    monkeypatch.setattr(RabbitMessage, "nack_async", spy)
    return calls


@pytest.fixture
def make_broker() -> Callable[..., TestBroker]:
    """Factory: a started TestBroker wired exactly like prod (serializer + DI)."""

    def _make(middlewares: list[Any] | None = None) -> TestBroker:
        broker = TestBroker(
            serializer=SerializationPipeline(JsonParser(), PydanticDecoder()),
            di_resolver=DIResolver(),
        )
        register_order_handlers(broker, middlewares=middlewares or [])
        broker.start()
        return broker

    return _make


def order_body(order_id: str = "o1", tenant_id: str = "t-1") -> bytes:
    return json.dumps(
        {
            "order_id": order_id,
            "tenant_id": tenant_id,
            "amount_cents": 100,
            "currency": "USD",
            "created_at": "2026-01-01T00:00:00Z",
            "event_version": 1,
        }
    ).encode()
