"""Tests for subscriber filtering (F1)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import AckPolicy


def _make_message(**kwargs: object) -> RabbitMessage:
    """Create a RabbitMessage with settlement mocks wired in."""
    defaults: dict[str, object] = {
        "body": b'{"key": "value"}',
        "routing_key": "test.key",
        "exchange": "test-exchange",
        "headers": {},
    }
    defaults.update(kwargs)
    msg = RabbitMessage(**defaults)  # type: ignore[arg-type]
    msg._ack_fn = MagicMock()
    msg._nack_fn = MagicMock()
    msg._reject_fn = MagicMock()
    msg._ack_async_fn = AsyncMock()
    msg._nack_async_fn = AsyncMock()
    msg._reject_async_fn = AsyncMock()
    return msg


def _make_route(
    handler: object | None = None,
    filter_fn: object | None = None,
    ack_policy: AckPolicy = AckPolicy.AUTO,
) -> RouteDefinition:
    """Create a minimal RouteDefinition for testing."""
    if handler is None:
        handler = MagicMock(return_value=None)
    return RouteDefinition(
        name="test-route",
        queue=RabbitQueue(name="test-queue"),
        exchange=None,
        handler=handler,  # type: ignore[arg-type]
        ack_policy=ack_policy,
        filter_fn=filter_fn,  # type: ignore[arg-type]
    )


class TestFilterSync:
    """Test filter_fn in sync pipeline."""

    def test_filter_none_passes_all_messages(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(handler=handler, filter_fn=None)
        msg = _make_message()

        HandlerPipeline().process_sync(route, msg)
        handler.assert_called_once()

    def test_filter_returns_true_passes_message(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(handler=handler, filter_fn=lambda m: True)
        msg = _make_message()

        HandlerPipeline().process_sync(route, msg)
        handler.assert_called_once()

    def test_filter_returns_false_drops_message(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(handler=handler, filter_fn=lambda m: False)
        msg = _make_message()

        HandlerPipeline().process_sync(route, msg)

        handler.assert_not_called()
        # nack(requeue) is called positionally by message.nack()
        msg._nack_fn.assert_called_once_with(False)

    def test_filter_drops_already_settled_message(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(handler=handler, filter_fn=lambda m: False)
        msg = _make_message()
        msg.ack()  # settle before filter

        HandlerPipeline().process_sync(route, msg)

        handler.assert_not_called()
        msg._nack_fn.assert_not_called()

    def test_filter_runs_before_ack_first(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(
            handler=handler,
            filter_fn=lambda m: False,
            ack_policy=AckPolicy.ACK_FIRST,
        )
        msg = _make_message()

        HandlerPipeline().process_sync(route, msg)

        handler.assert_not_called()
        # ACK_FIRST ack should NOT fire — filter runs first
        msg._ack_fn.assert_not_called()
        msg._nack_fn.assert_called_once_with(False)

    def test_filter_receives_message_object(self) -> None:
        received: list[RabbitMessage] = []

        def capture(m: RabbitMessage) -> bool:
            received.append(m)
            return True

        handler = MagicMock(return_value=None)
        route = _make_route(handler=handler, filter_fn=capture)
        msg = _make_message(body=b"test-body")

        HandlerPipeline().process_sync(route, msg)

        assert len(received) == 1
        assert received[0].body == b"test-body"

    def test_filter_by_routing_key_passes(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(
            handler=handler,
            filter_fn=lambda m: m.routing_key.startswith("orders."),
        )

        msg = _make_message(routing_key="orders.created")
        HandlerPipeline().process_sync(route, msg)
        handler.assert_called_once()

    def test_filter_by_routing_key_drops(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(
            handler=handler,
            filter_fn=lambda m: m.routing_key.startswith("orders."),
        )

        msg = _make_message(routing_key="users.updated")
        HandlerPipeline().process_sync(route, msg)
        handler.assert_not_called()

    def test_filter_by_header(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(
            handler=handler,
            filter_fn=lambda m: m.headers.get("x-priority") == "high",
        )

        msg = _make_message(headers={"x-priority": "high"})
        HandlerPipeline().process_sync(route, msg)
        handler.assert_called_once()

    def test_filter_by_body_content(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(
            handler=handler,
            filter_fn=lambda m: b"order" in m.body,
        )

        msg = _make_message(body=b'{"type": "order"}')
        HandlerPipeline().process_sync(route, msg)
        handler.assert_called_once()


class TestFilterAsync:
    """Test filter_fn in async pipeline."""

    @pytest.mark.asyncio
    async def test_filter_returns_false_drops_async(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(handler=handler, filter_fn=lambda m: False)
        msg = _make_message()

        await HandlerPipeline().process_async(route, msg)

        handler.assert_not_called()
        msg._nack_async_fn.assert_called_once_with(False)

    @pytest.mark.asyncio
    async def test_filter_none_passes_async(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(handler=handler, filter_fn=None)
        msg = _make_message()

        await HandlerPipeline().process_async(route, msg)
        handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_filter_runs_before_ack_first_async(self) -> None:
        handler = MagicMock(return_value=None)
        route = _make_route(
            handler=handler,
            filter_fn=lambda m: False,
            ack_policy=AckPolicy.ACK_FIRST,
        )
        msg = _make_message()

        await HandlerPipeline().process_async(route, msg)

        handler.assert_not_called()
        msg._ack_async_fn.assert_not_called()
        msg._nack_async_fn.assert_called_once_with(False)


class TestFilterRegistration:
    """Test that filter_fn flows through registration."""

    def test_route_definition_has_filter_fn_field(self) -> None:
        route = _make_route(filter_fn=lambda m: True)
        assert route.filter_fn is not None

    def test_route_definition_filter_fn_defaults_none(self) -> None:
        route = _make_route()
        assert route.filter_fn is None
