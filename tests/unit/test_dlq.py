"""Tests for dlq.py — DLQInspector."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.dlq import DLQInspector

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(
    body: bytes = b"dlq-msg",
    routing_key: str = "orders",
    headers: dict[str, Any] | None = None,
    **kwargs: Any,
) -> RabbitMessage:
    defaults: dict[str, Any] = {
        "body": body,
        "routing_key": routing_key,
        "headers": headers or {},
        "message_id": "msg-001",
    }
    defaults.update(kwargs)
    msg = RabbitMessage(**defaults)
    msg._ack_fn = MagicMock()
    msg._nack_fn = MagicMock()
    return msg


class _FakeTransport:
    """In-memory transport mock for DLQ tests."""

    def __init__(self, messages: list[RabbitMessage] | None = None) -> None:
        self._queue: list[RabbitMessage] = list(messages or [])
        self.published: list[MessageEnvelope] = []
        self.purge_count = 0

    def basic_get(self, queue: str) -> RabbitMessage | None:
        if self._queue:
            return self._queue.pop(0)
        return None

    def publish(self, envelope: MessageEnvelope) -> None:
        self.published.append(envelope)

    def purge_queue(self, queue: str) -> int:
        count = self.purge_count
        return count


class _FakeAsyncTransport:
    """Async transport mock for DLQ tests."""

    def __init__(self, messages: list[RabbitMessage] | None = None) -> None:
        self._queue: list[RabbitMessage] = list(messages or [])
        self.published: list[MessageEnvelope] = []
        self.purge_count = 0

    async def basic_get(self, queue: str) -> RabbitMessage | None:
        if self._queue:
            return self._queue.pop(0)
        return None

    async def publish(self, envelope: MessageEnvelope) -> None:
        self.published.append(envelope)

    async def purge_queue(self, queue: str) -> int:
        return self.purge_count


# ── peek tests ───────────────────────────────────────────────────────────


class TestPeek:
    def test_peek_returns_messages(self) -> None:
        """Peek fetches messages and nacks them for requeue."""
        msgs = [_make_message(body=b"m1"), _make_message(body=b"m2")]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        result = inspector.peek("dlq", limit=10)

        assert len(result) == 2
        assert result[0].body == b"m1"
        assert result[1].body == b"m2"
        # All messages nacked with requeue=True
        result[0]._nack_fn.assert_called_once_with(True)
        result[1]._nack_fn.assert_called_once_with(True)

    def test_peek_empty_queue(self) -> None:
        """Peek on empty queue returns empty list."""
        transport = _FakeTransport(messages=[])
        inspector = DLQInspector(transport)

        result = inspector.peek("dlq", limit=10)
        assert result == []

    def test_peek_respects_limit(self) -> None:
        """Peek returns at most `limit` messages."""
        msgs = [_make_message(body=f"m{i}".encode()) for i in range(10)]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        result = inspector.peek("dlq", limit=3)

        assert len(result) == 3

    def test_peek_nacks_for_requeue(self) -> None:
        """Peek nacks all messages for requeue."""
        msgs = [_make_message()]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        result = inspector.peek("dlq")

        assert len(result) == 1
        result[0]._nack_fn.assert_called_once_with(True)


# ── replay tests ─────────────────────────────────────────────────────────


class TestReplay:
    def test_replay_publishes_to_target(self) -> None:
        """Replay publishes messages to the target queue."""
        msgs = [_make_message(body=b"replay-me")]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        count = inspector.replay("dlq", target_queue="orders")

        assert count == 1
        assert len(transport.published) == 1
        assert transport.published[0].routing_key == "orders"
        assert transport.published[0].body == b"replay-me"

    def test_replay_with_predicate(self) -> None:
        """Replay only replays messages matching predicate."""
        msgs = [
            _make_message(body=b"match", headers={"x-error": "timeout"}),
            _make_message(body=b"skip", headers={"x-error": "permanent"}),
        ]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        count = inspector.replay(
            "dlq",
            predicate=lambda m: m.headers.get("x-error") == "timeout",
            target_queue="orders",
        )

        assert count == 1
        assert transport.published[0].body == b"match"
        # Non-matching message was nacked for requeue
        msgs[1]._nack_fn.assert_called_once_with(True)

    def test_replay_acks_source(self) -> None:
        """Replay acks the original message after publishing."""
        msgs = [_make_message()]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        inspector.replay("dlq", target_queue="target")

        msgs[0]._ack_fn.assert_called_once()

    def test_replay_returns_count(self) -> None:
        """Replay returns the number of replayed messages."""
        msgs = [_make_message() for _ in range(3)]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        count = inspector.replay("dlq", target_queue="target")
        assert count == 3

    def test_replay_empty_queue(self) -> None:
        """Replay on empty queue returns 0."""
        transport = _FakeTransport(messages=[])
        inspector = DLQInspector(transport)

        count = inspector.replay("dlq", target_queue="target")
        assert count == 0

    def test_replay_uses_original_queue_header(self) -> None:
        """Replay uses x-rabbitkit-original-queue when no target specified."""
        msgs = [_make_message(headers={"x-rabbitkit-original-queue": "original-q"})]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        count = inspector.replay("dlq")

        assert count == 1
        assert transport.published[0].routing_key == "original-q"

    def test_replay_preserves_headers(self) -> None:
        """Replay preserves original message headers."""
        msgs = [_make_message(headers={"x-tenant": "acme", "x-retry": "3"})]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        inspector.replay("dlq", target_queue="target")

        published = transport.published[0]
        assert published.headers["x-tenant"] == "acme"
        assert published.headers["x-retry"] == "3"


# ── purge tests ──────────────────────────────────────────────────────────


class TestPurge:
    def test_purge_returns_count(self) -> None:
        """Purge returns the count from transport."""
        transport = _FakeTransport()
        transport.purge_count = 42
        inspector = DLQInspector(transport)

        count = inspector.purge("dlq")
        assert count == 42


# ── async variants ───────────────────────────────────────────────────────


class TestAsync:
    async def test_peek_async(self) -> None:
        """Async peek fetches and requeues messages."""
        msg = _make_message(body=b"async-peek")
        # Set up async ack/nack fns
        nack_called = []

        async def async_nack(requeue: bool = True) -> None:
            nack_called.append(requeue)

        msg._nack_async_fn = async_nack

        transport = _FakeAsyncTransport(messages=[msg])
        inspector = DLQInspector(transport)

        result = await inspector.peek_async("dlq", limit=5)

        assert len(result) == 1
        assert result[0].body == b"async-peek"
        assert nack_called == [True]

    async def test_replay_async(self) -> None:
        """Async replay publishes and acks."""
        msg = _make_message(body=b"async-replay")
        ack_called = False

        async def async_ack() -> None:
            nonlocal ack_called
            ack_called = True

        msg._ack_async_fn = async_ack

        transport = _FakeAsyncTransport(messages=[msg])
        inspector = DLQInspector(transport)

        count = await inspector.replay_async("dlq", target_queue="target")

        assert count == 1
        assert len(transport.published) == 1
        assert ack_called is True

    async def test_replay_async_empty(self) -> None:
        """Async replay on empty queue returns 0."""
        transport = _FakeAsyncTransport(messages=[])
        inspector = DLQInspector(transport)

        count = await inspector.replay_async("dlq", target_queue="target")
        assert count == 0

    async def test_purge_async(self) -> None:
        """Async purge delegates to transport."""
        transport = _FakeAsyncTransport()
        transport.purge_count = 15
        inspector = DLQInspector(transport)

        count = await inspector.purge_async("dlq")
        assert count == 15


class TestReplayAsyncPredicate:
    async def test_replay_async_predicate_nacks_rejected_message(self) -> None:
        """Lines 181-183: predicate rejects a message — nack_async + continue."""
        from unittest.mock import AsyncMock

        from rabbitkit.dlq import DLQInspector

        msg = _make_message(routing_key="orders")
        nack_async_mock = AsyncMock()
        msg._nack_async_fn = nack_async_mock

        class _AsyncTransport:
            def __init__(self, messages: list) -> None:
                self._msgs = list(messages)
                self.published: list = []

            async def basic_get(self, queue: str) -> RabbitMessage | None:
                return self._msgs.pop(0) if self._msgs else None

            async def publish(self, envelope: object) -> None:
                self.published.append(envelope)

        transport = _AsyncTransport([msg])
        inspector = DLQInspector(transport)  # type: ignore[arg-type]

        # predicate always rejects
        count = await inspector.replay_async("dlq", predicate=lambda m: False)

        assert count == 0
        nack_async_mock.assert_called_once_with(True)
