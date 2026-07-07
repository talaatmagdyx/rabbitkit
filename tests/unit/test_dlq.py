"""Tests for dlq.py — DLQInspector."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.dlq import DLQInspector, ReplayResult

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

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        # L3: a None-returning publish is now treated as UNVERIFIED (failure)
        # — the fake must return a real OK outcome like both transports do.
        self.published.append(envelope)
        return PublishOutcome(status=PublishStatus.CONFIRMED)

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

    async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        # L3: see the sync fake — None outcome = unverified = failure.
        self.published.append(envelope)
        return PublishOutcome(status=PublishStatus.CONFIRMED)

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

    def test_replay_preserves_message_properties(self) -> None:
        """Replay used to silently drop priority/expiration/type/app_id/
        user_id/reply_to -- e.g. a priority-queue message lost its priority
        on replay, and an RPC request's reply_to never survived the replay
        for the eventual reply to route back."""
        msgs = [
            _make_message(
                reply_to="amq.rabbitmq.reply-to",
                priority=9,
                expiration="30000",
                type="order.created",
                app_id="order-service",
                user_id="guest",
            )
        ]
        transport = _FakeTransport(messages=msgs)
        inspector = DLQInspector(transport)

        inspector.replay("dlq", target_queue="target")

        published = transport.published[0]
        assert published.reply_to == "amq.rabbitmq.reply-to"
        assert published.priority == 9
        assert published.expiration == "30000"
        assert published.type == "order.created"
        assert published.app_id == "order-service"
        assert published.user_id == "guest"


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


# ── C2 regression: failed republish must NOT ack the DLQ original ─────────


class _OutcomeTransport(_FakeTransport):
    """Fake transport whose publish returns a real PublishOutcome, failing
    when ``fail_when(envelope)`` is True."""

    def __init__(
        self,
        messages: list[RabbitMessage] | None = None,
        fail_when: Any = None,
    ) -> None:
        super().__init__(messages)
        self._fail_when = fail_when or (lambda env: False)

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        self.published.append(envelope)
        if self._fail_when(envelope):
            return PublishOutcome(status=PublishStatus.ERROR)
        return PublishOutcome(status=PublishStatus.CONFIRMED)


class _AsyncOutcomeTransport(_FakeAsyncTransport):
    def __init__(
        self,
        messages: list[RabbitMessage] | None = None,
        fail_when: Any = None,
    ) -> None:
        super().__init__(messages)
        self._fail_when = fail_when or (lambda env: False)

    async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        self.published.append(envelope)
        if self._fail_when(envelope):
            return PublishOutcome(status=PublishStatus.ERROR)
        return PublishOutcome(status=PublishStatus.CONFIRMED)


class TestReplayPublishOutcome:
    def test_failed_publish_keeps_message_on_dlq(self) -> None:
        """C2 regression: a failed republish must nack-requeue the original
        (message stays on the DLQ), never ack it."""
        msg = _make_message()
        transport = _OutcomeTransport(messages=[msg], fail_when=lambda env: True)
        inspector = DLQInspector(transport)

        result = inspector.replay("dlq")

        msg._ack_fn.assert_not_called()
        msg._nack_fn.assert_called_once_with(True)  # stays on the DLQ
        assert result == 0  # nothing replayed (int-compatible)
        assert result.failed == 1

    def test_partial_failure_counts_and_settles_correctly(self) -> None:
        """One publish fails, one succeeds: the failure is requeued, the
        success is acked, and both are reported."""
        ok_msg = _make_message(routing_key="good")
        bad_msg = _make_message(routing_key="bad")
        transport = _OutcomeTransport(
            messages=[ok_msg, bad_msg],
            fail_when=lambda env: env.routing_key == "bad",
        )
        inspector = DLQInspector(transport)

        result = inspector.replay("dlq")

        ok_msg._ack_fn.assert_called_once()
        bad_msg._ack_fn.assert_not_called()
        bad_msg._nack_fn.assert_called_once_with(True)
        assert result == 1
        assert result.failed == 1

    def test_replay_envelope_is_mandatory(self) -> None:
        """Republishes use mandatory=True so an unroutable target comes back
        as RETURNED instead of being confirmed into the void."""
        transport = _OutcomeTransport(messages=[_make_message()])
        inspector = DLQInspector(transport)

        inspector.replay("dlq")

        assert transport.published[0].mandatory is True

    def test_reset_retry_count_strips_header(self) -> None:
        """reset_retry_count=True grants a fresh retry ladder; default
        preserves headers verbatim."""
        headers = {"x-rabbitkit-retry-count": 4, "x-other": "kept"}
        msg1 = _make_message(headers=dict(headers))
        msg2 = _make_message(headers=dict(headers))
        transport = _OutcomeTransport(messages=[msg1])
        inspector = DLQInspector(transport)

        inspector.replay("dlq", reset_retry_count=True)
        assert "x-rabbitkit-retry-count" not in transport.published[0].headers
        assert transport.published[0].headers["x-other"] == "kept"

        transport2 = _OutcomeTransport(messages=[msg2])
        DLQInspector(transport2).replay("dlq")  # default: preserved
        assert transport2.published[0].headers["x-rabbitkit-retry-count"] == 4

    def test_replay_result_is_int_compatible(self) -> None:
        result = ReplayResult(3, failed=2, requeued=1)
        assert result == 3
        assert isinstance(result, int)
        assert result + 1 == 4
        assert result.failed == 2
        assert result.requeued == 1
        assert repr(result) == "ReplayResult(replayed=3, failed=2, requeued=1)"

    def test_predicate_rejections_counted_as_requeued(self) -> None:
        transport = _OutcomeTransport(messages=[_make_message(), _make_message()])
        inspector = DLQInspector(transport)

        result = inspector.replay("dlq", predicate=lambda m: False)

        assert result == 0
        assert result.requeued == 2
        assert result.failed == 0

    async def test_async_failed_publish_keeps_message_on_dlq(self) -> None:
        from unittest.mock import AsyncMock

        msg = _make_message()
        ack_async = AsyncMock()
        nack_async = AsyncMock()
        msg._ack_async_fn = ack_async
        msg._nack_async_fn = nack_async

        transport = _AsyncOutcomeTransport(messages=[msg], fail_when=lambda env: True)
        inspector = DLQInspector(transport)

        result = await inspector.replay_async("dlq")

        ack_async.assert_not_called()
        nack_async.assert_called_once_with(True)
        assert result == 0
        assert result.failed == 1

    async def test_async_reset_retry_count_strips_header(self) -> None:
        from unittest.mock import AsyncMock

        msg = _make_message(headers={"x-rabbitkit-retry-count": 4})
        msg._ack_async_fn = AsyncMock()
        transport = _AsyncOutcomeTransport(messages=[msg])
        inspector = DLQInspector(transport)

        result = await inspector.replay_async("dlq", reset_retry_count=True)

        assert result == 1
        assert "x-rabbitkit-retry-count" not in transport.published[0].headers


class TestReplayLimit:
    """L2: `limit=` bounds the fetch loop — the defense against a live
    consumer dead-lettering replayed messages back into the same DLQ faster
    than the drain completes (the held-until-drained guarantee covers
    self-refetch, not genuine re-arrival)."""

    def test_limit_bounds_fetches(self) -> None:
        transport = _FakeTransport(messages=[_make_message(f"m{i}".encode()) for i in range(5)])
        inspector = DLQInspector(transport)
        result = inspector.replay("dlq", limit=2)
        assert int(result) == 2
        assert len(transport.published) == 2

    async def test_limit_bounds_fetches_async(self) -> None:
        transport = _FakeAsyncTransport(messages=[_make_message(f"m{i}".encode()) for i in range(5)])
        inspector = DLQInspector(transport)
        result = await inspector.replay_async("dlq", limit=3)
        assert int(result) == 3
        assert len(transport.published) == 3

    def test_none_limit_drains_all(self) -> None:
        transport = _FakeTransport(messages=[_make_message(b"a"), _make_message(b"b")])
        inspector = DLQInspector(transport)
        assert int(inspector.replay("dlq")) == 2
