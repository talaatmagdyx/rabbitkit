"""Tests for async_/consumers.py — ConsumerSupervisor (G-26) with in-memory fakes."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aiormq.exceptions import DuplicateConsumerTag

from rabbitkit.async_ import consumers as consumers_mod
from rabbitkit.async_.consumers import (
    RESUBSCRIBE_BACKOFF_INITIAL,
    RESUBSCRIBE_BACKOFF_MAX,
    ConsumerRecord,
    ConsumerSupervisor,
)

# ── fakes ─────────────────────────────────────────────────────────────────


class _Frame:
    def __init__(self, consumer_tag: str) -> None:
        self.consumer_tag = consumer_tag


class _Underlay:
    """aiormq channel: consumer table, cancel callbacks (aiormq >= 7 only)."""

    def __init__(self, *, callbacks: bool = True) -> None:
        self.consumers: dict[str, Any] = {}
        self.is_closed = False
        self.basic_cancel = AsyncMock()
        if callbacks:
            self.on_consumer_cancel_callbacks: set[Any] = set()

    def broker_cancel(self, tag: str) -> None:
        self.consumers.pop(tag, None)
        for cb in list(getattr(self, "on_consumer_cancel_callbacks", ())):
            cb(_Frame(tag))


class _Channel:
    def __init__(self, underlay: _Underlay) -> None:
        self.underlay = underlay
        self.is_closed = False
        self.fail = False

    async def get_underlay_channel(self) -> _Underlay:
        if self.fail:
            raise RuntimeError("Channel was not opened")
        return self.underlay


class _Queue:
    def __init__(self, underlay: _Underlay) -> None:
        self.underlay = underlay
        self.calls: list[dict[str, Any]] = []
        self.error: BaseException | None = None
        self.on_consume: Any = None

    async def consume(self, callback: Any, *, consumer_tag: str, no_ack: bool, robust: bool) -> str:
        self.calls.append({"tag": consumer_tag, "no_ack": no_ack, "robust": robust})
        if self.on_consume is not None:
            self.on_consume()
        if self.error is not None:
            raise self.error
        self.underlay.consumers[consumer_tag] = callback
        return consumer_tag


async def _noop(_message: Any) -> None:
    return None


class _Harness:
    def __init__(self, *, callbacks: bool = True, exists: bool = True) -> None:
        self.underlay = _Underlay(callbacks=callbacks)
        self.channel = _Channel(self.underlay)
        self.queue = _Queue(self.underlay)
        self.exists = exists
        self.probes: list[str] = []
        self.cancelled: list[str] = []
        self.resumed: list[str] = []
        self.supervisor = ConsumerSupervisor(self._queue_exists)
        self.supervisor.on_cancelled(self.cancelled.append)
        self.supervisor.on_resumed(self.resumed.append)
        self.record = ConsumerRecord(
            queue="orders", tag="t1", channel=self.channel, queue_obj=self.queue, callback=_noop
        )
        self.underlay.consumers["t1"] = _noop

    async def _queue_exists(self, queue: str) -> bool:
        self.probes.append(queue)
        return self.exists

    def due(self) -> None:
        """Make the next re-subscribe attempt due now (skip the backoff wait)."""
        self.record.next_attempt = 0.0


@pytest.fixture
async def harness() -> Any:
    h = _Harness()
    h.supervisor.register(h.record)
    yield h
    await h.supervisor.stop()


# ── detection ─────────────────────────────────────────────────────────────


class TestCallbackDetection:
    async def test_broker_cancel_marks_the_queue_and_fires_the_hook(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()  # attaches the callback
        assert harness.record.watched_underlay is harness.underlay

        harness.underlay.broker_cancel("t1")

        assert harness.supervisor.cancelled_queues == frozenset({"orders"})
        assert harness.cancelled == ["orders"]

    async def test_repeated_cancel_frames_fire_once(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()
        harness.underlay.broker_cancel("t1")
        harness.underlay.broker_cancel("t1")
        assert harness.cancelled == ["orders"]

    async def test_unknown_tag_is_ignored(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()
        harness.underlay.broker_cancel("someone-else")
        assert harness.cancelled == []

    async def test_missing_tag_in_callback_mode_is_not_a_cancel(self, harness: _Harness) -> None:
        """A restore in progress looks like a missing tag; only the callback counts."""
        await harness.supervisor.check_once()
        harness.underlay.consumers.clear()
        for _ in range(3):
            await harness.supervisor.check_once()
        assert harness.cancelled == []

    async def test_watch_follows_a_new_underlay_after_reconnect(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()
        fresh = _Underlay()
        fresh.consumers["t1"] = _noop
        harness.channel.underlay = fresh
        await harness.supervisor.check_once()
        assert harness.record.watched_underlay is fresh
        fresh.broker_cancel("t1")
        assert harness.cancelled == ["orders"]


class TestPollDetection:
    async def test_two_consecutive_misses_mark_cancelled(self) -> None:
        h = _Harness(callbacks=False)
        h.supervisor.register(h.record)
        try:
            h.underlay.consumers.clear()
            await h.supervisor.check_once()
            assert h.cancelled == []
            await h.supervisor.check_once()
            assert h.cancelled == ["orders"]
            assert h.record.watched_underlay is None
        finally:
            await h.supervisor.stop()

    async def test_a_present_tag_resets_the_miss_count(self) -> None:
        h = _Harness(callbacks=False)
        h.supervisor.register(h.record)
        try:
            h.underlay.consumers.clear()
            await h.supervisor.check_once()
            h.underlay.consumers["t1"] = _noop
            await h.supervisor.check_once()
            h.underlay.consumers.clear()
            await h.supervisor.check_once()
            assert h.cancelled == []
        finally:
            await h.supervisor.stop()


class TestNothingToCheck:
    async def test_closed_channel_is_left_to_the_reconnect(self, harness: _Harness) -> None:
        harness.record.missed_polls = 1
        harness.channel.is_closed = True
        await harness.supervisor.check_once()
        assert harness.record.missed_polls == 0
        assert harness.record.watched_underlay is None

    async def test_unavailable_underlay_is_skipped(self, harness: _Harness) -> None:
        harness.channel.fail = True
        await harness.supervisor.check_once()
        assert harness.record.watched_underlay is None

    async def test_closed_underlay_is_skipped(self, harness: _Harness) -> None:
        harness.underlay.is_closed = True
        await harness.supervisor.check_once()
        assert harness.record.watched_underlay is None

    async def test_stopped_and_unsupervised_records_are_skipped(self) -> None:
        h = _Harness(callbacks=False)
        h.record.supervised = False
        h.supervisor.register(h.record)  # unsupervised: no task started
        assert h.supervisor._task is None
        h.underlay.consumers.clear()
        await h.supervisor.check_once()
        await h.supervisor.check_once()
        assert h.cancelled == []
        h.supervisor.unregister("orders")
        assert h.record.stopped is True
        assert h.supervisor.unregister("orders") is None


# ── recovery ──────────────────────────────────────────────────────────────


class TestResubscribe:
    async def test_waits_for_the_queue_with_bounded_backoff_then_resubscribes(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()
        harness.underlay.broker_cancel("t1")
        harness.exists = False

        await harness.supervisor.check_once()  # backoff not due yet
        assert harness.probes == []

        delays = []
        for _ in range(8):
            harness.due()
            await harness.supervisor.check_once()
            delays.append(harness.record.retry_delay)
        assert len(harness.probes) == 8
        assert harness.queue.calls == []
        assert delays[0] == RESUBSCRIBE_BACKOFF_INITIAL * 2
        assert max(delays) == RESUBSCRIBE_BACKOFF_MAX
        assert harness.supervisor.cancelled_queues == frozenset({"orders"})

        harness.exists = True
        harness.due()
        await harness.supervisor.check_once()

        assert harness.queue.calls == [{"tag": "t1", "no_ack": False, "robust": False}]
        assert harness.resumed == ["orders"]
        assert harness.supervisor.cancelled_queues == frozenset()
        assert harness.record.retry_delay == RESUBSCRIBE_BACKOFF_INITIAL

    async def test_tag_restored_by_aio_pika_counts_as_resumed(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()
        harness.underlay.broker_cancel("t1")
        harness.underlay.consumers["t1"] = _noop  # aio-pika 10 re-subscribed it
        await harness.supervisor.check_once()
        assert harness.resumed == ["orders"]
        assert harness.queue.calls == []

    async def test_tag_restored_during_the_probe_is_not_subscribed_twice(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()
        harness.underlay.broker_cancel("t1")
        harness.due()

        async def exists_and_restored(queue: str) -> bool:
            harness.underlay.consumers["t1"] = _noop
            return True

        harness.supervisor._queue_exists = exists_and_restored
        await harness.supervisor.check_once()
        assert harness.queue.calls == []
        assert harness.resumed == ["orders"]

    async def test_duplicate_tag_means_someone_else_restored_it(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()
        harness.underlay.broker_cancel("t1")
        harness.queue.error = DuplicateConsumerTag(1)
        harness.due()
        await harness.supervisor.check_once()
        assert harness.resumed == ["orders"]

    async def test_failed_subscribe_is_retried_later(self, harness: _Harness, caplog: pytest.LogCaptureFixture) -> None:
        await harness.supervisor.check_once()
        harness.underlay.broker_cancel("t1")
        harness.queue.error = ConnectionResetError("dropped")
        harness.due()
        with caplog.at_level(logging.WARNING, logger="rabbitkit.async_.consumers"):
            await harness.supervisor.check_once()
        assert "Consumer supervision of queue=orders failed" in caplog.text
        assert harness.supervisor.cancelled_queues == frozenset({"orders"})

        harness.queue.error = None
        harness.due()
        await harness.supervisor.check_once()
        assert harness.resumed == ["orders"]

    async def test_cancelled_by_rabbitkit_during_the_probe_does_not_subscribe(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()
        harness.underlay.broker_cancel("t1")
        harness.due()

        async def exists_then_stopped(queue: str) -> bool:
            harness.supervisor.unregister(queue)
            return True

        harness.supervisor._queue_exists = exists_then_stopped
        await harness.supervisor._check(harness.record)  # unregistered: drive the record directly
        assert harness.queue.calls == []

    async def test_cancelled_by_rabbitkit_during_subscribe_is_undone(self, harness: _Harness) -> None:
        await harness.supervisor.check_once()
        harness.underlay.broker_cancel("t1")
        harness.due()
        harness.queue.on_consume = lambda: harness.supervisor.unregister("orders")
        await harness.supervisor._try_resubscribe(harness.record, harness.underlay)
        harness.underlay.basic_cancel.assert_awaited_once_with("t1")
        assert harness.resumed == []


class TestHooksAndLifecycle:
    async def test_a_raising_hook_is_logged_not_propagated(self, caplog: pytest.LogCaptureFixture) -> None:
        h = _Harness()

        def boom(queue: str) -> None:
            raise RuntimeError("hook bug")

        h.supervisor.on_cancelled(boom)
        h.supervisor.register(h.record)
        try:
            await h.supervisor.check_once()
            with caplog.at_level(logging.WARNING, logger="rabbitkit.async_.consumers"):
                h.underlay.broker_cancel("t1")
            assert "consumer state callback raised" in caplog.text
            assert h.cancelled == ["orders"]
        finally:
            await h.supervisor.stop()

    async def test_background_task_wakes_on_the_cancel_frame(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(consumers_mod, "RESUBSCRIBE_BACKOFF_INITIAL", 0.0)
        monkeypatch.setattr(consumers_mod, "WATCH_INTERVAL", 0.01)
        h = _Harness()
        h.supervisor.register(h.record)
        try:
            async with asyncio.timeout(5):
                while h.record.watched_underlay is None:  # first pass
                    await asyncio.sleep(0.01)
                h.underlay.broker_cancel("t1")
                while not h.resumed:  # the frame woke the task; backoff 0 -> immediate
                    await asyncio.sleep(0.01)
            assert h.queue.calls[0]["robust"] is False
        finally:
            await h.supervisor.stop()
        assert h.supervisor._task is None
        assert h.record.stopped is True

    async def test_register_restarts_a_finished_task(self) -> None:
        h = _Harness()
        h.supervisor.register(h.record)
        first = h.supervisor._task
        assert first is not None
        h.supervisor.register(h.record)
        assert h.supervisor._task is first  # still running: reused
        await h.supervisor.stop()
        h.supervisor.register(h.record)
        assert h.supervisor._task is not None and h.supervisor._task is not first
        await h.supervisor.stop()

    async def test_stop_without_a_task(self) -> None:
        supervisor = ConsumerSupervisor(AsyncMock(return_value=True))
        await supervisor.stop()
        assert supervisor.cancelled_queues == frozenset()
