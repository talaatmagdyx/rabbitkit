"""Tests for core/streams.py: offsets, offset stores, StreamOffsetTracker."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.streams import (
    RESUBSCRIBE_CLOCK_MARGIN,
    STREAM_OFFSET_ARG,
    InMemoryOffsetStore,
    OffsetStore,
    SQLiteOffsetStore,
    StreamConsumerConfig,
    StreamOffset,
    StreamOffsetTracker,
)
from rabbitkit.core.types import StreamOffsetType

# ── StreamOffset ─────────────────────────────────────────────────────────


class TestStreamOffsetValidation:
    def test_type_given_as_string_is_coerced(self) -> None:
        assert StreamOffset(type="first").type is StreamOffsetType.FIRST  # type: ignore[arg-type]

    def test_unknown_type_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Unknown stream offset type 'middle'"):
            StreamOffset(type="middle")  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", [None, True, 1.5, "3"])
    def test_offset_needs_an_int(self, value: Any) -> None:
        with pytest.raises(TypeError, match="OFFSET stream offset needs an int"):
            StreamOffset(type=StreamOffsetType.OFFSET, value=value)

    def test_offset_must_be_non_negative(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            StreamOffset(type=StreamOffsetType.OFFSET, value=-1)

    def test_naive_timestamp_is_taken_as_utc(self) -> None:
        offset = StreamOffset.timestamp(datetime(2026, 1, 1, 12, 0))
        assert offset.value == datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    def test_aware_timestamp_is_kept(self) -> None:
        at = datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=3)))
        assert StreamOffset.timestamp(at).value is at

    @pytest.mark.parametrize("kind", [StreamOffsetType.FIRST, StreamOffsetType.LAST, StreamOffsetType.NEXT])
    def test_relative_offsets_take_no_value(self, kind: StreamOffsetType) -> None:
        with pytest.raises(ValueError, match="takes no value"):
            StreamOffset(type=kind, value=3)

    def test_timestamp_consume_argument_is_a_datetime_not_an_int(self) -> None:
        """RabbitMQ reads an int x-stream-offset as an offset; a time must be
        sent as an AMQP timestamp, which pika/aio-pika encode from a datetime."""
        at = datetime(2026, 6, 15, tzinfo=UTC)
        assert StreamOffset.timestamp(at).to_consume_arguments() == {STREAM_OFFSET_ARG: at}


class TestStreamOffsetParse:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("first", StreamOffset.first()),
            ("last", StreamOffset.last()),
            ("next", StreamOffset.next()),
            (StreamOffsetType.FIRST, StreamOffset.first()),
            (0, StreamOffset.offset(0)),
            (17, StreamOffset.offset(17)),
            (datetime(2026, 1, 1, tzinfo=UTC), StreamOffset.timestamp(datetime(2026, 1, 1, tzinfo=UTC))),
        ],
    )
    def test_accepted_forms(self, value: Any, expected: StreamOffset) -> None:
        assert StreamOffset.parse(value) == expected

    def test_a_stream_offset_is_returned_as_is(self) -> None:
        offset = StreamOffset.offset(3)
        assert StreamOffset.parse(offset) is offset

    @pytest.mark.parametrize("value", ["FIRST", "offset", "1h", ""])
    def test_unknown_strings_are_refused(self, value: str) -> None:
        with pytest.raises(ValueError, match="Unknown stream offset"):
            StreamOffset.parse(value)

    @pytest.mark.parametrize("value", [True, 1.5, None, b"first"])
    def test_other_types_are_refused(self, value: Any) -> None:
        with pytest.raises(TypeError, match="Not a stream offset"):
            StreamOffset.parse(value)

    def test_negative_int_is_refused(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            StreamOffset.parse(-5)


# ── StreamConsumerConfig ─────────────────────────────────────────────────


class TestStreamConsumerConfig:
    @pytest.mark.parametrize("value", ["first", 5, datetime(2026, 1, 1, tzinfo=UTC)])
    def test_offset_shorthands_are_parsed(self, value: Any) -> None:
        assert StreamConsumerConfig(offset=value).offset == StreamOffset.parse(value)

    def test_defaults(self) -> None:
        config = StreamConsumerConfig()
        assert config.offset == StreamOffset.next()
        assert config.offset_store is None
        assert config.commit_every == 100
        assert config.commit_interval == 5.0

    @pytest.mark.parametrize("value", [0, -1, True, 2.5])
    def test_commit_every_must_be_a_positive_int(self, value: Any) -> None:
        with pytest.raises(ValueError, match="commit_every must be an int >= 1"):
            StreamConsumerConfig(commit_every=value)

    @pytest.mark.parametrize("value", [0, -1.0])
    def test_commit_interval_must_be_positive(self, value: float) -> None:
        with pytest.raises(ValueError, match="commit_interval must be None or > 0"):
            StreamConsumerConfig(commit_interval=value)

    def test_commit_interval_none_means_count_only(self) -> None:
        assert StreamConsumerConfig(commit_interval=None).commit_interval is None

    def test_consumer_name_must_not_be_empty(self) -> None:
        with pytest.raises(ValueError, match="consumer_name must not be empty"):
            StreamConsumerConfig(consumer_name="")

    def test_consumer_name_is_an_amqp_shortstr(self) -> None:
        with pytest.raises(ValueError, match="consumer_name"):
            StreamConsumerConfig(consumer_name="x" * 256)

    def test_offset_store_must_implement_the_protocol(self) -> None:
        with pytest.raises(TypeError, match="must implement load"):
            StreamConsumerConfig(offset_store=object())  # type: ignore[arg-type]

    def test_offset_key_is_the_consumer_name_else_the_queue(self) -> None:
        assert StreamConsumerConfig().offset_key("events") == "events"
        assert StreamConsumerConfig(consumer_name="billing").offset_key("events") == "billing"


# ── offset stores ────────────────────────────────────────────────────────


class TestInMemoryOffsetStore:
    def test_round_trip_and_reset(self) -> None:
        store = InMemoryOffsetStore()
        assert isinstance(store, OffsetStore)
        assert store.load("a") is None
        store.save("a", 5)
        store.save("b", 9)
        assert (store.load("a"), store.load("b")) == (5, 9)
        store.reset("a")
        store.reset("missing")
        assert store.load("a") is None


class TestSQLiteOffsetStore:
    def test_offsets_survive_a_new_store_on_the_same_file(self, tmp_path: Path) -> None:
        db = tmp_path / "offsets.db"
        with SQLiteOffsetStore(db) as store:
            assert isinstance(store, OffsetStore)
            assert store.path == str(db)
            assert store.load("events") is None
            store.save("events", 10)
            store.save("events", 11)
            store.save("audit", 3)
        with SQLiteOffsetStore(db) as reopened:
            assert reopened.load("events") == 11
            assert reopened.load("audit") == 3
            reopened.reset("events")
            assert reopened.load("events") is None
            assert reopened.load("audit") == 3

    def test_close_closes_the_connection(self, tmp_path: Path) -> None:
        store = SQLiteOffsetStore(tmp_path / "o.db")
        store.close()
        with pytest.raises(sqlite3.ProgrammingError):
            store.load("x")

    def test_a_file_that_is_not_a_database_fails_at_construction(self, tmp_path: Path) -> None:
        bogus = tmp_path / "not.db"
        bogus.write_bytes(b"this is not a sqlite database, just some bytes" * 100)
        with pytest.raises(sqlite3.DatabaseError):
            SQLiteOffsetStore(bogus)


# ── tracker ──────────────────────────────────────────────────────────────


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _RecordingStore:
    """OffsetStore that records saves; can be told to fail."""

    def __init__(self, stored: int | None = None) -> None:
        self.stored = stored
        self.saves: list[int] = []
        self.fail = False

    def load(self, key: str) -> int | None:
        return self.stored

    def save(self, key: str, offset: int) -> None:
        if self.fail:
            raise OSError("disk full")
        self.saves.append(offset)
        self.stored = offset


def _tracker(
    *,
    offset: Any = "first",
    store: Any = None,
    commit_every: int = 100,
    commit_interval: float | None = None,
    consumer_name: str | None = None,
    clock: _Clock | None = None,
    wall: datetime | None = None,
) -> StreamOffsetTracker:
    config = StreamConsumerConfig(
        offset=offset,
        offset_store=store,
        commit_every=commit_every,
        commit_interval=commit_interval,
        consumer_name=consumer_name,
    )
    at = wall or datetime(2026, 10, 28, 12, 0, tzinfo=UTC)
    return StreamOffsetTracker(
        route_name="events:handle",
        queue="events",
        config=config,
        clock=clock or _Clock(),
        wall_clock=lambda: at,
    )


def _sync_msg(offset: int | None, tag: int, sent: list[tuple[str, int | None]] | None = None) -> RabbitMessage:
    """A sync-transport message whose settlement records into *sent*."""
    headers = {} if offset is None else {"x-stream-offset": offset}
    message = RabbitMessage(body=b"", headers=headers, delivery_tag=tag)
    log = sent if sent is not None else []
    message._ack_fn = lambda: log.append(("ack", offset))
    message._nack_fn = lambda requeue: log.append(("nack", offset))
    message._reject_fn = lambda requeue: log.append(("reject", offset))
    return message


def _async_msg(offset: int, tag: int, sent: list[tuple[str, int]] | None = None) -> RabbitMessage:
    log = sent if sent is not None else []
    message = RabbitMessage(body=b"", headers={"x-stream-offset": offset}, delivery_tag=tag)

    async def ack() -> None:
        log.append(("ack", offset))

    async def nack(requeue: bool = True) -> None:
        log.append(("nack", offset))

    async def reject(requeue: bool = False) -> None:
        log.append(("reject", offset))

    message._ack_async_fn = ack
    message._nack_async_fn = nack
    message._reject_async_fn = reject
    return message


class TestTrackerStart:
    def test_nothing_stored_uses_the_configured_offset(self) -> None:
        tracker = _tracker(offset="first", consumer_name="billing")
        assert tracker.consume_arguments == {STREAM_OFFSET_ARG: "first", "x-stream-consumer-name": "billing"}
        assert tracker.processed_offset is None
        assert tracker.committed_offset is None
        assert tracker.key == "billing"
        assert isinstance(tracker.store, InMemoryOffsetStore)

    def test_a_stored_offset_resumes_right_after_it(self, caplog: pytest.LogCaptureFixture) -> None:
        store = _RecordingStore(stored=41)
        with caplog.at_level(logging.INFO, logger="rabbitkit.core.streams"):
            tracker = _tracker(offset="first", store=store)
        assert tracker.consume_arguments == {STREAM_OFFSET_ARG: 42}
        assert tracker.processed_offset == 41
        assert tracker.committed_offset == 41
        assert tracker.key == "events"
        assert tracker.store is store
        assert "resumes at offset 42" in caplog.text


class TestTrackerSync:
    def test_ack_advances_the_resume_point(self) -> None:
        tracker = _tracker()
        sent: list[tuple[str, int | None]] = []
        message = _sync_msg(0, 1, sent)
        tracker.track(message)
        message.ack()
        assert sent == [("ack", 0)]
        assert tracker.processed_offset == 0
        assert tracker.consume_arguments[STREAM_OFFSET_ARG] == 1

    def test_a_message_without_an_offset_is_not_tracked(self) -> None:
        tracker = _tracker()
        message = _sync_msg(None, 1)
        original = message._ack_fn
        tracker.track(message)
        assert message._ack_fn is original
        message.ack()
        assert tracker.processed_offset is None

    def test_out_of_order_completion_never_passes_an_unsettled_delivery(self) -> None:
        tracker = _tracker()
        messages = [_sync_msg(i, i + 1) for i in range(3)]
        for message in messages:
            tracker.track(message)
        messages[2].ack()
        messages[1].ack()
        assert tracker.processed_offset is None  # offset 0 still in progress
        assert tracker.consume_arguments[STREAM_OFFSET_ARG] == "first"
        messages[0].ack()
        assert tracker.processed_offset == 2
        assert tracker.consume_arguments[STREAM_OFFSET_ARG] == 3

    def test_commits_every_n_messages(self) -> None:
        store = _RecordingStore()
        tracker = _tracker(store=store, commit_every=3)
        for i in range(7):
            message = _sync_msg(i, i + 1)
            tracker.track(message)
            message.ack()
        assert store.saves == [2, 5]
        assert tracker.committed_offset == 5
        tracker.flush()
        assert store.saves == [2, 5, 6]
        tracker.flush()  # nothing new: no write
        assert store.saves == [2, 5, 6]

    def test_commits_after_the_interval(self) -> None:
        clock = _Clock()
        store = _RecordingStore()
        tracker = _tracker(store=store, commit_every=1000, commit_interval=5.0, clock=clock)
        first = _sync_msg(0, 1)
        tracker.track(first)
        first.ack()
        assert store.saves == []
        clock.now = 5.0
        second = _sync_msg(1, 2)
        tracker.track(second)
        second.ack()
        assert store.saves == [1]

    def test_flush_with_nothing_processed_writes_nothing(self) -> None:
        store = _RecordingStore()
        _tracker(store=store).flush()
        assert store.saves == []

    def test_a_failed_message_is_skipped_and_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        tracker = _tracker()
        nacked, rejected = _sync_msg(0, 1), _sync_msg(1, 2)
        tracker.track(nacked)
        tracker.track(rejected)
        with caplog.at_level(logging.WARNING, logger="rabbitkit.core.streams"):
            nacked.nack(requeue=True)
            rejected.reject(requeue=False)
        assert tracker.processed_offset == 1
        assert tracker.failed == 2
        assert "offset 0 failed and is not retried" in caplog.text

    def test_after_begin_shutdown_a_nack_hands_the_message_back(self) -> None:
        tracker = _tracker()
        acked, handed_back, later = _sync_msg(0, 1), _sync_msg(1, 2), _sync_msg(2, 3)
        for message in (acked, handed_back, later):
            tracker.track(message)
        tracker.begin_shutdown()
        acked.ack()
        handed_back.nack(requeue=True)
        later.ack()
        assert tracker.processed_offset == 0, "replayed from offset 1 on the next start"
        assert tracker.failed == 0

    def test_a_settlement_that_raises_does_not_advance(self) -> None:
        tracker = _tracker()
        message = _sync_msg(0, 1)

        def dead_channel() -> None:
            raise ConnectionError("channel closed")

        message._ack_fn = dead_channel
        tracker.track(message)
        with pytest.raises(ConnectionError):
            message.ack()
        assert tracker.processed_offset is None

    def test_a_new_subscription_forgets_the_old_ones_pending_deliveries(self) -> None:
        """After a reconnect the deliveries restart at the resume point on a new
        channel (delivery tags from 1): the old in-flight ones are delivered
        again, and a late settlement of an old handle must not count."""
        tracker = _tracker()
        old = [_sync_msg(i, i + 1) for i in range(3)]
        for message in old:
            tracker.track(message)
        old[0].ack()
        assert tracker.consume_arguments[STREAM_OFFSET_ARG] == 1

        again = _sync_msg(1, 1)  # new channel: tag 1, resumes at offset 1
        tracker.track(again)
        old[2].ack()  # stale handle from the dead channel
        assert tracker.processed_offset == 0
        again.ack()
        assert tracker.processed_offset == 1

    def test_a_resubscription_on_the_same_channel_is_detected_by_the_offset(self) -> None:
        tracker = _tracker()
        old = [_sync_msg(i, i + 1) for i in range(2)]
        for message in old:
            tracker.track(message)
        again = _sync_msg(0, 3)  # same channel, tags keep increasing, offset goes back
        tracker.track(again)
        old[0].ack()
        assert tracker.processed_offset is None
        again.ack()
        assert tracker.processed_offset == 0

    def test_an_already_processed_offset_is_not_tracked(self) -> None:
        tracker = _tracker(store=_RecordingStore(stored=10))
        message = _sync_msg(10, 1)
        original = message._ack_fn
        tracker.track(message)
        assert message._ack_fn is original
        message.ack()
        assert tracker.processed_offset == 10

    def test_settlement_of_an_untracked_offset_is_ignored(self) -> None:
        tracker = _tracker()
        assert tracker._settled(5, 0, failed=False) is False

    def test_a_failing_store_is_counted_and_retried(self, caplog: pytest.LogCaptureFixture) -> None:
        store = _RecordingStore()
        store.fail = True
        tracker = _tracker(store=store, commit_every=1)
        message = _sync_msg(0, 1)
        tracker.track(message)
        with caplog.at_level(logging.ERROR, logger="rabbitkit.core.streams"):
            message.ack()
        assert tracker.commit_failures == 1
        assert tracker.committed_offset is None
        assert "committing offset 0 failed" in caplog.text
        store.fail = False
        tracker.flush()
        assert store.saves == [0]
        assert tracker.committed_offset == 0


class TestTrackerSubscribed:
    def test_next_without_progress_resubscribes_from_the_subscription_time(self) -> None:
        at = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)
        tracker = _tracker(offset="next", wall=at)
        tracker.subscribed()
        assert tracker.consume_arguments[STREAM_OFFSET_ARG] == at - RESUBSCRIBE_CLOCK_MARGIN

    def test_last_without_progress_resubscribes_from_the_subscription_time(self) -> None:
        at = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)
        tracker = _tracker(offset="last", wall=at)
        tracker.subscribed()
        assert tracker.consume_arguments[STREAM_OFFSET_ARG] == at - RESUBSCRIBE_CLOCK_MARGIN

    def test_the_first_subscription_time_is_kept(self) -> None:
        times = iter([datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)])
        tracker = StreamOffsetTracker(
            route_name="r", queue="q", config=StreamConsumerConfig(), wall_clock=lambda: next(times)
        )
        tracker.subscribed()
        tracker.subscribed()
        assert tracker.consume_arguments[STREAM_OFFSET_ARG] == datetime(2026, 1, 1, tzinfo=UTC) - (
            RESUBSCRIBE_CLOCK_MARGIN
        )

    def test_progress_or_an_absolute_start_keeps_the_arguments(self) -> None:
        first = _tracker(offset="first")
        first.subscribed()
        assert first.consume_arguments[STREAM_OFFSET_ARG] == "first"
        resumed = _tracker(offset="next", store=_RecordingStore(stored=3))
        resumed.subscribed()
        assert resumed.consume_arguments[STREAM_OFFSET_ARG] == 4

    def test_subscribing_again_ends_a_shutdown(self) -> None:
        tracker = _tracker()
        tracker.begin_shutdown()
        tracker.subscribed()
        message = _sync_msg(0, 1)
        tracker.track(message)
        message.nack(requeue=True)
        assert tracker.processed_offset == 0


class TestTrackerAsync:
    async def test_async_settlements_advance_and_commit_inline_for_memory(self) -> None:
        tracker = _tracker(commit_every=2)
        sent: list[tuple[str, int]] = []
        messages = [_async_msg(i, i + 1, sent) for i in range(3)]
        for message in messages:
            tracker.track(message)
        await messages[0].ack_async()
        await messages[1].nack_async(requeue=False)
        await messages[2].reject_async(requeue=False)
        assert sent == [("ack", 0), ("nack", 1), ("reject", 2)]
        assert tracker.processed_offset == 2
        assert tracker.committed_offset == 1
        assert tracker.store.load("events") == 1
        await tracker.flush_async()
        assert tracker.committed_offset == 2

    async def test_a_durable_store_is_committed_off_the_loop(self, tmp_path: Path) -> None:
        with SQLiteOffsetStore(tmp_path / "o.db") as store:
            tracker = _tracker(store=store, commit_every=1)
            first, second = _async_msg(0, 1), _async_msg(1, 2)
            tracker.track(first)
            tracker.track(second)
            await first.ack_async()
            task = tracker._commit_task
            assert task is not None
            await second.ack_async()  # a commit is already running: no second task
            assert tracker._commit_task is task
            await tracker.flush_async()
            assert tracker._commit_task is None
            assert store.load("events") == 1
            assert tracker.committed_offset == 1

    async def test_flush_async_survives_a_failed_commit_task(self) -> None:
        tracker = _tracker(store=_RecordingStore())

        async def boom() -> None:
            raise RuntimeError("thread died")

        tracker._commit_task = asyncio.get_running_loop().create_task(boom())
        await tracker.flush_async()
        assert tracker._commit_task is None

    async def test_a_new_commit_starts_once_the_previous_finished(self) -> None:
        store = _RecordingStore()
        tracker = _tracker(store=store, commit_every=1)
        first, second = _async_msg(0, 1), _async_msg(1, 2)
        tracker.track(first)
        tracker.track(second)
        await first.ack_async()
        task = tracker._commit_task
        assert task is not None
        await task
        await second.ack_async()
        assert tracker._commit_task is not task
        await tracker.flush_async()
        assert store.saves == [0, 1]


# ── RabbitMessage.stream_offset ──────────────────────────────────────────


class TestMessageStreamOffset:
    @pytest.mark.parametrize(
        ("headers", "expected"),
        [
            ({"x-stream-offset": 12}, 12),
            ({"x-stream-offset": 0}, 0),
            ({}, None),
            ({"x-stream-offset": True}, None),
            ({"x-stream-offset": "12"}, None),
        ],
    )
    def test_read_from_the_broker_header(self, headers: dict[str, Any], expected: int | None) -> None:
        assert RabbitMessage(body=b"", headers=headers).stream_offset == expected
