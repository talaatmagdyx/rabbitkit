"""Tests for highload/batch.py — BatchPublisher and BatchAcker."""

from __future__ import annotations

from unittest.mock import MagicMock

from rabbitkit.core.config import BatchAckConfig, BatchPublishConfig
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.highload.batch import BatchAcker, BatchPublisher

# ── helpers ───────────────────────────────────────────────────────────────


def _make_envelope(routing_key: str = "test.rk", body: bytes = b"data") -> MessageEnvelope:
    return MessageEnvelope(routing_key=routing_key, body=body)


# ── BatchPublisher ───────────────────────────────────────────────────────


class TestBatchPublisher:
    def test_add_below_batch_size_no_flush(self) -> None:
        """Adding below batch_size does not trigger flush."""
        publish_fn = MagicMock()
        bp = BatchPublisher(publish_fn=publish_fn, config=BatchPublishConfig(batch_size=5))

        bp.add(_make_envelope())
        bp.add(_make_envelope())

        publish_fn.assert_not_called()
        assert bp.pending == 2

    def test_add_at_batch_size_triggers_flush(self) -> None:
        """Adding at batch_size auto-flushes the buffer."""
        publish_fn = MagicMock()
        bp = BatchPublisher(publish_fn=publish_fn, config=BatchPublishConfig(batch_size=3))

        bp.add(_make_envelope(routing_key="a"))
        bp.add(_make_envelope(routing_key="b"))
        bp.add(_make_envelope(routing_key="c"))

        assert publish_fn.call_count == 3
        assert bp.pending == 0

    def test_flush_publishes_all(self) -> None:
        """Flush publishes all buffered envelopes."""
        publish_fn = MagicMock()
        bp = BatchPublisher(publish_fn=publish_fn)

        for i in range(5):
            bp._buffer.append(_make_envelope(routing_key=f"rk-{i}"))

        count = bp.flush()

        assert count == 5
        assert publish_fn.call_count == 5
        assert bp.pending == 0

    def test_flush_empty_is_noop(self) -> None:
        """Flushing an empty buffer returns 0 and calls nothing."""
        publish_fn = MagicMock()
        confirm_fn = MagicMock()
        bp = BatchPublisher(publish_fn=publish_fn, confirm_fn=confirm_fn)

        count = bp.flush()

        assert count == 0
        publish_fn.assert_not_called()
        confirm_fn.assert_not_called()

    def test_flush_calls_confirm_fn(self) -> None:
        """Flush calls confirm_fn after publishing."""
        publish_fn = MagicMock()
        confirm_fn = MagicMock()
        bp = BatchPublisher(publish_fn=publish_fn, confirm_fn=confirm_fn)

        bp.add(_make_envelope())
        bp.flush()

        confirm_fn.assert_called_once()

    def test_no_confirm_fn(self) -> None:
        """Flush works without confirm_fn."""
        publish_fn = MagicMock()
        bp = BatchPublisher(publish_fn=publish_fn, confirm_fn=None)

        bp.add(_make_envelope())
        bp.flush()  # should not raise

        publish_fn.assert_called_once()

    def test_close_flushes_remaining(self) -> None:
        """Close flushes any remaining buffered envelopes."""
        publish_fn = MagicMock()
        bp = BatchPublisher(publish_fn=publish_fn)

        bp.add(_make_envelope())
        bp.add(_make_envelope())

        count = bp.close()

        assert count == 2
        assert publish_fn.call_count == 2
        assert bp.pending == 0

    def test_close_empty(self) -> None:
        """Close on empty buffer returns 0."""
        bp = BatchPublisher(publish_fn=MagicMock())
        assert bp.close() == 0

    def test_default_config(self) -> None:
        """Default batch publish config values."""
        bp = BatchPublisher(publish_fn=MagicMock())
        assert bp._config.batch_size == 100
        assert bp._config.flush_interval_ms == 50


class TestBatchPublisherAsync:
    async def test_add_async_auto_flush(self) -> None:
        """Async add auto-flushes at batch_size."""
        published: list[MessageEnvelope] = []

        async def publish_fn(env: MessageEnvelope) -> None:
            published.append(env)

        bp = BatchPublisher(publish_fn=publish_fn, config=BatchPublishConfig(batch_size=2))

        await bp.add_async(_make_envelope(routing_key="a"))
        assert len(published) == 0

        await bp.add_async(_make_envelope(routing_key="b"))
        assert len(published) == 2

    async def test_flush_async(self) -> None:
        """Async flush publishes all buffered envelopes."""
        published: list[MessageEnvelope] = []

        async def publish_fn(env: MessageEnvelope) -> None:
            published.append(env)

        bp = BatchPublisher(publish_fn=publish_fn)
        bp._buffer.append(_make_envelope())
        bp._buffer.append(_make_envelope())

        count = await bp.flush_async()

        assert count == 2
        assert len(published) == 2

    async def test_flush_async_empty(self) -> None:
        """Async flush of empty buffer returns 0."""
        bp = BatchPublisher(publish_fn=MagicMock())
        count = await bp.flush_async()
        assert count == 0

    async def test_close_async(self) -> None:
        """Async close flushes remaining."""
        published: list[MessageEnvelope] = []

        async def publish_fn(env: MessageEnvelope) -> None:
            published.append(env)

        bp = BatchPublisher(publish_fn=publish_fn)
        bp._buffer.append(_make_envelope())

        count = await bp.close_async()
        assert count == 1
        assert len(published) == 1


# ── BatchAcker ───────────────────────────────────────────────────────────


class TestBatchAcker:
    def test_add_below_batch_size_no_flush(self) -> None:
        """Adding below batch_size does not trigger ack."""
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(batch_size=5))

        ba.add(1)
        ba.add(2)

        ack_fn.assert_not_called()
        assert ba.pending == 2

    def test_add_at_batch_size_triggers_flush(self) -> None:
        """Adding at batch_size auto-flushes with max tag."""
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(batch_size=3))

        ba.add(5)
        ba.add(3)
        ba.add(7)

        ack_fn.assert_called_once_with(7, multiple=True)
        assert ba.pending == 0

    def test_flush_uses_max_tag(self) -> None:
        """Flush acks with the maximum delivery tag and multiple=True."""
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn)

        ba._tags = [10, 5, 20, 15]
        count = ba.flush()

        assert count == 4
        ack_fn.assert_called_once_with(20, multiple=True)
        assert ba.pending == 0

    def test_flush_empty_noop(self) -> None:
        """Flushing empty tags returns 0 and calls nothing."""
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn)

        count = ba.flush()

        assert count == 0
        ack_fn.assert_not_called()

    def test_close_flushes_remaining(self) -> None:
        """Close flushes remaining tags."""
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn)

        ba.add(1)
        ba.add(2)
        ba.add(3)

        count = ba.close()

        assert count == 3
        ack_fn.assert_called_once_with(3, multiple=True)

    def test_close_empty(self) -> None:
        """Close on empty tags returns 0."""
        ba = BatchAcker(ack_fn=MagicMock())
        assert ba.close() == 0

    def test_default_config(self) -> None:
        """Default batch ack config values."""
        ba = BatchAcker(ack_fn=MagicMock())
        assert ba._config.batch_size == 100
        assert ba._config.flush_interval_ms == 200


class TestBatchAckerAsync:
    async def test_add_async_auto_flush(self) -> None:
        """Async add auto-flushes at batch_size."""
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn, config=BatchAckConfig(batch_size=2))

        await ba.add_async(10)
        ack_fn.assert_not_called()

        await ba.add_async(20)
        ack_fn.assert_called_once_with(20, multiple=True)

    async def test_flush_async(self) -> None:
        """Async flush acks all buffered tags."""
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn)

        ba._tags = [5, 10, 15]
        count = await ba.flush_async()

        assert count == 3
        ack_fn.assert_called_once_with(15, multiple=True)

    async def test_flush_async_empty(self) -> None:
        """Async flush of empty tags returns 0."""
        ba = BatchAcker(ack_fn=MagicMock())
        count = await ba.flush_async()
        assert count == 0

    async def test_close_async(self) -> None:
        """Async close flushes remaining."""
        ack_fn = MagicMock()
        ba = BatchAcker(ack_fn=ack_fn)
        ba._tags = [1, 2, 3]

        count = await ba.close_async()
        assert count == 3
        ack_fn.assert_called_once_with(3, multiple=True)


# ── Timer-based flush (flush_interval_ms) ────────────────────────────────


class TestBatchPublisherFlushInterval:
    def test_flush_interval_zero_no_timer(self) -> None:
        """No timer is created when flush_interval_ms=0."""
        bp = BatchPublisher(
            publish_fn=MagicMock(),
            config=BatchPublishConfig(batch_size=1000, flush_interval_ms=0),
        )
        bp.add(_make_envelope())
        assert bp._timer is None
        bp.close()

    def test_timer_starts_on_first_add(self) -> None:
        """Timer is created after first add() when flush_interval_ms>0."""
        bp = BatchPublisher(
            publish_fn=MagicMock(),
            config=BatchPublishConfig(batch_size=1000, flush_interval_ms=50),
        )
        assert bp._timer is None
        bp.add(_make_envelope())
        assert bp._timer is not None
        bp.close()

    def test_close_cancels_timer(self) -> None:
        """close() cancels the pending timer so no further flushes occur."""
        publish_fn = MagicMock()
        bp = BatchPublisher(
            publish_fn=publish_fn,
            config=BatchPublishConfig(batch_size=1000, flush_interval_ms=50),
        )
        bp.add(_make_envelope())
        assert bp._timer is not None
        bp.close()
        assert bp._timer is None

    def test_flush_interval_triggers_after_timeout(self) -> None:
        """Timer fires and flushes buffered envelopes before batch_size is hit."""
        import time

        publish_fn = MagicMock()
        bp = BatchPublisher(
            publish_fn=publish_fn,
            config=BatchPublishConfig(batch_size=1000, flush_interval_ms=50),
        )
        bp.add(_make_envelope())
        publish_fn.assert_not_called()

        # Wait for at least 2 timer intervals
        time.sleep(0.2)
        assert publish_fn.call_count >= 1
        bp.close()


class TestBatchAckerFlushInterval:
    def test_flush_interval_zero_no_timer(self) -> None:
        """No timer created when flush_interval_ms=0."""
        ba = BatchAcker(
            ack_fn=MagicMock(),
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=0),
        )
        ba.add(1)
        assert ba._timer is None
        ba.close()

    def test_timer_starts_on_first_add(self) -> None:
        """Timer created on first add when flush_interval_ms>0."""
        ba = BatchAcker(
            ack_fn=MagicMock(),
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=50),
        )
        assert ba._timer is None
        ba.add(1)
        assert ba._timer is not None
        ba.close()

    def test_close_cancels_timer(self) -> None:
        """close() cancels the pending timer."""
        ba = BatchAcker(
            ack_fn=MagicMock(),
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=50),
        )
        ba.add(1)
        ba.close()
        assert ba._timer is None

    def test_flush_interval_triggers_after_timeout(self) -> None:
        """Timer fires and acks buffered tags before batch_size is hit."""
        import time

        ack_fn = MagicMock()
        ba = BatchAcker(
            ack_fn=ack_fn,
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=50),
        )
        ba.add(42)
        ack_fn.assert_not_called()

        time.sleep(0.2)
        assert ack_fn.call_count >= 1
        ba.close()


# ── BatchPublisher async awaitable paths (lines 143, 174-176, 184-185) ───


class TestBatchPublisherAsyncAwaitablePaths:
    async def test_add_async_triggers_flush_at_batch_size(self) -> None:
        """Line 143: flush_async is awaited when buffer reaches batch_size in add_async."""
        published: list[MessageEnvelope] = []

        def publish_fn(env: MessageEnvelope) -> None:
            published.append(env)

        bp = BatchPublisher(
            publish_fn=publish_fn,
            config=BatchPublishConfig(batch_size=2, flush_interval_ms=0),
        )

        await bp.add_async(_make_envelope(routing_key="a"))
        assert len(published) == 0

        await bp.add_async(_make_envelope(routing_key="b"))
        # Both should be published after flush triggered by reaching batch_size
        assert len(published) == 2

    async def test_flush_async_awaitable_confirm_fn(self) -> None:
        """Lines 174-176: confirm_fn returning an awaitable is awaited in flush_async."""
        published: list[MessageEnvelope] = []
        confirmed: list[bool] = []

        def publish_fn(env: MessageEnvelope) -> None:
            published.append(env)

        async def confirm_fn() -> None:
            confirmed.append(True)

        bp = BatchPublisher(publish_fn=publish_fn, confirm_fn=confirm_fn)
        bp._buffer.append(_make_envelope())

        count = await bp.flush_async()

        assert count == 1
        assert len(published) == 1
        assert len(confirmed) == 1

    async def test_close_async_cancels_flush_task(self) -> None:
        """Lines 184-185: close_async cancels the interval task when it exists."""
        publish_fn = MagicMock()
        bp = BatchPublisher(
            publish_fn=publish_fn,
            config=BatchPublishConfig(batch_size=1000, flush_interval_ms=10),
        )

        # Start the interval task by adding an envelope (below batch_size)
        await bp.add_async(_make_envelope())
        assert bp._flush_task is not None

        # close_async should cancel the task
        count = await bp.close_async()

        assert bp._flush_task is None
        assert count == 1  # the one buffered envelope was flushed

    async def test_interval_loop_async_flushes_publisher(self) -> None:
        """Line 143: _interval_loop_async calls flush_async() on each tick.

        This covers the 'await self.flush_async()' line inside the loop body.
        """
        import asyncio

        published: list[MessageEnvelope] = []

        def publish_fn(env: MessageEnvelope) -> None:
            published.append(env)

        bp = BatchPublisher(
            publish_fn=publish_fn,
            config=BatchPublishConfig(batch_size=1000, flush_interval_ms=10),
        )

        # Add an envelope and start the interval task
        await bp.add_async(_make_envelope())
        assert bp._flush_task is not None
        assert len(published) == 0

        # Wait for at least one interval to fire (loop: sleep 10ms → flush_async)
        await asyncio.sleep(0.05)

        # flush_async should have been triggered at least once via the loop
        assert len(published) >= 1

        await bp.close_async()


# ── BatchAcker async awaitable paths (lines 299, 326, 334-335) ──────────


class TestBatchAckerAsyncAwaitablePaths:
    async def test_interval_loop_async_flushes(self) -> None:
        """Line 299: _interval_loop_async calls flush_async on each tick."""
        import asyncio

        ack_calls: list[tuple] = []

        def ack_fn(tag: int, multiple: bool) -> None:
            ack_calls.append((tag, multiple))

        ba = BatchAcker(
            ack_fn=ack_fn,
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=10),
        )

        # Start async interval loop
        await ba.add_async(5)
        assert ba._flush_task is not None

        # Wait for at least one interval to fire
        await asyncio.sleep(0.05)
        assert len(ack_calls) >= 1

        await ba.close_async()

    async def test_flush_async_awaitable_ack_fn(self) -> None:
        """Line 326: ack_fn returning an awaitable is awaited in flush_async."""
        acked: list[tuple] = []

        async def ack_fn(tag: int, multiple: bool) -> None:
            acked.append((tag, multiple))

        ba = BatchAcker(ack_fn=ack_fn)
        ba._tags = [10, 20, 15]

        count = await ba.flush_async()

        assert count == 3
        assert len(acked) == 1
        assert acked[0] == (20, True)

    async def test_close_async_cancels_flush_task(self) -> None:
        """Lines 334-335: close_async cancels the interval task when it exists."""
        ack_fn = MagicMock()
        ba = BatchAcker(
            ack_fn=ack_fn,
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=10),
        )

        # Start the interval task by adding a tag (below batch_size)
        await ba.add_async(42)
        assert ba._flush_task is not None

        # close_async should cancel the task and flush remaining
        count = await ba.close_async()

        assert ba._flush_task is None
        assert count == 1  # the one buffered tag was flushed
        ack_fn.assert_called_once_with(42, multiple=True)
