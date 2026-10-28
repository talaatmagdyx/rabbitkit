"""Tests for sync/consumers.py — SyncConsumerSupervisor (sync)."""

from __future__ import annotations

import pytest

from rabbitkit.sync.consumers import (
    RESUBSCRIBE_BACKOFF_INITIAL,
    RESUBSCRIBE_BACKOFF_MAX,
    SyncConsumerRecord,
    SyncConsumerSupervisor,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _noop(ch: object, method: object, props: object, body: bytes) -> None:
    return None


class _Harness:
    def __init__(self) -> None:
        self.clock = _Clock()
        self.exists = False
        self.resubscribe_ok = True
        self.probed: list[str] = []
        self.resubscribed: list[str] = []
        self.events: list[tuple[str, str]] = []
        self.sup = SyncConsumerSupervisor(self._exists, self._resubscribe, clock=self.clock)
        self.sup.on_cancelled(lambda q: self.events.append(("cancelled", q)))
        self.sup.on_resumed(lambda q: self.events.append(("resumed", q)))

    def _exists(self, queue: str) -> bool:
        self.probed.append(queue)
        return self.exists

    def _resubscribe(self, record: SyncConsumerRecord) -> bool:
        self.resubscribed.append(record.tag)
        return self.resubscribe_ok

    def add(self, queue: str = "orders", tag: str = "t1", *, supervised: bool = True) -> SyncConsumerRecord:
        record = SyncConsumerRecord(queue=queue, tag=tag, on_message=_noop, supervised=supervised)
        self.sup.register(record)
        return record


class TestDetection:
    def test_broker_cancel_marks_reports_and_fires(self, caplog: pytest.LogCaptureFixture) -> None:
        h = _Harness()
        record = h.add()
        with caplog.at_level("WARNING", logger="rabbitkit.sync.consumers"):
            h.sup.broker_cancelled("t1")
        assert record.broker_cancelled is True
        assert record.next_attempt == 100.0 + RESUBSCRIBE_BACKOFF_INITIAL
        assert h.sup.cancelled_queues == frozenset({"orders"})
        assert h.events == [("cancelled", "orders")]
        assert "was cancelled by the broker" in caplog.text

    def test_a_second_cancel_frame_fires_once(self) -> None:
        h = _Harness()
        h.add()
        h.sup.broker_cancelled("t1")
        h.sup.broker_cancelled("t1")
        assert h.events == [("cancelled", "orders")]

    def test_unknown_tag_and_unsupervised_consumers_are_ignored(self) -> None:
        h = _Harness()
        h.add("amq.rabbitmq.reply-to", "reply", supervised=False)
        h.sup.broker_cancelled("nope")
        h.sup.broker_cancelled("reply")
        assert h.events == []
        assert h.sup.cancelled_queues == frozenset()

    def test_a_raising_callback_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        h = _Harness()
        h.add()

        def boom(queue: str) -> None:
            raise RuntimeError("hook bug")

        h.sup.on_cancelled(boom)
        with caplog.at_level("WARNING", logger="rabbitkit.sync.consumers"):
            h.sup.broker_cancelled("t1")
        assert "consumer state callback raised" in caplog.text
        assert h.events == [("cancelled", "orders")]


class TestRecovery:
    def test_waits_for_the_backoff_then_probes_with_doubling_capped_delay(self) -> None:
        h = _Harness()
        record = h.add()
        h.sup.broker_cancelled("t1")
        h.sup.check_once()
        assert h.probed == []  # backoff not elapsed
        delays = []
        for _ in range(8):
            h.clock.now = record.next_attempt
            h.sup.check_once()
            delays.append(record.next_attempt - h.clock.now)
        assert delays[:3] == [1.0, 2.0, 4.0]
        assert max(delays) == RESUBSCRIBE_BACKOFF_MAX
        assert h.resubscribed == []
        assert record.broker_cancelled is True

    def test_resubscribes_when_the_queue_is_back(self) -> None:
        h = _Harness()
        record = h.add()
        h.sup.broker_cancelled("t1")
        h.exists = True
        h.clock.now += 1.0
        h.sup.check_once()
        assert h.resubscribed == ["t1"]
        assert record.broker_cancelled is False
        assert record.retry_delay == RESUBSCRIBE_BACKOFF_INITIAL
        assert h.events == [("cancelled", "orders"), ("resumed", "orders")]
        assert h.sup.cancelled_queues == frozenset()

    def test_a_failed_resubscribe_retries_later(self) -> None:
        h = _Harness()
        record = h.add()
        h.sup.broker_cancelled("t1")
        h.exists, h.resubscribe_ok = True, False
        h.clock.now += 1.0
        h.sup.check_once()
        assert record.broker_cancelled is True
        assert h.events == [("cancelled", "orders")]
        h.resubscribe_ok = True
        h.clock.now = record.next_attempt
        h.sup.check_once()
        assert h.events[-1] == ("resumed", "orders")

    def test_a_consumer_unregistered_during_the_probe_is_not_resubscribed(self) -> None:
        h = _Harness()
        h.add()
        h.sup.broker_cancelled("t1")
        h.clock.now += 1.0

        def exists_then_cancelled(queue: str) -> bool:
            h.sup.unregister(queue)
            return True

        h.sup._queue_exists = exists_then_cancelled
        h.sup.check_once()
        assert h.resubscribed == []

    def test_connection_errors_from_the_probe_propagate(self) -> None:
        h = _Harness()
        h.add()
        h.sup.broker_cancelled("t1")
        h.clock.now += 1.0

        def dead(queue: str) -> bool:
            raise ConnectionResetError("gone")

        h.sup._queue_exists = dead
        with pytest.raises(ConnectionResetError):
            h.sup.check_once()

    def test_healthy_consumers_are_not_probed(self) -> None:
        h = _Harness()
        h.add()
        h.sup.check_once()
        assert h.probed == []


class TestRegistry:
    def test_unregister_record_and_clear(self) -> None:
        h = _Harness()
        record = h.add()
        assert h.sup.record("orders") is record
        assert h.sup.unregister("orders") is record
        assert h.sup.unregister("orders") is None
        h.add("a", "ta")
        h.sup.broker_cancelled("ta")
        h.sup.clear()
        assert h.sup.record("a") is None
        assert h.sup.cancelled_queues == frozenset()

    def test_default_clock_is_monotonic(self) -> None:
        sup = SyncConsumerSupervisor(lambda q: False, lambda r: False)
        record = SyncConsumerRecord(queue="q", tag="t", on_message=_noop)
        sup.register(record)
        sup.broker_cancelled("t")
        assert record.next_attempt > 0
