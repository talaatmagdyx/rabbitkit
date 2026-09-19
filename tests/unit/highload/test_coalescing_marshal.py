"""Regression tests for the two ways a CoalescingAcker used to fail silently.

1. ``flush_interval_ms`` fires on a ``threading.Timer`` thread, so the emit
   callables ran off the transport owner. asyncio only raises for a
   cross-thread ``create_task`` under debug mode, so in production the task
   was queued without waking the loop: a busy loop happened to pick it up, an
   idle one never did. At ``prefetch=1`` the loop goes idle waiting for the
   delivery that the un-emitted ack would have unlocked → deadlock.
   ``marshal=`` now hands the whole interval flush to the owner, and arming
   the timer without one warns.

2. A failing emit callable was recorded on ``last_error`` and in the report,
   but never logged — so a broken ``ack_fn`` was invisible in the logs.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
import warnings
from collections.abc import Iterator
from typing import Any

import pytest

from rabbitkit.core.config import BatchAckConfig, BatchPublishConfig
from rabbitkit.core.types import FlushReason, MessageEnvelope
from rabbitkit.highload.batch import BatchAcker, BatchPublisher, CoalescingAcker


@contextlib.contextmanager
def warnings_recorded() -> Iterator[list[warnings.WarningMessage]]:
    """Record warnings without letting the project's filters hide them."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def _acker(**kw: Any) -> CoalescingAcker:
    kw.setdefault("ack_fn", lambda t, m: None)
    kw.setdefault("nack_fn", lambda t, r: None)
    kw.setdefault("reject_fn", lambda t, r: None)
    kw.setdefault("config", BatchAckConfig(batch_size=1000, flush_interval_ms=0))
    return CoalescingAcker(**kw)


# ── the warning ───────────────────────────────────────────────────────────


class TestUnmarshalledTimerWarning:
    def test_arming_a_timer_without_marshal_warns(self) -> None:
        acker = _acker(config=BatchAckConfig(batch_size=1000, flush_interval_ms=50))
        with pytest.warns(RuntimeWarning, match="no marshal="):
            acker.register(1)
        acker._closed = True
        if acker._timer is not None:
            acker._timer.cancel()

    def test_the_warning_fires_only_once_per_acker(self) -> None:
        acker = _acker(config=BatchAckConfig(batch_size=1000, flush_interval_ms=50))
        with pytest.warns(RuntimeWarning):
            acker.register(1)
        with warnings_recorded() as caught:
            acker.register(2)
            acker.register(3)
        assert [w for w in caught if "no marshal=" in str(w.message)] == []
        acker._closed = True
        if acker._timer is not None:
            acker._timer.cancel()

    def test_no_warning_with_marshal(self) -> None:
        acker = _acker(
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=50),
            marshal=lambda fn: fn(),
        )
        with warnings_recorded() as caught:
            acker.register(1)
        assert [w for w in caught if "no marshal=" in str(w.message)] == []
        acker.close()

    def test_no_warning_without_a_timer(self) -> None:
        acker = _acker(config=BatchAckConfig(batch_size=1000, flush_interval_ms=0))
        with warnings_recorded() as caught:
            acker.register(1)
            acker.complete(1)
            acker.flush()
        assert [w for w in caught if "no marshal=" in str(w.message)] == []


# ── marshalling the interval flush ────────────────────────────────────────


class TestIntervalFlushIsMarshalled:
    def test_interval_flush_runs_through_marshal(self) -> None:
        marshalled: list[str] = []
        emitted_on: list[str] = []
        owner = threading.current_thread().name

        def marshal(fn: Any) -> None:
            marshalled.append(threading.current_thread().name)
            fn()  # in a real app this hops to the loop/IO thread

        acker = _acker(
            ack_fn=lambda t, m: emitted_on.append(threading.current_thread().name),
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=20),
            marshal=marshal,
        )
        acker.register(1)
        acker.complete(1)
        deadline = time.monotonic() + 3
        while not emitted_on and time.monotonic() < deadline:
            time.sleep(0.01)
        acker.close()

        assert emitted_on, "the interval flush never emitted"
        assert marshalled, "the interval flush bypassed marshal"
        assert marshalled[0] != owner, "the timer really does run off the owner thread"

    def test_emission_happens_on_the_thread_marshal_chooses(self) -> None:
        """The whole point: with marshal, ack_fn runs wherever marshal puts
        it, not on the timer thread."""
        owner_queue: list[Any] = []
        emitted_on: list[str] = []
        done = threading.Event()

        def marshal(fn: Any) -> None:
            owner_queue.append(fn)  # defer to "the owner"

        acker = _acker(
            ack_fn=lambda t, m: (emitted_on.append(threading.current_thread().name), done.set()),
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=20),
            marshal=marshal,
        )
        acker.register(1)
        acker.complete(1)
        deadline = time.monotonic() + 3
        while not owner_queue and time.monotonic() < deadline:
            time.sleep(0.01)
        assert owner_queue, "marshal never received the flush"
        assert emitted_on == [], "the timer thread emitted before the owner ran it"

        owner_queue.pop()()  # the "owner" now runs it
        assert done.is_set()
        assert emitted_on == [threading.current_thread().name]
        acker.close()

    def test_size_and_manual_flushes_do_not_use_marshal(self) -> None:
        """Those already run on the caller's thread — marshalling them would
        change when the caller's own acks are emitted."""
        marshalled: list[Any] = []
        acker = _acker(
            config=BatchAckConfig(batch_size=2, flush_interval_ms=0),
            marshal=marshalled.append,
        )
        acker.register(1)
        acker.register(2)
        acker.complete(1)
        acker.complete(2)  # size-triggered
        acker.flush()  # manual
        acker.close()  # close
        assert marshalled == []

    def test_a_raising_marshal_is_logged_and_does_not_kill_the_timer(self, caplog: pytest.LogCaptureFixture) -> None:
        def marshal(fn: Any) -> None:
            raise RuntimeError("loop is closed")

        acker = _acker(
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=20),
            marshal=marshal,
        )
        with caplog.at_level(logging.ERROR):
            acker.register(1)
            acker.complete(1)
            # Poll for the LOG RECORD, not for last_error: the timer thread
            # assigns last_error immediately before calling logger.error, so
            # waiting on the attribute alone races the log call.
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                if any("could not marshal" in r.getMessage() for r in caplog.records):
                    break
                time.sleep(0.01)
            logged = [r.getMessage() for r in caplog.records if "could not marshal" in r.getMessage()]
        assert logged, "the marshal failure was never logged"
        assert isinstance(acker.last_error, RuntimeError)
        acker.close()


# ── the real asyncio deadlock this prevents ───────────────────────────────


class TestAsyncioOwnerThread:
    async def test_interval_flush_reaches_an_idle_event_loop(self) -> None:
        """The prefetch=1 deadlock in miniature: the loop is idle, so a
        cross-thread create_task would never be noticed. With
        marshal=loop.call_soon_threadsafe the ack always arrives."""
        loop = asyncio.get_running_loop()
        acked = asyncio.Event()
        emitted_on: list[str] = []

        async def do_ack(tag: int, multiple: bool) -> None:
            emitted_on.append(threading.current_thread().name)
            acked.set()

        acker = _acker(
            ack_fn=lambda t, m: loop.create_task(do_ack(t, m)),
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=20),
            marshal=loop.call_soon_threadsafe,
        )
        acker.register(1)
        acker.complete(1)
        # The loop now has nothing else to do — exactly the idle case.
        await asyncio.wait_for(acked.wait(), timeout=5)
        assert emitted_on == [threading.current_thread().name]
        acker.close()

    async def test_marshal_accepts_call_soon_threadsafe_directly(self) -> None:
        """No wrapper lambda needed: the signature matches."""
        loop = asyncio.get_running_loop()
        acker = _acker(
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=1000),
            marshal=loop.call_soon_threadsafe,
        )
        # bound methods compare equal, not identical (each access makes a new one)
        assert acker._marshal == loop.call_soon_threadsafe
        acker.close()


# ── emit failures must be visible ─────────────────────────────────────────


class TestEmitFailuresAreLogged:
    def test_a_failing_ack_fn_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        def boom(tag: int, multiple: bool) -> None:
            raise ConnectionError("channel closed")

        acker = _acker(ack_fn=boom)
        acker.register(1)
        acker.register(2)
        acker.complete(1)
        acker.complete(2)
        with caplog.at_level(logging.ERROR):
            report = acker.flush()

        assert report.errors, "the failure must still be in the report"
        assert isinstance(acker.last_error, ConnectionError)
        messages = [r.getMessage() for r in caplog.records]
        assert any("failed to emit ack" in m for m in messages), messages
        assert any("delivery_tag=2" in m for m in messages), messages
        assert any("covering 2 tag(s)" in m for m in messages), messages

    def test_every_failing_command_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        def boom(*_: Any) -> None:
            raise RuntimeError("down")

        acker = CoalescingAcker(
            ack_fn=boom,
            nack_fn=boom,
            reject_fn=boom,
            config=BatchAckConfig(batch_size=1000, flush_interval_ms=0),
        )
        acker.register(1)
        acker.register(2)
        acker.register(3)
        acker.complete(1)
        acker.fail(2, requeue=False)
        acker.fail(3, reject=True)
        with caplog.at_level(logging.ERROR):
            report = acker.flush()

        assert len(report.errors) == 3
        logged = [r.getMessage() for r in caplog.records if "failed to emit" in r.getMessage()]
        assert len(logged) == 3
        assert any("emit nack" in m for m in logged)
        assert any("emit reject" in m for m in logged)

    def test_the_generation_is_in_the_log_line(self, caplog: pytest.LogCaptureFixture) -> None:
        def boom(*_: Any) -> None:
            raise RuntimeError("down")

        acker = _acker(ack_fn=boom)
        acker.on_reconnect()  # generation 1
        acker.register(1)
        acker.complete(1)
        with caplog.at_level(logging.ERROR):
            acker.flush()
        assert any("generation 1" in r.getMessage() for r in caplog.records)

    def test_a_successful_flush_logs_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        acker = _acker()
        acker.register(1)
        acker.complete(1)
        with caplog.at_level(logging.ERROR):
            acker.flush(FlushReason.MANUAL)
        assert [r for r in caplog.records if "failed to emit" in r.getMessage()] == []
        assert acker.last_error is None


# ── the same rule holds for the sibling helpers ───────────────────────────


class TestSiblingHelpersShareTheRule:
    """BatchAcker and BatchPublisher call user code from the same
    ``threading.Timer`` thread, so they take the same ``marshal``."""

    def _acker(self, **kw: Any) -> BatchAcker:
        return BatchAcker(ack_fn=lambda t, multiple=False: None, **kw)

    def _publisher(self, **kw: Any) -> BatchPublisher:
        return BatchPublisher(publish_fn=lambda e: None, **kw)

    def test_batch_acker_warns_without_marshal(self) -> None:
        acker = self._acker(config=BatchAckConfig(batch_size=1000, flush_interval_ms=50))
        with pytest.warns(RuntimeWarning, match="BatchAcker has flush_interval_ms"):
            acker.add(1)
        acker._closed = True
        if acker._timer is not None:
            acker._timer.cancel()

    def test_batch_publisher_warns_without_marshal(self) -> None:
        publisher = self._publisher(config=BatchPublishConfig(batch_size=1000, flush_interval_ms=50))
        with pytest.warns(RuntimeWarning, match="BatchPublisher has flush_interval_ms"):
            publisher.add(MessageEnvelope(routing_key="q", body=b"x"))
        publisher._closed = True
        if publisher._timer is not None:
            publisher._timer.cancel()

    def test_batch_acker_interval_flush_is_marshalled(self) -> None:
        acked: list[int] = []
        marshalled: list[str] = []

        def marshal(fn: Any) -> None:
            marshalled.append(threading.current_thread().name)
            fn()

        acker = self._acker(config=BatchAckConfig(batch_size=1000, flush_interval_ms=20), marshal=marshal)
        acker._ack_fn = lambda tag, multiple=False: acked.append(tag)
        acker.add(1)
        deadline = time.monotonic() + 3
        while not acked and time.monotonic() < deadline:
            time.sleep(0.01)
        acker.close()
        assert acked == [1]
        assert marshalled, "the interval flush bypassed marshal"

    def test_batch_publisher_interval_flush_is_marshalled(self) -> None:
        published: list[Any] = []
        marshalled: list[str] = []

        def marshal(fn: Any) -> None:
            marshalled.append(threading.current_thread().name)
            fn()

        publisher = BatchPublisher(
            publish_fn=published.append,
            config=BatchPublishConfig(batch_size=1000, flush_interval_ms=20),
            marshal=marshal,
        )
        publisher.add(MessageEnvelope(routing_key="q", body=b"x"))
        deadline = time.monotonic() + 3
        while not published and time.monotonic() < deadline:
            time.sleep(0.01)
        publisher.close()
        assert len(published) == 1
        assert marshalled, "the interval flush bypassed marshal"

    @pytest.mark.parametrize("interval", [0])
    def test_no_warning_without_a_timer(self, interval: int) -> None:
        with warnings_recorded() as caught:
            self._acker(config=BatchAckConfig(batch_size=1000, flush_interval_ms=interval)).add(1)
            self._publisher(config=BatchPublishConfig(batch_size=1000, flush_interval_ms=interval)).add(
                MessageEnvelope(routing_key="q", body=b"x")
            )
        assert [w for w in caught if "no marshal=" in str(w.message)] == []
