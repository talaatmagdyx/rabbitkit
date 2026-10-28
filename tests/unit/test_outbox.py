"""AsyncOutbox / SyncOutbox relays over fake brokers: relay order, delete only
on CONFIRMED, transient backoff, refusals to the failed state, drain and stop,
crash resume on the same file, confirms required, metrics wiring, parity."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from rabbitkit import (
    AsyncOutbox,
    MessageEnvelope,
    OutboxClosedError,
    OutboxConfig,
    OutboxEntryState,
    OutboxPassReport,
    OutboxStore,
    PublishOutcome,
    PublishReason,
    PublishStatus,
    SyncOutbox,
)
from rabbitkit.core.config import MetricsConfig
from rabbitkit.core.errors import ConfigValidationError

Result = PublishOutcome | Exception
Script = Callable[[MessageEnvelope, int], Result]

CONFIRMED = PublishOutcome(status=PublishStatus.CONFIRMED)
NOT_SENT = PublishOutcome(status=PublishStatus.NOT_SENT, reason=PublishReason.NOT_CONNECTED)
RETURNED = PublishOutcome(status=PublishStatus.RETURNED, reason=PublishReason.RETURNED, reply_code=312)


def _fast(**kw: Any) -> OutboxConfig:
    kw.setdefault("backoff_initial", 0.01)
    kw.setdefault("backoff_max", 0.02)
    kw.setdefault("jitter", 0.0)
    kw.setdefault("poll_interval", 0.02)
    return OutboxConfig(**kw)


def _env(n: int) -> MessageEnvelope:
    return MessageEnvelope(routing_key="orders", body=f"m{n}".encode(), mandatory=True)


def _broker_config(confirm: bool) -> SimpleNamespace:
    return SimpleNamespace(publisher=SimpleNamespace(confirm_delivery=confirm))


class Recorder:
    def __init__(self) -> None:
        self.counters: list[tuple[str, dict[str, str], float]] = []

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        self.counters.append((name, labels, value))

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        raise AssertionError

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        pass

    def total(self, name: str) -> float:
        return sum(v for n, _l, v in self.counters if n == name)


class FakeAsyncBroker:
    def __init__(self, script: Script | None = None, *, confirm: bool = True) -> None:
        self.script = script
        self.config = _broker_config(confirm)
        self.published: list[MessageEnvelope] = []
        self.timeouts: list[float | None] = []
        self.gate: asyncio.Event | None = None

    async def publish(self, envelope: MessageEnvelope | None = None, *, timeout: float | None = None) -> PublishOutcome:
        assert envelope is not None
        self.published.append(envelope)
        self.timeouts.append(timeout)
        if self.gate is not None:
            await self.gate.wait()
        await asyncio.sleep(0)
        result = CONFIRMED if self.script is None else self.script(envelope, len(self.published))
        if isinstance(result, Exception):
            raise result
        return result

    def bodies(self) -> list[bytes]:
        return [e.body for e in self.published]


class FakeSyncBroker:
    def __init__(self, script: Script | None = None, *, confirm: bool = True, delay: float = 0.0) -> None:
        self.script = script
        self.config = _broker_config(confirm)
        self.published: list[MessageEnvelope] = []
        self.delay = delay

    def publish(self, envelope: MessageEnvelope | None = None) -> PublishOutcome:
        assert envelope is not None
        self.published.append(envelope)
        if self.delay:
            time.sleep(self.delay)
        result = CONFIRMED if self.script is None else self.script(envelope, len(self.published))
        if isinstance(result, Exception):
            raise result
        return result


async def _until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "outbox.db"


# ── AsyncOutbox ───────────────────────────────────────────────────────────


class TestAsyncOutbox:
    async def test_relays_in_enqueue_order_and_deletes_on_confirm(self, db: Path) -> None:
        broker = FakeAsyncBroker()
        outbox = AsyncOutbox(broker, db, _fast(max_in_flight=2))
        async with asyncio.timeout(10):
            for n in range(5):
                await outbox.enqueue(_env(n))
            await outbox.start()
            assert outbox.running
            await _until(lambda: len(broker.published) == 5)
            assert await outbox.stop() is True
        assert broker.bodies() == [b"m0", b"m1", b"m2", b"m3", b"m4"]
        assert not outbox.running
        assert OutboxStore(db).stats().pending == 0

    async def test_enqueue_wakes_an_idle_relay_at_once(self, db: Path) -> None:
        broker = FakeAsyncBroker()
        outbox = AsyncOutbox(broker, db, _fast(poll_interval=30.0))
        async with asyncio.timeout(10):
            await outbox.start()
            await asyncio.sleep(0.05)  # relay is idle, waiting up to 30 s
            ids = await outbox.enqueue_many([_env(0), _env(1)])
            await _until(lambda: len(broker.published) == 2, timeout=2)
            await outbox.stop()
        assert len(ids) == 2

    async def test_transient_outcomes_back_off_and_republish_with_the_same_message_id(self, db: Path) -> None:
        broker = FakeAsyncBroker(lambda _e, n: NOT_SENT if n <= 2 else CONFIRMED)
        outbox = AsyncOutbox(broker, db, _fast())
        async with asyncio.timeout(10):
            await outbox.enqueue(_env(0))
            await outbox.start()
            await _until(lambda: len(broker.published) == 3)
            await outbox.stop()
        assert len({e.message_id for e in broker.published}) == 1
        assert (await asyncio.to_thread(OutboxStore(db).stats)).pending == 0

    async def test_refused_entries_end_in_failed_and_replay_delivers_them(self, db: Path) -> None:
        outcomes: dict[str, Result] = {"next": RETURNED}
        broker = FakeAsyncBroker(lambda _e, _n: outcomes["next"])
        outbox = AsyncOutbox(broker, db, _fast(max_refusals=2))
        async with asyncio.timeout(10):
            await outbox.start()
            await outbox.enqueue(_env(0))
            await _until(lambda: len(broker.published) == 2)
            await _until(lambda: outbox.store.stats().failed == 1)
            failed = await outbox.list_entries()
            assert [e.state for e in failed] == [OutboxEntryState.FAILED]
            assert failed[0].last_status == "returned"
            outcomes["next"] = CONFIRMED
            assert await outbox.replay_failed([failed[0].id]) == 1
            await _until(lambda: len(broker.published) == 3)
            await outbox.stop()
        assert (await asyncio.to_thread(OutboxStore(db).stats)).entries == 0

    async def test_publish_exceptions_are_recorded_not_raised(self, db: Path) -> None:
        broker = FakeAsyncBroker(lambda _e, n: RuntimeError("boom") if n == 1 else CONFIRMED)
        outbox = AsyncOutbox(broker, db, _fast())
        async with asyncio.timeout(10):
            await outbox.enqueue(_env(0))
            report = await outbox.relay_once()
            assert report == OutboxPassReport(attempted=1, retrying=1)
            entry = (await outbox.list_entries(OutboxEntryState.PENDING))[0]
            assert entry.last_reason == "exception"
            outbox.store.replay_failed()  # nothing failed: no-op

    async def test_stop_timeout_cancels_and_keeps_the_entries(self, db: Path) -> None:
        broker = FakeAsyncBroker()
        broker.gate = asyncio.Event()  # publishes never complete
        outbox = AsyncOutbox(broker, db, _fast())
        async with asyncio.timeout(10):
            await outbox.enqueue_many([_env(0), _env(1)])
            await outbox.start()
            await _until(lambda: len(broker.published) == 2)
            assert await outbox.stop(timeout=0.05) is False
        assert OutboxStore(db).stats().pending == 2

    async def test_crash_mid_pass_then_restart_delivers_everything_at_least_once(self, db: Path) -> None:
        first = FakeAsyncBroker()
        first.gate = asyncio.Event()  # in flight when the "process" dies
        crashed = AsyncOutbox(first, db, _fast(max_in_flight=3))
        async with asyncio.timeout(10):
            await crashed.enqueue_many([_env(n) for n in range(5)])
            await crashed.start()
            await _until(lambda: len(first.published) == 3)
            # crash: the relay dies without stop(); nothing is recorded
            task = crashed._task
            assert task is not None
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            second = FakeAsyncBroker()
            restarted = AsyncOutbox(second, db, _fast(max_in_flight=3))
            await restarted.start()
            await _until(lambda: len(second.published) == 5)
            assert await restarted.stop() is True
        assert second.bodies() == [f"m{n}".encode() for n in range(5)]
        republished = {e.message_id for e in first.published} & {e.message_id for e in second.published}
        assert len(republished) == 3  # the in-flight window, never more

    async def test_pass_errors_are_logged_and_the_relay_keeps_going(
        self, db: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        broker = FakeAsyncBroker()
        outbox = AsyncOutbox(broker, db, _fast())
        real_due = outbox.store.due
        calls = {"n": 0}

        def flaky_due(limit: int) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("disk hiccup")
            return real_due(limit)

        monkeypatch.setattr(outbox.store, "due", flaky_due)
        async with asyncio.timeout(10):
            await outbox.enqueue(_env(0))
            await outbox.start()
            await _until(lambda: len(broker.published) == 1)
            await outbox.stop()
        assert "Outbox relay pass failed" in caplog.text

    async def test_requires_publisher_confirms(self, db: Path) -> None:
        outbox = AsyncOutbox(FakeAsyncBroker(confirm=False), db)
        with pytest.raises(ConfigValidationError, match="confirm"):
            await outbox.start()

    async def test_enqueue_after_stop_is_refused(self, db: Path) -> None:
        outbox = AsyncOutbox(FakeAsyncBroker(), db, _fast())
        async with asyncio.timeout(10):
            assert await outbox.stop() is True  # never started: just closes
            with pytest.raises(OutboxClosedError):
                await outbox.enqueue(_env(0))
            with pytest.raises(OutboxClosedError):
                await outbox.enqueue_many([_env(0)])

    async def test_start_is_idempotent_and_restartable(self, db: Path) -> None:
        broker = FakeAsyncBroker()
        outbox = AsyncOutbox(broker, db, _fast())
        async with asyncio.timeout(10):
            await outbox.start()
            task = outbox._task
            await outbox.start()
            assert outbox._task is task
            await outbox.stop()
            await outbox.start()
            await outbox.enqueue(_env(0))
            await _until(lambda: len(broker.published) == 1)
            await outbox.stop()

    async def test_context_manager(self, db: Path) -> None:
        broker = FakeAsyncBroker()
        async with asyncio.timeout(10), AsyncOutbox(broker, db, _fast()) as outbox:
            await outbox.enqueue(_env(0))
            await _until(lambda: len(broker.published) == 1)
        assert not outbox.running

    async def test_publish_timeout_is_passed_to_the_broker(self, db: Path) -> None:
        broker = FakeAsyncBroker()
        async with asyncio.timeout(10):
            outbox = AsyncOutbox(broker, db, _fast(publish_timeout=2.5))
            await outbox.enqueue(_env(0))
            await outbox.relay_once()
            plain = AsyncOutbox(broker, db, _fast())
            await plain.enqueue(_env(1))
            await plain.relay_once()
        assert broker.timeouts == [2.5, None]
        assert outbox.config.publish_timeout == 2.5

    async def test_operator_wrappers(self, db: Path) -> None:
        broker = FakeAsyncBroker(lambda _e, _n: RETURNED)
        outbox = AsyncOutbox(broker, db, _fast(max_refusals=1))
        async with asyncio.timeout(10):
            outbox.notify()  # before start: nothing to wake
            await outbox.enqueue_many([_env(0), _env(1)])
            await outbox.relay_once()
            stats = await outbox.stats()
            assert (stats.pending, stats.failed) == (0, 2)
            assert await outbox.replay_failed() == 2
            await outbox.relay_once()
            assert await outbox.purge_failed([(await outbox.list_entries())[0].id]) == 1
            assert await outbox.purge_failed() == 1

    async def test_metrics_come_from_the_broker_or_an_explicit_collector(self, db: Path) -> None:
        from_broker = Recorder()
        broker = FakeAsyncBroker()
        broker._bulk_metrics = lambda: (from_broker, MetricsConfig(namespace="svc"))  # type: ignore[attr-defined]
        async with asyncio.timeout(10):
            outbox = AsyncOutbox(broker, db, _fast())
            await outbox.enqueue(_env(0))
            await outbox.relay_once()
            assert from_broker.total("svc_outbox_enqueued_total") == 1
            assert from_broker.total("svc_outbox_relayed_total") == 1

            explicit = Recorder()
            outbox = AsyncOutbox(broker, db, _fast(), collector=explicit)
            await outbox.enqueue(_env(1))
            assert explicit.total("rabbitkit_outbox_enqueued_total") == 1

            broker._bulk_metrics = lambda: None  # type: ignore[attr-defined]
            await AsyncOutbox(broker, db, _fast()).enqueue(_env(2))


# ── SyncOutbox ────────────────────────────────────────────────────────────


class TestSyncOutbox:
    def test_relay_once_publishes_in_order_and_deletes(self, db: Path) -> None:
        broker = FakeSyncBroker()
        outbox = SyncOutbox(broker, db, _fast())
        outbox.enqueue_many([_env(n) for n in range(3)])
        assert outbox.relay_once() == OutboxPassReport(attempted=3, confirmed=3)
        assert [e.body for e in broker.published] == [b"m0", b"m1", b"m2"]
        assert outbox.stats().pending == 0
        assert outbox.relay_once() == OutboxPassReport()

    def test_a_pass_stops_at_the_first_transient_outcome(self, db: Path) -> None:
        broker = FakeSyncBroker(lambda _e, _n: NOT_SENT)
        outbox = SyncOutbox(broker, db, _fast())
        outbox.enqueue_many([_env(n) for n in range(3)])
        report = outbox.relay_once()
        assert report == OutboxPassReport(attempted=1, retrying=1, transient=True)
        assert len(broker.published) == 1
        assert outbox.stats().pending == 3

    def test_publish_exceptions_are_refusals(self, db: Path) -> None:
        outbox = SyncOutbox(FakeSyncBroker(lambda _e, _n: ValueError("bad")), db, _fast(max_refusals=1))
        outbox.enqueue(_env(0))
        assert outbox.relay_once().failed == 1
        assert outbox.list_entries()[0].last_reason == "exception"

    def test_drain_returns_true_when_nothing_is_due(self, db: Path) -> None:
        broker = FakeSyncBroker(lambda _e, n: CONFIRMED if n != 2 else RETURNED)
        outbox = SyncOutbox(broker, db, _fast(max_in_flight=1, backoff_initial=30.0, backoff_max=30.0))
        outbox.enqueue_many([_env(n) for n in range(3)])
        assert outbox.drain(timeout=5) is True
        # the refused one is backing off: pending, not due, not lost
        assert outbox.stats().pending == 1
        assert len(broker.published) == 3

    def test_drain_gives_up_at_the_deadline_while_the_broker_is_down(self, db: Path) -> None:
        broker = FakeSyncBroker(lambda _e, _n: NOT_SENT)
        outbox = SyncOutbox(broker, db, _fast())
        outbox.enqueue(_env(0))
        started = time.monotonic()
        assert outbox.drain(timeout=0.1) is False
        assert time.monotonic() - started < 2
        assert len(broker.published) >= 2
        assert outbox.stats().pending == 1

    def test_drain_checks_the_deadline_between_publishes(self, db: Path) -> None:
        broker = FakeSyncBroker(delay=0.03)
        outbox = SyncOutbox(broker, db, _fast(max_in_flight=10))
        outbox.enqueue_many([_env(n) for n in range(10)])
        assert outbox.drain(timeout=0.05) is False
        assert 1 <= len(broker.published) < 10
        assert outbox.stats().pending == 10 - len(broker.published)

    def test_run_relays_until_request_stop_then_drains(self, db: Path) -> None:
        broker = FakeSyncBroker(lambda _e, n: NOT_SENT if n == 1 else CONFIRMED)
        outbox = SyncOutbox(broker, db, _fast(poll_interval=30.0))
        results: list[bool] = []
        relay = threading.Thread(target=lambda: results.append(outbox.run()))
        relay.start()
        try:
            outbox.enqueue(_env(0))  # from another thread: wakes the idle relay
            deadline = time.monotonic() + 5
            while len(broker.published) < 2 and time.monotonic() < deadline:
                time.sleep(0.005)
            outbox.enqueue(_env(1))
        finally:
            outbox.request_stop()
            relay.join(timeout=10)
        assert results == [True]
        assert outbox.stats().pending == 0
        assert [e.body for e in broker.published] == [b"m0", b"m0", b"m1"]

    def test_run_survives_a_failing_pass(
        self, db: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        broker = FakeSyncBroker()
        outbox = SyncOutbox(broker, db, _fast())
        real_due = outbox.store.due
        calls = {"n": 0}

        def flaky_due(limit: int) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("disk hiccup")
            if calls["n"] > 3:
                outbox.request_stop()
            return real_due(limit)

        monkeypatch.setattr(outbox.store, "due", flaky_due)
        outbox.enqueue(_env(0))
        assert outbox.run() is True
        assert len(broker.published) == 1
        assert "Outbox relay pass failed" in caplog.text

    def test_close_refuses_further_use(self, db: Path) -> None:
        with SyncOutbox(FakeSyncBroker(), db, _fast()) as outbox:
            outbox.enqueue(_env(0))
        with pytest.raises(OutboxClosedError):
            outbox.enqueue(_env(1))
        with pytest.raises(OutboxClosedError):
            outbox.enqueue_many([_env(1)])
        with pytest.raises(OutboxClosedError):
            outbox.relay_once()
        assert OutboxStore(db).stats().pending == 1

    def test_requires_publisher_confirms(self, db: Path) -> None:
        outbox = SyncOutbox(FakeSyncBroker(confirm=False), db)
        outbox.enqueue(_env(0))
        with pytest.raises(ConfigValidationError):
            outbox.relay_once()

    def test_operator_wrappers(self, db: Path) -> None:
        outbox = SyncOutbox(FakeSyncBroker(lambda _e, _n: RETURNED), db, _fast(max_refusals=1))
        outbox.enqueue_many([_env(0), _env(1)])
        outbox.relay_once()
        assert outbox.stats().failed == 2
        assert outbox.replay_failed([outbox.list_entries()[0].id]) == 1
        outbox.notify()
        assert outbox.purge_failed() == 1
        assert outbox.config.max_refusals == 1


# ── parity ────────────────────────────────────────────────────────────────


class TestParity:
    """The two relays share one store and one vocabulary; differences are the
    lifecycle each concurrency model needs (see docs/outbox.md)."""

    ASYNC_ONLY: ClassVar[set[str]] = {"start", "stop", "running"}
    SYNC_ONLY: ClassVar[set[str]] = {"drain", "run", "request_stop", "close"}

    @staticmethod
    def _public(cls: type) -> set[str]:
        return {name for name in dir(cls) if not name.startswith("_")}

    def test_only_the_documented_lifecycle_differs(self) -> None:
        assert self._public(AsyncOutbox) - self._public(SyncOutbox) == self.ASYNC_ONLY
        assert self._public(SyncOutbox) - self._public(AsyncOutbox) == self.SYNC_ONLY
