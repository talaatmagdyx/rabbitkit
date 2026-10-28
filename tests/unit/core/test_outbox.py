"""OutboxStore: durability settings, codec, ordering, bounds, outcome policy,
failed state, crash resume on a reopened file, transactional enqueue, metrics."""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from rabbitkit.core.config import MetricsConfig, OutboxConfig
from rabbitkit.core.errors import (
    BrokerNotStartedError,
    ConfigValidationError,
    MessageTooLargeError,
    OutboxError,
    OutboxFullError,
)
from rabbitkit.core.outbox import (
    OutboxEntry,
    OutboxPassReport,
    OutboxStats,
    OutboxStore,
    _decode_value,
    is_transient_result,
    outbox_backoff,
)
from rabbitkit.core.types import MessageEnvelope, OutboxEntryState, PublishOutcome, PublishReason, PublishStatus
from rabbitkit.middleware.metrics import bounded_label_values, metric_inventory


class Clock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class Recorder:
    def __init__(self) -> None:
        self.counters: list[tuple[str, dict[str, str], float]] = []
        self.gauges: dict[str, float] = {}
        self.emitted: list[tuple[str, str, dict[str, str]]] = []

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        self.counters.append((name, dict(labels), value))
        self.emitted.append(("counter", name, dict(labels)))

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        self.emitted.append(("histogram", name, dict(labels)))

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        self.gauges[name] = value
        self.emitted.append(("gauge", name, dict(labels)))

    def total(self, name: str, **labels: str) -> float:
        return sum(v for n, lab, v in self.counters if n == name and all(lab.get(k) == x for k, x in labels.items()))


def _env(n: int = 0, **kw: Any) -> MessageEnvelope:
    return MessageEnvelope(routing_key="orders", body=f"m{n}".encode(), **kw)


def _cfg(**kw: Any) -> OutboxConfig:
    kw.setdefault("jitter", 0.0)
    return OutboxConfig(**kw)


def _ok() -> PublishOutcome:
    return PublishOutcome(status=PublishStatus.CONFIRMED)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "outbox.db"


# ── durability settings ───────────────────────────────────────────────────


class TestDurability:
    def test_store_connection_uses_wal_and_synchronous_full(self, db: Path) -> None:
        store = OutboxStore(db)
        store.open()
        conn = store._conn
        assert conn is not None
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL
        assert conn.execute("PRAGMA fullfsync").fetchone()[0] == 0

    def test_full_fsync_option_turns_on_fullfsync(self, db: Path) -> None:
        store = OutboxStore(db, _cfg(full_fsync=True))
        store.open()
        conn = store._conn
        assert conn is not None
        assert conn.execute("PRAGMA fullfsync").fetchone()[0] == 1
        assert conn.execute("PRAGMA checkpoint_fullfsync").fetchone()[0] == 1

    def test_connect_returns_an_app_connection_with_the_same_guarantees(self, db: Path) -> None:
        store = OutboxStore(db)
        conn = store.connect()
        try:
            assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2
            assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            assert conn.isolation_level == ""  # Python's default transaction handling
        finally:
            conn.close()

    def test_wal_refused_is_an_error(self, db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        store = OutboxStore(db)

        class NoWal:
            def __init__(self, conn: sqlite3.Connection) -> None:
                self._conn = conn

            def execute(self, sql: str, *args: Any) -> Any:
                if sql.startswith("PRAGMA journal_mode"):
                    return self._conn.execute("SELECT 'delete'")
                return self._conn.execute(sql, *args)

            def close(self) -> None:
                self._conn.close()

        real_connect = sqlite3.connect
        monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: NoWal(real_connect(*a, **k)))
        with pytest.raises(OutboxError, match="WAL"):
            store.open()

    @pytest.mark.parametrize("path", ["", ":memory:", "file:x?mode=memory"])
    def test_non_file_paths_are_rejected(self, path: str) -> None:
        with pytest.raises(ValueError, match="plain file path"):
            OutboxStore(path)

    def test_schema_version_mismatch_is_refused(self, db: Path) -> None:
        OutboxStore(db).open()
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE rabbitkit_outbox_meta SET value = '99' WHERE key = 'schema_version'")
        store = OutboxStore(db)
        with pytest.raises(OutboxError, match="schema version 99"):
            store.open()
        assert store._conn is None

    def test_close_releases_and_next_call_reopens(self, db: Path) -> None:
        store = OutboxStore(db)
        store.enqueue(_env())
        store.close()
        store.close()  # idempotent
        assert store._conn is None
        assert store.stats().pending == 1
        assert store.path == str(db)
        assert store.config == OutboxConfig()


# ── codec ─────────────────────────────────────────────────────────────────


class TestCodec:
    def test_every_property_and_header_type_round_trips(self, db: Path) -> None:
        store = OutboxStore(db)
        envelope = MessageEnvelope(
            routing_key="orders.created",
            body=b"\x00\xffbinary",
            exchange="orders",
            headers={
                "s": "text",
                "i": 7,
                "f": 1.5,
                "b": True,
                "n": None,
                "y": b"\x01\x02",
                "t": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
                "d": Decimal("12.50"),
                "l": [1, "two", [3.0], {"k": False}],
                "m": {"nested": {"deep": b"x"}},
                "tuple": (1, 2),
            },
            message_id="mid-1",
            correlation_id="corr",
            reply_to="replies",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2))),
            content_type="application/octet-stream",
            content_encoding="gzip",
            expiration="60000",
            priority=5,
            mandatory=True,
            delivery_mode=1,
            type="order.created",
            user_id="guest",
            app_id="shop",
        )
        store.enqueue(envelope)
        stored = store.due(10)[0].envelope
        expected_headers = dict(envelope.headers, tuple=[1, 2])
        assert stored.headers == expected_headers
        assert type(stored.headers["y"]) is bytes
        assert type(stored.headers["d"]) is Decimal
        assert stored == MessageEnvelope(**{**_fields(envelope), "headers": expected_headers})

    def test_bytearray_body_is_stored_as_bytes(self, db: Path) -> None:
        store = OutboxStore(db)
        store.enqueue(MessageEnvelope(routing_key="q", body=bytearray(b"abc")))  # type: ignore[arg-type]
        assert store.due(1)[0].envelope.body == b"abc"

    def test_missing_message_id_gets_a_stable_one(self, db: Path) -> None:
        store = OutboxStore(db)
        store.enqueue(_env(message_id=""))
        first = store.due(1)[0].envelope.message_id
        assert first
        store.close()
        assert OutboxStore(db).due(1)[0].envelope.message_id == first

    @pytest.mark.parametrize(
        ("headers", "match"),
        [
            ({"bad": {1, 2}}, "header 'bad' of type set"),
            ({"bad": [object()]}, r"header 'bad\[0\]'"),
            ({"bad": {1: "x"}}, "key 1 is not a str"),
            ({"m": {"inner": object()}}, "header 'm.inner'"),
        ],
    )
    def test_unstorable_header_values_fail_at_enqueue(self, db: Path, headers: dict[str, Any], match: str) -> None:
        store = OutboxStore(db)
        with pytest.raises(TypeError, match=match):
            store.enqueue(_env(headers=headers))
        assert store.stats().pending == 0

    def test_non_envelope_and_non_bytes_body_are_rejected(self, db: Path) -> None:
        store = OutboxStore(db)
        with pytest.raises(TypeError, match="MessageEnvelope"):
            store.enqueue({"routing_key": "q"})  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="body must be bytes"):
            store.enqueue(MessageEnvelope(routing_key="q", body="text"))  # type: ignore[arg-type]

    def test_unknown_tag_and_version_are_errors(self, db: Path) -> None:
        with pytest.raises(OutboxError, match="tag"):
            _decode_value(["?", 1])
        store = OutboxStore(db)
        store.enqueue(_env())
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE rabbitkit_outbox SET properties = '{\"v\": 2}'")
        with pytest.raises(OutboxError, match="version 2"):
            store.due(1)


def _fields(envelope: MessageEnvelope) -> dict[str, Any]:
    import dataclasses

    return {f.name: getattr(envelope, f.name) for f in dataclasses.fields(envelope)}


# ── ordering, bounds ──────────────────────────────────────────────────────


class TestOrderingAndBounds:
    def test_due_returns_oldest_first_and_respects_the_limit(self, db: Path) -> None:
        store = OutboxStore(db)
        ids = [store.enqueue(_env(n)) for n in range(5)]
        assert ids == sorted(ids)
        assert [e.envelope.body for e in store.due(3)] == [b"m0", b"m1", b"m2"]
        assert [e.id for e in store.due(10)] == ids

    def test_enqueue_many_is_one_ordered_batch(self, db: Path) -> None:
        store = OutboxStore(db)
        ids = store.enqueue_many([_env(n) for n in range(3)])
        assert [e.id for e in store.due(10)] == ids
        assert store.enqueue_many([]) == []

    def test_max_entries_refuses_and_writes_nothing(self, db: Path) -> None:
        rec = Recorder()
        store = OutboxStore(db, _cfg(max_entries=2), collector=rec)
        store.enqueue_many([_env(0), _env(1)])
        with pytest.raises(OutboxFullError) as info:
            store.enqueue(_env(2))
        assert info.value.limit == "max_entries"
        assert store.stats() == OutboxStats(pending=2, failed=0, bytes=store.stats().bytes)
        assert rec.total("rabbitkit_outbox_full_total", reason="max_entries") == 1

    def test_a_batch_that_does_not_fit_is_all_or_nothing(self, db: Path) -> None:
        store = OutboxStore(db, _cfg(max_entries=3))
        store.enqueue(_env())
        with pytest.raises(OutboxFullError):
            store.enqueue_many([_env(n) for n in range(3)])
        assert store.stats().pending == 1

    def test_max_bytes_refuses(self, db: Path) -> None:
        store = OutboxStore(db, _cfg(max_bytes=600))
        store.enqueue(_env(body_size := 0))
        assert body_size == 0
        with pytest.raises(OutboxFullError) as info:
            store.enqueue(MessageEnvelope(routing_key="q", body=b"x" * 600))
        assert info.value.limit == "max_bytes"
        assert store.stats().pending == 1

    def test_failed_entries_count_against_the_bound(self, db: Path) -> None:
        store = OutboxStore(db, _cfg(max_entries=1, max_refusals=1))
        store.enqueue(_env())
        store.settle([(store.due(1)[0], PublishOutcome(status=PublishStatus.NACKED))])
        assert store.stats() == OutboxStats(pending=0, failed=1, bytes=store.stats().bytes)
        assert store.stats().entries == 1
        with pytest.raises(OutboxFullError):
            store.enqueue(_env())
        store.purge_failed()
        store.enqueue(_env())

    def test_stats_bytes_follow_inserts_and_deletes(self, db: Path) -> None:
        store = OutboxStore(db)
        store.enqueue_many([_env(0), _env(1)])
        two = store.stats().bytes
        assert two > 0
        entries = store.due(10)
        store.settle([(entries[0], _ok())])
        assert 0 < store.stats().bytes < two
        store.settle([(entries[1], _ok())])
        assert store.stats() == OutboxStats(pending=0, failed=0, bytes=0)

    def test_concurrent_enqueues_from_threads_are_all_stored_once(self, db: Path) -> None:
        store = OutboxStore(db)
        ids: list[int] = []
        lock = threading.Lock()

        def worker(base: int) -> None:
            for n in range(25):
                entry_id = store.enqueue(_env(base + n))
                with lock:
                    ids.append(entry_id)

        threads = [threading.Thread(target=worker, args=(i * 100,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert len(ids) == len(set(ids)) == 100
        assert store.stats().pending == 100
        assert [e.id for e in store.due(1000)] == sorted(ids)


# ── outcome policy ────────────────────────────────────────────────────────


class TestSettle:
    def test_confirmed_deletes(self, db: Path) -> None:
        store = OutboxStore(db)
        store.enqueue(_env())
        report = store.settle([(store.due(1)[0], _ok())])
        assert report == OutboxPassReport(attempted=1, confirmed=1)
        assert store.stats().pending == 0

    @pytest.mark.parametrize(
        "result",
        [
            PublishOutcome(status=PublishStatus.NOT_SENT, reason=PublishReason.NOT_CONNECTED),
            PublishOutcome(status=PublishStatus.UNKNOWN, reason=PublishReason.CONNECTION_LOST),
            PublishOutcome(status=PublishStatus.TIMEOUT, reason=PublishReason.CONFIRM_TIMEOUT),
            BrokerNotStartedError("stopped"),
        ],
    )
    def test_transient_outcomes_keep_the_entry_due_without_spending_refusals(
        self, db: Path, result: PublishOutcome | BaseException
    ) -> None:
        store = OutboxStore(db, _cfg(max_refusals=1))
        store.enqueue(_env())
        for _ in range(5):
            report = store.settle([(store.due(1)[0], result)])
            assert report.transient
            assert report.retrying == 1
        entry = store.due(1)[0]
        assert entry.state is OutboxEntryState.PENDING
        assert entry.refusals == 0
        assert entry.attempts == 5
        assert entry.next_attempt_at == 0.0
        assert entry.last_status in {"not_sent", "unknown", "timeout"}
        assert is_transient_result(result)

    @pytest.mark.parametrize(
        "result",
        [
            PublishOutcome(status=PublishStatus.RETURNED, reason="returned", reply_code=312, reply_text="NO_ROUTE"),
            PublishOutcome(status=PublishStatus.NACKED, reason="nacked"),
            PublishOutcome(status=PublishStatus.ERROR, reason="exchange_not_found", error=RuntimeError("404")),
            ValueError("bad envelope"),
        ],
    )
    def test_refused_outcomes_back_off_then_fail(self, db: Path, result: PublishOutcome | BaseException) -> None:
        clock = Clock()
        rec = Recorder()
        store = OutboxStore(db, _cfg(max_refusals=3, backoff_initial=1.0), collector=rec, clock=clock)
        store.enqueue(_env())
        delays = []
        for attempt in (1, 2):
            entry = store.due(1)[0]
            report = store.settle([(entry, result)])
            assert report == OutboxPassReport(attempted=1, retrying=1)
            assert store.due(1) == []  # backing off
            pending = store.list_entries(OutboxEntryState.PENDING)[0]
            assert pending.refusals == attempt
            delays.append(pending.next_attempt_at - clock.now)
            clock.now = pending.next_attempt_at
        assert delays == [1.0, 2.0]
        report = store.settle([(store.due(1)[0], result)])
        assert report == OutboxPassReport(attempted=1, failed=1)
        failed = store.list_entries()
        assert len(failed) == 1
        assert failed[0].state is OutboxEntryState.FAILED
        assert failed[0].failed_at == clock.now
        assert failed[0].last_status == (result.status.value if isinstance(result, PublishOutcome) else "error")
        assert store.stats() == OutboxStats(pending=0, failed=1, bytes=store.stats().bytes)
        assert rec.total("rabbitkit_outbox_failed_total") == 1
        assert rec.total("rabbitkit_outbox_publish_retries_total") == 2
        assert rec.gauges["rabbitkit_outbox_failed_entries"] == 1
        assert not is_transient_result(result)

    @pytest.mark.parametrize(
        "result",
        [PublishOutcome(status=PublishStatus.SENT), MessageTooLargeError("too big")],
    )
    def test_unconfirmable_outcomes_fail_at_once(self, db: Path, result: PublishOutcome | BaseException) -> None:
        store = OutboxStore(db, _cfg(max_refusals=5))
        store.enqueue(_env())
        assert store.settle([(store.due(1)[0], result)]).failed == 1
        failed = store.list_entries()[0]
        assert failed.last_error

    def test_error_text_is_truncated_and_reasons_are_bounded(self, db: Path) -> None:
        store = OutboxStore(db)
        store.enqueue(_env())
        store.settle(
            [
                (
                    store.due(1)[0],
                    PublishOutcome(status=PublishStatus.NACKED, reason="custom", error=ValueError("x" * 900)),
                )
            ]
        )
        entry = store.list_entries(OutboxEntryState.PENDING)[0]
        assert entry.last_reason == "other"
        assert entry.last_error is not None
        assert len(entry.last_error) == 500
        assert entry.last_error.endswith("...")

    def test_one_pass_mixes_outcomes_in_one_transaction(self, db: Path) -> None:
        store = OutboxStore(db, _cfg(max_refusals=1))
        store.enqueue_many([_env(n) for n in range(4)])
        entries = store.due(10)
        report = store.settle(
            [
                (entries[0], _ok()),
                (entries[1], PublishOutcome(status=PublishStatus.UNKNOWN)),
                (entries[2], PublishOutcome(status=PublishStatus.RETURNED)),
                (entries[3], _ok()),
            ]
        )
        assert report == OutboxPassReport(attempted=4, confirmed=2, retrying=1, failed=1, transient=True)
        assert [e.id for e in store.due(10)] == [entries[1].id]
        assert [e.id for e in store.list_entries()] == [entries[2].id]

    def test_a_failed_commit_rolls_back_and_raises(self, db: Path) -> None:
        store = OutboxStore(db)
        store.enqueue(_env())
        entry = store.due(1)[0]
        conn = store._conn
        assert conn is not None
        conn.execute("CREATE TRIGGER boom BEFORE DELETE ON rabbitkit_outbox BEGIN SELECT RAISE(ABORT, 'disk'); END")
        with pytest.raises(sqlite3.IntegrityError, match="disk"):
            store.settle([(entry, _ok())])
        assert not conn.in_transaction
        assert store.stats().pending == 1

    def test_backoff_is_capped_and_jittered(self) -> None:
        cfg = OutboxConfig(backoff_initial=1.0, backoff_max=8.0, jitter=0.5)
        assert outbox_backoff(cfg, 1, lambda: 0.5) == 1.0
        assert outbox_backoff(cfg, 3, lambda: 0.5) == 4.0
        assert outbox_backoff(cfg, 50, lambda: 0.5) == 8.0
        assert outbox_backoff(cfg, 1, lambda: 0.0) == 0.5
        assert outbox_backoff(cfg, 1, lambda: 0.999999) == pytest.approx(1.5, rel=1e-5)
        assert outbox_backoff(OutboxConfig(jitter=0.0), 2, lambda: 0.0) == 1.0


# ── crash resume ──────────────────────────────────────────────────────────


class TestCrashResume:
    def test_entries_taken_by_a_crashed_relay_are_due_again_in_a_new_store(self, db: Path) -> None:
        first = OutboxStore(db)
        first.enqueue_many([_env(n) for n in range(5)])
        taken = first.due(3)  # "in flight" when the process dies: never settled
        first.settle([(taken[0], _ok())])
        # simulate the crash: no close(), a new process opens the same file
        second = OutboxStore(db)
        resumed = second.due(10)
        assert [e.envelope.body for e in resumed] == [b"m1", b"m2", b"m3", b"m4"]
        assert [e.envelope.message_id for e in resumed[:2]] == [e.envelope.message_id for e in taken[1:]]

    def test_backoff_and_failed_state_survive_a_restart(self, db: Path) -> None:
        clock = Clock()
        first = OutboxStore(db, _cfg(max_refusals=2), clock=clock)
        first.enqueue_many([_env(0), _env(1)])
        a, b = first.due(2)
        first.settle([(a, PublishOutcome(status=PublishStatus.RETURNED))])
        first.settle([(b, PublishOutcome(status=PublishStatus.SENT))])
        second = OutboxStore(db, _cfg(max_refusals=2), clock=clock)
        assert second.due(10) == []
        assert [e.id for e in second.list_entries()] == [b.id]
        clock.now += 60
        assert [e.id for e in second.due(10)] == [a.id]


# ── failed state operations ───────────────────────────────────────────────


class TestFailedState:
    def _two_failed(self, db: Path) -> tuple[OutboxStore, list[OutboxEntry]]:
        store = OutboxStore(db, _cfg(max_refusals=1))
        store.enqueue_many([_env(0), _env(1), _env(2)])
        entries = store.due(3)
        store.settle([(entries[0], PublishOutcome(status=PublishStatus.NACKED)), (entries[1], _ok())])
        store.settle([(entries[2], PublishOutcome(status=PublishStatus.RETURNED))])
        return store, store.list_entries()

    def test_replay_selected_ids_moves_them_back_with_a_fresh_budget(self, db: Path) -> None:
        store, failed = self._two_failed(db)
        assert store.replay_failed([failed[1].id, 999]) == 1
        due = store.due(10)
        assert [e.id for e in due] == [failed[1].id]
        assert due[0].refusals == 0
        assert due[0].failed_at is None
        assert due[0].envelope.message_id == failed[1].envelope.message_id
        assert [e.id for e in store.list_entries()] == [failed[0].id]

    def test_replay_all(self, db: Path) -> None:
        store, failed = self._two_failed(db)
        assert store.replay_failed() == 2
        assert [e.id for e in store.due(10)] == [e.id for e in failed]
        assert store.stats().failed == 0

    def test_purge_selected_and_all(self, db: Path) -> None:
        store, failed = self._two_failed(db)
        assert store.purge_failed([failed[0].id]) == 1
        assert store.purge_failed() == 1
        assert store.stats() == OutboxStats(pending=0, failed=0, bytes=0)

    def test_a_bad_id_rolls_the_whole_action_back(self, db: Path) -> None:
        store, failed = self._two_failed(db)
        with pytest.raises(ValueError):
            store.replay_failed([failed[0].id, "not-an-id"])  # type: ignore[list-item]
        assert store.stats().failed == 2

    def test_pending_entries_are_never_touched_by_failed_actions(self, db: Path) -> None:
        store = OutboxStore(db)
        entry_id = store.enqueue(_env())
        assert store.purge_failed([entry_id]) == 0
        assert store.replay_failed([entry_id]) == 0
        assert store.stats().pending == 1

    def test_list_entries_pages_with_after_id(self, db: Path) -> None:
        store = OutboxStore(db)
        ids = store.enqueue_many([_env(n) for n in range(5)])
        page = store.list_entries("pending", limit=2)  # type: ignore[arg-type]
        assert [e.id for e in page] == ids[:2]
        assert [e.id for e in store.list_entries(OutboxEntryState.PENDING, limit=2, after_id=ids[1])] == ids[2:4]


# ── transactional enqueue ─────────────────────────────────────────────────


class TestEnqueueIn:
    def test_commits_with_the_business_write(self, db: Path) -> None:
        store = OutboxStore(db)
        conn = store.connect()
        try:
            with conn:
                conn.execute("CREATE TABLE orders (id INTEGER)")
            with conn:
                conn.execute("INSERT INTO orders VALUES (1)")
                store.enqueue_in(conn, _env())
                # not visible to the relay before the commit
                assert store.stats().pending == 0
            assert store.stats().pending == 1
        finally:
            conn.close()

    def test_rolls_back_with_the_business_write(self, db: Path) -> None:
        store = OutboxStore(db)
        conn = store.connect()
        try:
            with conn:
                conn.execute("CREATE TABLE orders (id INTEGER)")
            with pytest.raises(RuntimeError), conn:
                conn.execute("INSERT INTO orders VALUES (1)")
                store.enqueue_in(conn, _env())
                raise RuntimeError("business rule failed")
            assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0
            assert store.stats().pending == 0
        finally:
            conn.close()

    def test_full_outbox_removes_the_row_and_raises(self, db: Path) -> None:
        rec = Recorder()
        store = OutboxStore(db, _cfg(max_entries=1), collector=rec)
        store.enqueue(_env())
        conn = store.connect()
        try:
            with pytest.raises(OutboxFullError), conn:
                store.enqueue_in(conn, _env(1))
            assert store.stats().pending == 1
            assert rec.total("rabbitkit_outbox_full_total") == 1
        finally:
            conn.close()

    def test_counts_enqueued(self, db: Path) -> None:
        rec = Recorder()
        store = OutboxStore(db, collector=rec)
        conn = store.connect()
        try:
            with conn:
                store.enqueue_in(conn, _env())
        finally:
            conn.close()
        assert rec.total("rabbitkit_outbox_enqueued_total") == 1

    def test_requires_synchronous_full(self, db: Path) -> None:
        store = OutboxStore(db)
        store.open()
        conn = sqlite3.connect(db)
        try:
            conn.execute("PRAGMA synchronous=NORMAL")
            with pytest.raises(OutboxError, match="synchronous=FULL"):
                store.enqueue_in(conn, _env())
        finally:
            conn.close()

    def test_requires_a_connection_to_the_same_file(self, db: Path, tmp_path: Path) -> None:
        store = OutboxStore(db)
        other = sqlite3.connect(tmp_path / "other.db")
        try:
            other.execute("PRAGMA synchronous=FULL")
            with pytest.raises(OutboxError, match="needs a connection to"):
                store.enqueue_in(other, _env())
        finally:
            other.close()


# ── metrics ───────────────────────────────────────────────────────────────


class TestMetrics:
    def test_every_emission_is_in_the_inventory_with_bounded_labels(self, db: Path) -> None:
        rec = Recorder()
        store = OutboxStore(db, _cfg(max_entries=6, max_refusals=1), collector=rec)
        store.enqueue_many([_env(n) for n in range(6)])
        with pytest.raises(OutboxFullError):
            store.enqueue(_env())
        entries = store.due(10)
        results: list[PublishOutcome | BaseException] = [
            _ok(),
            PublishOutcome(status=PublishStatus.TIMEOUT, reason="confirm_timeout"),
            PublishOutcome(status=PublishStatus.RETURNED, reason="weird"),
            PublishOutcome(status=PublishStatus.SENT),
            RuntimeError("x"),
            BrokerNotStartedError("x"),
        ]
        store.settle(list(zip(entries, results, strict=True)))
        store.stats()
        specs = {spec.name: spec for spec in metric_inventory()}
        allowed = bounded_label_values()
        assert {name for _k, name, _l in rec.emitted} >= {
            "rabbitkit_outbox_enqueued_total",
            "rabbitkit_outbox_full_total",
            "rabbitkit_outbox_relayed_total",
            "rabbitkit_outbox_publish_retries_total",
            "rabbitkit_outbox_failed_total",
            "rabbitkit_outbox_pending",
            "rabbitkit_outbox_failed_entries",
            "rabbitkit_outbox_bytes",
        }
        for kind, name, labels in rec.emitted:
            spec = specs[name]
            assert spec.kind == kind
            assert tuple(sorted(labels)) == tuple(sorted(spec.labels))
            for label, value in labels.items():
                values = allowed[label]
                assert values is not None
                assert value in values, (name, label, value)

    def test_namespace_and_no_collector(self, db: Path) -> None:
        rec = Recorder()
        store = OutboxStore(db, collector=rec, metrics_config=MetricsConfig(namespace="svc"))
        store.enqueue(_env())
        assert rec.total("svc_outbox_enqueued_total") == 1
        OutboxStore(db).enqueue(_env())  # no collector: no-op

    def test_collector_without_set_gauge_gets_counters_only(self, db: Path) -> None:
        counted: list[str] = []

        class CounterOnly:
            def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
                counted.append(name)

            def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
                raise AssertionError

        store = OutboxStore(db, collector=CounterOnly())  # type: ignore[arg-type]
        store.enqueue(_env())
        store.stats()
        assert counted == ["rabbitkit_outbox_enqueued_total"]


# ── config ────────────────────────────────────────────────────────────────


class TestOutboxConfig:
    def test_defaults(self) -> None:
        cfg = OutboxConfig()
        assert cfg.max_entries == 100_000
        assert cfg.max_in_flight == 100
        assert cfg.publish_timeout is None

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_entries": 0},
            {"max_bytes": -1},
            {"max_in_flight": True},
            {"max_refusals": 1.5},
            {"backoff_initial": 0},
            {"poll_interval": -1},
            {"drain_timeout": 0},
            {"busy_timeout": 0},
            {"backoff_initial": 2.0, "backoff_max": 1.0},
            {"jitter": 1.0},
            {"jitter": -0.1},
            {"publish_timeout": 0},
        ],
    )
    def test_invalid_values_raise(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ConfigValidationError):
            OutboxConfig(**kwargs)

    def test_frozen(self) -> None:
        cfg = OutboxConfig()
        with pytest.raises(AttributeError):
            cfg.max_entries = 1  # type: ignore[misc]
