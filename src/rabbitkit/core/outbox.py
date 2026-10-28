"""Producer outbox: a durable, bounded SQLite store and the relay policy.

Transport-free (stdlib ``sqlite3`` only). :class:`OutboxStore` persists whole
:class:`~rabbitkit.core.types.MessageEnvelope` objects and decides what each
publish outcome does to an entry; ``rabbitkit.outbox.AsyncOutbox`` and
``SyncOutbox`` are the thin relays that publish through a broker. See
``docs/outbox.md`` for the guarantees.

Durability: the store's connection runs ``journal_mode=WAL`` with
``synchronous=FULL``, so a committed write survives a process crash and, on
storage that honours ``fsync``, an OS crash or power loss. Every write is one
transaction. An entry stays ``pending`` until the transaction that records
its ``CONFIRMED`` outcome commits; there is no "in flight" state, which is
what makes a crashed relay's entries publish again (at-least-once).

Thread safety: one connection guarded by a lock; every method may be called
from any thread. Methods block on disk I/O, so async code runs them in
``asyncio.to_thread`` (``AsyncOutbox`` does).
"""

from __future__ import annotations

import base64
import json
import logging
import os
import random
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from rabbitkit.core.config import MetricsConfig, OutboxConfig
from rabbitkit.core.errors import BrokerNotStartedError, MessageTooLargeError, OutboxError, OutboxFullError
from rabbitkit.core.retry_handoff import _capped_exponential
from rabbitkit.core.types import (
    MessageEnvelope,
    OutboxEntryState,
    PublishOutcome,
    PublishReason,
    PublishStatus,
    safe_exception_repr,
)

if TYPE_CHECKING:
    from rabbitkit.middleware.metrics import MetricsCollector

__all__ = [
    "OutboxEntry",
    "OutboxPassReport",
    "OutboxStats",
    "OutboxStore",
    "is_transient_result",
    "outbox_backoff",
]

logger = logging.getLogger("rabbitkit.outbox")

_SCHEMA_VERSION = 1
_ERROR_TEXT_MAX = 500
# Bounded metric label values (they must stay inside
# rabbitkit.middleware.metrics.bounded_label_values()).
_REASON_NONE = "none"
_REASON_OTHER = "other"
_REASON_EXCEPTION = "exception"

_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS rabbitkit_outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending',
        enqueued_at REAL NOT NULL,
        next_attempt_at REAL NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0,
        refusals INTEGER NOT NULL DEFAULT 0,
        last_status TEXT,
        last_reason TEXT,
        last_error TEXT,
        failed_at REAL,
        size INTEGER NOT NULL,
        properties TEXT NOT NULL,
        body BLOB NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS rabbitkit_outbox_state_id ON rabbitkit_outbox (state, id)",
    """
    CREATE TABLE IF NOT EXISTS rabbitkit_outbox_stats (
        one INTEGER PRIMARY KEY CHECK (one = 1),
        entries INTEGER NOT NULL,
        bytes INTEGER NOT NULL,
        failed INTEGER NOT NULL
    )
    """,
    "INSERT OR IGNORE INTO rabbitkit_outbox_stats (one, entries, bytes, failed) VALUES (1, 0, 0, 0)",
    """
    CREATE TRIGGER IF NOT EXISTS rabbitkit_outbox_count_insert AFTER INSERT ON rabbitkit_outbox
    BEGIN
        UPDATE rabbitkit_outbox_stats
        SET entries = entries + 1, bytes = bytes + NEW.size, failed = failed + (NEW.state = 'failed')
        WHERE one = 1;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS rabbitkit_outbox_count_delete AFTER DELETE ON rabbitkit_outbox
    BEGIN
        UPDATE rabbitkit_outbox_stats
        SET entries = entries - 1, bytes = bytes - OLD.size, failed = failed - (OLD.state = 'failed')
        WHERE one = 1;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS rabbitkit_outbox_count_state AFTER UPDATE OF state ON rabbitkit_outbox
    WHEN NEW.state <> OLD.state
    BEGIN
        UPDATE rabbitkit_outbox_stats
        SET failed = failed + (NEW.state = 'failed') - (OLD.state = 'failed')
        WHERE one = 1;
    END
    """,
    "CREATE TABLE IF NOT EXISTS rabbitkit_outbox_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "INSERT OR IGNORE INTO rabbitkit_outbox_meta (key, value) VALUES ('schema_version', '1')",
)

_COLUMNS = (
    "id, state, enqueued_at, next_attempt_at, attempts, refusals, last_status, last_reason, "
    "last_error, failed_at, properties, body"
)
_INSERT = "INSERT INTO rabbitkit_outbox (message_id, enqueued_at, size, properties, body) VALUES (?, ?, ?, ?, ?)"


# ── envelope codec ───────────────────────────────────────────────────────
#
# Every header value is stored as a tagged pair so it reads back as the
# same Python type (bytes stay bytes, datetime stays datetime, Decimal stays
# Decimal); a plain JSON dump would turn them into strings or fail.


def _encode_value(value: Any, path: str) -> list[Any]:
    if value is None:
        return ["n"]
    if isinstance(value, bool):
        return ["b", value]
    if isinstance(value, int):
        return ["i", value]
    if isinstance(value, float):
        return ["f", value]
    if isinstance(value, str):
        return ["s", value]
    if isinstance(value, bytes | bytearray | memoryview):
        return ["y", base64.b64encode(bytes(value)).decode("ascii")]
    if isinstance(value, datetime):
        return ["t", value.isoformat()]
    if isinstance(value, Decimal):
        return ["d", str(value)]
    if isinstance(value, list | tuple):
        return ["l", [_encode_value(item, f"{path}[{index}]") for index, item in enumerate(value)]]
    if isinstance(value, dict):
        return ["m", _encode_map(value, path)]
    raise TypeError(
        f"The outbox cannot store header {path!r} of type {type(value).__name__}: AMQP header values are "
        "str, int, float, bool, bytes, datetime, Decimal, None, or lists/dicts of these"
    )


def _encode_map(mapping: dict[Any, Any], path: str) -> dict[str, list[Any]]:
    encoded: dict[str, list[Any]] = {}
    for key, item in mapping.items():
        if not isinstance(key, str):
            raise TypeError(f"The outbox cannot store header {path!r}: key {key!r} is not a str")
        encoded[key] = _encode_value(item, f"{path}.{key}" if path else key)
    return encoded


def _decode_value(tagged: list[Any]) -> Any:
    tag = tagged[0]
    if tag == "n":
        return None
    payload = tagged[1]
    if tag in ("b", "i", "f", "s"):
        return payload
    if tag == "y":
        return base64.b64decode(payload)
    if tag == "t":
        return datetime.fromisoformat(payload)
    if tag == "d":
        return Decimal(payload)
    if tag == "l":
        return [_decode_value(item) for item in payload]
    if tag == "m":
        return {key: _decode_value(item) for key, item in payload.items()}
    raise OutboxError(f"Unknown outbox value tag {tag!r}")


def _encode_envelope(envelope: MessageEnvelope) -> str:
    return json.dumps(
        {
            "v": _SCHEMA_VERSION,
            "routing_key": envelope.routing_key,
            "exchange": envelope.exchange,
            "headers": _encode_map(envelope.headers, ""),
            "message_id": envelope.message_id,
            "correlation_id": envelope.correlation_id,
            "reply_to": envelope.reply_to,
            "timestamp": None if envelope.timestamp is None else envelope.timestamp.isoformat(),
            "content_type": envelope.content_type,
            "content_encoding": envelope.content_encoding,
            "expiration": envelope.expiration,
            "priority": envelope.priority,
            "mandatory": envelope.mandatory,
            "delivery_mode": envelope.delivery_mode,
            "type": envelope.type,
            "user_id": envelope.user_id,
            "app_id": envelope.app_id,
        },
        separators=(",", ":"),
    )


def _decode_envelope(properties: str, body: bytes) -> MessageEnvelope:
    data = json.loads(properties)
    if data.get("v") != _SCHEMA_VERSION:
        raise OutboxError(f"Unsupported outbox entry version {data.get('v')!r}")
    timestamp = data["timestamp"]
    return MessageEnvelope(
        routing_key=data["routing_key"],
        body=bytes(body),
        exchange=data["exchange"],
        headers={key: _decode_value(item) for key, item in data["headers"].items()},
        message_id=data["message_id"],
        correlation_id=data["correlation_id"],
        reply_to=data["reply_to"],
        timestamp=None if timestamp is None else datetime.fromisoformat(timestamp),
        content_type=data["content_type"],
        content_encoding=data["content_encoding"],
        expiration=data["expiration"],
        priority=data["priority"],
        mandatory=data["mandatory"],
        delivery_mode=data["delivery_mode"],
        type=data["type"],
        user_id=data["user_id"],
        app_id=data["app_id"],
    )


# ── public value types ───────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """One stored message.

    Attributes:
        id: Store sequence number (enqueue order; never reused).
        envelope: The message exactly as it will be published.
        state: ``PENDING`` or ``FAILED``.
        enqueued_at: Wall-clock time of the enqueue (epoch seconds).
        next_attempt_at: Earliest wall-clock time of the next attempt.
        attempts: Publishes tried so far.
        refusals: Refused outcomes counted against ``max_refusals``.
        last_status: ``PublishStatus`` value of the last attempt (or ``error``
            for an exception).
        last_reason: Bounded reason of the last attempt.
        last_error: Short description of the last failure.
        failed_at: When it moved to the failed state.
    """

    id: int
    envelope: MessageEnvelope
    state: OutboxEntryState
    enqueued_at: float
    next_attempt_at: float
    attempts: int
    refusals: int
    last_status: str | None
    last_reason: str | None
    last_error: str | None
    failed_at: float | None


@dataclass(frozen=True, slots=True)
class OutboxStats:
    """Counts kept by the store (exact; maintained in each write's transaction)."""

    pending: int
    failed: int
    bytes: int

    @property
    def entries(self) -> int:
        """Pending plus failed: what ``OutboxConfig.max_entries`` bounds."""
        return self.pending + self.failed


@dataclass(frozen=True, slots=True)
class OutboxPassReport:
    """What one relay pass did.

    Attributes:
        attempted: Entries published in this pass.
        confirmed: Confirmed by the broker and deleted.
        retrying: Not confirmed; still pending for a later attempt.
        failed: Moved to the failed state.
        transient: An outcome said the broker is unreachable (``NOT_SENT``,
            ``UNKNOWN``, ``TIMEOUT``): the relay backs off before the next pass.
    """

    attempted: int = 0
    confirmed: int = 0
    retrying: int = 0
    failed: int = 0
    transient: bool = False


def outbox_backoff(config: OutboxConfig, attempt: int, rng: Callable[[], float] = random.random) -> float:
    """Capped exponential delay with jitter for the *attempt*-th consecutive failure."""
    delay = _capped_exponential(config.backoff_initial, config.backoff_max, attempt)
    if config.jitter > 0:
        delay *= 1 + config.jitter * (2 * rng() - 1)
    return max(0.0, delay)


# ── outcome classification ───────────────────────────────────────────────

_DONE = "done"
_TRANSIENT = "transient"
_REFUSED = "refused"
_FATAL = "fatal"

_TRANSIENT_STATUSES = frozenset({PublishStatus.NOT_SENT, PublishStatus.UNKNOWN, PublishStatus.TIMEOUT})
_REFUSED_STATUSES = frozenset({PublishStatus.RETURNED, PublishStatus.NACKED, PublishStatus.ERROR})


@dataclass(frozen=True, slots=True)
class _Verdict:
    kind: str
    status: str
    reason: str
    error: str | None


def _truncate(text: str) -> str:
    return text if len(text) <= _ERROR_TEXT_MAX else text[: _ERROR_TEXT_MAX - 3] + "..."


def _classify(result: PublishOutcome | BaseException) -> _Verdict:
    error: str | None
    if isinstance(result, BaseException):
        error = _truncate(safe_exception_repr(result))
        if isinstance(result, BrokerNotStartedError):
            # Publishing is closed (broker not started or stopping): nothing
            # left the client, so this is the same as NOT_SENT.
            return _Verdict(_TRANSIENT, PublishStatus.NOT_SENT.value, PublishReason.PUBLISHER_CLOSED, error)
        if isinstance(result, MessageTooLargeError):
            return _Verdict(_FATAL, PublishStatus.ERROR.value, _REASON_EXCEPTION, error)
        return _Verdict(_REFUSED, PublishStatus.ERROR.value, _REASON_EXCEPTION, error)
    status = result.status
    reason = _REASON_NONE if result.reason is None else result.reason
    if reason not in PublishReason.ALL and reason != _REASON_NONE:
        reason = _REASON_OTHER
    if result.reply_code is not None:
        error = _truncate(f"{result.reply_code} {result.reply_text or ''}".strip())
    elif result.error is not None:
        error = _truncate(safe_exception_repr(result.error))
    else:
        error = None
    if status is PublishStatus.CONFIRMED:
        return _Verdict(_DONE, status.value, reason, None)
    if status in _TRANSIENT_STATUSES:
        return _Verdict(_TRANSIENT, status.value, reason, error)
    if status in _REFUSED_STATUSES:
        return _Verdict(_REFUSED, status.value, reason, error)
    # SENT: confirms are off, so the broker never acknowledged it. Retrying
    # cannot produce a confirm; keep it for an operator instead of deleting
    # a message nobody confirmed.
    return _Verdict(_FATAL, status.value, reason, error or "publisher confirms are off (status sent)")


def is_transient_result(result: PublishOutcome | BaseException) -> bool:
    """True when *result* means the broker is unreachable (``NOT_SENT``,
    ``UNKNOWN``, ``TIMEOUT``, ``BrokerNotStartedError``): the entry stays
    pending without using its refusal budget, and the relay backs off."""
    return _classify(result).kind == _TRANSIENT


# ── metrics ──────────────────────────────────────────────────────────────


class _OutboxMetrics:
    """Emits the ``outbox_*`` metrics (see ``MetricsConfig``); no-op without a collector."""

    def __init__(self, collector: MetricsCollector | None, config: MetricsConfig | None) -> None:
        self._collector = collector
        self._cfg = config or MetricsConfig()
        self._set_gauge: Callable[[str, dict[str, str], float], None] | None = getattr(collector, "set_gauge", None)

    def inc(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        if self._collector is not None and value:
            self._collector.inc_counter(name, labels, value)

    def enqueued(self, count: int) -> None:
        self.inc(self._cfg.outbox_enqueued_total, {}, float(count))

    def full(self, limit: str) -> None:
        self.inc(self._cfg.outbox_full_total, {"reason": limit})

    def relayed(self, count: int) -> None:
        self.inc(self._cfg.outbox_relayed_total, {}, float(count))

    def retry(self, verdict: _Verdict) -> None:
        self.inc(self._cfg.outbox_publish_retries_total, {"status": verdict.status, "reason": verdict.reason})

    def failed(self, verdict: _Verdict) -> None:
        self.inc(self._cfg.outbox_failed_total, {"status": verdict.status, "reason": verdict.reason})

    def gauges(self, stats: OutboxStats) -> None:
        if self._set_gauge is None:
            return
        self._set_gauge(self._cfg.outbox_pending, {}, float(stats.pending))
        self._set_gauge(self._cfg.outbox_failed_entries, {}, float(stats.failed))
        self._set_gauge(self._cfg.outbox_bytes, {}, float(stats.bytes))


# ── the store ────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class _Row:
    message_id: str
    size: int
    properties: str
    body: bytes


def _rollback(conn: sqlite3.Connection) -> None:
    # A failed COMMIT may already have rolled back; a second ROLLBACK would
    # raise and hide the original error.
    if conn.in_transaction:
        conn.execute("ROLLBACK")


def _log_fields(entry: OutboxEntry, verdict: _Verdict) -> dict[str, Any]:
    return {
        "outbox_id": entry.id,
        "message_id": entry.envelope.message_id,
        "publish_status": verdict.status,
        "publish_reason": verdict.reason,
    }


def _same_file(a: str, b: str) -> bool:
    return os.path.realpath(a) == os.path.realpath(b)


class OutboxStore:
    """Durable, bounded store of outgoing messages (one SQLite file).

    Args:
        path: The SQLite file. Created if missing; may be shared with
            application tables (the store's tables are prefixed
            ``rabbitkit_outbox``). Must be on local disk.
        config: Bounds, backoff and durability settings.
        collector: Optional metrics collector (``inc_counter`` /
            ``set_gauge``) for the ``outbox_*`` metrics.
        metrics_config: Metric names (default ``MetricsConfig()``).
        clock: Wall-clock source (epoch seconds); persisted backoff times
            use it, so they survive a restart.
        rng: Jitter source in ``[0, 1)``.

    The file is opened lazily on first use (or by :meth:`open`).
    :meth:`close` releases the connection; a later call reopens it.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        config: OutboxConfig | None = None,
        *,
        collector: MetricsCollector | None = None,
        metrics_config: MetricsConfig | None = None,
        clock: Callable[[], float] = time.time,
        rng: Callable[[], float] = random.random,
    ) -> None:
        resolved = os.fspath(path)
        if not resolved or resolved == ":memory:" or resolved.startswith("file:"):
            raise ValueError(f"The outbox needs a plain file path (an in-memory database is not durable), got {path!r}")
        self._path = resolved
        self._config = config or OutboxConfig()
        self._metrics = _OutboxMetrics(collector, metrics_config)
        self._clock = clock
        self._rng = rng
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    @property
    def path(self) -> str:
        return self._path

    @property
    def config(self) -> OutboxConfig:
        return self._config

    # ── connections ───────────────────────────────────────────────────

    def _configure(self, conn: sqlite3.Connection) -> None:
        mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(mode).lower() != "wal":
            raise OutboxError(f"Could not enable WAL journaling on {self._path!r} (got {mode!r})")
        conn.execute("PRAGMA synchronous=FULL")
        if self._config.full_fsync:
            conn.execute("PRAGMA fullfsync=ON")
            conn.execute("PRAGMA checkpoint_fullfsync=ON")

    def _open_connection(self, *, own: bool) -> sqlite3.Connection:
        # The store's own connection manages its transactions explicitly
        # (BEGIN IMMEDIATE) and is shared across threads under self._lock; an
        # application connection keeps Python's defaults.
        if own:
            conn = sqlite3.connect(
                self._path, timeout=self._config.busy_timeout, isolation_level=None, check_same_thread=False
            )
        else:
            conn = sqlite3.connect(self._path, timeout=self._config.busy_timeout)
        try:
            self._configure(conn)
        except BaseException:
            conn.close()
            raise
        return conn

    def _db(self) -> sqlite3.Connection:
        """The store's own connection (caller holds ``self._lock``)."""
        if self._conn is None:
            conn = self._open_connection(own=True)
            try:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA:
                        conn.execute(statement)
                    version = conn.execute(
                        "SELECT value FROM rabbitkit_outbox_meta WHERE key = 'schema_version'"
                    ).fetchone()[0]
                    if version != str(_SCHEMA_VERSION):
                        raise OutboxError(
                            f"{self._path!r} holds outbox schema version {version}; this rabbitkit "
                            f"understands version {_SCHEMA_VERSION}"
                        )
                    conn.execute("COMMIT")
                except BaseException:
                    _rollback(conn)
                    raise
            except BaseException:
                conn.close()
                raise
            self._conn = conn
        return self._conn

    def open(self) -> None:
        """Open the file and create the tables if needed (idempotent)."""
        with self._lock:
            self._db()

    def close(self) -> None:
        """Close the store's connection (a later call reopens it)."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def connect(self) -> sqlite3.Connection:
        """A new connection to the same file, configured for transactional
        enqueue (WAL, ``synchronous=FULL``, the store's busy timeout).

        Use it for the business write and :meth:`enqueue_in` in one
        transaction (``with conn: ...``). It uses Python's default
        transaction handling and is bound to the calling thread; close it
        when done.
        """
        self.open()
        return self._open_connection(own=False)

    # ── enqueue ───────────────────────────────────────────────────────

    def _row(self, envelope: MessageEnvelope) -> _Row:
        if not isinstance(envelope, MessageEnvelope):
            raise TypeError(f"enqueue() takes a MessageEnvelope, got {type(envelope).__name__}")
        if not isinstance(envelope.body, bytes | bytearray):
            raise TypeError(f"MessageEnvelope.body must be bytes, got {type(envelope.body).__name__}")
        if not envelope.message_id:
            # The id must be fixed now: every republish of this entry carries
            # it, which is what lets a consumer drop the duplicates.
            envelope = replace(envelope, message_id=str(uuid.uuid4()))
        properties = _encode_envelope(envelope)
        body = bytes(envelope.body)
        return _Row(envelope.message_id, len(body) + len(properties.encode("utf-8")), properties, body)

    def _totals(self, conn: sqlite3.Connection) -> tuple[int, int, int]:
        row = conn.execute("SELECT entries, bytes, failed FROM rabbitkit_outbox_stats WHERE one = 1").fetchone()
        return int(row[0]), int(row[1]), int(row[2])

    def _over_limit(self, entries: int, size: int) -> str | None:
        if entries > self._config.max_entries:
            return "max_entries"
        if size > self._config.max_bytes:
            return "max_bytes"
        return None

    def _full(self, limit: str, entries: int, size: int) -> OutboxFullError:
        self._metrics.full(limit)
        logger.warning(
            "Outbox full (%s): %d entries, %d bytes; enqueue refused",
            limit,
            entries,
            size,
            extra={"outbox_limit": limit},
        )
        return OutboxFullError(
            f"Outbox {self._path!r} is full ({limit}: max_entries={self._config.max_entries}, "
            f"max_bytes={self._config.max_bytes}; holding {entries} entries, {size} bytes). "
            "Nothing was written.",
            limit=limit,
        )

    def enqueue(self, envelope: MessageEnvelope) -> int:
        """Store *envelope* durably and return its entry id.

        Returns after the transaction commits. Raises ``OutboxFullError``
        (nothing written) when the store is at a bound, ``TypeError`` for a
        header value AMQP cannot carry.
        """
        return self.enqueue_many([envelope])[0]

    def enqueue_many(self, envelopes: Iterable[MessageEnvelope]) -> list[int]:
        """Store several envelopes in ONE transaction (one ``fsync``).

        All or nothing: if the batch does not fit, ``OutboxFullError`` and
        nothing is written.
        """
        rows = [self._row(envelope) for envelope in envelopes]
        if not rows:
            return []
        now = self._clock()
        with self._lock:
            conn = self._db()
            conn.execute("BEGIN IMMEDIATE")
            try:
                entries, size, _failed = self._totals(conn)
                entries += len(rows)
                size += sum(row.size for row in rows)
                limit = self._over_limit(entries, size)
                if limit is not None:
                    raise self._full(limit, entries - len(rows), size - sum(row.size for row in rows))
                ids = [
                    int(conn.execute(_INSERT, (row.message_id, now, row.size, row.properties, row.body)).lastrowid or 0)
                    for row in rows
                ]
                conn.execute("COMMIT")
            except BaseException:
                _rollback(conn)
                raise
            stats = self._stats_locked(conn)
        self._metrics.enqueued(len(ids))
        self._metrics.gauges(stats)
        return ids

    def enqueue_in(self, conn: sqlite3.Connection, envelope: MessageEnvelope) -> int:
        """Write *envelope* through the application's connection, inside its
        transaction: the entry commits or rolls back with the business write.

        *conn* must be a connection to this store's file with
        ``synchronous`` at ``FULL`` or ``EXTRA`` (use :meth:`connect`). The
        entry is durable when the caller commits, and the relay sees it at
        its next poll (call the outbox's ``notify()`` after the commit to
        relay it at once). This method does not commit.

        The bounds are checked after the insert, in the caller's transaction:
        over a bound, the row is deleted again and ``OutboxFullError`` is
        raised; the caller's other writes are left to the caller.
        """
        row = self._row(envelope)
        self.open()
        level = int(conn.execute("PRAGMA synchronous").fetchone()[0])
        if level < 2:
            raise OutboxError(
                "enqueue_in() needs a connection with PRAGMA synchronous=FULL (got "
                f"{level}); open it with OutboxStore.connect()"
            )
        main = next((r[2] for r in conn.execute("PRAGMA database_list").fetchall() if r[1] == "main"), "")
        if not main or not _same_file(main, self._path):
            raise OutboxError(f"enqueue_in() needs a connection to {self._path!r}, got {main!r}")
        cursor = conn.execute(_INSERT, (row.message_id, self._clock(), row.size, row.properties, row.body))
        entry_id = int(cursor.lastrowid or 0)
        entries, size, _failed = self._totals(conn)
        limit = self._over_limit(entries, size)
        if limit is not None:
            conn.execute("DELETE FROM rabbitkit_outbox WHERE id = ?", (entry_id,))
            raise self._full(limit, entries - 1, size - row.size)
        self._metrics.enqueued(1)
        return entry_id

    # ── relay ─────────────────────────────────────────────────────────

    @staticmethod
    def _entry(row: tuple[Any, ...]) -> OutboxEntry:
        return OutboxEntry(
            id=int(row[0]),
            envelope=_decode_envelope(row[10], row[11]),
            state=OutboxEntryState(row[1]),
            enqueued_at=float(row[2]),
            next_attempt_at=float(row[3]),
            attempts=int(row[4]),
            refusals=int(row[5]),
            last_status=row[6],
            last_reason=row[7],
            last_error=row[8],
            failed_at=None if row[9] is None else float(row[9]),
        )

    def due(self, limit: int) -> list[OutboxEntry]:
        """Up to *limit* pending entries whose backoff has passed, oldest first."""
        with self._lock:
            rows = (
                self._db()
                .execute(
                    f"SELECT {_COLUMNS} FROM rabbitkit_outbox WHERE state = 'pending' AND next_attempt_at <= ? "  # noqa: S608 -- constant column list
                    "ORDER BY id LIMIT ?",
                    (self._clock(), limit),
                )
                .fetchall()
            )
        return [self._entry(row) for row in rows]

    def settle(self, results: Sequence[tuple[OutboxEntry, PublishOutcome | BaseException]]) -> OutboxPassReport:
        """Record the outcome of each publish in ONE transaction.

        ``CONFIRMED``: deleted. ``NOT_SENT`` / ``UNKNOWN`` / ``TIMEOUT`` (or
        ``BrokerNotStartedError``): stays pending, due again at once (the
        relay backs off as a whole). ``RETURNED`` / ``NACKED`` / ``ERROR`` /
        another exception: stays pending with its own backoff until
        ``max_refusals``, then failed. ``SENT`` or ``MessageTooLargeError``:
        failed at once.
        """
        now = self._clock()
        done: list[tuple[int]] = []
        updates: list[tuple[Any, ...]] = []
        retried: list[tuple[OutboxEntry, _Verdict, float | None]] = []
        failed: list[tuple[OutboxEntry, _Verdict]] = []
        transient: _Verdict | None = None
        for entry, result in results:
            verdict = _classify(result)
            if verdict.kind == _DONE:
                done.append((entry.id,))
                continue
            refusals = entry.refusals
            retry_in: float | None = None
            state = OutboxEntryState.PENDING
            if verdict.kind == _TRANSIENT:
                transient = verdict
            elif verdict.kind == _REFUSED:
                refusals += 1
                if refusals >= self._config.max_refusals:
                    state = OutboxEntryState.FAILED
                else:
                    retry_in = outbox_backoff(self._config, refusals, self._rng)
            else:
                state = OutboxEntryState.FAILED
            if state is OutboxEntryState.FAILED:
                failed.append((entry, verdict))
            else:
                retried.append((entry, verdict, retry_in))
            updates.append(
                (
                    state.value,
                    refusals,
                    0.0 if retry_in is None else now + retry_in,
                    verdict.status,
                    verdict.reason,
                    verdict.error,
                    now if state is OutboxEntryState.FAILED else None,
                    entry.id,
                )
            )
        with self._lock:
            conn = self._db()
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany("DELETE FROM rabbitkit_outbox WHERE id = ? AND state = 'pending'", done)
                conn.executemany(
                    "UPDATE rabbitkit_outbox SET state = ?, refusals = ?, next_attempt_at = ?, "
                    "attempts = attempts + 1, last_status = ?, last_reason = ?, last_error = ?, failed_at = ? "
                    "WHERE id = ? AND state = 'pending'",
                    updates,
                )
                conn.execute("COMMIT")
            except BaseException:
                _rollback(conn)
                raise
            stats = self._stats_locked(conn)
        self._metrics.relayed(len(done))
        for entry, verdict, retry_in in retried:
            self._metrics.retry(verdict)
            if retry_in is not None:
                logger.warning(
                    "Outbox entry %d (message_id=%s) refused: %s/%s (%s); retry %d/%d in %.1fs",
                    entry.id,
                    entry.envelope.message_id,
                    verdict.status,
                    verdict.reason,
                    verdict.error,
                    entry.refusals + 1,
                    self._config.max_refusals,
                    retry_in,
                    extra=_log_fields(entry, verdict),
                )
        for entry, verdict in failed:
            self._metrics.failed(verdict)
            logger.error(
                "Outbox entry %d (message_id=%s) moved to failed after %s/%s (%s); it stays in %s until "
                "replayed or purged",
                entry.id,
                entry.envelope.message_id,
                verdict.status,
                verdict.reason,
                verdict.error,
                self._path,
                extra=_log_fields(entry, verdict),
            )
        if transient is not None:
            logger.warning(
                "Outbox relay: broker unreachable (%s/%s, %s); entries stay pending",
                transient.status,
                transient.reason,
                transient.error,
                extra={"publish_status": transient.status, "publish_reason": transient.reason},
            )
        self._metrics.gauges(stats)
        return OutboxPassReport(
            attempted=len(results),
            confirmed=len(done),
            retrying=len(retried),
            failed=len(failed),
            transient=transient is not None,
        )

    # ── inspection and operator actions ───────────────────────────────

    def _stats_locked(self, conn: sqlite3.Connection) -> OutboxStats:
        entries, size, failed = self._totals(conn)
        return OutboxStats(pending=entries - failed, failed=failed, bytes=size)

    def stats(self) -> OutboxStats:
        """Pending and failed counts and stored bytes (also refreshes the gauges)."""
        with self._lock:
            stats = self._stats_locked(self._db())
        self._metrics.gauges(stats)
        return stats

    def list_entries(
        self,
        state: OutboxEntryState = OutboxEntryState.FAILED,
        *,
        limit: int = 100,
        after_id: int = 0,
    ) -> list[OutboxEntry]:
        """Entries in *state* with ``id > after_id``, oldest first (page with ``after_id``)."""
        state = OutboxEntryState(state)
        with self._lock:
            rows = (
                self._db()
                .execute(
                    f"SELECT {_COLUMNS} FROM rabbitkit_outbox WHERE state = ? AND id > ? ORDER BY id LIMIT ?",  # noqa: S608 -- constant column list
                    (state.value, after_id, limit),
                )
                .fetchall()
            )
        return [self._entry(row) for row in rows]

    def _failed_action(self, sql: str, ids: Iterable[int] | None) -> int:
        with self._lock:
            conn = self._db()
            conn.execute("BEGIN IMMEDIATE")
            try:
                if ids is None:
                    count = conn.execute(sql).rowcount
                else:
                    count = sum(conn.execute(f"{sql} AND id = ?", (int(i),)).rowcount for i in ids)
                conn.execute("COMMIT")
            except BaseException:
                _rollback(conn)
                raise
            stats = self._stats_locked(conn)
        self._metrics.gauges(stats)
        return int(count)

    def replay_failed(self, ids: Iterable[int] | None = None) -> int:
        """Move failed entries (all, or *ids*) back to pending with a fresh
        refusal budget. They keep their id, position and ``message_id``.
        Returns how many moved."""
        count = self._failed_action(
            "UPDATE rabbitkit_outbox SET state = 'pending', refusals = 0, next_attempt_at = 0, failed_at = NULL "
            "WHERE state = 'failed'",
            ids,
        )
        logger.warning("Outbox: %d failed entries moved back to pending in %s", count, self._path)
        return count

    def purge_failed(self, ids: Iterable[int] | None = None) -> int:
        """Delete failed entries (all, or *ids*). This is the only way the
        store discards a message; it is logged. Returns how many were deleted."""
        count = self._failed_action("DELETE FROM rabbitkit_outbox WHERE state = 'failed'", ids)
        logger.warning("Outbox: %d failed entries purged from %s", count, self._path)
        return count
