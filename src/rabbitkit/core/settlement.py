"""Selected / bulk settlement — planning, reporting, and safe coalescing.

Transport-free (hard invariant 1). Two layers:

1. **Selected settlement** (``ack_many`` / ``nack_many`` on the brokers):
   :func:`plan_selected_settlement` validates a caller-supplied list of
   :class:`~rabbitkit.core.message.RabbitMessage` objects — duplicates,
   already-settled, no-ack deliveries, stale channels — and the broker then
   issues ONE individual ``multiple=False`` settlement per approved message.
   Bulk here means "one API call", never "one cumulative frame".

2. **Safe coalescing** (:class:`SettlementCoordinator`): a channel-wide
   ledger that knows EVERY outstanding delivery on one channel generation
   and only emits a cumulative ``ack(tag, multiple=True)`` when every still-
   outstanding lower tag is approved for success. Anything else is settled
   individually. This is the only place a ``multiple=True`` ack may originate.

Safety invariants enforced here (see the plan, §3):

* Never coalesce across an unfinished or retry-pending delivery (3).
* Delivery handles belong to one channel generation; ``invalidate()`` on
  reconnect drops the whole ledger — old tags are never replayed (4).
* Consumer settlement has no broker confirmation; the report vocabulary
  says ``DISPATCHED``, never "confirmed" (8).
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import DeliveryState, SettlementAction, SettlementItemStatus

# ── Selected settlement (ack_many / nack_many) ─────────────────────────────


#: Bounded reason codes for :class:`SettlementItem.reason` (metric-label safe).
SETTLE_REASON_OK = "ok"
SETTLE_REASON_ALREADY = "already_settled"
SETTLE_REASON_DUPLICATE = "duplicate_handle"
SETTLE_REASON_NOT_MESSAGE = "not_a_rabbit_message"
SETTLE_REASON_NO_SETTLEMENT_FN = "no_settlement_fn"
SETTLE_REASON_WRONG_RUNTIME = "wrong_runtime"
SETTLE_REASON_STALE_CHANNEL = "stale_channel"
SETTLE_REASON_VALIDATION_ABORT = "validation_failed_fail_fast"
SETTLE_REASON_TRANSPORT_ERROR = "transport_error"

_SETTLE_REASONS: frozenset[str] = frozenset(
    {
        SETTLE_REASON_OK,
        SETTLE_REASON_ALREADY,
        SETTLE_REASON_DUPLICATE,
        SETTLE_REASON_NOT_MESSAGE,
        SETTLE_REASON_NO_SETTLEMENT_FN,
        SETTLE_REASON_WRONG_RUNTIME,
        SETTLE_REASON_STALE_CHANNEL,
        SETTLE_REASON_VALIDATION_ABORT,
        SETTLE_REASON_TRANSPORT_ERROR,
    }
)


@dataclass(frozen=True, slots=True)
class SettlementItem:
    index: int
    status: SettlementItemStatus
    reason: str
    delivery_tag: int | None = None
    message_id: str | None = None
    error: BaseException | None = None

    def __post_init__(self) -> None:
        if self.reason not in _SETTLE_REASONS:
            raise ValueError(f"SettlementItem.reason must be a bounded reason code, got {self.reason!r}")

    @property
    def ok(self) -> bool:
        return self.status is SettlementItemStatus.DISPATCHED


class SettlementReportError(Exception):
    """Raised by :meth:`SettlementReport.raise_for_status` when any item was
    not dispatched (already-settled items are tolerated — they are no-ops)."""

    def __init__(self, report: SettlementReport) -> None:
        self.report = report
        counts = ", ".join(f"{s.value}={n}" for s, n in sorted(report.counts.items(), key=lambda kv: kv[0].value))
        super().__init__(f"{report.action.value}_many incomplete: {counts}")


@dataclass(frozen=True, slots=True)
class SettlementReport:
    """Input-ordered per-item report for one ``ack_many``/``nack_many`` call."""

    action: SettlementAction
    items: tuple[SettlementItem, ...]
    requeue: bool | None = None
    finished_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __len__(self) -> int:
        return len(self.items)

    @property
    def counts(self) -> dict[SettlementItemStatus, int]:
        return dict(Counter(it.status for it in self.items))

    def by_status(self, status: SettlementItemStatus) -> tuple[SettlementItem, ...]:
        return tuple(it for it in self.items if it.status is status)

    @property
    def dispatched(self) -> tuple[SettlementItem, ...]:
        return self.by_status(SettlementItemStatus.DISPATCHED)

    @property
    def problems(self) -> tuple[SettlementItem, ...]:
        """Everything except DISPATCHED and ALREADY_SETTLED."""
        return tuple(
            it
            for it in self.items
            if it.status not in (SettlementItemStatus.DISPATCHED, SettlementItemStatus.ALREADY_SETTLED)
        )

    @property
    def all_dispatched(self) -> bool:
        return not self.problems

    def raise_for_status(self) -> SettlementReport:
        if not self.all_dispatched:
            raise SettlementReportError(self)
        return self


@dataclass(frozen=True, slots=True)
class SettlementPlan:
    """Result of validating a selected-settlement input.

    ``approved`` holds ``(index, message)`` pairs the caller may settle;
    ``rejected`` holds finished items for everything else.
    """

    approved: tuple[tuple[int, RabbitMessage], ...]
    rejected: tuple[SettlementItem, ...]

    @property
    def has_problems(self) -> bool:
        return any(it.status is not SettlementItemStatus.ALREADY_SETTLED for it in self.rejected)


def plan_selected_settlement(
    messages: Iterable[Any],
    *,
    runtime: str,
    fail_fast: bool = True,
) -> SettlementPlan:
    """Validate a selected-settlement input before any I/O.

    Args:
        messages: Caller-supplied deliveries (must be ``RabbitMessage``).
        runtime: ``"sync"`` or ``"async"`` — which settlement functions the
            broker will call. A message wired only for the other runtime is
            ``INVALID`` (``wrong_runtime``).
        fail_fast: When True and ANY item is invalid/stale/duplicate, nothing
            is approved: every otherwise-valid item is reported
            ``NOT_ATTEMPTED`` (``validation_failed_fail_fast``). Already-settled
            items are not problems (idempotent no-ops). When False, valid
            items are approved and problems are reported alongside.
    """
    if runtime not in ("sync", "async"):
        raise ValueError(f"runtime must be 'sync' or 'async', got {runtime!r}")
    approved: list[tuple[int, RabbitMessage]] = []
    rejected: list[SettlementItem] = []
    seen: set[int] = set()

    for index, raw in enumerate(messages):
        if not isinstance(raw, RabbitMessage):
            rejected.append(
                SettlementItem(
                    index=index,
                    status=SettlementItemStatus.INVALID,
                    reason=SETTLE_REASON_NOT_MESSAGE,
                    error=TypeError(f"expected RabbitMessage, got {type(raw).__name__}"),
                )
            )
            continue
        tag = raw.delivery_tag
        if id(raw) in seen:
            rejected.append(
                SettlementItem(
                    index=index,
                    status=SettlementItemStatus.DUPLICATE,
                    reason=SETTLE_REASON_DUPLICATE,
                    delivery_tag=tag,
                    message_id=raw.message_id,
                )
            )
            continue
        seen.add(id(raw))
        if raw.is_settled:
            rejected.append(
                SettlementItem(
                    index=index,
                    status=SettlementItemStatus.ALREADY_SETTLED,
                    reason=SETTLE_REASON_ALREADY,
                    delivery_tag=tag,
                    message_id=raw.message_id,
                )
            )
            continue
        has_sync = raw._ack_fn is not None
        has_async = raw._ack_async_fn is not None
        if not has_sync and not has_async:
            rejected.append(
                SettlementItem(
                    index=index,
                    status=SettlementItemStatus.INVALID,
                    reason=SETTLE_REASON_NO_SETTLEMENT_FN,
                    delivery_tag=tag,
                    message_id=raw.message_id,
                )
            )
            continue
        # Sync runtime needs a sync fn; async runtime accepts either
        # (RabbitMessage.ack_async falls back to the sync fn).
        if runtime == "sync" and not has_sync:
            rejected.append(
                SettlementItem(
                    index=index,
                    status=SettlementItemStatus.INVALID,
                    reason=SETTLE_REASON_WRONG_RUNTIME,
                    delivery_tag=tag,
                    message_id=raw.message_id,
                )
            )
            continue
        alive = raw._channel_alive
        if alive is not None:
            try:
                is_alive = bool(alive())
            except Exception:
                is_alive = False
            if not is_alive:
                rejected.append(
                    SettlementItem(
                        index=index,
                        status=SettlementItemStatus.STALE,
                        reason=SETTLE_REASON_STALE_CHANNEL,
                        delivery_tag=tag,
                        message_id=raw.message_id,
                    )
                )
                continue
        approved.append((index, raw))

    plan = SettlementPlan(approved=tuple(approved), rejected=tuple(rejected))
    if fail_fast and plan.has_problems and approved:
        aborted = tuple(
            SettlementItem(
                index=i,
                status=SettlementItemStatus.NOT_ATTEMPTED,
                reason=SETTLE_REASON_VALIDATION_ABORT,
                delivery_tag=m.delivery_tag,
                message_id=m.message_id,
            )
            for i, m in approved
        )
        return SettlementPlan(approved=(), rejected=tuple(sorted((*rejected, *aborted), key=lambda it: it.index)))
    return plan


def build_report(
    action: SettlementAction,
    items: Iterable[SettlementItem],
    *,
    requeue: bool | None = None,
) -> SettlementReport:
    """Assemble an input-ordered report from unordered items."""
    return SettlementReport(action=action, items=tuple(sorted(items, key=lambda it: it.index)), requeue=requeue)


# ── Coalescing coordinator ─────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SettlementCommand:
    """One wire command the transport adapter must issue, in order.

    ``covers`` lists every delivery tag this command settles (for a
    cumulative ack that is the whole approved prefix) so adapters and tests
    can account for each tag exactly once.
    """

    kind: SettlementAction
    delivery_tag: int
    multiple: bool = False
    requeue: bool = False
    covers: tuple[int, ...] = ()
    generation: int = 0


@dataclass(frozen=True, slots=True)
class SettlementBatch:
    """Commands PREPARED for the wire but not yet settled in the ledger.

    This is the unit of the plan/emit/commit protocol. A batch is produced by
    :meth:`SettlementCoordinator.prepare`, which reserves the covered tags
    without consuming them, and is resolved one command at a time by
    :meth:`SettlementCoordinator.commit_command` (the frame reached the
    broker) or :meth:`SettlementCoordinator.fail_command` (it did not).

    Why this exists: a cumulative ``basic_ack(tag, multiple=True)`` settles
    every delivery still unacknowledged up to ``tag``. A plan that mixes a
    nack with a later cumulative ack is therefore only correct if the nack
    actually reached the broker first. Consuming the ledger at planning time
    made the coordinator believe a settlement happened that may never have
    left the process.
    """

    commands: tuple[SettlementCommand, ...]
    generation: int
    batch_id: int

    def __len__(self) -> int:
        return len(self.commands)

    def __bool__(self) -> bool:
        return bool(self.commands)

    @property
    def covered_tags(self) -> tuple[int, ...]:
        """Every delivery tag this batch would settle, ascending."""
        return tuple(sorted(t for c in self.commands for t in c.covers))


@dataclass(frozen=True, slots=True)
class EmissionReport:
    """Outcome of driving a :class:`SettlementBatch` onto the wire.

    ``emitted`` reached the broker and are settled. ``failed`` raised.
    ``not_attempted`` were deliberately NOT sent, because a command whose
    safety depends on an earlier frame must never follow a failure.
    """

    batch_id: int
    generation: int
    emitted: tuple[SettlementCommand, ...] = ()
    failed: SettlementCommand | None = None
    error: BaseException | None = None
    not_attempted: tuple[SettlementCommand, ...] = ()
    invalidated: bool = False
    unresolved_tags: tuple[int, ...] = ()

    @property
    def ok(self) -> bool:
        """True when every command in the batch reached the broker."""
        return self.failed is None and not self.not_attempted

    @property
    def settled_tags(self) -> int:
        return sum(len(c.covers) for c in self.emitted)

    @property
    def coalesced_tags(self) -> int:
        """Tags settled by a cumulative ack beyond the frame's own tag."""
        return sum(len(c.covers) - 1 for c in self.emitted if c.multiple and len(c.covers) > 1)


class CoordinatorError(RuntimeError):
    """Invalid use of :class:`SettlementCoordinator`. Base class for the
    specific errors below; catching it catches all of them."""


class UnknownDeliveryError(CoordinatorError):
    """A delivery tag that was never registered on this generation.

    Silently ignoring it would let a caller bug corrupt the frontier, so it
    is always loud. A tag from an OLDER generation (the channel was rebuilt
    under you) raises :class:`StaleGenerationError` instead.
    """


class StaleGenerationError(CoordinatorError):
    """A delivery tag from a previous channel generation.

    The channel it arrived on is gone; the broker will redeliver the message
    on the new one with a NEW tag. Emitting anything for the old tag would
    settle an unrelated message (invariants I2, I5).
    """


class ContradictorySettlementError(CoordinatorError):
    """A settlement decision that contradicts one already recorded.

    Repeating the SAME decision is an idempotent no-op; changing it (ack →
    nack, nack → ack, acking something that FAILED) is never silently
    accepted (invariant I4).
    """


class LedgerFullError(CoordinatorError):
    """``max_pending`` reached — the ledger refuses to grow further.

    Raised by :meth:`SettlementCoordinator.register`, so the caller applies
    backpressure (stop consuming / lower prefetch). The delivery stays
    unacked and the broker redelivers it. Bounds NEVER cause an unsafe ack
    (invariant I7).
    """


class SettlementCoordinator:
    """Channel-wide delivery ledger with provably safe ack coalescing.

    One coordinator per channel generation. EVERY delivery on that channel
    must be :meth:`register`-ed before its handler runs; if that ownership
    cannot be guaranteed (another consumer shares the channel and does not
    register), coalescing is unsafe — construct with ``coalesce=False``.

    Decision rule for a cumulative ack: let ``P`` be the longest prefix of
    the ledger (ascending tag order) in which every entry is ``SUCCESS``.
    If ``|P| >= 2`` emit ``ack(max(P), multiple=True)``; ``|P| == 1`` is a
    plain individual ack. Any ``SUCCESS`` entry AFTER the first non-success
    entry is acked individually (never buffered past ``max_hold``). NACK /
    REJECT intents are always individual and are emitted in tag order
    interleaved with acks, so a later prefix decision can never leap over
    an unsettled reject.

    Thread-safe: every method takes the internal lock. Command *emission*
    is the adapter's job and must happen on the transport owner (sync I/O
    thread / event loop).
    """

    #: Which state transitions are legal. Repeating the SAME state is an
    #: idempotent no-op (handled before this table); anything not listed is a
    #: contradiction. SUCCESS is reachable ONLY from OUTSTANDING/RETRY_PENDING
    #: — a delivery that FAILED or was CANCELLED can never be acked (I3).
    _TRANSITIONS: ClassVar[dict[DeliveryState, frozenset[DeliveryState]]] = {
        DeliveryState.OUTSTANDING: frozenset(
            {
                DeliveryState.SUCCESS,
                DeliveryState.NACK,
                DeliveryState.REJECT,
                DeliveryState.RETRY_PENDING,
                DeliveryState.FAILED,
                DeliveryState.CANCELLED,
            }
        ),
        DeliveryState.RETRY_PENDING: frozenset(
            {
                DeliveryState.SUCCESS,
                DeliveryState.NACK,
                DeliveryState.REJECT,
                DeliveryState.FAILED,
                DeliveryState.CANCELLED,
            }
        ),
        DeliveryState.FAILED: frozenset({DeliveryState.NACK, DeliveryState.REJECT}),
        DeliveryState.CANCELLED: frozenset({DeliveryState.NACK, DeliveryState.REJECT}),
        DeliveryState.SUCCESS: frozenset(),
        DeliveryState.NACK: frozenset(),
        DeliveryState.REJECT: frozenset(),
    }

    def __init__(
        self,
        *,
        generation: int = 0,
        coalesce: bool = True,
        max_hold: int = 0,
        max_pending: int = 0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._generation = generation
        self._coalesce = coalesce
        # max_hold: how many planning rounds a SUCCESS entry stranded behind
        # a blocking tag may wait for a cumulative ack before being acked
        # individually. 0 = never hold (always individual fallback).
        self._max_hold = max(0, max_hold)
        # max_pending: hard ceiling on ledger size. 0 = unbounded (default).
        # Exceeding it raises LedgerFullError from register() so the caller
        # applies backpressure — it NEVER relaxes the ack rules (I7).
        self._max_pending = max(0, max_pending)
        self._clock = clock
        self._lock = threading.Lock()
        self._ledger: dict[int, DeliveryState] = {}  # insertion == ascending tag order
        self._hold: dict[int, int] = {}
        self._requeue: dict[int, bool] = {}
        self._registered_at: dict[int, float] = {}
        self._last_registered = 0
        self._prev_high_water = 0  # highest tag of any dropped generation
        self._registered_count = 0
        self._settled_count = 0
        self._coalesced_count = 0
        self._frames_sent = 0
        self._invalidations = 0
        self._dropped_on_invalidate = 0
        # plan/emit/commit protocol: tags reserved by an in-flight batch.
        # They are NOT settled yet, so they stay in the ledger, but a second
        # prepare() must not re-plan them AND nothing above them may be
        # cumulatively acked while their wire outcome is unknown.
        self._reserved: dict[int, int] = {}  # tag -> batch_id
        self._next_batch_id = 1
        self._unresolved_count = 0
        self._frames_failed = 0
        self._frames_not_attempted = 0

    # ── inspection ────────────────────────────────────────────────────────

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def coalesce_enabled(self) -> bool:
        return self._coalesce

    @property
    def max_pending(self) -> int:
        return self._max_pending

    @property
    def outstanding(self) -> int:
        with self._lock:
            return sum(1 for s in self._ledger.values() if s is DeliveryState.OUTSTANDING)

    @property
    def pending(self) -> int:
        """Registered but not yet emitted (any state)."""
        with self._lock:
            return len(self._ledger)

    @property
    def ack_ready(self) -> int:
        """Deliveries approved for an ack but not yet emitted."""
        with self._lock:
            return sum(1 for s in self._ledger.values() if s.is_ack_safe)

    @property
    def frontier(self) -> int:
        """Highest tag a cumulative ack could cover right now (0 = none).

        This is the end of the leading all-SUCCESS run, NOT the highest
        completed tag — that difference is the whole point of the
        coordinator (invariant I1).
        """
        with self._lock:
            return self._frontier_locked()

    def _frontier_locked(self) -> int:
        frontier = 0
        for tag in sorted(self._ledger):
            if self._ledger[tag].is_ack_safe:
                frontier = tag
            else:
                break
        return frontier

    @property
    def gap_count(self) -> int:
        """Blocking deliveries sitting below an ack-ready one — i.e. how many
        stragglers are stranding otherwise-settleable work."""
        with self._lock:
            tags = sorted(self._ledger)
            highest_ready = max((t for t in tags if self._ledger[t].is_ack_safe), default=0)
            return sum(1 for t in tags if t < highest_ready and self._ledger[t].blocks_frontier)

    @property
    def oldest_pending_age(self) -> float:
        """Seconds since the oldest still-unsettled delivery was registered
        (0.0 when the ledger is empty). The signal for a stuck handler
        holding the frontier back."""
        with self._lock:
            if not self._registered_at:
                return 0.0
            return max(0.0, self._clock() - min(self._registered_at.values()))

    @property
    def stats(self) -> dict[str, float]:
        """Snapshot for metrics/debugging. ``coalescing_ratio`` is
        deliveries settled per AMQP frame sent — 1.0 means no coalescing."""
        with self._lock:
            return {
                "registered": self._registered_count,
                "pending": len(self._ledger),
                "outstanding": sum(1 for s in self._ledger.values() if s is DeliveryState.OUTSTANDING),
                "ack_ready": sum(1 for s in self._ledger.values() if s.is_ack_safe),
                "frontier": self._frontier_locked(),
                "settled": self._settled_count,
                "coalesced": self._coalesced_count,
                "frames_sent": self._frames_sent,
                "coalescing_ratio": (self._settled_count / self._frames_sent) if self._frames_sent else 0.0,
                "invalidations": self._invalidations,
                "dropped_on_invalidate": self._dropped_on_invalidate,
                "reserved": len(self._reserved),
                "unresolved": self._unresolved_count,
                "frames_failed": self._frames_failed,
                "frames_not_attempted": self._frames_not_attempted,
                "oldest_pending_age": (
                    max(0.0, self._clock() - min(self._registered_at.values())) if self._registered_at else 0.0
                ),
            }

    def state_of(self, delivery_tag: int) -> DeliveryState | None:
        with self._lock:
            return self._ledger.get(delivery_tag)

    # ── registration & intents ───────────────────────────────────────────

    def register(self, delivery_tag: int) -> None:
        """Register a delivery BEFORE its handler runs. Tags must be strictly
        increasing within a generation (AMQP guarantees this per channel), so
        a redelivered message — which always arrives with a NEW, higher tag —
        registers cleanly while the old tag can never be re-registered.

        Raises :class:`LedgerFullError` when ``max_pending`` is reached.
        """
        with self._lock:
            if delivery_tag <= 0:
                raise CoordinatorError(f"delivery_tag must be positive, got {delivery_tag}")
            if delivery_tag <= self._last_registered:
                raise CoordinatorError(
                    f"delivery_tag {delivery_tag} is not greater than last registered "
                    f"{self._last_registered} (stale or duplicate delivery on generation "
                    f"{self._generation})"
                )
            if self._max_pending and len(self._ledger) >= self._max_pending:
                raise LedgerFullError(
                    f"ledger holds {len(self._ledger)} unsettled deliveries (max_pending="
                    f"{self._max_pending}) on generation {self._generation}; apply backpressure "
                    "(stop consuming / lower prefetch) — the delivery stays unacked and the "
                    "broker will redeliver it."
                )
            self._ledger[delivery_tag] = DeliveryState.OUTSTANDING
            self._registered_at[delivery_tag] = self._clock()
            self._last_registered = delivery_tag
            self._registered_count += 1

    def _missing(self, delivery_tag: int) -> CoordinatorError:
        """Classify a tag that is not in the ledger (best effort — all three
        are CoordinatorError and none of them ever emits a frame)."""
        if delivery_tag <= self._last_registered:
            return ContradictorySettlementError(
                f"delivery_tag {delivery_tag} was already settled or released on generation {self._generation}"
            )
        if delivery_tag <= self._prev_high_water:
            return StaleGenerationError(
                f"delivery_tag {delivery_tag} belongs to a previous generation (current is "
                f"{self._generation}); that channel is gone and the broker will redeliver "
                "the message with a new tag"
            )
        return UnknownDeliveryError(
            f"delivery_tag {delivery_tag} was never registered on generation {self._generation}"
        )

    def _set_intent(self, delivery_tag: int, state: DeliveryState, requeue: bool | None = None) -> None:
        current = self._ledger.get(delivery_tag)
        if current is None:
            raise self._missing(delivery_tag)
        if current is state:
            # Idempotent repeat — but a nack/reject that flips requeue is a
            # different decision, not a repeat.
            if requeue is not None and self._requeue.get(delivery_tag) != requeue:
                raise ContradictorySettlementError(
                    f"delivery_tag {delivery_tag} is already {state.value} with "
                    f"requeue={self._requeue.get(delivery_tag)}; cannot change it to requeue={requeue}"
                )
            return
        if state not in self._TRANSITIONS[current]:
            raise ContradictorySettlementError(
                f"delivery_tag {delivery_tag} is {current.value}; refusing to change it to "
                f"{state.value} (legal next states: "
                f"{sorted(x.value for x in self._TRANSITIONS[current]) or 'none — terminal'})"
            )
        self._ledger[delivery_tag] = state
        if requeue is not None:
            self._requeue[delivery_tag] = requeue

    def mark_success(self, delivery_tag: int) -> None:
        """Handler succeeded: the delivery is ack-safe. Idempotent."""
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.SUCCESS)

    def mark_retry_pending(self, delivery_tag: int) -> None:
        """Handler failed and the retry/terminal path now owns this delivery.
        It is NOT settleable through the coordinator; it blocks any
        cumulative ack above it until :meth:`release` (settled elsewhere)."""
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.RETRY_PENDING)

    def mark_failed(self, delivery_tag: int) -> None:
        """Handler failed with no settlement decision: block the frontier and
        never emit anything. The delivery stays unacked, so the broker
        redelivers it when the channel closes. It can still be settled
        explicitly with :meth:`mark_nack` / :meth:`mark_reject`, but never
        acked."""
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.FAILED)

    def mark_cancelled(self, delivery_tag: int) -> None:
        """Handler was cancelled (shutdown, timeout): same treatment as
        :meth:`mark_failed` — blocks the frontier, never acked."""
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.CANCELLED)

    def mark_nack(self, delivery_tag: int, *, requeue: bool = True) -> None:
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.NACK, requeue=requeue)

    def mark_reject(self, delivery_tag: int, *, requeue: bool = False) -> None:
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.REJECT, requeue=requeue)

    def release(self, delivery_tag: int) -> None:
        """Remove a delivery that was settled OUTSIDE the coordinator (e.g. by
        the retry middleware's own ack after a confirmed handoff, or a
        MANUAL handler). Unknown tags are ignored (idempotent)."""
        with self._lock:
            self._ledger.pop(delivery_tag, None)
            self._hold.pop(delivery_tag, None)
            self._requeue.pop(delivery_tag, None)
            self._registered_at.pop(delivery_tag, None)

    # ── planning ─────────────────────────────────────────────────────────

    def _plan_locked(self, *, force_individual: bool) -> tuple[list[SettlementCommand], set[int]]:
        """The planning walk. Caller must hold ``self._lock``.

        Returns the commands and the set of tags they cover. Nothing is
        removed from the ledger here — consuming is the caller's decision,
        which is what makes the plan/emit/commit protocol possible.

        Walks the ledger in ascending tag order and coalesces each contiguous
        run of ack-safe deliveries. A nack/reject in the middle is emitted
        individually and, because it settles that tag on the wire first, the
        run AFTER it can form a new cumulative range::

            101 SUCCESS  102 SUCCESS  103 NACK  104 SUCCESS  105 SUCCESS
            -> ack(102, multiple=True), nack(103), ack(105, multiple=True)

        The moment a BLOCKING delivery is reached (still outstanding, retry-
        pending, failed, cancelled, or RESERVED by an in-flight batch) no
        cumulative ack may reach past it again in this plan: everything above
        is acked individually, optionally after a bounded hold.

        Commands come back in ascending tag order and MUST be emitted in that
        order. A cumulative ack is only correct because the frames below it
        went out first -- see :func:`emit_batch`, which is the only supported
        way to put a plan on the wire.
        """
        commands: list[SettlementCommand] = []
        covered: set[int] = set()
        run: list[int] = []  # current contiguous ack-safe run
        blocked = False  # a blocking delivery has been passed

        def flush_run() -> None:
            if not run:
                return
            if self._coalesce and not force_individual and len(run) >= 2:
                commands.append(
                    SettlementCommand(
                        kind=SettlementAction.ACK,
                        delivery_tag=run[-1],
                        multiple=True,
                        covers=tuple(run),
                        generation=self._generation,
                    )
                )
            else:
                commands.extend(
                    SettlementCommand(
                        kind=SettlementAction.ACK,
                        delivery_tag=t,
                        multiple=False,
                        covers=(t,),
                        generation=self._generation,
                    )
                    for t in run
                )
            covered.update(run)
            run.clear()

        for tag in sorted(self._ledger):
            # A reserved tag is on the wire with an UNKNOWN outcome. It may
            # still be unacknowledged at the broker, so no cumulative ack may
            # sweep past it and it must not be planned twice.
            if tag in self._reserved:
                flush_run()
                blocked = True
                continue
            state = self._ledger[tag]
            if state.is_ack_safe:
                if not blocked:
                    run.append(tag)
                    continue
                # Stranded behind a blocker: hold briefly hoping the blocker
                # resolves, then fall back to an individual ack so one slow
                # handler cannot pin its siblings forever.
                if self._coalesce and not force_individual and self._hold.get(tag, 0) < self._max_hold:
                    self._hold[tag] = self._hold.get(tag, 0) + 1
                    continue
                commands.append(
                    SettlementCommand(
                        kind=SettlementAction.ACK,
                        delivery_tag=tag,
                        multiple=False,
                        covers=(tag,),
                        generation=self._generation,
                    )
                )
                covered.add(tag)
            elif state is DeliveryState.NACK or state is DeliveryState.REJECT:
                # Settles this tag on the wire before anything above it, so it
                # does NOT block a later cumulative range -- PROVIDED it
                # actually reaches the broker. emit_batch enforces that.
                flush_run()
                commands.append(
                    SettlementCommand(
                        kind=SettlementAction.NACK if state is DeliveryState.NACK else SettlementAction.REJECT,
                        delivery_tag=tag,
                        requeue=self._requeue.get(tag, state is DeliveryState.NACK),
                        covers=(tag,),
                        generation=self._generation,
                    )
                )
                covered.add(tag)
            else:  # OUTSTANDING / RETRY_PENDING / FAILED / CANCELLED
                flush_run()
                blocked = True
        flush_run()
        commands.sort(key=lambda c: c.delivery_tag)
        return commands, covered

    def _settle_tags_locked(self, tags: Iterable[int]) -> None:
        """Drop tags from the ledger for good. Caller holds ``self._lock``."""
        for tag in tags:
            self._ledger.pop(tag, None)
            self._hold.pop(tag, None)
            self._requeue.pop(tag, None)
            self._registered_at.pop(tag, None)
            self._reserved.pop(tag, None)

    def prepare(self, *, force_individual: bool = False) -> SettlementBatch:
        """Plan the commands that are safe to emit now, WITHOUT settling them.

        The covered tags are *reserved*: they stay in the ledger, a second
        ``prepare()`` will not re-plan them, and nothing above them may be
        cumulatively acked while their wire outcome is unknown. Resolve the
        batch with :func:`emit_batch`, or by hand via
        :meth:`commit_command` / :meth:`fail_command` / :meth:`release_commands`.

        Every command MUST be emitted in the order given, and a command must
        never be emitted after an earlier one failed.
        """
        with self._lock:
            if not self._ledger:
                return SettlementBatch(commands=(), generation=self._generation, batch_id=0)
            commands, covered = self._plan_locked(force_individual=force_individual)
            batch_id = self._next_batch_id
            self._next_batch_id += 1
            for tag in covered:
                self._reserved[tag] = batch_id
            return SettlementBatch(
                commands=tuple(commands),
                generation=self._generation,
                batch_id=batch_id,
            )

    def commit_command(self, batch: SettlementBatch, command: SettlementCommand) -> None:
        """The frame reached the broker: settle its tags for good.

        A no-op when the generation moved on under us (the channel was rebuilt
        mid-batch), because those tags are already gone and the broker will
        redeliver them.
        """
        with self._lock:
            if batch.generation != self._generation:
                return
            self._settle_tags_locked(command.covers)
            self._settled_count += len(command.covers)
            self._frames_sent += 1
            if command.multiple and len(command.covers) > 1:
                self._coalesced_count += len(command.covers)

    def fail_command(self, batch: SettlementBatch, command: SettlementCommand, error: BaseException) -> bool:
        """The frame did NOT reach the broker, or we cannot tell.

        The covered tags become UNRESOLVED: they leave the ledger and are
        never re-emitted, because re-sending a settlement whose first attempt
        may have landed is how you turn an ambiguity into a protocol error.
        Leaving them unacknowledged is always safe -- the broker redelivers
        them when the channel goes away.

        Returns True when the generation was invalidated, which the caller
        must treat as "this channel can no longer be used for coalescing".
        That happens for a failed NACK or REJECT: the tag may still be
        unacknowledged at the broker, so ANY later cumulative ack above it
        would silently acknowledge the very delivery we meant to requeue.
        An ACK that fails is not dangerous in the same way -- a later
        cumulative ack sweeping it up produces exactly the intended outcome.
        """
        with self._lock:
            if batch.generation != self._generation:
                return False
            self._settle_tags_locked(command.covers)
            self._unresolved_count += len(command.covers)
            self._frames_failed += 1
            must_invalidate = command.kind in (SettlementAction.NACK, SettlementAction.REJECT)
        if must_invalidate:
            self.invalidate()
            return True
        return False

    def release_commands(self, batch: SettlementBatch, commands: Sequence[SettlementCommand]) -> None:
        """These commands were never attempted: un-reserve their tags.

        They go back to being ordinary ledger entries and will be planned
        again on the next :meth:`prepare`. Nothing reached the wire, so this
        is always safe.
        """
        with self._lock:
            if batch.generation != self._generation:
                return
            for command in commands:
                for tag in command.covers:
                    if self._reserved.get(tag) == batch.batch_id:
                        del self._reserved[tag]
                self._frames_not_attempted += 1

    def plan(self, *, force_individual: bool = False) -> list[SettlementCommand]:
        """Plan AND immediately settle every command in the ledger.

        This is the pure-planning primitive. It assumes the caller emits every
        returned command, in order, and that they all succeed -- so it is only
        appropriate for tests, for ``TestBroker``, and for callers that supply
        their own ordering and failure guarantees.

        For anything that touches a real channel use :meth:`prepare` plus
        :func:`emit_batch`, which stops on the first failure instead of
        letting a cumulative ack follow a nack that never landed.
        """
        with self._lock:
            if not self._ledger:
                return []
            commands, covered = self._plan_locked(force_individual=force_individual)
            self._settle_tags_locked(covered)
            self._settled_count += len(covered)
            self._frames_sent += len(commands)
            self._coalesced_count += sum(len(c.covers) for c in commands if c.multiple and len(c.covers) > 1)
            return commands

    # ── lifecycle ────────────────────────────────────────────────────────

    def invalidate(self) -> tuple[int, ...]:
        """Reconnect / channel rebuild: drop the WHOLE ledger and bump the
        generation. Returns the dropped tags for logging. Old tags must never
        be replayed onto the replacement channel — the broker will redeliver
        every unacked message on it anyway (invariants I2, I5)."""
        with self._lock:
            dropped = tuple(sorted(self._ledger))
            self._prev_high_water = max(self._prev_high_water, self._last_registered)
            self._ledger.clear()
            self._hold.clear()
            self._requeue.clear()
            self._registered_at.clear()
            # Any in-flight batch is void: its generation no longer matches,
            # so commit/fail/release for it become no-ops.
            self._reserved.clear()
            self._last_registered = 0
            self._generation += 1
            self._invalidations += 1
            self._dropped_on_invalidate += len(dropped)
            return dropped

    def drain_plan(self) -> list[SettlementCommand]:
        """Shutdown: plan AND settle everything already approved.

        Individual frames only — at shutdown there is no reason to wait for a
        prefix. Every blocking delivery is left UNACKED for redelivery; this
        never acks merely to empty the ledger (invariant I6). Same caveat as
        :meth:`plan`: use :meth:`prepare_drain` for a real channel.
        """
        return self.plan(force_individual=True)

    def prepare_drain(self) -> SettlementBatch:
        """Shutdown equivalent of :meth:`prepare`: individual frames only."""
        return self.prepare(force_individual=True)


def _emit_one(
    command: SettlementCommand,
    *,
    ack: Callable[[int, bool], Any],
    nack: Callable[[int, bool], Any],
    reject: Callable[[int, bool], Any],
) -> None:
    if command.kind is SettlementAction.ACK:
        ack(command.delivery_tag, command.multiple)
    elif command.kind is SettlementAction.NACK:
        nack(command.delivery_tag, command.requeue)
    else:
        reject(command.delivery_tag, command.requeue)


def apply_commands(
    commands: Sequence[SettlementCommand],
    *,
    ack: Callable[[int, bool], Any],
    nack: Callable[[int, bool], Any],
    reject: Callable[[int, bool], Any],
) -> list[tuple[SettlementCommand, BaseException | None]]:
    """Emit *commands* in order, STOPPING at the first failure.

    Three callables: ``(tag, multiple)`` for ack, ``(tag, requeue)`` for
    nack/reject. Returns ``(command, error)`` for every command that was
    attempted; commands after a failure are simply absent from the result,
    because they were deliberately not sent.

    Stopping is not a convenience, it is the correctness rule. A coalesced
    plan can read ``ack(102, multiple=True)``, ``nack(103)``,
    ``ack(105, multiple=True)``. If the nack fails and the last frame still
    goes out, the broker acknowledges everything unacknowledged up to 105 --
    including tag 103, the delivery that was supposed to be requeued. The
    message is then lost rather than retried.

    Prefer :func:`emit_batch`, which additionally keeps the coordinator's
    ledger in step with what actually reached the broker.
    """
    results: list[tuple[SettlementCommand, BaseException | None]] = []
    for cmd in commands:
        try:
            _emit_one(cmd, ack=ack, nack=nack, reject=reject)
        except Exception as exc:
            results.append((cmd, exc))
            break
        results.append((cmd, None))
    return results


def emit_batch(
    batch: SettlementBatch,
    coordinator: SettlementCoordinator,
    *,
    ack: Callable[[int, bool], Any],
    nack: Callable[[int, bool], Any],
    reject: Callable[[int, bool], Any],
) -> EmissionReport:
    """Drive a prepared batch onto the wire: emit, observe, then commit.

    This is the only supported way to settle a coalesced plan against a real
    channel. For each command in order:

    * emit it;
    * on success, commit its tags in the coordinator;
    * on failure, stop. The failed command's tags become UNRESOLVED (never
      re-emitted, left for the broker to redeliver), every later command is
      reported as ``not_attempted`` and its tags are released back to the
      ledger, and a failed nack/reject invalidates the generation.

    The ledger therefore only ever moves forward on evidence that a frame
    actually reached the broker, and a cumulative ack can never follow a
    settlement that did not land.
    """
    emitted: list[SettlementCommand] = []
    for index, cmd in enumerate(batch.commands):
        try:
            _emit_one(cmd, ack=ack, nack=nack, reject=reject)
        except Exception as exc:
            rest = batch.commands[index + 1 :]
            invalidated = coordinator.fail_command(batch, cmd, exc)
            if not invalidated:
                coordinator.release_commands(batch, rest)
            return EmissionReport(
                batch_id=batch.batch_id,
                generation=batch.generation,
                emitted=tuple(emitted),
                failed=cmd,
                error=exc,
                not_attempted=tuple(rest),
                invalidated=invalidated,
                unresolved_tags=cmd.covers,
            )
        coordinator.commit_command(batch, cmd)
        emitted.append(cmd)
    return EmissionReport(
        batch_id=batch.batch_id,
        generation=batch.generation,
        emitted=tuple(emitted),
    )


# ── Selected settlement runners (used by both brokers + TestBroker) ────────


async def _emit_one_async(
    command: SettlementCommand,
    *,
    ack: Callable[[int, bool], Awaitable[Any]],
    nack: Callable[[int, bool], Awaitable[Any]],
    reject: Callable[[int, bool], Awaitable[Any]],
) -> None:
    if command.kind is SettlementAction.ACK:
        await ack(command.delivery_tag, command.multiple)
    elif command.kind is SettlementAction.NACK:
        await nack(command.delivery_tag, command.requeue)
    else:
        await reject(command.delivery_tag, command.requeue)


async def emit_batch_async(
    batch: SettlementBatch,
    coordinator: SettlementCoordinator,
    *,
    ack: Callable[[int, bool], Awaitable[Any]],
    nack: Callable[[int, bool], Awaitable[Any]],
    reject: Callable[[int, bool], Awaitable[Any]],
) -> EmissionReport:
    """Awaitable twin of :func:`emit_batch`, with identical guarantees.

    This is what makes the protocol implementable on aio-pika at all. A
    synchronous driver can only call something that schedules the settlement
    and returns, so it learns nothing about whether the frame landed and
    cannot decide whether the next one is safe. Awaiting each command gives
    the emit/observe/commit loop the evidence it needs.

    Cancellation is treated as a failure of the command in flight, because
    its outcome is genuinely unknown: the frame may or may not have been
    written. The tags become unresolved rather than settled, and the
    remaining commands are withheld.
    """
    emitted: list[SettlementCommand] = []
    for index, cmd in enumerate(batch.commands):
        try:
            await _emit_one_async(cmd, ack=ack, nack=nack, reject=reject)
        except (Exception, asyncio.CancelledError) as exc:
            rest = batch.commands[index + 1 :]
            invalidated = coordinator.fail_command(batch, cmd, exc)
            if not invalidated:
                coordinator.release_commands(batch, rest)
            report = EmissionReport(
                batch_id=batch.batch_id,
                generation=batch.generation,
                emitted=tuple(emitted),
                failed=cmd,
                error=exc,
                not_attempted=tuple(rest),
                invalidated=invalidated,
                unresolved_tags=cmd.covers,
            )
            if isinstance(exc, asyncio.CancelledError):
                # Never swallow cancellation: the ledger is consistent now,
                # so let the task actually die.
                raise
            return report
        coordinator.commit_command(batch, cmd)
        emitted.append(cmd)
    return EmissionReport(
        batch_id=batch.batch_id,
        generation=batch.generation,
        emitted=tuple(emitted),
    )


def settle_many_sync(
    messages: Iterable[Any],
    action: SettlementAction,
    *,
    requeue: bool | None = None,
    fail_fast: bool = True,
    on_item: Callable[[SettlementItem], None] | None = None,
) -> SettlementReport:
    """Validate then individually settle *messages* via their sync settlement
    functions (``msg.ack()`` / ``msg.nack(requeue)`` / ``msg.reject(requeue)``).

    Each approved message is settled with its own ``multiple=False`` frame,
    in input order; the transport (``RabbitMessage._ack_fn``) already
    marshals onto the owning I/O thread. A transport exception on one item
    is recorded as ``FAILED`` and the run continues — later items may live
    on a different channel and must still get their outcome.
    """
    plan = plan_selected_settlement(messages, runtime="sync", fail_fast=fail_fast)
    items: list[SettlementItem] = list(plan.rejected)
    for index, msg in plan.approved:
        try:
            if action is SettlementAction.ACK:
                msg.ack()
            elif action is SettlementAction.NACK:
                msg.nack(requeue=True if requeue is None else requeue)
            else:
                msg.reject(requeue=False if requeue is None else requeue)
        except Exception as exc:
            items.append(
                SettlementItem(
                    index=index,
                    status=SettlementItemStatus.FAILED,
                    reason=SETTLE_REASON_TRANSPORT_ERROR,
                    delivery_tag=msg.delivery_tag,
                    message_id=msg.message_id,
                    error=exc,
                )
            )
        else:
            items.append(
                SettlementItem(
                    index=index,
                    status=SettlementItemStatus.DISPATCHED,
                    reason=SETTLE_REASON_OK,
                    delivery_tag=msg.delivery_tag,
                    message_id=msg.message_id,
                )
            )
    if on_item is not None:
        for it in items:
            on_item(it)
    return build_report(action, items, requeue=requeue)


async def settle_many_async(
    messages: Iterable[Any],
    action: SettlementAction,
    *,
    requeue: bool | None = None,
    fail_fast: bool = True,
    on_item: Callable[[SettlementItem], None] | None = None,
) -> SettlementReport:
    """Async twin of :func:`settle_many_sync` (``await msg.ack_async()`` ...).

    Settles sequentially on the calling event loop — aio-pika channel
    methods are not safe to interleave from concurrent tasks, and the
    frames are tiny; the win of a bulk call is one validation pass and one
    report, not parallel socket writes.
    """
    plan = plan_selected_settlement(messages, runtime="async", fail_fast=fail_fast)
    items: list[SettlementItem] = list(plan.rejected)
    for index, msg in plan.approved:
        try:
            if action is SettlementAction.ACK:
                await msg.ack_async()
            elif action is SettlementAction.NACK:
                await msg.nack_async(requeue=True if requeue is None else requeue)
            else:
                await msg.reject_async(requeue=False if requeue is None else requeue)
        except Exception as exc:
            items.append(
                SettlementItem(
                    index=index,
                    status=SettlementItemStatus.FAILED,
                    reason=SETTLE_REASON_TRANSPORT_ERROR,
                    delivery_tag=msg.delivery_tag,
                    message_id=msg.message_id,
                    error=exc,
                )
            )
        else:
            items.append(
                SettlementItem(
                    index=index,
                    status=SettlementItemStatus.DISPATCHED,
                    reason=SETTLE_REASON_OK,
                    delivery_tag=msg.delivery_tag,
                    message_id=msg.message_id,
                )
            )
    if on_item is not None:
        for it in items:
            on_item(it)
    return build_report(action, items, requeue=requeue)
