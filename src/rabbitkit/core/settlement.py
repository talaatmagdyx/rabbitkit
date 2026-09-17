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

import threading
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

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


class CoordinatorError(RuntimeError):
    """Invalid use of :class:`SettlementCoordinator` (unregistered tag,
    non-monotonic registration, double intent)."""


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

    def __init__(
        self,
        *,
        generation: int = 0,
        coalesce: bool = True,
        max_hold: int = 0,
    ) -> None:
        self._generation = generation
        self._coalesce = coalesce
        # max_hold: how many planning rounds a SUCCESS entry stranded behind
        # an outstanding tag may wait for a cumulative ack before being
        # acked individually. 0 = never hold (always individual fallback).
        self._max_hold = max(0, max_hold)
        self._lock = threading.Lock()
        self._ledger: dict[int, DeliveryState] = {}  # insertion == ascending tag order
        self._hold: dict[int, int] = {}
        self._requeue: dict[int, bool] = {}
        self._last_registered = 0
        self._settled_count = 0
        self._coalesced_count = 0
        self._dropped_on_invalidate = 0

    # ── inspection ────────────────────────────────────────────────────────

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def coalesce_enabled(self) -> bool:
        return self._coalesce

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
    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "pending": len(self._ledger),
                "settled": self._settled_count,
                "coalesced": self._coalesced_count,
                "dropped_on_invalidate": self._dropped_on_invalidate,
            }

    def state_of(self, delivery_tag: int) -> DeliveryState | None:
        with self._lock:
            return self._ledger.get(delivery_tag)

    # ── registration & intents ───────────────────────────────────────────

    def register(self, delivery_tag: int) -> None:
        """Register a delivery BEFORE its handler runs. Tags must be strictly
        increasing within a generation (AMQP guarantees this per channel)."""
        with self._lock:
            if delivery_tag <= 0:
                raise CoordinatorError(f"delivery_tag must be positive, got {delivery_tag}")
            if delivery_tag <= self._last_registered:
                raise CoordinatorError(
                    f"delivery_tag {delivery_tag} is not greater than last registered "
                    f"{self._last_registered} (stale or duplicate delivery on generation "
                    f"{self._generation})"
                )
            self._ledger[delivery_tag] = DeliveryState.OUTSTANDING
            self._last_registered = delivery_tag

    def _set_intent(self, delivery_tag: int, state: DeliveryState) -> None:
        current = self._ledger.get(delivery_tag)
        if current is None:
            raise CoordinatorError(f"delivery_tag {delivery_tag} is not registered on generation {self._generation}")
        if current is not DeliveryState.OUTSTANDING and current is not DeliveryState.RETRY_PENDING:
            raise CoordinatorError(f"delivery_tag {delivery_tag} already has intent {current.value}")
        self._ledger[delivery_tag] = state

    def mark_success(self, delivery_tag: int) -> None:
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.SUCCESS)

    def mark_retry_pending(self, delivery_tag: int) -> None:
        """Handler failed and the retry/terminal path now owns this delivery.
        It is NOT settleable through the coordinator; it blocks any
        cumulative ack above it until :meth:`release` (settled elsewhere)."""
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.RETRY_PENDING)

    def mark_nack(self, delivery_tag: int, *, requeue: bool = True) -> None:
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.NACK)
            self._requeue[delivery_tag] = requeue

    def mark_reject(self, delivery_tag: int, *, requeue: bool = False) -> None:
        with self._lock:
            self._set_intent(delivery_tag, DeliveryState.REJECT)
            self._requeue[delivery_tag] = requeue

    def release(self, delivery_tag: int) -> None:
        """Remove a delivery that was settled OUTSIDE the coordinator (e.g. by
        the retry middleware's own ack after a confirmed handoff, or a
        MANUAL handler). Unknown tags are ignored (idempotent)."""
        with self._lock:
            self._ledger.pop(delivery_tag, None)
            self._hold.pop(delivery_tag, None)
            self._requeue.pop(delivery_tag, None)

    # ── planning ─────────────────────────────────────────────────────────

    def plan(self, *, force_individual: bool = False) -> list[SettlementCommand]:
        """Compute and CONSUME the commands that are safe to emit now.

        Every returned command's tags are removed from the ledger, so a tag
        is planned at most once. Entries still OUTSTANDING / RETRY_PENDING
        stay. Emission order is ascending by the highest tag each command
        settles, which keeps individual nacks below a cumulative ack ahead
        of it.
        """
        with self._lock:
            if not self._ledger:
                return []
            commands: list[SettlementCommand] = []
            tags = sorted(self._ledger)  # ascending, defensive (dict is insertion-ordered anyway)

            # 1. Longest all-SUCCESS prefix → cumulative ack (if allowed).
            prefix: list[int] = []
            for t in tags:
                if self._ledger[t] is DeliveryState.SUCCESS:
                    prefix.append(t)
                else:
                    break
            use_multiple = self._coalesce and not force_individual and len(prefix) >= 2
            consumed: set[int] = set()
            if use_multiple:
                commands.append(
                    SettlementCommand(
                        kind=SettlementAction.ACK,
                        delivery_tag=prefix[-1],
                        multiple=True,
                        covers=tuple(prefix),
                        generation=self._generation,
                    )
                )
                consumed.update(prefix)
                self._coalesced_count += len(prefix)
                prefix_boundary = prefix[-1]
            else:
                prefix_boundary = 0

            # 2. Everything else: individual, in tag order.
            blocked = False  # True once we pass an OUTSTANDING/RETRY_PENDING entry
            for t in tags:
                if t in consumed:
                    continue
                state = self._ledger[t]
                if state is DeliveryState.OUTSTANDING or state is DeliveryState.RETRY_PENDING:
                    blocked = True
                    continue
                if state is DeliveryState.SUCCESS:
                    # Behind an unfinished sibling: optionally hold a few rounds
                    # hoping for a cumulative ack; never hold when coalescing
                    # is off or the prefix already advanced past us.
                    if (
                        self._coalesce
                        and not force_individual
                        and blocked
                        and t > prefix_boundary
                        and self._hold.get(t, 0) < self._max_hold
                    ):
                        self._hold[t] = self._hold.get(t, 0) + 1
                        continue
                    commands.append(
                        SettlementCommand(
                            kind=SettlementAction.ACK,
                            delivery_tag=t,
                            multiple=False,
                            covers=(t,),
                            generation=self._generation,
                        )
                    )
                    consumed.add(t)
                elif state is DeliveryState.NACK:
                    commands.append(
                        SettlementCommand(
                            kind=SettlementAction.NACK,
                            delivery_tag=t,
                            requeue=self._requeue.get(t, True),
                            covers=(t,),
                            generation=self._generation,
                        )
                    )
                    consumed.add(t)
                elif state is DeliveryState.REJECT:
                    commands.append(
                        SettlementCommand(
                            kind=SettlementAction.REJECT,
                            delivery_tag=t,
                            requeue=self._requeue.get(t, False),
                            covers=(t,),
                            generation=self._generation,
                        )
                    )
                    consumed.add(t)

            for t in consumed:
                self._ledger.pop(t, None)
                self._hold.pop(t, None)
                self._requeue.pop(t, None)
            self._settled_count += len(consumed)
            commands.sort(key=lambda c: c.delivery_tag)
            return commands

    # ── lifecycle ────────────────────────────────────────────────────────

    def invalidate(self) -> tuple[int, ...]:
        """Reconnect / channel rebuild: drop the WHOLE ledger and bump the
        generation. Returns the dropped tags for logging. Old tags must never
        be replayed onto the replacement channel — the broker will redeliver
        every unacked message on it anyway."""
        with self._lock:
            dropped = tuple(sorted(self._ledger))
            self._ledger.clear()
            self._hold.clear()
            self._requeue.clear()
            self._last_registered = 0
            self._generation += 1
            self._dropped_on_invalidate += len(dropped)
            return dropped

    def drain_plan(self) -> list[SettlementCommand]:
        """Shutdown: emit everything approved (individually — no reason to
        wait for a prefix), leave OUTSTANDING/RETRY_PENDING unacked."""
        return self.plan(force_individual=True)


def apply_commands(
    commands: Sequence[SettlementCommand],
    *,
    ack: Callable[[int, bool], Any],
    nack: Callable[[int, bool], Any],
    reject: Callable[[int, bool], Any],
) -> list[tuple[SettlementCommand, BaseException | None]]:
    """Emit *commands* through three callables (``(tag, multiple)`` for ack,
    ``(tag, requeue)`` for nack/reject). Returns ``(command, error)`` per
    command — the first transport error does NOT stop later commands on
    other tags, but every failure is visible to the caller."""
    results: list[tuple[SettlementCommand, BaseException | None]] = []
    for cmd in commands:
        try:
            if cmd.kind is SettlementAction.ACK:
                ack(cmd.delivery_tag, cmd.multiple)
            elif cmd.kind is SettlementAction.NACK:
                nack(cmd.delivery_tag, cmd.requeue)
            else:
                reject(cmd.delivery_tag, cmd.requeue)
            results.append((cmd, None))
        except Exception as exc:
            results.append((cmd, exc))
    return results


# ── Selected settlement runners (used by both brokers + TestBroker) ────────


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
