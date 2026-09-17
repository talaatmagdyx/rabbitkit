"""Tests for core/settlement.py — selected settlement planning/runners and the
coalescing SettlementCoordinator."""

from __future__ import annotations

from typing import Any

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.settlement import (
    SETTLE_REASON_ALREADY,
    SETTLE_REASON_DUPLICATE,
    SETTLE_REASON_NO_SETTLEMENT_FN,
    SETTLE_REASON_NOT_MESSAGE,
    SETTLE_REASON_OK,
    SETTLE_REASON_STALE_CHANNEL,
    SETTLE_REASON_TRANSPORT_ERROR,
    SETTLE_REASON_VALIDATION_ABORT,
    SETTLE_REASON_WRONG_RUNTIME,
    CoordinatorError,
    SettlementCommand,
    SettlementCoordinator,
    SettlementItem,
    SettlementReport,
    SettlementReportError,
    apply_commands,
    build_report,
    plan_selected_settlement,
    settle_many_async,
    settle_many_sync,
)
from rabbitkit.core.types import DeliveryState, SettlementAction, SettlementItemStatus

# ── helpers ────────────────────────────────────────────────────────────────


def _msg(tag: int, *, sync: bool = True, async_: bool = False, alive: bool | None = True) -> RabbitMessage:
    m = RabbitMessage(body=b"x", delivery_tag=tag, message_id=f"m{tag}")
    calls: list[tuple[str, Any]] = []
    m.raw_message = calls  # stash call log for assertions
    if sync:
        m._ack_fn = lambda: calls.append(("ack", None))
        m._nack_fn = lambda rq: calls.append(("nack", rq))
        m._reject_fn = lambda rq: calls.append(("reject", rq))
    if async_:

        async def _a() -> None:
            calls.append(("ack", None))

        async def _n(rq: bool) -> None:
            calls.append(("nack", rq))

        async def _r(rq: bool) -> None:
            calls.append(("reject", rq))

        m._ack_async_fn = _a
        m._nack_async_fn = _n
        m._reject_async_fn = _r
    if alive is not None:
        m._channel_alive = lambda: alive
    return m


def _calls(m: RabbitMessage) -> list[tuple[str, Any]]:
    return m.raw_message  # type: ignore[no-any-return]


# ── plan_selected_settlement ───────────────────────────────────────────────


class TestPlanSelectedSettlement:
    def test_all_valid(self) -> None:
        msgs = [_msg(1), _msg(2), _msg(3)]
        plan = plan_selected_settlement(msgs, runtime="sync")
        assert [i for i, _ in plan.approved] == [0, 1, 2]
        assert plan.rejected == ()
        assert not plan.has_problems

    def test_not_a_message(self) -> None:
        plan = plan_selected_settlement([_msg(1), "nope"], runtime="sync", fail_fast=False)
        assert [i for i, _ in plan.approved] == [0]
        (bad,) = plan.rejected
        assert bad.status is SettlementItemStatus.INVALID and bad.reason == SETTLE_REASON_NOT_MESSAGE
        assert isinstance(bad.error, TypeError)

    def test_duplicate_handle(self) -> None:
        m = _msg(1)
        plan = plan_selected_settlement([m, m], runtime="sync", fail_fast=False)
        assert len(plan.approved) == 1
        (dup,) = plan.rejected
        assert dup.status is SettlementItemStatus.DUPLICATE and dup.reason == SETTLE_REASON_DUPLICATE
        assert dup.index == 1 and dup.delivery_tag == 1

    def test_already_settled_is_not_a_problem(self) -> None:
        m = _msg(1)
        m.ack()
        plan = plan_selected_settlement([m, _msg(2)], runtime="sync")
        assert [i for i, _ in plan.approved] == [1]
        (done,) = plan.rejected
        assert done.status is SettlementItemStatus.ALREADY_SETTLED and done.reason == SETTLE_REASON_ALREADY
        assert not plan.has_problems

    def test_no_settlement_fn(self) -> None:
        m = RabbitMessage(body=b"x", delivery_tag=9)
        plan = plan_selected_settlement([m], runtime="sync")
        (bad,) = plan.rejected
        assert bad.status is SettlementItemStatus.INVALID and bad.reason == SETTLE_REASON_NO_SETTLEMENT_FN

    def test_wrong_runtime_async_only_message_on_sync(self) -> None:
        m = _msg(1, sync=False, async_=True)
        plan = plan_selected_settlement([m], runtime="sync")
        (bad,) = plan.rejected
        assert bad.status is SettlementItemStatus.INVALID and bad.reason == SETTLE_REASON_WRONG_RUNTIME

    def test_async_runtime_accepts_sync_only_message(self) -> None:
        plan = plan_selected_settlement([_msg(1)], runtime="async")
        assert len(plan.approved) == 1

    def test_stale_channel(self) -> None:
        plan = plan_selected_settlement([_msg(1, alive=False)], runtime="sync")
        (bad,) = plan.rejected
        assert bad.status is SettlementItemStatus.STALE and bad.reason == SETTLE_REASON_STALE_CHANNEL

    def test_probe_raising_counts_as_stale(self) -> None:
        m = _msg(1)

        def boom() -> bool:
            raise RuntimeError("channel gone")

        m._channel_alive = boom
        plan = plan_selected_settlement([m], runtime="sync")
        assert plan.rejected[0].status is SettlementItemStatus.STALE

    def test_unknown_liveness_is_approved(self) -> None:
        plan = plan_selected_settlement([_msg(1, alive=None)], runtime="sync")
        assert len(plan.approved) == 1

    def test_fail_fast_aborts_everything(self) -> None:
        plan = plan_selected_settlement([_msg(1), "bad", _msg(3)], runtime="sync", fail_fast=True)
        assert plan.approved == ()
        statuses = {it.index: it.status for it in plan.rejected}
        assert statuses == {
            0: SettlementItemStatus.NOT_ATTEMPTED,
            1: SettlementItemStatus.INVALID,
            2: SettlementItemStatus.NOT_ATTEMPTED,
        }
        assert all(
            it.reason == SETTLE_REASON_VALIDATION_ABORT
            for it in plan.rejected
            if it.status is SettlementItemStatus.NOT_ATTEMPTED
        )
        assert [it.index for it in plan.rejected] == [0, 1, 2]

    def test_fail_fast_with_only_already_settled_still_proceeds(self) -> None:
        m = _msg(1)
        m.ack()
        plan = plan_selected_settlement([m, _msg(2)], runtime="sync", fail_fast=True)
        assert len(plan.approved) == 1

    def test_bad_runtime(self) -> None:
        with pytest.raises(ValueError):
            plan_selected_settlement([], runtime="threads")


# ── SettlementItem / Report ────────────────────────────────────────────────


class TestSettlementReport:
    def test_item_reason_bounded(self) -> None:
        with pytest.raises(ValueError):
            SettlementItem(index=0, status=SettlementItemStatus.FAILED, reason="ChannelClosed: oops")

    def test_report_helpers(self) -> None:
        r = build_report(
            SettlementAction.ACK,
            [
                SettlementItem(index=2, status=SettlementItemStatus.FAILED, reason=SETTLE_REASON_TRANSPORT_ERROR),
                SettlementItem(index=0, status=SettlementItemStatus.DISPATCHED, reason=SETTLE_REASON_OK),
                SettlementItem(index=1, status=SettlementItemStatus.ALREADY_SETTLED, reason=SETTLE_REASON_ALREADY),
            ],
        )
        assert [it.index for it in r.items] == [0, 1, 2]
        assert len(r) == 3
        assert r.counts[SettlementItemStatus.DISPATCHED] == 1
        assert [it.index for it in r.dispatched] == [0]
        assert [it.index for it in r.problems] == [2]
        assert not r.all_dispatched
        with pytest.raises(SettlementReportError) as ei:
            r.raise_for_status()
        assert ei.value.report is r
        assert "ack_many incomplete" in str(ei.value)

    def test_all_dispatched_tolerates_already_settled(self) -> None:
        r = SettlementReport(
            action=SettlementAction.NACK,
            items=(SettlementItem(index=0, status=SettlementItemStatus.ALREADY_SETTLED, reason=SETTLE_REASON_ALREADY),),
            requeue=False,
        )
        assert r.all_dispatched
        assert r.raise_for_status() is r
        assert r.requeue is False


# ── settle_many_sync / async ───────────────────────────────────────────────


class TestSettleManySync:
    def test_ack_each_individually_in_order(self) -> None:
        msgs = [_msg(1), _msg(3), _msg(2)]
        report = settle_many_sync(msgs, SettlementAction.ACK)
        assert report.all_dispatched
        assert [it.delivery_tag for it in report.items] == [1, 3, 2]
        for m in msgs:
            assert _calls(m) == [("ack", None)]
            assert m.disposition == "acked"

    def test_selected_ack_does_not_touch_unselected_sibling(self) -> None:
        """Plan §4.1 reproduction: completing tags 1 and 3 must not settle 2."""
        m1, m2, m3 = _msg(1), _msg(2), _msg(3)
        settle_many_sync([m1, m3], SettlementAction.ACK)
        assert m1.is_settled and m3.is_settled
        assert not m2.is_settled and _calls(m2) == []

    def test_nack_requeue_flag(self) -> None:
        m = _msg(1)
        report = settle_many_sync([m], SettlementAction.NACK, requeue=False)
        assert _calls(m) == [("nack", False)]
        assert report.requeue is False and report.action is SettlementAction.NACK

    def test_nack_default_requeue_true(self) -> None:
        m = _msg(1)
        settle_many_sync([m], SettlementAction.NACK)
        assert _calls(m) == [("nack", True)]

    def test_reject_default_requeue_false(self) -> None:
        m = _msg(1)
        settle_many_sync([m], SettlementAction.REJECT)
        assert _calls(m) == [("reject", False)]

    def test_transport_error_is_reported_and_run_continues(self) -> None:
        good1, bad, good2 = _msg(1), _msg(2), _msg(3)

        def boom() -> None:
            raise ConnectionError("channel closed")

        bad._ack_fn = boom
        report = settle_many_sync([good1, bad, good2], SettlementAction.ACK)
        statuses = [it.status for it in report.items]
        assert statuses == [
            SettlementItemStatus.DISPATCHED,
            SettlementItemStatus.FAILED,
            SettlementItemStatus.DISPATCHED,
        ]
        assert report.items[1].reason == SETTLE_REASON_TRANSPORT_ERROR
        assert isinstance(report.items[1].error, ConnectionError)
        assert not bad.is_settled  # disposition stays pending on failure
        assert good2.is_settled

    def test_fail_fast_attempts_nothing(self) -> None:
        good = _msg(1)
        report = settle_many_sync([good, "x"], SettlementAction.ACK, fail_fast=True)
        assert not good.is_settled
        assert report.items[0].status is SettlementItemStatus.NOT_ATTEMPTED

    def test_on_item_hook(self) -> None:
        seen: list[SettlementItemStatus] = []
        settle_many_sync(
            [_msg(1), "x"], SettlementAction.ACK, fail_fast=False, on_item=lambda it: seen.append(it.status)
        )
        assert sorted(s.value for s in seen) == ["dispatched", "invalid"]


class TestSettleManyAsync:
    async def test_ack_async(self) -> None:
        msgs = [_msg(1, sync=False, async_=True), _msg(2, sync=False, async_=True)]
        report = await settle_many_async(msgs, SettlementAction.ACK)
        assert report.all_dispatched
        for m in msgs:
            assert _calls(m) == [("ack", None)]

    async def test_async_falls_back_to_sync_fn(self) -> None:
        m = _msg(1)
        report = await settle_many_async([m], SettlementAction.NACK, requeue=False)
        assert report.all_dispatched and _calls(m) == [("nack", False)]

    async def test_async_transport_error(self) -> None:
        m = _msg(1, sync=False, async_=True)

        async def boom() -> None:
            raise RuntimeError("closed")

        m._ack_async_fn = boom
        report = await settle_many_async([m], SettlementAction.ACK)
        assert report.items[0].status is SettlementItemStatus.FAILED

    async def test_reject_async(self) -> None:
        m = _msg(1, sync=False, async_=True)
        await settle_many_async([m], SettlementAction.REJECT, requeue=True)
        assert _calls(m) == [("reject", True)]


# ── SettlementCoordinator ──────────────────────────────────────────────────


def _acks(cmds: list[SettlementCommand]) -> list[tuple[str, int, bool]]:
    return [(c.kind.value, c.delivery_tag, c.multiple) for c in cmds]


class TestCoordinatorRegistration:
    def test_register_monotonic(self) -> None:
        c = SettlementCoordinator()
        c.register(1)
        c.register(2)
        with pytest.raises(CoordinatorError):
            c.register(2)
        with pytest.raises(CoordinatorError):
            c.register(1)
        with pytest.raises(CoordinatorError):
            c.register(0)

    def test_intent_requires_registration(self) -> None:
        c = SettlementCoordinator()
        with pytest.raises(CoordinatorError):
            c.mark_success(5)

    def test_double_intent_rejected(self) -> None:
        c = SettlementCoordinator()
        c.register(1)
        c.mark_success(1)
        with pytest.raises(CoordinatorError):
            c.mark_nack(1)

    def test_retry_pending_can_transition(self) -> None:
        c = SettlementCoordinator()
        c.register(1)
        c.mark_retry_pending(1)
        assert c.state_of(1) is DeliveryState.RETRY_PENDING
        c.mark_success(1)  # e.g. handoff confirmed and retry path decided to ack via coordinator
        assert c.state_of(1) is DeliveryState.SUCCESS

    def test_release_is_idempotent(self) -> None:
        c = SettlementCoordinator()
        c.register(1)
        c.release(1)
        c.release(1)
        assert c.pending == 0


class TestCoordinatorPlanning:
    def test_plan_table_from_spec(self) -> None:
        """Plan §8.3 example: 101 success, 102 running, 103 success, 104 retry pending."""
        c = SettlementCoordinator(max_hold=0)
        for t in (101, 102, 103, 104):
            c.register(t)
        c.mark_success(101)
        c.mark_success(103)
        c.mark_retry_pending(104)
        cmds = c.plan()
        assert _acks(cmds) == [("ack", 101, False), ("ack", 103, False)]
        # 102 and 104 untouched
        assert c.state_of(102) is DeliveryState.OUTSTANDING
        assert c.state_of(104) is DeliveryState.RETRY_PENDING
        # never a cumulative 103 while 102 outstanding
        assert not any(cmd.multiple for cmd in cmds)

    def test_all_success_prefix_coalesces(self) -> None:
        c = SettlementCoordinator()
        for t in (1, 2, 3, 4):
            c.register(t)
        for t in (1, 2, 3):
            c.mark_success(t)
        cmds = c.plan()
        assert _acks(cmds) == [("ack", 3, True)]
        assert cmds[0].covers == (1, 2, 3)
        assert c.pending == 1 and c.state_of(4) is DeliveryState.OUTSTANDING
        assert c.stats["coalesced"] == 3

    def test_single_success_prefix_is_individual(self) -> None:
        c = SettlementCoordinator()
        c.register(1)
        c.register(2)
        c.mark_success(1)
        assert _acks(c.plan()) == [("ack", 1, False)]

    def test_coalescing_disabled(self) -> None:
        c = SettlementCoordinator(coalesce=False)
        for t in (1, 2, 3):
            c.register(t)
            c.mark_success(t)
        assert _acks(c.plan()) == [("ack", 1, False), ("ack", 2, False), ("ack", 3, False)]

    def test_force_individual(self) -> None:
        c = SettlementCoordinator()
        for t in (1, 2):
            c.register(t)
            c.mark_success(t)
        assert _acks(c.plan(force_individual=True)) == [("ack", 1, False), ("ack", 2, False)]

    def test_hold_then_individual_fallback(self) -> None:
        c = SettlementCoordinator(max_hold=1)
        c.register(1)
        c.register(2)
        c.mark_success(2)
        assert c.plan() == []  # held once hoping 1 completes
        assert _acks(c.plan()) == [("ack", 2, False)]  # fallback: never wait forever

    def test_hold_rewarded_by_coalesce(self) -> None:
        c = SettlementCoordinator(max_hold=2)
        c.register(1)
        c.register(2)
        c.mark_success(2)
        assert c.plan() == []
        c.mark_success(1)
        cmds = c.plan()
        assert _acks(cmds) == [("ack", 2, True)] and cmds[0].covers == (1, 2)

    def test_nack_reject_individual_in_order(self) -> None:
        c = SettlementCoordinator()
        for t in (1, 2, 3, 4):
            c.register(t)
        c.mark_success(1)
        c.mark_nack(2, requeue=False)
        c.mark_reject(3, requeue=True)
        c.mark_success(4)
        cmds = c.plan()
        assert [(cm.kind.value, cm.delivery_tag, cm.multiple, cm.requeue) for cm in cmds] == [
            ("ack", 1, False, False),
            ("nack", 2, False, False),
            ("reject", 3, False, True),
            ("ack", 4, False, False),
        ]

    def test_cumulative_never_covers_non_success(self) -> None:
        c = SettlementCoordinator()
        for t in (1, 2, 3):
            c.register(t)
        c.mark_success(1)
        c.mark_nack(2)
        c.mark_success(3)
        cmds = c.plan()
        for cmd in cmds:
            if cmd.multiple:
                pytest.fail("cumulative ack emitted across a nack")
        assert _acks(cmds) == [("ack", 1, False), ("nack", 2, False), ("ack", 3, False)]

    def test_each_tag_settled_once(self) -> None:
        c = SettlementCoordinator()
        for t in (1, 2):
            c.register(t)
            c.mark_success(t)
        first = c.plan()
        assert first
        assert c.plan() == []
        assert c.pending == 0

    def test_empty_plan(self) -> None:
        assert SettlementCoordinator().plan() == []


class TestCoordinatorLifecycle:
    def test_invalidate_drops_everything_and_bumps_generation(self) -> None:
        c = SettlementCoordinator(generation=3)
        for t in (1, 2):
            c.register(t)
        c.mark_success(1)
        dropped = c.invalidate()
        assert dropped == (1, 2)
        assert c.generation == 4
        assert c.plan() == []  # old tags never replayed
        c.register(1)  # new generation restarts tags
        assert c.stats["dropped_on_invalidate"] == 2

    def test_drain_plan_leaves_outstanding(self) -> None:
        c = SettlementCoordinator()
        for t in (1, 2, 3):
            c.register(t)
        c.mark_success(1)
        c.mark_success(3)
        cmds = c.drain_plan()
        assert _acks(cmds) == [("ack", 1, False), ("ack", 3, False)]
        assert c.state_of(2) is DeliveryState.OUTSTANDING

    def test_outstanding_counter(self) -> None:
        c = SettlementCoordinator()
        c.register(1)
        c.register(2)
        c.mark_success(1)
        assert c.outstanding == 1 and c.pending == 2
        assert c.coalesce_enabled


class TestApplyCommands:
    def test_apply_routes_and_collects_errors(self) -> None:
        log: list[tuple[str, int, bool]] = []

        def ack(t: int, m: bool) -> None:
            log.append(("ack", t, m))

        def nack(t: int, rq: bool) -> None:
            raise RuntimeError("nack failed")

        def reject(t: int, rq: bool) -> None:
            log.append(("reject", t, rq))

        cmds = [
            SettlementCommand(kind=SettlementAction.ACK, delivery_tag=3, multiple=True, covers=(1, 2, 3)),
            SettlementCommand(kind=SettlementAction.NACK, delivery_tag=4, requeue=True, covers=(4,)),
            SettlementCommand(kind=SettlementAction.REJECT, delivery_tag=5, requeue=False, covers=(5,)),
        ]
        results = apply_commands(cmds, ack=ack, nack=nack, reject=reject)
        assert log == [("ack", 3, True), ("reject", 5, False)]
        assert results[0][1] is None
        assert isinstance(results[1][1], RuntimeError)
        assert results[2][1] is None
