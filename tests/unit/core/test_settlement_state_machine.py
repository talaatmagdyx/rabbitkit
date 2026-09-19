"""SettlementCoordinator as a correctness-critical state machine.

Pins the eight safety invariants, one test class each:

* **I1** Never cumulative-ack across an unsafe gap.
* **I2** Never settle a delivery from another channel/generation.
* **I3** Never ack a delivery that did not reach ACK_READY.
* **I4** A contradictory settlement decision is never silently accepted.
* **I5** Channel loss invalidates every pending settlement for that channel.
* **I6** Shutdown may leave messages unacked, never acks merely to flush.
* **I7** Optimization failure may cost throughput, never delivery semantics.
* **I8** Coalescing changes frame count, not application settlement semantics.
"""

from __future__ import annotations

import threading
from typing import ClassVar

import pytest

from rabbitkit.core.settlement import (
    ContradictorySettlementError,
    CoordinatorError,
    LedgerFullError,
    SettlementCommand,
    SettlementCoordinator,
    StaleGenerationError,
    UnknownDeliveryError,
)
from rabbitkit.core.types import DeliveryState, SettlementAction

#: Every state that must block the cumulative-ack frontier.
BLOCKING = ("outstanding", "retry_pending", "failed", "cancelled")

MARK = {
    "success": "mark_success",
    "retry_pending": "mark_retry_pending",
    "failed": "mark_failed",
    "cancelled": "mark_cancelled",
    "nack": "mark_nack",
    "reject": "mark_reject",
}


def _mark(coord: SettlementCoordinator, tag: int, state: str) -> None:
    if state != "outstanding":
        getattr(coord, MARK[state])(tag)


def _emitted(commands: list[SettlementCommand]) -> list[tuple[str, int, bool]]:
    return [(c.kind.value, c.delivery_tag, c.multiple) for c in commands]


def _covered(commands: list[SettlementCommand]) -> set[int]:
    """Every tag the broker would consider settled by these commands — for a
    cumulative ack that is its whole ``covers`` tuple."""
    return {tag for c in commands for tag in c.covers}


def _coord(tags: dict[int, str], **kw: object) -> SettlementCoordinator:
    coord = SettlementCoordinator(**kw)  # type: ignore[arg-type]
    for tag in sorted(tags):
        coord.register(tag)
    for tag in sorted(tags):
        _mark(coord, tag, tags[tag])
    return coord


# ── the state machine itself ──────────────────────────────────────────────


class TestDeliveryStateSemantics:
    def test_only_success_is_ack_safe(self) -> None:
        for state in DeliveryState:
            assert state.is_ack_safe is (state is DeliveryState.SUCCESS), state

    def test_only_success_nack_reject_are_emittable(self) -> None:
        emittable = {DeliveryState.SUCCESS, DeliveryState.NACK, DeliveryState.REJECT}
        for state in DeliveryState:
            assert state.is_emittable is (state in emittable), state
            assert state.blocks_frontier is (state not in emittable), state

    @pytest.mark.parametrize("state", list(DeliveryState))
    def test_finished_is_not_the_same_as_ack_safe(self, state: DeliveryState) -> None:
        """FAILED and CANCELLED are finished but must never be acked."""
        if state in (DeliveryState.FAILED, DeliveryState.CANCELLED):
            assert not state.is_ack_safe and state.blocks_frontier


class TestLegalTransitions:
    @pytest.mark.parametrize("target", ["success", "nack", "reject", "retry_pending", "failed", "cancelled"])
    def test_outstanding_may_become_anything(self, target: str) -> None:
        coord = _coord({1: "outstanding"})
        _mark(coord, 1, target)
        assert coord.state_of(1) is DeliveryState(target)

    @pytest.mark.parametrize("target", ["success", "nack", "reject", "failed", "cancelled"])
    def test_retry_pending_may_be_resolved(self, target: str) -> None:
        coord = _coord({1: "retry_pending"})
        _mark(coord, 1, target)
        assert coord.state_of(1) is DeliveryState(target)

    @pytest.mark.parametrize("blocked", ["failed", "cancelled"])
    @pytest.mark.parametrize("target", ["nack", "reject"])
    def test_failed_or_cancelled_may_still_be_settled_explicitly(self, blocked: str, target: str) -> None:
        coord = _coord({1: blocked})
        _mark(coord, 1, target)
        assert coord.state_of(1) is DeliveryState(target)

    @pytest.mark.parametrize("terminal", ["success", "nack", "reject"])
    @pytest.mark.parametrize("target", ["success", "nack", "reject", "failed", "cancelled"])
    def test_terminal_states_never_change(self, terminal: str, target: str) -> None:
        coord = _coord({1: terminal})
        if terminal == target:
            _mark(coord, 1, target)  # idempotent repeat
            assert coord.state_of(1) is DeliveryState(terminal)
            return
        with pytest.raises(ContradictorySettlementError):
            _mark(coord, 1, target)
        assert coord.state_of(1) is DeliveryState(terminal)


# ── I1: never cumulative-ack across an unsafe gap ─────────────────────────


class TestI1NoCumulativeAcrossGap:
    def test_the_plans_worked_example(self) -> None:
        """101 DONE, 102 DONE, 103 RUNNING, 104 DONE, 105 DONE
        → the maximum safe cumulative ack is 102. Never 105."""
        coord = _coord({101: "success", 102: "success", 103: "outstanding", 104: "success", 105: "success"})
        commands = coord.plan()
        assert _emitted(commands) == [("ack", 102, True), ("ack", 104, False), ("ack", 105, False)]
        assert 103 not in _covered(commands)
        for command in commands:
            if command.multiple:
                assert max(command.covers) <= 102

    @pytest.mark.parametrize("blocker", BLOCKING)
    def test_every_blocking_state_stops_the_frontier(self, blocker: str) -> None:
        coord = _coord({1: "success", 2: blocker, 3: "success", 4: "success"})
        commands = coord.plan()
        assert 2 not in _covered(commands)
        assert not any(c.multiple for c in commands), "a cumulative ack would have covered tag 2"
        assert coord.state_of(2) is DeliveryState(blocker)

    def test_frontier_reflects_the_leading_run_not_the_highest_done(self) -> None:
        coord = _coord({1: "success", 2: "success", 3: "outstanding", 4: "success"})
        assert coord.frontier == 2
        assert coord.ack_ready == 3  # three are done...
        assert coord.gap_count == 1  # ...but one straggler strands the last

    def test_reverse_completion_order_never_over_acks(self) -> None:
        coord = SettlementCoordinator()
        for tag in range(1, 9):
            coord.register(tag)
        for tag in reversed(range(2, 9)):  # 8..2 complete, 1 still running
            coord.mark_success(tag)
        commands = coord.plan()
        assert 1 not in _covered(commands)
        assert not any(c.multiple for c in commands)
        coord.mark_success(1)
        assert _emitted(coord.plan()) == [("ack", 1, False)]

    def test_shuffled_completion_then_full_coalesce(self) -> None:
        """delivery 1..8, completion order 5 2 8 3 1 7 4 6."""
        coord = SettlementCoordinator()
        for tag in range(1, 9):
            coord.register(tag)
        emitted: list[SettlementCommand] = []
        for tag in (5, 2, 8, 3, 1, 7, 4, 6):
            coord.mark_success(tag)
            emitted.extend(coord.plan())
        assert _covered(emitted) == set(range(1, 9))
        assert sum(len(c.covers) for c in emitted) == 8  # each tag settled exactly once


# ── I2 / I5: channel generation ───────────────────────────────────────────


class TestI2AndI5Generation:
    def test_invalidate_drops_everything_and_bumps_generation(self) -> None:
        coord = _coord({1: "success", 2: "outstanding", 3: "success"}, generation=17)
        assert coord.invalidate() == (1, 2, 3)
        assert coord.generation == 18
        assert coord.plan() == [], "old tags must never reach the replacement channel"
        assert coord.stats["invalidations"] == 1
        assert coord.stats["dropped_on_invalidate"] == 3

    def test_late_completion_after_channel_loss_is_stale_not_unknown(self) -> None:
        coord = _coord({1: "success", 2: "outstanding"})
        coord.invalidate()
        with pytest.raises(StaleGenerationError):
            coord.mark_success(2)  # the worker finished after the channel died
        assert coord.plan() == []

    def test_new_generation_restarts_tags_without_touching_the_old(self) -> None:
        coord = _coord({1: "success", 2: "outstanding"})
        coord.invalidate()
        coord.register(1)  # the broker redelivers on a fresh channel
        coord.mark_success(1)
        commands = coord.plan()
        assert _emitted(commands) == [("ack", 1, False)]
        assert all(c.generation == coord.generation for c in commands)

    def test_commands_carry_their_generation(self) -> None:
        coord = _coord({1: "success", 2: "success"}, generation=42)
        assert {c.generation for c in coord.plan()} == {42}


# ── I3: never ack something that did not reach ACK_READY ──────────────────


class TestI3NeverAckUnfinished:
    @pytest.mark.parametrize("state", ["failed", "cancelled"])
    def test_failed_and_cancelled_can_never_become_success(self, state: str) -> None:
        coord = _coord({1: state})
        with pytest.raises(ContradictorySettlementError, match="refusing to change it to success"):
            coord.mark_success(1)

    @pytest.mark.parametrize("state", BLOCKING)
    def test_blocking_states_are_never_emitted(self, state: str) -> None:
        coord = _coord({1: state})
        assert coord.plan() == []
        assert coord.drain_plan() == []
        assert coord.pending == 1

    def test_an_unregistered_tag_can_never_be_completed(self) -> None:
        coord = SettlementCoordinator()
        with pytest.raises(UnknownDeliveryError):
            coord.mark_success(999)
        assert coord.plan() == []


# ── I4: contradictions are never silently accepted ────────────────────────


class TestI4NoSilentContradiction:
    def test_repeating_the_same_decision_is_idempotent(self) -> None:
        coord = _coord({1: "success"})
        coord.mark_success(1)
        coord.mark_success(1)
        assert coord.state_of(1) is DeliveryState.SUCCESS
        assert _emitted(coord.plan()) == [("ack", 1, False)]  # settled once, not three times

    def test_repeating_a_nack_with_the_same_requeue_is_idempotent(self) -> None:
        coord = _coord({1: "outstanding"})
        coord.mark_nack(1, requeue=True)
        coord.mark_nack(1, requeue=True)
        assert _emitted(coord.plan()) == [("nack", 1, False)]

    def test_flipping_requeue_is_a_contradiction(self) -> None:
        coord = _coord({1: "outstanding"})
        coord.mark_nack(1, requeue=True)
        with pytest.raises(ContradictorySettlementError, match="requeue"):
            coord.mark_nack(1, requeue=False)

    @pytest.mark.parametrize(("first", "second"), [("success", "nack"), ("nack", "success"), ("reject", "success")])
    def test_contradictory_settlement_raises(self, first: str, second: str) -> None:
        coord = _coord({1: first})
        with pytest.raises(ContradictorySettlementError):
            _mark(coord, 1, second)

    def test_settling_after_the_frame_was_emitted_raises(self) -> None:
        coord = _coord({1: "success"})
        coord.plan()
        with pytest.raises(ContradictorySettlementError, match="already settled"):
            coord.mark_nack(1)

    def test_every_coordinator_error_is_catchable_as_one_type(self) -> None:
        for error in (
            UnknownDeliveryError,
            StaleGenerationError,
            ContradictorySettlementError,
            LedgerFullError,
        ):
            assert issubclass(error, CoordinatorError)


# ── item 4: nack/reject in the middle, and the segment after it ───────────


class TestNackAndRejectSegments:
    def test_the_plans_nack_example_forms_a_second_segment(self) -> None:
        """101 ACK, 102 ACK, 103 NACK, 104 ACK, 105 ACK →
        ack(102, multiple), nack(103), then 104-105 may coalesce again."""
        coord = _coord(
            {101: "success", 102: "success", 103: "nack", 104: "success", 105: "success"},
        )
        commands = coord.plan()
        assert _emitted(commands) == [("ack", 102, True), ("nack", 103, False), ("ack", 105, True)]
        assert _covered(commands) == {101, 102, 103, 104, 105}

    def test_commands_are_ordered_so_the_lower_frames_go_first(self) -> None:
        """A cumulative ack is only safe because everything below it was
        settled by an earlier frame in the same batch."""
        coord = _coord({1: "success", 2: "success", 3: "reject", 4: "success", 5: "success"})
        commands = coord.plan()
        assert [c.delivery_tag for c in commands] == sorted(c.delivery_tag for c in commands)
        assert _emitted(commands)[1] == ("reject", 3, False)

    def test_a_blocker_after_a_nack_still_stops_coalescing(self) -> None:
        coord = _coord({1: "success", 2: "nack", 3: "outstanding", 4: "success", 5: "success"})
        commands = coord.plan()
        assert 3 not in _covered(commands)
        assert not any(c.multiple for c in commands)

    @pytest.mark.parametrize("requeue", [True, False])
    def test_nack_requeue_flag_is_carried_through(self, requeue: bool) -> None:
        coord = SettlementCoordinator()
        coord.register(1)
        coord.mark_nack(1, requeue=requeue)
        (command,) = coord.plan()
        assert command.kind is SettlementAction.NACK and command.requeue is requeue

    @pytest.mark.parametrize("requeue", [True, False])
    def test_reject_requeue_flag_is_carried_through(self, requeue: bool) -> None:
        coord = SettlementCoordinator()
        coord.register(1)
        coord.mark_reject(1, requeue=requeue)
        (command,) = coord.plan()
        assert command.kind is SettlementAction.REJECT and command.requeue is requeue


# ── item 5: requeue and redelivery ────────────────────────────────────────


class TestRequeueAndRedelivery:
    def test_a_redelivered_message_is_a_new_delivery(self) -> None:
        """After nack(102, requeue=True) the broker redelivers with a NEW tag.
        The old tag must never be reused for it."""
        coord = _coord({101: "success", 102: "nack", 103: "success"})
        coord.plan()
        coord.register(104)  # the redelivery
        coord.mark_success(104)
        assert _emitted(coord.plan()) == [("ack", 104, False)]

    def test_the_old_tag_can_never_be_re_registered(self) -> None:
        coord = _coord({101: "success", 102: "nack"})
        coord.plan()
        coord.register(103)
        with pytest.raises(CoordinatorError, match="not greater than last registered"):
            coord.register(102)

    def test_registration_must_be_strictly_increasing(self) -> None:
        coord = SettlementCoordinator()
        coord.register(5)
        for tag in (5, 4, 0, -1):
            with pytest.raises(CoordinatorError):
                coord.register(tag)


# ── I6: shutdown ──────────────────────────────────────────────────────────


class TestI6Shutdown:
    def test_drain_emits_approved_work_and_leaves_the_rest_unacked(self) -> None:
        """101 DONE, 102 DONE, 103 RUNNING, 104 DONE, 105 DONE at shutdown:
        ack through 102 and stop — never ack 103's siblings past it."""
        coord = _coord({101: "success", 102: "success", 103: "outstanding", 104: "success", 105: "success"})
        commands = coord.drain_plan()
        assert 103 not in _covered(commands)
        assert coord.state_of(103) is DeliveryState.OUTSTANDING
        assert coord.pending == 1

    def test_drain_never_coalesces(self) -> None:
        coord = _coord({1: "success", 2: "success", 3: "success"})
        assert not any(c.multiple for c in coord.drain_plan())

    @pytest.mark.parametrize("state", BLOCKING)
    def test_drain_never_acks_merely_to_empty_the_ledger(self, state: str) -> None:
        coord = _coord({1: state, 2: state})
        assert coord.drain_plan() == []
        assert coord.pending == 2


# ── I7: bounds apply backpressure, never relax the ack rules ──────────────


class TestI7Bounds:
    def test_unbounded_by_default(self) -> None:
        coord = SettlementCoordinator()
        assert coord.max_pending == 0
        for tag in range(1, 501):
            coord.register(tag)
        assert coord.pending == 500

    def test_register_over_the_cap_raises(self) -> None:
        coord = SettlementCoordinator(max_pending=3)
        for tag in (1, 2, 3):
            coord.register(tag)
        with pytest.raises(LedgerFullError, match="backpressure"):
            coord.register(4)

    def test_a_full_ledger_never_relaxes_the_ack_rules(self) -> None:
        coord = SettlementCoordinator(max_pending=3)
        for tag in (1, 2, 3):
            coord.register(tag)
        coord.mark_success(2)
        coord.mark_success(3)
        with pytest.raises(LedgerFullError):
            coord.register(4)
        commands = coord.plan()
        assert 1 not in _covered(commands), "tag 1 is still running — pressure must not ack it"
        assert not any(c.multiple for c in commands)

    def test_settling_frees_capacity_again(self) -> None:
        coord = SettlementCoordinator(max_pending=2)
        coord.register(1)
        coord.register(2)
        coord.mark_success(1)
        coord.mark_success(2)
        coord.plan()
        coord.register(3)  # no longer full
        assert coord.pending == 1


# ── I8: coalescing changes frames, not semantics ──────────────────────────


class TestI8SemanticsUnchanged:
    LEDGERS: ClassVar[list[dict[int, str]]] = [
        {1: "success", 2: "success", 3: "success"},
        {1: "success", 2: "outstanding", 3: "success"},
        {1: "success", 2: "nack", 3: "success", 4: "success"},
        {1: "reject", 2: "success", 3: "success", 4: "failed", 5: "success"},
        {1: "cancelled", 2: "success", 3: "success"},
        {1: "retry_pending", 2: "success"},
    ]

    @pytest.mark.parametrize("ledger", LEDGERS)
    def test_same_tags_settled_with_and_without_coalescing(self, ledger: dict[int, str]) -> None:
        on = _coord(ledger, coalesce=True)
        off = _coord(ledger, coalesce=False)
        assert _covered(on.plan()) == _covered(off.plan())

    @pytest.mark.parametrize("ledger", LEDGERS)
    def test_same_settlement_kind_per_tag(self, ledger: dict[int, str]) -> None:
        def kinds(coord: SettlementCoordinator) -> dict[int, str]:
            return {tag: c.kind.value for c in coord.plan() for tag in c.covers}

        assert kinds(_coord(ledger, coalesce=True)) == kinds(_coord(ledger, coalesce=False))

    def test_coalescing_only_reduces_frame_count(self) -> None:
        ledger = dict.fromkeys(range(1, 21), "success")
        on, off = _coord(ledger, coalesce=True), _coord(ledger, coalesce=False)
        assert len(on.plan()) == 1 and len(off.plan()) == 20

    @pytest.mark.parametrize("ledger", LEDGERS)
    def test_a_tag_is_never_settled_twice(self, ledger: dict[int, str]) -> None:
        coord = _coord(ledger)
        commands = coord.plan()
        covered = [tag for c in commands for tag in c.covers]
        assert len(covered) == len(set(covered))


# ── item 16: instrumentation ──────────────────────────────────────────────


class TestInstrumentation:
    def test_stats_expose_every_documented_signal(self) -> None:
        coord = _coord({1: "success", 2: "outstanding", 3: "success"})
        stats = coord.stats
        assert set(stats) == {
            "registered",
            "pending",
            "outstanding",
            "ack_ready",
            "frontier",
            "settled",
            "coalesced",
            "frames_sent",
            "coalescing_ratio",
            "invalidations",
            "dropped_on_invalidate",
            "oldest_pending_age",
        }
        assert stats["registered"] == 3 and stats["ack_ready"] == 2 and stats["frontier"] == 1

    def test_coalescing_ratio_matches_the_benchmark_shape(self) -> None:
        """2000 deliveries settled in 20 frames → ratio 100."""
        coord = SettlementCoordinator()
        for batch in range(20):
            for tag in range(batch * 100 + 1, batch * 100 + 101):
                coord.register(tag)
                coord.mark_success(tag)
            coord.plan()
        stats = coord.stats
        assert stats["settled"] == 2000
        assert stats["frames_sent"] == 20
        assert stats["coalescing_ratio"] == 100.0

    def test_ratio_is_one_without_coalescing(self) -> None:
        coord = SettlementCoordinator(coalesce=False)
        for tag in range(1, 11):
            coord.register(tag)
            coord.mark_success(tag)
        coord.plan()
        assert coord.stats["coalescing_ratio"] == 1.0

    def test_oldest_pending_age_tracks_the_straggler(self) -> None:
        now = [1000.0]
        coord = SettlementCoordinator(clock=lambda: now[0])
        coord.register(1)
        now[0] += 30.0
        coord.register(2)
        assert coord.oldest_pending_age == pytest.approx(30.0)
        coord.mark_success(1)
        coord.plan()  # the old one is settled; age now reflects tag 2
        assert coord.oldest_pending_age == pytest.approx(0.0)

    def test_empty_ledger_reports_zero_age(self) -> None:
        assert SettlementCoordinator().oldest_pending_age == 0.0

    def test_gap_count_counts_stranding_blockers(self) -> None:
        coord = _coord({1: "outstanding", 2: "success", 3: "failed", 4: "success", 5: "outstanding"})
        assert coord.gap_count == 2  # tags 1 and 3 sit below the ack-ready tag 4


# ── item 11/12: serialization under concurrency ───────────────────────────


class TestConcurrentMutation:
    def test_concurrent_completion_never_over_acks(self) -> None:
        """Many workers complete concurrently while a planner drains. The
        straggler must never be covered, and no tag settled twice."""
        coord = SettlementCoordinator(max_hold=1)
        total = 400
        for tag in range(1, total + 1):
            coord.register(tag)
        straggler = 1
        emitted: list[SettlementCommand] = []
        emit_lock = threading.Lock()
        stop = threading.Event()
        barrier = threading.Barrier(5)

        def completer(offset: int) -> None:
            barrier.wait()
            for tag in range(2 + offset, total + 1, 4):
                coord.mark_success(tag)

        def planner() -> None:
            barrier.wait()
            while not stop.is_set():
                commands = coord.plan()
                with emit_lock:
                    emitted.extend(commands)

        threads = [threading.Thread(target=completer, args=(i,)) for i in range(4)]
        planner_thread = threading.Thread(target=planner)
        planner_thread.start()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        stop.set()
        planner_thread.join()
        emitted.extend(coord.plan())

        covered = [tag for c in emitted for tag in c.covers]
        assert straggler not in covered, "the unfinished delivery was acked"
        assert len(covered) == len(set(covered)), "a tag was settled twice"
        assert set(covered) == set(range(2, total + 1))
        for command in emitted:
            if command.multiple:
                assert straggler not in command.covers

    def test_concurrent_plan_and_invalidate_never_emit_stale_tags(self) -> None:
        coord = SettlementCoordinator()
        emitted: list[SettlementCommand] = []
        emit_lock = threading.Lock()
        generations: list[int] = []

        def worker() -> None:
            for _ in range(200):
                try:
                    tag = coord._last_registered + 1
                    coord.register(tag)
                    coord.mark_success(tag)
                except CoordinatorError:
                    continue
                commands = coord.plan()
                with emit_lock:
                    emitted.extend(commands)

        def invalidator() -> None:
            for _ in range(50):
                coord.invalidate()
                generations.append(coord.generation)

        threads = [threading.Thread(target=worker) for _ in range(3)] + [threading.Thread(target=invalidator)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Every emitted command must name the generation it was planned in,
        # and no tag may be emitted twice within one generation.
        per_generation: dict[int, list[int]] = {}
        for command in emitted:
            per_generation.setdefault(command.generation, []).extend(command.covers)
        for generation, tags in per_generation.items():
            assert len(tags) == len(set(tags)), f"duplicate settlement in generation {generation}"

    def test_contradiction_detection_holds_under_races(self) -> None:
        coord = SettlementCoordinator()
        coord.register(1)
        outcomes: list[str] = []
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def try_ack() -> None:
            barrier.wait()
            try:
                coord.mark_success(1)
                with lock:
                    outcomes.append("ack")
            except CoordinatorError:
                with lock:
                    outcomes.append("ack-refused")

        def try_nack() -> None:
            barrier.wait()
            try:
                coord.mark_nack(1)
                with lock:
                    outcomes.append("nack")
            except CoordinatorError:
                with lock:
                    outcomes.append("nack-refused")

        ack_thread, nack_thread = threading.Thread(target=try_ack), threading.Thread(target=try_nack)
        ack_thread.start()
        nack_thread.start()
        ack_thread.join()
        nack_thread.join()

        # Exactly one decision wins; the other is refused. Never both.
        assert sorted(outcomes) in (["ack", "nack-refused"], ["ack-refused", "nack"])
        assert coord.state_of(1) in (DeliveryState.SUCCESS, DeliveryState.NACK)
