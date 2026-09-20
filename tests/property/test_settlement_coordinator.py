"""Property / state-machine tests for SettlementCoordinator (plan §14).

Invariant under ANY interleaving of register / complete / fail / retry-pending /
plan / invalidate:

1. A cumulative ack (``multiple=True``) never covers a tag that was not
   approved for success — i.e. it never settles unfinished or retry-pending
   work.
2. Every tag is settled at most once, and only after it was registered in the
   current generation.
3. After ``invalidate()`` no tag from the old generation is ever emitted.
4. Every approved (success/nack/reject) tag is eventually emitted by a
   ``drain_plan()``; blocking tags (outstanding, retry-pending, failed,
   cancelled) never are.
5. Commands always come back in ascending tag order — a cumulative ack is
   only safe because the frames below it go out first.
6. Repeating a decision is an idempotent no-op; contradicting one always
   raises; an unregistered tag always raises.
"""

from __future__ import annotations

import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from rabbitkit.core.settlement import (
    ContradictorySettlementError,
    CoordinatorError,
    SettlementCommand,
    SettlementCoordinator,
)
from rabbitkit.core.types import DeliveryState, SettlementAction


class CoordinatorMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.coord: SettlementCoordinator
        self.next_tag = 1
        # model: tag -> state within the CURRENT generation
        self.model: dict[int, DeliveryState] = {}
        self.requeue: dict[int, bool] = {}
        self.emitted: set[tuple[int, int]] = set()  # (generation, tag)
        self.old_generation_tags: set[tuple[int, int]] = set()
        self.commands: list[SettlementCommand] = []

    @initialize(coalesce=st.booleans(), max_hold=st.integers(min_value=0, max_value=3))
    def setup(self, coalesce: bool, max_hold: int) -> None:
        self.coord = SettlementCoordinator(coalesce=coalesce, max_hold=max_hold)

    # ── actions ──

    @rule(n=st.integers(min_value=1, max_value=4))
    def register(self, n: int) -> None:
        for _ in range(n):
            self.coord.register(self.next_tag)
            self.model[self.next_tag] = DeliveryState.OUTSTANDING
            self.next_tag += 1

    def _pick(self, data: st.DataObject, states: tuple[DeliveryState, ...]) -> int | None:
        candidates = [t for t, s in self.model.items() if s in states]
        if not candidates:
            return None
        return data.draw(st.sampled_from(candidates))

    @rule(data=st.data())
    def complete(self, data: st.DataObject) -> None:
        t = self._pick(data, (DeliveryState.OUTSTANDING, DeliveryState.RETRY_PENDING))
        if t is None:
            return
        self.coord.mark_success(t)
        self.model[t] = DeliveryState.SUCCESS

    @rule(data=st.data(), requeue=st.booleans())
    def fail(self, data: st.DataObject, requeue: bool) -> None:
        t = self._pick(data, (DeliveryState.OUTSTANDING, DeliveryState.RETRY_PENDING))
        if t is None:
            return
        self.coord.mark_nack(t, requeue=requeue)
        self.model[t] = DeliveryState.NACK
        self.requeue[t] = requeue

    @rule(data=st.data())
    def reject(self, data: st.DataObject) -> None:
        t = self._pick(data, (DeliveryState.OUTSTANDING, DeliveryState.RETRY_PENDING))
        if t is None:
            return
        self.coord.mark_reject(t)
        self.model[t] = DeliveryState.REJECT
        self.requeue[t] = False

    @rule(data=st.data())
    def retry_pending(self, data: st.DataObject) -> None:
        t = self._pick(data, (DeliveryState.OUTSTANDING,))
        if t is None:
            return
        self.coord.mark_retry_pending(t)
        self.model[t] = DeliveryState.RETRY_PENDING

    @rule(data=st.data())
    def abandon(self, data: st.DataObject) -> None:
        """Handler failed with no settlement decision — must never be acked."""
        t = self._pick(data, (DeliveryState.OUTSTANDING, DeliveryState.RETRY_PENDING))
        if t is None:
            return
        self.coord.mark_failed(t)
        self.model[t] = DeliveryState.FAILED

    @rule(data=st.data())
    def cancel(self, data: st.DataObject) -> None:
        """Handler cancelled (shutdown/timeout) — must never be acked."""
        t = self._pick(data, (DeliveryState.OUTSTANDING, DeliveryState.RETRY_PENDING))
        if t is None:
            return
        self.coord.mark_cancelled(t)
        self.model[t] = DeliveryState.CANCELLED

    @rule(data=st.data())
    def settle_abandoned(self, data: st.DataObject) -> None:
        """A FAILED/CANCELLED delivery may still be nacked explicitly — but
        never acked (the coordinator refuses that transition)."""
        t = self._pick(data, (DeliveryState.FAILED, DeliveryState.CANCELLED))
        if t is None:
            return
        with pytest.raises(ContradictorySettlementError):
            self.coord.mark_success(t)
        self.coord.mark_nack(t, requeue=True)
        self.model[t] = DeliveryState.NACK
        self.requeue[t] = True

    @rule(data=st.data())
    def repeat_decision(self, data: st.DataObject) -> None:
        """Repeating the SAME decision is an idempotent no-op; the model must
        not change and no extra frame may ever be produced."""
        t = self._pick(data, (DeliveryState.SUCCESS, DeliveryState.NACK, DeliveryState.REJECT))
        if t is None:
            return
        state = self.model[t]
        if state is DeliveryState.SUCCESS:
            self.coord.mark_success(t)
        elif state is DeliveryState.NACK:
            self.coord.mark_nack(t, requeue=self.requeue[t])
        else:
            self.coord.mark_reject(t, requeue=self.requeue[t])
        assert self.coord.state_of(t) is state

    @rule(data=st.data())
    def contradict(self, data: st.DataObject) -> None:
        """A contradictory decision must always be refused, never silently
        applied (I4)."""
        t = self._pick(data, (DeliveryState.SUCCESS,))
        if t is None:
            return
        with pytest.raises(ContradictorySettlementError):
            self.coord.mark_nack(t)
        assert self.coord.state_of(t) is DeliveryState.SUCCESS

    @rule(data=st.data())
    def settle_unknown(self, data: st.DataObject) -> None:
        """An unregistered tag can never enter the ledger (I3/item 7)."""
        tag = self.next_tag + data.draw(st.integers(min_value=1000, max_value=2000))
        with pytest.raises(CoordinatorError):
            self.coord.mark_success(tag)

    @rule(data=st.data())
    def release_externally(self, data: st.DataObject) -> None:
        t = self._pick(data, (DeliveryState.RETRY_PENDING,))
        if t is None:
            return
        self.coord.release(t)
        del self.model[t]
        self.requeue.pop(t, None)

    def _apply(self, cmds: list[SettlementCommand]) -> None:
        gen = self.coord.generation
        assert [c.delivery_tag for c in cmds] == sorted(c.delivery_tag for c in cmds), (
            "commands must be emitted in ascending tag order — a cumulative ack is only "
            "safe because the frames below it went out first"
        )
        for c in cmds:
            self.commands.append(c)
            for t in c.covers:
                key = (gen, t)
                assert key not in self.emitted, f"tag {t} settled twice"
                assert t in self.model, f"tag {t} emitted but not registered/pending"
                state = self.model.pop(t)
                self.requeue.pop(t, None)
                if c.kind is SettlementAction.ACK:
                    assert state is DeliveryState.SUCCESS, f"ack covered tag {t} in state {state}"
                elif c.kind is SettlementAction.NACK:
                    assert state is DeliveryState.NACK
                else:
                    assert state is DeliveryState.REJECT
                self.emitted.add(key)
            if c.multiple:
                assert c.kind is SettlementAction.ACK
                assert len(c.covers) >= 2
                assert c.delivery_tag == max(c.covers)
                # THE invariant: commands are emitted in ascending tag order,
                # so by the time this cumulative ack reaches the broker every
                # lower tag has already been settled by an earlier frame.
                # Nothing unsettled may remain below it.
                for t in list(self.model):
                    assert t > c.delivery_tag, f"cumulative ack {c.delivery_tag} skipped unfinished tag {t}"

    @rule()
    def plan(self) -> None:
        self._apply(self.coord.plan())

    @rule()
    def drain(self) -> None:
        self._apply(self.coord.drain_plan())
        # I6: a drain emits every APPROVED tag and leaves every blocking one
        # unacked — outstanding, retry-pending, failed and cancelled alike.
        for t, s in self.model.items():
            assert s.blocks_frontier, f"{t} in {s} survived drain but is emittable"

    @rule()
    def reconnect(self) -> None:
        gen = self.coord.generation
        for t in self.model:
            self.old_generation_tags.add((gen, t))
        dropped = self.coord.invalidate()
        assert set(dropped) == set(self.model)
        self.model.clear()
        self.requeue.clear()
        self.next_tag = 1

    # ── invariants ──

    @invariant()
    def ledger_matches_model(self) -> None:
        assert self.coord.pending == len(self.model)
        for t, s in self.model.items():
            assert self.coord.state_of(t) is s

    @invariant()
    def frontier_matches_the_leading_success_run(self) -> None:
        """The frontier is the end of the leading ack-safe run, never the
        highest completed tag."""
        expected = 0
        for t in sorted(self.model):
            if self.model[t] is DeliveryState.SUCCESS:
                expected = t
            else:
                break
        assert self.coord.frontier == expected

    @invariant()
    def never_ack_ready_for_a_blocking_state(self) -> None:
        blocking = {s for s in DeliveryState if s.blocks_frontier}
        for t, state in self.model.items():
            if state in blocking:
                assert not self.coord.state_of(t).is_ack_safe, t

    @invariant()
    def old_generation_never_emitted(self) -> None:
        assert not (self.emitted & self.old_generation_tags)


TestCoordinatorStateMachine = CoordinatorMachine.TestCase
# ~300 sequences x 60 steps = ~18k random operations per run; the machine
# asserts the safety invariants after EVERY one of them.
TestCoordinatorStateMachine.settings = settings(max_examples=300, stateful_step_count=60, deadline=None)
