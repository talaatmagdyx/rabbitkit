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
   ``drain_plan()``; outstanding / retry-pending tags never are.
"""

from __future__ import annotations

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from rabbitkit.core.settlement import SettlementCommand, SettlementCoordinator
from rabbitkit.core.types import DeliveryState, SettlementAction


class CoordinatorMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.coord: SettlementCoordinator
        self.next_tag = 1
        # model: tag -> state within the CURRENT generation
        self.model: dict[int, DeliveryState] = {}
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

    @rule(data=st.data())
    def reject(self, data: st.DataObject) -> None:
        t = self._pick(data, (DeliveryState.OUTSTANDING, DeliveryState.RETRY_PENDING))
        if t is None:
            return
        self.coord.mark_reject(t)
        self.model[t] = DeliveryState.REJECT

    @rule(data=st.data())
    def retry_pending(self, data: st.DataObject) -> None:
        t = self._pick(data, (DeliveryState.OUTSTANDING,))
        if t is None:
            return
        self.coord.mark_retry_pending(t)
        self.model[t] = DeliveryState.RETRY_PENDING

    @rule(data=st.data())
    def release_externally(self, data: st.DataObject) -> None:
        t = self._pick(data, (DeliveryState.RETRY_PENDING,))
        if t is None:
            return
        self.coord.release(t)
        del self.model[t]

    def _apply(self, cmds: list[SettlementCommand]) -> None:
        gen = self.coord.generation
        for c in cmds:
            self.commands.append(c)
            for t in c.covers:
                key = (gen, t)
                assert key not in self.emitted, f"tag {t} settled twice"
                assert t in self.model, f"tag {t} emitted but not registered/pending"
                state = self.model.pop(t)
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
                # everything <= delivery_tag that is still pending in the
                # model must have been covered (no unsettled lower tag)
                for t in list(self.model):
                    assert t > c.delivery_tag, f"cumulative ack {c.delivery_tag} skipped unfinished tag {t}"

    @rule()
    def plan(self) -> None:
        self._apply(self.coord.plan())

    @rule()
    def drain(self) -> None:
        self._apply(self.coord.drain_plan())
        # after a drain every approved tag is gone
        for t, s in self.model.items():
            assert s in (DeliveryState.OUTSTANDING, DeliveryState.RETRY_PENDING), f"{t} in {s} survived drain"

    @rule()
    def reconnect(self) -> None:
        gen = self.coord.generation
        for t in self.model:
            self.old_generation_tags.add((gen, t))
        dropped = self.coord.invalidate()
        assert set(dropped) == set(self.model)
        self.model.clear()
        self.next_tag = 1

    # ── invariants ──

    @invariant()
    def ledger_matches_model(self) -> None:
        assert self.coord.pending == len(self.model)
        for t, s in self.model.items():
            assert self.coord.state_of(t) is s

    @invariant()
    def old_generation_never_emitted(self) -> None:
        assert not (self.emitted & self.old_generation_tags)


TestCoordinatorStateMachine = CoordinatorMachine.TestCase
TestCoordinatorStateMachine.settings = settings(max_examples=150, stateful_step_count=40, deadline=None)
