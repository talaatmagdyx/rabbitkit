"""The plan -> emit -> commit protocol, and the wire failure it exists for.

Before this protocol, ``plan()`` removed deliveries from the ledger at
planning time and the executor continued after a failed frame. Those two
choices combine into message loss:

    101 SUCCESS  102 SUCCESS  103 NACK  104 SUCCESS  105 SUCCESS
    -> ack(102, multiple=True), nack(103), ack(105, multiple=True)

``basic_ack(105, multiple=True)`` acknowledges every delivery still
unacknowledged up to tag 105. If the nack never reached the broker, that
includes 103 -- the delivery that was supposed to be requeued. An intended
NACK silently becomes an ACK and the work is lost.

The rule is therefore: a command whose safety depends on an earlier frame
must never be emitted once that earlier frame failed.
"""

from __future__ import annotations

from typing import Any

import pytest

from rabbitkit.core.settlement import (
    SettlementCoordinator,
    emit_batch,
)
from rabbitkit.core.types import SettlementAction

# ── helpers ───────────────────────────────────────────────────────────────


class Wire:
    """Records every settlement frame, and can fail a chosen one."""

    def __init__(self, fail_at: int | None = None, exc: BaseException | None = None) -> None:
        self.calls: list[tuple[str, int, bool]] = []
        self._fail_at = fail_at
        self._exc = exc or RuntimeError("wire failure")
        self._n = 0

    def _record(self, kind: str, tag: int, flag: bool) -> None:
        if self._fail_at is not None and self._n == self._fail_at:
            self._n += 1
            self.calls.append((kind, tag, flag))
            raise self._exc
        self._n += 1
        self.calls.append((kind, tag, flag))

    def ack(self, tag: int, multiple: bool) -> None:
        self._record("ack", tag, multiple)

    def nack(self, tag: int, requeue: bool) -> None:
        self._record("nack", tag, requeue)

    def reject(self, tag: int, requeue: bool) -> None:
        self._record("reject", tag, requeue)

    @property
    def cumulative_acks(self) -> list[tuple[str, int, bool]]:
        return [c for c in self.calls if c[0] == "ack" and c[2] is True]


def _five_with_nack_in_the_middle() -> SettlementCoordinator:
    coord = SettlementCoordinator()
    for tag in (101, 102, 103, 104, 105):
        coord.register(tag)
    coord.mark_success(101)
    coord.mark_success(102)
    coord.mark_nack(103, requeue=True)
    coord.mark_success(104)
    coord.mark_success(105)
    return coord


def _mixed_five() -> SettlementCoordinator:
    """A plan with one of every command kind, for the position sweep."""
    coord = SettlementCoordinator()
    for tag in range(1, 8):
        coord.register(tag)
    coord.mark_success(1)
    coord.mark_success(2)  # -> ack(2, multiple=True)
    coord.mark_nack(3, requeue=True)  # -> nack(3)
    coord.mark_success(4)  # -> ack(4)
    coord.mark_reject(5, requeue=False)  # -> reject(5)
    coord.mark_success(6)
    coord.mark_success(7)  # -> ack(7, multiple=True)
    return coord


# ── the headline regression ───────────────────────────────────────────────


class TestTheOverAckRegression:
    def test_the_plan_still_coalesces_around_the_nack(self) -> None:
        """The efficient shape is kept; only its execution is made safe."""
        batch = _five_with_nack_in_the_middle().prepare()
        assert [(c.kind, c.delivery_tag, c.multiple) for c in batch.commands] == [
            (SettlementAction.ACK, 102, True),
            (SettlementAction.NACK, 103, False),
            (SettlementAction.ACK, 105, True),
        ]

    def test_a_cumulative_ack_never_follows_a_failed_nack(self) -> None:
        """THE regression test. The exact wire calls, not a count."""
        coord = _five_with_nack_in_the_middle()
        batch = coord.prepare()
        wire = Wire(fail_at=1)  # the nack

        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)

        assert wire.calls == [("ack", 102, True), ("nack", 103, True)]
        assert ("ack", 105, True) not in wire.calls, "ack(105, multiple=True) would have swallowed tag 103"
        assert report.failed is not None
        assert report.failed.delivery_tag == 103
        assert [c.delivery_tag for c in report.not_attempted] == [105]

    def test_when_every_frame_lands_all_three_go_out(self) -> None:
        coord = _five_with_nack_in_the_middle()
        batch = coord.prepare()
        wire = Wire()

        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)

        assert wire.calls == [("ack", 102, True), ("nack", 103, True), ("ack", 105, True)]
        assert report.ok
        assert report.settled_tags == 5
        assert coord.pending == 0


# ── failure injected at every position (review item 16) ───────────────────


class TestFailureAtEveryPosition:
    @pytest.mark.parametrize("fail_at", [0, 1, 2, 3, 4])
    def test_nothing_after_the_failure_is_emitted(self, fail_at: int) -> None:
        coord = _mixed_five()
        batch = coord.prepare()
        assert len(batch) == 5, "the fixture must produce a five-command plan"
        wire = Wire(fail_at=fail_at)

        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)

        assert len(wire.calls) == fail_at + 1, "emission must stop at the failing frame"
        assert len(report.emitted) == fail_at
        assert report.failed is batch.commands[fail_at]
        assert report.not_attempted == batch.commands[fail_at + 1 :]

    @pytest.mark.parametrize("fail_at", [0, 1, 2, 3, 4])
    def test_no_tag_is_ever_settled_by_a_frame_that_did_not_go_out(self, fail_at: int) -> None:
        coord = _mixed_five()
        batch = coord.prepare()
        wire = Wire(fail_at=fail_at)

        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)

        settled = {t for c in report.emitted for t in c.covers}
        withheld = {t for c in report.not_attempted for t in c.covers}
        assert settled.isdisjoint(withheld)
        assert settled.isdisjoint(set(report.unresolved_tags))

    @pytest.mark.parametrize("fail_at", [0, 1, 2, 3, 4])
    def test_a_failed_settlement_is_never_retried(self, fail_at: int) -> None:
        """Re-sending a frame whose first attempt may have landed turns an
        ambiguity into a protocol error. Unresolved tags leave the ledger."""
        coord = _mixed_five()
        batch = coord.prepare()
        wire = Wire(fail_at=fail_at)
        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)

        next_batch = coord.prepare()
        replanned = {t for c in next_batch.commands for t in c.covers}
        assert replanned.isdisjoint(set(report.unresolved_tags))


# ── what a failure does to the generation ─────────────────────────────────


class TestInvalidationRules:
    def test_a_failed_nack_invalidates_the_generation(self) -> None:
        """The tag may still be unacknowledged, so no later cumulative ack on
        this channel can be trusted. Burn the generation."""
        coord = _five_with_nack_in_the_middle()
        gen = coord.generation
        batch = coord.prepare()
        wire = Wire(fail_at=1)

        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)

        assert report.invalidated is True
        assert coord.generation == gen + 1
        assert coord.pending == 0

    def test_a_failed_reject_also_invalidates(self) -> None:
        coord = SettlementCoordinator()
        for tag in (1, 2):
            coord.register(tag)
        coord.mark_reject(1, requeue=False)
        coord.mark_success(2)
        batch = coord.prepare()
        wire = Wire(fail_at=0)

        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)
        assert report.invalidated is True

    def test_a_failed_ack_does_not_invalidate(self) -> None:
        """An ambiguous ACK is benign: if a later cumulative ack sweeps those
        tags up, the outcome is the one that was intended anyway."""
        coord = SettlementCoordinator()
        for tag in (1, 2, 3):
            coord.register(tag)
        coord.mark_success(1)
        coord.mark_success(2)
        coord.mark_success(3)
        gen = coord.generation
        batch = coord.prepare()
        wire = Wire(fail_at=0)

        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)

        assert report.invalidated is False
        assert coord.generation == gen


# ── prepare does not commit ───────────────────────────────────────────────


class TestPrepareDoesNotSettle:
    def test_the_ledger_is_untouched_until_a_frame_lands(self) -> None:
        coord = _five_with_nack_in_the_middle()
        assert coord.pending == 5
        batch = coord.prepare()
        assert coord.pending == 5, "prepare() must not settle anything"
        assert len(batch) == 3

    def test_committing_one_command_settles_only_its_tags(self) -> None:
        coord = _five_with_nack_in_the_middle()
        batch = coord.prepare()
        coord.commit_command(batch, batch.commands[0])  # ack(102, multiple=True)
        assert coord.pending == 3
        assert coord.state_of(101) is None
        assert coord.state_of(102) is None
        assert coord.state_of(103) is not None

    def test_reserved_tags_are_not_planned_twice(self) -> None:
        coord = _five_with_nack_in_the_middle()
        first = coord.prepare()
        second = coord.prepare()
        assert first.commands, "the first prepare must produce work"
        assert second.commands == (), "everything is already reserved"

    def test_released_tags_come_back_on_the_next_prepare(self) -> None:
        coord = _five_with_nack_in_the_middle()
        batch = coord.prepare()
        coord.release_commands(batch, batch.commands)
        again = coord.prepare()
        assert [c.delivery_tag for c in again.commands] == [c.delivery_tag for c in batch.commands]

    def test_a_stale_batch_cannot_settle_anything(self) -> None:
        """The channel was rebuilt mid-batch: commit/fail/release are no-ops."""
        coord = _five_with_nack_in_the_middle()
        batch = coord.prepare()
        coord.invalidate()
        for tag in (201, 202):
            coord.register(tag)
            coord.mark_success(tag)

        coord.commit_command(batch, batch.commands[0])
        assert coord.pending == 2, "the new generation's deliveries must survive"
        assert coord.fail_command(batch, batch.commands[1], RuntimeError("x")) is False


# ── accounting stays honest (review item 7) ───────────────────────────────


class TestMetricsDistinguishPlannedFromSettled:
    def test_planned_is_not_counted_as_settled(self) -> None:
        coord = _five_with_nack_in_the_middle()
        batch = coord.prepare()
        wire = Wire(fail_at=1)
        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)

        assert len(batch) == 3, "three frames planned"
        assert report.settled_tags == 2, "only ack(102, multiple=True) landed"
        assert report.unresolved_tags == (103,)
        assert len(report.not_attempted) == 1

    def test_stats_separate_failed_frames_from_sent_ones(self) -> None:
        coord = _mixed_five()
        batch = coord.prepare()
        wire = Wire(fail_at=2)
        emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)

        stats = coord.stats
        assert stats["frames_failed"] == 1
        assert stats["unresolved"] >= 1
        assert stats["frames_sent"] == 2, "two frames actually reached the broker"

    def test_a_clean_run_reports_full_coalescing(self) -> None:
        coord = _five_with_nack_in_the_middle()
        batch = coord.prepare()
        wire = Wire()
        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)
        assert report.coalesced_tags == 2, "two cumulative acks each cover 2 tags => 1 extra each"
        assert report.ok


# ── the batch value type ──────────────────────────────────────────────────


class TestSettlementBatch:
    def test_covered_tags_are_sorted_and_complete(self) -> None:
        batch = _five_with_nack_in_the_middle().prepare()
        assert batch.covered_tags == (101, 102, 103, 104, 105)

    def test_an_empty_ledger_yields_a_falsey_batch(self) -> None:
        batch = SettlementCoordinator().prepare()
        assert not batch
        assert len(batch) == 0

    def test_a_populated_batch_is_truthy(self) -> None:
        assert _five_with_nack_in_the_middle().prepare()

    def test_every_command_carries_the_generation(self) -> None:
        coord = _five_with_nack_in_the_middle()
        coord.invalidate()
        for tag in (900, 901):
            coord.register(tag)
            coord.mark_success(tag)
        batch = coord.prepare()
        assert batch.generation == coord.generation
        assert all(c.generation == coord.generation for c in batch.commands)


# ── emit_batch guards ─────────────────────────────────────────────────────


class TestEmitBatchEdges:
    def test_an_empty_batch_is_a_clean_no_op(self) -> None:
        coord = SettlementCoordinator()
        batch = coord.prepare()
        wire = Wire()
        report = emit_batch(batch, coord, ack=wire.ack, nack=wire.nack, reject=wire.reject)
        assert report.ok
        assert wire.calls == []

    def test_a_base_exception_is_not_swallowed(self) -> None:
        """KeyboardInterrupt must propagate, not be recorded as a wire error."""
        coord = _five_with_nack_in_the_middle()
        batch = coord.prepare()

        def boom(*_: Any) -> None:
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            emit_batch(batch, coord, ack=boom, nack=boom, reject=boom)

    def test_drain_prepares_individual_frames_only(self) -> None:
        coord = _five_with_nack_in_the_middle()
        batch = coord.prepare_drain()
        assert all(not c.multiple for c in batch.commands), "shutdown never waits for a prefix"
