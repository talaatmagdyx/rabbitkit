"""Per-channel ack isolation — the ``channel_key`` guard and CoalescingAckerGroup.

    Channel A → CoalescingAcker A → SettlementCoordinator A
    Channel B → CoalescingAcker B → SettlementCoordinator B

Delivery tags are a PER-CHANNEL counter, so tag 7 on A and tag 7 on B are
different messages. These tests pin that one ledger never settles another
channel's deliveries, and that mixing channels fails loudly at registration
instead of silently acking the wrong messages.
"""

from __future__ import annotations

import threading
from typing import Any

import pytest

from rabbitkit.core.config import BatchAckConfig
from rabbitkit.core.settlement import CoordinatorError
from rabbitkit.core.types import FlushReason
from rabbitkit.highload.batch import (
    BatchClosedError,
    ChannelMismatchError,
    CoalescingAcker,
    CoalescingAckerGroup,
)


class FakeChannel:
    """Stands in for a pika/aio-pika channel: identity-hashable, records frames."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.frames: list[tuple[str, int, bool]] = []

    def ack(self, tag: int, multiple: bool) -> None:
        self.frames.append(("ack", tag, multiple))

    def nack(self, tag: int, requeue: bool) -> None:
        self.frames.append(("nack", tag, requeue))

    def reject(self, tag: int, requeue: bool) -> None:
        self.frames.append(("reject", tag, requeue))

    def __repr__(self) -> str:
        return f"<FakeChannel {self.name}>"


def _acker(channel: FakeChannel, **kw: Any) -> CoalescingAcker:
    kw.setdefault("config", BatchAckConfig(batch_size=1000, flush_interval_ms=0))
    kw.setdefault("channel_key", channel)
    # max_hold=0 keeps flushes deterministic: a success stranded behind an
    # unfinished sibling is acked individually on the FIRST flush instead of
    # being held for a round hoping the prefix completes. TestHoldIsPerChannel
    # covers the holding behavior explicitly.
    kw.setdefault("max_hold", 0)
    return CoalescingAcker(
        ack_fn=channel.ack,
        nack_fn=channel.nack,
        reject_fn=channel.reject,
        **kw,
    )


def _group(**kw: Any) -> CoalescingAckerGroup:
    return CoalescingAckerGroup(factory=lambda ch: _acker(ch, **kw))


# ── channel_key guard ─────────────────────────────────────────────────────


class TestChannelKeyGuard:
    def test_unbound_by_default(self) -> None:
        ch = FakeChannel("a")
        acker = CoalescingAcker(ack_fn=ch.ack, nack_fn=ch.nack, reject_fn=ch.reject)
        assert acker.channel_key is None

    def test_binds_on_first_register(self) -> None:
        ch = FakeChannel("a")
        acker = CoalescingAcker(
            ack_fn=ch.ack,
            nack_fn=ch.nack,
            reject_fn=ch.reject,
            config=BatchAckConfig(flush_interval_ms=0),
        )
        acker.register(1, channel_key=ch)
        assert acker.channel_key is ch

    def test_constructor_binding_rejects_another_channel(self) -> None:
        a, b = FakeChannel("a"), FakeChannel("b")
        acker = _acker(a)
        acker.register(1, channel_key=a)
        with pytest.raises(ChannelMismatchError, match="per-channel counter"):
            acker.register(2, channel_key=b)
        # the rejected tag never entered the ledger
        assert acker.pending == 1
        assert acker.coordinator.state_of(2) is None

    def test_first_register_binding_rejects_another_channel(self) -> None:
        a, b = FakeChannel("a"), FakeChannel("b")
        acker = CoalescingAcker(
            ack_fn=a.ack, nack_fn=a.nack, reject_fn=a.reject, config=BatchAckConfig(flush_interval_ms=0)
        )
        acker.register(1, channel_key=a)
        with pytest.raises(ChannelMismatchError):
            acker.register(2, channel_key=b)

    def test_error_message_names_both_channels(self) -> None:
        a, b = FakeChannel("alpha"), FakeChannel("beta")
        acker = _acker(a)
        with pytest.raises(ChannelMismatchError) as ei:
            acker.register(1, channel_key=b)
        assert "alpha" in str(ei.value) and "beta" in str(ei.value)

    def test_equal_non_identical_keys_are_accepted(self) -> None:
        """A caller may key by name rather than by object."""
        ch = FakeChannel("a")
        acker = _acker(ch, channel_key="consumer-channel-1")
        acker.register(1, channel_key="consumer-channel-" + "1")  # equal, not identical
        assert acker.pending == 1

    def test_register_without_key_is_unchecked(self) -> None:
        """Backward compatible: no key supplied, no check performed."""
        a, b = FakeChannel("a"), FakeChannel("b")
        acker = _acker(a)
        acker.register(1)
        acker.register(2)
        assert acker.pending == 2
        assert b.frames == []

    def test_closed_check_precedes_channel_check(self) -> None:
        a, b = FakeChannel("a"), FakeChannel("b")
        acker = _acker(a)
        acker.close()
        with pytest.raises(BatchClosedError):
            acker.register(1, channel_key=b)

    def test_binding_survives_reconnect(self) -> None:
        """on_reconnect() invalidates the ledger, not the channel binding."""
        a, b = FakeChannel("a"), FakeChannel("b")
        acker = _acker(a)
        acker.register(1, channel_key=a)
        acker.on_reconnect()
        assert acker.channel_key is a
        with pytest.raises(ChannelMismatchError):
            acker.register(1, channel_key=b)


# ── CoalescingAckerGroup ──────────────────────────────────────────────────


class TestGroupPerChannelAckers:
    def test_one_acker_per_channel_cached(self) -> None:
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        acker_a, acker_b = group.for_channel(a), group.for_channel(b)
        assert acker_a is not acker_b
        assert group.for_channel(a) is acker_a
        assert group.channels == 2
        assert acker_a.channel_key is a and acker_b.channel_key is b

    def test_factory_called_once_per_channel(self) -> None:
        calls: list[FakeChannel] = []

        def factory(ch: FakeChannel) -> CoalescingAcker:
            calls.append(ch)
            return _acker(ch)

        group = CoalescingAckerGroup(factory=factory)
        a = FakeChannel("a")
        for _ in range(5):
            group.for_channel(a)
        assert calls == [a]

    def test_coordinators_are_separate(self) -> None:
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        assert group.for_channel(a).coordinator is not group.for_channel(b).coordinator

    def test_same_tags_on_two_channels_do_not_collide(self) -> None:
        """Tag 1..3 exist on BOTH channels and are different messages."""
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        for tag in (1, 2, 3):
            group.register(a, tag)
            group.register(b, tag)
        # only channel A's deliveries complete
        for tag in (1, 2, 3):
            group.complete(a, tag)
        report = group.flush()

        assert a.frames == [("ack", 3, True)]  # coalesced across A's own prefix
        assert b.frames == []  # B untouched — its tags are still outstanding
        assert group.for_channel(b).pending == 3
        assert report.settled_tags == 3 and report.coalesced_tags == 3

    def test_cumulative_ack_never_crosses_channels(self) -> None:
        """A's prefix is complete while B's tag 1 is still running: a cumulative
        ack must be emitted on A only, and must not cover B's tag 2."""
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        for tag in (1, 2):
            group.register(a, tag)
            group.register(b, tag)
        group.complete(a, 1)
        group.complete(a, 2)
        group.complete(b, 2)  # b1 still outstanding
        group.flush()

        assert a.frames == [("ack", 2, True)]
        assert b.frames == [("ack", 2, False)]  # individual: b1 is unfinished
        assert not any(multiple for _, _, multiple in b.frames)

    def test_per_delivery_proxies_route_to_the_right_channel(self) -> None:
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        group.register(a, 1)
        group.register(b, 1)
        group.register(b, 2)
        group.complete(a, 1)
        group.fail(b, 1, requeue=False)
        group.fail(b, 2, reject=True, requeue=True)
        group.flush()

        assert a.frames == [("ack", 1, False)]
        assert b.frames == [("nack", 1, False), ("reject", 2, True)]

    def test_retry_pending_and_release(self) -> None:
        group = _group()
        a = FakeChannel("a")
        group.register(a, 1)
        group.register(a, 2)
        group.retry_pending(a, 1)
        group.complete(a, 2)
        group.flush()
        assert a.frames == [("ack", 2, False)]  # 1 is retry-pending → no cumulative

        group.release(a, 1)  # retry path settled it elsewhere
        assert group.pending == 0

    def test_mixing_channels_on_one_acker_raises(self) -> None:
        """The failure the group exists to prevent, proven at the acker level."""
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        acker_a = group.for_channel(a)
        with pytest.raises(ChannelMismatchError):
            acker_a.register(1, channel_key=b)

    def test_completing_an_unregistered_tag_raises(self) -> None:
        group = _group()
        a = FakeChannel("a")
        with pytest.raises(CoordinatorError):
            group.complete(a, 99)


class TestHoldIsPerChannel:
    """A completed tag stranded behind an unfinished sibling is held for up to
    ``max_hold`` planning rounds hoping for a cumulative ack, then acked
    individually. The hold counter lives in each channel's own coordinator."""

    def test_hold_then_individual_fallback_does_not_leak_across_channels(self) -> None:
        group = CoalescingAckerGroup(factory=lambda ch: _acker(ch, max_hold=1))
        a, b = FakeChannel("a"), FakeChannel("b")
        for ch in (a, b):
            group.register(ch, 1)
            group.register(ch, 2)
        group.complete(a, 2)  # a1 still running
        group.complete(b, 1)
        group.complete(b, 2)  # b's whole prefix is done

        group.flush()
        assert a.frames == []  # a2 held for one round
        assert b.frames == [("ack", 2, True)]  # b coalesced immediately

        group.flush()
        assert a.frames == [("ack", 2, False)]  # fallback, never a cumulative
        assert b.frames == [("ack", 2, True)]  # b unchanged
        assert group.for_channel(a).coordinator.state_of(1) is not None  # a1 untouched


class TestGroupLifecycle:
    def test_flush_fans_out_and_aggregates(self) -> None:
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        for ch in (a, b):
            for tag in (1, 2):
                group.register(ch, tag)
                group.complete(ch, tag)
        report = group.flush(FlushReason.SIZE)

        assert report.reason is FlushReason.SIZE
        assert report.channels == 2
        assert report.settled_tags == 4 and report.coalesced_tags == 4
        assert report.errors == ()
        assert a.frames == [("ack", 2, True)] and b.frames == [("ack", 2, True)]

    def test_flush_records_errors_without_skipping_other_channels(self) -> None:
        a, b = FakeChannel("a"), FakeChannel("b")

        def factory(ch: FakeChannel) -> CoalescingAcker:
            if ch is a:

                def boom(tag: int, multiple: bool) -> None:
                    raise RuntimeError("channel closed")

                return CoalescingAcker(
                    ack_fn=boom,
                    nack_fn=ch.nack,
                    reject_fn=ch.reject,
                    config=BatchAckConfig(batch_size=1000, flush_interval_ms=0),
                    channel_key=ch,
                    max_hold=0,
                )
            return _acker(ch)

        group = CoalescingAckerGroup(factory=factory)
        for ch in (a, b):
            group.register(ch, 1)
            group.complete(ch, 1)
        report = group.flush()

        assert len(report.errors) == 1
        assert isinstance(report.errors[0][1], RuntimeError)
        assert b.frames == [("ack", 1, False)]  # B still settled
        assert isinstance(group.last_error, RuntimeError)

    def test_on_reconnect_drops_only_that_channel(self) -> None:
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        for ch in (a, b):
            group.register(ch, 1)
            group.register(ch, 2)
        acker_b = group.for_channel(b)

        dropped = group.on_reconnect(a)

        assert dropped == (1, 2)
        assert group.channels == 1
        assert group.for_channel(b) is acker_b and acker_b.pending == 2
        assert a.frames == []  # never replay old tags onto a rebuilt channel

    def test_on_reconnect_builds_a_fresh_acker_next_time(self) -> None:
        group = _group()
        a = FakeChannel("a")
        first = group.for_channel(a)
        group.register(a, 1)
        group.on_reconnect(a)
        second = group.for_channel(a)
        assert second is not first
        assert second.pending == 0

    def test_on_reconnect_unknown_channel_is_a_noop(self) -> None:
        group = _group()
        assert group.on_reconnect(FakeChannel("never-seen")) == ()
        assert group.channels == 0

    def test_reset_drops_every_channel(self) -> None:
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        for ch in (a, b):
            group.register(ch, 1)
            group.register(ch, 2)
        assert group.reset() == 4
        assert group.channels == 0 and group.pending == 0
        assert a.frames == [] and b.frames == []

    def test_close_drains_approved_work_and_clears(self) -> None:
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        for ch in (a, b):
            group.register(ch, 1)
            group.register(ch, 2)
            group.complete(ch, 1)  # 2 stays outstanding
        report = group.close()

        assert report.reason is FlushReason.CLOSE
        assert report.settled_tags == 2
        assert a.frames == [("ack", 1, False)] and b.frames == [("ack", 1, False)]
        assert group.closed and group.channels == 0
        with pytest.raises(BatchClosedError):
            group.for_channel(a)

    def test_stats_survive_reconnect_and_close(self) -> None:
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        for tag in (1, 2):
            group.register(a, tag)
            group.complete(a, tag)
        group.flush()
        assert group.settled_total == 2 and group.coalesced_total == 2

        group.on_reconnect(a)  # channel retired
        assert group.settled_total == 2  # not reset to 0
        assert group.coalesced_total == 2

        group.register(b, 1)
        group.complete(b, 1)
        group.flush()
        assert group.settled_total == 3
        group.close()
        assert group.settled_total == 3

    def test_pending_aggregates_across_channels(self) -> None:
        group = _group()
        a, b = FakeChannel("a"), FakeChannel("b")
        group.register(a, 1)
        group.register(b, 1)
        group.register(b, 2)
        assert group.pending == 3

    def test_last_error_is_none_when_clean(self) -> None:
        group = _group()
        group.for_channel(FakeChannel("a"))
        assert group.last_error is None


class TestGroupConcurrency:
    def test_concurrent_for_channel_creates_one_acker(self) -> None:
        created: list[CoalescingAcker] = []
        lock = threading.Lock()

        def factory(ch: FakeChannel) -> CoalescingAcker:
            acker = _acker(ch)
            with lock:
                created.append(acker)
            return acker

        group = CoalescingAckerGroup(factory=factory)
        channel = FakeChannel("shared")
        seen: list[CoalescingAcker] = []
        barrier = threading.Barrier(8)

        def worker() -> None:
            barrier.wait()
            acker = group.for_channel(channel)
            with lock:
                seen.append(acker)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len({id(a) for a in seen}) == 1  # everyone got the same acker
        assert group.channels == 1

    def test_concurrent_registration_across_channels_stays_isolated(self) -> None:
        group = _group()
        channels = [FakeChannel(f"ch{i}") for i in range(4)]

        def worker(ch: FakeChannel) -> None:
            for tag in range(1, 51):
                group.register(ch, tag)
                group.complete(ch, tag)

        threads = [threading.Thread(target=worker, args=(ch,)) for ch in channels]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        group.flush()

        for ch in channels:
            covered = [tag for kind, tag, multiple in ch.frames if kind == "ack"]
            assert max(covered) == 50  # each channel settled its own 1..50
            assert all(kind == "ack" for kind, _, _ in ch.frames)
        assert group.settled_total == 200
