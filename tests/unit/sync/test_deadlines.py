"""Tests for sync/deadlines.py — processing_timeout on the sync broker."""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.errors import HandlerTimeoutError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.sync.deadlines import DeliveryClaim, LateSettlementError, SyncDeadlines


class _Clock:
    def __init__(self) -> None:
        self.now = 10.0

    def __call__(self) -> float:
        return self.now


def _route(name: str = "orders") -> Any:
    route = MagicMock()
    route.queue.name = name
    return route


def _message(*, alive: bool | None = True) -> tuple[RabbitMessage, list[tuple[str, tuple[Any, ...]]]]:
    sent: list[tuple[str, tuple[Any, ...]]] = []
    msg = RabbitMessage(body=b"{}", message_id="m-1", delivery_tag=7)
    msg._ack_fn = lambda: sent.append(("ack", ()))
    msg._nack_fn = lambda requeue=True: sent.append(("nack", (requeue,)))
    msg._reject_fn = lambda requeue=False: sent.append(("reject", (requeue,)))
    if alive is not None:
        msg._channel_alive = lambda: alive
    return msg, sent


class _Settler:
    """Stands in for HandlerPipeline.settle_expired_sync."""

    def __init__(self, result: bool = True) -> None:
        self.result = result
        self.calls: list[tuple[Any, RabbitMessage, HandlerTimeoutError]] = []

    def __call__(self, route: Any, message: RabbitMessage, exc: HandlerTimeoutError) -> bool:
        self.calls.append((route, message, exc))
        if self.result:
            message.nack(requeue=True)
        return self.result


def _deadlines(seconds: float = 2.0, settler: _Settler | None = None) -> tuple[SyncDeadlines, _Clock, _Settler]:
    clock = _Clock()
    settler = settler or _Settler()
    return SyncDeadlines(seconds, settler, clock=clock), clock, settler


class TestPollInterval:
    @pytest.mark.parametrize(("seconds", "expected"), [(0.02, 0.01), (0.4, 0.1), (30.0, 1.0)])
    def test_bounded_between_10ms_and_1s(self, seconds: float, expected: float) -> None:
        assert SyncDeadlines(seconds, _Settler()).poll_interval == pytest.approx(expected)


class TestInTime:
    def test_a_handler_settling_in_time_is_untouched(self) -> None:
        deadlines, clock, settler = _deadlines()
        msg, sent = _message()
        claim = deadlines.begin(_route(), msg)
        assert deadlines.running == 1
        msg.ack()
        clock.now += 5
        deadlines.check()  # past the deadline, but the handler already owns it
        deadlines.finish(claim)
        assert sent == [("ack", ())]
        assert settler.calls == []
        assert deadlines.running == 0
        assert deadlines.expired_total == 0

    def test_nothing_is_due_before_the_deadline(self) -> None:
        deadlines, clock, settler = _deadlines()
        msg, _sent = _message()
        deadlines.begin(_route(), msg)
        clock.now += 1.9
        deadlines.check()
        assert settler.calls == []

    def test_a_message_without_settlement_fns_keeps_none(self) -> None:
        deadlines, _clock, _settler = _deadlines()
        msg = RabbitMessage(body=b"")
        deadlines.begin(_route(), msg)
        assert msg._ack_fn is None and msg._nack_fn is None and msg._reject_fn is None


class TestExpired:
    def test_the_deadline_settles_and_the_late_handler_is_dropped(self, caplog: pytest.LogCaptureFixture) -> None:
        deadlines, clock, settler = _deadlines()
        msg, sent = _message()
        route = _route()
        claim = deadlines.begin(route, msg)
        clock.now += 2.0
        with caplog.at_level("WARNING", logger="rabbitkit.sync.deadlines"):
            deadlines.check()
            assert claim.expired
            assert deadlines.expired_total == 1
            assert settler.calls[0][0] is route and isinstance(settler.calls[0][2], HandlerTimeoutError)
            assert sent == [("nack", (True,))]
            # The late handler's ack: already settled -> a no-op.
            msg.ack()
            clock.now += 1.0
            deadlines.finish(claim)
        assert sent == [("nack", (True,))]
        assert "exceeded processing_timeout=2.0s" in caplog.text
        assert "returned after 3.0s" in caplog.text
        deadlines.check()  # finished: nothing left
        assert len(settler.calls) == 1

    def test_a_racing_handler_settlement_raises_late_settlement(self) -> None:
        """The deadline claimed the delivery but has not set the disposition
        yet: the handler thread's settlement must not reach the channel."""
        msg, sent = _message()
        claim = DeliveryClaim(_route(), msg, 0.0, 1.0)
        guarded_ack = claim.guard(msg._ack_fn)  # type: ignore[arg-type]
        assert claim.claim_for_deadline() is True
        errors: list[BaseException] = []

        def handler_thread() -> None:
            try:
                guarded_ack()
            except LateSettlementError as exc:
                errors.append(exc)

        t = threading.Thread(target=handler_thread)
        t.start()
        t.join()
        assert len(errors) == 1
        assert sent == []
        guarded_ack()  # the deadline's own (owner) thread goes through
        assert sent == [("ack", ())]

    def test_the_deadline_cannot_claim_a_delivery_the_handler_settles(self) -> None:
        msg, sent = _message()
        claim = DeliveryClaim(_route(), msg, 0.0, 1.0)
        claim.guard(msg._nack_fn)(False)  # type: ignore[arg-type]
        assert claim.claim_for_deadline() is False
        assert sent == [("nack", (False,))]
        assert not claim.expired

    def test_policy_leaving_it_to_the_handler_hands_the_claim_back(self) -> None:
        deadlines, clock, settler = _deadlines(settler=_Settler(result=False))
        msg, sent = _message()
        claim = deadlines.begin(_route(), msg)
        clock.now += 3
        deadlines.check()
        assert len(settler.calls) == 1
        assert not claim.expired
        assert deadlines.current_expired() is False
        msg.ack()  # the handler still owns it
        assert sent == [("ack", ())]
        deadlines.check()  # not offered again
        assert len(settler.calls) == 1

    def test_a_delivery_on_a_closed_channel_is_claimed_but_not_settled(self) -> None:
        deadlines, clock, settler = _deadlines()
        msg, sent = _message(alive=False)
        claim = deadlines.begin(_route(), msg)
        clock.now += 3
        deadlines.check()
        assert claim.expired
        assert settler.calls == [] and sent == []
        assert deadlines.expired_total == 0

    def test_a_failing_settlement_is_logged_and_the_other_checks_go_on(self, caplog: pytest.LogCaptureFixture) -> None:
        calls: list[str] = []

        def settle(route: Any, message: RabbitMessage, exc: HandlerTimeoutError) -> bool:
            calls.append(route.queue.name)
            if route.queue.name == "a":
                raise RuntimeError("channel bug")
            return True

        clock = _Clock()
        deadlines = SyncDeadlines(1.0, settle, clock=clock)
        deadlines.begin(_route("a"), _message()[0])
        deadlines.begin(_route("b"), _message()[0])
        clock.now += 2
        with caplog.at_level("ERROR", logger="rabbitkit.sync.deadlines"):
            deadlines.check()
        assert sorted(calls) == ["a", "b"]
        assert "Settling an expired delivery failed (queue=a)" in caplog.text


class TestThreadLocalState:
    def test_current_expired_follows_the_calling_threads_delivery(self) -> None:
        deadlines, clock, _settler = _deadlines()
        assert deadlines.current_expired() is False
        msg, _sent = _message()
        claim = deadlines.begin(_route(), msg)
        assert deadlines.current_expired() is False
        clock.now += 5
        deadlines.check()
        assert deadlines.current_expired() is True
        seen: list[bool] = []
        t = threading.Thread(target=lambda: seen.append(deadlines.current_expired()))
        t.start()
        t.join()
        assert seen == [False]  # another thread has no delivery
        deadlines.finish(claim)
        assert deadlines.current_expired() is False

    def test_begin_arms_the_once_only_settlement_metric(self) -> None:
        deadlines, _clock, _settler = _deadlines()
        msg, _sent = _message()
        assert msg._settlement_metric_once is None
        deadlines.begin(_route(), msg)
        assert msg._settlement_metric_once == [None]
