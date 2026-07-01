"""Tests for handler timeout middleware (F7)."""

from __future__ import annotations

import asyncio
import threading
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.timeout import HandlerTimeoutError, TimeoutConfig, TimeoutMiddleware


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b"test",
        "routing_key": "test",
        "exchange": "",
        "headers": {},
    }
    defaults.update(kwargs)
    msg = RabbitMessage(**defaults)  # type: ignore[arg-type]
    msg._ack_fn = MagicMock()
    msg._nack_fn = MagicMock()
    msg._reject_fn = MagicMock()
    msg._ack_async_fn = AsyncMock()
    msg._nack_async_fn = AsyncMock()
    msg._reject_async_fn = AsyncMock()
    return msg


class TestTimeoutConfig:
    def test_defaults(self) -> None:
        config = TimeoutConfig()
        assert config.timeout_seconds == 30.0

    def test_custom_timeout(self) -> None:
        config = TimeoutConfig(timeout_seconds=5.0)
        assert config.timeout_seconds == 5.0

    def test_invalid_timeout(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            TimeoutConfig(timeout_seconds=0)

    def test_negative_timeout(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            TimeoutConfig(timeout_seconds=-1)

    def test_frozen(self) -> None:
        config = TimeoutConfig()
        with pytest.raises(AttributeError):
            config.timeout_seconds = 10  # type: ignore[misc]


class TestTimeoutMiddlewareSync:
    def test_fast_handler_passes(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=1.0))
        msg = _make_message()
        call_next = MagicMock(return_value="result")

        result = mw.consume_scope(call_next, msg)

        call_next.assert_called_once_with(msg)
        assert result == "result"

    def test_slow_handler_raises_timeout(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()

        def slow_handler(m: RabbitMessage) -> str:
            time.sleep(1.0)
            return "late"

        with pytest.raises(HandlerTimeoutError, match=r"0\.1s"):
            mw.consume_scope(slow_handler, msg)

    def test_handler_exception_propagates(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=1.0))
        msg = _make_message()

        def failing_handler(m: RabbitMessage) -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            mw.consume_scope(failing_handler, msg)


class TestTimeoutMiddlewareAsync:
    @pytest.mark.asyncio
    async def test_fast_handler_passes_async(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=1.0))
        msg = _make_message()
        call_next = AsyncMock(return_value="result")

        result = await mw.consume_scope_async(call_next, msg)

        call_next.assert_called_once_with(msg)
        assert result == "result"

    @pytest.mark.asyncio
    async def test_slow_handler_raises_timeout_async(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()

        async def slow_handler(m: RabbitMessage) -> str:
            await asyncio.sleep(1.0)
            return "late"

        with pytest.raises(HandlerTimeoutError, match=r"0\.1s"):
            await mw.consume_scope_async(slow_handler, msg)

    @pytest.mark.asyncio
    async def test_handler_exception_propagates_async(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=1.0))
        msg = _make_message()

        async def failing_handler(m: RabbitMessage) -> None:
            raise ValueError("async boom")

        with pytest.raises(ValueError, match="async boom"):
            await mw.consume_scope_async(failing_handler, msg)

    @pytest.mark.asyncio
    async def test_timeout_error_type(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()

        async def slow(m: RabbitMessage) -> None:
            await asyncio.sleep(1.0)

        with pytest.raises(HandlerTimeoutError) as exc_info:
            await mw.consume_scope_async(slow, msg)
        assert exc_info.value.timeout_seconds == 0.1


# ── sync abandon observability + on_timeout (M-S4) ──────────────────────


class TestSyncAbandonObservability:
    def test_sync_abandon_increments_counter_and_logs_critical(self) -> None:
        from unittest.mock import patch

        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()

        def slow_handler(m: RabbitMessage) -> str:
            time.sleep(1.0)
            return "late"

        with patch("rabbitkit.middleware.timeout.logger") as mock_logger:
            with pytest.raises(HandlerTimeoutError):
                mw.consume_scope(slow_handler, msg)

        assert mw.abandoned_threads == 1
        mock_logger.critical.assert_called_once()
        assert "abandoned" in mock_logger.critical.call_args[0][0]

    def test_on_timeout_callback_invoked_on_abandon(self) -> None:
        calls: list[tuple] = []

        def on_timeout(m: RabbitMessage, seconds: float) -> None:
            calls.append((m, seconds))

        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1), on_timeout=on_timeout)
        msg = _make_message()

        def slow_handler(m: RabbitMessage) -> str:
            time.sleep(1.0)
            return "late"

        with pytest.raises(HandlerTimeoutError):
            mw.consume_scope(slow_handler, msg)

        assert len(calls) == 1
        assert calls[0][0] is msg
        assert calls[0][1] == 0.1
        assert mw.abandoned_threads == 1

    @pytest.mark.asyncio
    async def test_async_clean_cancellation_does_not_abandon(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()

        async def slow(m: RabbitMessage) -> str:
            await asyncio.sleep(1.0)
            return "late"

        with pytest.raises(HandlerTimeoutError):
            await mw.consume_scope_async(slow, msg)
        # Async path cancels cleanly — no thread abandoned.
        assert mw.abandoned_threads == 0


# ── H9: settlement is exclusive to the consumer thread ────────────────────


class TestSyncSettlementRace:
    """H9: a sync handler that settles the message itself (e.g. under
    AckPolicy.MANUAL) must never touch the real (pika-backed) settlement fn
    from the background thread TimeoutMiddleware runs it in — only from the
    consumer thread that called consume_scope. If the handler finishes
    within the deadline, its settlement is captured and replayed for real
    on the consumer thread. If it's abandoned after a timeout, any later
    settlement attempt from that thread is discarded outright — the
    consumer thread's own subsequent settlement (AckPolicy/RetryMiddleware,
    after HandlerTimeoutError propagates) must still work deterministically."""

    def test_in_time_handler_ack_is_real_and_runs_on_consumer_thread(self) -> None:
        """Handler acks itself (MANUAL-style) well within the deadline: the
        real ack fn must be called exactly once, from the CONSUMER
        (calling) thread — never the background thread."""
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=2.0))
        msg = _make_message()
        consumer_thread_ident = threading.get_ident()
        ack_called_from: list[int] = []

        def real_ack_spy() -> None:
            ack_called_from.append(threading.get_ident())

        msg._ack_fn = real_ack_spy

        def handler(m: RabbitMessage) -> str:
            m.ack()
            return "done"

        result = mw.consume_scope(handler, msg)

        assert result == "done"
        assert ack_called_from == [consumer_thread_ident]
        assert msg._disposition == "acked"

    def test_in_time_handler_nack_is_real_and_runs_on_consumer_thread(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=2.0))
        msg = _make_message()
        consumer_thread_ident = threading.get_ident()
        nack_called_from: list[int] = []

        def real_nack_spy(requeue: bool = True) -> None:
            nack_called_from.append(threading.get_ident())

        msg._nack_fn = real_nack_spy

        def handler(m: RabbitMessage) -> None:
            m.nack(requeue=False)

        mw.consume_scope(handler, msg)

        assert nack_called_from == [consumer_thread_ident]
        assert msg._disposition == "nacked"

    def test_in_time_handler_reject_is_real_and_runs_on_consumer_thread(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=2.0))
        msg = _make_message()
        consumer_thread_ident = threading.get_ident()
        reject_called_from: list[int] = []

        def real_reject_spy(requeue: bool = False) -> None:
            reject_called_from.append(threading.get_ident())

        msg._reject_fn = real_reject_spy

        def handler(m: RabbitMessage) -> None:
            m.reject()

        mw.consume_scope(handler, msg)

        assert reject_called_from == [consumer_thread_ident]
        assert msg._disposition == "rejected"

    def test_abandoned_handler_ack_is_discarded_not_double_settled(self) -> None:
        """H9's exact spec: handler sleeps past the deadline then acks --
        the real ack fn must NEVER be called from the background thread (no
        cross-thread pika call), the message must NOT be left looking
        "already settled", and the consumer thread's own subsequent
        settlement must still work deterministically. NOTE: msg._ack_fn is
        deliberately never reassigned after consume_scope returns -- it
        must stay _guarded_ack (see the middleware's docstring for why) so
        the "consumer thread" assertion below actually exercises the guard's
        own cross-thread passthrough branch, not some fresh substitute fn."""
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()
        ack_attempted_from_background = threading.Event()
        real_ack_calls: list[int] = []  # thread idents that reached the real fn

        def real_ack_spy() -> None:
            real_ack_calls.append(threading.get_ident())

        msg._ack_fn = real_ack_spy

        def slow_then_acking_handler(m: RabbitMessage) -> None:
            time.sleep(0.3)  # past the 0.1s deadline
            try:
                m.ack()
            finally:
                # The discard path raises an internal sentinel through
                # m.ack() (see _DiscardedSettlement) -- set the event
                # regardless, since we're testing "reached the ack call",
                # not "ack() returned normally".
                ack_attempted_from_background.set()

        with pytest.raises(HandlerTimeoutError):
            mw.consume_scope(slow_then_acking_handler, msg)

        # Wait for the abandoned thread to actually reach and attempt the ack
        # (deterministic synchronization, not a timing guess).
        assert ack_attempted_from_background.wait(timeout=2.0), "background thread never reached ack()"

        # The real (pika-backed) ack fn must NEVER have been called.
        assert real_ack_calls == []

        # Disposition must NOT look "already settled" -- otherwise the
        # consumer's own real settlement below would be silently skipped.
        assert msg._disposition == "pending"

        # The consumer's own subsequent ack (what a MANUAL-ack-policy caller,
        # or in principle AckPolicy, would do after HandlerTimeoutError
        # propagates) must still reach the real fn, from THIS thread —
        # msg._ack_fn is still _guarded_ack; this exercises its cross-thread
        # passthrough branch for real.
        msg.ack()
        assert real_ack_calls == [threading.get_ident()]
        assert msg._disposition == "acked"

    def test_abandoned_handler_nack_is_discarded(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()
        nack_attempted = threading.Event()
        real_nack_calls: list[int] = []

        def real_nack_spy(requeue: bool = True) -> None:
            real_nack_calls.append(threading.get_ident())

        msg._nack_fn = real_nack_spy

        def slow_then_nacking_handler(m: RabbitMessage) -> None:
            time.sleep(0.3)
            try:
                m.nack(requeue=True)
            finally:
                nack_attempted.set()

        with pytest.raises(HandlerTimeoutError):
            mw.consume_scope(slow_then_nacking_handler, msg)

        assert nack_attempted.wait(timeout=2.0)
        assert real_nack_calls == []
        assert msg._disposition == "pending"

        # Consumer thread's own subsequent nack must reach the real fn —
        # msg._nack_fn is still _guarded_nack (never reassigned).
        msg.nack(requeue=False)
        assert real_nack_calls == [threading.get_ident()]
        assert msg._disposition == "nacked"

    def test_abandoned_handler_reject_is_discarded(self) -> None:
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.1))
        msg = _make_message()
        reject_attempted = threading.Event()
        real_reject_calls: list[int] = []

        def real_reject_spy(requeue: bool = False) -> None:
            real_reject_calls.append(threading.get_ident())

        msg._reject_fn = real_reject_spy

        def slow_then_rejecting_handler(m: RabbitMessage) -> None:
            time.sleep(0.3)
            try:
                m.reject()
            finally:
                reject_attempted.set()

        with pytest.raises(HandlerTimeoutError):
            mw.consume_scope(slow_then_rejecting_handler, msg)

        assert reject_attempted.wait(timeout=2.0)
        assert real_reject_calls == []
        assert msg._disposition == "pending"

        # Consumer thread's own subsequent reject must reach the real fn —
        # msg._reject_fn is still _guarded_reject (never reassigned).
        msg.reject(requeue=True)
        assert real_reject_calls == [threading.get_ident()]
        assert msg._disposition == "rejected"

    def test_no_ack_fn_set_still_raises_runtime_error(self) -> None:
        """Regression guard: a message with no real settlement fn (e.g. a
        no-ack delivery) must still raise RabbitMessage's own RuntimeError
        when ack() is called -- the guard must not silently swallow it by
        installing a stand-in where none should exist."""
        mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=2.0))
        msg = _make_message()
        msg._ack_fn = None

        caught: list[BaseException] = []

        def handler(m: RabbitMessage) -> None:
            try:
                m.ack()
            except BaseException as exc:
                caught.append(exc)

        mw.consume_scope(handler, msg)

        assert len(caught) == 1
        assert isinstance(caught[0], RuntimeError)
