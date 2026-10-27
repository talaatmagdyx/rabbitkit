"""async_/transport.py publish outcome contract: NOT_SENT / UNKNOWN / RETURNED
on every path, no hidden republish, channel release on every path, byte
budget, per-call timeout and publisher shutdown (G-01, G-03, G-04, G-12,
G-14, PUB-004)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import (
    ChannelAccessRefused,
    ChannelInvalidStateError,
    ChannelNotFoundEntity,
    PublishError,
)
from pamqp.commands import Basic
from pamqp.header import ContentHeader

from rabbitkit.async_.pool import AsyncChannelPool, ChannelPoolClosedError, ChannelPoolExhaustedError
from rabbitkit.async_.transport import AsyncTransportImpl
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig
from rabbitkit.core.types import DIRECT_REPLY_TO_QUEUE, MessageEnvelope, PublishStatus


def _transport(**kwargs: Any) -> AsyncTransportImpl:
    transport = AsyncTransportImpl(connection_config=ConnectionConfig(), security_config=SecurityConfig(), **kwargs)
    transport._connected = True
    return transport


def _channel(publish: Any = None) -> AsyncMock:
    channel = AsyncMock()
    channel.is_closed = False

    async def _close() -> None:
        channel.is_closed = True

    channel.close = AsyncMock(side_effect=_close)
    exchange = MagicMock()
    exchange.publish = publish if publish is not None else AsyncMock(return_value=Basic.Ack())
    channel.default_exchange = exchange
    channel.get_exchange = AsyncMock(return_value=exchange)
    return channel


def _wire(transport: AsyncTransportImpl, *channels: AsyncMock) -> AsyncMock:
    """Give *transport* a live publisher connection that hands out *channels*."""
    conn = AsyncMock()
    conn.is_closed = False
    conn.connected = MagicMock()
    conn.connected.is_set = MagicMock(return_value=True)
    conn.channel = AsyncMock(side_effect=list(channels))
    pool = transport._conn_pool
    pool._publisher_connection = conn
    pool._publisher_channel_pool = AsyncChannelPool(
        conn,
        pool_size=pool._pool_config.channel_pool_size,
        acquire_timeout=pool._pool_config.channel_acquire_timeout,
    )
    return conn


def _returned_message(code: int = 312, text: str = "NO_ROUTE") -> DeliveredMessage:
    frame = Basic.Return(reply_code=code, reply_text=text, exchange="", routing_key="nowhere")
    return DeliveredMessage(delivery=frame, header=ContentHeader(), body=b"x", channel=MagicMock())


def _env(**kwargs: Any) -> MessageEnvelope:
    kwargs.setdefault("routing_key", "q")
    kwargs.setdefault("body", b"x")
    return MessageEnvelope(**kwargs)


# ── G-01: mandatory → RETURNED on every channel kind ─────────────────────────


class TestReturnedOnEveryPath:
    @pytest.mark.asyncio
    async def test_a_returned_message_on_a_plain_confirmed_channel_is_returned(self) -> None:
        """Batch workers and the reply-to channel have on_return_raises=False:
        aio-pika resolves the confirm with the returned message. That used to
        be reported CONFIRMED."""
        transport = _transport()
        channel = _channel(AsyncMock(return_value=_returned_message()))
        outcome = await transport._publish_on_channel(channel, _env(mandatory=True))
        assert outcome.status is PublishStatus.RETURNED
        assert (outcome.reason, outcome.reply_code, outcome.reply_text) == ("returned", 312, "NO_ROUTE")
        assert not outcome.ok

    @pytest.mark.asyncio
    async def test_publish_error_carries_the_reply_fields(self) -> None:
        returned = _returned_message(313, "NO_CONSUMERS")
        transport = _transport()
        channel = _channel(AsyncMock(side_effect=PublishError(returned, returned.delivery)))
        outcome = await transport._publish_on_channel(channel, _env(mandatory=True))
        assert outcome.status is PublishStatus.RETURNED
        assert (outcome.reply_code, outcome.reply_text) == (313, "NO_CONSUMERS")

    @pytest.mark.asyncio
    async def test_an_ack_frame_is_confirmed(self) -> None:
        transport = _transport()
        outcome = await transport._publish_on_channel(_channel(), _env())
        assert outcome.status is PublishStatus.CONFIRMED
        assert outcome.reason is None

    @pytest.mark.asyncio
    async def test_reply_to_publish_is_returned_too(self) -> None:
        transport = _transport()
        reply_channel = _channel(AsyncMock(return_value=_returned_message()))
        transport._reply_to_channel = reply_channel
        outcome = await transport.publish(_env(reply_to=DIRECT_REPLY_TO_QUEUE, mandatory=True))
        assert outcome.status is PublishStatus.RETURNED


# ── G-03: classification ─────────────────────────────────────────────────────


class TestFailureClassification:
    @pytest.mark.asyncio
    async def test_missing_exchange_is_an_error_not_a_connection_error_and_not_retried(self) -> None:
        refused = ChannelNotFoundEntity("NOT_FOUND - no exchange 'gone' in vhost '/'")
        first = _channel(AsyncMock(side_effect=refused))
        second = _channel()
        transport = _transport()
        _wire(transport, first, second)

        outcome = await transport.publish(_env(exchange="gone"))

        assert outcome.status is PublishStatus.ERROR
        assert (outcome.reason, outcome.reply_code) == ("exchange_not_found", 404)
        assert outcome.reply_text is not None and "no exchange 'gone'" in outcome.reply_text
        first.default_exchange.publish.assert_awaited_once()
        second.default_exchange.publish.assert_not_awaited()  # no hidden retry

    @pytest.mark.asyncio
    async def test_refusal_on_a_shared_channel_is_ours_only_if_it_names_our_exchange(self) -> None:
        transport = _transport()
        ours = ChannelNotFoundEntity("NOT_FOUND - no exchange 'mine' in vhost '/'")
        outcome = await transport._publish_on_channel(_channel(AsyncMock(side_effect=ours)), _env(exchange="mine"))
        assert (outcome.status, outcome.reason) == (PublishStatus.ERROR, "exchange_not_found")

        sibling = ChannelNotFoundEntity("NOT_FOUND - no exchange 'other' in vhost '/'")
        outcome = await transport._publish_on_channel(_channel(AsyncMock(side_effect=sibling)), _env(exchange="mine"))
        assert (outcome.status, outcome.reason) == (PublishStatus.UNKNOWN, "channel_closed")

    @pytest.mark.asyncio
    async def test_access_refused_on_an_exclusive_channel(self) -> None:
        transport = _transport()
        refused = ChannelAccessRefused("ACCESS_REFUSED - user_id mismatch")
        outcome = await transport._publish_on_channel(_channel(AsyncMock(side_effect=refused)), _env(), exclusive=True)
        assert (outcome.status, outcome.reason, outcome.reply_code) == (PublishStatus.ERROR, "access_refused", 403)

    @pytest.mark.asyncio
    async def test_a_lost_channel_mid_publish_is_unknown(self) -> None:
        transport = _transport()
        outcome = await transport._publish_on_channel(
            _channel(AsyncMock(side_effect=ChannelInvalidStateError("closed"))), _env()
        )
        assert (outcome.status, outcome.reason) == (PublishStatus.UNKNOWN, "connection_lost")
        assert outcome.is_unknown

    @pytest.mark.asyncio
    async def test_a_lost_channel_during_shutdown_is_unknown_shutdown(self) -> None:
        transport = _transport()
        transport._publish_shutdown = True
        outcome = await transport._publish_on_channel(_channel(AsyncMock(side_effect=ConnectionResetError())), _env())
        assert (outcome.status, outcome.reason) == (PublishStatus.UNKNOWN, "shutdown")

    @pytest.mark.asyncio
    async def test_an_unclassified_failure_is_error(self) -> None:
        transport = _transport()
        outcome = await transport._publish_on_channel(_channel(AsyncMock(side_effect=KeyError("?"))), _env())
        assert (outcome.status, outcome.reason) == (PublishStatus.ERROR, "publish_error")

    @pytest.mark.asyncio
    async def test_cancellation_propagates(self) -> None:
        transport = _transport()
        with pytest.raises(asyncio.CancelledError):
            await transport._publish_on_channel(_channel(AsyncMock(side_effect=asyncio.CancelledError())), _env())


# ── PUB-004: per-call timeout ────────────────────────────────────────────────


class TestPerCallTimeout:
    @pytest.mark.asyncio
    async def test_caller_deadline_shorter_than_confirm_timeout_is_publish_timeout(self) -> None:
        async def never(*_: Any, **__: Any) -> None:
            await asyncio.sleep(5)

        transport = _transport(confirm_timeout=5.0)
        outcome = await transport._publish_on_channel(_channel(never), _env(), timeout=0.02)
        assert (outcome.status, outcome.reason) == (PublishStatus.TIMEOUT, "publish_timeout")

    @pytest.mark.asyncio
    async def test_confirm_timeout_keeps_its_reason(self) -> None:
        async def never(*_: Any, **__: Any) -> None:
            await asyncio.sleep(5)

        transport = _transport(confirm_timeout=0.02)
        outcome = await transport._publish_on_channel(_channel(never), _env(), timeout=5.0)
        assert (outcome.status, outcome.reason) == (PublishStatus.TIMEOUT, "confirm_timeout")

    @pytest.mark.asyncio
    async def test_an_expired_deadline_sends_nothing(self) -> None:
        transport = _transport()
        channel = _channel()
        outcome = await transport._publish_on_channel(channel, _env(), timeout=0)
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "admission_timeout")
        channel.default_exchange.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_timeout_bounds_the_channel_wait(self) -> None:
        transport = _transport(pool_config=PoolConfig(channel_pool_size=1, channel_acquire_timeout=30.0))
        gate = asyncio.Event()

        async def blocked(*_: Any, **__: Any) -> Any:
            await gate.wait()
            return Basic.Ack()

        _wire(transport, _channel(blocked))
        first = asyncio.create_task(transport.publish(_env()))
        await asyncio.sleep(0.01)
        loop = asyncio.get_running_loop()
        start = loop.time()
        outcome = await transport.publish(_env(), timeout=0.05)
        assert loop.time() - start < 1.0
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "admission_timeout")
        gate.set()
        assert (await first).status is PublishStatus.CONFIRMED


# ── G-03/G-05: admission failures are NOT_SENT ───────────────────────────────


class TestNotSent:
    @pytest.mark.asyncio
    async def test_connect_failure_is_not_connected(self) -> None:
        transport = _transport()
        transport._connected = False
        with patch.object(transport, "connect", new=AsyncMock(side_effect=ConnectionRefusedError("down"))):
            outcome = await transport.publish(_env())
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "not_connected")

    @pytest.mark.parametrize(
        ("error", "reason"),
        [
            (ChannelPoolExhaustedError("busy"), "admission_timeout"),
            (ChannelPoolClosedError("closed"), "publisher_closed"),
            (RuntimeError("cannot open a channel"), "channel_unavailable"),
        ],
    )
    @pytest.mark.asyncio
    async def test_acquire_failures(self, error: Exception, reason: str) -> None:
        transport = _transport()
        transport._conn_pool.acquire_publisher_channel = AsyncMock(side_effect=error)  # type: ignore[method-assign]
        outcome = await transport.publish(_env())
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, reason)
        assert outcome.error is error

    @pytest.mark.asyncio
    async def test_reconnecting_publisher_waits_at_most_the_deadline(self) -> None:
        transport = _transport()
        conn = _wire(transport, _channel())
        conn.connected.is_set = MagicMock(return_value=False)
        loop = asyncio.get_running_loop()
        start = loop.time()
        outcome = await transport.publish(_env(), timeout=0.15)
        assert loop.time() - start < 1.0
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "not_connected")

    @pytest.mark.asyncio
    async def test_publish_after_close_publishing_is_not_sent(self) -> None:
        transport = _transport()
        await transport.close_publishing(timeout=0)
        with patch.object(transport, "_publish_admitted", new=AsyncMock()) as admitted:
            outcome = await transport.publish(_env())
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "publisher_closed")
        admitted.assert_not_awaited()


# ── fire-and-forget path ─────────────────────────────────────────────────────


class TestFastPath:
    @pytest.mark.asyncio
    async def test_no_channel_is_not_sent(self) -> None:
        transport = _transport(confirm_delivery=False)
        with patch.object(transport, "_get_fast_channel", new=AsyncMock(side_effect=RuntimeError("no conn"))):
            outcome = await transport.publish(_env())
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "channel_unavailable")

    @pytest.mark.asyncio
    async def test_a_failed_send_is_unknown(self) -> None:
        transport = _transport(confirm_delivery=False)
        channel = _channel(AsyncMock(side_effect=ConnectionResetError()))
        with patch.object(transport, "_get_fast_channel", new=AsyncMock(return_value=channel)):
            outcome = await transport.publish(_env())
        assert (outcome.status, outcome.reason) == (PublishStatus.UNKNOWN, "connection_lost")

    @pytest.mark.asyncio
    async def test_cancellation_propagates(self) -> None:
        transport = _transport(confirm_delivery=False)
        with patch.object(transport, "_get_fast_channel", new=AsyncMock(side_effect=asyncio.CancelledError())):
            with pytest.raises(asyncio.CancelledError):
                await transport.publish(_env())
        channel = _channel(AsyncMock(side_effect=asyncio.CancelledError()))
        with patch.object(transport, "_get_fast_channel", new=AsyncMock(return_value=channel)):
            with pytest.raises(asyncio.CancelledError):
                await transport.publish(_env())
        assert transport.publishes_in_flight == 0


# ── G-04: channel returned on every path ─────────────────────────────────────


class TestChannelReleasedOnEveryPath:
    @pytest.mark.asyncio
    async def test_a_cancelled_publish_discards_its_channel_and_frees_the_slot(self) -> None:
        gate = asyncio.Event()

        async def hang(*_: Any, **__: Any) -> Any:
            await gate.wait()
            return Basic.Ack()  # pragma: no cover

        cancelled_channel = _channel(hang)
        next_channel = _channel()
        transport = _transport(pool_config=PoolConfig(channel_pool_size=1, channel_acquire_timeout=0.5))
        _wire(transport, cancelled_channel, next_channel)

        task = asyncio.create_task(transport.publish(_env()))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)

        cancelled_channel.close.assert_awaited_once()  # confirm state unknown: never reused
        outcome = await transport.publish(_env())  # pool_size=1: the slot must be free
        assert outcome.status is PublishStatus.CONFIRMED
        pool = transport._conn_pool._publisher_channel_pool
        assert pool is not None
        assert pool.created_count == 1
        assert transport.publishes_in_flight == 0

    @pytest.mark.asyncio
    async def test_a_confirmed_channel_goes_back_to_the_pool(self) -> None:
        channel = _channel()
        transport = _transport()
        _wire(transport, channel)
        await transport.publish(_env())
        await transport.publish(_env())
        assert channel.default_exchange.publish.await_count == 2  # same channel, reused
        channel.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_raising_publish_on_channel_is_classified(self) -> None:
        transport = _transport()
        _wire(transport, _channel())
        with patch.object(transport, "_publish_on_channel", new=AsyncMock(side_effect=ConnectionResetError())):
            outcome = await transport.publish(_env())
        assert outcome.status is PublishStatus.UNKNOWN


# ── G-14: byte budget ────────────────────────────────────────────────────────


class TestByteBudget:
    @pytest.mark.asyncio
    async def test_no_budget_by_default(self) -> None:
        assert _transport().byte_limiter is None

    @pytest.mark.asyncio
    async def test_oversized_message_is_not_sent(self) -> None:
        transport = _transport(max_in_flight_bytes=4)
        outcome = await transport.publish(_env(body=b"12345"))
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "exceeds_max_in_flight_bytes")

    @pytest.mark.asyncio
    async def test_waits_for_room_then_times_out_and_releases_on_completion(self) -> None:
        gate = asyncio.Event()

        async def slow(*_: Any, **__: Any) -> Any:
            await gate.wait()
            return Basic.Ack()

        transport = _transport(max_in_flight_bytes=5, pool_config=PoolConfig(channel_acquire_timeout=0.05))
        _wire(transport, _channel(slow), _channel())
        limiter = transport.byte_limiter
        assert limiter is not None

        first = asyncio.create_task(transport.publish(_env(body=b"12345")))
        await asyncio.sleep(0.01)
        assert limiter.used == 5
        second = await transport.publish(_env(body=b"1"))
        assert (second.status, second.reason) == (PublishStatus.NOT_SENT, "admission_timeout")
        gate.set()
        assert (await first).status is PublishStatus.CONFIRMED
        assert limiter.used == 0

    @pytest.mark.asyncio
    async def test_cancelled_publish_releases_its_bytes(self) -> None:
        async def hang(*_: Any, **__: Any) -> Any:
            await asyncio.Event().wait()

        transport = _transport(max_in_flight_bytes=10)
        _wire(transport, _channel(hang))
        task = asyncio.create_task(transport.publish(_env(body=b"12345")))
        await asyncio.sleep(0.01)
        assert transport.byte_limiter is not None and transport.byte_limiter.used == 5
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert transport.byte_limiter.used == 0


# ── G-12: publisher shutdown ─────────────────────────────────────────────────


class TestClosePublishing:
    @pytest.mark.asyncio
    async def test_idle_close_returns_at_once(self) -> None:
        transport = _transport()
        async with asyncio.timeout(1):
            await transport.close_publishing(timeout=10)
        assert transport._publish_closed and not transport._publish_shutdown

    @pytest.mark.asyncio
    async def test_waits_for_in_flight_publishes_within_the_deadline(self) -> None:
        async def slowish(*_: Any, **__: Any) -> Any:
            await asyncio.sleep(0.05)
            return Basic.Ack()

        transport = _transport()
        _wire(transport, _channel(slowish))
        task = asyncio.create_task(transport.publish(_env()))
        await asyncio.sleep(0.01)
        await transport.close_publishing(timeout=2)
        assert (await task).status is PublishStatus.CONFIRMED
        assert not transport._publish_shutdown

    @pytest.mark.asyncio
    async def test_past_the_deadline_in_flight_is_unknown_and_waiters_not_sent(self) -> None:
        on_wire = _channel()

        async def until_closed(*_: Any, **__: Any) -> Any:
            while not on_wire.is_closed:
                await asyncio.sleep(0.005)
            raise ChannelInvalidStateError("channel closed")

        on_wire.default_exchange.publish = until_closed
        transport = _transport(
            max_in_flight_bytes=6, pool_config=PoolConfig(channel_pool_size=1, channel_acquire_timeout=30.0)
        )
        _wire(transport, on_wire)
        fast = AsyncMock()
        fast.is_closed = False
        transport._fast_publish_channel = fast

        in_flight = asyncio.create_task(transport.publish(_env(body=b"123")))
        await asyncio.sleep(0.01)
        channel_waiter = asyncio.create_task(transport.publish(_env(body=b"123")))
        byte_waiter = asyncio.create_task(transport.publish(_env(body=b"12")))
        await asyncio.sleep(0.01)

        async with asyncio.timeout(5):
            await transport.close_publishing(timeout=0.05)

        a, b, c = await asyncio.gather(in_flight, channel_waiter, byte_waiter)
        assert (a.status, a.reason) == (PublishStatus.UNKNOWN, "shutdown")
        assert (b.status, b.reason) == (PublishStatus.NOT_SENT, "publisher_closed")
        assert (c.status, c.reason) == (PublishStatus.NOT_SENT, "publisher_closed")
        fast.close.assert_awaited_once()
        assert transport.publishes_in_flight == 0

    @pytest.mark.asyncio
    async def test_a_publish_that_never_unwinds_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        transport = _transport()
        transport._publish_entered()  # a publish stuck somewhere we cannot reach
        with patch("rabbitkit.async_.transport._SHUTDOWN_UNWIND_SECONDS", 0.01):
            await transport.close_publishing(timeout=0.01)
        assert "did not unwind" in caplog.text
        transport._publish_exited()

    @pytest.mark.asyncio
    async def test_recovery_wait_stops_on_shutdown(self) -> None:
        transport = _transport()
        transport._publish_shutdown = True
        with patch.object(transport, "_publisher_live", return_value=False):
            async with asyncio.timeout(1):
                assert await transport._wait_for_recovery(timeout=30) is False


class TestAdmissionEdges:
    @pytest.mark.asyncio
    async def test_cancelled_connect_propagates(self) -> None:
        transport = _transport()
        transport._connected = False
        with patch.object(transport, "connect", new=AsyncMock(side_effect=asyncio.CancelledError())):
            with pytest.raises(asyncio.CancelledError):
                await transport.publish(_env())
        assert transport.publishes_in_flight == 0

    @pytest.mark.asyncio
    async def test_cancelled_channel_wait_propagates(self) -> None:
        transport = _transport()
        transport._conn_pool.acquire_publisher_channel = AsyncMock(  # type: ignore[method-assign]
            side_effect=asyncio.CancelledError()
        )
        with pytest.raises(asyncio.CancelledError):
            await transport.publish(_env())

    @pytest.mark.asyncio
    async def test_a_new_publish_marks_the_publisher_busy_again(self) -> None:
        transport = _transport()
        transport._publishes_idle = asyncio.Event()
        transport._publishes_idle.set()
        transport._publish_entered()
        assert not transport._publishes_idle.is_set()
        transport._publish_exited()
        assert transport._publishes_idle.is_set()


class TestPreSendFailures:
    @pytest.mark.asyncio
    async def test_invalid_envelope_is_an_error_not_a_raise(self) -> None:
        transport = _transport()
        channel = _channel()
        outcome = await transport._publish_on_channel(channel, _env(delivery_mode=7))
        assert (outcome.status, outcome.reason) == (PublishStatus.ERROR, "invalid_envelope")
        channel.default_exchange.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_exchange_lookup_failure_is_not_sent(self) -> None:
        transport = _transport()
        channel = _channel()
        channel.get_exchange = AsyncMock(side_effect=RuntimeError("channel gone"))
        outcome = await transport._publish_on_channel(channel, _env(exchange="ex"))
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "channel_unavailable")

    @pytest.mark.asyncio
    async def test_cancelled_exchange_lookup_propagates(self) -> None:
        transport = _transport()
        channel = _channel()
        channel.get_exchange = AsyncMock(side_effect=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await transport._publish_on_channel(channel, _env(exchange="ex"))

    @pytest.mark.asyncio
    async def test_invalid_envelope_on_the_fast_path_is_an_error(self) -> None:
        transport = _transport(confirm_delivery=False)
        outcome = await transport.publish(_env(delivery_mode=7))
        assert (outcome.status, outcome.reason) == (PublishStatus.ERROR, "invalid_envelope")
