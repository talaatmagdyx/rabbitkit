"""SyncTransport: ``PublisherConfig.max_in_flight_bytes`` (the byte budget)."""

from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.core.config import ConnectionConfig
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.sync.pool import InFlightByteLimiter
from rabbitkit.sync.transport import SyncTransport

pika = pytest.importorskip("pika")


def _transport(**kw: Any) -> SyncTransport:
    return SyncTransport(connection_config=ConnectionConfig(), **kw)


def _connected(**kw: Any) -> tuple[SyncTransport, MagicMock]:
    """A transport on a mocked, open connection owned by this thread."""
    transport = _transport(**kw)
    channel = MagicMock()
    channel.is_open = True
    with patch("rabbitkit.sync.transport.make_pika_connection_params"), patch("pika.BlockingConnection") as conn:
        conn.return_value.channel.return_value = channel
        conn.return_value.is_open = True
        conn.return_value.is_closed = False
        transport.connect()
    return transport, transport._connection


def _env(body: bytes = b"12345") -> MessageEnvelope:
    return MessageEnvelope(routing_key="q", body=body)


def _limiter(transport: SyncTransport) -> InFlightByteLimiter:
    limiter = transport.byte_limiter
    assert limiter is not None
    return limiter


def _confirmed(_envelope: MessageEnvelope) -> PublishOutcome:
    return PublishOutcome(status=PublishStatus.CONFIRMED)


class TestAdmission:
    def test_no_budget_by_default(self) -> None:
        assert _transport().byte_limiter is None

    def test_the_budget_and_admission_timeout_come_from_the_constructor(self) -> None:
        transport = _transport(max_in_flight_bytes=64, admission_timeout=2.5)
        assert _limiter(transport).limit == 64
        assert transport._admission_timeout == 2.5

    def test_a_body_larger_than_the_budget_is_not_sent(self) -> None:
        transport = _transport(max_in_flight_bytes=4)
        with patch.object(transport, "_publish") as inner:
            outcome = transport.publish(_env(b"12345"))
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "exceeds_max_in_flight_bytes")
        inner.assert_not_called()
        assert transport.publishes_in_flight == 0

    def test_no_room_within_the_admission_timeout_is_not_sent(self) -> None:
        transport = _transport(max_in_flight_bytes=5, admission_timeout=0.02)
        limiter = _limiter(transport)
        assert limiter.acquire(5, timeout=0.1)  # another publish holds the whole budget
        with patch.object(transport, "_publish") as inner:
            outcome = transport.publish(_env(b"1"))
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "admission_timeout")
        inner.assert_not_called()
        assert limiter.used == 5 and not limiter._waiters
        assert transport.publishes_in_flight == 0

    def test_a_waiting_publish_is_admitted_when_bytes_are_released(self) -> None:
        transport = _transport(max_in_flight_bytes=5, admission_timeout=5.0)
        limiter = _limiter(transport)
        assert limiter.acquire(5, timeout=0.1)
        threading.Timer(0.02, limiter.release, args=(5,)).start()
        with patch.object(transport, "_publish", side_effect=_confirmed):
            assert transport.publish(_env(b"123")).ok
        assert limiter.used == 0


class TestRelease:
    def test_bytes_are_held_while_publishing_and_released_on_success(self) -> None:
        transport = _transport(max_in_flight_bytes=10)
        seen: list[int] = []

        def inner(envelope: MessageEnvelope) -> PublishOutcome:
            seen.append(_limiter(transport).used)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        with patch.object(transport, "_publish", side_effect=inner):
            assert transport.publish(_env(b"123")).ok
        assert seen == [3]
        assert _limiter(transport).used == 0

    @pytest.mark.parametrize("status", [PublishStatus.NACKED, PublishStatus.UNKNOWN, PublishStatus.TIMEOUT])
    def test_released_on_a_failed_outcome(self, status: PublishStatus) -> None:
        transport = _transport(max_in_flight_bytes=10)
        with patch.object(transport, "_publish", return_value=PublishOutcome(status=status)):
            assert transport.publish(_env()).status is status
        assert _limiter(transport).used == 0

    def test_released_when_the_publish_raises(self) -> None:
        transport = _transport(max_in_flight_bytes=10)
        with patch.object(transport, "_publish", side_effect=RuntimeError("bug")), pytest.raises(RuntimeError):
            transport.publish(_env())
        assert _limiter(transport).used == 0
        assert transport.publishes_in_flight == 0

    def test_released_after_a_real_publish_attempt_on_a_mocked_connection(self) -> None:
        transport, _conn = _connected(max_in_flight_bytes=10)
        assert transport.publish(_env()).status is PublishStatus.CONFIRMED
        assert _limiter(transport).used == 0


class TestOwnerThreadPumps:
    """The owner thread of a consuming connection must keep the I/O loop
    running while it waits: a worker holding the bytes it needs is waiting
    for the owner to run that worker's publish."""

    def test_the_owner_of_a_consuming_connection_pumps_while_it_waits(self) -> None:
        transport, conn = _connected(max_in_flight_bytes=5, admission_timeout=5.0)
        transport._ever_consumed = True
        limiter = _limiter(transport)
        assert limiter.acquire(5, timeout=0.1)  # a worker's publish holds the budget
        slices: list[float] = []

        def pump(time_limit: float) -> None:
            slices.append(time_limit)
            if len(slices) == 2:  # the loop runs the worker's publish, which releases
                limiter.release(5)

        conn.process_data_events.side_effect = pump
        with patch.object(transport, "_publish", side_effect=_confirmed):
            assert transport.publish(_env(b"12")).ok
        assert len(slices) == 2 and all(0 < s <= 0.01 for s in slices)
        assert limiter.used == 0

    def test_the_owner_pumps_after_recovery_too(self) -> None:
        transport, conn = _connected(max_in_flight_bytes=5, admission_timeout=0.03)
        transport._ever_consumed_any = True  # a consume loop ran before a reconnect
        assert _limiter(transport).acquire(5, timeout=0.1)
        outcome = transport.publish(_env(b"1"))
        assert outcome.reason == "admission_timeout"
        assert conn.process_data_events.called

    def test_a_worker_thread_sleeps_instead_of_pumping(self) -> None:
        transport, conn = _connected(max_in_flight_bytes=5, admission_timeout=5.0)
        transport._ever_consumed = True
        limiter = _limiter(transport)
        assert limiter.acquire(5, timeout=0.1)
        outcomes: list[PublishOutcome] = []

        def worker() -> None:
            with patch.object(transport, "_publish", side_effect=_confirmed):
                outcomes.append(transport.publish(_env(b"2")))

        t = threading.Thread(target=worker)
        t.start()
        deadline = time.monotonic() + 5
        while not limiter._waiters and time.monotonic() < deadline:
            time.sleep(0.001)
        limiter.release(5)
        t.join(timeout=5)
        assert outcomes and outcomes[0].ok
        conn.process_data_events.assert_not_called()

    def test_a_pure_producer_owner_does_not_pump(self) -> None:
        transport, conn = _connected(max_in_flight_bytes=5, admission_timeout=0.02)
        assert _limiter(transport).acquire(5, timeout=0.1)
        assert transport.publish(_env(b"1")).reason == "admission_timeout"
        conn.process_data_events.assert_not_called()

    def test_pump_reports_a_missing_or_closed_connection(self) -> None:
        transport = _transport()
        assert transport._pump_for_budget(0.01) is False
        transport, conn = _connected()
        conn.is_open = False
        assert transport._pump_for_budget(0.01) is False
        conn.process_data_events.assert_not_called()

    def test_a_connection_lost_while_pumping_is_not_sent_and_holds_nothing(self) -> None:
        transport, conn = _connected(max_in_flight_bytes=5, admission_timeout=5.0)
        transport._ever_consumed = True
        limiter = _limiter(transport)
        assert limiter.acquire(5, timeout=0.1)
        conn.process_data_events.side_effect = pika.exceptions.StreamLostError("lost")
        outcome = transport.publish(_env(b"1"))
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "not_connected")
        assert isinstance(outcome.error, pika.exceptions.StreamLostError)
        assert limiter.used == 5 and not limiter._waiters

    def test_pikas_dead_poll_value_error_is_not_sent(self) -> None:
        transport, conn = _connected(max_in_flight_bytes=5, admission_timeout=5.0)
        transport._ever_consumed = True
        assert _limiter(transport).acquire(5, timeout=0.1)
        conn.process_data_events.side_effect = ValueError("Timeout closed before call")
        outcome = transport.publish(_env(b"1"))
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "not_connected")

    def test_another_value_error_while_pumping_propagates(self) -> None:
        transport, conn = _connected(max_in_flight_bytes=5, admission_timeout=5.0)
        transport._ever_consumed = True
        limiter = _limiter(transport)
        assert limiter.acquire(5, timeout=0.1)
        conn.process_data_events.side_effect = ValueError("a handler bug")
        with pytest.raises(ValueError, match="handler bug"):
            transport.publish(_env(b"1"))
        assert limiter.used == 5 and not limiter._waiters
        assert transport.publishes_in_flight == 0
