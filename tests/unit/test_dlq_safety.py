"""DLQInspector safety: issues #32-#36.

- #32 quorum delivery limits (management guard + x-delivery-count tripwire)
- #33 replay into the queue being drained
- #34 held messages released when anything raises or the task is cancelled
- #35 one channel per inspection
- #36 replay keeps the original's properties
"""

from __future__ import annotations

import asyncio
import contextlib
import urllib.error
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.errors import UnsafeToBrowseError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.dlq import DLQInspector, original_queue, original_routing_key

# ── fakes ─────────────────────────────────────────────────────────────────


def _msg(body: bytes = b"m", *, headers: dict[str, Any] | None = None, **kwargs: Any) -> RabbitMessage:
    defaults: dict[str, Any] = {"body": body, "routing_key": "orders.dlq", "headers": headers or {}}
    defaults.update(kwargs)
    msg = RabbitMessage(**defaults)
    msg._ack_fn = MagicMock()
    msg._nack_fn = MagicMock()

    async def _async_settle(*_a: Any) -> None:
        return None

    msg._ack_async_fn = MagicMock(side_effect=_async_settle)
    msg._nack_async_fn = MagicMock(side_effect=_async_settle)
    return msg


class _Transport:
    def __init__(self, messages: list[RabbitMessage], *, fail_after: int | None = None) -> None:
        self.queue = list(messages)
        self.published: list[MessageEnvelope] = []
        self.gets = 0
        self.fail_after = fail_after

    def basic_get(self, queue: str) -> RabbitMessage | None:
        if self.fail_after is not None and self.gets >= self.fail_after:
            raise ConnectionError("basic_get failed midway")
        self.gets += 1
        return self.queue.pop(0) if self.queue else None

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        self.published.append(envelope)
        return PublishOutcome(status=PublishStatus.CONFIRMED)


class _AsyncTransport(_Transport):
    async def basic_get(self, queue: str) -> RabbitMessage | None:  # type: ignore[override]
        return _Transport.basic_get(self, queue)

    async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:  # type: ignore[override]
        return _Transport.publish(self, envelope)


class _SelfFeedingTransport(_Transport):
    """A queue that never empties: every publish to it lands back on it.

    That is what replay without a target did to a broker dead-lettered
    message, whose routing key is the DLQ's own name (#33)."""

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        self.published.append(envelope)
        if envelope.exchange == "" and envelope.routing_key == "orders.dlq":
            self.queue.append(_msg(envelope.body))
        return PublishOutcome(status=PublishStatus.CONFIRMED)


def _deaths(queue: str = "orders", routing_key: str = "orders.created") -> list[dict[str, Any]]:
    return [{"queue": queue, "reason": "rejected", "count": 1, "exchange": "orders", "routing-keys": [routing_key]}]


# ── #33: where a replay goes ──────────────────────────────────────────────


class TestOriginalQueue:
    def test_rabbitkit_header_first(self) -> None:
        headers = {"x-rabbitkit-original-queue": "a", "x-last-death-queue": "b", "x-death": _deaths("c")}
        assert original_queue(headers) == "a"

    def test_empty_rabbitkit_header_is_absent(self) -> None:
        """The retry envelope writes "" when the header was missing; that
        used to route the replay to a queue named ""."""
        assert original_queue({"x-rabbitkit-original-queue": "", "x-last-death-queue": "b"}) == "b"

    def test_broker_last_death_queue(self) -> None:
        assert original_queue({"x-last-death-queue": "orders"}) == "orders"

    def test_newest_x_death_entry(self) -> None:
        assert original_queue({"x-death": [*_deaths("orders"), *_deaths("orders.retry.1")]}) == "orders"

    def test_bytes_values(self) -> None:
        assert original_queue({"x-last-death-queue": b"orders"}) == "orders"
        assert original_queue({"x-death": [{"queue": b"orders"}]}) == "orders"

    def test_nothing_usable(self) -> None:
        assert original_queue({}) is None
        assert original_queue({"x-death": "garbage"}) is None
        assert original_queue({"x-death": ["garbage"]}) is None


class TestOriginalRoutingKey:
    def test_rabbitkit_header_first(self) -> None:
        headers = {"x-rabbitkit-original-routing-key": "a.b", "x-death": _deaths(routing_key="c.d")}
        assert original_routing_key(headers) == "a.b"

    def test_x_death_routing_keys(self) -> None:
        assert original_routing_key({"x-death": _deaths(routing_key="orders.created")}) == "orders.created"

    def test_nothing_usable(self) -> None:
        assert original_routing_key({}) is None
        assert original_routing_key({"x-death": [{"routing-keys": []}]}) is None
        assert original_routing_key({"x-death": [{"queue": "q"}]}) is None


class TestSelfReplay:
    def test_broker_dead_lettered_message_routes_to_its_source(self) -> None:
        transport = _Transport([_msg(headers={"x-death": _deaths("orders")})])
        result = DLQInspector(transport).replay("orders.dlq")
        assert int(result) == 1
        assert transport.published[0].routing_key == "orders"

    def test_unroutable_message_is_skipped_not_replayed_into_the_dlq(self) -> None:
        """No origin anywhere: the routing key is the DLQ's own name."""
        msg = _msg()
        transport = _SelfFeedingTransport([msg])
        result = DLQInspector(transport).replay("orders.dlq")  # limit=None used to loop forever
        assert int(result) == 0
        assert result.skipped == 1
        assert transport.published == []
        msg._nack_fn.assert_called_once_with(True)  # left on the DLQ
        assert "skipped=1" in repr(result)

    def test_allow_self_replay_opts_in(self) -> None:
        transport = _Transport([_msg()])
        result = DLQInspector(transport).replay("orders.dlq", allow_self_replay=True)
        assert int(result) == 1 and transport.published[0].routing_key == "orders.dlq"

    def test_explicit_exchange_is_not_self_replay(self) -> None:
        transport = _Transport([_msg()])
        result = DLQInspector(transport).replay("orders.dlq", target_exchange="orders")
        assert int(result) == 1

    async def test_async_skips_too(self) -> None:
        transport = _AsyncTransport([_msg()])
        result = await DLQInspector(transport).replay_async("orders.dlq")
        assert result.skipped == 1 and transport.published == []


# ── #36: replay keeps the original's properties ───────────────────────────


class TestReplayProperties:
    def _envelope(self, msg: RabbitMessage) -> MessageEnvelope:
        return DLQInspector._build_replay_envelope(msg, "orders", None, False)

    def test_timestamp_and_delivery_mode_survive(self) -> None:
        ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        env = self._envelope(_msg(timestamp=ts, delivery_mode=1, expiration="1001"))
        assert env.timestamp == ts
        assert env.delivery_mode == 1
        assert env.expiration == "1001"

    def test_absent_properties_stay_absent(self) -> None:
        env = self._envelope(_msg())  # no message_id, content_type, delivery_mode
        assert env.message_id == ""  # transports send "" as no property
        assert env.content_type == ""
        assert env.delivery_mode == 2  # unknown: never less durable than replay always was

    def test_explicit_transient_stays_transient(self) -> None:
        assert self._envelope(_msg(delivery_mode=1)).delivery_mode == 1

    def test_persistent_stays_persistent(self) -> None:
        assert self._envelope(_msg(delivery_mode=2)).delivery_mode == 2

    def test_present_properties_copied(self) -> None:
        env = self._envelope(_msg(message_id="m1", content_type="application/json"))
        assert env.message_id == "m1" and env.content_type == "application/json"


# ── #34: nothing stays unacked when something raises ──────────────────────


class TestReleaseOnError:
    def test_peek_releases_when_basic_get_fails_midway(self) -> None:
        held = [_msg(b"1"), _msg(b"2")]
        transport = _Transport([*held, _msg(b"3")], fail_after=2)
        with pytest.raises(ConnectionError):
            DLQInspector(transport).peek("orders.dlq", limit=5)
        for m in held:
            m._nack_fn.assert_called_once_with(True)

    def test_replay_releases_when_the_predicate_raises(self) -> None:
        held = [_msg(b"keep"), _msg(b"boom")]

        def predicate(m: RabbitMessage) -> bool:
            if m.body == b"boom":
                raise ValueError("bad predicate")
            return False

        with pytest.raises(ValueError, match="bad predicate"):
            DLQInspector(_Transport(held)).replay("orders.dlq", predicate=predicate, target_queue="orders")
        for m in held:
            m._nack_fn.assert_called_once_with(True)

    def test_a_failing_release_does_not_mask_the_original_error(self) -> None:
        msg = _msg()
        msg._nack_fn.side_effect = RuntimeError("channel gone")
        transport = _Transport([msg], fail_after=1)
        with pytest.raises(ConnectionError):
            DLQInspector(transport).peek("orders.dlq")

    def test_acked_messages_are_not_requeued(self) -> None:
        msgs = [_msg(b"1"), _msg(b"2")]
        DLQInspector(_Transport(msgs)).replay("orders.dlq", target_queue="orders")
        for m in msgs:
            m._ack_fn.assert_called_once()
            m._nack_fn.assert_not_called()

    async def test_peek_async_releases_on_cancellation(self) -> None:
        held = _msg()
        gate = asyncio.Event()

        class _Slow(_AsyncTransport):
            async def basic_get(self, queue: str) -> RabbitMessage | None:  # type: ignore[override]
                if self.gets == 0:
                    self.gets += 1
                    return held
                gate.set()
                await asyncio.sleep(3600)
                return None

        task = asyncio.create_task(DLQInspector(_Slow([])).peek_async("orders.dlq"))
        await gate.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        held._nack_async_fn.assert_called_once_with(True)

    async def test_replay_async_releases_when_the_predicate_raises(self) -> None:
        held = _msg()
        with pytest.raises(ZeroDivisionError):
            await DLQInspector(_AsyncTransport([held])).replay_async(
                "orders.dlq", predicate=lambda m: bool(1 / 0), target_queue="orders"
            )
        held._nack_async_fn.assert_called_once_with(True)


# ── #35: one channel per inspection ───────────────────────────────────────


class _SessionTransport(_Transport):
    """Has inspection_session(): each operation gets its own session."""

    def __init__(self, messages: list[RabbitMessage]) -> None:
        super().__init__(messages)
        self.sessions_opened = 0
        self.sessions_closed = 0
        self.direct_gets = 0

    def basic_get(self, queue: str) -> RabbitMessage | None:
        self.direct_gets += 1
        return None

    @contextlib.contextmanager
    def inspection_session(self) -> Iterator[Any]:
        self.sessions_opened += 1
        outer = self

        class _S:
            def basic_get(self, queue: str) -> RabbitMessage | None:
                return outer.queue.pop(0) if outer.queue else None

        try:
            yield _S()
        finally:
            self.sessions_closed += 1


class _AsyncSessionTransport(_AsyncTransport):
    def __init__(self, messages: list[RabbitMessage]) -> None:
        super().__init__(messages)
        self.sessions_opened = 0
        self.sessions_closed = 0

    @contextlib.asynccontextmanager
    async def inspection_session(self) -> AsyncIterator[Any]:
        self.sessions_opened += 1
        outer = self

        class _S:
            async def basic_get(self, queue: str) -> RabbitMessage | None:
                return outer.queue.pop(0) if outer.queue else None

        try:
            yield _S()
        finally:
            self.sessions_closed += 1


class TestSessions:
    def test_peek_and_replay_use_their_own_session(self) -> None:
        transport = _SessionTransport([_msg(b"1"), _msg(b"2")])
        inspector = DLQInspector(transport)
        assert len(inspector.peek("orders.dlq")) == 2
        inspector.replay("orders.dlq", target_queue="orders")
        assert (transport.sessions_opened, transport.sessions_closed) == (2, 2)
        assert transport.direct_gets == 0

    def test_session_closed_when_the_operation_raises(self) -> None:
        transport = _SessionTransport([_msg()])
        with pytest.raises(ValueError):
            DLQInspector(transport).replay("orders.dlq", predicate=MagicMock(side_effect=ValueError))
        assert transport.sessions_closed == 1

    async def test_async_sessions(self) -> None:
        transport = _AsyncSessionTransport([_msg(b"1")])
        inspector = DLQInspector(transport)
        assert len(await inspector.peek_async("orders.dlq")) == 1
        await inspector.replay_async("orders.dlq", target_queue="orders")
        assert (transport.sessions_opened, transport.sessions_closed) == (2, 2)

    def test_magicmock_transport_is_not_mistaken_for_a_session_provider(self) -> None:
        """Class-level lookup: a MagicMock answers every attribute, and a
        mocked session's basic_get never returns None (the loop would spin)."""
        transport = MagicMock()
        transport.basic_get.side_effect = [_msg(), None]
        assert len(DLQInspector(transport).peek("q")) == 1


# ── #32: quorum delivery limits ───────────────────────────────────────────


def _management(info: dict[str, Any] | Exception, version: Any = "4.1.8") -> MagicMock:
    client = MagicMock()
    if isinstance(info, Exception):
        client.get_queue.side_effect = info
    else:
        client.get_queue.return_value = info
    if isinstance(version, Exception):
        client.overview.side_effect = version
    else:
        client.overview.return_value = {"rabbitmq_version": version}
    return client


def _quorum(limit: Any = None) -> dict[str, Any]:
    args: dict[str, Any] = {"x-queue-type": "quorum"}
    if limit is not None:
        args["x-delivery-limit"] = limit
    return {"type": "quorum", "arguments": args, "effective_policy_definition": {}, "messages": 1}


def _not_found() -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://x/api/queues/%2F/q", 404, "Not Found", None, None)  # type: ignore[arg-type]


class TestManagementGuard:
    def test_limited_quorum_queue_is_refused_before_any_fetch(self) -> None:
        transport = _Transport([_msg()])
        inspector = DLQInspector(transport, management=_management(_quorum()))
        with pytest.raises(UnsafeToBrowseError, match="delivery limit of 20"):
            inspector.peek("orders.dlq")
        assert transport.gets == 0

    def test_unlimited_quorum_queue_is_peeked(self) -> None:
        transport = _Transport([_msg(headers={"x-delivery-count": 3})])
        management = _management(_quorum(-1))
        inspector = DLQInspector(transport, management=management, vhost="prod")
        assert len(inspector.peek("orders.dlq")) == 1
        management.get_queue.assert_called_once_with("orders.dlq", vhost="prod")

    def test_classic_queue_is_peeked(self) -> None:
        info = {"type": "classic", "arguments": {}, "messages": 1}
        assert len(DLQInspector(_Transport([_msg()]), management=_management(info)).peek("q")) == 1

    def test_version_is_cached(self) -> None:
        management = _management(_quorum(-1))
        inspector = DLQInspector(_Transport([]), management=management)
        inspector.peek("q")
        inspector.peek("q")
        management.overview.assert_called_once()

    def test_missing_queue_defers_to_amqp(self) -> None:
        transport = _Transport([])
        assert DLQInspector(transport, management=_management(_not_found())).peek("nope") == []

    def test_management_error_fails_closed(self) -> None:
        inspector = DLQInspector(_Transport([_msg()]), management=_management(OSError("down")))
        with pytest.raises(UnsafeToBrowseError, match="management API"):
            inspector.peek("q")

    def test_version_error_fails_closed(self) -> None:
        inspector = DLQInspector(_Transport([]), management=_management(_quorum(-1), version=OSError("x")))
        with pytest.raises(UnsafeToBrowseError, match="version"):
            inspector.peek("q")

    def test_missing_version_fails_closed_for_quorum(self) -> None:
        inspector = DLQInspector(_Transport([]), management=_management(_quorum(-1), version=None))
        with pytest.raises(UnsafeToBrowseError, match="unknown"):
            inspector.peek("q")

    def test_filtered_replay_is_guarded(self) -> None:
        inspector = DLQInspector(_Transport([_msg()]), management=_management(_quorum()))
        with pytest.raises(UnsafeToBrowseError):
            inspector.replay("q", predicate=lambda m: True, target_queue="orders")

    def test_unfiltered_replay_is_allowed(self) -> None:
        """It acks what it republishes; only failures are requeued."""
        transport = _Transport([_msg()])
        management = _management(_quorum())
        result = DLQInspector(transport, management=management).replay("q", target_queue="orders")
        assert int(result) == 1
        management.get_queue.assert_not_called()

    def test_check_can_be_disabled(self) -> None:
        management = _management(_quorum())
        inspector = DLQInspector(_Transport([_msg()]), management=management, check_delivery_limit=False)
        assert len(inspector.peek("q")) == 1
        management.get_queue.assert_not_called()

    async def test_async_guard_runs_off_the_loop(self) -> None:
        inspector = DLQInspector(_AsyncTransport([_msg()]), management=_management(_quorum()))
        with pytest.raises(UnsafeToBrowseError):
            await inspector.peek_async("q")
        with pytest.raises(UnsafeToBrowseError):
            await inspector.replay_async("q", predicate=lambda m: True)

    async def test_async_unlimited_is_peeked(self) -> None:
        inspector = DLQInspector(_AsyncTransport([_msg()]), management=_management(_quorum(-1)))
        assert len(await inspector.peek_async("q")) == 1


class TestTripwire:
    """No management client: x-delivery-count proves the queue is quorum."""

    def test_peek_stops_at_the_first_counted_message(self) -> None:
        first, counted, never = _msg(b"1"), _msg(b"2", headers={"x-delivery-count": 1}), _msg(b"3")
        transport = _Transport([first, counted, never])
        inspector = DLQInspector(transport)
        with pytest.raises(UnsafeToBrowseError, match="quorum queue"):
            inspector.peek("orders.dlq")
        assert transport.gets == 2  # stopped; the third was never fetched
        first._nack_fn.assert_called_once_with(True)
        counted._nack_fn.assert_called_once_with(True)

    def test_second_peek_refuses_without_fetching(self) -> None:
        transport = _Transport([_msg(headers={"x-delivery-count": 1})])
        inspector = DLQInspector(transport)
        with pytest.raises(UnsafeToBrowseError):
            inspector.peek("orders.dlq")
        gets = transport.gets
        with pytest.raises(UnsafeToBrowseError):
            inspector.peek("orders.dlq")
        assert transport.gets == gets

    def test_fresh_quorum_messages_carry_no_count(self) -> None:
        """First delivery of a quorum message has no header: one peek costs one delivery."""
        assert len(DLQInspector(_Transport([_msg(), _msg()])).peek("q")) == 2

    def test_filtered_replay_trips(self) -> None:
        transport = _Transport([_msg(headers={"x-delivery-count": 2})])
        with pytest.raises(UnsafeToBrowseError):
            DLQInspector(transport).replay("q", predicate=lambda m: False, target_queue="orders")

    def test_unfiltered_replay_does_not_trip(self) -> None:
        transport = _Transport([_msg(headers={"x-delivery-count": 2})])
        assert int(DLQInspector(transport).replay("q", target_queue="orders")) == 1

    def test_disabled(self) -> None:
        transport = _Transport([_msg(headers={"x-delivery-count": 2})])
        assert len(DLQInspector(transport, check_delivery_limit=False).peek("q")) == 1

    async def test_async(self) -> None:
        inspector = DLQInspector(_AsyncTransport([_msg(headers={"x-delivery-count": 1})]))
        with pytest.raises(UnsafeToBrowseError):
            await inspector.peek_async("q")
        with pytest.raises(UnsafeToBrowseError):
            await inspector.peek_async("q")  # remembered
        with pytest.raises(UnsafeToBrowseError):
            await DLQInspector(_AsyncTransport([_msg(headers={"x-delivery-count": 1})])).replay_async(
                "q", predicate=lambda m: True
            )


class TestReleaseHelpers:
    def test_sync_skips_settled_and_survives_errors(self) -> None:
        from rabbitkit.dlq import _release_messages

        settled, failing, ok = _msg(), _msg(), _msg()
        settled.ack()
        failing._nack_fn.side_effect = RuntimeError("channel closed")
        _release_messages([settled, failing, ok])
        settled._nack_fn.assert_not_called()
        ok._nack_fn.assert_called_once_with(True)

    async def test_async_skips_settled_and_survives_errors(self) -> None:
        from rabbitkit.dlq import _release_messages_async

        settled, failing, ok = _msg(), _msg(), _msg()
        await settled.ack_async()
        failing._nack_async_fn.side_effect = RuntimeError("channel closed")
        await _release_messages_async([settled, failing, ok])
        settled._nack_async_fn.assert_not_called()
        ok._nack_async_fn.assert_called_once_with(True)


class TestReviewFindings:
    def test_vhost_defaults_to_the_transports(self) -> None:
        transport = _Transport([])
        transport._connection_config = MagicMock(vhost="orders/eu")  # type: ignore[attr-defined]
        management = _management(_quorum(-1))
        DLQInspector(transport, management=management).peek("q")
        management.get_queue.assert_called_once_with("q", vhost="orders/eu")

    def test_explicit_vhost_wins(self) -> None:
        transport = _Transport([])
        transport._connection_config = MagicMock(vhost="a")  # type: ignore[attr-defined]
        management = _management(_quorum(-1))
        DLQInspector(transport, management=management, vhost="b").peek("q")
        management.get_queue.assert_called_once_with("q", vhost="b")

    def test_management_404_keeps_the_tripwire_armed(self) -> None:
        """A wrong vhost makes every lookup a 404; that must not disable the
        x-delivery-count check as well."""
        transport = _Transport([_msg(headers={"x-delivery-count": 1})])
        with pytest.raises(UnsafeToBrowseError, match="quorum queue"):
            DLQInspector(transport, management=_management(_not_found())).peek("q")

    def test_unfiltered_replay_vets_at_the_first_failure(self) -> None:
        good = _msg(b"good", headers={"x-rabbitkit-original-queue": "orders"})
        orphan = _msg(b"orphan")  # would be self-replayed: skipped, i.e. requeued
        later = _msg(b"later", headers={"x-rabbitkit-original-queue": "orders"})
        transport = _Transport([good, orphan, later])
        management = _management(_quorum())  # default limit 20
        with pytest.raises(UnsafeToBrowseError):
            DLQInspector(transport, management=management).replay("orders.dlq")
        good._ack_fn.assert_called_once()  # replayed before the check
        orphan._nack_fn.assert_called_once_with(True)  # released
        assert transport.gets == 2  # stopped before fetching the rest
        management.get_queue.assert_called_once()

    def test_unfiltered_replay_vets_once(self) -> None:
        msgs = [_msg(), _msg()]
        management = _management(_quorum(-1))
        result = DLQInspector(_Transport(msgs), management=management).replay("orders.dlq")
        assert result.skipped == 2
        management.get_queue.assert_called_once()

    def test_unfiltered_replay_tripwire_on_a_counted_failure(self) -> None:
        transport = _Transport([_msg(headers={"x-delivery-count": 4})])
        with pytest.raises(UnsafeToBrowseError):
            DLQInspector(transport).replay("orders.dlq")

    def test_unfiltered_replay_unaffected_when_nothing_is_requeued(self) -> None:
        transport = _Transport([_msg(headers={"x-delivery-count": 4})])
        management = _management(_quorum())
        assert int(DLQInspector(transport, management=management).replay("q", target_queue="orders")) == 1
        management.get_queue.assert_not_called()

    async def test_async_unfiltered_replay_vets_at_the_first_failure(self) -> None:
        transport = _AsyncTransport([_msg()])
        with pytest.raises(UnsafeToBrowseError):
            await DLQInspector(transport, management=_management(_quorum())).replay_async("orders.dlq")

    async def test_async_unfiltered_replay_tripwire(self) -> None:
        class _Failing(_AsyncTransport):
            async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:  # type: ignore[override]
                return PublishOutcome(status=PublishStatus.RETURNED)

        transport = _Failing([_msg(headers={"x-delivery-count": 2})])
        with pytest.raises(UnsafeToBrowseError):
            await DLQInspector(transport).replay_async("orders.dlq", target_queue="orders")


# ── quorum queues return messages to the back: read them whole ────────────


def _ordered(n: int, released: list[int]) -> list[RabbitMessage]:
    """Messages that record the order they are requeued in."""
    out = []
    for i in range(n):
        msg = _msg(str(i).encode())
        msg._nack_fn = MagicMock(side_effect=lambda *_a, i=i: released.append(i))

        async def _nack(*_a: Any, i: int = i) -> None:
            released.append(i)

        msg._nack_async_fn = MagicMock(side_effect=_nack)
        out.append(msg)
    return out


def _quorum_with(messages: int) -> dict[str, Any]:
    return {**_quorum(-1), "messages": messages, "messages_ready": messages}


class TestQuorumOrder:
    """Measured on 3.13 and 4.1: a quorum queue puts returned messages at the
    back, so peeking part of one rotated what it read to the tail."""

    def test_a_quorum_queue_is_read_whole_and_requeued_in_order(self) -> None:
        released: list[int] = []
        transport = _Transport(_ordered(6, released))
        inspector = DLQInspector(transport, management=_management(_quorum_with(6)))
        peeked = inspector.peek("q", limit=2)
        assert [m.body for m in peeked] == [b"0", b"1"]
        assert transport.gets == 7  # all six, then the empty get that proves the end
        assert released == [0, 1, 2, 3, 4, 5]  # a full rotation: the order is as it was

    def test_a_classic_queue_reads_only_the_limit(self) -> None:
        transport = _Transport([_msg() for _ in range(6)])
        info = {"type": "classic", "arguments": {}, "messages": 6}
        assert len(DLQInspector(transport, management=_management(info)).peek("q", limit=2)) == 2
        assert transport.gets == 2

    def test_a_quorum_queue_deeper_than_one_scan_is_refused_before_any_read(self) -> None:
        transport = _Transport([_msg() for _ in range(10)])
        inspector = DLQInspector(transport, management=_management(_quorum_with(10)), max_quorum_scan=5)
        with pytest.raises(UnsafeToBrowseError, match="only whole"):
            inspector.peek("q")
        assert transport.gets == 0

    def test_a_quorum_queue_that_grows_while_it_is_read_is_refused_and_released(self) -> None:
        released: list[int] = []
        transport = _Transport(_ordered(7, released))  # the stats said 3; 7 are there
        inspector = DLQInspector(transport, management=_management(_quorum_with(3)), max_quorum_scan=5)
        with pytest.raises(UnsafeToBrowseError, match="holding 6 messages"):
            inspector.peek("q")
        assert released == [0, 1, 2, 3, 4, 5]

    def test_a_filtered_replay_with_a_limit_is_refused_on_a_quorum_queue(self) -> None:
        inspector = DLQInspector(_Transport([_msg()]), management=_management(_quorum_with(1)))
        with pytest.raises(UnsafeToBrowseError, match="reorders the queue"):
            inspector.replay("q", predicate=lambda m: True, target_queue="t", limit=5)
        assert int(inspector.replay("q", predicate=lambda m: True, target_queue="t")) == 1

    async def test_async_reads_whole_and_refuses_a_partial_filtered_replay(self) -> None:
        released: list[int] = []
        transport = _AsyncTransport(_ordered(4, released))
        inspector = DLQInspector(transport, management=_management(_quorum_with(4)))
        assert [m.body for m in await inspector.peek_async("q", limit=1)] == [b"0"]
        assert released == [0, 1, 2, 3]
        with pytest.raises(UnsafeToBrowseError, match="reorders the queue"):
            await inspector.replay_async("q", predicate=lambda m: True, limit=2)
        grown = DLQInspector(_AsyncTransport([_msg() for _ in range(3)]),
                             management=_management(_quorum_with(1)), max_quorum_scan=2)
        with pytest.raises(UnsafeToBrowseError, match="holding 3 messages"):
            await grown.peek_async("q")
