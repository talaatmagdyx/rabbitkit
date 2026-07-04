"""Tests for sync/batch.py — SyncBatchPublisher (mocked pika SelectConnection).

The fakes below stand in for pika's SelectConnection/Channel: the publisher's
I/O thread drives the fake ioloop, and the TEST thread invokes the captured
pika callbacks (ack/nack/return/close) directly — simulating frames arriving
on the I/O thread. All shared publisher state is lock-guarded, so invoking
callbacks from the test thread is equivalent and fully deterministic.
"""

from __future__ import annotations

import random
import sys
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from rabbitkit.core.config import ConnectionConfig
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.sync.batch import IO_THREAD_NAME, SyncBatchPublisher, _Slot

pika = pytest.importorskip("pika")

# ── fakes ─────────────────────────────────────────────────────────────────


class FakeIOLoop:
    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn
        self._stopped = threading.Event()

    def start(self) -> None:
        self._conn._run()
        self._stopped.wait(timeout=30)

    def stop(self) -> None:
        self._stopped.set()

    def add_callback_threadsafe(self, cb: Any) -> None:
        if not self._conn.is_open:
            raise pika.exceptions.ConnectionWrongStateError("connection closed")
        cb()  # inline — equivalent to the ioloop dispatching it (state is locked)


class FakeChannel:
    def __init__(self, conn: FakeConnection, auto: str | None = None) -> None:
        self._conn = conn
        self.auto = auto  # "ack" | "nack" | None
        self.is_open = True
        self.ack_nack_cb: Any = None
        self.return_cb: Any = None
        self.close_cb: Any = None
        self.published: list[SimpleNamespace] = []
        self.publish_raises: BaseException | None = None

    def add_on_close_callback(self, cb: Any) -> None:
        self.close_cb = cb

    def add_on_return_callback(self, cb: Any) -> None:
        self.return_cb = cb

    def confirm_delivery(self, ack_nack_callback: Any = None, callback: Any = None) -> None:
        self.ack_nack_cb = ack_nack_callback
        if callback is not None:  # Confirm.SelectOk
            callback(SimpleNamespace(method=pika.spec.Confirm.SelectOk()))

    def basic_publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        properties: Any = None,
        mandatory: bool = False,
    ) -> None:
        if self.publish_raises is not None:
            raise self.publish_raises
        self.published.append(
            SimpleNamespace(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties,
                mandatory=mandatory,
            )
        )
        tag = len(self.published)
        if self.auto == "ack":
            self.ack(tag)
        elif self.auto == "nack":
            self.nack(tag)

    # test drivers — simulate broker frames arriving
    def ack(self, tag: int, multiple: bool = False) -> None:
        self.ack_nack_cb(SimpleNamespace(method=pika.spec.Basic.Ack(delivery_tag=tag, multiple=multiple)))

    def nack(self, tag: int, multiple: bool = False) -> None:
        self.ack_nack_cb(SimpleNamespace(method=pika.spec.Basic.Nack(delivery_tag=tag, multiple=multiple)))

    def deliver_return(
        self, exchange: str = "", routing_key: str = "", message_id: str | None = None, body: bytes = b""
    ) -> None:
        method = SimpleNamespace(
            exchange=exchange, routing_key=routing_key, reply_code=312, reply_text="NO_ROUTE"
        )
        self.return_cb(self, method, SimpleNamespace(message_id=message_id), body)

    def close(self, reason: BaseException | None = None) -> None:
        if not self.is_open:
            return
        self.is_open = False
        if self.close_cb is not None:
            self.close_cb(self, reason or RuntimeError("channel closed"))


class FakeConnection:
    def __init__(
        self,
        factory: ConnFactory,
        parameters: Any = None,
        on_open_callback: Any = None,
        on_open_error_callback: Any = None,
        on_close_callback: Any = None,
    ) -> None:
        self._factory = factory
        self.parameters = parameters
        self.ioloop = FakeIOLoop(self)
        self._on_open = on_open_callback
        self._on_open_error = on_open_error_callback
        self._on_close = on_close_callback
        self.is_open = False
        self.channel_obj: FakeChannel | None = None
        self.mode = factory.next_mode()  # "ok" | "fail" | "hang"

    def _run(self) -> None:
        if self.mode == "fail":
            self._on_open_error(self, RuntimeError("connection refused"))
        elif self.mode == "hang":
            self.is_open = True  # accepts TCP, never completes the AMQP handshake
        else:
            self.is_open = True
            self._on_open(self)

    def channel(self, on_open_callback: Any = None) -> FakeChannel:
        self.channel_obj = FakeChannel(self, auto=self._factory.auto)
        on_open_callback(self.channel_obj)
        return self.channel_obj

    def close(self, *_args: Any) -> None:
        if not self.is_open:
            return
        self.is_open = False
        if self.channel_obj is not None:
            self.channel_obj.close()
        self._on_close(self, RuntimeError("connection closed"))

    def die(self) -> None:
        """Simulate unexpected connection death (broker/network gone)."""
        self.close()


class ConnFactory:
    """Callable standing in for ``pika.SelectConnection``; records instances."""

    def __init__(self, modes: tuple[str, ...] = (), auto: str | None = None) -> None:
        self._modes = list(modes)  # per-instance connect mode; "ok" when exhausted
        self.auto = auto
        self.instances: list[FakeConnection] = []

    def next_mode(self) -> str:
        return self._modes.pop(0) if self._modes else "ok"

    def __call__(self, parameters: Any = None, **callbacks: Any) -> FakeConnection:
        mode = self._modes[0] if self._modes else "ok"
        if mode == "raise":
            self._modes.pop(0)
            raise RuntimeError("SelectConnection constructor boom")
        conn = FakeConnection(self, parameters=parameters, **callbacks)
        self.instances.append(conn)
        return conn


# ── helpers ───────────────────────────────────────────────────────────────


def _env(**kw: Any) -> MessageEnvelope:
    kw.setdefault("routing_key", "test.q")
    kw.setdefault("body", b'{"x":1}')
    return MessageEnvelope(**kw)


@contextmanager
def running_publisher(
    factory: ConnFactory,
    confirm_timeout: float = 5.0,
    max_attempts: int = 2,
    close_timeout: float = 2.0,
):
    pub = SyncBatchPublisher(
        connection_config=ConnectionConfig(reconnect_backoff_base=0.001, reconnect_backoff_max=0.002),
        confirm_timeout=confirm_timeout,
    )
    pub.max_reconnect_attempts = max_attempts
    with patch("pika.SelectConnection", factory):
        pub.start(ready_timeout=5.0)
        try:
            yield pub
        finally:
            pub.close(timeout=close_timeout)


def _publish_async(
    pub: SyncBatchPublisher, env: MessageEnvelope, timeout: float | None = None
) -> tuple[threading.Thread, dict[str, PublishOutcome]]:
    box: dict[str, PublishOutcome] = {}

    def run() -> None:
        box["outcome"] = pub.publish(env, timeout=timeout)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, box


def _wait_until(cond: Any, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        if time.monotonic() >= deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.001)


# ── start / ready handshake ───────────────────────────────────────────────


class TestStart:
    def test_start_ready_handshake(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            assert pub._ready.is_set()
            assert pub._thread is not None
            assert pub._thread.name == IO_THREAD_NAME
            assert pub._thread.daemon
            ch = factory.instances[0].channel_obj
            assert ch is not None
            assert ch.ack_nack_cb is not None  # Basic.Ack/Nack wired
            assert ch.return_cb is not None  # Basic.Return wired
        assert len(factory.instances) == 1

    def test_start_idempotent(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            pub.start()  # second start is a no-op
            assert len(factory.instances) == 1

    def test_start_after_close_raises(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            pass
        with pytest.raises(RuntimeError, match="closed"):
            pub.start()

    def test_start_connect_failure_raises_after_bounded_attempts(self) -> None:
        factory = ConnFactory(modes=("fail", "fail", "fail", "fail"))
        pub = SyncBatchPublisher(
            connection_config=ConnectionConfig(reconnect_backoff_base=0.001, reconnect_backoff_max=0.002)
        )
        pub.max_reconnect_attempts = 1
        with patch("pika.SelectConnection", factory), pytest.raises(RuntimeError, match="failed to connect"):
            pub.start(ready_timeout=5.0)

    def test_start_constructor_error_raises_after_bounded_attempts(self) -> None:
        factory = ConnFactory(modes=("raise", "raise", "raise"))
        pub = SyncBatchPublisher(
            connection_config=ConnectionConfig(reconnect_backoff_base=0.001, reconnect_backoff_max=0.002)
        )
        pub.max_reconnect_attempts = 1
        with patch("pika.SelectConnection", factory), pytest.raises(RuntimeError, match="failed to connect"):
            pub.start(ready_timeout=5.0)

    def test_start_ready_timeout(self) -> None:
        factory = ConnFactory(modes=("hang",))
        pub = SyncBatchPublisher(
            connection_config=ConnectionConfig(reconnect_backoff_base=0.001, reconnect_backoff_max=0.002)
        )
        with patch("pika.SelectConnection", factory), pytest.raises(TimeoutError, match="not ready"):
            pub.start(ready_timeout=0.05)

    def test_missing_pika_raises(self) -> None:
        pub = SyncBatchPublisher()
        with patch.dict(sys.modules, {"pika": None}), pytest.raises(RuntimeError) as excinfo:
            pub.start(ready_timeout=5.0)
        assert isinstance(excinfo.value.__cause__, ImportError)
        assert "rabbitkit[sync]" in str(excinfo.value.__cause__)


# ── publish → confirm ─────────────────────────────────────────────────────


class TestPublishConfirm:
    def test_publish_ack_confirmed(self) -> None:
        factory = ConnFactory(auto="ack")
        with running_publisher(factory) as pub:
            outcome = pub.publish(_env())
            assert outcome.status is PublishStatus.CONFIRMED
            assert outcome.ok
            assert outcome.delivery_tag == 1
            assert outcome.routing_key == "test.q"
            ch = factory.instances[0].channel_obj
            assert ch is not None and len(ch.published) == 1

    def test_publish_nack_nacked(self) -> None:
        factory = ConnFactory(auto="nack")
        with running_publisher(factory) as pub:
            outcome = pub.publish(_env())
            assert outcome.status is PublishStatus.NACKED
            assert not outcome.ok
            assert outcome.error is not None

    def test_multiple_ack_settles_all_tags_up_to(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            workers = [_publish_async(pub, _env()) for _ in range(3)]
            _wait_until(lambda: len(ch.published) == 3)
            ch.ack(3, multiple=True)
            for t, box in workers:
                t.join(timeout=5.0)
                assert box["outcome"].status is PublishStatus.CONFIRMED
            assert sorted(box["outcome"].delivery_tag for _, box in workers) == [1, 2, 3]
            assert pub._pending == {}

    def test_multiple_nack_then_single_ack(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            workers = [_publish_async(pub, _env()) for _ in range(3)]
            _wait_until(lambda: len(ch.published) == 3)
            ch.nack(2, multiple=True)  # settles tags 1 and 2
            ch.ack(3)
            statuses = []
            for t, box in workers:
                t.join(timeout=5.0)
                statuses.append(box["outcome"].status)
            assert sorted(statuses, key=str) == [
                PublishStatus.CONFIRMED,
                PublishStatus.NACKED,
                PublishStatus.NACKED,
            ]

    def test_confirm_for_unknown_tag_is_ignored(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            ch.ack(99)  # no pending tags — must not raise
            assert pub._pending == {}


# ── Basic.Return ──────────────────────────────────────────────────────────


class TestReturn:
    def test_return_then_ack_is_returned_not_confirmed(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            env = _env(mandatory=True)
            t, box = _publish_async(pub, env)
            _wait_until(lambda: len(ch.published) == 1)
            # pika delivers the Return BEFORE the Ack for the same publish.
            ch.deliver_return(exchange="", routing_key="test.q", message_id=env.message_id)
            ch.ack(1)  # must NOT overwrite RETURNED (first settlement wins)
            t.join(timeout=5.0)
            assert box["outcome"].status is PublishStatus.RETURNED
            assert box["outcome"].error is not None
            assert pub._pending == {}  # the Ack still reaped the tag

    def test_return_matches_by_message_id(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            env1, env2 = _env(), _env()
            t1, box1 = _publish_async(pub, env1)
            _wait_until(lambda: len(ch.published) == 1)
            t2, box2 = _publish_async(pub, env2)
            _wait_until(lambda: len(ch.published) == 2)
            ch.deliver_return(exchange="", routing_key="test.q", message_id=env1.message_id)
            ch.ack(2, multiple=True)
            t1.join(timeout=5.0)
            t2.join(timeout=5.0)
            assert box1["outcome"].status is PublishStatus.RETURNED
            assert box2["outcome"].status is PublishStatus.CONFIRMED

    def test_return_without_message_id_matches_most_recent(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            t1, box1 = _publish_async(pub, _env())
            _wait_until(lambda: len(ch.published) == 1)
            t2, box2 = _publish_async(pub, _env())
            _wait_until(lambda: len(ch.published) == 2)
            ch.deliver_return(exchange="", routing_key="test.q", message_id=None)
            ch.ack(2, multiple=True)
            t1.join(timeout=5.0)
            t2.join(timeout=5.0)
            assert box1["outcome"].status is PublishStatus.CONFIRMED
            assert box2["outcome"].status is PublishStatus.RETURNED  # most recent match

    def test_return_with_no_matching_publish_is_ignored(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            # An unsettled publish for a DIFFERENT routing key must not match.
            t, box = _publish_async(pub, _env(routing_key="other.q"))
            _wait_until(lambda: len(ch.published) == 1)
            ch.deliver_return(exchange="", routing_key="nobody.published.this")
            ch.ack(1)
            t.join(timeout=5.0)
            assert box["outcome"].status is PublishStatus.CONFIRMED  # untouched by the Return

    def test_return_skips_abandoned_slots(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            env = _env()
            outcome = pub.publish(env, timeout=0.01)  # abandons the slot
            assert outcome.status is PublishStatus.TIMEOUT
            # Return arriving after abandonment must not resurrect the slot.
            ch.deliver_return(exchange="", routing_key="test.q", message_id=env.message_id)
            assert outcome.status is PublishStatus.TIMEOUT


# ── caller timeout ────────────────────────────────────────────────────────


class TestTimeout:
    def test_confirm_timeout_and_late_ack_is_noop(self) -> None:
        factory = ConnFactory()  # no auto-ack: confirm never arrives
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            outcome = pub.publish(_env(), timeout=0.01)
            assert outcome.status is PublishStatus.TIMEOUT
            assert isinstance(outcome.error, TimeoutError)
            # Late confirm: slot is abandoned, settle is a no-op; tag is reaped.
            ch.ack(1)
            assert outcome.status is PublishStatus.TIMEOUT
            assert pub._pending == {}

    def test_default_confirm_timeout_used(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory, confirm_timeout=0.01) as pub:
            outcome = pub.publish(_env())
            assert outcome.status is PublishStatus.TIMEOUT


# ── connection / channel death ────────────────────────────────────────────


class TestConnectionDeath:
    def test_connection_death_fails_all_unsettled_then_reconnects(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            conn = factory.instances[0]
            ch = conn.channel_obj
            assert ch is not None
            workers = [_publish_async(pub, _env()) for _ in range(2)]
            _wait_until(lambda: len(ch.published) == 2)
            conn.die()  # unexpected close: M17 — every in-flight slot fails
            for t, box in workers:
                t.join(timeout=5.0)
                assert box["outcome"].status is PublishStatus.ERROR
                assert box["outcome"].error is not None
            _wait_until(lambda: pub._ready.is_set())  # reconnected
            assert len(factory.instances) == 2

    def test_reconnect_exhaustion_then_publish_fails_fast(self) -> None:
        factory = ConnFactory(modes=("ok", "fail", "fail", "fail"))
        with running_publisher(factory, max_attempts=1, close_timeout=0.5) as pub:
            factory.instances[0].die()
            _wait_until(lambda: pub._io_dead.is_set())
            outcome = pub.publish(_env())
            assert outcome.status is PublishStatus.ERROR

    def test_channel_death_fails_pending_and_recycles_connection(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            conn = factory.instances[0]
            ch = conn.channel_obj
            assert ch is not None
            t, box = _publish_async(pub, _env())
            _wait_until(lambda: len(ch.published) == 1)
            ch.close()  # channel closed by broker; connection still open
            t.join(timeout=5.0)
            assert box["outcome"].status is PublishStatus.ERROR
            _wait_until(lambda: pub._ready.is_set())
            assert len(factory.instances) == 2

    def test_stale_channel_close_callback_is_ignored(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            stale = SimpleNamespace(is_open=False)
            pub._on_channel_closed(stale, RuntimeError("old channel"))
            assert pub._ready.is_set()  # current channel untouched

    def test_basic_publish_error_settles_slot_and_recycles(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            boom = RuntimeError("publish boom")
            ch.publish_raises = boom
            outcome = pub.publish(_env())
            assert outcome.status is PublishStatus.ERROR
            assert outcome.error is boom
            _wait_until(lambda: pub._ready.is_set() and len(factory.instances) == 2)

    def test_publish_on_silently_closed_channel_fails(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            ch.is_open = False  # closed without firing callbacks
            outcome = pub.publish(_env())
            assert outcome.status is PublishStatus.ERROR
            ch.is_open = True  # restore for clean shutdown

    def test_publish_wakeup_failure_fails_fast(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            conn = factory.instances[0]
            conn.is_open = False  # add_callback_threadsafe now raises
            outcome = pub.publish(_env())
            assert outcome.status is PublishStatus.ERROR
            assert len(pub._queue) == 0
            conn.is_open = True  # restore for clean shutdown

    def test_publish_wakeup_race_slot_already_failed(self) -> None:
        """Connection dies between the ready-check and the ioloop wake-up,
        and the close callback's fail-all already settled+dequeued the slot."""
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            conn = factory.instances[0]

            def racing_wakeup(_cb: Any) -> None:
                pub._fail_all(RuntimeError("connection died mid-publish"))
                raise pika.exceptions.ConnectionWrongStateError("closed")

            with patch.object(conn.ioloop, "add_callback_threadsafe", racing_wakeup):
                outcome = pub.publish(_env())
            assert outcome.status is PublishStatus.ERROR


# ── close ─────────────────────────────────────────────────────────────────


class TestClose:
    def test_close_settles_stragglers_with_error(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            ch = factory.instances[0].channel_obj
            assert ch is not None
            t, box = _publish_async(pub, _env())
            _wait_until(lambda: len(ch.published) == 1)
            pub.close(timeout=0.05)  # straggler never confirmed
            t.join(timeout=5.0)
            assert box["outcome"].status is PublishStatus.ERROR
            assert pub._pending == {}

    def test_close_idempotent(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            pub.close()
            pub.close()  # second close is a no-op

    def test_publish_after_close_errors_immediately(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            pass
        t0 = time.monotonic()
        outcome = pub.publish(_env())
        assert time.monotonic() - t0 < 1.0
        assert outcome.status is PublishStatus.ERROR
        assert outcome.error is not None

    def test_publish_before_start_errors_immediately(self) -> None:
        pub = SyncBatchPublisher()
        outcome = pub.publish(_env())
        assert outcome.status is PublishStatus.ERROR

    def test_close_before_start_is_safe(self) -> None:
        pub = SyncBatchPublisher()
        pub.close()
        assert pub._closed

    def test_context_manager(self) -> None:
        factory = ConnFactory(auto="ack")
        cfg = ConnectionConfig(reconnect_backoff_base=0.001, reconnect_backoff_max=0.002)
        with patch("pika.SelectConnection", factory):
            with SyncBatchPublisher(connection_config=cfg) as pub:
                assert pub.publish(_env()).status is PublishStatus.CONFIRMED
        assert pub._closed

    def test_shutdown_io_with_dead_connection_stops_ioloop(self) -> None:
        factory = ConnFactory()
        with running_publisher(factory) as pub:
            conn = factory.instances[0]
            ch = conn.channel_obj
            assert ch is not None
            conn.is_open = False
            ch.is_open = False
            pub._shutdown_io()  # falls through to ioloop.stop()
            # Not closing, so the I/O thread treats the stopped loop as a
            # dead connection and reconnects on a fresh SelectConnection.
            _wait_until(lambda: len(factory.instances) == 2 and pub._ready.is_set())


# ── properties built from envelope ────────────────────────────────────────


class TestProperties:
    def test_properties_built_from_envelope(self) -> None:
        factory = ConnFactory(auto="ack")
        ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        env = _env(
            headers={"x-tenant": "acme"},
            correlation_id="corr-1",
            reply_to="reply.q",
            content_encoding="gzip",
            delivery_mode=1,
            priority=7,
            expiration="60000",
            type="created",
            user_id="guest",
            app_id="svc",
            timestamp=ts,
            mandatory=True,
        )
        with running_publisher(factory) as pub:
            assert pub.publish(env).status is PublishStatus.CONFIRMED
            ch = factory.instances[0].channel_obj
            assert ch is not None
            published = ch.published[0]
            props = published.properties
            assert isinstance(props, pika.BasicProperties)
            assert props.message_id == env.message_id
            assert props.headers == {"x-tenant": "acme"}
            assert props.delivery_mode == 1
            assert props.correlation_id == "corr-1"
            assert props.reply_to == "reply.q"
            assert props.content_type == "application/json"
            assert props.content_encoding == "gzip"
            assert props.priority == 7
            assert props.expiration == "60000"
            assert props.type == "created"
            assert props.user_id == "guest"
            assert props.app_id == "svc"
            assert props.timestamp == int(ts.timestamp())
            assert published.mandatory is True
            assert published.body == b'{"x":1}'

    def test_empty_headers_become_none(self) -> None:
        factory = ConnFactory(auto="ack")
        with running_publisher(factory) as pub:
            assert pub.publish(_env()).ok
            ch = factory.instances[0].channel_obj
            assert ch is not None
            assert ch.published[0].properties.headers is None
            assert ch.published[0].properties.timestamp is None

    def test_broken_properties_settle_error(self) -> None:
        factory = ConnFactory(auto="ack")
        with running_publisher(factory) as pub:
            env = _env(timestamp="not-a-datetime")  # .timestamp() will raise
            outcome = pub.publish(env)
            assert outcome.status is PublishStatus.ERROR
            assert outcome.error is not None
            assert pub._ready.is_set()  # channel untouched — build failed pre-publish


# ── every slot is ALWAYS settled (M17 property) ───────────────────────────


class TestEverySlotSettled:
    def test_randomized_interleaving_settles_every_slot(self) -> None:
        """Under a seeded random interleaving of ack/nack/return/nothing plus
        a random terminal event (connection death or close), every publish
        slot ends with a terminal outcome — none silently dropped, none hung."""
        rng = random.Random(0xBA7C4)
        for _ in range(20):
            factory = ConnFactory()
            with running_publisher(factory, confirm_timeout=5.0, close_timeout=1.0) as pub:
                conn = factory.instances[0]
                ch = conn.channel_obj
                assert ch is not None
                n = rng.randint(1, 5)
                workers = [_publish_async(pub, _env()) for _ in range(n)]
                _wait_until(lambda: len(ch.published) == n)  # noqa: B023

                tags = list(range(1, n + 1))
                rng.shuffle(tags)
                for tag in tags:
                    action = rng.choice(("ack", "nack", "return", "skip"))
                    if action == "ack":
                        ch.ack(tag, multiple=rng.random() < 0.3)
                    elif action == "nack":
                        ch.nack(tag, multiple=rng.random() < 0.3)
                    elif action == "return":
                        ch.deliver_return(exchange="", routing_key="test.q")

                if rng.random() < 0.5:
                    conn.die()  # unsettled slots must fail with ERROR
                else:
                    pub.close(timeout=0.05)  # stragglers must fail with ERROR

                for t, box in workers:
                    t.join(timeout=5.0)
                    assert not t.is_alive(), "publish caller hung — invariant 3 violated"
                    assert "outcome" in box, "slot silently dropped — invariant 2 violated"
                    assert isinstance(box["outcome"], PublishOutcome)
                    assert box["outcome"].status in tuple(PublishStatus)
                assert all(s.outcome is not None for s in pub._pending.values())


# ── internals ─────────────────────────────────────────────────────────────


class TestSlot:
    def test_settle_is_first_wins(self) -> None:
        pub = SyncBatchPublisher()
        slot = _Slot(_env())
        pub._settle(slot, PublishStatus.RETURNED)
        pub._settle(slot, PublishStatus.CONFIRMED)
        assert slot.outcome is not None
        assert slot.outcome.status is PublishStatus.RETURNED
