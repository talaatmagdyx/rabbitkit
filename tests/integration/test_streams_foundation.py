"""Real-broker tests for stream consumption over AMQP 0.9.1 (async and sync).

Each scenario runs against RabbitMQ 3.13 (the suite's shared broker) and 4.1,
with only the default plugins: the ``rabbitmq_stream`` plugin is not needed
for AMQP 0.9.1 stream consumers.

What is asserted is what the consumer actually received (offsets from the
broker's ``x-stream-offset`` header, bodies) and what the offset store holds:

* ``"first"`` sees every message, in order, each with its own offset (a
  publisher-set ``x-stream-offset`` header is overwritten by the broker);
* the default (``"next"``) sees only messages published after subscribing;
* a timestamp start sees only messages published after that time;
* stop after K, restart with a durable store: resumes at K, no gap, no
  duplicate; a crash (no final commit) replays at most ``commit_every``;
* a connection kill mid-stream resumes from the processed position: no gap,
  not from ``"first"`` again;
* a failed message is skipped (streams do not redeliver), never retried;
* retry / ACK_FIRST on a stream route are refused at registration.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import skip_without_docker


class _FileOffsetStore:
    """A durable OffsetStore written the way an application would: one JSON
    file, replaced atomically on every save. rabbitkit ships no durable store;
    the service brings its own storage."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()

    def load(self, key: str) -> int | None:
        with self._lock:
            if not self._path.exists():
                return None
            value = json.loads(self._path.read_text()).get(key)
        return None if value is None else int(value)

    def save(self, key: str, offset: int) -> None:
        with self._lock:
            data = json.loads(self._path.read_text()) if self._path.exists() else {}
            data[key] = offset
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data))
            os.replace(tmp, self._path)

    def __enter__(self) -> _FileOffsetStore:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


pytestmark = pytest.mark.integration

N = 30


@pytest.fixture(scope="module")
def rabbit4() -> Iterator[dict[str, Any]]:
    """A RabbitMQ 4.1 broker for this module."""
    skip_without_docker()
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

    with RabbitMqContainer("rabbitmq:4.1-management-alpine").with_exposed_ports(15672) as container:
        host = container.get_container_host_ip()
        yield {
            "url": f"amqp://guest:guest@{host}:{container.get_exposed_port(5672)}/",
            "mgmt": f"http://{host}:{container.get_exposed_port(15672)}",
            "container": container,
        }


@pytest.fixture(params=["3.13", "4.1"])
def rabbit(request: pytest.FixtureRequest) -> dict[str, Any]:
    fixture = "rabbit_container" if request.param == "3.13" else "rabbit4"
    env: dict[str, Any] = request.getfixturevalue(fixture)
    return env


# ── helpers ──────────────────────────────────────────────────────────────


def _name(prefix: str) -> str:
    return f"streams-{prefix}-{uuid.uuid4().hex[:8]}"


def _config(url: str, *, connection_name: str | None = None, **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    connection = ConnectionConfig.from_url(url)
    if connection_name is not None:
        connection = replace(connection, connection_name=connection_name)
    return RabbitConfig(connection=connection, **kw)


def _stream(name: str, **kw: Any) -> Any:
    from rabbitkit import QueueType, RabbitQueue

    return RabbitQueue(name, queue_type=QueueType.STREAM, **kw)


def _declare(url: str, queue: Any) -> None:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(url))
    try:
        conn.channel().queue_declare(**queue.to_declare_kwargs())
    finally:
        conn.close()


def _publish(url: str, queue: str, bodies: list[bytes], *, spoof: int | None = None) -> None:
    """Publish with confirms; message *spoof* carries a fake x-stream-offset header."""
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(url))
    try:
        ch = conn.channel()
        ch.confirm_delivery()
        for i, body in enumerate(bodies):
            headers = {"x-stream-offset": 999_999} if i == spoof else None
            ch.basic_publish("", queue, body, pika.BasicProperties(headers=headers, delivery_mode=2))
    finally:
        conn.close()


def _bodies(start: int, stop: int) -> list[bytes]:
    return [f"m{i}".encode() for i in range(start, stop)]


def _kill_connections(rabbit: dict[str, Any], connection_name: str) -> int:
    """Force-close only this test's connections (the 3.13 broker is shared)."""
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    client = RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))
    deadline = time.monotonic() + 15
    killed = 0
    while killed == 0 and time.monotonic() < deadline:
        for connection in client.list_connections():
            if (connection.get("client_properties") or {}).get("connection_name") != connection_name:
                continue
            with contextlib.suppress(Exception):
                client.close_connection(str(connection["name"]), reason="streams foundation test")
                killed += 1
        if killed == 0:
            time.sleep(0.2)  # the management API lists a new connection after a moment
    return killed


class _Seen:
    """Thread-safe record of ``(offset, body)`` deliveries."""

    def __init__(self) -> None:
        self.items: list[tuple[int, bytes]] = []
        self._lock = threading.Lock()

    def add(self, offset: int | None, body: bytes) -> None:
        assert offset is not None, "a stream delivery without x-stream-offset"
        with self._lock:
            self.items.append((offset, body))

    @property
    def offsets(self) -> list[int]:
        with self._lock:
            return [o for o, _ in self.items]

    def wait(self, predicate: Callable[[list[int]], bool], timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate(self.offsets):
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out; seen offsets {self.offsets}")
            time.sleep(0.02)

    async def await_(self, predicate: Callable[[list[int]], bool], timeout: float = 30.0) -> None:
        await asyncio.to_thread(self.wait, predicate, timeout)


class _CrashingStore:
    """An OffsetStore that stops persisting once ``crash()`` is called, like a
    process that died: no later commit (including stop()'s final one) lands."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.crashed = False

    def load(self, key: str) -> int | None:
        loaded: int | None = self.inner.load(key)
        return loaded

    def save(self, key: str, offset: int) -> None:
        if not self.crashed:
            self.inner.save(key, offset)

    def crash(self) -> None:
        self.crashed = True


@contextlib.contextmanager
def _running(broker: Any, **run_kw: Any) -> Iterator[None]:
    """Run a SyncBroker's ``run()`` loop (start, reconnects, stop) on its own thread."""
    errors: list[BaseException] = []

    def target() -> None:
        try:
            broker.run(**run_kw)
        except BaseException as exc:  # surfaced in the test thread below
            errors.append(exc)

    thread = threading.Thread(target=target, name="sync-broker-run", daemon=True)
    thread.start()
    deadline = time.monotonic() + 20
    while not broker.started and thread.is_alive():
        if time.monotonic() > deadline:
            raise AssertionError("SyncBroker did not start")
        time.sleep(0.01)
    try:
        yield
    finally:
        if broker.started:
            broker._on_sigterm(signal.SIGTERM, None)  # what SIGTERM does: stop_consuming, then stop()
        thread.join(timeout=30)
        assert not thread.is_alive(), "SyncBroker.run() did not return"
        if errors:
            raise errors[0]


# ── "first": every message, in order, correct offsets ────────────────────


async def test_async_first_sees_every_message_in_order_with_offsets(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitMessage

    url = rabbit["url"]
    queue = _stream(
        _name("first-a"), max_age="7D", max_length_bytes=100_000_000, stream_max_segment_size_bytes=10_000_000
    )
    await asyncio.to_thread(_declare, url, queue)
    await asyncio.to_thread(_publish, url, queue.name, _bodies(0, N), spoof=3)

    broker = AsyncBroker(_config(url))
    seen = _Seen()

    @broker.subscriber(queue, stream_offset="first", prefetch_count=1)
    async def handle(body: bytes, message: RabbitMessage) -> None:
        seen.add(message.stream_offset, body)

    await broker.start(install_signal_handlers=False)
    try:
        await seen.await_(lambda offsets: len(offsets) >= N)
    finally:
        await broker.stop(timeout=5)
    assert seen.items == list(zip(range(N), _bodies(0, N), strict=True))


def test_sync_first_sees_every_message_in_order_with_offsets(rabbit: dict[str, Any]) -> None:
    from rabbitkit import RabbitMessage, SyncBroker

    url = rabbit["url"]
    queue = _stream(_name("first-s"), max_age="1h", max_length_bytes=100_000_000)
    _declare(url, queue)
    _publish(url, queue.name, _bodies(0, N), spoof=3)

    broker = SyncBroker(_config(url))
    seen = _Seen()

    @broker.subscriber(queue, stream_offset="first")
    def handle(body: bytes, message: RabbitMessage) -> None:
        seen.add(message.stream_offset, body)

    with _running(broker):
        seen.wait(lambda offsets: len(offsets) >= N)
    assert seen.items == list(zip(range(N), _bodies(0, N), strict=True))


# ── "next" (the default): only messages published after subscribing ──────


async def test_async_default_next_sees_only_new_messages(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitMessage

    url = rabbit["url"]
    queue = _stream(_name("next-a"))
    await asyncio.to_thread(_declare, url, queue)
    await asyncio.to_thread(_publish, url, queue.name, _bodies(0, 5))

    broker = AsyncBroker(_config(url))
    seen = _Seen()

    @broker.subscriber(queue)  # no stream options: starts at "next"
    async def handle(body: bytes, message: RabbitMessage) -> None:
        seen.add(message.stream_offset, body)

    await broker.start(install_signal_handlers=False)
    try:
        # start() returned after basic.consume-ok: the consumer is registered.
        # (A passive declare reports no consumers on a stream, so the
        # suite's consumer probe cannot be used here.)
        await asyncio.to_thread(_publish, url, queue.name, _bodies(5, 8))
        await seen.await_(lambda offsets: len(offsets) >= 3)
        await asyncio.sleep(0.5)
    finally:
        await broker.stop(timeout=5)
    assert seen.items == list(zip(range(5, 8), _bodies(5, 8), strict=True))


def test_sync_default_next_sees_only_new_messages(rabbit: dict[str, Any]) -> None:
    from rabbitkit import RabbitMessage, SyncBroker

    url = rabbit["url"]
    queue = _stream(_name("next-s"))
    _declare(url, queue)
    _publish(url, queue.name, _bodies(0, 5))

    broker = SyncBroker(_config(url))
    seen = _Seen()

    @broker.subscriber(queue, stream_offset="next")
    def handle(body: bytes, message: RabbitMessage) -> None:
        seen.add(message.stream_offset, body)

    with _running(broker):
        _publish(url, queue.name, _bodies(5, 8))  # _running waited for start(): consuming
        seen.wait(lambda offsets: len(offsets) >= 3)
        time.sleep(0.5)
    assert seen.items == list(zip(range(5, 8), _bodies(5, 8), strict=True))


# ── timestamp offset ─────────────────────────────────────────────────────


def _publish_around_a_timestamp(url: str, queue: str) -> datetime:
    """5 messages, then (1.5 s later) the timestamp, then (1.5 s later) 3 more."""
    _publish(url, queue, _bodies(0, 5))
    time.sleep(1.5)
    at = datetime.now(UTC)
    time.sleep(1.5)
    _publish(url, queue, _bodies(5, 8))
    return at


async def test_async_timestamp_offset_starts_at_that_time(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitMessage

    url = rabbit["url"]
    queue = _stream(_name("ts-a"))
    await asyncio.to_thread(_declare, url, queue)
    at = await asyncio.to_thread(_publish_around_a_timestamp, url, queue.name)

    broker = AsyncBroker(_config(url))
    seen = _Seen()

    @broker.subscriber(queue, stream_offset=at)
    async def handle(body: bytes, message: RabbitMessage) -> None:
        seen.add(message.stream_offset, body)

    await broker.start(install_signal_handlers=False)
    try:
        await seen.await_(lambda offsets: len(offsets) >= 3)
        await asyncio.sleep(0.5)
    finally:
        await broker.stop(timeout=5)
    assert sorted(seen.items) == list(zip(range(5, 8), _bodies(5, 8), strict=True))


def test_sync_timestamp_offset_starts_at_that_time(rabbit: dict[str, Any]) -> None:
    from rabbitkit import RabbitMessage, StreamConsumerConfig, StreamOffset, SyncBroker

    url = rabbit["url"]
    queue = _stream(_name("ts-s"))
    _declare(url, queue)
    at = _publish_around_a_timestamp(url, queue.name)

    broker = SyncBroker(_config(url))
    seen = _Seen()

    @broker.subscriber(queue, stream=StreamConsumerConfig(offset=StreamOffset.timestamp(at)))
    def handle(body: bytes, message: RabbitMessage) -> None:
        seen.add(message.stream_offset, body)

    with _running(broker):
        seen.wait(lambda offsets: len(offsets) >= 3)
        time.sleep(0.5)
    assert seen.items == list(zip(range(5, 8), _bodies(5, 8), strict=True))


# ── stop after K, restart with the durable store ─────────────────────────

K = 12


def _resume_config(store: Any) -> Any:
    from rabbitkit import StreamConsumerConfig

    return StreamConsumerConfig(offset="first", offset_store=store, commit_every=5, commit_interval=None)  # type: ignore[arg-type]


async def _async_run(url: str, queue: Any, store: Any, until: Callable[[_Seen], bool], seen: _Seen) -> None:
    """One consumer lifetime: start, consume until *until*, stop."""
    from rabbitkit import AsyncBroker, RabbitMessage

    broker = AsyncBroker(_config(url))

    @broker.subscriber(queue, stream=_resume_config(store), prefetch_count=1)
    async def handle(body: bytes, message: RabbitMessage) -> None:
        if isinstance(store, _CrashingStore) and store.crashed:
            return  # the process is "dead": nothing after the crash happened
        seen.add(message.stream_offset, body)
        if until(seen) and isinstance(store, _CrashingStore):
            # Crash right after the K-th message: let the commit that was
            # already due (offset 9) land first, as it would have in a real
            # process, then stop persisting (no final commit at stop()).
            tracker = broker._stream_trackers[f"{queue.name}:{handle.__qualname__}"]
            for _ in range(500):
                if tracker.committed_offset == 9:
                    break
                await asyncio.sleep(0.01)
            store.crash()

    await broker.start(install_signal_handlers=False)
    try:
        await seen.await_(lambda _offsets: until(seen))
    finally:
        await broker.stop(timeout=5)


def _sync_run(url: str, queue: Any, store: Any, until: Callable[[_Seen], bool], seen: _Seen) -> None:
    from rabbitkit import RabbitMessage, SyncBroker

    broker = SyncBroker(_config(url))

    @broker.subscriber(queue, stream=_resume_config(store), prefetch_count=1)
    def handle(body: bytes, message: RabbitMessage) -> None:
        if isinstance(store, _CrashingStore) and store.crashed:
            return
        seen.add(message.stream_offset, body)
        if until(seen) and isinstance(store, _CrashingStore):
            store.crash()  # sync commits inline: offset 9 is already saved

    with _running(broker):
        seen.wait(lambda _offsets: until(seen))


async def test_async_restart_resumes_from_the_durable_store(rabbit: dict[str, Any], tmp_path: Path) -> None:
    url = rabbit["url"]
    queue = _stream(_name("resume-a"))
    await asyncio.to_thread(_declare, url, queue)
    await asyncio.to_thread(_publish, url, queue.name, _bodies(0, N))
    db = tmp_path / "offsets.db"

    first = _Seen()
    with _FileOffsetStore(db) as store:
        await _async_run(url, queue, store, lambda s: len(s.offsets) >= K, first)
        stored = store.load(queue.name)
    processed = first.offsets
    assert processed == list(range(len(processed))) and len(processed) >= K
    assert stored == processed[-1], "stop() commits the last processed offset"

    # More messages while it is down (the first run may have drained all N).
    await asyncio.to_thread(_publish, url, queue.name, _bodies(N, 2 * N))

    second = _Seen()
    with _FileOffsetStore(db) as store:
        await _async_run(url, queue, store, lambda s: bool(s.offsets) and s.offsets[-1] == 2 * N - 1, second)
        assert store.load(queue.name) == 2 * N - 1
    assert second.offsets[0] == stored + 1, "the restart resumes right after the committed offset"
    assert processed + second.offsets == list(range(2 * N)), "no gap and no duplicate"


def test_sync_restart_resumes_from_the_durable_store(rabbit: dict[str, Any], tmp_path: Path) -> None:
    url = rabbit["url"]
    queue = _stream(_name("resume-s"))
    _declare(url, queue)
    _publish(url, queue.name, _bodies(0, N))
    db = tmp_path / "offsets.db"

    first = _Seen()
    with _FileOffsetStore(db) as store:
        _sync_run(url, queue, store, lambda s: len(s.offsets) >= K, first)
        stored = store.load(queue.name)
    processed = first.offsets
    assert processed == list(range(len(processed))) and len(processed) >= K
    assert stored == processed[-1]

    _publish(url, queue.name, _bodies(N, 2 * N))

    second = _Seen()
    with _FileOffsetStore(db) as store:
        _sync_run(url, queue, store, lambda s: bool(s.offsets) and s.offsets[-1] == 2 * N - 1, second)
        assert store.load(queue.name) == 2 * N - 1
    assert second.offsets[0] == stored + 1
    assert processed + second.offsets == list(range(2 * N))


async def test_async_crash_replays_at_most_the_commit_window(rabbit: dict[str, Any], tmp_path: Path) -> None:
    url = rabbit["url"]
    queue = _stream(_name("crash-a"))
    await asyncio.to_thread(_declare, url, queue)
    await asyncio.to_thread(_publish, url, queue.name, _bodies(0, N))
    db = tmp_path / "offsets.db"

    first = _Seen()
    with _FileOffsetStore(db) as inner:
        await _async_run(url, queue, _CrashingStore(inner), lambda s: len(s.offsets) >= K, first)
        stored = inner.load(queue.name)
    assert first.offsets == list(range(K))
    # AsyncBroker commits off the event loop, taking the processed position
    # when the commit runs, so it can land past a commit_every boundary; the
    # contract is the window, not the exact boundary.
    assert stored is not None and 9 <= stored <= K - 1, "committed at least the commit_every boundary, never ahead"

    second = _Seen()
    with _FileOffsetStore(db) as store:
        await _async_run(url, queue, store, lambda s: bool(s.offsets) and s.offsets[-1] == N - 1, second)
    assert second.offsets == list(range(stored + 1, N))
    duplicates = set(first.offsets) & set(second.offsets)
    assert duplicates == set(range(stored + 1, K)) and len(duplicates) <= 5, "a crash replays at most commit_every"
    assert set(first.offsets) | set(second.offsets) == set(range(N))


def test_sync_crash_replays_at_most_the_commit_window(rabbit: dict[str, Any], tmp_path: Path) -> None:
    url = rabbit["url"]
    queue = _stream(_name("crash-s"))
    _declare(url, queue)
    _publish(url, queue.name, _bodies(0, N))
    db = tmp_path / "offsets.db"

    first = _Seen()
    with _FileOffsetStore(db) as inner:
        _sync_run(url, queue, _CrashingStore(inner), lambda s: len(s.offsets) >= K, first)
        stored = inner.load(queue.name)
    assert first.offsets == list(range(K))
    assert stored == 9

    second = _Seen()
    with _FileOffsetStore(db) as store:
        _sync_run(url, queue, store, lambda s: bool(s.offsets) and s.offsets[-1] == N - 1, second)
    assert second.offsets == list(range(stored + 1, N))
    assert set(first.offsets) & set(second.offsets) == {10, 11}
    assert set(first.offsets) | set(second.offsets) == set(range(N))


# ── connection kill mid-stream ───────────────────────────────────────────

KILL_N = 60
KILL_PREFETCH = 10


async def test_async_connection_kill_resumes_without_skipping(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitMessage

    url = rabbit["url"]
    queue = _stream(_name("kill-a"))
    await asyncio.to_thread(_declare, url, queue)
    await asyncio.to_thread(_publish, url, queue.name, _bodies(0, KILL_N))
    connection_name = _name("conn")

    broker = AsyncBroker(_config(url, connection_name=connection_name))
    seen = _Seen()
    gate = asyncio.Event()

    @broker.subscriber(queue, stream_offset="first", prefetch_count=KILL_PREFETCH)
    async def handle(body: bytes, message: RabbitMessage) -> None:
        offset = message.stream_offset
        if offset is not None and offset >= 15:
            await gate.wait()  # hold the consumer mid-stream until the kill
        await asyncio.sleep(0.005)
        seen.add(offset, body)

    reconnects: list[None] = []
    await broker.start(install_signal_handlers=False)
    broker._transport.on_reconnect(lambda: reconnects.append(None))
    try:
        await seen.await_(lambda offsets: len(offsets) >= 15)
        assert await asyncio.to_thread(_kill_connections, rabbit, connection_name) > 0
        await seen.await_(lambda _offsets: bool(reconnects))
        gate.set()
        await seen.await_(lambda offsets: set(range(KILL_N)) <= set(offsets))
        await asyncio.sleep(0.5)
    finally:
        gate.set()
        await broker.stop(timeout=5)

    offsets = seen.offsets
    assert set(offsets) == set(range(KILL_N)), "no message skipped across the reconnect"
    assert offsets.count(0) == 1, "the reconnect resumed from the processed position, not from 'first'"
    assert len(offsets) - KILL_N <= KILL_PREFETCH, "only in-flight deliveries are replayed"


def test_sync_connection_kill_resumes_without_skipping(rabbit: dict[str, Any]) -> None:
    from rabbitkit import RabbitMessage, SyncBroker

    url = rabbit["url"]
    queue = _stream(_name("kill-s"))
    _declare(url, queue)
    _publish(url, queue.name, _bodies(0, KILL_N))
    connection_name = _name("conn")

    broker = SyncBroker(_config(url, connection_name=connection_name))
    seen = _Seen()
    killed = threading.Event()

    @broker.subscriber(queue, stream_offset="first", prefetch_count=KILL_PREFETCH)
    def handle(body: bytes, message: RabbitMessage) -> None:
        offset = message.stream_offset
        if offset is not None and offset >= 15:
            killed.wait(timeout=20)  # hold the I/O thread mid-stream until the kill
        seen.add(offset, body)

    with _running(broker):
        seen.wait(lambda offsets: len(offsets) >= 15)
        kill = threading.Thread(target=lambda: (_kill_connections(rabbit, connection_name), killed.set()))
        kill.start()
        kill.join(timeout=30)
        seen.wait(lambda offsets: set(range(KILL_N)) <= set(offsets), timeout=60)
        time.sleep(0.5)

    offsets = seen.offsets
    assert set(offsets) == set(range(KILL_N))
    assert offsets.count(0) == 1
    assert len(offsets) - KILL_N <= KILL_PREFETCH


async def test_async_reconnect_before_the_first_delivery_does_not_skip(rabbit: dict[str, Any]) -> None:
    """A "next" consumer knows no offset until a message arrives; a reconnect
    in that window resubscribes from when it first subscribed, so a message
    published around the outage is still delivered."""
    from rabbitkit import AsyncBroker, RabbitMessage

    url = rabbit["url"]
    queue = _stream(_name("next-kill"))
    await asyncio.to_thread(_declare, url, queue)
    await asyncio.to_thread(_publish, url, queue.name, _bodies(0, 3))
    # Older than RESUBSCRIBE_CLOCK_MARGIN when the consumer subscribes, so the
    # resubscription (subscription time - margin) does not include them.
    await asyncio.sleep(6)
    connection_name = _name("conn")

    broker = AsyncBroker(_config(url, connection_name=connection_name))
    seen = _Seen()

    @broker.subscriber(queue)
    async def handle(body: bytes, message: RabbitMessage) -> None:
        seen.add(message.stream_offset, body)

    reconnects: list[None] = []
    await broker.start(install_signal_handlers=False)
    broker._transport.on_reconnect(lambda: reconnects.append(None))
    try:
        assert await asyncio.to_thread(_kill_connections, rabbit, connection_name) > 0
        await asyncio.to_thread(_publish, url, queue.name, _bodies(3, 5))
        await seen.await_(lambda _offsets: bool(reconnects))
        await seen.await_(lambda offsets: {3, 4} <= set(offsets))
        await asyncio.sleep(0.5)
    finally:
        await broker.stop(timeout=5)
    assert sorted(set(seen.offsets)) == [3, 4], "nothing published before subscribing, nothing after skipped"


# ── failures are skipped, not retried ─────────────────────────────────────


async def test_async_failed_message_is_skipped_not_redelivered(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitMessage

    url = rabbit["url"]
    queue = _stream(_name("fail-a"))
    await asyncio.to_thread(_declare, url, queue)
    await asyncio.to_thread(_publish, url, queue.name, _bodies(0, 5))

    broker = AsyncBroker(_config(url))
    attempts: list[int] = []

    @broker.subscriber(queue, stream_offset="first", prefetch_count=1)
    async def handle(body: bytes, message: RabbitMessage) -> None:
        offset = message.stream_offset
        assert offset is not None
        attempts.append(offset)
        if offset == 2:
            raise ValueError("boom")

    await broker.start(install_signal_handlers=False)
    try:
        await _Seen().await_(lambda _o: len(attempts) >= 5)
        await asyncio.sleep(1.0)
    finally:
        await broker.stop(timeout=5)
    tracker = next(iter(broker._stream_trackers.values()))
    assert attempts == [0, 1, 2, 3, 4], "the failed message was not redelivered"
    assert tracker.failed == 1
    assert tracker.committed_offset == 4, "the consumer moved past the failure"


def test_sync_failed_message_is_skipped_not_redelivered(rabbit: dict[str, Any]) -> None:
    from rabbitkit import RabbitMessage, SyncBroker

    url = rabbit["url"]
    queue = _stream(_name("fail-s"))
    _declare(url, queue)
    _publish(url, queue.name, _bodies(0, 5))

    broker = SyncBroker(_config(url))
    attempts: list[int] = []

    @broker.subscriber(queue, stream_offset="first")
    def handle(body: bytes, message: RabbitMessage) -> None:
        offset = message.stream_offset
        assert offset is not None
        attempts.append(offset)
        if offset == 2:
            raise ValueError("boom")

    with _running(broker):
        _Seen().wait(lambda _o: len(attempts) >= 5)
        time.sleep(1.0)
    tracker = next(iter(broker._stream_trackers.values()))
    assert attempts == [0, 1, 2, 3, 4]
    assert tracker.failed == 1
    assert tracker.committed_offset == 4


# ── refused at registration ──────────────────────────────────────────────


@pytest.mark.parametrize("broker_kind", ["async", "sync"])
def test_retry_dlq_and_ack_first_on_a_stream_route_are_refused(rabbit: dict[str, Any], broker_kind: str) -> None:
    from rabbitkit import AckPolicy, AsyncBroker, ConfigurationError, RetryConfig, SyncBroker

    url = rabbit["url"]
    broker_cls: Any = AsyncBroker if broker_kind == "async" else SyncBroker
    queue = _stream(_name("refused"))

    def handler(body: bytes) -> None: ...

    with pytest.raises(ConfigurationError, match="retry/DLQ is not supported on stream queue"):
        broker_cls(_config(url)).subscriber(queue, retry=RetryConfig(max_retries=3))(handler)
    with pytest.raises(ConfigurationError, match="the broker's default RetryConfig"):
        broker_cls(_config(url, retry=RetryConfig(max_retries=3))).subscriber(queue)(handler)
    with pytest.raises(ConfigurationError, match="ACK_FIRST is not supported on a stream queue"):
        broker_cls(_config(url)).subscriber(queue, ack_policy=AckPolicy.ACK_FIRST)(handler)
    from rabbitkit import TopologyValidationError

    with pytest.raises(TopologyValidationError, match="does not support dead_letter_exchange"):
        _stream(_name("dlx"), dead_letter_exchange="")
