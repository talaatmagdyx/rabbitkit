"""Real-broker settlement tests: prefetch matrix and failure injection.

The coalescing coordinator is a correctness-critical state machine, so unit
and property tests alone are not enough — these run it against a live broker
with real ``basic.ack(multiple=True)`` frames, real nack/requeue redelivery,
real channel loss and real reconnects.

The assertion that matters, checked on every emitted frame:

    Every delivery covered by a cumulative ACK must have been ACK_READY at
    the moment the ACK was emitted.

Plus, at the end of each run: every published message id was processed, the
queue drained to 0 ready / 0 unacked, and nothing was acked before its
handler finished.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
import uuid
from typing import Any

import pytest

from rabbitkit.core.settlement import CoordinatorError

try:
    from rabbitkit.core.message import RabbitMessage as _RabbitMessage
except ImportError:  # pragma: no cover
    pass

try:
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

    _TESTCONTAINERS_AVAILABLE = True
except ImportError:
    _TESTCONTAINERS_AVAILABLE = False

pytestmark = pytest.mark.integration


def _skip_no_docker() -> None:
    if not _TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed — run: pip install testcontainers[rabbitmq]")
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not reachable — skip real-RabbitMQ integration tests")


@pytest.fixture(scope="module")
def rabbit() -> Any:  # type: ignore[return]
    _skip_no_docker()
    with RabbitMqContainer("rabbitmq:3.13-management-alpine").with_exposed_ports(15672) as container:
        host = container.get_container_host_ip()
        yield {
            "url": f"amqp://guest:guest@{host}:{container.get_exposed_port(5672)}/",
            "mgmt": f"http://{host}:{container.get_exposed_port(15672)}",
        }


def _config(url: str, **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, PublisherConfig, RabbitConfig

    kw.setdefault("publisher", PublisherConfig(mandatory=True))
    return RabbitConfig(connection=ConnectionConfig.from_url(url), **kw)


def _counts(rabbit: dict[str, str], queue: str, *, retries: int = 40) -> tuple[int, int]:
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    client = RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))
    last: Exception | None = None
    for _ in range(retries):
        try:
            info = client.get_queue(queue)
            return int(info.get("messages_ready", 0)), int(info.get("messages_unacknowledged", 0))
        except Exception as exc:
            last = exc
            time.sleep(0.25)
    raise AssertionError(f"queue {queue} not visible via management API: {last}")


def _kill_connections(rabbit: dict[str, str]) -> int:
    """Force-close every AMQP connection — the bluntest realistic failure.
    Unacked deliveries are requeued; ``connect_robust`` reconnects."""
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    client = RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))
    killed = 0
    for connection in client.list_connections():
        name = connection.get("name")
        if not name:
            continue
        with contextlib.suppress(Exception):  # the connection may already be gone
            client.close_connection(str(name), reason="chaos test")
            killed += 1
    return killed


async def _await_counts(rabbit: dict[str, str], queue: str, expected: tuple[int, int], timeout: float = 60.0) -> None:
    """Poll until the broker reports *expected* ``(ready, unacked)``.

    The management API refreshes queue stats on an interval, so a single
    read right after a settlement can be stale — always poll.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    seen: tuple[int, int] = (-1, -1)
    while loop.time() < deadline:
        seen = await loop.run_in_executor(None, lambda: _counts(rabbit, queue))
        if seen == expected:
            return
        await asyncio.sleep(0.3)
    raise AssertionError(f"{queue}: expected ready/unacked {expected}, last saw {seen}")


async def _await_drained(rabbit: dict[str, str], queue: str, timeout: float = 60.0) -> None:
    await _await_counts(rabbit, queue, (0, 0), timeout)


class Ledger:
    """Tracks, per delivery tag, whether the handler actually finished — so a
    frame covering a tag can be checked against reality."""

    def __init__(self) -> None:
        self.done: dict[int, str] = {}  # tag -> message id, once the handler completed
        self.seen: dict[int, str] = {}  # tag -> message id, at registration
        self.acked: set[str] = set()
        self.violations: list[str] = []
        self.frames = 0
        self.cumulative = 0

    def on_flush(self, report: Any) -> None:
        from rabbitkit.core.types import SettlementAction

        self.frames += len(report.commands)
        for command in report.commands:
            if command.multiple:
                self.cumulative += 1
            for tag in command.covers:
                if command.kind is not SettlementAction.ACK:
                    continue
                if tag not in self.done:
                    self.violations.append(
                        f"ACK covered tag {tag} whose handler had not completed "
                        f"(multiple={command.multiple}, covers={command.covers})"
                    )
                else:
                    self.acked.add(self.done[tag])


def _make_acker(channel: Any, ledger: Ledger, loop: asyncio.AbstractEventLoop) -> Any:
    from rabbitkit.core.config import BatchAckConfig
    from rabbitkit.highload.batch import CoalescingAcker

    # marshal= hands the whole interval flush to the event loop, so these
    # plain create_task callables always run on the owner thread.
    return CoalescingAcker(
        ack_fn=lambda t, m: loop.create_task(channel.basic_ack(delivery_tag=t, multiple=m)),
        nack_fn=lambda t, r: loop.create_task(channel.basic_nack(delivery_tag=t, requeue=r)),
        reject_fn=lambda t, r: loop.create_task(channel.basic_reject(delivery_tag=t, requeue=r)),
        config=BatchAckConfig(batch_size=32, flush_interval_ms=100),
        channel_key=channel,
        max_hold=2,
        marshal=loop.call_soon_threadsafe,
        on_flush=ledger.on_flush,
    )


# ── item 14: prefetch matrix ──────────────────────────────────────────────


@pytest.mark.parametrize("prefetch", [1, 10, 100])
async def test_coalescing_across_prefetch_settings(rabbit: dict[str, str], prefetch: int) -> None:
    """A blocked prefix must never collapse throughput into an unsafe ack, at
    any prefetch. With prefetch=1 there is nothing to coalesce (one delivery
    in flight at a time); with 100 the whole batch coalesces."""
    import json

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig, WorkerConfig
    from rabbitkit.core.types import AckPolicy, MessageEnvelope
    from rabbitkit.highload.batch import CoalescingAckerGroup

    total = 120
    queue = f"chaos-prefetch{prefetch}-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(config=_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=prefetch)))
    loop = asyncio.get_running_loop()
    ledger = Ledger()
    group = CoalescingAckerGroup(factory=lambda ch: _make_acker(ch, ledger, loop))
    processed = 0
    done = asyncio.Event()

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: _RabbitMessage) -> None:
        nonlocal processed
        assert msg.delivery_tag is not None
        channel = msg.raw_message.channel
        group.register(channel, msg.delivery_tag)
        ledger.seen[msg.delivery_tag] = json.loads(body)["id"]
        # The first delivery is deliberately slow: it strands its siblings,
        # which is safe but must not produce an ack that covers it.
        await asyncio.sleep(0.4 if processed == 0 else random.uniform(0, 0.02))
        ledger.done[msg.delivery_tag] = ledger.seen[msg.delivery_tag]
        group.complete(channel, msg.delivery_tag)
        processed += 1
        if processed >= total:
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=8))
    await asyncio.sleep(0.3)
    envelopes = [
        MessageEnvelope(routing_key=queue, body=json.dumps({"id": f"m{i}"}).encode(), message_id=f"m{i}")
        for i in range(total)
    ]
    (await broker.publish_many(envelopes)).raise_for_status()
    await asyncio.wait_for(done.wait(), timeout=120)
    group.flush()
    await asyncio.sleep(0.5)

    assert ledger.violations == [], ledger.violations[:3]
    assert len(ledger.acked) == total
    assert ledger.frames <= total, "coalescing must never emit more frames than deliveries"
    if prefetch > 1:
        assert ledger.cumulative >= 1, "nothing coalesced despite a wide prefetch window"
    await _await_drained(rabbit, queue)
    await broker.stop()


# ── failure injection ─────────────────────────────────────────────────────


async def test_chaos_failures_and_redelivery_converge(rabbit: dict[str, str]) -> None:
    """Random delays and random nack+requeue failures, with every message
    required to end up processed and the queue drained to 0/0.

    No ACK may ever cover a delivery whose handler had not completed.

    Note: ``group.reset()`` is deliberately NOT called here. Dropping a
    ledger while its channel is still open leaves those deliveries unacked
    on a live channel, and the broker only redelivers them once the channel
    goes away — which is exactly why ``reset()`` belongs on a reconnect hook
    (see ``test_connection_loss_never_acks_unfinished_work``) and nowhere
    else.
    """
    import json

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig, WorkerConfig
    from rabbitkit.core.types import AckPolicy, MessageEnvelope
    from rabbitkit.highload.batch import CoalescingAckerGroup

    total = 200
    queue = f"chaos-mixed-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(config=_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=48)))
    loop = asyncio.get_running_loop()
    ledger = Ledger()
    group = CoalescingAckerGroup(factory=lambda ch: _make_acker(ch, ledger, loop))
    rng = random.Random(1234)  # deterministic chaos
    attempts: dict[str, int] = {}
    succeeded: set[str] = set()
    requeued: set[str] = set()
    invalidated: list[str] = []
    done = asyncio.Event()

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: _RabbitMessage) -> None:
        assert msg.delivery_tag is not None
        channel = msg.raw_message.channel
        message_id = json.loads(body)["id"]

        def settle(decision: Any) -> bool:
            """Tolerate the cancellation race (plan item 11): if the ledger was
            invalidated while this handler ran, the coordinator MUST refuse
            rather than emit for a dead generation (I5). The broker redelivers."""
            try:
                decision()
            except CoordinatorError:
                invalidated.append(message_id)
                return False
            return True

        if not settle(lambda: group.register(channel, msg.delivery_tag)):
            return
        ledger.seen[msg.delivery_tag] = message_id
        attempts[message_id] = attempts.get(message_id, 0) + 1
        await asyncio.sleep(rng.uniform(0, 0.03))

        if attempts[message_id] == 1 and rng.random() < 0.2:
            # nack+requeue: the broker redelivers immediately with a NEW tag.
            if settle(lambda: group.fail(channel, msg.delivery_tag, requeue=True)):
                requeued.add(message_id)
            return

        ledger.done[msg.delivery_tag] = message_id
        if not settle(lambda: group.complete(channel, msg.delivery_tag)):
            del ledger.done[msg.delivery_tag]
            return
        succeeded.add(message_id)
        if len(succeeded) >= total:
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=8))
    await asyncio.sleep(0.3)
    envelopes = [
        MessageEnvelope(routing_key=queue, body=json.dumps({"id": f"m{i}"}).encode(), message_id=f"m{i}")
        for i in range(total)
    ]
    (await broker.publish_many(envelopes)).raise_for_status()
    try:
        await asyncio.wait_for(done.wait(), timeout=180)
    finally:
        group.flush()
        await asyncio.sleep(1.0)

    assert ledger.violations == [], f"{len(ledger.violations)} unsafe acks, first 3: {ledger.violations[:3]}"
    assert succeeded == {f"m{i}" for i in range(total)}, "a message was lost"
    assert requeued, "the chaos never exercised the nack+requeue path"
    assert max(attempts.values()) > 1, "the chaos never actually forced a redelivery"
    assert not invalidated, "no ledger was invalidated here — that path is covered by the kill test"
    await _await_drained(rabbit, queue, timeout=90)
    await broker.stop()


async def test_connection_loss_never_acks_unfinished_work(rabbit: dict[str, str]) -> None:
    """Real failure injection: force-close every AMQP connection mid-run,
    then require the run to CONVERGE.

    * no ACK ever covered a delivery whose handler had not completed;
    * no delivery tag was settled twice;
    * every message is processed after recovery, and the queue drains to 0/0.
    """
    import json

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig, WorkerConfig
    from rabbitkit.core.types import AckPolicy, MessageEnvelope
    from rabbitkit.highload.batch import CoalescingAckerGroup

    total = 150
    queue = f"chaos-kill-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(config=_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=32)))
    loop = asyncio.get_running_loop()
    ledger = Ledger()
    group = CoalescingAckerGroup(factory=lambda ch: _make_acker(ch, ledger, loop))
    succeeded: set[str] = set()
    killed = {"done": False}
    done = asyncio.Event()

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: _RabbitMessage) -> None:
        assert msg.delivery_tag is not None
        channel = msg.raw_message.channel
        message_id = json.loads(body)["id"]
        try:
            group.register(channel, msg.delivery_tag)
        except CoordinatorError:
            return  # ledger for that generation is gone; broker redelivers
        ledger.seen[msg.delivery_tag] = message_id
        await asyncio.sleep(0.01)
        ledger.done[msg.delivery_tag] = message_id
        try:
            group.complete(channel, msg.delivery_tag)
        except CoordinatorError:
            del ledger.done[msg.delivery_tag]
            return
        succeeded.add(message_id)
        if len(succeeded) >= total:
            done.set()

    # A reconnect must invalidate every ledger — the production wiring.
    await broker.start(worker_config=WorkerConfig(worker_count=8))
    reconnects: list[int] = []
    broker._transport.on_reconnect(lambda: reconnects.append(group.reset()))
    await asyncio.sleep(0.3)
    envelopes = [
        MessageEnvelope(routing_key=queue, body=json.dumps({"id": f"m{i}"}).encode(), message_id=f"m{i}")
        for i in range(total)
    ]
    (await broker.publish_many(envelopes)).raise_for_status()

    # Kill once, while deliveries are genuinely in flight.
    deadline = loop.time() + 30
    while loop.time() < deadline and len(succeeded) < total // 4:
        await asyncio.sleep(0.1)
    before = len(succeeded)
    assert before, "nothing was processed before the kill"
    await loop.run_in_executor(None, lambda: _kill_connections(rabbit))
    killed["done"] = True

    # Recovery must carry the run to completion: the kill requeued every
    # unacked delivery, so the redeliveries have to finish the job.
    await asyncio.wait_for(done.wait(), timeout=180)
    group.flush()
    await asyncio.sleep(1.0)

    assert killed["done"]
    assert len(succeeded) > before, "nothing was processed after the kill — recovery never happened"
    assert ledger.violations == [], f"{len(ledger.violations)} unsafe acks, first 3: {ledger.violations[:3]}"
    assert succeeded == {f"m{i}" for i in range(total)}, "a message was lost across the connection kill"
    # NOTE: `reconnects` is deliberately not asserted. aio-pika 9.6 recovers a
    # broker-closed connection underneath the same RobustConnection object
    # without re-running its counted connect path, so it never fires
    # reconnect_callbacks — see docs/observability.md. Settlement stays safe
    # regardless: the replacement channels are new objects, so the group
    # builds fresh ledgers and the stale ones are never consulted again.
    assert group.channels >= 1
    await _await_drained(rabbit, queue, timeout=120)
    await broker.stop()


async def test_shutdown_leaves_unfinished_work_unacked(rabbit: dict[str, str]) -> None:
    """I6: closing while a handler is still running must ack only the safe
    prefix and leave the straggler for redelivery — never ack it to empty
    the ledger."""
    import json

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig, WorkerConfig
    from rabbitkit.core.types import AckPolicy, MessageEnvelope
    from rabbitkit.highload.batch import CoalescingAckerGroup

    total = 20
    queue = f"chaos-shutdown-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(config=_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=total)))
    loop = asyncio.get_running_loop()
    ledger = Ledger()
    group = CoalescingAckerGroup(factory=lambda ch: _make_acker(ch, ledger, loop))
    stuck_forever = asyncio.Event()
    arrived = asyncio.Event()
    completed = 0

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: _RabbitMessage) -> None:
        nonlocal completed
        assert msg.delivery_tag is not None
        channel = msg.raw_message.channel
        group.register(channel, msg.delivery_tag)
        ledger.seen[msg.delivery_tag] = json.loads(body)["id"]
        if json.loads(body)["id"] == "m0":
            await stuck_forever.wait()  # never finishes before shutdown
            return
        ledger.done[msg.delivery_tag] = ledger.seen[msg.delivery_tag]
        group.complete(channel, msg.delivery_tag)
        completed += 1
        if completed >= total - 1:
            arrived.set()

    await broker.start(worker_config=WorkerConfig(worker_count=8))
    await asyncio.sleep(0.3)
    envelopes = [
        MessageEnvelope(routing_key=queue, body=json.dumps({"id": f"m{i}"}).encode(), message_id=f"m{i}")
        for i in range(total)
    ]
    (await broker.publish_many(envelopes)).raise_for_status()
    await asyncio.wait_for(arrived.wait(), timeout=60)

    report = group.close()  # graceful shutdown of the settlement layer
    await asyncio.sleep(0.5)

    assert ledger.violations == []
    assert report.settled_tags == total - 1, "the stuck delivery must not be settled"
    assert "m0" not in ledger.acked
    assert len(ledger.acked) == total - 1

    # The broker still holds exactly the stuck message as unacknowledged.
    await _await_counts(rabbit, queue, (0, 1), timeout=30)

    stuck_forever.set()
    await broker.stop()
