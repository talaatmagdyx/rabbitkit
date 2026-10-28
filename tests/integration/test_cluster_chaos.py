"""Multi-node failure tests against real brokers (failure-matrix rows F19, F20).

* **F19 -- quorum loss.** A 3-node RabbitMQ cluster with a 3-replica quorum
  queue loses 2 of its 3 nodes. A publish to the queue must not be reported
  CONFIRMED (the broker cannot commit it without a majority), it must resolve
  within the confirm timeout instead of hanging, and once the nodes come back
  publishing confirms again and every message ever reported CONFIRMED is
  still in the queue -- exactly once.
* **F20 -- network partition.** Toxiproxy sits between the client and a single
  broker. A blackhole toxic (``timeout`` with ``timeout=0``: data is dropped,
  the TCP connection is NOT closed) must be detected by the AMQP heartbeat,
  in-flight publishes must resolve as TIMEOUT/UNKNOWN (never CONFIRMED), the
  broker must report itself not connected, and after the toxic is removed the
  client reconnects on its own, its consumer resumes, and no confirmed
  message is lost. Run both as a full partition and as a one-way
  (``downstream``) one, where the broker never closes anything and only the
  client's heartbeat can notice.

The partition scenario also covers three defects it originally exposed, now
fixed: queued publishes resolving one admission timeout after another,
readiness not recovering without publish traffic, and ``stop(timeout)`` not
being bounded during a partition.

These tests start their own containers on a private Docker network and are
SLOW (cluster formation and node restarts dominate), so they carry the
``cluster_chaos`` marker and NOT ``integration``: the per-PR integration job
(``-m integration``) does not run them; the nightly workflow does, under
``RK_REQUIRE_BROKER=1`` so a missing Docker daemon fails instead of skipping.

Run locally::

    RK_REQUIRE_BROKER=1 .venv/bin/pytest tests/integration/test_cluster_chaos.py \\
        -m cluster_chaos -o addopts="" -p no:cacheprovider -v

Every container and network created here is removed in a ``finally``, even
when the test fails. Only containers this module started are ever stopped.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import json
import logging
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest

from tests.integration.conftest import skip_without_docker, wait_for_consumers

pytestmark = [pytest.mark.cluster_chaos]

logger = logging.getLogger(__name__)

RABBIT_IMAGE = "rabbitmq:3.13-management-alpine"
TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0"
LABEL = {"io.rabbitkit.test": "cluster_chaos"}
HOST = "127.0.0.1"


# ── docker plumbing ──────────────────────────────────────────────────────────


def _docker() -> Any:
    import docker  # type: ignore[import-untyped]

    return docker.from_env()


def _exec(container: Any, *cmd: str) -> tuple[int, str]:
    result = container.exec_run(list(cmd))
    output = result.output.decode("utf-8", "replace") if isinstance(result.output, bytes) else str(result.output)
    return int(result.exit_code), output


def _wait(predicate: Callable[[], bool], timeout: float, what: str, interval: float = 0.5) -> float:
    """Poll until *predicate* holds; return the seconds it took."""
    start = time.monotonic()
    while True:
        if predicate():
            return time.monotonic() - start
        if time.monotonic() - start > timeout:
            raise AssertionError(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(interval)


def _host_port(container: Any, port: str) -> int:
    container.reload()
    return int(container.ports[port][0]["HostPort"])


def _remove_quietly(*objects: Any) -> None:
    """Force-remove containers / remove networks this module created."""
    for obj in objects:
        with contextlib.suppress(Exception):
            if hasattr(obj, "kill"):  # a container
                obj.remove(force=True, v=True)
            else:  # a network
                obj.remove()


@contextlib.contextmanager
def _network(client: Any, name: str) -> Iterator[Any]:
    network = client.networks.create(name, driver="bridge", labels=LABEL)
    try:
        yield network
    finally:
        _remove_quietly(network)


def _run_rabbit(client: Any, name: str, hostname: str, network: str, cookie: str) -> Any:
    return client.containers.run(
        RABBIT_IMAGE,
        name=name,
        hostname=hostname,
        network=network,
        environment={"RABBITMQ_ERLANG_COOKIE": cookie},
        # 15672: the management API, for the QueueMetricsPoller quorum test.
        ports={"5672/tcp": (HOST, None), "15672/tcp": (HOST, None)},
        labels=LABEL,
        detach=True,
    )


def _running(container: Any) -> bool:
    container.reload()
    if container.status == "exited":
        tail = container.logs(tail=40).decode("utf-8", "replace")
        raise AssertionError(f"container {container.name} exited:\n{tail}")
    return bool(container.status == "running")


def _booted(container: Any) -> bool:
    """First boot finished, judged from the log.

    Deliberately NOT ``rabbitmqctl``/``rabbitmq-diagnostics`` on a fresh
    container: the CLI writes the Erlang cookie file from
    ``RABBITMQ_ERLANG_COOKIE`` and, if it wins the race with the server, the
    server refuses to start ("cookie file ... wrong permissions"/conflict) and
    the container exits.
    """
    return _running(container) and b"Server startup complete" in container.logs()


def _node_up(container: Any) -> bool:
    """A (re)started node answers the CLI (its cookie file already exists)."""
    return _running(container) and _exec(container, "rabbitmq-diagnostics", "-q", "check_running")[0] == 0


# ── F19 fixture: a 3-node cluster ────────────────────────────────────────────


@dataclass
class Cluster:
    nodes: list[Any]
    hostnames: list[str]
    events: list[str] = field(default_factory=list)

    def url(self, index: int = 0) -> str:
        return f"amqp://guest:guest@{HOST}:{_host_port(self.nodes[index], '5672/tcp')}/"

    def mgmt(self, index: int = 0) -> str:
        return f"http://{HOST}:{_host_port(self.nodes[index], '15672/tcp')}"

    def running_nodes(self) -> int:
        code, out = _exec(self.nodes[0], "rabbitmqctl", "cluster_status", "--formatter", "json")
        if code != 0:
            return 0
        try:
            return len(json.loads(out[out.index("{") :]).get("running_nodes", []))
        except ValueError:
            return 0

    def stop(self, *indexes: int) -> None:
        threads = [threading.Thread(target=self.nodes[i].stop, kwargs={"timeout": 30}) for i in indexes]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def start(self, *indexes: int) -> None:
        """Restart stopped nodes ONE AT A TIME.

        Two nodes booting together can each run RabbitMQ 3.13's cluster
        consistency check against the other while it is still starting; the
        loser exits with ``{node_not_running, ...}`` (seen locally about 1 run
        in 6). That is a broker boot race, not the scenario under test, so a
        node that exits during boot is started again (at most twice more).
        """
        for i in indexes:
            node, name = self.nodes[i], self.hostnames[i]
            for attempt in range(3):
                node.start()
                try:
                    _wait(lambda node=node: _node_up(node), 120, f"node {name} to restart")
                    break
                except AssertionError as exc:
                    if "exited" not in str(exc) or attempt == 2:
                        raise
                    self.events.append(f"{name}: exited during boot, restarted (attempt {attempt + 2})")
                    logger.warning("F19: node %s exited during boot; starting it again", name)
        _wait(lambda: self.running_nodes() == len(self.nodes), 120, "all nodes to rejoin the cluster")


@pytest.fixture(scope="module")
def cluster() -> Iterator[Cluster]:
    skip_without_docker()
    client = _docker()
    tag = uuid.uuid4().hex[:8]
    cookie = f"rkchaos{uuid.uuid4().hex}"
    hostnames = [f"rk{tag}n{i}" for i in (1, 2, 3)]
    containers: list[Any] = []
    with _network(client, f"rk-chaos-{tag}") as network:
        try:
            for hostname in hostnames:
                containers.append(_run_rabbit(client, f"rk-chaos-{hostname}", hostname, network.name, cookie))
            for container, hostname in zip(containers, hostnames, strict=True):
                _wait(lambda c=container: _booted(c), 120, f"node {hostname} to boot")
            for container in containers[1:]:
                for cmd in (
                    ("rabbitmqctl", "stop_app"),
                    ("rabbitmqctl", "join_cluster", f"rabbit@{hostnames[0]}"),
                    ("rabbitmqctl", "start_app"),
                ):
                    code, out = _exec(container, *cmd)
                    assert code == 0, f"{' '.join(cmd)} failed: {out}"
            cluster = Cluster(nodes=containers, hostnames=hostnames)
            _wait(lambda: cluster.running_nodes() == 3, 120, "a 3-node cluster")
            yield cluster
        finally:
            _remove_quietly(*containers)


def _declare_quorum(url: str, queue: str) -> None:
    import pika

    connection = pika.BlockingConnection(pika.URLParameters(url))
    try:
        connection.channel().queue_declare(
            queue=queue,
            durable=True,
            arguments={"x-queue-type": "quorum", "x-quorum-initial-group-size": 3},
        )
    finally:
        connection.close()


def _quorum_members_online(cluster: Cluster, queue: str) -> int:
    code, out = _exec(cluster.nodes[0], "rabbitmq-queues", "quorum_status", queue, "--formatter", "json")
    if code != 0:
        return 0
    try:
        rows = json.loads(out[out.index("[") :])
    except ValueError:
        return 0
    return sum(1 for row in rows if str(row.get("Raft State", "")) in ("leader", "follower"))


def _drain_ids(url: str, queue: str) -> list[str]:
    """Every message_id in *queue* (consumed with ack), in queue order."""
    import pika

    ids: list[str] = []
    connection = pika.BlockingConnection(pika.URLParameters(url))
    try:
        channel = connection.channel()
        while True:
            method, props, _body = channel.basic_get(queue=queue, auto_ack=True)
            if method is None:
                return ids
            ids.append(str(props.message_id))
    finally:
        connection.close()


def _rabbit_config(url: str, **publisher: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, PublisherConfig, RabbitConfig, SafetyConfig

    return RabbitConfig(
        connection=ConnectionConfig.from_url(url),
        publisher=PublisherConfig(confirm_delivery=True, mandatory=True, **publisher),
        safety=SafetyConfig(reject_without_dlx="discard"),
    )


def _envelope(queue: str, message_id: str) -> Any:
    from rabbitkit.core.types import MessageEnvelope

    return MessageEnvelope(routing_key=queue, body=message_id.encode(), message_id=message_id, mandatory=True)


def _assert_no_loss(confirmed: list[str], drained: list[str]) -> None:
    counts = collections.Counter(drained)
    duplicated = sorted(mid for mid, n in counts.items() if n > 1)
    missing = sorted(set(confirmed) - set(drained))
    assert not missing, f"{len(missing)} CONFIRMED message(s) lost: {missing[:10]}"
    assert not duplicated, f"message(s) delivered more than once (republished?): {duplicated[:10]}"


CONFIRM_TIMEOUT = 3.0
# A publish must resolve within its confirm timeout plus scheduling slack.
# Anything longer is the "client hangs" failure F19 is about.
RESOLVE_BOUND = CONFIRM_TIMEOUT + 5.0


# ── F19: quorum loss (async) ─────────────────────────────────────────────────


@pytest.mark.timeout(600)
@pytest.mark.asyncio
async def test_async_quorum_loss_never_confirms_and_loses_no_confirmed_message(cluster: Cluster) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.types import PublishStatus

    url = cluster.url(0)
    queue = f"rk-chaos-qq-async-{uuid.uuid4().hex[:8]}"
    await asyncio.to_thread(_declare_quorum, url, queue)
    await asyncio.to_thread(_wait, lambda: _quorum_members_online(cluster, queue) == 3, 60, "3 quorum members")

    confirmed: list[str] = []
    broker = AsyncBroker(_rabbit_config(url, confirm_timeout=CONFIRM_TIMEOUT))
    await broker.start(install_signal_handlers=False)
    try:
        for i in range(20):
            outcome = await broker.publish(_envelope(queue, f"before-{i}"))
            assert outcome.status is PublishStatus.CONFIRMED, outcome
            confirmed.append(f"before-{i}")

        await asyncio.to_thread(cluster.stop, 1, 2)

        during = [f"during-{i}" for i in range(5)]
        started = time.monotonic()
        outcomes = await asyncio.wait_for(
            asyncio.gather(*(broker.publish(_envelope(queue, mid)) for mid in during)), timeout=RESOLVE_BOUND
        )
        resolved_in = time.monotonic() - started
        statuses = [o.status for o in outcomes]
        logger.warning("F19 async: quorum-loss publishes resolved in %.2fs as %s", resolved_in, statuses)
        assert PublishStatus.CONFIRMED not in statuses, outcomes
        assert all(o.is_unknown or o.status is PublishStatus.NACKED for o in outcomes), outcomes

        await asyncio.to_thread(cluster.start, 1, 2)

        # Publishing confirms again once the queue has a majority. Probes that
        # land before the leader is back may still time out; they are not
        # counted as confirmed.
        deadline = time.monotonic() + 120
        recovered_in: float | None = None
        probe = 0
        while recovered_in is None:
            assert time.monotonic() < deadline, "publishing never confirmed again after the nodes returned"
            mid = f"probe-{probe}"
            probe += 1
            outcome = await asyncio.wait_for(broker.publish(_envelope(queue, mid)), timeout=RESOLVE_BOUND)
            if outcome.status is PublishStatus.CONFIRMED:
                confirmed.append(mid)
                recovered_in = time.monotonic() - started
        logger.warning("F19 async: confirms resumed %.1fs after the quorum loss began", recovered_in)

        for i in range(20):
            outcome = await broker.publish(_envelope(queue, f"after-{i}"))
            assert outcome.status is PublishStatus.CONFIRMED, outcome
            confirmed.append(f"after-{i}")
    finally:
        await broker.stop(timeout=10)

    drained = await asyncio.to_thread(_drain_ids, url, queue)
    logger.warning(
        "F19 async: %d confirmed, %d in queue, unconfirmed quorum-loss publishes present: %s",
        len(confirmed),
        len(drained),
        sorted(set(during) & set(drained)),
    )
    _assert_no_loss(confirmed, drained)


# ── F19: quorum loss (sync) ──────────────────────────────────────────────────


@pytest.mark.timeout(600)
def test_sync_quorum_loss_never_confirms_and_loses_no_confirmed_message(cluster: Cluster) -> None:
    from rabbitkit.core.types import PublishStatus
    from rabbitkit.sync.broker import SyncBroker

    url = cluster.url(0)
    queue = f"rk-chaos-qq-sync-{uuid.uuid4().hex[:8]}"
    _declare_quorum(url, queue)
    _wait(lambda: _quorum_members_online(cluster, queue) == 3, 60, "3 quorum members")

    confirmed: list[str] = []
    during = [f"during-{i}" for i in range(3)]
    broker = SyncBroker(_rabbit_config(url, confirm_timeout=CONFIRM_TIMEOUT))
    broker.start()
    try:
        for i in range(20):
            outcome = broker.publish(_envelope(queue, f"before-{i}"))
            assert outcome.status is PublishStatus.CONFIRMED, outcome
            confirmed.append(f"before-{i}")

        cluster.stop(1, 2)

        for mid in during:
            started = time.monotonic()
            outcome = broker.publish(_envelope(queue, mid))
            took = time.monotonic() - started
            logger.warning("F19 sync: quorum-loss publish resolved in %.2fs as %s", took, outcome.status)
            assert took < RESOLVE_BOUND, f"publish took {took:.1f}s (confirm_timeout={CONFIRM_TIMEOUT})"
            assert outcome.status is not PublishStatus.CONFIRMED, outcome
            assert outcome.is_unknown or outcome.status is PublishStatus.NACKED, outcome

        cluster.start(1, 2)

        deadline = time.monotonic() + 120
        probe = 0
        while True:
            assert time.monotonic() < deadline, "publishing never confirmed again after the nodes returned"
            mid = f"probe-{probe}"
            probe += 1
            outcome = broker.publish(_envelope(queue, mid))
            if outcome.status is PublishStatus.CONFIRMED:
                confirmed.append(mid)
                break

        for i in range(20):
            outcome = broker.publish(_envelope(queue, f"after-{i}"))
            assert outcome.status is PublishStatus.CONFIRMED, outcome
            confirmed.append(f"after-{i}")
    finally:
        broker.stop()

    drained = _drain_ids(url, queue)
    logger.warning(
        "F19 sync: %d confirmed, %d in queue, unconfirmed quorum-loss publishes present: %s",
        len(confirmed),
        len(drained),
        sorted(set(during) & set(drained)),
    )
    _assert_no_loss(confirmed, drained)


# ── quorum health gauges (QueueMetricsPoller) ───────────────────────────────


@pytest.mark.timeout(600)
def test_queue_metrics_poller_quorum_online_drops_when_a_member_stops(cluster: Cluster) -> None:
    """The management API's ``members`` / ``online`` lists, exported as
    rabbitkit_queue_quorum_members / _online, follow a member going down and
    coming back: what RabbitMQQuorumUnhealthy / RabbitMQQuorumMajorityLost
    alert on, with no broker plugin."""
    from rabbitkit.management import ManagementConfig, RabbitManagementClient
    from rabbitkit.queue_metrics import QueueMetricsPoller

    queue = f"rk-chaos-qq-poll-{uuid.uuid4().hex[:8]}"
    _declare_quorum(cluster.url(0), queue)
    client = RabbitManagementClient(ManagementConfig(url=cluster.mgmt(0), username="guest", password="guest"))
    gauges: dict[str, float] = {}

    class Collector:
        def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
            gauges[name] = value

    poller = QueueMetricsPoller(client, Collector(), queue_filter=lambda name: name == queue)

    def online_is(expected: float) -> bool:
        poller.poll_once()
        return gauges.get("rabbitkit_queue_quorum_online") == expected

    _wait(lambda: online_is(3.0), 60, "3 of 3 members online", interval=1.0)
    assert gauges["rabbitkit_queue_quorum_members"] == 3.0
    cluster.stop(2)
    try:
        took = _wait(lambda: online_is(2.0), 120, "the stopped member to drop out of `online`", interval=1.0)
        logger.warning("quorum poller: online dropped to 2 of 3 %.1fs after the node stopped", took)
        assert gauges["rabbitkit_queue_quorum_members"] == 3.0  # still a member, just not online
    finally:
        cluster.start(2)
    _wait(lambda: online_is(3.0), 120, "the member to come back online", interval=1.0)


# ── F20 fixture: broker behind toxiproxy ─────────────────────────────────────

#: Which directions the blackhole drops. ``both``: a full partition (the
#: broker also stops hearing the client, so after its own heartbeat timeout
#: it closes its side and toxiproxy relays that close). ``downstream``: the
#: client hears nothing but the broker still hears the client, so the broker
#: never closes anything -- only the CLIENT's heartbeat can detect it.
DIRECTIONS = {"both": ("upstream", "downstream"), "downstream": ("downstream",)}


@dataclass
class Partition:
    rabbit: Any
    proxy: Any

    @property
    def direct_url(self) -> str:
        """Straight to the broker, bypassing the proxy (for verification)."""
        return f"amqp://guest:guest@{HOST}:{_host_port(self.rabbit, '5672/tcp')}/"

    @property
    def proxy_url(self) -> str:
        return f"amqp://guest:guest@{HOST}:{_host_port(self.proxy, '5673/tcp')}/"

    def _api(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        url = f"http://{HOST}:{_host_port(self.proxy, '8474/tcp')}{path}"
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(url, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 -- local toxiproxy API
            body = response.read()
        return json.loads(body) if body else None

    def create_proxy(self, upstream: str) -> None:
        self._api("POST", "/proxies", {"name": "amqp", "listen": "0.0.0.0:5673", "upstream": upstream})

    def blackhole(self, direction: str = "both") -> None:
        """Drop bytes WITHOUT closing the TCP connection (``timeout`` toxic, ``timeout=0``)."""
        for stream in DIRECTIONS[direction]:
            self._api(
                "POST",
                "/proxies/amqp/toxics",
                {"name": f"blackhole-{stream}", "type": "timeout", "stream": stream, "attributes": {"timeout": 0}},
            )

    def heal(self) -> None:
        """Remove every blackhole toxic (idempotent: absent toxics are fine)."""
        for stream in DIRECTIONS["both"]:
            with contextlib.suppress(urllib.error.HTTPError):
                self._api("DELETE", f"/proxies/amqp/toxics/blackhole-{stream}")


@pytest.fixture(scope="module")
def partition() -> Iterator[Partition]:
    skip_without_docker()
    client = _docker()
    tag = uuid.uuid4().hex[:8]
    containers: list[Any] = []
    with _network(client, f"rk-chaos-{tag}-p") as network:
        try:
            rabbit = _run_rabbit(client, f"rk-chaos-{tag}-rmq", f"rk{tag}rmq", network.name, f"rkchaos{tag}")
            containers.append(rabbit)
            proxy = client.containers.run(
                TOXIPROXY_IMAGE,
                name=f"rk-chaos-{tag}-toxiproxy",
                network=network.name,
                ports={"8474/tcp": (HOST, None), "5673/tcp": (HOST, None)},
                labels=LABEL,
                detach=True,
            )
            containers.append(proxy)
            _wait(lambda: _booted(rabbit), 120, "the broker to boot")
            setup = Partition(rabbit=rabbit, proxy=proxy)

            def api_up() -> bool:
                try:
                    setup._api("GET", "/proxies")
                except OSError:
                    return False
                return True

            _wait(api_up, 60, "the toxiproxy API")
            setup.create_proxy(f"rk{tag}rmq:5672")
            yield setup
        finally:
            _remove_quietly(*containers)


HEARTBEAT = 2
# Detection bound, from the client library rather than a guess: aiormq
# declares a connection stuck when no frame arrived for
# (heartbeat + 1) * HEARTBEAT_GRACE_MULTIPLIER(3) seconds and checks every
# heartbeat // 2 seconds. With heartbeat=2 that is ~9-10s, i.e. ~5x the
# heartbeat, not 2x. Plus scheduling slack.
DETECTION_BOUND = (HEARTBEAT + 1) * 3 + HEARTBEAT + 3.0
ACQUIRE_TIMEOUT = 3.0


def _partition_config(url: str) -> Any:
    from dataclasses import replace

    from rabbitkit.core.config import PoolConfig

    config = _rabbit_config(url, confirm_timeout=CONFIRM_TIMEOUT)
    return replace(
        config,
        connection=replace(
            config.connection,
            heartbeat=HEARTBEAT,
            socket_timeout=3.0,
            reconnect_backoff_base=0.5,
            reconnect_backoff_max=2.0,
        ),
        pool=PoolConfig(channel_acquire_timeout=ACQUIRE_TIMEOUT),
    )


async def _until(predicate: Callable[[], Any], timeout: float, what: str) -> float:
    """Await until *predicate* (sync or async) holds; return the seconds it took."""
    start = time.monotonic()
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return time.monotonic() - start
        if time.monotonic() - start > timeout:
            raise AssertionError(f"timed out after {timeout:.0f}s waiting for {what}")
        await asyncio.sleep(0.1)


@contextlib.asynccontextmanager
async def _partitioned_broker(partition: Partition, queue: str, received: list[str]) -> Any:
    """A started AsyncBroker (through the proxy) consuming *queue*.

    On exit the partition is healed BEFORE ``stop()``: ``stop()`` during a
    partition does not honour its timeout (see
    ``test_async_stop_during_partition_honours_its_timeout``), and a test that
    failed mid-partition must still clean up.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.topology import RabbitQueue

    broker = AsyncBroker(_partition_config(partition.proxy_url))

    @broker.subscriber(queue=RabbitQueue(name=queue, durable=True))
    async def handle(message: RabbitMessage) -> None:
        received.append(str(message.message_id))

    await asyncio.to_thread(partition.heal)
    await broker.start(install_signal_handlers=False)
    try:
        yield broker
    finally:
        await asyncio.to_thread(partition.heal)
        await broker.stop(timeout=10)


async def _connected(broker: Any) -> bool:
    from rabbitkit.health import broker_health_check_async

    return (await broker_health_check_async(broker)).connected


async def _disconnected(broker: Any) -> bool:
    return not await _connected(broker)


# ── F20: network partition (async) ───────────────────────────────────────────


@pytest.mark.timeout(300)
@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["both", "downstream"])
async def test_async_blackhole_partition_detected_unknown_then_recovers_without_loss(
    partition: Partition, direction: str
) -> None:
    from rabbitkit.core.types import PublishStatus
    from rabbitkit.health import broker_health_check_async

    queue = f"rk-chaos-part-{direction}-{uuid.uuid4().hex[:8]}"
    received: list[str] = []
    confirmed: list[str] = []
    during = [f"during-{i}" for i in range(3)]
    async with _partitioned_broker(partition, queue, received) as broker:
        for i in range(10):
            outcome = await broker.publish(_envelope(queue, f"before-{i}"))
            assert outcome.status is PublishStatus.CONFIRMED, outcome
            confirmed.append(f"before-{i}")
        await _until(lambda: len(received) >= 10, 30, "the pre-partition messages to be consumed")
        assert await _connected(broker)

        await asyncio.to_thread(partition.blackhole, direction)
        partitioned_at = time.monotonic()
        detection = asyncio.create_task(_until(lambda: _disconnected(broker), DETECTION_BOUND, "partition detection"))

        # The first publish holds a channel and is on the wire: it can only
        # end TIMEOUT/UNKNOWN. Publishes queued behind it never get a channel
        # and may end NOT_SENT. None may end CONFIRMED. The bound allows for
        # admissions resolving one after another (one ACQUIRE_TIMEOUT each);
        # see test_async_publishes_queued_during_partition_resolve_within_their_own_budget.
        resolve_bound = CONFIRM_TIMEOUT + DETECTION_BOUND + len(during) * (ACQUIRE_TIMEOUT + 3.0)
        outcomes = await asyncio.wait_for(
            asyncio.gather(*(broker.publish(_envelope(queue, mid)) for mid in during)), timeout=resolve_bound
        )
        logger.warning(
            "F20[%s]: publishes during the partition resolved within %.1fs as %s",
            direction,
            time.monotonic() - partitioned_at,
            [(o.status.value, o.reason) for o in outcomes],
        )
        assert all(o.status is not PublishStatus.CONFIRMED for o in outcomes), outcomes
        assert all(o.is_unknown or o.status is PublishStatus.NOT_SENT for o in outcomes), outcomes
        assert outcomes[0].is_unknown, outcomes[0]

        detected_in = await detection
        logger.warning("F20[%s]: broker reported not connected %.1fs into the partition", direction, detected_in)
        assert not (await broker_health_check_async(broker)).connected

        await asyncio.to_thread(partition.heal)
        healed_at = time.monotonic()
        # The consumer connection reconnects on its own (with backoff) and
        # re-registers its consumer.
        await asyncio.to_thread(wait_for_consumers, partition.direct_url, queue, timeout=60)
        logger.warning("F20[%s]: consumer re-registered %.1fs after the heal", direction, time.monotonic() - healed_at)

        for i in range(10):
            outcome = await broker.publish(_envelope(queue, f"after-{i}"))
            assert outcome.status is PublishStatus.CONFIRMED, outcome
            confirmed.append(f"after-{i}")
        # Readiness is back once a publish has rebuilt the publisher
        # connection; without traffic it is not (see
        # test_async_readiness_recovers_after_partition_without_publish_traffic).
        await _until(lambda: _connected(broker), 30, "the broker to report connected")
        back_in = time.monotonic() - healed_at
        logger.warning("F20[%s]: publishing and readiness back %.1fs after the heal", direction, back_in)
        await _until(lambda: set(confirmed) <= set(received), 30, "the consumer to resume")

    leftover = await asyncio.to_thread(_drain_ids, partition.direct_url, queue)
    delivered = received + leftover
    logger.warning(
        "F20[%s]: %d confirmed, %d delivered, unconfirmed publishes from the partition delivered: %s",
        direction,
        len(confirmed),
        len(delivered),
        sorted(set(during) & set(delivered)),
    )
    missing = sorted(set(confirmed) - set(delivered))
    assert not missing, f"{len(missing)} CONFIRMED message(s) lost across the partition: {missing}"


@pytest.mark.timeout(300)
@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["both", "downstream"])
async def test_async_heartbeat_detects_a_silent_partition(partition: Partition, direction: str) -> None:
    """No traffic during the partition, so only the heartbeat can notice it.

    ``downstream`` is the pure client-side case: the broker keeps hearing the
    client's heartbeats and never closes anything, so only aiormq's
    missed-heartbeat check can flip the broker to not connected.
    """
    from rabbitkit.core.types import PublishStatus

    queue = f"rk-chaos-hb-{direction}-{uuid.uuid4().hex[:8]}"
    received: list[str] = []
    async with _partitioned_broker(partition, queue, received) as broker:
        assert (await broker.publish(_envelope(queue, "warm"))).status is PublishStatus.CONFIRMED
        assert await _connected(broker)
        await asyncio.to_thread(partition.blackhole, direction)
        detected_in = await _until(lambda: _disconnected(broker), DETECTION_BOUND, "heartbeat detection")
        logger.warning(
            "F20[%s]: idle partition detected after %.1fs (heartbeat=%ss, bound %.0fs)",
            direction,
            detected_in,
            HEARTBEAT,
            DETECTION_BOUND,
        )
        await asyncio.to_thread(partition.heal)
        await asyncio.to_thread(wait_for_consumers, partition.direct_url, queue, timeout=60)
        outcome = await broker.publish(_envelope(queue, "after"))
        assert outcome.status is PublishStatus.CONFIRMED, outcome
        await _until(lambda: {"warm", "after"} <= set(received), 30, "the consumer to resume")


# ── F20: bounded admission, readiness recovery and stop() during a partition ──
#
# These were strict xfails pinning three defects; fixed in the publish path
# (pool deadline-bounded reconnects, background publisher restore, bounded
# stop()), so they now run as ordinary tests.


@pytest.mark.timeout(300)
@pytest.mark.asyncio
async def test_async_publishes_queued_during_partition_resolve_within_their_own_budget(
    partition: Partition,
) -> None:
    queue = f"rk-chaos-admit-{uuid.uuid4().hex[:8]}"
    received: list[str] = []
    async with _partitioned_broker(partition, queue, received) as broker:
        await asyncio.to_thread(partition.blackhole, "both")
        started = time.monotonic()
        tasks = [asyncio.create_task(broker.publish(_envelope(queue, f"q-{i}"))) for i in range(5)]
        took: list[float] = []
        for task in tasks:
            task.add_done_callback(lambda _t: took.append(time.monotonic() - started))
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=120)
        logger.warning("F20 admission: queued publishes resolved after %s s", [round(t, 1) for t in took])
        # Each publish has its own budget: admission (ACQUIRE_TIMEOUT) plus
        # the confirm wait, independent of how many others are queued.
        assert max(took) <= ACQUIRE_TIMEOUT + CONFIRM_TIMEOUT + 5.0


@pytest.mark.timeout(300)
@pytest.mark.asyncio
async def test_async_readiness_recovers_after_partition_without_publish_traffic(partition: Partition) -> None:
    from rabbitkit.core.types import PublishStatus

    queue = f"rk-chaos-ready-{uuid.uuid4().hex[:8]}"
    received: list[str] = []
    async with _partitioned_broker(partition, queue, received) as broker:
        await broker.publish(_envelope(queue, "warm"))
        await asyncio.to_thread(partition.blackhole, "both")
        # Two concurrent publishes during the partition: the first holds the
        # open channel (and times out); the second needs a new channel, whose
        # open fails and triggers a publisher-connection rebuild that cannot
        # connect while the partition lasts (NOT_SENT, channel_unavailable).
        outcomes = await asyncio.gather(
            broker.publish(_envelope(queue, "during-0")), broker.publish(_envelope(queue, "during-1"))
        )
        assert [o.status for o in outcomes] == [PublishStatus.TIMEOUT, PublishStatus.NOT_SENT], outcomes
        await _until(lambda: _disconnected(broker), DETECTION_BOUND, "the partition to be detected")
        await asyncio.to_thread(partition.heal)
        await asyncio.to_thread(wait_for_consumers, partition.direct_url, queue, timeout=60)
        # The network is back and the consumer reconnected; readiness must
        # follow without anyone having to publish first.
        await _until(lambda: _connected(broker), 30, "readiness to recover without publish traffic")


@pytest.mark.timeout(300)
@pytest.mark.asyncio
async def test_async_stop_during_partition_honours_its_timeout(partition: Partition) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.topology import RabbitQueue

    queue = f"rk-chaos-stop-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(_partition_config(partition.proxy_url))

    @broker.subscriber(queue=RabbitQueue(name=queue, durable=True))
    async def handle(message: RabbitMessage) -> None:
        return None

    stop_timeout = 10.0
    await asyncio.to_thread(partition.heal)
    await broker.start(install_signal_handlers=False)
    stop: asyncio.Task[None] | None = None
    try:
        await asyncio.to_thread(partition.blackhole, "both")
        await _until(lambda: _disconnected(broker), DETECTION_BOUND, "the partition to be detected")
        started = time.monotonic()
        stop = asyncio.create_task(broker.stop(timeout=stop_timeout))
        done, _ = await asyncio.wait({stop}, timeout=stop_timeout + 10.0)
        took = time.monotonic() - started
        logger.warning("F20 stop: stop(timeout=%.0f) %s after %.1fs", stop_timeout, bool(done), took)
        assert done, f"stop(timeout={stop_timeout}) still running after {took:.1f}s of partition"
    finally:
        await asyncio.to_thread(partition.heal)
        if stop is not None:
            await asyncio.wait_for(stop, timeout=120)  # healed: the stuck stop now completes
        else:
            await broker.stop(timeout=10)
