"""`rabbitkit dlq` safety: issues #31, #32, #33.

#31: `inspect` (and `replay --dry-run`) nacked each message with requeue
before the next basic_get on the same channel. The requeued message went back
to the head, so `--limit 20` printed the first message 20 times and, on a
quorum queue, spent 20 of its deliveries.
"""

from __future__ import annotations

import urllib.error
from typing import Any
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from rabbitkit.cli import app

runner = CliRunner()


class _Queue:
    """A broker queue as basic_get + nack(requeue) see it: a requeued
    message goes back to the head."""

    def __init__(self, bodies: list[bytes], headers: dict[str, Any] | None = None) -> None:
        self.ready: list[tuple[int, bytes]] = list(enumerate(bodies, start=1))
        self.unacked: dict[int, bytes] = {}
        self.headers = headers or {}
        self.events: list[str] = []

    def basic_get(self, queue: str, auto_ack: bool = False) -> tuple[Any, Any, Any]:
        self.events.append("get")
        if not self.ready:
            return None, None, None
        tag, body = self.ready.pop(0)
        self.unacked[tag] = body
        method = MagicMock(routing_key=queue, exchange="", redelivered=False, delivery_tag=tag)
        props = MagicMock(message_id=f"id-{tag}", correlation_id=None, headers=dict(self.headers))
        return method, props, body

    def basic_nack(self, delivery_tag: int, requeue: bool = True) -> None:
        self.events.append("nack")
        body = self.unacked.pop(delivery_tag)
        if requeue:
            self.ready.insert(0, (delivery_tag, body))

    def basic_ack(self, delivery_tag: int) -> None:
        self.events.append("ack")
        self.unacked.pop(delivery_tag)


def _pika(queue: _Queue) -> MagicMock:
    mock_pika = MagicMock()
    channel = MagicMock()
    channel.basic_get.side_effect = queue.basic_get
    channel.basic_nack.side_effect = queue.basic_nack
    channel.basic_ack.side_effect = queue.basic_ack
    mock_pika.BlockingConnection.return_value.channel.return_value = channel

    class _Unroutable(Exception):
        pass

    class _Nack(Exception):
        pass

    mock_pika.exceptions.UnroutableError = _Unroutable
    mock_pika.exceptions.NackError = _Nack
    return mock_pika


def _run(queue: _Queue, args: list[str]) -> tuple[Any, MagicMock]:
    mock_pika = _pika(queue)
    with patch.dict("sys.modules", {"pika": mock_pika, "pika.exceptions": mock_pika.exceptions}):
        result = runner.invoke(app, ["dlq", *args])
    return result, mock_pika.BlockingConnection.return_value.channel.return_value


class TestInspectShowsEachMessageOnce:
    def test_three_messages_three_distinct_entries(self) -> None:
        queue = _Queue([b"one", b"two", b"three"])
        result, _ = _run(queue, ["inspect", "orders.dlq", "--limit", "20", "--format", "json"])
        assert result.exit_code == 0, result.output
        import json

        bodies = [m["body_preview"] for m in json.loads(result.output)]
        assert bodies == ["one", "two", "three"]  # not "one" x 20
        assert queue.unacked == {} and len(queue.ready) == 3  # everything back

    def test_nacks_only_after_the_fetch_loop(self) -> None:
        queue = _Queue([b"a", b"b"])
        _run(queue, ["inspect", "orders.dlq"])
        assert queue.events == ["get", "get", "get", "nack", "nack"]

    def test_releases_when_basic_get_fails_midway(self) -> None:
        queue = _Queue([b"a", b"b"])
        mock_pika = _pika(queue)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        calls = {"n": 0}

        def flaky(queue_name: str, auto_ack: bool = False) -> Any:
            calls["n"] += 1
            if calls["n"] == 2:
                raise ConnectionError("lost")
            return queue.basic_get(queue_name, auto_ack)

        channel.basic_get.side_effect = flaky
        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "inspect", "orders.dlq"])
        assert result.exit_code != 0
        assert queue.unacked == {}  # the first message was requeued
        mock_pika.BlockingConnection.return_value.close.assert_called_once()


class TestDryRunShowsEachMessageOnce:
    def test_dry_run(self) -> None:
        queue = _Queue([b"a", b"b"], headers={"x-rabbitkit-original-routing-key": "orders.created"})
        result, channel = _run(queue, ["replay", "orders.dlq", "orders", "--dry-run", "--limit", "10"])
        assert result.exit_code == 0
        assert result.output.count("[dry-run]") == 2
        assert "routing_key='orders.created'" in result.output
        channel.basic_publish.assert_not_called()
        assert queue.events[-2:] == ["nack", "nack"] and queue.unacked == {}


class TestTripwire:
    def test_inspect_stops_on_a_quorum_message(self) -> None:
        queue = _Queue([b"a", b"b", b"c"], headers={"x-delivery-count": 1})
        result, _ = _run(queue, ["inspect", "orders.dlq"])
        assert result.exit_code == 2
        assert "quorum queue" in result.output
        assert queue.events.count("get") == 1  # stopped at the first one
        assert queue.unacked == {}

    def test_inspect_check_can_be_disabled(self) -> None:
        queue = _Queue([b"a"], headers={"x-delivery-count": 1})
        result, _ = _run(queue, ["inspect", "orders.dlq", "--no-delivery-limit-check"])
        assert result.exit_code == 0

    def test_dry_run_trips(self) -> None:
        queue = _Queue([b"a"], headers={"x-delivery-count": 3})
        result, _ = _run(queue, ["replay", "orders.dlq", "orders", "--dry-run"])
        assert result.exit_code == 2 and queue.unacked == {}

    def test_real_replay_does_not_trip(self) -> None:
        """It acks what it republishes, so it doesn't spend deliveries."""
        queue = _Queue([b"a"], headers={"x-delivery-count": 3, "x-rabbitkit-original-routing-key": "rk"})
        result, channel = _run(queue, ["replay", "orders.dlq", "orders"])
        assert result.exit_code == 0
        channel.basic_publish.assert_called_once()


class TestManagementGuard:
    def _client(self, info: Any, version: str = "4.1.8") -> MagicMock:
        client = MagicMock()
        if isinstance(info, Exception):
            client.get_queue.side_effect = info
        else:
            client.get_queue.return_value = info
        client.overview.return_value = {"rabbitmq_version": version}
        return client

    def _run_with(self, client: MagicMock, args: list[str], queue: _Queue | None = None) -> tuple[Any, _Queue, Any]:
        queue = queue or _Queue([b"a"])
        with patch("rabbitkit.management.RabbitManagementClient", return_value=client) as ctor:
            result, _ = _run(queue, args)
        return result, queue, ctor

    def test_limited_quorum_refused_before_connecting(self) -> None:
        info = {"type": "quorum", "arguments": {"x-queue-type": "quorum"}, "messages": 1}
        result, queue, _ = self._run_with(
            self._client(info), ["inspect", "orders.dlq", "-m", "http://ops:pw@rabbit:15672"]
        )
        assert result.exit_code == 2
        assert "delivery limit of 20" in result.output
        assert queue.events == []

    def test_management_credentials_and_vhost(self) -> None:
        info = {"type": "classic", "arguments": {}, "messages": 1}
        client = self._client(info)
        result, _, ctor = self._run_with(
            client,
            ["inspect", "orders.dlq", "-u", "amqp://app:secret@rabbit/prod%2Feu", "-m", "http://rabbit:15672/"],
        )
        assert result.exit_code == 0, result.output
        config = ctor.call_args.args[0]
        assert (config.url, config.username, config.password) == ("http://rabbit:15672", "app", "secret")
        client.get_queue.assert_called_once_with("orders.dlq", vhost="prod/eu")

    def test_missing_queue_falls_through_to_amqp(self) -> None:
        err = urllib.error.HTTPError("u", 404, "nf", None, None)  # type: ignore[arg-type]
        args = ["inspect", "nope", "-m", "http://localhost:15672"]
        result, _, _ = self._run_with(self._client(err), args, _Queue([]))
        assert result.exit_code == 0

    def test_management_error_refuses(self) -> None:
        result, _, _ = self._run_with(self._client(OSError("down")), ["inspect", "q", "-m", "http://localhost:15672"])
        assert result.exit_code == 2 and "management API" in result.output

    def test_version_error_refuses(self) -> None:
        client = self._client({"type": "quorum", "messages": 1})
        client.overview.side_effect = OSError("down")
        result, _, _ = self._run_with(client, ["inspect", "q", "-m", "http://localhost:15672"])
        assert result.exit_code == 2 and "version" in result.output

    def test_counted_messages_are_fine_once_verified(self) -> None:
        info = {"type": "quorum", "arguments": {"x-delivery-limit": -1}, "messages": 1}
        queue = _Queue([b"a"], headers={"x-delivery-count": 5})
        result, _, _ = self._run_with(self._client(info), ["inspect", "q", "-m", "http://localhost:15672"], queue)
        assert result.exit_code == 0


class TestReplayRouting:
    def test_original_routing_key_from_x_death(self) -> None:
        deaths = [{"queue": "orders", "reason": "rejected", "routing-keys": ["orders.created"]}]
        queue = _Queue([b"a"], headers={"x-death": deaths})
        result, channel = _run(queue, ["replay", "orders.dlq", "orders"])
        assert result.exit_code == 0
        assert channel.basic_publish.call_args.kwargs["routing_key"] == "orders.created"

    def test_self_replay_into_the_dlq_is_skipped(self) -> None:
        """Default exchange + the DLQ's own name as routing key = publish it
        straight back onto the queue being drained (#33)."""
        queue = _Queue([b"a"])
        result, channel = _run(queue, ["replay", "orders.dlq", ""])
        assert result.exit_code == 1
        channel.basic_publish.assert_not_called()
        assert "SKIPPED" in result.output and queue.unacked == {}

    def test_failures_are_held_until_the_end(self) -> None:
        """A failed publish requeued at once came straight back on the next basic_get."""
        queue = _Queue([b"a", b"b"], headers={"x-rabbitkit-original-routing-key": "rk"})
        mock_pika = _pika(queue)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        channel.basic_publish.side_effect = mock_pika.exceptions.UnroutableError("no route")
        with patch.dict("sys.modules", {"pika": mock_pika, "pika.exceptions": mock_pika.exceptions}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "orders", "--limit", "10"])
        assert result.exit_code == 1
        assert channel.basic_publish.call_count == 2  # each message tried once
        assert queue.events == ["get", "get", "get", "nack", "nack"]


class TestEdgeCases:
    def test_release_stops_at_the_first_failure(self) -> None:
        from rabbitkit.cli.commands.dlq import _release

        channel = MagicMock()
        channel.basic_nack.side_effect = [None, RuntimeError("closed"), None]
        _release(channel, [1, 2, 3])
        assert channel.basic_nack.call_count == 2  # the channel close requeues the rest

    def test_dry_run_with_management_refuses_limited_queue(self) -> None:
        client = MagicMock()
        client.get_queue.return_value = {"type": "quorum", "arguments": {}, "messages": 1}
        client.overview.return_value = {"rabbitmq_version": "4.1.8"}
        queue = _Queue([b"a"])
        with patch("rabbitkit.management.RabbitManagementClient", return_value=client):
            result, _ = _run(queue, ["replay", "q", "orders", "--dry-run", "-m", "http://localhost:15672"])
        assert result.exit_code == 2 and queue.events == []


class TestReviewFindingsCli:
    def test_management_404_keeps_the_tripwire_armed(self) -> None:
        client = MagicMock()
        client.get_queue.side_effect = urllib.error.HTTPError("u", 404, "nf", None, None)  # type: ignore[arg-type]
        queue = _Queue([b"a"], headers={"x-delivery-count": 1})
        with patch("rabbitkit.management.RabbitManagementClient", return_value=client):
            result, _ = _run(queue, ["inspect", "q", "-m", "http://localhost:15672"])
        assert result.exit_code == 2 and "quorum queue" in result.output

    def test_real_replay_stops_at_the_first_requeue_on_a_limited_queue(self) -> None:
        client = MagicMock()
        client.get_queue.return_value = {"type": "quorum", "arguments": {}, "messages": 2}
        client.overview.return_value = {"rabbitmq_version": "4.1.8"}
        queue = _Queue([b"a", b"b"])  # no origin headers: both would be skipped
        with patch("rabbitkit.management.RabbitManagementClient", return_value=client):
            result, _ = _run(queue, ["replay", "orders.dlq", "", "-m", "http://localhost:15672"])
        assert result.exit_code == 2 and "delivery limit of 20" in result.output
        assert queue.events.count("get") == 1 and queue.unacked == {}

    def test_real_replay_tripwire_on_a_counted_failure(self) -> None:
        queue = _Queue([b"a", b"b"], headers={"x-delivery-count": 3})
        result, _ = _run(queue, ["replay", "orders.dlq", ""])  # self-replay -> skipped -> requeue
        assert result.exit_code == 2
        assert queue.events.count("get") == 1 and queue.unacked == {}

    def test_real_replay_vets_once_when_safe(self) -> None:
        client = MagicMock()
        client.get_queue.return_value = {"type": "classic", "arguments": {}, "messages": 2}
        queue = _Queue([b"a", b"b"])
        with patch("rabbitkit.management.RabbitManagementClient", return_value=client):
            result, _ = _run(queue, ["replay", "orders.dlq", "", "-m", "http://localhost:15672"])
        assert result.exit_code == 1  # two skipped, nothing refused
        client.get_queue.assert_called_once()

    def test_no_limit_check_skips_lazy_vetting(self) -> None:
        queue = _Queue([b"a"], headers={"x-delivery-count": 3})
        result, _ = _run(queue, ["replay", "orders.dlq", "", "--no-delivery-limit-check"])
        assert result.exit_code == 1

    def test_real_replay_stops_at_a_counted_publish_failure(self) -> None:
        queue = _Queue([b"a", b"b"], headers={"x-delivery-count": 3, "x-rabbitkit-original-routing-key": "rk"})
        mock_pika = _pika(queue)
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        channel.basic_publish.side_effect = mock_pika.exceptions.UnroutableError("no route")
        with patch.dict("sys.modules", {"pika": mock_pika, "pika.exceptions": mock_pika.exceptions}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "orders"])
        assert result.exit_code == 2
        assert channel.basic_publish.call_count == 1 and queue.unacked == {}


class _QuorumQueue(_Queue):
    """A quorum queue: a requeued message goes to the BACK (measured on 3.13 and 4.1)."""

    def basic_nack(self, delivery_tag: int, requeue: bool = True) -> None:
        self.events.append("nack")
        body = self.unacked.pop(delivery_tag)
        if requeue:
            self.ready.append((delivery_tag, body))


class TestQuorumInspectKeepsOrder:
    def _run_quorum(self, queue: _Queue, ready: int, args: list[str]) -> Any:
        info = {"type": "quorum", "arguments": {"x-queue-type": "quorum", "x-delivery-limit": -1},
                "messages": ready, "messages_ready": ready}
        client = MagicMock()
        client.get_queue.return_value = info
        client.overview.return_value = {"rabbitmq_version": "4.1.8"}
        with patch("rabbitkit.management.RabbitManagementClient", return_value=client):
            result, _ = _run(queue, ["inspect", "orders.dlq", "-m", "http://localhost:15672", *args])
        return result

    def test_a_quorum_queue_is_read_whole_so_its_order_survives(self) -> None:
        queue = _QuorumQueue([b"a", b"b", b"c", b"d"])
        result = self._run_quorum(queue, 4, ["--limit", "2", "--format", "json"])
        assert result.exit_code == 0, result.output
        assert result.output.count("id-") == 2  # shows --limit of them
        assert [body for _, body in queue.ready] == [b"a", b"b", b"c", b"d"]  # not rotated

    def test_a_partial_read_would_have_rotated_it(self) -> None:
        """What 0.19.0 did: --limit 2 moved a and b behind c and d."""
        queue = _QuorumQueue([b"a", b"b", b"c", b"d"])
        result, _ = _run(queue, ["inspect", "orders.dlq", "--limit", "2", "--no-delivery-limit-check"])
        assert result.exit_code == 0
        assert [body for _, body in queue.ready] == [b"c", b"d", b"a", b"b"]

    def test_a_quorum_queue_deeper_than_one_scan_is_refused_before_connecting(self) -> None:
        queue = _QuorumQueue([b"a"])
        result = self._run_quorum(queue, 5001, [])
        assert result.exit_code == 2 and "only whole" in result.output
        assert queue.events == []

    def test_a_quorum_queue_that_grew_while_read_is_refused_and_released(self) -> None:
        queue = _QuorumQueue([b"m"] * 5001)  # the stats said 10
        result = self._run_quorum(queue, 10, [])
        assert result.exit_code == 2 and "holding 5001 messages" in result.output
        assert len(queue.ready) == 5001 and not queue.unacked
