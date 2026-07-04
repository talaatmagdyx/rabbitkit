"""Tests for cli/commands/migrate.py — `rabbitkit topology migrate`."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from rabbitkit.cli import app
from rabbitkit.cli.commands.migrate import _management_client
from rabbitkit.core.types import QueueType

runner = CliRunner()

# Every RabbitManagementClient method the migrate command may call that mutates broker state.
MUTATING = ("declare_queue", "put_parameter", "delete_parameter", "delete_queue", "bind_queue", "purge_queue")

AMQP_DEFAULT = "amqp://guest:guest@localhost/"

DEFAULT_BINDINGS = [
    # Default-exchange binding — implicit, must never be recreated explicitly.
    {"source": "", "destination": "orders.q", "routing_key": "orders.q", "arguments": {}},
    {"source": "orders-ex", "destination": "orders.q", "routing_key": "orders.#", "arguments": {}},
]

ALL_STEPS = [
    "check-consumers",
    "snapshot",
    "create-tmp",
    "shovel-to-tmp",
    "wait-source-empty",
    "delete-source",
    "redeclare-quorum",
    "recreate-bindings",
    "shovel-back",
    "wait-tmp-empty",
    "delete-tmp",
]


def _make_broker(
    queues: tuple[str, ...] = ("orders.q",),
    queue_type: QueueType = QueueType.QUORUM,
) -> MagicMock:
    broker = MagicMock()
    routes = []
    for name in queues:
        route = MagicMock()
        route.queue.name = name
        route.queue.queue_type = queue_type
        routes.append(route)
    broker.routes = routes
    return broker


def _live_queue(name: str = "orders.q", **overrides: Any) -> dict[str, Any]:
    q: dict[str, Any] = {
        "name": name,
        "type": "classic",
        "durable": True,
        "auto_delete": False,
        "arguments": {"x-message-ttl": 60000},
        "consumers": 0,
        "messages": 0,
    }
    q.update(overrides)
    return q


def _make_client(
    live_queues: list[dict[str, Any]] | None = None,
    bindings: list[dict[str, Any]] | None = None,
    queue_info: dict[str, Any] | None = None,
) -> MagicMock:
    client = MagicMock()
    client.list_queues.return_value = live_queues if live_queues is not None else [_live_queue()]
    client.get_queue_bindings.return_value = bindings if bindings is not None else list(DEFAULT_BINDINGS)
    client.get_queue.return_value = queue_info or {
        "name": "orders.q",
        "durable": True,
        "arguments": {"x-message-ttl": 60000},
        "consumers": 0,
        "messages": 0,
    }
    client.list_shovel_statuses.return_value = []
    return client


def _invoke(client: MagicMock, *args: str, broker: MagicMock | None = None) -> Any:
    with patch("rabbitkit.cli.commands.migrate.load_broker", return_value=broker or _make_broker()):
        with patch("rabbitkit.cli.commands.migrate._management_client", return_value=client):
            return runner.invoke(app, ["topology", "migrate", "myapp:broker", *args])


def _assert_no_mutations(client: MagicMock) -> None:
    for name in MUTATING:
        assert getattr(client, name).call_count == 0, f"unexpected mutating call: {name}"


def _mutating_sequence(client: MagicMock) -> list[str]:
    return [c[0] for c in client.method_calls if c[0] in MUTATING]


class TestMigratePlan:
    def test_plan_prints_ordered_runbook_and_writes_snapshot(self, tmp_path: Any) -> None:
        """Default mode: numbered runbook in order, snapshot written, zero mutations."""
        snap = tmp_path / "snap.json"
        client = _make_client()

        result = _invoke(client, "--snapshot-file", str(snap))

        assert result.exit_code == 0
        # Runbook steps appear in the mandated order.
        ordered_phrases = [
            "Verify zero consumers",
            "Snapshot bindings",
            "Create temporary quorum queue",
            "rabbitkit-migrate-orders.q-out",
            "Wait until 'orders.q' is empty",
            "Delete classic queue",
            "Redeclare 'orders.q' with x-queue-type=quorum",
            "Recreate 1 binding(s)",
            "rabbitkit-migrate-orders.q-back",
            "Wait until 'orders.q.migrate-tmp' is empty",
            "Delete temporary queue",
        ]
        positions = [result.output.index(p) for p in ordered_phrases]
        assert positions == sorted(positions)
        for i in range(1, 12):
            assert f"{i:2d}. " in result.output

        # Snapshot file is the rollback artifact: bindings + queue args.
        snapshot = json.loads(snap.read_text())
        assert snapshot["vhost"] == "/"
        assert snapshot["queues"]["orders.q"]["bindings"] == DEFAULT_BINDINGS
        assert snapshot["queues"]["orders.q"]["queue"]["arguments"] == {"x-message-ttl": 60000}
        assert snapshot["queues"]["orders.q"]["queue"]["durable"] is True

        # Plan NEVER mutates.
        _assert_no_mutations(client)

    def test_plan_no_candidates_when_declared_classic(self, tmp_path: Any) -> None:
        """Queues declared classic are not migration candidates."""
        client = _make_client()
        broker = _make_broker(queue_type=QueueType.CLASSIC)

        result = _invoke(client, "--snapshot-file", str(tmp_path / "s.json"), broker=broker)

        assert result.exit_code == 0
        assert "No queues need" in result.output
        _assert_no_mutations(client)

    def test_plan_skips_queue_already_quorum_live(self, tmp_path: Any) -> None:
        """A live queue that is already quorum needs no migration."""
        client = _make_client(live_queues=[_live_queue(type="quorum")])

        result = _invoke(client, "--snapshot-file", str(tmp_path / "s.json"))

        assert result.exit_code == 0
        assert "No queues need" in result.output

    def test_plan_detects_classic_via_arguments_and_default(self, tmp_path: Any) -> None:
        """Live type falls back to arguments x-queue-type, then to classic."""
        snap = tmp_path / "s.json"
        live = [
            _live_queue("orders.q", type=None, arguments={"x-queue-type": "classic"}),
            {"name": "billing.q", "durable": True, "consumers": 0, "messages": 0},
        ]
        client = _make_client(live_queues=live)
        broker = _make_broker(queues=("orders.q", "billing.q"))

        result = _invoke(client, "--snapshot-file", str(snap), broker=broker)

        assert result.exit_code == 0
        snapshot = json.loads(snap.read_text())
        assert sorted(snapshot["queues"]) == ["billing.q", "orders.q"]

    def test_plan_queue_option_limits_to_one_queue(self, tmp_path: Any) -> None:
        snap = tmp_path / "s.json"
        live = [_live_queue("orders.q"), _live_queue("billing.q")]
        client = _make_client(live_queues=live)
        broker = _make_broker(queues=("orders.q", "billing.q"))

        result = _invoke(client, "--queue", "billing.q", "--snapshot-file", str(snap), broker=broker)

        assert result.exit_code == 0
        snapshot = json.loads(snap.read_text())
        assert list(snapshot["queues"]) == ["billing.q"]

    def test_explicit_plan_flag(self, tmp_path: Any) -> None:
        client = _make_client()

        result = _invoke(client, "--plan", "--snapshot-file", str(tmp_path / "s.json"))

        assert result.exit_code == 0
        assert "Migration plan" in result.output
        _assert_no_mutations(client)


class TestMigrateValidation:
    def test_rejects_non_http_scheme(self) -> None:
        """--url feeds urllib — only http/https allowed (like topology validate)."""
        with patch("rabbitkit.cli.commands.migrate.load_broker", return_value=_make_broker()):
            result = runner.invoke(
                app, ["topology", "migrate", "myapp:broker", "--url", "ftp://host:15672"]
            )
        assert result.exit_code == 1
        assert "scheme" in result.output

    def test_plan_and_execute_are_mutually_exclusive(self) -> None:
        result = _invoke(_make_client(), "--plan", "--execute")
        assert result.exit_code == 1
        assert "mutually exclusive" in result.output

    def test_dry_run_requires_execute(self) -> None:
        result = _invoke(_make_client(), "--dry-run")
        assert result.exit_code == 1
        assert "--dry-run" in result.output

    def test_unknown_strategy(self) -> None:
        result = _invoke(_make_client(), "--execute", "--strategy", "yolo")
        assert result.exit_code == 1
        assert "unknown strategy" in result.output

    def test_management_api_unreachable(self) -> None:
        client = _make_client()
        client.list_queues.side_effect = ConnectionError("refused")
        result = _invoke(client)
        assert result.exit_code == 1
        assert "could not reach management API" in result.output


class TestManagementClientFactory:
    def test_parses_credentials_out_of_url(self) -> None:
        client = _management_client("http://admin:secret@rmq.example:15672")
        assert client._config.url == "http://rmq.example:15672"
        assert client._config.username == "admin"
        assert client._config.password == "secret"

    def test_defaults_to_guest_and_strips_trailing_slash(self) -> None:
        client = _management_client("http://localhost:15672/")
        assert client._config.url == "http://localhost:15672"
        assert client._config.username == "guest"
        assert client._config.password == "guest"

    def test_https_without_port(self) -> None:
        client = _management_client("https://admin:pw@rmq.example.com")
        assert client._config.url == "https://rmq.example.com"


class TestMigrateDryRun:
    def test_dry_run_issues_zero_mutating_calls(self, tmp_path: Any) -> None:
        state = tmp_path / "state.json"
        client = _make_client()

        result = _invoke(
            client, "--execute", "--strategy", "drain-cutover", "--dry-run",
            "--state-file", str(state),
        )

        assert result.exit_code == 0
        assert "[dry-run]" in result.output
        # Every planned call is printed...
        assert "GET /api/shovels" in result.output
        assert "PUT /api/queues/%2F/orders.q.migrate-tmp" in result.output
        assert "PUT /api/parameters/shovel/%2F/rabbitkit-migrate-orders.q-out" in result.output
        assert "DELETE /api/queues/%2F/orders.q" in result.output
        assert "POST /api/bindings/%2F/e/orders-ex/q/orders.q" in result.output
        # ...but none issued, and no state file written.
        _assert_no_mutations(client)
        client.list_shovel_statuses.assert_not_called()
        assert not state.exists()


class TestMigrateDrainCutover:
    def test_happy_path_issues_expected_call_sequence(self, tmp_path: Any) -> None:
        state = tmp_path / "state.json"
        client = _make_client()

        result = _invoke(
            client, "--execute", "--strategy", "drain-cutover", "--state-file", str(state)
        )

        assert result.exit_code == 0, result.output
        client.list_shovel_statuses.assert_called_once()
        assert _mutating_sequence(client) == [
            "declare_queue",   # tmp quorum queue
            "put_parameter",   # shovel old -> tmp
            "delete_queue",    # old classic queue
            "declare_queue",   # redeclare as quorum
            "bind_queue",      # recreate bindings
            "put_parameter",   # shovel tmp -> old name
            "delete_queue",    # tmp queue
        ]

        # tmp queue is quorum.
        first_declare = client.declare_queue.call_args_list[0]
        assert first_declare.args == ("orders.q.migrate-tmp",)
        assert first_declare.kwargs["arguments"] == {"x-queue-type": "quorum"}

        # Redeclare preserves original arguments and adds x-queue-type=quorum.
        second_declare = client.declare_queue.call_args_list[1]
        assert second_declare.args == ("orders.q",)
        assert second_declare.kwargs["arguments"] == {
            "x-message-ttl": 60000,
            "x-queue-type": "quorum",
        }

        # Shovel bodies match the dynamic-shovel spec exactly.
        client.put_parameter.assert_any_call(
            "shovel",
            "/",
            "rabbitkit-migrate-orders.q-out",
            {
                "value": {
                    "src-protocol": "amqp091",
                    "src-uri": AMQP_DEFAULT,
                    "src-queue": "orders.q",
                    "dest-protocol": "amqp091",
                    "dest-uri": AMQP_DEFAULT,
                    "dest-queue": "orders.q.migrate-tmp",
                    "src-delete-after": "queue-length",
                }
            },
        )
        client.put_parameter.assert_any_call(
            "shovel",
            "/",
            "rabbitkit-migrate-orders.q-back",
            {
                "value": {
                    "src-protocol": "amqp091",
                    "src-uri": AMQP_DEFAULT,
                    "src-queue": "orders.q.migrate-tmp",
                    "dest-protocol": "amqp091",
                    "dest-uri": AMQP_DEFAULT,
                    "dest-queue": "orders.q",
                    "src-delete-after": "queue-length",
                }
            },
        )

        # Default-exchange binding skipped; explicit binding recreated.
        client.bind_queue.assert_called_once_with("orders.q", "orders-ex", "orders.#", "/", None)

        # Both deletes hit the right queues.
        assert client.delete_queue.call_args_list[0].args == ("orders.q", "/")
        assert client.delete_queue.call_args_list[1].args == ("orders.q.migrate-tmp", "/")

        # Progress persisted after each step.
        saved = json.loads(state.read_text())
        assert saved["queues"]["orders.q"]["completed"] == ALL_STEPS
        assert saved["queues"]["orders.q"]["snapshot"]["bindings"] == DEFAULT_BINDINGS

    def test_refuses_queue_with_consumers(self, tmp_path: Any) -> None:
        client = _make_client(
            queue_info={"name": "orders.q", "consumers": 2, "messages": 0, "durable": True, "arguments": {}}
        )

        result = _invoke(client, "--execute", "--state-file", str(tmp_path / "s.json"))

        assert result.exit_code == 1
        assert "2 consumer(s)" in result.output
        assert "--force" in result.output
        _assert_no_mutations(client)

    def test_force_proceeds_despite_consumers(self, tmp_path: Any) -> None:
        client = _make_client(
            queue_info={"name": "orders.q", "consumers": 2, "messages": 0, "durable": True, "arguments": {}}
        )

        result = _invoke(client, "--execute", "--force", "--state-file", str(tmp_path / "s.json"))

        assert result.exit_code == 0, result.output
        assert client.delete_queue.call_count == 2

    def test_resume_skips_completed_steps(self, tmp_path: Any) -> None:
        state = tmp_path / "state.json"
        state.write_text(
            json.dumps(
                {
                    "queues": {
                        "orders.q": {
                            "completed": [
                                "check-consumers",
                                "snapshot",
                                "create-tmp",
                                "shovel-to-tmp",
                                "wait-source-empty",
                            ],
                            "snapshot": {
                                "queue": {"durable": True, "arguments": {}},
                                "bindings": [
                                    {"source": "ex1", "routing_key": "rk", "arguments": {}}
                                ],
                            },
                        }
                    }
                }
            )
        )
        client = _make_client()

        result = _invoke(client, "--execute", "--resume", "--state-file", str(state))

        assert result.exit_code == 0, result.output
        # Snapshot came from the state file — no re-read.
        client.get_queue_bindings.assert_not_called()
        # Completed steps are skipped: no tmp declare, no out-shovel.
        assert _mutating_sequence(client) == [
            "delete_queue",    # delete-source
            "declare_queue",   # redeclare-quorum
            "bind_queue",      # recreate-bindings (from state snapshot)
            "put_parameter",   # shovel-back only
            "delete_queue",    # delete-tmp
        ]
        client.put_parameter.assert_called_once()
        assert client.put_parameter.call_args.args[2] == "rabbitkit-migrate-orders.q-back"
        client.bind_queue.assert_called_once_with("orders.q", "ex1", "rk", "/", None)
        # State file updated with the remaining steps.
        saved = json.loads(state.read_text())
        assert saved["queues"]["orders.q"]["completed"] == ALL_STEPS

    def test_resume_with_missing_state_file_starts_fresh(self, tmp_path: Any) -> None:
        client = _make_client()

        result = _invoke(
            client, "--execute", "--resume", "--state-file", str(tmp_path / "missing.json")
        )

        assert result.exit_code == 0, result.output
        assert client.delete_queue.call_count == 2

    def test_shovel_plugin_missing(self, tmp_path: Any) -> None:
        client = _make_client()
        client.list_shovel_statuses.side_effect = OSError("HTTP Error 404: Not Found")

        result = _invoke(client, "--execute", "--state-file", str(tmp_path / "s.json"))

        assert result.exit_code == 1
        assert "rabbitmq-plugins enable rabbitmq_shovel" in result.output
        _assert_no_mutations(client)

    def test_drain_wait_times_out(self, tmp_path: Any) -> None:
        client = _make_client(
            queue_info={"name": "orders.q", "consumers": 0, "messages": 5, "durable": True, "arguments": {}}
        )

        result = _invoke(
            client, "--execute", "--timeout", "0", "--state-file", str(tmp_path / "s.json")
        )

        assert result.exit_code == 1
        assert "timed out" in result.output
        # Nothing destructive happened.
        client.delete_queue.assert_not_called()

    def test_drain_polls_until_empty(self, tmp_path: Any) -> None:
        """The wait loop sleeps between polls and proceeds once the queue drains."""
        client = _make_client()
        client.get_queue.side_effect = [
            {"consumers": 0, "messages": 0, "durable": True, "arguments": {}},  # check-consumers
            {"consumers": 0, "messages": 0, "durable": True, "arguments": {}},  # snapshot
            {"messages": 2},  # wait-source-empty poll 1 — still draining
            {"messages": 0},  # wait-source-empty poll 2 — drained
            {"messages": 0},  # verify before delete-source
            {"messages": 0},  # wait-tmp-empty
            {"messages": 0},  # verify before delete-tmp
        ]

        with patch("time.sleep") as mock_sleep:
            result = _invoke(client, "--execute", "--state-file", str(tmp_path / "s.json"))

        assert result.exit_code == 0, result.output
        mock_sleep.assert_called_once()
        assert client.delete_queue.call_count == 2

    def test_refuses_delete_when_messages_reappear(self, tmp_path: Any) -> None:
        """Rail: message count re-verified immediately before the destructive delete."""
        client = _make_client()
        client.get_queue.side_effect = [
            {"consumers": 0, "messages": 0, "durable": True, "arguments": {}},  # check-consumers
            {"consumers": 0, "messages": 0, "durable": True, "arguments": {}},  # snapshot
            {"messages": 0},  # wait-source-empty poll
            {"messages": 3},  # verify before delete-source — refuses
        ]

        result = _invoke(client, "--execute", "--state-file", str(tmp_path / "s.json"))

        assert result.exit_code == 1
        assert "refusing to delete" in result.output
        client.delete_queue.assert_not_called()


class TestMigrateBridge:
    def test_bridge_creates_queue_and_bindings_deletes_nothing(self) -> None:
        client = _make_client()

        result = _invoke(client, "--execute", "--strategy", "bridge")

        assert result.exit_code == 0, result.output
        client.declare_queue.assert_called_once()
        assert client.declare_queue.call_args.args == ("orders.q.q2",)
        assert client.declare_queue.call_args.kwargs["arguments"] == {"x-queue-type": "quorum"}
        client.bind_queue.assert_called_once_with("orders.q.q2", "orders-ex", "orders.#", "/", None)
        # Bridge never deletes or shovels.
        client.delete_queue.assert_not_called()
        client.delete_parameter.assert_not_called()
        client.put_parameter.assert_not_called()
        assert "Point consumers" in result.output
        assert "Nothing was deleted" in result.output

    def test_bridge_dry_run_issues_zero_mutating_calls(self) -> None:
        client = _make_client()

        result = _invoke(client, "--execute", "--strategy", "bridge", "--dry-run")

        assert result.exit_code == 0
        assert "[dry-run]" in result.output
        _assert_no_mutations(client)
