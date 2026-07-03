"""Tests for CLI tooling (F8+F9)."""

from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from rabbitkit.cli import app
from rabbitkit.cli._utils import load_broker, parse_app_path
from rabbitkit.health import BrokerHealthResult, HealthStatus

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_route(
    name: str = "test-route",
    queue_name: str = "orders",
    exchange_name: str = "amqp.topic",
    routing_key: str = "orders.*",
    ack_policy_value: str = "auto",
    tags: set | None = None,
    description: str = "Test route",
) -> MagicMock:
    route = MagicMock()
    route.name = name
    route.queue.name = queue_name
    route.queue.routing_key = routing_key
    route.exchange.name = exchange_name
    route.ack_policy.value = ack_policy_value
    route.tags = tags if tags is not None else set()
    route.description = description
    return route


# ---------------------------------------------------------------------------
# TestParseAppPath
# ---------------------------------------------------------------------------


class TestParseAppPath:
    def test_valid_path(self) -> None:
        mod, attr = parse_app_path("myapp.main:broker")
        assert mod == "myapp.main"
        assert attr == "broker"

    def test_invalid_path_no_colon(self) -> None:
        with pytest.raises(ValueError, match="Invalid app path"):
            parse_app_path("myapp.main.broker")

    def test_nested_path(self) -> None:
        mod, attr = parse_app_path("a.b.c.d:my_broker")
        assert mod == "a.b.c.d"
        assert attr == "my_broker"


# ---------------------------------------------------------------------------
# TestLoadBroker (_utils.py line 38)
# ---------------------------------------------------------------------------


class TestLoadBroker:
    def test_load_broker_success(self) -> None:
        """Line 38: getattr(module, attr) is returned for a real module."""
        fake_module = types.ModuleType("_fake_broker_module_xyz")
        fake_broker = MagicMock()
        fake_module.broker = fake_broker  # type: ignore[attr-defined]
        sys.modules["_fake_broker_module_xyz"] = fake_module
        try:
            result = load_broker("_fake_broker_module_xyz:broker")
            assert result is fake_broker
        finally:
            del sys.modules["_fake_broker_module_xyz"]

    def test_load_broker_missing_attr(self) -> None:
        fake_module = types.ModuleType("_fake_broker_module_noattr")
        sys.modules["_fake_broker_module_noattr"] = fake_module
        try:
            with pytest.raises(AttributeError):
                load_broker("_fake_broker_module_noattr:nonexistent")
        finally:
            del sys.modules["_fake_broker_module_noattr"]

    def test_load_broker_bad_path(self) -> None:
        with pytest.raises(ValueError, match="Invalid app path"):
            load_broker("no_colon_here")


# ---------------------------------------------------------------------------
# TestCliNoArgs
# ---------------------------------------------------------------------------


class TestCliNoArgs:
    def test_no_args_shows_help(self) -> None:
        result = runner.invoke(app, [])
        # no_args_is_help=True causes exit code 0 on some typer versions, 2 on others
        assert result.exit_code in (0, 2)
        assert "Usage" in result.output or "rabbitkit" in result.output

    def test_help_flag(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "health" in result.output
        assert "topology" in result.output


# ---------------------------------------------------------------------------
# TestRunCommand
# ---------------------------------------------------------------------------


class TestRunCommand:
    def test_run_invalid_module(self) -> None:
        result = runner.invoke(app, ["run", "nonexistent.module:broker"])
        assert result.exit_code != 0

    def test_run_no_colon(self) -> None:
        result = runner.invoke(app, ["run", "nocolon"])
        assert result.exit_code != 0

    # ------------------------------------------------------------------
    # _run_single — sync broker (has .run method)  lines 76-79
    # ------------------------------------------------------------------

    def test_run_single_sync_broker(self) -> None:
        """_run_single calls broker.run() when broker has a callable run attr."""
        mock_broker = MagicMock()
        mock_broker.run = MagicMock()  # sync broker — has .run
        with patch("rabbitkit.cli.commands.run.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["run", "myapp.main:broker"])
        assert result.exit_code == 0
        mock_broker.run.assert_called_once()
        assert "Starting sync broker" in result.output

    # ------------------------------------------------------------------
    # _run_single — async broker (no .run)  lines 81-95
    # ------------------------------------------------------------------

    def test_run_single_async_broker(self) -> None:
        """_run_single uses asyncio.run(broker.start/stop) when no .run attr."""
        async_broker = MagicMock(spec=[])  # spec=[] means no attributes at all
        async_broker.start = AsyncMock()
        async_broker.stop = AsyncMock()

        # Use a sync no-op so we don't actually run the coroutine (and avoid
        # the "coroutine was never awaited" RuntimeWarning that comes from
        # passing an async side_effect to a patched synchronous call).
        def _noop_run(coro: object) -> None:
            # Close the coroutine to suppress ResourceWarning
            try:
                coro.close()  # type: ignore[union-attr]
            except Exception:
                pass

        with patch("rabbitkit.cli.commands.run.load_broker", return_value=async_broker):
            with patch("rabbitkit.cli.commands.run.asyncio.run", side_effect=_noop_run):
                result = runner.invoke(app, ["run", "myapp.main:broker"])
        assert result.exit_code == 0
        assert "Starting async broker" in result.output

    # ------------------------------------------------------------------
    # _run_with_reload — watchfiles NOT installed  lines 100-107
    # ------------------------------------------------------------------

    def test_run_with_reload_no_watchfiles(self) -> None:
        """--reload exits with code 1 when watchfiles is not installed."""
        mock_broker = MagicMock()
        mock_broker.run = MagicMock()
        with patch("rabbitkit.cli.commands.run.load_broker", return_value=mock_broker):
            with patch.dict("sys.modules", {"watchfiles": None}):
                result = runner.invoke(app, ["run", "myapp.main:broker", "--reload"])
        assert result.exit_code == 1
        assert "watchfiles" in result.output.lower() or "reload" in result.output.lower()

    # ------------------------------------------------------------------
    # _run_with_reload — watchfiles IS installed  lines 109-123
    # ------------------------------------------------------------------

    def test_run_with_reload_with_watchfiles(self) -> None:
        """--reload calls watchfiles.run_process when watchfiles is available."""
        mock_broker = MagicMock()
        mock_broker.run = MagicMock()

        mock_watchfiles = MagicMock()
        mock_watchfiles.PythonFilter = MagicMock(return_value=MagicMock())
        mock_watchfiles.run_process = MagicMock()

        with patch("rabbitkit.cli.commands.run.load_broker", return_value=mock_broker):
            with patch.dict("sys.modules", {"watchfiles": mock_watchfiles}):
                result = runner.invoke(app, ["run", "myapp.main:broker", "--reload"])

        # run_process is called once
        mock_watchfiles.run_process.assert_called_once()
        assert result.exit_code == 0

    def test_run_with_reload_extra_extensions(self) -> None:
        """--reload-ext adds extra extensions to the watch filter."""
        mock_broker = MagicMock()
        mock_broker.run = MagicMock()

        mock_watchfiles = MagicMock()
        mock_watchfiles.PythonFilter = MagicMock(return_value=MagicMock())
        mock_watchfiles.run_process = MagicMock()

        with patch("rabbitkit.cli.commands.run.load_broker", return_value=mock_broker):
            with patch.dict("sys.modules", {"watchfiles": mock_watchfiles}):
                result = runner.invoke(
                    app,
                    ["run", "myapp.main:broker", "--reload", "--reload-ext", ".yml,.toml"],
                )

        mock_watchfiles.run_process.assert_called_once()
        assert result.exit_code == 0

    # ------------------------------------------------------------------
    # _run_multiprocess  lines 128-144
    # ------------------------------------------------------------------

    def test_run_multiprocess_starts_n_processes(self) -> None:
        """--workers N spawns N processes and joins them."""
        mock_proc = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.start = MagicMock()

        with patch("rabbitkit.cli.commands.run.load_broker"):
            with patch("rabbitkit.cli.commands.run.multiprocessing.Process", return_value=mock_proc) as mock_cls:
                result = runner.invoke(app, ["run", "myapp.main:broker", "--workers", "3"])

        assert result.exit_code == 0
        assert mock_cls.call_count == 3
        assert mock_proc.start.call_count == 3
        assert mock_proc.join.call_count == 3
        assert "3 worker processes" in result.output

    def test_run_multiprocess_keyboard_interrupt(self) -> None:
        """KeyboardInterrupt during join triggers terminate on all workers."""
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock(side_effect=[KeyboardInterrupt, None, None])
        mock_proc.terminate = MagicMock()

        with patch("rabbitkit.cli.commands.run.load_broker"):
            with patch("rabbitkit.cli.commands.run.multiprocessing.Process", return_value=mock_proc):
                result = runner.invoke(app, ["run", "myapp.main:broker", "--workers", "2"])

        # terminate should have been called on each worker
        assert mock_proc.terminate.call_count >= 1
        assert "Shutting down workers" in result.output

    # ------------------------------------------------------------------
    # _run_single — async broker inner coroutine body  lines 85-93
    # ------------------------------------------------------------------

    def test_run_single_async_broker_inner_coroutine(self) -> None:
        """Lines 85-93: _run_async coroutine calls start/stop on async broker."""
        import asyncio

        async_broker = MagicMock(spec=[])
        async_broker.start = AsyncMock()
        async_broker.stop = AsyncMock()

        # Let asyncio.run actually execute the coroutine, but make sleep raise
        # CancelledError immediately so the infinite loop exits.
        original_run = asyncio.run

        async def _fake_sleep(delay: float) -> None:
            raise asyncio.CancelledError()

        def _run_with_real_coro(coro: object) -> None:
            # Run the real coroutine but patch asyncio.sleep inside it
            async def _runner() -> None:
                with patch("rabbitkit.cli.commands.run.asyncio.sleep", side_effect=_fake_sleep):
                    await coro  # type: ignore[misc]
            original_run(_runner())

        with patch("rabbitkit.cli.commands.run.load_broker", return_value=async_broker):
            with patch("rabbitkit.cli.commands.run.asyncio.run", side_effect=_run_with_real_coro):
                result = runner.invoke(app, ["run", "myapp.main:broker"])

        assert result.exit_code == 0
        async_broker.start.assert_called_once()
        async_broker.stop.assert_called_once()

    def test_run_single_async_broker_keyboard_interrupt(self) -> None:
        """Lines 90-91: KeyboardInterrupt during sleep is caught gracefully."""
        import asyncio

        async_broker = MagicMock(spec=[])
        async_broker.start = AsyncMock()
        async_broker.stop = AsyncMock()

        original_run = asyncio.run

        async def _fake_sleep_keyboard(delay: float) -> None:
            raise KeyboardInterrupt()

        def _run_with_keyboard(coro: object) -> None:
            async def _runner() -> None:
                with patch("rabbitkit.cli.commands.run.asyncio.sleep", side_effect=_fake_sleep_keyboard):
                    await coro  # type: ignore[misc]
            original_run(_runner())

        with patch("rabbitkit.cli.commands.run.load_broker", return_value=async_broker):
            with patch("rabbitkit.cli.commands.run.asyncio.run", side_effect=_run_with_keyboard):
                result = runner.invoke(app, ["run", "myapp.main:broker"])

        assert result.exit_code == 0
        async_broker.start.assert_called_once()
        async_broker.stop.assert_called_once()

    # ------------------------------------------------------------------
    # _run_with_reload — ext without dot prefix  line 114
    # ------------------------------------------------------------------

    def test_run_with_reload_ext_without_dot_prefix(self) -> None:
        """Line 114: extension without leading dot gets a dot prepended."""
        mock_broker = MagicMock()
        mock_broker.run = MagicMock()

        captured_extensions: list[set] = []
        mock_watchfiles = MagicMock()
        mock_watchfiles.run_process = MagicMock()

        def _capture_filter(extra_extensions: set) -> MagicMock:  # type: ignore[return-value]
            captured_extensions.append(set(extra_extensions))
            return MagicMock()

        mock_watchfiles.PythonFilter = _capture_filter

        with patch("rabbitkit.cli.commands.run.load_broker", return_value=mock_broker):
            with patch.dict("sys.modules", {"watchfiles": mock_watchfiles}):
                result = runner.invoke(
                    app,
                    ["run", "myapp.main:broker", "--reload", "--reload-ext", "yml,toml"],
                )

        assert result.exit_code == 0
        mock_watchfiles.run_process.assert_called_once()
        # The captured extra_extensions should have dotted versions
        assert len(captured_extensions) == 1
        assert ".yml" in captured_extensions[0]
        assert ".toml" in captured_extensions[0]


# ---------------------------------------------------------------------------
# TestShellCommand
# ---------------------------------------------------------------------------


class TestShellCommand:
    def test_shell_in_help(self) -> None:
        result = runner.invoke(app, ["shell", "--help"])
        assert result.exit_code == 0
        assert "interactive" in result.output.lower() or "shell" in result.output.lower()

    # ------------------------------------------------------------------
    # shell_command — IPython available  lines 65-68
    # ------------------------------------------------------------------

    def test_shell_with_ipython(self) -> None:
        """Uses IPython.embed when IPython is importable."""
        mock_broker = MagicMock()
        mock_broker.routes = []
        mock_broker.config = MagicMock()
        mock_broker.publish = MagicMock()

        mock_embed = MagicMock()
        mock_ipython = MagicMock()
        mock_ipython.embed = mock_embed

        with patch("rabbitkit.cli.commands.shell.load_broker", return_value=mock_broker):
            with patch.dict("sys.modules", {"IPython": mock_ipython}):
                result = runner.invoke(app, ["shell", "myapp.main:broker"])

        assert result.exit_code == 0
        mock_embed.assert_called_once()

    # ------------------------------------------------------------------
    # shell_command — IPython NOT available  lines 69-72
    # ------------------------------------------------------------------

    def test_shell_without_ipython(self) -> None:
        """Falls back to code.interact when IPython is not importable."""
        mock_broker = MagicMock()
        mock_broker.routes = []
        mock_broker.config = MagicMock()
        mock_broker.publish = MagicMock()

        with patch("rabbitkit.cli.commands.shell.load_broker", return_value=mock_broker):
            with patch.dict("sys.modules", {"IPython": None}):
                with patch("code.interact") as mock_interact:
                    result = runner.invoke(app, ["shell", "myapp.main:broker"])

        assert result.exit_code == 0
        mock_interact.assert_called_once()

    def test_shell_banner_contains_route_count(self) -> None:
        """Banner reports correct number of loaded routes."""
        mock_broker = MagicMock()
        mock_broker.routes = [_make_route(), _make_route(name="route-2", queue_name="events")]
        mock_broker.config = MagicMock()
        mock_broker.publish = MagicMock()

        captured_banner: list[str] = []

        def _fake_interact(**kwargs: object) -> None:
            captured_banner.append(str(kwargs.get("banner", "")))

        with patch("rabbitkit.cli.commands.shell.load_broker", return_value=mock_broker):
            with patch.dict("sys.modules", {"IPython": None}):
                with patch("code.interact", side_effect=_fake_interact):
                    runner.invoke(app, ["shell", "myapp.main:broker"])

        assert captured_banner
        assert "2 routes" in captured_banner[0]


# ---------------------------------------------------------------------------
# TestTopologyCommand
# ---------------------------------------------------------------------------


class TestTopologyCommand:
    def test_topology_list_json_format(self) -> None:
        """--format json outputs valid JSON with route data."""
        import json as json_mod

        route = _make_route()
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["topology", "list", "myapp.main:broker", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["name"] == "test-route"
        assert data[0]["queue"] == "orders"
        assert data[0]["exchange"] == "amqp.topic"
        assert data[0]["routing_key"] == "orders.*"

    def test_topology_list_table_format_with_routes(self) -> None:
        """Default table format prints headers and one row per route."""
        route = _make_route()
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["topology", "list", "myapp.main:broker"])

        assert result.exit_code == 0
        assert "name" in result.output
        assert "queue" in result.output
        assert "test-route" in result.output
        assert "orders" in result.output

    def test_topology_list_table_format_empty_routes(self) -> None:
        """Table format with no routes prints 'No routes registered.'"""
        mock_broker = MagicMock()
        mock_broker.routes = []

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["topology", "list", "myapp.main:broker"])

        assert result.exit_code == 0
        assert "No routes registered" in result.output

    def test_topology_list_json_empty_routes(self) -> None:
        """JSON format with no routes outputs an empty list."""
        import json as json_mod

        mock_broker = MagicMock()
        mock_broker.routes = []

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["topology", "list", "myapp.main:broker", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data == []

    def test_topology_list_json_multiple_routes(self) -> None:
        """JSON format lists all routes."""
        import json as json_mod

        mock_broker = MagicMock()
        mock_broker.routes = [
            _make_route(name="r1", queue_name="q1", tags={"tag1"}),
            _make_route(name="r2", queue_name="q2", tags=set()),
        ]

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["topology", "list", "myapp.main:broker", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert len(data) == 2
        names = {row["name"] for row in data}
        assert names == {"r1", "r2"}


# ---------------------------------------------------------------------------
# TestHealthCommand
# ---------------------------------------------------------------------------


class TestHealthCommand:
    def _make_healthy_result(self) -> BrokerHealthResult:
        return BrokerHealthResult(
            status=HealthStatus.HEALTHY,
            started=True,
            connected=True,
            consumer_count=2,
            route_count=2,
        )

    def _make_unhealthy_result(self) -> BrokerHealthResult:
        return BrokerHealthResult(
            status=HealthStatus.UNHEALTHY,
            started=False,
            connected=False,
            consumer_count=0,
            route_count=0,
        )

    # ------------------------------------------------------------------
    # health check — healthy broker  lines 19-31
    # ------------------------------------------------------------------

    def test_health_check_healthy(self) -> None:
        """Healthy broker outputs JSON and exits with code 0."""
        import json as json_mod

        mock_broker = MagicMock()

        with patch("rabbitkit.cli.commands.health.load_broker", return_value=mock_broker):
            with patch(
                "rabbitkit.health.broker_health_check",
                return_value=self._make_healthy_result(),
            ):
                result = runner.invoke(app, ["health", "check", "myapp.main:broker"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data["status"] == "healthy"
        assert data["started"] is True
        assert data["connected"] is True

    # ------------------------------------------------------------------
    # health check — unhealthy broker  lines 33-34
    # ------------------------------------------------------------------

    def test_health_check_unhealthy(self) -> None:
        """Unhealthy broker exits with code 1."""
        import json as json_mod

        mock_broker = MagicMock()

        with patch("rabbitkit.cli.commands.health.load_broker", return_value=mock_broker):
            with patch(
                "rabbitkit.health.broker_health_check",
                return_value=self._make_unhealthy_result(),
            ):
                result = runner.invoke(app, ["health", "check", "myapp.main:broker"])

        assert result.exit_code == 1
        data = json_mod.loads(result.output)
        assert data["status"] == "unhealthy"

    def test_health_check_degraded(self) -> None:
        """Degraded broker (non-healthy) also exits with code 1."""
        import json as json_mod

        degraded_result = BrokerHealthResult(
            status=HealthStatus.DEGRADED,
            started=True,
            connected=True,
            consumer_count=1,
            route_count=3,
        )
        mock_broker = MagicMock()

        with patch("rabbitkit.cli.commands.health.load_broker", return_value=mock_broker):
            with patch(
                "rabbitkit.health.broker_health_check",
                return_value=degraded_result,
            ):
                result = runner.invoke(app, ["health", "check", "myapp.main:broker"])

        assert result.exit_code == 1
        data = json_mod.loads(result.output)
        assert data["status"] == "degraded"

    def test_health_check_output_fields(self) -> None:
        """Output contains all expected fields."""
        import json as json_mod

        mock_broker = MagicMock()

        with patch("rabbitkit.cli.commands.health.load_broker", return_value=mock_broker):
            with patch(
                "rabbitkit.health.broker_health_check",
                return_value=self._make_healthy_result(),
            ):
                result = runner.invoke(app, ["health", "check", "myapp.main:broker"])

        data = json_mod.loads(result.output)
        for field in ("status", "started", "connected", "consumer_count", "route_count"):
            assert field in data, f"Missing field: {field}"


# ---------------------------------------------------------------------------
# TestDlqCommand
# ---------------------------------------------------------------------------


def _make_pika_method(
    routing_key: str = "orders.created",
    exchange: str = "amqp.topic",
    redelivered: bool = False,
    delivery_tag: int = 1,
) -> MagicMock:
    method = MagicMock()
    method.routing_key = routing_key
    method.exchange = exchange
    method.redelivered = redelivered
    method.delivery_tag = delivery_tag
    return method


def _make_pika_properties(
    message_id: str = "msg-1",
    correlation_id: str = "corr-1",
    headers: dict | None = None,
) -> MagicMock:
    props = MagicMock()
    props.message_id = message_id
    props.correlation_id = correlation_id
    props.headers = headers or {}
    return props


def _make_mock_pika(messages: list | None = None) -> MagicMock:
    """Build a mock pika module with a BlockingConnection that returns messages."""
    mock_pika = MagicMock()

    channel = MagicMock()
    connection = MagicMock()
    connection.channel.return_value = channel
    mock_pika.BlockingConnection.return_value = connection
    mock_pika.URLParameters.return_value = MagicMock()

    if messages is None:
        # No messages — basic_get returns (None, None, None) immediately
        channel.basic_get.return_value = (None, None, None)
    else:
        # Return each message in turn, then (None, None, None) to signal end
        returns = [*list(messages), (None, None, None)]
        channel.basic_get.side_effect = returns

    return mock_pika


class TestDlqInspectCommand:
    def test_inspect_no_messages_table(self) -> None:
        """dlq inspect with empty queue prints 'No messages' in table mode."""
        mock_pika = _make_mock_pika(messages=[])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "inspect", "orders.dlq"])

        assert result.exit_code == 0
        assert "No messages" in result.output

    def test_inspect_messages_table(self) -> None:
        """dlq inspect with messages prints routing_key and body_preview."""
        method = _make_pika_method(routing_key="orders.created")
        props = _make_pika_properties(message_id="id-1", headers={"x-death": "true"})
        body = b'{"order_id": 42}'

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "inspect", "orders.dlq"])

        assert result.exit_code == 0
        assert "orders.created" in result.output
        assert "id-1" in result.output

    def test_inspect_messages_with_headers(self) -> None:
        """dlq inspect prints headers when present."""
        method = _make_pika_method()
        props = _make_pika_properties(headers={"x-retry": 3})
        body = b"hello"

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "inspect", "orders.dlq"])

        assert result.exit_code == 0
        assert "x-retry" in result.output

    def test_inspect_json_format(self) -> None:
        """--format json outputs valid JSON list."""
        import json as json_mod

        method = _make_pika_method(routing_key="rk", exchange="ex")
        props = _make_pika_properties(message_id="m1", correlation_id="c1")
        body = b"payload"

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "inspect", "orders.dlq", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["routing_key"] == "rk"
        assert data[0]["message_id"] == "m1"

    def test_inspect_json_format_empty(self) -> None:
        """--format json with empty queue outputs empty list."""
        import json as json_mod

        mock_pika = _make_mock_pika(messages=[])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "inspect", "orders.dlq", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data == []

    def test_inspect_pika_not_installed(self) -> None:
        """dlq inspect exits with code 1 when pika is not available."""
        with patch.dict("sys.modules", {"pika": None}):
            result = runner.invoke(app, ["dlq", "inspect", "orders.dlq"])

        assert result.exit_code == 1

    def test_inspect_limit_respected(self) -> None:
        """--limit caps the number of messages fetched."""
        msgs = []
        for i in range(5):
            method = _make_pika_method(routing_key=f"rk.{i}", delivery_tag=i + 1)
            props = _make_pika_properties(message_id=f"id-{i}")
            msgs.append((method, props, b"body"))

        mock_pika = _make_mock_pika(messages=msgs)

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "inspect", "orders.dlq", "--limit", "3"])

        assert result.exit_code == 0
        # basic_get should have been called only 3 times (limit) + 0 more since we have 5 msgs
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        assert channel.basic_get.call_count == 3

    def test_inspect_nacks_messages(self) -> None:
        """dlq inspect nacks each message to requeue it (non-destructive)."""
        method = _make_pika_method(delivery_tag=99)
        props = _make_pika_properties()
        body = b"x"

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            runner.invoke(app, ["dlq", "inspect", "orders.dlq"])

        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        channel.basic_nack.assert_called_once_with(delivery_tag=99, requeue=True)


class TestDlqReplayCommand:
    def test_replay_no_messages(self) -> None:
        """dlq replay with empty queue prints replayed 0 messages."""
        mock_pika = _make_mock_pika(messages=[])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "orders-exchange"])

        assert result.exit_code == 0
        assert "Replayed 0" in result.output

    def test_replay_publishes_messages(self) -> None:
        """dlq replay publishes messages and acks them."""
        method = _make_pika_method(routing_key="orders.created", delivery_tag=5)
        props = _make_pika_properties(message_id="mid-1")
        body = b"body-data"

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "orders-exchange"])

        assert result.exit_code == 0
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        channel.basic_publish.assert_called_once_with(
            exchange="orders-exchange",
            routing_key="orders.created",
            body=body,
            properties=props,
            mandatory=True,
        )
        channel.basic_ack.assert_called_once_with(delivery_tag=5)
        # Publisher confirms must be on, or ack-after-publish can lose the message
        channel.confirm_delivery.assert_called_once()
        assert "Replayed 1" in result.output

    def test_replay_routing_key_override(self) -> None:
        """--routing-key overrides the original routing key."""
        method = _make_pika_method(routing_key="original.rk", delivery_tag=1)
        props = _make_pika_properties()
        body = b"data"

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "exchange", "--routing-key", "override.rk"])

        assert result.exit_code == 0
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        call_kwargs = channel.basic_publish.call_args
        assert call_kwargs.kwargs["routing_key"] == "override.rk"

    def test_replay_dry_run(self) -> None:
        """--dry-run prints what would be replayed without publishing."""
        method = _make_pika_method(routing_key="rk", delivery_tag=2)
        props = _make_pika_properties()
        body = b"body"

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "orders-ex", "--dry-run"])

        assert result.exit_code == 0
        assert "[dry-run]" in result.output
        # Should NOT have published
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        channel.basic_publish.assert_not_called()
        # Should have nacked (requeued) in dry-run
        channel.basic_nack.assert_called_once_with(delivery_tag=2, requeue=True)
        # Should NOT print "Replayed N" in dry-run
        assert "Replayed" not in result.output

    def test_replay_pika_not_installed(self) -> None:
        """dlq replay exits with code 1 when pika is not available."""
        with patch.dict("sys.modules", {"pika": None}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "exchange"])

        assert result.exit_code == 1

    def test_replay_failed_publish_nacks_and_exits_nonzero(self) -> None:
        """C2 regression: an unroutable/nacked publish must NOT ack the DLQ
        message — it is nack-requeued (stays on the DLQ) and the command
        exits non-zero."""

        class _FakeUnroutable(Exception):
            pass

        class _FakeNack(Exception):
            pass

        method = _make_pika_method(routing_key="rk", delivery_tag=7)
        props = _make_pika_properties(message_id="doomed")
        mock_pika = _make_mock_pika(messages=[(method, props, b"body")])
        mock_pika.exceptions.UnroutableError = _FakeUnroutable
        mock_pika.exceptions.NackError = _FakeNack

        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        channel.basic_publish.side_effect = _FakeUnroutable("no binding")

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "orders-ex"])

        assert result.exit_code == 1
        channel.basic_ack.assert_not_called()
        channel.basic_nack.assert_called_once_with(delivery_tag=7, requeue=True)

    def test_replay_reset_retry_count_strips_header(self) -> None:
        """--reset-retry-count strips the retry-count header before
        publishing, so a previously max-retried message gets a fresh retry
        ladder instead of being instantly terminal on the next failure.

        The CLI replay path is hand-rolled raw pika (not DLQInspector), so
        DLQInspector.replay(reset_retry_count=True) existing in the library
        doesn't help a CLI user at all -- this flag is what actually closes
        the gap for the `rabbitkit dlq replay` operator workflow."""
        method = _make_pika_method(routing_key="rk", delivery_tag=3)
        props = _make_pika_properties(headers={"x-rabbitkit-retry-count": 4, "x-tenant": "acme"})
        body = b"body"

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(
                app, ["dlq", "replay", "orders.dlq", "orders-ex", "--reset-retry-count"]
            )

        assert result.exit_code == 0
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        published_props = channel.basic_publish.call_args.kwargs["properties"]
        assert "x-rabbitkit-retry-count" not in published_props.headers
        assert published_props.headers["x-tenant"] == "acme"  # other headers preserved

    def test_replay_without_reset_retry_count_preserves_header(self) -> None:
        """Default (no flag) preserves the retry-count header verbatim --
        matches the documented loop-safe default."""
        method = _make_pika_method(routing_key="rk", delivery_tag=3)
        props = _make_pika_properties(headers={"x-rabbitkit-retry-count": 4})
        body = b"body"

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "orders-ex"])

        assert result.exit_code == 0
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        published_props = channel.basic_publish.call_args.kwargs["properties"]
        assert published_props.headers["x-rabbitkit-retry-count"] == 4

    def test_replay_reset_retry_count_custom_header_name(self) -> None:
        """--retry-count-header lets an operator match a customized
        RetryConfig.retry_header."""
        method = _make_pika_method(routing_key="rk", delivery_tag=3)
        props = _make_pika_properties(headers={"my-custom-retry-count": 2})
        body = b"body"

        mock_pika = _make_mock_pika(messages=[(method, props, body)])

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(
                app,
                [
                    "dlq",
                    "replay",
                    "orders.dlq",
                    "orders-ex",
                    "--reset-retry-count",
                    "--retry-count-header",
                    "my-custom-retry-count",
                ],
            )

        assert result.exit_code == 0
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        published_props = channel.basic_publish.call_args.kwargs["properties"]
        assert "my-custom-retry-count" not in published_props.headers

    def test_replay_multiple_messages(self) -> None:
        """dlq replay processes multiple messages in order."""
        msgs = []
        for i in range(3):
            method = _make_pika_method(routing_key=f"rk.{i}", delivery_tag=i + 10)
            props = _make_pika_properties(message_id=f"msg-{i}")
            msgs.append((method, props, f"body-{i}".encode()))

        mock_pika = _make_mock_pika(messages=msgs)

        with patch.dict("sys.modules", {"pika": mock_pika}):
            result = runner.invoke(app, ["dlq", "replay", "orders.dlq", "orders-ex"])

        assert result.exit_code == 0
        channel = mock_pika.BlockingConnection.return_value.channel.return_value
        assert channel.basic_publish.call_count == 3
        assert channel.basic_ack.call_count == 3
        assert "Replayed 3" in result.output


# ---------------------------------------------------------------------------
# TestRoutesCommand
# ---------------------------------------------------------------------------


def _make_full_route(
    name: str = "orders-route",
    queue_name: str = "orders.q",
    exchange_name: str | None = "orders-ex",
    routing_key: str = "orders.*",
    ack_policy_value: str = "auto",
    tags: set | None = None,
    description: str = "Route description",
    has_retry: bool = False,
    max_retries: int = 3,
    delays: list | None = None,
    queue_durable: bool = True,
    queue_auto_delete: bool = False,
) -> MagicMock:
    """Build a rich route mock for routes list / describe tests."""
    route = MagicMock()
    route.name = name
    route.queue.name = queue_name
    route.queue.routing_key = routing_key
    route.queue.durable = queue_durable
    route.queue.auto_delete = queue_auto_delete
    route.ack_policy.value = ack_policy_value
    route.tags = tags if tags is not None else set()
    route.description = description

    if exchange_name is not None:
        route.exchange.name = exchange_name
        route.exchange.type.value = "topic"
    else:
        route.exchange = None

    if has_retry:
        retry = MagicMock()
        retry.max_retries = max_retries
        retry.delays = delays or [1, 5, 10]
        route.retry_config = retry
    else:
        route.retry_config = None

    return route


class TestRoutesListCommand:
    def test_routes_list_json(self) -> None:
        """routes list --format json outputs a list with route data."""
        import json as json_mod

        route = _make_full_route(name="my-route", queue_name="q1", tags={"tag-a"})
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "list", "myapp:broker", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["name"] == "my-route"
        assert data[0]["queue"] == "q1"
        assert data[0]["ack_policy"] == "auto"
        assert "tag-a" in data[0]["tags"]

    def test_routes_list_table(self) -> None:
        """routes list default table format prints headers and data."""
        route = _make_full_route(name="r1", queue_name="q1")
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "list", "myapp:broker"])

        assert result.exit_code == 0
        assert "name" in result.output
        assert "r1" in result.output
        assert "q1" in result.output

    def test_routes_list_empty_table(self) -> None:
        """routes list with no routes prints 'No routes registered.'"""
        mock_broker = MagicMock()
        mock_broker.routes = []

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "list", "myapp:broker"])

        assert result.exit_code == 0
        assert "No routes registered" in result.output

    def test_routes_list_empty_json(self) -> None:
        """routes list --format json with no routes outputs []."""
        import json as json_mod

        mock_broker = MagicMock()
        mock_broker.routes = []

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "list", "myapp:broker", "--format", "json"])

        assert result.exit_code == 0
        assert json_mod.loads(result.output) == []

    def test_routes_list_retry_enabled(self) -> None:
        """routes list shows retry count when retry_config is present."""
        import json as json_mod

        route = _make_full_route(has_retry=True, max_retries=5)
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "list", "myapp:broker", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data[0]["retry"] == "5x"

    def test_routes_list_retry_disabled(self) -> None:
        """routes list shows 'disabled' when no retry_config."""
        import json as json_mod

        route = _make_full_route(has_retry=False)
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "list", "myapp:broker", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data[0]["retry"] == "disabled"

    def test_routes_list_no_exchange(self) -> None:
        """routes list shows empty exchange column when exchange is None."""
        import json as json_mod

        route = _make_full_route(exchange_name=None)
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "list", "myapp:broker", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data[0]["exchange"] == ""

    def test_routes_list_multiple_routes(self) -> None:
        """routes list table with multiple routes has all names."""
        mock_broker = MagicMock()
        mock_broker.routes = [
            _make_full_route(name="route-a", queue_name="qa"),
            _make_full_route(name="route-b", queue_name="qb"),
        ]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "list", "myapp:broker"])

        assert result.exit_code == 0
        assert "route-a" in result.output
        assert "route-b" in result.output


class TestRoutesDescribeCommand:
    def test_describe_found(self) -> None:
        """routes describe outputs full JSON for a known route."""
        import json as json_mod

        route = _make_full_route(
            name="target-route",
            queue_name="q.target",
            exchange_name="ex.target",
            has_retry=True,
            max_retries=3,
            delays=[1, 5],
            description="My description",
            tags={"svc-a"},
        )
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "describe", "myapp:broker", "target-route"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data["name"] == "target-route"
        assert data["queue"]["name"] == "q.target"
        assert data["exchange"]["name"] == "ex.target"
        assert data["retry"]["max_retries"] == 3
        assert data["retry"]["delays"] == [1, 5]
        assert data["description"] == "My description"
        assert "svc-a" in data["tags"]

    def test_describe_not_found(self) -> None:
        """routes describe exits with code 1 when route name not found."""
        mock_broker = MagicMock()
        mock_broker.routes = [_make_full_route(name="other-route")]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "describe", "myapp:broker", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower() or "nonexistent" in result.output

    def test_describe_no_exchange(self) -> None:
        """routes describe outputs null exchange when route has no exchange."""
        import json as json_mod

        route = _make_full_route(name="no-ex-route", exchange_name=None)
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "describe", "myapp:broker", "no-ex-route"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data["exchange"] is None

    def test_describe_no_retry(self) -> None:
        """routes describe outputs null retry when no retry config."""
        import json as json_mod

        route = _make_full_route(name="no-retry", has_retry=False)
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "describe", "myapp:broker", "no-retry"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data["retry"] is None

    def test_describe_empty_tags(self) -> None:
        """routes describe outputs empty tags list when no tags."""
        import json as json_mod

        route = _make_full_route(name="no-tags", tags=set())
        mock_broker = MagicMock()
        mock_broker.routes = [route]

        with patch("rabbitkit.cli.commands.routes.load_broker", return_value=mock_broker):
            result = runner.invoke(app, ["routes", "describe", "myapp:broker", "no-tags"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert data["tags"] == []


# ---------------------------------------------------------------------------
# TestTopologyValidateDiffApply
# ---------------------------------------------------------------------------


def _make_exchange_route(
    queue_name: str = "orders.q",
    exchange_name: str = "orders-ex",
    exchange_type_value: str = "topic",
    durable: bool = True,
    exclusive: bool = False,
    auto_delete: bool = False,
) -> MagicMock:
    """Build a route mock suitable for topology tests."""
    route = MagicMock()
    route.queue.name = queue_name
    route.queue.durable = durable
    route.queue.exclusive = exclusive
    route.queue.auto_delete = auto_delete
    route.exchange.name = exchange_name
    route.exchange.type.value = exchange_type_value
    route.exchange.durable = durable
    route.exchange.auto_delete = auto_delete
    return route


def _make_broker_with_routes(routes: list) -> MagicMock:
    broker = MagicMock()
    broker.routes = routes
    return broker


def _declared_for(routes: list) -> dict:
    """Mirror _declared_resources logic for building test expectations."""
    queues = {}
    exchanges = {}
    for r in routes:
        queues[r.queue.name] = {
            "durable": r.queue.durable,
            "exclusive": r.queue.exclusive,
            "auto_delete": r.queue.auto_delete,
        }
        if r.exchange and r.exchange.name:
            exchanges[r.exchange.name] = {
                "type": r.exchange.type.value,
                "durable": r.exchange.durable,
                "auto_delete": r.exchange.auto_delete,
            }
    return {"queues": queues, "exchanges": exchanges}


class TestTopologyValidateCommand:
    def test_validate_ok(self) -> None:
        """topology validate exits 0 when declared matches live."""
        route = _make_exchange_route()
        declared = _declared_for([route])

        live = {
            "queues": {"orders.q": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {"orders-ex": {"type": "topic", "durable": True, "auto_delete": False}},
        }

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=_make_broker_with_routes([route])):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "validate", "myapp:broker"])

        assert result.exit_code == 0
        assert "OK" in result.output

    def test_validate_missing_queue(self) -> None:
        """topology validate exits 1 when a declared queue is missing in live."""
        route = _make_exchange_route(queue_name="missing.q", exchange_name="")
        declared = {
            "queues": {"missing.q": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }
        live = {"queues": {}, "exchanges": {}}

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=_make_broker_with_routes([route])):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "validate", "myapp:broker"])

        assert result.exit_code == 1
        assert "MISSING" in result.output
        assert "missing.q" in result.output

    def test_validate_queue_property_mismatch(self) -> None:
        """topology validate reports MISMATCH when queue property differs."""
        declared = {
            "queues": {"orders.q": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }
        live = {
            "queues": {"orders.q": {"durable": False, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "validate", "myapp:broker"])

        assert result.exit_code == 1
        assert "MISMATCH" in result.output
        assert "durable" in result.output

    def test_validate_missing_exchange(self) -> None:
        """topology validate reports MISSING when declared exchange absent from live."""
        declared = {
            "queues": {},
            "exchanges": {"my-ex": {"type": "topic", "durable": True, "auto_delete": False}},
        }
        live = {"queues": {}, "exchanges": {}}

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "validate", "myapp:broker"])

        assert result.exit_code == 1
        assert "MISSING" in result.output
        assert "my-ex" in result.output

    def test_validate_exchange_property_mismatch(self) -> None:
        """topology validate reports MISMATCH when exchange property differs."""
        declared = {
            "queues": {},
            "exchanges": {"my-ex": {"type": "fanout", "durable": True, "auto_delete": False}},
        }
        live = {
            "queues": {},
            "exchanges": {"my-ex": {"type": "direct", "durable": True, "auto_delete": False}},
        }

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "validate", "myapp:broker"])

        assert result.exit_code == 1
        assert "MISMATCH" in result.output
        assert "my-ex" in result.output

    def test_validate_management_api_error(self) -> None:
        """topology validate exits 1 when management API is unreachable."""
        declared = {"queues": {}, "exchanges": {}}

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch(
                    "rabbitkit.cli.commands.topology._live_resources",
                    side_effect=ConnectionError("refused"),
                ):
                    result = runner.invoke(app, ["topology", "validate", "myapp:broker"])

        assert result.exit_code == 1
        assert "ERROR" in result.output


class TestTopologyDiffCommand:
    def test_diff_no_diff_text(self) -> None:
        """topology diff exits 0 and prints 'No diff' when declared == live."""
        declared = {
            "queues": {"q1": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }
        live = {
            "queues": {"q1": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "diff", "myapp:broker"])

        assert result.exit_code == 0
        assert "No diff" in result.output

    def test_diff_declared_not_live_text(self) -> None:
        """topology diff shows '+' lines for queues declared but not live."""
        declared = {
            "queues": {"new.q": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }
        live = {"queues": {}, "exchanges": {}}

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "diff", "myapp:broker"])

        assert result.exit_code == 1
        assert "+ queue" in result.output
        assert "new.q" in result.output

    def test_diff_live_not_declared_text(self) -> None:
        """topology diff shows '~' lines for queues in live but not declared."""
        declared = {"queues": {}, "exchanges": {}}
        live = {
            "queues": {"zombie.q": {"durable": False, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "diff", "myapp:broker"])

        assert result.exit_code == 1
        assert "~" in result.output
        assert "zombie.q" in result.output

    def test_diff_property_mismatch_text(self) -> None:
        """topology diff shows '!' lines for property mismatches."""
        declared = {
            "queues": {"q1": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }
        live = {
            "queues": {"q1": {"durable": False, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "diff", "myapp:broker"])

        assert result.exit_code == 1
        assert "!" in result.output
        assert "durable" in result.output

    def test_diff_json_format_no_diff(self) -> None:
        """topology diff --format json outputs JSON even when no diff."""
        import json as json_mod

        declared = {"queues": {}, "exchanges": {}}
        live = {"queues": {}, "exchanges": {}}

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "diff", "myapp:broker", "--format", "json"])

        assert result.exit_code == 0
        data = json_mod.loads(result.output)
        assert "queues" in data
        assert "exchanges" in data

    def test_diff_json_format_with_diff(self) -> None:
        """topology diff --format json outputs diff data when differences exist."""
        import json as json_mod

        declared = {
            "queues": {"new.q": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {"new.ex": {"type": "topic", "durable": True, "auto_delete": False}},
        }
        live = {"queues": {}, "exchanges": {}}

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "diff", "myapp:broker", "--format", "json"])

        assert result.exit_code == 1
        data = json_mod.loads(result.output)
        assert "new.q" in data["queues"]["declared_not_live"]
        assert "new.ex" in data["exchanges"]["declared_not_live"]

    def test_diff_exchange_property_mismatch_text(self) -> None:
        """topology diff shows '!' for exchange property mismatches."""
        declared = {
            "queues": {},
            "exchanges": {"ex1": {"type": "fanout", "durable": True, "auto_delete": False}},
        }
        live = {
            "queues": {},
            "exchanges": {"ex1": {"type": "direct", "durable": True, "auto_delete": False}},
        }

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.cli.commands.topology._live_resources", return_value=live):
                    result = runner.invoke(app, ["topology", "diff", "myapp:broker"])

        assert result.exit_code == 1
        assert "!" in result.output
        assert "ex1" in result.output

    def test_diff_management_api_error(self) -> None:
        """topology diff exits 1 when management API is unreachable."""
        declared = {"queues": {}, "exchanges": {}}

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch(
                    "rabbitkit.cli.commands.topology._live_resources",
                    side_effect=ConnectionError("refused"),
                ):
                    result = runner.invoke(app, ["topology", "diff", "myapp:broker"])

        assert result.exit_code == 1
        assert "ERROR" in result.output


class TestTopologyApplyCommand:
    def test_apply_dry_run_no_queues_no_exchanges(self) -> None:
        """topology apply --dry-run with no resources prints 0 queues/exchanges."""
        declared = {"queues": {}, "exchanges": {}}

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                result = runner.invoke(app, ["topology", "apply", "myapp:broker", "--dry-run"])

        assert result.exit_code == 0
        assert "[dry-run]" in result.output
        assert "0 queue(s)" in result.output

    def test_apply_dry_run_with_queues_and_exchanges(self) -> None:
        """topology apply --dry-run lists queues and exchanges."""
        declared = {
            "queues": {"q1": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {"ex1": {"type": "topic", "durable": True, "auto_delete": False}},
        }

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                result = runner.invoke(app, ["topology", "apply", "myapp:broker", "--dry-run"])

        assert result.exit_code == 0
        assert "[dry-run]" in result.output
        assert "q1" in result.output
        assert "ex1" in result.output
        assert "durable=True" in result.output

    def test_apply_success(self) -> None:
        """topology apply calls asyncio.run and prints success message."""
        import asyncio

        declared = {
            "queues": {"q1": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {},
        }

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch.object(asyncio, "run") as mock_run:
                    result = runner.invoke(app, ["topology", "apply", "myapp:broker"])

        assert result.exit_code == 0
        mock_run.assert_called_once()
        assert "Applied" in result.output
        assert "1 queue(s)" in result.output

    def test_apply_error(self) -> None:
        """topology apply exits 1 when asyncio.run raises an exception."""
        import asyncio

        declared = {"queues": {}, "exchanges": {}}

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch.object(asyncio, "run", side_effect=RuntimeError("connection refused")):
                    result = runner.invoke(app, ["topology", "apply", "myapp:broker"])

        assert result.exit_code == 1
        assert "ERROR" in result.output

    def test_apply_inner_coroutine_queues_and_exchanges(self) -> None:
        """Lines 293-328: _apply() coroutine body runs with mocked AsyncBroker."""
        import asyncio

        declared = {
            "queues": {"q1": {"durable": True, "exclusive": False, "auto_delete": False}},
            "exchanges": {"ex1": {"type": "topic", "durable": True, "auto_delete": False}},
        }

        mock_transport = AsyncMock()
        mock_async_broker = AsyncMock()
        mock_async_broker.start = AsyncMock()
        mock_async_broker.stop = AsyncMock()
        mock_async_broker._transport = mock_transport

        original_asyncio_run = asyncio.run

        def _real_run(coro: object) -> None:
            original_asyncio_run(coro)  # type: ignore[arg-type]

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.async_.broker.AsyncBroker", return_value=mock_async_broker):
                    with patch.object(asyncio, "run", side_effect=_real_run):
                        result = runner.invoke(app, ["topology", "apply", "myapp:broker"])

        assert result.exit_code == 0
        assert "Applied" in result.output
        mock_async_broker.start.assert_called_once()
        mock_async_broker.stop.assert_called_once()
        assert mock_transport.declare_queue.call_count == 1
        assert mock_transport.declare_exchange.call_count == 1

    def test_apply_inner_coroutine_invalid_exchange_type(self) -> None:
        """Lines 318: unknown exchange type falls back to ExchangeType.DIRECT."""
        import asyncio

        declared = {
            "queues": {},
            "exchanges": {"ex1": {"type": "unknown-type", "durable": False, "auto_delete": False}},
        }

        mock_transport = AsyncMock()
        mock_async_broker = AsyncMock()
        mock_async_broker.start = AsyncMock()
        mock_async_broker.stop = AsyncMock()
        mock_async_broker._transport = mock_transport

        original_asyncio_run = asyncio.run

        def _real_run(coro: object) -> None:
            original_asyncio_run(coro)  # type: ignore[arg-type]

        with patch("rabbitkit.cli.commands.topology.load_broker", return_value=MagicMock()):
            with patch("rabbitkit.cli.commands.topology._declared_resources", return_value=declared):
                with patch("rabbitkit.async_.broker.AsyncBroker", return_value=mock_async_broker):
                    with patch.object(asyncio, "run", side_effect=_real_run):
                        result = runner.invoke(app, ["topology", "apply", "myapp:broker"])

        assert result.exit_code == 0
        assert mock_transport.declare_exchange.call_count == 1
        # Verify that the exchange was declared with DIRECT type as fallback
        called_with = mock_transport.declare_exchange.call_args[0][0]
        from rabbitkit.core.types import ExchangeType
        assert called_with.type == ExchangeType.DIRECT


class TestDeclaredResources:
    """Unit tests for _declared_resources helper."""

    def test_declared_resources_basic(self) -> None:
        """_declared_resources extracts queues and exchanges from routes."""
        from rabbitkit.cli.commands.topology import _declared_resources

        route = _make_exchange_route(
            queue_name="my.queue",
            exchange_name="my.exchange",
            exchange_type_value="topic",
            durable=True,
            exclusive=False,
            auto_delete=False,
        )
        broker = _make_broker_with_routes([route])
        result = _declared_resources(broker)

        assert "my.queue" in result["queues"]
        assert result["queues"]["my.queue"]["durable"] is True
        assert "my.exchange" in result["exchanges"]
        assert result["exchanges"]["my.exchange"]["type"] == "topic"

    def test_declared_resources_no_exchange(self) -> None:
        """_declared_resources skips exchanges when route.exchange is None."""
        from rabbitkit.cli.commands.topology import _declared_resources

        route = MagicMock()
        route.queue.name = "solo.q"
        route.queue.durable = False
        route.queue.exclusive = False
        route.queue.auto_delete = True
        route.exchange = None

        broker = _make_broker_with_routes([route])
        result = _declared_resources(broker)

        assert "solo.q" in result["queues"]
        assert result["exchanges"] == {}

    def test_declared_resources_exchange_no_name(self) -> None:
        """_declared_resources skips exchanges with empty name."""
        from rabbitkit.cli.commands.topology import _declared_resources

        route = MagicMock()
        route.queue.name = "q1"
        route.queue.durable = True
        route.queue.exclusive = False
        route.queue.auto_delete = False
        route.exchange.name = ""  # empty name — should be skipped
        route.exchange.type.value = "direct"
        route.exchange.durable = True
        route.exchange.auto_delete = False

        broker = _make_broker_with_routes([route])
        result = _declared_resources(broker)

        assert result["exchanges"] == {}

    def test_declared_resources_exchange_type_no_value(self) -> None:
        """_declared_resources uses str() when exchange type has no .value."""
        from rabbitkit.cli.commands.topology import _declared_resources

        class _FakeType:
            """Simulates an exchange type enum without a .value attribute."""

            def __str__(self) -> str:
                return "direct"

        route = MagicMock()
        route.queue.name = "q1"
        route.queue.durable = True
        route.queue.exclusive = False
        route.queue.auto_delete = False
        route.exchange.name = "ex1"
        route.exchange.durable = True
        route.exchange.auto_delete = False
        route.exchange.type = _FakeType()  # no .value attribute

        broker = _make_broker_with_routes([route])
        result = _declared_resources(broker)

        assert "ex1" in result["exchanges"]
        assert result["exchanges"]["ex1"]["type"] == "direct"


class TestLiveResources:
    """Unit tests for _live_resources helper."""

    def _mock_urlopen(self, queues_data: list, exchanges_data: list) -> MagicMock:
        """Build a context-manager mock that returns JSON for each call in sequence."""
        import json as json_mod

        responses = [
            MagicMock(read=MagicMock(return_value=json_mod.dumps(queues_data).encode())),
            MagicMock(read=MagicMock(return_value=json_mod.dumps(exchanges_data).encode())),
        ]
        cm_mocks = []
        for resp in responses:
            cm = MagicMock()
            cm.__enter__ = MagicMock(return_value=resp)
            cm.__exit__ = MagicMock(return_value=False)
            cm_mocks.append(cm)

        mock_urlopen = MagicMock(side_effect=cm_mocks)
        return mock_urlopen

    def test_live_resources_basic(self) -> None:
        """_live_resources returns queues and exchanges from management API."""
        from rabbitkit.cli.commands.topology import _live_resources

        queues_data = [{"name": "q1", "durable": True, "exclusive": False, "auto_delete": False}]
        exchanges_data = [{"name": "ex1", "type": "topic", "durable": True, "auto_delete": False}]

        mock_urlopen = self._mock_urlopen(queues_data, exchanges_data)

        with patch("urllib.request.urlopen", mock_urlopen):
            result = _live_resources("http://guest:guest@localhost:15672", "/")

        assert "q1" in result["queues"]
        assert "ex1" in result["exchanges"]
        assert result["queues"]["q1"]["durable"] is True
        assert result["exchanges"]["ex1"]["type"] == "topic"

    def test_live_resources_skips_default_exchange(self) -> None:
        """_live_resources skips the default exchange (empty name)."""
        from rabbitkit.cli.commands.topology import _live_resources

        queues_data: list = []
        exchanges_data = [
            {"name": "", "type": "direct", "durable": True, "auto_delete": False},
            {"name": "real-ex", "type": "fanout", "durable": False, "auto_delete": False},
        ]

        mock_urlopen = self._mock_urlopen(queues_data, exchanges_data)

        with patch("urllib.request.urlopen", mock_urlopen):
            result = _live_resources("http://guest:guest@localhost:15672", "/")

        assert "" not in result["exchanges"]
        assert "real-ex" in result["exchanges"]

    def test_live_resources_queues_api_failure(self) -> None:
        """_live_resources returns empty queues dict when queues API fails."""
        import json as json_mod

        from rabbitkit.cli.commands.topology import _live_resources

        # First call (queues) raises, second call (exchanges) succeeds
        exchanges_resp = MagicMock(read=MagicMock(return_value=json_mod.dumps([]).encode()))
        exchanges_cm = MagicMock()
        exchanges_cm.__enter__ = MagicMock(return_value=exchanges_resp)
        exchanges_cm.__exit__ = MagicMock(return_value=False)

        call_count = [0]

        def _urlopen_side_effect(*args: object, **kwargs: object) -> MagicMock:
            call_count[0] += 1
            if call_count[0] == 1:
                raise ConnectionError("refused")
            return exchanges_cm

        with patch("urllib.request.urlopen", side_effect=_urlopen_side_effect):
            result = _live_resources("http://guest:guest@localhost:15672", "/")

        assert result["queues"] == {}

    def test_live_resources_exchanges_api_failure(self) -> None:
        """_live_resources returns empty exchanges dict when exchanges API fails."""
        import json as json_mod

        from rabbitkit.cli.commands.topology import _live_resources

        # First call (queues) succeeds, second call (exchanges) raises
        queues_resp = MagicMock(read=MagicMock(return_value=json_mod.dumps([]).encode()))
        queues_cm = MagicMock()
        queues_cm.__enter__ = MagicMock(return_value=queues_resp)
        queues_cm.__exit__ = MagicMock(return_value=False)

        call_count = [0]

        def _urlopen_side_effect(*args: object, **kwargs: object) -> MagicMock:
            call_count[0] += 1
            if call_count[0] == 1:
                return queues_cm
            raise ConnectionError("refused")

        with patch("urllib.request.urlopen", side_effect=_urlopen_side_effect):
            result = _live_resources("http://guest:guest@localhost:15672", "/")

        assert result["exchanges"] == {}

    def test_live_resources_rejects_non_http_scheme(self) -> None:
        """--url is passed straight to urlopen -- without a scheme allowlist
        it would happily fetch file:// (arbitrary local file read) or other
        registered urllib handlers instead of just the management API."""
        from rabbitkit.cli.commands.topology import _live_resources

        with pytest.raises(ValueError, match="Unsupported management URL scheme"):
            _live_resources("file:///etc/passwd", "/")

    def test_live_resources_rejects_non_http_scheme_before_any_request(self) -> None:
        """The scheme check must happen before urlopen is ever called."""
        from rabbitkit.cli.commands.topology import _live_resources

        with patch("urllib.request.urlopen") as mock_urlopen:
            with pytest.raises(ValueError, match="Unsupported management URL scheme"):
                _live_resources("ftp://example.com", "/")
        mock_urlopen.assert_not_called()
