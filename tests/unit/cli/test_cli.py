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
