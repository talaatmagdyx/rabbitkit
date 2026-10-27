"""Tests for async_/reconnect.py — rabbitkit's reconnect policy on aio-pika's RobustConnection (G-08)."""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Iterator
from typing import Any

import aio_pika
import pytest
from aiormq.exceptions import ConnectionClosed, ProbableAuthenticationError
from yarl import URL

from rabbitkit.async_ import reconnect
from rabbitkit.async_.reconnect import (
    RabbitkitRobustConnection,
    ReconnectAttempt,
    ReconnectPolicy,
    backoff_delay,
)
from rabbitkit.core.config import ConnectionConfig
from rabbitkit.core.errors import AuthenticationFailedError, ReconnectBudgetExhaustedError, TerminalConnectionError

_URL = URL("amqp://user:old@primary:5672/%2F?heartbeat=30")


@pytest.fixture
def no_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reconnect, "_jitter", lambda ceiling: ceiling)


@pytest.fixture
def connected_ok(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """aio-pika's own _on_connected needs a live transport; stand it in."""

    async def _ok(self: Any) -> None:
        return None

    monkeypatch.setattr(aio_pika.RobustConnection, "_on_connected", _ok)
    yield


class _Hooks:
    def __init__(self) -> None:
        self.attempts: list[ReconnectAttempt] = []
        self.terminal: list[TerminalConnectionError] = []
        self.instances: list[Any] = []


def _make(config: ConnectionConfig, hooks: _Hooks | None = None, **policy_kw: Any) -> RabbitkitRobustConnection:
    hooks = hooks or _Hooks()
    policy = ReconnectPolicy(
        config,
        on_attempt=hooks.attempts.append,
        on_terminal=hooks.terminal.append,
        on_instance=hooks.instances.append,
        **policy_kw,
    )
    return RabbitkitRobustConnection(_URL, rk_policy=policy, reconnect_interval=1.5)


def _failed(error: BaseException) -> asyncio.Future[Any]:
    future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    future.set_exception(error)
    return future


async def _up(conn: RabbitkitRobustConnection) -> None:
    await conn._on_connected()


async def _drop(conn: RabbitkitRobustConnection, error: BaseException | None = None) -> None:
    await conn._on_connection_close(_failed(error or ConnectionClosed(320, "CONNECTION_FORCED")))


def _refused() -> BaseException:
    return ConnectionRefusedError(61, "Connection refused")


class TestBackoffDelay:
    def test_doubles_from_base_up_to_the_cap(self, no_jitter: None) -> None:
        assert [backoff_delay(n, 0.5, 3.0) for n in range(1, 6)] == [0.5, 1.0, 2.0, 3.0, 3.0]

    def test_full_jitter_stays_within_the_ceiling(self) -> None:
        delays = [backoff_delay(3, 1.0, 30.0) for _ in range(200)]
        assert all(0.0 <= d <= 4.0 for d in delays)
        assert len(set(delays)) > 1

    def test_huge_attempt_counts_do_not_overflow(self, no_jitter: None) -> None:
        assert backoff_delay(10_000, 1.0, 30.0) == 30.0

    def test_attempt_below_one_uses_base(self, no_jitter: None) -> None:
        assert backoff_delay(0, 2.0, 30.0) == 2.0


class TestPolicy:
    def test_endpoint_index_wraps(self) -> None:
        policy = ReconnectPolicy(ConnectionConfig(host="a", nodes=("b",)), endpoint_index=3)
        assert policy.endpoint_index == 1
        assert policy.role == "connection"


@pytest.mark.usefixtures("connected_ok", "no_jitter")
class TestRabbitkitRobustConnection:
    async def test_reports_itself_and_keeps_the_steady_interval(self) -> None:
        hooks = _Hooks()
        conn = _make(ConnectionConfig(host="primary"), hooks)
        assert hooks.instances == [conn]
        assert conn.reconnect_interval == 1.5
        assert conn.terminal_error is None
        assert conn.current_endpoint == "primary:5672"

    async def test_failures_before_the_first_connect_are_aio_pikas_business(self) -> None:
        hooks = _Hooks()
        conn = _make(ConnectionConfig(), hooks)
        await conn._on_connection_close(_failed(ProbableAuthenticationError("ACCESS_REFUSED")))
        assert hooks.attempts == [] and hooks.terminal == []

    async def test_drop_refreshes_credentials_and_keeps_the_node(self) -> None:
        secret = {"password": "old"}
        config = ConnectionConfig(
            host="primary", nodes=("backup:5673",), credentials_provider=lambda: ("u2", secret["password"])
        )
        conn = _make(config)
        await _up(conn)
        secret["password"] = "n3w:p@ss/"
        await _drop(conn)
        assert conn.url.host == "primary"
        assert conn.url.user == "u2"
        assert conn.url.password == "n3w:p@ss/"
        assert conn.url.query["heartbeat"] == "30"

    async def test_failed_attempts_back_off_and_move_through_the_nodes(self) -> None:
        hooks = _Hooks()
        config = ConnectionConfig(
            host="primary", nodes=("backup:5673",), reconnect_backoff_base=0.5, reconnect_backoff_max=1.5
        )
        conn = _make(config, hooks, role="consumer")
        await _up(conn)
        await _drop(conn)
        for _ in range(4):
            await conn._on_connection_close(_failed(_refused()))

        assert [a.attempt for a in hooks.attempts] == [1, 2, 3, 4]
        assert [a.delay for a in hooks.attempts] == [0.5, 1.0, 1.5, 1.5]
        assert [a.endpoint for a in hooks.attempts] == ["primary:5672", "backup:5673", "primary:5672", "backup:5673"]
        assert all(a.role == "consumer" and isinstance(a.error, ConnectionRefusedError) for a in hooks.attempts)
        assert conn.reconnect_interval == 1.5
        assert conn.url.host == "primary" and conn.url.port == 5672
        assert hooks.terminal == []

    async def test_success_resets_the_outage(self, caplog: pytest.LogCaptureFixture) -> None:
        hooks = _Hooks()
        conn = _make(ConnectionConfig(reconnect_backoff_base=0.5, reconnect_backoff_max=8.0), hooks)
        await _up(conn)
        await _drop(conn)
        await conn._on_connection_close(_failed(_refused()))
        await conn._on_connection_close(_failed(_refused()))
        with caplog.at_level(logging.INFO, logger="rabbitkit.async_.reconnect"):
            await _up(conn)
        assert "after 2 failed attempt(s)" in caplog.text
        assert conn.reconnect_interval == 1.5  # steady again

        await _drop(conn)
        await conn._on_connection_close(_failed(_refused()))
        assert hooks.attempts[-1].attempt == 1
        assert hooks.attempts[-1].delay == 0.5

    async def test_permanent_failure_gives_up_for_good(self) -> None:
        hooks = _Hooks()
        conn = _make(ConnectionConfig(), hooks)
        await _up(conn)
        await _drop(conn)
        cause = ProbableAuthenticationError("ACCESS_REFUSED - Login was refused")
        await conn._on_connection_close(_failed(cause))

        assert len(hooks.terminal) == 1
        error = hooks.terminal[0]
        assert isinstance(error, AuthenticationFailedError)
        assert error.__cause__ is cause
        assert conn.terminal_error is error
        assert hooks.attempts == []
        assert conn._rk_close_task is not None
        await conn._rk_close_task
        assert conn._close_called  # is_closed stays False on aio-pika 9 without a transport

        # Closed for good: later closes are ignored.
        await _drop(conn)
        assert len(hooks.terminal) == 1

    async def test_attempt_budget(self) -> None:
        hooks = _Hooks()
        conn = _make(ConnectionConfig(reconnect_budget_attempts=2), hooks)
        await _up(conn)
        await _drop(conn)
        await conn._on_connection_close(_failed(_refused()))
        assert hooks.terminal == []
        await conn._on_connection_close(_failed(_refused()))

        assert len(hooks.attempts) == 1
        assert isinstance(hooks.terminal[0], ReconnectBudgetExhaustedError)
        assert hooks.terminal[0].reason == "reconnect_budget_exhausted"
        assert isinstance(hooks.terminal[0].__cause__, ConnectionRefusedError)
        assert conn._rk_close_task is not None
        await conn._rk_close_task

    async def test_seconds_budget_clamps_the_last_delay_then_gives_up(self) -> None:
        hooks = _Hooks()
        conn = _make(
            ConnectionConfig(reconnect_backoff_base=5.0, reconnect_backoff_max=5.0, reconnect_budget_seconds=10.0),
            hooks,
        )
        await _up(conn)
        await _drop(conn)
        assert conn._rk_outage_started is not None
        conn._rk_outage_started = time.monotonic() - 8.0
        await conn._on_connection_close(_failed(_refused()))
        assert hooks.attempts[0].delay == pytest.approx(2.0, abs=0.1)

        conn._rk_outage_started = time.monotonic() - 10.5
        await conn._on_connection_close(_failed(_refused()))
        assert isinstance(hooks.terminal[0], ReconnectBudgetExhaustedError)
        assert conn._rk_close_task is not None
        await conn._rk_close_task

    async def test_failing_credentials_provider_keeps_the_previous_credentials(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        def provider() -> tuple[str, str]:
            raise RuntimeError("vault says no: s3cret")

        conn = _make(ConnectionConfig(host="primary", credentials_provider=provider))
        await _up(conn)
        with caplog.at_level(logging.WARNING, logger="rabbitkit.async_.reconnect"):
            await _drop(conn)
        assert conn.url.user == "user" and conn.url.password == "old"
        assert "credentials_provider raised RuntimeError" in caplog.text
        assert "s3cret" not in caplog.text

    async def test_a_raising_hook_never_breaks_the_reconnect(self, caplog: pytest.LogCaptureFixture) -> None:
        def boom(_attempt: Any) -> None:
            raise ValueError("hook bug")

        policy = ReconnectPolicy(ConnectionConfig(), on_attempt=boom)
        conn = RabbitkitRobustConnection(_URL, rk_policy=policy)
        await _up(conn)
        await _drop(conn)
        with caplog.at_level(logging.ERROR, logger="rabbitkit.async_.reconnect"):
            await conn._on_connection_close(_failed(_refused()))
        assert "on_reconnect_attempt hook raised" in caplog.text

    async def test_close_after_close_called_is_not_a_failure(self) -> None:
        hooks = _Hooks()
        conn = _make(ConnectionConfig(), hooks)
        await _up(conn)
        await conn.close()
        await _drop(conn)
        assert hooks.attempts == [] and conn._rk_outage_started is None

    async def test_unreadable_closing_futures_count_as_unknown_errors(self) -> None:
        hooks = _Hooks()
        conn = _make(ConnectionConfig(), hooks)
        await _up(conn)
        cancelled: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        cancelled.cancel()
        await conn._on_connection_close(cancelled)  # the drop
        clean: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        clean.set_result(None)
        await conn._on_connection_close(clean)
        assert hooks.attempts[0].error is None
        assert hooks.terminal == []

    async def test_close_for_good_swallows_close_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        conn = _make(ConnectionConfig())

        async def broken_close(self: Any, exc: Any = None) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(aio_pika.RobustConnection, "close", broken_close)
        await conn._rk_close_for_good(ReconnectBudgetExhaustedError("x", reason="reconnect_budget_exhausted"))


def test_closing_error_of_a_pending_future_is_none() -> None:
    loop = asyncio.new_event_loop()
    try:
        assert reconnect._closing_error(loop.create_future()) is None
    finally:
        loop.close()


async def test_connect_robust_builds_the_subclass(monkeypatch: pytest.MonkeyPatch) -> None:
    """The pool passes ``connection_class=partial(...)``; connect_robust must build it."""
    built: list[Any] = []

    async def no_connect(self: Any, timeout: Any = None) -> None:
        return None

    monkeypatch.setattr(RabbitkitRobustConnection, "connect", no_connect)
    policy = ReconnectPolicy(ConnectionConfig(), on_instance=built.append)
    conn = await aio_pika.connect_robust(
        "amqp://u:p@h:5672/",
        reconnect_interval=2.0,
        connection_class=functools.partial(RabbitkitRobustConnection, rk_policy=policy),  # type: ignore[arg-type]
    )
    assert isinstance(conn, RabbitkitRobustConnection)
    assert built == [conn]
    assert conn.reconnect_interval == 2.0


@pytest.mark.usefixtures("connected_ok", "no_jitter")
class TestConnectionStateHooks:
    """G-22: the policy reports a live drop and a recovery, per role, so the
    connection_up / broker_connected metrics follow reconnects."""

    async def test_drop_and_recovery_fire_once_each(self) -> None:
        lost: list[tuple[str, BaseException | None]] = []
        restored: list[str] = []
        conn = _make(
            ConnectionConfig(),
            role="publisher",
            on_lost=lambda role, error: lost.append((role, error)),
            on_restored=restored.append,
        )
        await _up(conn)
        assert lost == [] and restored == []  # the first connect is not a recovery
        await _drop(conn)
        await conn._on_connection_close(_failed(_refused()))  # a failed attempt is not another drop
        assert len(lost) == 1
        assert lost[0][0] == "publisher"
        assert isinstance(lost[0][1], ConnectionClosed)
        await _up(conn)
        assert restored == ["publisher"]

    async def test_a_raising_lost_hook_never_breaks_the_reconnect(self, caplog: pytest.LogCaptureFixture) -> None:
        def boom(_role: str, _error: BaseException | None) -> None:
            raise ValueError("hook bug")

        conn = _make(ConnectionConfig(), on_lost=boom)
        await _up(conn)
        with caplog.at_level(logging.ERROR, logger="rabbitkit.async_.reconnect"):
            await _drop(conn)
        assert "on_lost hook raised" in caplog.text
        assert conn._rk_outage_started is not None

    async def test_terminal_error_carries_the_role(self) -> None:
        hooks = _Hooks()
        conn = _make(ConnectionConfig(reconnect_budget_attempts=1), hooks, role="consumer")
        await _up(conn)
        await _drop(conn)
        await conn._on_connection_close(_failed(_refused()))
        assert hooks.terminal[0].role == "consumer"
        assert conn._rk_close_task is not None
        await conn._rk_close_task
