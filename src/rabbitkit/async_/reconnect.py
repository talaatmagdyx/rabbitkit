"""rabbitkit-driven reconnects for aio-pika's ``RobustConnection`` (G-08, ADR-M8).

aio-pika's robust connection restores channels, QoS, exchanges, queues and
consumers after a reconnect, and rabbitkit keeps all of that. What it does
not do: it waits a FIXED ``reconnect_interval`` between attempts forever, it
retries a refused login or a failed TLS verification forever, and every
attempt reuses the URL (credentials and host) of the very first connect.

:class:`RabbitkitRobustConnection` fixes those without replacing aio-pika's
reconnect loop. Every failed reconnect attempt, and the loss of a live
connection, passes through ``RobustConnection._on_connection_close`` before
aio-pika sleeps ``reconnect_interval`` and tries again (aio-pika 9.1 to 10.x).
That hook is where this class

* classifies the failure: a permanent one (bad credentials, vhost refused,
  TLS verification) stops the loop for good;
* counts the attempt against ``ConnectionConfig.reconnect_budget_attempts`` /
  ``reconnect_budget_seconds`` and stops the loop when either runs out;
* sets ``reconnect_interval`` to an exponential backoff with full jitter
  (``reconnect_backoff_base`` doubling up to ``reconnect_backoff_max``);
* rebuilds the URL for the next attempt: fresh credentials from
  ``credentials_provider`` and, after a failure, the next ``nodes`` endpoint.

Stopping the loop closes the connection and reports a
:class:`~rabbitkit.core.errors.TerminalConnectionError` to the pool.

Imports aio-pika at module level: import it lazily (the pool does).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from aio_pika import RobustConnection

from rabbitkit.async_.connection import permanent_connection_error
from rabbitkit.core.config import ConnectionConfig
from rabbitkit.core.errors import ReconnectBudgetExhaustedError, TerminalConnectionError

logger = logging.getLogger(__name__)

# 2**62 seconds is far beyond any reconnect_backoff_max: cap the exponent so a
# months-long outage never overflows the float.
_MAX_BACKOFF_EXPONENT = 62
# Bound on closing a connection that gave up for good.
_CLOSE_TIMEOUT = 10.0


@dataclass(frozen=True, slots=True)
class ReconnectAttempt:
    """One failed reconnect attempt of a connection that had been up.

    Passed to ``AsyncTransportImpl.on_reconnect_attempt`` hooks.

    Attributes:
        role: Which connection: ``"publisher"`` or ``"consumer"``.
        attempt: Consecutive failed attempts in this outage, from 1.
        delay: Seconds aio-pika waits before the next attempt.
        elapsed: Seconds since the connection was lost.
        endpoint: ``host:port`` the failed attempt went to.
        error: Why the attempt failed (never carries credentials).
    """

    role: str
    attempt: int
    delay: float
    elapsed: float
    endpoint: str
    error: BaseException | None


def _jitter(ceiling: float) -> float:
    """Full jitter: a uniform delay in ``[0, ceiling]``. Separate so tests can pin it."""
    return random.uniform(0.0, ceiling)  # noqa: S311 — jitter, not crypto


def backoff_delay(attempt: int, base: float, cap: float) -> float:
    """Delay before the next reconnect after *attempt* consecutive failures.

    Exponential backoff with full jitter: uniform over
    ``[0, min(cap, base * 2 ** (attempt - 1))]``.
    """
    exponent = min(max(attempt - 1, 0), _MAX_BACKOFF_EXPONENT)
    return _jitter(min(cap, base * (2.0**exponent)))


class ReconnectPolicy:
    """Everything one :class:`RabbitkitRobustConnection` needs from rabbitkit.

    Args:
        config: Backoff, budgets, credentials (re-resolved per attempt) and
            the cluster endpoints.
        role: Which connection this is (``"publisher"``/``"consumer"``), for
            hooks and logs.
        endpoint_index: Index into ``config.cluster_endpoints()`` the initial
            connect used; reconnects start there.
        on_attempt: Called after every failed reconnect attempt.
        on_terminal: Called once when the connection gives up for good.
        on_instance: Called with each connection object as it is built, so the
            caller can close one whose initial connect it abandons.
        on_lost: Called with ``(role, error)`` when a live connection drops.
        on_restored: Called with ``role`` when a reconnect after a drop succeeds.
    """

    def __init__(
        self,
        config: ConnectionConfig,
        *,
        role: str = "connection",
        endpoint_index: int = 0,
        on_attempt: Callable[[ReconnectAttempt], None] | None = None,
        on_terminal: Callable[[TerminalConnectionError], None] | None = None,
        on_instance: Callable[[Any], None] | None = None,
        on_lost: Callable[[str, BaseException | None], None] | None = None,
        on_restored: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config
        self.role = role
        self.endpoints = config.cluster_endpoints()
        self.endpoint_index = endpoint_index % len(self.endpoints)
        self.on_attempt = on_attempt
        self.on_terminal = on_terminal
        self.on_instance = on_instance
        self.on_lost = on_lost
        self.on_restored = on_restored


def _closing_error(closing: Any) -> BaseException | None:
    try:
        error = closing.exception()
    except (asyncio.CancelledError, asyncio.InvalidStateError):
        return None
    return error if isinstance(error, BaseException) else None


def _fire(callback: Callable[[Any], None] | None, value: Any, what: str) -> None:
    if callback is None:
        return
    try:
        callback(value)
    except Exception:  # never let a hook break aio-pika's reconnect loop
        logger.exception("%s hook raised", what)


class RabbitkitRobustConnection(RobustConnection):
    """aio-pika ``RobustConnection`` whose reconnects follow rabbitkit's policy.

    Built by ``aio_pika.connect_robust(..., connection_class=partial(cls,
    rk_policy=policy))``. See the module docstring.
    """

    def __init__(self, url: Any, loop: Any = None, *, rk_policy: ReconnectPolicy, **kwargs: Any) -> None:
        super().__init__(url, loop=loop, **kwargs)
        self._rk_policy = rk_policy
        self._rk_endpoint_index = rk_policy.endpoint_index
        # aio-pika also sleeps reconnect_interval after a SUCCESSFUL connect
        # and between consumer-restore retries; restore this after a backoff.
        self._rk_steady_interval: float = self.reconnect_interval
        self._rk_connected_once = False
        self._rk_outage_started: float | None = None
        self._rk_failures = 0
        self._rk_terminal: TerminalConnectionError | None = None
        self._rk_close_task: asyncio.Task[None] | None = None
        _fire(rk_policy.on_instance, self, "on_instance")

    @property
    def terminal_error(self) -> TerminalConnectionError | None:
        """Why this connection stopped reconnecting for good, else ``None``."""
        return self._rk_terminal

    @property
    def current_endpoint(self) -> str:
        host, port = self._rk_policy.endpoints[self._rk_endpoint_index]
        return f"{host}:{port}"

    async def _on_connected(self) -> None:
        # Raises if aio-pika could not restore the channels; that close goes
        # through _on_connection_close as a failed attempt.
        await super()._on_connected()
        outage_started = self._rk_outage_started
        restored = outage_started is not None
        if outage_started is not None:
            logger.info(
                "Reconnected %s connection to %s after %d failed attempt(s) in %.1fs",
                self._rk_policy.role,
                self.current_endpoint,
                self._rk_failures,
                time.monotonic() - outage_started,
            )
        self._rk_connected_once = True
        self._rk_outage_started = None
        self._rk_failures = 0
        self.reconnect_interval = self._rk_steady_interval
        if restored:
            _fire(self._rk_policy.on_restored, self._rk_policy.role, "on_restored")

    async def _on_connection_close(self, closing: Any) -> None:
        # Runs on the loss of a live connection AND after every failed connect
        # attempt (aio-pika's UnderlayConnection.connect calls it before
        # re-raising), always before aio-pika reads reconnect_interval.
        if self._rk_connected_once and self._rk_terminal is None and not (self._close_called or self.is_closed):
            self._rk_on_failure(_closing_error(closing))
        await super()._on_connection_close(closing)

    def _rk_on_failure(self, error: BaseException | None) -> None:
        now = time.monotonic()
        if self._rk_outage_started is None:
            # A live connection dropped. aio-pika retries right away; refresh
            # the credentials first so a rotated password is used.
            self._rk_outage_started = now
            logger.warning(
                "%s connection to %s lost (%r); reconnecting", self._rk_policy.role, self.current_endpoint, error
            )
            self._rk_prepare_attempt(advance=False)
            on_lost = self._rk_policy.on_lost
            if on_lost is not None:
                try:
                    on_lost(self._rk_policy.role, error)
                except Exception:  # never let a hook break aio-pika's reconnect loop
                    logger.exception("on_lost hook raised")
            return

        self._rk_failures += 1
        elapsed = now - self._rk_outage_started
        endpoint = self.current_endpoint
        config = self._rk_policy.config
        permanent = None if error is None else permanent_connection_error(error, endpoint=endpoint, vhost=config.vhost)
        if permanent is not None:
            self._rk_give_up(permanent, error)
            return
        budget_attempts = config.reconnect_budget_attempts
        budget_seconds = config.reconnect_budget_seconds
        if (budget_attempts is not None and self._rk_failures >= budget_attempts) or (
            budget_seconds is not None and elapsed >= budget_seconds
        ):
            exhausted = ReconnectBudgetExhaustedError(
                f"Gave up reconnecting to RabbitMQ after {self._rk_failures} failed attempt(s) in "
                f"{elapsed:.1f}s (reconnect_budget_attempts={budget_attempts}, "
                f"reconnect_budget_seconds={budget_seconds}); last endpoint {endpoint}.",
                reason="reconnect_budget_exhausted",
                endpoint=endpoint,
            )
            self._rk_give_up(exhausted, error)
            return

        delay = backoff_delay(self._rk_failures, config.reconnect_backoff_base, config.reconnect_backoff_max)
        if budget_seconds is not None:
            delay = min(delay, max(0.0, budget_seconds - elapsed))
        self.reconnect_interval = delay
        self._rk_prepare_attempt(advance=True)
        logger.warning(
            "%s connection: reconnect attempt %d to %s failed (%r); next attempt to %s in %.2fs",
            self._rk_policy.role,
            self._rk_failures,
            endpoint,
            error,
            self.current_endpoint,
            delay,
        )
        _fire(
            self._rk_policy.on_attempt,
            ReconnectAttempt(
                role=self._rk_policy.role,
                attempt=self._rk_failures,
                delay=delay,
                elapsed=elapsed,
                endpoint=endpoint,
                error=error,
            ),
            "on_reconnect_attempt",
        )

    def _rk_prepare_attempt(self, *, advance: bool) -> None:
        """Point ``self.url`` (what aio-pika connects to) at the next endpoint
        with freshly resolved credentials."""
        policy = self._rk_policy
        if advance:
            self._rk_endpoint_index = (self._rk_endpoint_index + 1) % len(policy.endpoints)
        host, port = policy.endpoints[self._rk_endpoint_index]
        url = self.url.with_host(host).with_port(port)
        try:
            user, password = policy.config.resolve_credentials()
        except Exception as exc:  # user code: keep the previous credentials, never log the secret
            logger.warning(
                "credentials_provider raised %s; reconnecting with the previous credentials",
                type(exc).__name__,
            )
        else:
            url = url.with_user(user).with_password(password)
        self.url = url

    def _rk_give_up(self, error: TerminalConnectionError, cause: BaseException | None) -> None:
        error.__cause__ = cause
        error.role = self._rk_policy.role
        self._rk_terminal = error
        logger.error("Giving up on the %s connection to RabbitMQ: %s", self._rk_policy.role, error)
        # This hook can run inside aio-pika's reconnect task, which close()
        # cancels and awaits, so close from a task of its own.
        self._rk_close_task = asyncio.get_running_loop().create_task(self._rk_close_for_good(error))
        _fire(self._rk_policy.on_terminal, error, "on_connection_failed")

    async def _rk_close_for_good(self, error: TerminalConnectionError) -> None:
        with contextlib.suppress(Exception):
            async with asyncio.timeout(_CLOSE_TIMEOUT):
                await self.close(error)


__all__ = [
    "RabbitkitRobustConnection",
    "ReconnectAttempt",
    "ReconnectPolicy",
    "backoff_delay",
]
