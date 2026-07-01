"""aio-pika-specific connection parameter builders and error tuples.

This is where all aio-pika imports live — core/ stays clean.
Provides helpers to build aio_pika.connect_robust() kwargs from rabbitkit config objects.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Any
from urllib.parse import quote

from rabbitkit.core.config import ConnectionConfig, SecurityConfig, SSLConfig

logger = logging.getLogger(__name__)


# ── Transport-specific connection errors ──────────────────────────────────


def get_connection_errors() -> tuple[type[BaseException], ...]:
    """Get aio-pika-specific connection error tuple.

    Returns generic stdlib errors if aio-pika is not installed.
    """
    base_errors: tuple[type[BaseException], ...] = (
        ConnectionResetError,
        BrokenPipeError,
        ConnectionAbortedError,
        ConnectionRefusedError,
        TimeoutError,
        EOFError,
        OSError,
    )

    try:
        import aio_pika.exceptions

        aio_pika_errors: tuple[type[BaseException], ...] = (
            aio_pika.exceptions.AMQPConnectionError,
            aio_pika.exceptions.ChannelClosed,
            aio_pika.exceptions.ConnectionClosed,
        )
        return aio_pika_errors + base_errors
    except (ImportError, AttributeError):
        return base_errors


def build_ssl_context(ssl_config: SSLConfig) -> ssl.SSLContext | None:
    """Build stdlib ssl.SSLContext from SSLConfig.

    Returns None if SSL is not enabled.
    Shared with sync/connection.py — same logic.
    """
    if not ssl_config.enabled:
        return None

    cert_reqs_map = {
        "CERT_REQUIRED": ssl.CERT_REQUIRED,
        "CERT_OPTIONAL": ssl.CERT_OPTIONAL,
        "CERT_NONE": ssl.CERT_NONE,
    }
    cert_reqs = cert_reqs_map.get(ssl_config.cert_reqs, ssl.CERT_REQUIRED)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    # Defense in depth: never negotiate below TLS 1.2.
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2

    # check_hostname must be disabled BEFORE setting verify_mode=CERT_NONE
    if cert_reqs == ssl.CERT_NONE:
        ctx.check_hostname = False

    ctx.verify_mode = cert_reqs

    if ssl_config.ca_certs:
        ctx.load_verify_locations(ssl_config.ca_certs)
    elif cert_reqs == ssl.CERT_REQUIRED:
        # No explicit CA bundle configured — fall back to the system trust
        # store so verification actually succeeds against broker certs
        # signed by a well-known CA. Guarded so the explicit-ca_certs path
        # above is unchanged.
        try:
            ctx.load_default_certs()
        except Exception:  # pragma: no cover — best effort, platform-dependent
            try:
                ctx.set_default_verify_paths()
            except Exception:  # pragma: no cover
                pass

    if ssl_config.certfile:
        ctx.load_cert_chain(
            certfile=ssl_config.certfile,
            keyfile=ssl_config.keyfile,
        )

    return ctx


def make_aio_pika_connect_kwargs(
    connection: ConnectionConfig,
    security: SecurityConfig,
) -> dict[str, Any]:
    """Build kwargs for aio_pika.connect_robust().

    Returns a dict of keyword arguments.
    Raises ImportError if aio-pika is not installed.

    Note: aio-pika has no native ``blocked_connection_timeout`` knob (unlike
    pika's ``ConnectionParameters.blocked_connection_timeout``). To honour
    ``ConnectionConfig.blocked_connection_timeout`` on the async side, call
    :func:`install_blocked_connection_watchdog` on the returned connection
    after ``connect_robust`` succeeds. That helper drives a timer task that
    closes the connection when a ``connection.blocked`` alarm is not cleared
    by ``connection.unblocked`` within the configured timeout, forcing a
    reconnect instead of stalling publishes indefinitely.
    """
    try:
        import aio_pika  # noqa: F401
    except ImportError:
        raise ImportError(
            "aio-pika is required for async transport. Install it with: pip install rabbitkit[async]"
        ) from None

    # Build URL — carry heartbeat as a query param. aio-pika/aiormq read heartbeat
    # from the URL; passing it as a kwarg is not portable across versions. Without
    # this the configured ConnectionConfig.heartbeat was silently dropped on async
    # (the sync transport already honors it).
    #
    # URL-encode username/password so credentials containing special characters
    # (e.g. ":", "@", "/", "+") don't corrupt the AMQP URL or leak as plaintext
    # delimiters. ConnectionConfig.url (core/config.py) is NOT in this module's
    # scope, so we rebuild a safe URL here.
    user = quote(connection.username, safe="")
    pwd = quote(connection.password, safe="")
    vhost = connection.vhost
    if vhost == "/":
        vhost = "%2F"
    base_url = f"amqp://{user}:{pwd}@{connection.host}:{connection.port}/{vhost}"
    sep = "&" if "?" in base_url else "?"
    url = f"{base_url}{sep}heartbeat={connection.heartbeat}"

    kwargs: dict[str, Any] = {
        "url": url,
        "timeout": connection.socket_timeout,
        # connect_robust uses a FIXED reconnect interval; map our backoff base to it.
        # aio-pika has no exponential backoff, so reconnect_backoff_max is not applied here.
        "reconnect_interval": connection.reconnect_backoff_base,
    }

    # SSL
    ssl_context = build_ssl_context(security.ssl)
    if ssl_context is not None:
        kwargs["ssl_context"] = ssl_context

    # Client properties
    if connection.connection_name:
        kwargs["client_properties"] = {
            "connection_name": connection.connection_name,
        }

    return kwargs


async def install_blocked_connection_watchdog(connection: Any, blocked_timeout: float) -> None:
    """Install a watchdog that closes *connection* when a blocked alarm lingers.

    aio-pika has no native ``blocked_connection_timeout`` (I-11), so without
    this a ``connection.blocked`` alarm from RabbitMQ (memory/disk pressure)
    can stall publishes indefinitely while the connection stays "open". This
    helper registers ``connection.connection_blocked`` /
    ``connection.connection_unblocked`` callbacks that drive a timer task:

    - on ``blocked``: start (or replace) a task that sleeps *blocked_timeout*
      then closes the connection (forcing ``connect_robust`` to reconnect).
    - on ``unblocked``: cancel the pending timer so a transient alarm does not
      tear down a recovered connection.

    Safe to call with ``blocked_timeout <= 0`` (no-op) or on a connection
    that does not expose the callback collections (logged at debug, no raise).
    Must be called from the event loop that owns *connection*.
    """
    if blocked_timeout <= 0:
        return

    blocked_cb_collection = getattr(connection, "connection_blocked", None)
    unblocked_cb_collection = getattr(connection, "connection_unblocked", None)
    if blocked_cb_collection is None or unblocked_cb_collection is None:
        logger.debug(
            "connection does not expose connection_blocked/connection_unblocked "
            "callback collections; blocked-connection watchdog not installed"
        )
        return

    loop = asyncio.get_event_loop()
    state: dict[str, asyncio.Task[None] | None] = {"timer": None}

    async def _on_blocked(*_args: Any) -> None:
        # Replace any pending timer with a fresh one.
        existing = state.get("timer")
        if existing is not None and not existing.done():
            existing.cancel()
        logger.warning(
            "Connection blocked by RabbitMQ; will close in %.1fs if not unblocked",
            blocked_timeout,
        )
        state["timer"] = asyncio.ensure_future(_close_after(blocked_timeout))

    async def _on_unblocked(*_args: Any) -> None:
        existing = state.get("timer")
        if existing is not None and not existing.done():
            existing.cancel()
        state["timer"] = None
        logger.info("Connection unblocked; watchdog timer cancelled")

    async def _close_after(delay: float) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        logger.warning("Connection blocked for > %.1fs; closing to force reconnect", delay)
        try:
            close = connection.close
            result = close()
            if hasattr(result, "__await__"):
                await result
        except Exception:  # pragma: no cover — best effort; connect_robust will retry
            logger.debug("watchdog close raised", exc_info=True)

    # aio-pika CallbackCollection.add_callback accepts a coroutine fn.
    try:
        blocked_cb_collection.add_callback(_on_blocked)
        unblocked_cb_collection.add_callback(_on_unblocked)
    except Exception:  # pragma: no cover — defensive across aio-pika versions
        logger.debug("Could not register blocked/unblocked watchdog callbacks", exc_info=True)
        return
    # Keep a reference so the timer is not GC'd and the callbacks are traceable.
    connection._rabbitkit_blocked_watchdog = state
    connection._rabbitkit_blocked_watchdog_loop = loop
