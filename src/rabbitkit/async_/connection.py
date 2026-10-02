"""aio-pika-specific connection parameter builders and error tuples.

This is where all aio-pika imports live — core/ stays clean.
Provides helpers to build aio_pika.connect_robust() kwargs from rabbitkit config objects.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import ssl
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

from rabbitkit._version import __version__
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


class _PinnedHostnameSSLContext(ssl.SSLContext):
    """An SSLContext that verifies the peer against a fixed hostname.

    aiormq opens the TLS socket with ``server_hostname=url.host`` and aio-pika
    drops any ``server_hostname`` kwarg, so ``SSLConfig.server_hostname`` (for
    connecting by IP or through a load balancer) can only be applied here:
    asyncio hands the hostname to ``wrap_bio``, which substitutes this one.
    """

    pinned_hostname: str = ""

    def wrap_bio(
        self,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
        server_side: bool = False,
        server_hostname: str | bytes | None = None,
        session: ssl.SSLSession | None = None,
    ) -> ssl.SSLObject:
        return super().wrap_bio(incoming, outgoing, server_side, self.pinned_hostname, session)


def build_ssl_context(ssl_config: SSLConfig) -> ssl.SSLContext | None:
    """Build stdlib ssl.SSLContext from SSLConfig.

    Returns None if SSL is not enabled. Mirrors sync/connection.py, except
    that ``server_hostname`` is applied by the context itself (pika takes it
    through ``SSLOptions``; aio-pika has no equivalent).
    """
    if not ssl_config.enabled:
        return None

    cert_reqs_map = {
        "CERT_REQUIRED": ssl.CERT_REQUIRED,
        "CERT_OPTIONAL": ssl.CERT_OPTIONAL,
        "CERT_NONE": ssl.CERT_NONE,
    }
    cert_reqs = cert_reqs_map.get(ssl_config.cert_reqs, ssl.CERT_REQUIRED)

    # M13: disabling certificate verification makes the connection MITM-able —
    # warn loudly (see sync/connection.py for rationale).
    if cert_reqs == ssl.CERT_NONE:
        import warnings

        warnings.warn(
            "SSLConfig(cert_reqs='CERT_NONE') disables TLS certificate and hostname "
            "verification — the connection is encrypted but MITM-able. Use "
            "'CERT_REQUIRED' (the default) with a proper ca_certs bundle in production.",
            RuntimeWarning,
            stacklevel=2,
        )

    if ssl_config.server_hostname:
        ctx: ssl.SSLContext = _PinnedHostnameSSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.pinned_hostname = ssl_config.server_hostname  # type: ignore[attr-defined]
    else:
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
    *,
    host_override: str | None = None,
    port_override: int | None = None,
) -> dict[str, Any]:
    """Build kwargs for aio_pika.connect_robust().

    Returns a dict of keyword arguments.
    Raises ImportError if aio-pika is not installed.

    Note: aio-pika has no native ``blocked_connection_timeout`` knob (unlike
    pika's ``ConnectionParameters.blocked_connection_timeout``). To honour
    ``ConnectionConfig.blocked_connection_timeout`` on the async side, start a
    :class:`BlockedConnectionMonitor` on the returned connection after
    ``connect_robust`` succeeds. It forces a reconnect when a
    ``connection.blocked`` alarm is not cleared within the configured timeout,
    instead of stalling publishes indefinitely.
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
    # M13: resolve credentials via credentials_provider if set (rotation).
    raw_user, raw_pwd = connection.resolve_credentials()
    user = quote(raw_user, safe="")
    pwd = quote(raw_pwd, safe="")
    # Every vhost is percent-encoded, not just "/": a vhost such as
    # "orders/eu" or "a#b" inserted raw splits the path or starts a fragment,
    # so the client connects to the wrong vhost or fails to parse the URL.
    # Matches ConnectionConfig.url.
    vhost = quote(connection.vhost, safe="")
    # M9: allow the caller (pool) to target a specific cluster node.
    host = host_override if host_override is not None else connection.host
    port = port_override if port_override is not None else connection.port
    # TLS is chosen by the URL SCHEME: aiormq uses its TLS transport only for
    # "amqps" and otherwise ignores ssl_context. With "amqp" here, an
    # SSLConfig(enabled=True) connection spoke plaintext AMQP: it failed
    # against a TLS port and, against a plain port, connected UNENCRYPTED.
    ssl_context = build_ssl_context(security.ssl)
    scheme = "amqps" if ssl_context is not None else "amqp"
    base_url = f"{scheme}://{user}:{pwd}@{host}:{port}/{vhost}"
    sep = "&" if "?" in base_url else "?"
    url = f"{base_url}{sep}heartbeat={connection.heartbeat}"

    # H4: aio-pika's connect_robust uses a FIXED reconnect_interval — no
    # exponential backoff. A fleet of consumers restarted together (e.g. a
    # broker bounce under 200 pods) would otherwise retry in lockstep every
    # `reconnect_backoff_base` seconds, hammering the recovering node in a
    # thundering herd. We can't inject exponential backoff into connect_robust,
    # but randomizing the interval PER PROCESS (full jitter over
    # [base, min(base*2, backoff_max)]) de-synchronizes the herd so retries
    # spread across the window instead of arriving as a spike. Each process
    # picks its interval once at connect time; `random` needs no seeding for
    # inter-process spread since PYTHONHASHSEED/os entropy differ per process.
    base = connection.reconnect_backoff_base
    jitter_span = max(0.0, min(base, connection.reconnect_backoff_max - base))
    jittered_interval = base + random.uniform(0.0, jitter_span)  # noqa: S311 — jitter, not crypto
    kwargs: dict[str, Any] = {
        "url": url,
        "timeout": connection.socket_timeout,
        "reconnect_interval": jittered_interval,
    }

    # SSL
    if ssl_context is not None:
        kwargs["ssl_context"] = ssl_context  # carries SSLConfig.server_hostname, if set

    # Client properties (item 8): rabbitkit always identifies itself;
    # connection_name and any caller-supplied escape-hatch properties
    # (ConnectionConfig.client_properties) are additive on top. aiormq merges
    # this dict into its own base properties with a SHALLOW update (see
    # aiormq.connection.Connection._client_properties), so a same-named key
    # replaces aiormq's: rabbitkit's names avoid "product"/"version"/
    # "platform"/"capabilities"/"information", and ConnectionConfig rejects a
    # user "capabilities". Note aio-pika 9 drops client_properties when given
    # a URL, so these reach the broker only on aio-pika 10+.
    client_properties: dict[str, str] = {
        "library": "rabbitkit",
        "library_version": __version__,
    }
    client_properties.update(connection.client_properties)
    if connection.connection_name:
        client_properties["connection_name"] = connection.connection_name
    kwargs["client_properties"] = client_properties

    return kwargs


def has_small_negative_int(arguments: dict[str, Any] | None) -> bool:
    """True if a field table holds an int pamqp < 4 can't encode (-128..-1)."""
    return any(
        isinstance(v, int) and not isinstance(v, bool) and -128 <= v < 0 for v in (arguments or {}).values()
    )


def ensure_pamqp_encodes_negative_ints() -> None:
    """Fix pamqp 3.x's encoding of field-table integers from -128 to -1.

    pamqp 3 (what aio-pika 9 depends on) tags them as the signed short-short
    type ``b`` but packs them unsigned, so encoding raises
    ``struct.error: 'B' format requires 0 <= number <= 255``. That made a
    quorum queue with ``x-delivery-limit: -1`` impossible to declare on the
    async transport. pamqp 4 packs them signed. This replaces
    ``pamqp.encode.table_integer`` with one that does the same for the
    broken range and defers to the original for everything else; it only
    changes values that raised before. A no-op where pamqp is already fixed.
    """
    import struct

    from pamqp import encode

    if getattr(encode.table_integer, "_rabbitkit_signed_fix", False):
        return
    try:
        encode.table_integer(-1)
        return  # pamqp >= 4
    except struct.error:
        pass

    original = encode.table_integer
    signed = struct.Struct(">b")

    def table_integer(value: int) -> bytes:
        if -128 <= value < 0:
            return b"b" + signed.pack(value)
        return original(value)

    table_integer._rabbitkit_signed_fix = True  # type: ignore[attr-defined]
    encode.table_integer = table_integer


# Poll period for BlockedConnectionMonitor. Reading an asyncio.Event is a few
# attribute lookups, so a quarter second costs nothing and bounds how late
# FlowController hears about an alarm.
BLOCKED_POLL_INTERVAL = 0.25


def aiormq_unblocked_event(connection: Any) -> asyncio.Event | None:
    """Return aiormq's internal "not blocked" event for an aio-pika connection.

    No aio-pika release surfaces ``connection.blocked``: 9.x and 10.x have no
    ``connection_blocked`` / ``connection_unblocked`` callback collections.
    aiormq (6.x and 7.x) handles the frames itself. It clears a private
    ``Connection.__connection_unblocked`` event on ``Connection.Blocked`` and
    sets it on ``Connection.Unblocked``, so that event is the only place the
    state exists. ``None`` when the connection has no live transport
    (mid-reconnect) or aiormq no longer has the attribute.
    """
    underlay = getattr(getattr(connection, "transport", None), "connection", None)
    event = getattr(underlay, "_Connection__connection_unblocked", None)
    return event if isinstance(event, asyncio.Event) else None


class BlockedConnectionMonitor:
    """Track RabbitMQ's ``connection.blocked`` state for one aio-pika connection.

    Polls :func:`aiormq_unblocked_event` and, on each transition, calls
    *on_blocked* / *on_unblocked* (the transport's ``is_blocked`` flag and any
    ``FlowController``). With ``blocked_timeout > 0`` it also forces a
    reconnect when an alarm outlasts the timeout. It does that by closing the
    UNDERLYING aiormq connection, so ``RobustConnection`` reconnects. Calling
    ``RobustConnection.close()`` would shut it down for good.

    *may_force_reconnect* limits the forced reconnect to some connections:
    the transport allows it only on its publisher connection, because closing
    the consumer connection requeues every in-flight delivery.

    The task ends by itself once the connection is closed for good. Call
    :meth:`stop` on shutdown.
    """

    def __init__(
        self,
        connection: Any,
        *,
        blocked_timeout: float,
        on_blocked: Callable[[], None] | None = None,
        on_unblocked: Callable[[], None] | None = None,
        poll_interval: float = BLOCKED_POLL_INTERVAL,
        may_force_reconnect: Callable[[], bool] | None = None,
    ) -> None:
        self._connection = connection
        self._blocked_timeout = blocked_timeout
        self._on_blocked = on_blocked
        self._on_unblocked = on_unblocked
        self._poll_interval = poll_interval
        self._may_force_reconnect = may_force_reconnect
        self._blocked = False
        self._blocked_since: float | None = None
        self._underlay: Any = None
        self._task: asyncio.Task[None] | None = None
        self.forced_reconnects = 0

    @property
    def is_blocked(self) -> bool:
        return self._blocked

    @staticmethod
    def supported(connection: Any) -> bool:
        """False when the connection is live but aiormq has no unblocked event.

        That means an aiormq release this code doesn't know. The monitor
        can't work there, and that should be visible, not a silent no-op.
        """
        underlay = getattr(getattr(connection, "transport", None), "connection", None)
        return underlay is None or aiormq_unblocked_event(connection) is not None

    def start(self) -> bool:
        """Start polling. Returns False (and logs a warning) if unsupported."""
        if not self.supported(self._connection):
            logger.warning(
                "aiormq exposes no connection-blocked state on this version; "
                "is_blocked, FlowController and blocked_connection_timeout are "
                "inactive on the async transport"
            )
            return False
        if self._task is None or self._task.done():
            self._task = asyncio.get_running_loop().create_task(self._run())
        return True

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while not bool(getattr(self._connection, "is_closed", False)):
            await self.poll(loop.time())
            await asyncio.sleep(self._poll_interval)

    async def poll(self, now: float) -> None:
        """One observation. Public so tests can drive it without sleeping."""
        event = aiormq_unblocked_event(self._connection)
        if event is None:
            return  # no live transport (reconnecting): keep the last state
        underlay = self._connection.transport.connection
        if underlay is not self._underlay:
            # A new AMQP connection starts unblocked; any earlier alarm
            # belonged to the old one, so its timer must not carry over.
            self._underlay = underlay
            self._blocked_since = now if self._blocked else None
        blocked = not event.is_set()
        if blocked and not self._blocked:
            self._blocked = True
            self._blocked_since = now
            if self._blocked_timeout > 0:
                logger.warning(
                    "Connection blocked by RabbitMQ; forcing a reconnect in %.1fs if not unblocked",
                    self._blocked_timeout,
                )
            else:
                logger.warning("Connection blocked by RabbitMQ (resource alarm)")
            self._fire(self._on_blocked)
        elif not blocked and self._blocked:
            self._blocked = False
            self._blocked_since = None
            logger.info("Connection unblocked by RabbitMQ")
            self._fire(self._on_unblocked)
        if (
            self._blocked
            and self._blocked_timeout > 0
            and self._blocked_since is not None
            and now - self._blocked_since >= self._blocked_timeout
        ):
            self._blocked_since = None  # one close per alarm, not one per poll
            if self._may_force_reconnect is None or self._may_force_reconnect():
                await self._force_reconnect(underlay)

    async def _force_reconnect(self, underlay: Any) -> None:
        logger.warning(
            "Connection blocked for > %.1fs; closing it so the robust connection reconnects",
            self._blocked_timeout,
        )
        self.forced_reconnects += 1
        try:
            await underlay.close(
                ConnectionError(f"connection blocked by RabbitMQ for more than {self._blocked_timeout:.1f}s")
            )
        except Exception:  # pragma: no cover — best effort; connect_robust still retries
            logger.debug("closing the blocked connection raised", exc_info=True)

    @staticmethod
    def _fire(callback: Callable[[], None] | None) -> None:
        if callback is None:
            return
        try:
            callback()
        except Exception:  # pragma: no cover — never break the poll loop
            logger.exception("blocked/unblocked callback raised")
