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
from rabbitkit.core.errors import (
    AuthenticationFailedError,
    PermanentConnectionError,
    TLSVerificationError,
    VhostAccessRefusedError,
)

logger = logging.getLogger(__name__)


# ── Transport-specific connection errors ──────────────────────────────────


def get_infrastructure_errors() -> tuple[type[BaseException], ...]:
    """aio-pika / aiormq errors that mean "the broker (a dependency) is
    unavailable": connection loss and channel closure.

    Registered with :func:`rabbitkit.core.errors.register_infrastructure_errors`
    by :class:`~rabbitkit.async_.broker.AsyncBroker`, so a handler that hits
    one (e.g. while publishing) is classified ``INFRASTRUCTURE`` and retried
    instead of dead-lettered. ``ChannelClosed`` is aiormq's base for every
    channel-level close, so a 403/404/406 raised inside a handler is retried
    too (bounded by the route's ``RetryConfig``). Empty when aio-pika is not
    installed.
    """
    try:
        import aiormq.exceptions
    except ImportError:
        return ()
    return (
        aiormq.exceptions.AMQPConnectionError,
        aiormq.exceptions.ChannelClosed,
        aiormq.exceptions.ChannelInvalidStateError,
    )


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


# ── Permanent connect failures ─────────────────────────────────────

# OpenSSL reasons for a handshake that fails the same way on every retry: the
# broker rejected the client certificate, no protocol/cipher in common, or the
# port does not speak TLS at all. Transient handshake failures (a reset or EOF
# while the broker restarts) carry other reasons and stay retryable.
_PERMANENT_TLS_REASONS = frozenset(
    {
        "TLSV1_ALERT_UNKNOWN_CA",
        "TLSV1_ALERT_ACCESS_DENIED",
        "TLSV1_ALERT_PROTOCOL_VERSION",
        "TLSV13_ALERT_CERTIFICATE_REQUIRED",
        "SSLV3_ALERT_BAD_CERTIFICATE",
        "SSLV3_ALERT_CERTIFICATE_EXPIRED",
        "SSLV3_ALERT_CERTIFICATE_REVOKED",
        "SSLV3_ALERT_CERTIFICATE_UNKNOWN",
        "SSLV3_ALERT_UNSUPPORTED_CERTIFICATE",
        "SSLV3_ALERT_HANDSHAKE_FAILURE",
        "WRONG_VERSION_NUMBER",
        "UNSUPPORTED_PROTOCOL",
        "NO_PROTOCOLS_AVAILABLE",
    }
)


def _connection_open_refusal(exc: BaseException) -> tuple[int | None, str] | None:
    """``(reply_code, reply_text)`` if *exc* is the broker refusing ``connection.open``.

    RabbitMQ answers ``connection.open`` for a missing vhost, or one the user
    has no permissions on, with ``connection.close`` 530 NOT_ALLOWED. aiormq
    checks the frame type first, so this surfaces as a frame-type error, not
    as ``ConnectionClosed``: aiormq 6 raises ``AMQPInternalError("one of
    ['Connection.OpenOk']", <Close frame>)``, aiormq 7 raises
    ``InvalidFrameError`` whose message names both frames (and loses the reply
    code, hence ``None``).
    """
    first = str(exc.args[0]) if exc.args else ""
    for arg in exc.args[1:]:
        if getattr(arg, "name", None) == "Connection.Close" and "Connection.OpenOk" in first:
            return getattr(arg, "reply_code", None), str(getattr(arg, "reply_text", "") or "")
    if "Connection.OpenOk" in first and "Connection.Close" in first:
        return None, ""
    return None


# aiormq 7 wraps a transport OSError (including ssl errors) in
# AMQPConnectionError(...) from the original: look a few causes deep.
_MAX_CAUSE_DEPTH = 4


def permanent_failure_reason(exc: BaseException) -> str | None:
    """Reason code if a connect attempt failed in a way retrying cannot fix.

    ``"authentication_failed"``, ``"vhost_access_refused"``,
    ``"tls_verification_failed"``, ``"tls_handshake_rejected"`` or ``None``
    (transient: refused, reset, timeout, broker shutting down, ...). A
    ``NOT_ALLOWED`` that says the vhost "is down" (its process is restarting)
    stays transient. *exc* and its ``__cause__`` chain are inspected.
    """
    found = _find_permanent(exc)
    return None if found is None else found[0]


def _find_permanent(exc: BaseException) -> tuple[str, BaseException] | None:
    current: BaseException | None = exc
    for _ in range(_MAX_CAUSE_DEPTH):
        if current is None:
            return None
        reason = _permanent_reason(current)
        if reason is not None:
            return reason, current
        current = current.__cause__
    return None


def _permanent_reason(exc: BaseException) -> str | None:
    if isinstance(exc, ssl.SSLCertVerificationError):
        return "tls_verification_failed"
    if isinstance(exc, ssl.SSLError) and getattr(exc, "reason", None) in _PERMANENT_TLS_REASONS:
        return "tls_handshake_rejected"
    try:
        from aiormq.exceptions import AuthenticationError, ConnectionClosed, ProbableAuthenticationError
    except ImportError:  # pragma: no cover — aiormq ships with aio-pika
        return None
    if isinstance(exc, (ProbableAuthenticationError, AuthenticationError)):
        return "authentication_failed"
    if isinstance(exc, ConnectionClosed) and exc.args and exc.args[0] == 403:
        return "authentication_failed"
    refusal = _connection_open_refusal(exc)
    if refusal is not None:
        code, text = refusal
        if code in (None, 530) and "is down" not in text.lower():
            return "vhost_access_refused"
    return None


def permanent_connection_error(exc: BaseException, *, endpoint: str, vhost: str) -> PermanentConnectionError | None:
    """The typed, actionable error for a permanent connect failure, else ``None``.

    Messages name the endpoint and vhost, never the username or password.
    The caller chains *exc* as the cause.
    """
    found = _find_permanent(exc)
    if found is None:
        return None
    reason, exc = found
    if reason == "authentication_failed":
        return AuthenticationFailedError(
            f"RabbitMQ at {endpoint} refused the login (ACCESS_REFUSED). Check the username and "
            "password (or what ConnectionConfig.credentials_provider returns); retrying with the "
            "same credentials cannot succeed.",
            reason=reason,
            endpoint=endpoint,
        )
    if reason == "vhost_access_refused":
        return VhostAccessRefusedError(
            f"RabbitMQ at {endpoint} refused access to vhost {vhost!r} (NOT_ALLOWED): the vhost does "
            "not exist or the user has no permissions on it. Create the vhost or grant the user "
            "permissions (rabbitmqctl set_permissions).",
            reason=reason,
            endpoint=endpoint,
        )
    if reason == "tls_verification_failed":
        detail = getattr(exc, "verify_message", None) or type(exc).__name__
        return TLSVerificationError(
            f"TLS certificate verification failed for {endpoint}: {detail}. Check "
            "SSLConfig.ca_certs (the CA that signed the broker certificate) and "
            "SSLConfig.server_hostname (the name in the certificate).",
            reason=reason,
            endpoint=endpoint,
        )
    # tls_handshake_rejected
    return TLSVerificationError(
        f"TLS handshake with {endpoint} failed ({getattr(exc, 'reason', None)}): the broker "
        "rejected the client certificate, no TLS version is shared, or the port does not "
        "speak TLS. Check SSLConfig and the broker's listener.",
        reason=reason,
        endpoint=endpoint,
    )


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

    # H4 / this is only aio-pika's STEADY-STATE interval: its pause
    # after a successful (re)connect and between consumer-restore retries.
    # The delay after each FAILED reconnect attempt is set per attempt by
    # rabbitkit's RobustConnection subclass (async_/reconnect.py): exponential
    # backoff with full jitter. The steady interval is still randomized per
    # process over [base, min(base*2, backoff_max)] so a fleet restarted
    # together does not pause in lockstep.
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
    # user "capabilities". aio-pika 9.x drops connect_robust(client_properties=)
    # when given a URL; the pool also hands them to RabbitkitRobustConnection
    # (rk_client_properties), which sets them where aiormq reads them on
    # every aio-pika version.
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
    return any(isinstance(v, int) and not isinstance(v, bool) and -128 <= v < 0 for v in (arguments or {}).values())


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


_BLOCKED_REASON_ATTR = "_rabbitkit_blocked_reason"


def ensure_aiormq_records_blocked_reason() -> bool:
    """Make aiormq keep the ``connection.blocked`` reason it receives.

    aiormq (6.x and 7.x) only LOGS ``Connection.Blocked.reason`` (``"low on
    disk"`` / ``"low on memory"``) before clearing its private unblocked
    event, so the reason that tells a disk alarm from a memory alarm is lost.
    This wraps the private ``Connection.__handle_connection_blocked`` at
    class level to store ``frame.reason`` on the connection first, then runs
    the original. Class level, before the connection exists, because aiormq
    6 binds the handler into a dispatch table when the reader starts.
    Idempotent. Returns False (and changes nothing) when aiormq has no such
    handler; :func:`aiormq_blocked_reason` then reports ``None``.
    """
    from aiormq.connection import Connection

    name = "_Connection__handle_connection_blocked"
    original = getattr(Connection, name, None)
    if original is None:
        return False
    if getattr(original, "_rabbitkit_records_reason", False):
        return True

    async def handle_connection_blocked(self: Any, frame: Any) -> None:
        reason = getattr(frame, "reason", None)
        setattr(self, _BLOCKED_REASON_ATTR, reason if isinstance(reason, str) else None)
        await original(self, frame)

    handle_connection_blocked._rabbitkit_records_reason = True  # type: ignore[attr-defined]
    setattr(Connection, name, handle_connection_blocked)
    return True


def aiormq_blocked_reason(connection: Any) -> str | None:
    """The last ``connection.blocked`` reason the live aiormq connection under
    an aio-pika *connection* received, or ``None`` (none received, no live
    transport, or :func:`ensure_aiormq_records_blocked_reason` not active)."""
    underlay = getattr(getattr(connection, "transport", None), "connection", None)
    reason = getattr(underlay, _BLOCKED_REASON_ATTR, None)
    return reason if isinstance(reason, str) else None


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
        self._blocked_reason: str | None = None
        self._blocked_since: float | None = None
        self._underlay: Any = None
        self._task: asyncio.Task[None] | None = None
        self.forced_reconnects = 0

    @property
    def is_blocked(self) -> bool:
        return self._blocked

    @property
    def blocked_reason(self) -> str | None:
        """The broker's blocked reason while :attr:`is_blocked`, else ``None``."""
        return self._blocked_reason if self._blocked else None

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
            self._blocked_reason = aiormq_blocked_reason(self._connection)
            self._blocked_since = now
            if self._blocked_timeout > 0:
                logger.warning(
                    "Connection blocked by RabbitMQ; forcing a reconnect in %.1fs if not unblocked",
                    self._blocked_timeout,
                )
            else:
                logger.warning("Connection blocked by RabbitMQ (resource alarm: %s)", self._blocked_reason)
            self._fire(self._on_blocked)
        elif not blocked and self._blocked:
            self._blocked = False
            self._blocked_reason = None
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
