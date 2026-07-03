"""Pika-specific connection parameter builders and error tuples.

This is where all pika imports live — core/ stays clean.
Provides helpers to build pika.ConnectionParameters from rabbitkit config objects.
"""

from __future__ import annotations

import logging
import socket
import ssl
from typing import Any

from rabbitkit.core.config import ConnectionConfig, SecurityConfig, SocketConfig, SSLConfig

logger = logging.getLogger(__name__)


# ── Transport-specific connection errors ──────────────────────────────────
# These extend the core TRANSIENT_ERRORS for pika-specific exceptions.
# Lazy-loaded to avoid import errors when pika is not installed.


def get_connection_errors() -> tuple[type[BaseException], ...]:
    """Get pika-specific connection error tuple.

    Returns generic stdlib errors if pika is not installed.
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
        import pika.exceptions

        pika_errors: tuple[type[BaseException], ...] = (
            pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ChannelClosedByBroker,
            pika.exceptions.AMQPChannelError,
        )
        return pika_errors + base_errors
    except ImportError:
        return base_errors


def build_ssl_context(ssl_config: SSLConfig) -> ssl.SSLContext | None:
    """Build stdlib ssl.SSLContext from SSLConfig.

    Returns None if SSL is not enabled.
    """
    if not ssl_config.enabled:
        return None

    # Determine cert_reqs
    cert_reqs_map = {
        "CERT_REQUIRED": ssl.CERT_REQUIRED,
        "CERT_OPTIONAL": ssl.CERT_OPTIONAL,
        "CERT_NONE": ssl.CERT_NONE,
    }
    cert_reqs = cert_reqs_map.get(ssl_config.cert_reqs, ssl.CERT_REQUIRED)

    # M13: disabling certificate verification makes the connection
    # MITM-able — warn loudly, since it's a copy-paste "make TLS errors go
    # away" footgun that otherwise ships silently to production.
    if cert_reqs == ssl.CERT_NONE:
        import warnings

        warnings.warn(
            "SSLConfig(cert_reqs='CERT_NONE') disables TLS certificate and hostname "
            "verification — the connection is encrypted but MITM-able. Use "
            "'CERT_REQUIRED' (the default) with a proper ca_certs bundle in production.",
            RuntimeWarning,
            stacklevel=2,
        )

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    # Defense in depth: never negotiate below TLS 1.2.
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2

    # check_hostname must be disabled BEFORE setting verify_mode=CERT_NONE
    # (Python 3.12+ raises ValueError otherwise)
    if cert_reqs == ssl.CERT_NONE:
        ctx.check_hostname = False

    ctx.verify_mode = cert_reqs

    if ssl_config.ca_certs:
        ctx.load_verify_locations(ssl_config.ca_certs)
    elif cert_reqs == ssl.CERT_REQUIRED:
        # No explicit CA bundle configured — fall back to the system trust
        # store so verification actually succeeds against broker certs
        # signed by a well-known CA. Without this, CERT_REQUIRED + no ca_certs
        # silently leaves the context with zero trusted CAs → every handshake
        # fails. Guarded so the explicit-ca_certs path above is unchanged.
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


def make_pika_connection_params(
    connection: ConnectionConfig,
    socket_config: SocketConfig,
    security: SecurityConfig,
) -> Any:
    """Build pika.ConnectionParameters with TCP tuning, SSL, heartbeat.

    Returns a pika.ConnectionParameters object.
    Raises ImportError if pika is not installed.
    """
    try:
        import pika
    except ImportError:
        raise ImportError("pika is required for sync transport. Install it with: pip install rabbitkit[sync]") from None

    # SSL context
    ssl_context = build_ssl_context(security.ssl)
    ssl_options = None
    if ssl_context is not None:
        ssl_options = pika.SSLOptions(
            context=ssl_context,
            server_hostname=security.ssl.server_hostname or connection.host,
        )

    # Credentials (M13: resolve via credentials_provider if set, so a rotated
    # secret is picked up on this (re)connect).
    username, password = connection.resolve_credentials()
    credentials = pika.PlainCredentials(
        username=username,
        password=password,
    )

    # Client properties
    client_properties: dict[str, str] = {}
    if connection.connection_name:
        client_properties["connection_name"] = connection.connection_name

    def _params_for(host: str, port: int) -> Any:
        return pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=connection.vhost,
            credentials=credentials,
            heartbeat=connection.heartbeat,
            socket_timeout=connection.socket_timeout,
            blocked_connection_timeout=connection.blocked_connection_timeout,
            ssl_options=ssl_options,
            client_properties=client_properties if client_properties else None,
        )

    endpoints = connection.cluster_endpoints()
    if len(endpoints) == 1:
        return _params_for(*endpoints[0])
    # M9: pika.BlockingConnection accepts a LIST of ConnectionParameters and
    # tries each in order until one connects — native cluster failover.
    return [_params_for(host, port) for host, port in endpoints]


def apply_socket_options(sock: socket.socket, config: SocketConfig) -> None:
    """Apply TCP_NODELAY, keepalive, buffer sizes to a socket.

    Best-effort — not all options are universally guaranteed
    depending on OS and backend internals.
    """
    try:
        # TCP_NODELAY — disable Nagle's algorithm
        if config.tcp_nodelay:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # TCP keepalive
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        # Platform-specific keepalive options
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, config.tcp_keepidle)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, config.tcp_keepintvl)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, config.tcp_keepcnt)

        # Buffer sizes
        if config.tcp_sndbuf > 0:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, config.tcp_sndbuf)
        if config.tcp_rcvbuf > 0:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, config.tcp_rcvbuf)

    except OSError as e:
        logger.warning("Failed to apply socket option: %s (best-effort, continuing)", e)
