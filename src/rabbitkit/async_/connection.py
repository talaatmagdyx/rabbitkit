"""aio-pika-specific connection parameter builders and error tuples.

This is where all aio-pika imports live — core/ stays clean.
Provides helpers to build aio_pika.connect_robust() kwargs from rabbitkit config objects.
"""

from __future__ import annotations

import logging
import ssl
from typing import Any

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

    # check_hostname must be disabled BEFORE setting verify_mode=CERT_NONE
    if cert_reqs == ssl.CERT_NONE:
        ctx.check_hostname = False

    ctx.verify_mode = cert_reqs

    if ssl_config.ca_certs:
        ctx.load_verify_locations(ssl_config.ca_certs)

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
    """
    try:
        import aio_pika  # noqa: F401
    except ImportError:
        raise ImportError(
            "aio-pika is required for async transport. "
            "Install it with: pip install rabbitkit[async]"
        ) from None

    # Build URL
    url = connection.url

    kwargs: dict[str, Any] = {
        "url": url,
        "timeout": connection.socket_timeout,
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
