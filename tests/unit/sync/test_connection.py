"""Tests for sync/connection.py — connection helpers."""

from __future__ import annotations

import socket
import ssl
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.config import ConnectionConfig, SecurityConfig, SocketConfig, SSLConfig
from rabbitkit.sync.connection import (
    apply_socket_options,
    build_ssl_context,
    get_connection_errors,
)

# ── get_connection_errors ────────────────────────────────────────────────


class TestGetConnectionErrors:
    def test_includes_stdlib_errors(self) -> None:
        errors = get_connection_errors()
        assert ConnectionResetError in errors
        assert BrokenPipeError in errors
        assert TimeoutError in errors
        assert OSError in errors

    def test_returns_tuple(self) -> None:
        errors = get_connection_errors()
        assert isinstance(errors, tuple)
        assert all(isinstance(e, type) for e in errors)

    def test_includes_pika_errors_when_available(self) -> None:
        """If pika is installed, pika-specific errors are included."""
        try:
            import pika.exceptions

            errors = get_connection_errors()
            assert pika.exceptions.AMQPConnectionError in errors
        except ImportError:
            # pika not installed — only stdlib errors
            errors = get_connection_errors()
            assert len(errors) >= 7  # at least the stdlib errors


# ── build_ssl_context ────────────────────────────────────────────────────


class TestBuildSSLContext:
    def test_disabled_returns_none(self) -> None:
        config = SSLConfig(enabled=False)
        assert build_ssl_context(config) is None

    def test_enabled_returns_context(self) -> None:
        config = SSLConfig(enabled=True, cert_reqs="CERT_NONE")
        ctx = build_ssl_context(config)
        assert ctx is not None
        assert isinstance(ctx, ssl.SSLContext)

    def test_cert_none_disables_hostname_check(self) -> None:
        config = SSLConfig(enabled=True, cert_reqs="CERT_NONE")
        ctx = build_ssl_context(config)
        assert ctx is not None
        assert ctx.check_hostname is False

    def test_cert_required_default(self) -> None:
        config = SSLConfig(enabled=True, cert_reqs="CERT_REQUIRED")
        ctx = build_ssl_context(config)
        assert ctx is not None
        assert ctx.verify_mode == ssl.CERT_REQUIRED


# ── apply_socket_options ─────────────────────────────────────────────────


class TestApplySocketOptions:
    def test_applies_tcp_nodelay(self) -> None:
        sock = MagicMock(spec=socket.socket)
        config = SocketConfig(tcp_nodelay=True)

        apply_socket_options(sock, config)

        # Should call setsockopt with TCP_NODELAY
        calls = sock.setsockopt.call_args_list
        nodelay_calls = [
            c for c in calls
            if c[0][0] == socket.IPPROTO_TCP and c[0][1] == socket.TCP_NODELAY
        ]
        assert len(nodelay_calls) == 1
        assert nodelay_calls[0][0][2] == 1

    def test_applies_keepalive(self) -> None:
        sock = MagicMock(spec=socket.socket)
        config = SocketConfig()

        apply_socket_options(sock, config)

        # Should enable SO_KEEPALIVE
        keepalive_calls = [
            c for c in sock.setsockopt.call_args_list
            if c[0][0] == socket.SOL_SOCKET and c[0][1] == socket.SO_KEEPALIVE
        ]
        assert len(keepalive_calls) == 1
        assert keepalive_calls[0][0][2] == 1

    def test_applies_buffer_sizes(self) -> None:
        sock = MagicMock(spec=socket.socket)
        config = SocketConfig(tcp_sndbuf=65536, tcp_rcvbuf=65536)

        apply_socket_options(sock, config)

        sndbuf_calls = [
            c for c in sock.setsockopt.call_args_list
            if c[0][0] == socket.SOL_SOCKET and c[0][1] == socket.SO_SNDBUF
        ]
        assert len(sndbuf_calls) == 1
        assert sndbuf_calls[0][0][2] == 65536

    def test_handles_oserror_gracefully(self) -> None:
        sock = MagicMock(spec=socket.socket)
        sock.setsockopt.side_effect = OSError("not supported")
        config = SocketConfig()

        # Should not raise
        apply_socket_options(sock, config)


# ── make_pika_connection_params ──────────────────────────────────────────


class TestMakePikaConnectionParams:
    def test_builds_params(self) -> None:
        """Builds pika params when pika is available."""
        try:
            import pika
        except ImportError:
            return  # skip if pika not installed

        from rabbitkit.sync.connection import make_pika_connection_params

        conn = ConnectionConfig(host="myhost", port=5673, username="user", password="pass")
        sock = SocketConfig()
        sec = SecurityConfig()

        params = make_pika_connection_params(conn, sock, sec)

        assert isinstance(params, pika.ConnectionParameters)
        assert params.host == "myhost"
        assert params.port == 5673

    def test_with_connection_name(self) -> None:
        """Client properties include connection_name."""
        pytest.importorskip("pika")

        from rabbitkit.sync.connection import make_pika_connection_params

        conn = ConnectionConfig(connection_name="my-service")
        sock = SocketConfig()
        sec = SecurityConfig()

        params = make_pika_connection_params(conn, sock, sec)

        assert params.client_properties is not None
        assert params.client_properties["connection_name"] == "my-service"

    def test_with_ssl(self) -> None:
        """SSL options are applied when enabled."""
        pytest.importorskip("pika")

        from rabbitkit.sync.connection import make_pika_connection_params

        conn = ConnectionConfig()
        sock = SocketConfig()
        sec = SecurityConfig(ssl=SSLConfig(enabled=True, cert_reqs="CERT_NONE"))

        params = make_pika_connection_params(conn, sock, sec)

        assert params.ssl_options is not None

    def test_raises_import_error_when_pika_missing(self) -> None:
        """ImportError raised when pika is not installed."""
        import sys
        from unittest.mock import patch


        conn = ConnectionConfig()
        sock = SocketConfig()
        sec = SecurityConfig()

        with patch.dict(sys.modules, {"pika": None}):
            import importlib

            import rabbitkit.sync.connection as conn_mod
            importlib.reload(conn_mod)
            with pytest.raises(ImportError, match="pika is required"):
                conn_mod.make_pika_connection_params(conn, sock, sec)

        # Reload back to normal
        importlib.reload(conn_mod)


# ── get_connection_errors without pika ──────────────────────────────────


class TestGetConnectionErrorsNoPika:
    def test_returns_base_errors_when_pika_missing(self) -> None:
        """Falls back to stdlib errors if pika is not importable."""
        import importlib
        import sys
        from unittest.mock import patch

        with patch.dict(sys.modules, {"pika": None, "pika.exceptions": None}):
            import rabbitkit.sync.connection as conn_mod
            importlib.reload(conn_mod)
            errors = conn_mod.get_connection_errors()

        assert ConnectionResetError in errors
        assert BrokenPipeError in errors
        assert TimeoutError in errors
        assert OSError in errors
        # Reload so other tests are not affected
        importlib.reload(conn_mod)


# ── build_ssl_context with ca_certs and certfile ────────────────────────


class TestBuildSSLContextExtended:
    def test_ssl_with_certfile(self) -> None:
        """load_cert_chain is called when certfile is set."""
        from unittest.mock import MagicMock, patch

        config = SSLConfig(
            enabled=True,
            cert_reqs="CERT_NONE",
            certfile="/path/to/cert.pem",
            keyfile="/path/to/key.pem",
        )

        mock_ctx = MagicMock()
        with patch("ssl.SSLContext", return_value=mock_ctx):
            build_ssl_context(config)

        mock_ctx.load_cert_chain.assert_called_once_with(
            certfile="/path/to/cert.pem",
            keyfile="/path/to/key.pem",
        )

    def test_ssl_with_ca_certs(self) -> None:
        """load_verify_locations is called when ca_certs is set."""
        from unittest.mock import MagicMock, patch

        config = SSLConfig(
            enabled=True,
            cert_reqs="CERT_NONE",
            ca_certs="/path/to/ca.pem",
        )

        mock_ctx = MagicMock()
        with patch("ssl.SSLContext", return_value=mock_ctx):
            build_ssl_context(config)

        mock_ctx.load_verify_locations.assert_called_once_with("/path/to/ca.pem")


# ── apply_socket_options — platform-specific keepalive ──────────────────


class TestApplySocketOptionsKeepalive:
    def test_tcp_keepidle_applied_when_available(self) -> None:
        """TCP_KEEPIDLE setsockopt called when socket module has it."""
        import socket as socket_mod
        from unittest.mock import MagicMock, patch

        sock = MagicMock(spec=socket_mod.socket)
        config = SocketConfig(tcp_keepidle=60, tcp_keepintvl=10, tcp_keepcnt=5)

        # Ensure socket has TCP_KEEPIDLE
        with patch.object(socket_mod, "TCP_KEEPIDLE", 4, create=True), \
             patch.object(socket_mod, "TCP_KEEPINTVL", 5, create=True), \
             patch.object(socket_mod, "TCP_KEEPCNT", 6, create=True):
            apply_socket_options(sock, config)

        calls = sock.setsockopt.call_args_list
        keepidle_calls = [
            c for c in calls
            if c[0][0] == socket_mod.IPPROTO_TCP and c[0][1] == 4  # TCP_KEEPIDLE value
        ]
        assert len(keepidle_calls) >= 1
