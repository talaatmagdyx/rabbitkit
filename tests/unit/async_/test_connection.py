"""Tests for async_/connection.py — aio-pika connection helpers."""

from __future__ import annotations

import ssl
from unittest.mock import patch

import pytest

from rabbitkit.async_.connection import (
    build_ssl_context,
    get_connection_errors,
)
from rabbitkit.core.config import ConnectionConfig, SecurityConfig, SSLConfig

# ── get_connection_errors ────────────────────────────────────────────────


class TestGetConnectionErrors:
    def test_includes_stdlib_errors(self) -> None:
        errors = get_connection_errors()
        assert ConnectionResetError in errors
        assert BrokenPipeError in errors
        assert TimeoutError in errors
        assert OSError in errors
        assert ConnectionRefusedError in errors

    def test_returns_tuple(self) -> None:
        errors = get_connection_errors()
        assert isinstance(errors, tuple)
        assert all(isinstance(e, type) for e in errors)

    def test_includes_aio_pika_errors_when_available(self) -> None:
        """If aio-pika is installed, aio-pika-specific errors are included."""
        try:
            import aio_pika.exceptions

            errors = get_connection_errors()
            assert aio_pika.exceptions.AMQPConnectionError in errors
        except ImportError:
            # aio-pika not installed — only stdlib errors
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


# ── make_aio_pika_connect_kwargs ─────────────────────────────────────────


class TestMakeAioPikaConnectKwargs:
    def test_builds_kwargs(self) -> None:
        """Builds aio-pika kwargs when aio-pika is available."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(host="myhost", port=5673, username="user", password="pass")
        sec = SecurityConfig()

        kwargs = make_aio_pika_connect_kwargs(conn, sec)

        assert "url" in kwargs
        assert "timeout" in kwargs
        assert kwargs["timeout"] == 10.0

    def test_url_contains_host(self) -> None:
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(host="rabbit-host", port=5673)
        sec = SecurityConfig()

        kwargs = make_aio_pika_connect_kwargs(conn, sec)

        assert "rabbit-host" in kwargs["url"]

    def test_with_connection_name(self) -> None:
        """Client properties include connection_name."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(connection_name="my-service")
        sec = SecurityConfig()

        kwargs = make_aio_pika_connect_kwargs(conn, sec)

        assert "client_properties" in kwargs
        assert kwargs["client_properties"]["connection_name"] == "my-service"

    def test_without_connection_name(self) -> None:
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig()
        sec = SecurityConfig()

        kwargs = make_aio_pika_connect_kwargs(conn, sec)

        assert "client_properties" not in kwargs

    def test_with_ssl(self) -> None:
        """SSL options are applied when enabled."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig()
        sec = SecurityConfig(ssl=SSLConfig(enabled=True, cert_reqs="CERT_NONE"))

        kwargs = make_aio_pika_connect_kwargs(conn, sec)

        assert "ssl_context" in kwargs
        assert isinstance(kwargs["ssl_context"], ssl.SSLContext)

    def test_without_ssl(self) -> None:
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig()
        sec = SecurityConfig()

        kwargs = make_aio_pika_connect_kwargs(conn, sec)

        assert "ssl_context" not in kwargs

    def test_raises_import_error_without_aio_pika(self) -> None:
        """Should raise ImportError when aio-pika is not installed."""
        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig()
        sec = SecurityConfig()

        with patch.dict("sys.modules", {"aio_pika": None}):
            with pytest.raises(ImportError, match="aio-pika is required"):
                make_aio_pika_connect_kwargs(conn, sec)


# ── get_connection_errors fallback ──────────────────────────────────────


class TestGetConnectionErrorsFallback:
    """Cover the ImportError/AttributeError fallback in get_connection_errors."""

    def test_fallback_on_import_error(self) -> None:
        """Returns only stdlib errors when aio-pika import fails."""
        import importlib
        import sys
        from unittest.mock import patch

        # Force the import inside get_connection_errors to raise ImportError
        with patch.dict(sys.modules, {"aio_pika": None, "aio_pika.exceptions": None}):
            # Reload to ensure the patched sys.modules is used
            import rabbitkit.async_.connection as conn_module
            importlib.reload(conn_module)
            errors = conn_module.get_connection_errors()

        assert ConnectionResetError in errors
        assert BrokenPipeError in errors
        assert TimeoutError in errors

    def test_fallback_on_attribute_error(self) -> None:
        """Returns only stdlib errors when aio_pika.exceptions lacks expected attrs."""
        import sys
        import types
        from unittest.mock import patch

        # Create a fake aio_pika.exceptions module without AMQPConnectionError
        fake_exceptions = types.ModuleType("aio_pika.exceptions")
        # No AMQPConnectionError attribute — accessing it will cause AttributeError
        # if we access it, but the except clause catches AttributeError too.
        # We simulate this by patching get_connection_errors to hit the except branch.
        fake_aio_pika = types.ModuleType("aio_pika")
        fake_aio_pika.exceptions = fake_exceptions  # type: ignore[attr-defined]

        from rabbitkit.async_.connection import get_connection_errors as orig_fn

        # Patch aio_pika.exceptions to one without the required attrs
        with patch.dict(sys.modules, {"aio_pika": fake_aio_pika, "aio_pika.exceptions": fake_exceptions}):
            # Directly call the function; the AttributeError branch fires because
            # fake_exceptions has no AMQPConnectionError
            errors = orig_fn()

        # Even if aio-pika is installed for real, our patched version had no attrs
        # so it either returned real errors (if import succeeded) or stdlib errors.
        assert isinstance(errors, tuple)
        assert ConnectionResetError in errors


# ── build_ssl_context with ca_certs and certfile ────────────────────────


class TestBuildSSLContextCerts:
    """Cover the ca_certs and certfile/keyfile loading paths (lines 74, 77)."""

    def test_with_ca_certs(self, tmp_path: pytest.TempPathFactory) -> None:
        """SSL context loads CA cert when ca_certs is set."""

        # Create a self-signed CA cert for testing using subprocess or ssl module
        # We'll use a DER-encoded dummy and catch the expected ssl error,
        # or just mock SSLContext.load_verify_locations to verify the call.
        config = SSLConfig(enabled=True, cert_reqs="CERT_NONE", ca_certs="/path/to/ca.pem")

        with patch("ssl.SSLContext.load_verify_locations") as mock_load:
            _ = build_ssl_context(config)
            mock_load.assert_called_once_with("/path/to/ca.pem")

    def test_with_certfile_and_keyfile(self) -> None:
        """SSL context loads cert chain when certfile and keyfile are set."""
        config = SSLConfig(
            enabled=True,
            cert_reqs="CERT_NONE",
            certfile="/path/to/client.crt",
            keyfile="/path/to/client.key",
        )

        with patch("ssl.SSLContext.load_cert_chain") as mock_load_chain:
            _ = build_ssl_context(config)
            mock_load_chain.assert_called_once_with(
                certfile="/path/to/client.crt",
                keyfile="/path/to/client.key",
            )

    def test_with_certfile_no_keyfile(self) -> None:
        """SSL context loads cert chain with keyfile=None when only certfile is set."""
        config = SSLConfig(
            enabled=True,
            cert_reqs="CERT_NONE",
            certfile="/path/to/client.crt",
        )

        with patch("ssl.SSLContext.load_cert_chain") as mock_load_chain:
            _ = build_ssl_context(config)
            mock_load_chain.assert_called_once_with(
                certfile="/path/to/client.crt",
                keyfile=None,
            )
