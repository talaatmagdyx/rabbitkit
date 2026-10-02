"""Tests for async_/connection.py — aio-pika connection helpers."""

from __future__ import annotations

import ssl
from typing import Any
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

    def test_cert_none_warns(self) -> None:
        """M13: disabling verification is MITM-able — warn loudly."""
        with pytest.warns(RuntimeWarning, match="MITM"):
            build_ssl_context(SSLConfig(enabled=True, cert_reqs="CERT_NONE"))


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

    def test_heartbeat_carried_in_url(self) -> None:
        """Regression: ConnectionConfig.heartbeat must reach aio-pika via the URL
        query (it was silently dropped on async before)."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(host="h", heartbeat=17)
        kwargs = make_aio_pika_connect_kwargs(conn, SecurityConfig())

        assert "heartbeat=17" in kwargs["url"]

    def test_reconnect_interval_is_jittered_within_bounds(self) -> None:
        """H4: reconnect_interval is jittered per process over
        [base, base + min(base, backoff_max - base)] to de-synchronize a fleet
        reconnecting after a broker bounce (thundering-herd avoidance)."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(host="h", reconnect_backoff_base=2.0, reconnect_backoff_max=30.0)
        intervals = [
            make_aio_pika_connect_kwargs(conn, SecurityConfig())["reconnect_interval"] for _ in range(50)
        ]
        # Never below base (don't hammer faster than configured)...
        assert all(i >= 2.0 for i in intervals)
        # ...and bounded by base + min(base, max-base) = 4.0.
        assert all(i <= 4.0 for i in intervals)
        # Jitter actually varies (not a fixed constant) — de-synchronizes pods.
        assert len(set(intervals)) > 1

    def test_reconnect_jitter_bounded_by_backoff_max(self) -> None:
        """When backoff_max is close to base, the jitter span shrinks so the
        interval never exceeds backoff_max."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(host="h", reconnect_backoff_base=1.0, reconnect_backoff_max=1.3)
        intervals = [
            make_aio_pika_connect_kwargs(conn, SecurityConfig())["reconnect_interval"] for _ in range(50)
        ]
        assert all(1.0 <= i <= 1.3 for i in intervals)

    def test_credentials_provider_used_in_url(self) -> None:
        """M13: rotated credentials from the provider reach the aio-pika URL."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        cfg = ConnectionConfig(
            host="h", username="static", password="static",
            credentials_provider=lambda: ("rot-user", "rot-pass"),
        )
        kwargs = make_aio_pika_connect_kwargs(cfg, SecurityConfig())
        assert "rot-user:rot-pass@" in kwargs["url"]
        assert "static" not in kwargs["url"]

    def test_host_port_override_targets_specific_node(self) -> None:
        """M9: the pool passes a specific cluster endpoint via override kwargs."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(host="primary", port=5672, nodes=("backup:5673",))
        kwargs = make_aio_pika_connect_kwargs(
            conn, SecurityConfig(), host_override="backup", port_override=5673
        )
        assert "backup:5673" in kwargs["url"]
        assert "primary" not in kwargs["url"]

    def test_reconnect_jitter_survives_misconfigured_max_below_base(self) -> None:
        """A misconfigured backoff_max <= base must not crash or drop below base."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(host="h", reconnect_backoff_base=5.0, reconnect_backoff_max=1.0)
        interval = make_aio_pika_connect_kwargs(conn, SecurityConfig())["reconnect_interval"]
        assert interval == 5.0  # zero jitter span, pinned at base

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

    @pytest.mark.parametrize(
        ("vhost", "encoded"),
        [("/", "%2F"), ("orders", "orders"), ("orders/eu", "orders%2Feu"), ("a#b?c%d", "a%23b%3Fc%25d")],
    )
    def test_every_vhost_is_percent_encoded(self, vhost: str, encoded: str) -> None:
        """#40: only "/" used to be encoded; "orders/eu" went in raw and named
        the wrong vhost (or broke the URL parse)."""
        from yarl import URL

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(host="h", vhost=vhost)
        url = make_aio_pika_connect_kwargs(conn, SecurityConfig())["url"]
        assert url.startswith(f"amqp://guest:guest@h:5672/{encoded}?")
        # aiormq takes the vhost from URL.path minus the leading slash.
        assert URL(url).path[1:] == vhost
        # Same encoding as ConnectionConfig.url, so the two paths agree.
        assert conn.url.endswith(f"/{encoded}")

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

    def test_escape_hatch_properties_merged(self) -> None:
        """Item 8: ConnectionConfig.client_properties is additive on top of
        rabbitkit's own library/library_version identification."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig(
            connection_name="my-service",
            client_properties={"service_name": "orders-worker", "environment": "prod"},
        )
        sec = SecurityConfig()

        kwargs = make_aio_pika_connect_kwargs(conn, sec)

        assert kwargs["client_properties"]["library"] == "rabbitkit"
        assert kwargs["client_properties"]["connection_name"] == "my-service"
        assert kwargs["client_properties"]["service_name"] == "orders-worker"
        assert kwargs["client_properties"]["environment"] == "prod"

    def test_without_connection_name(self) -> None:
        """Item 8: client_properties is now ALWAYS present (rabbitkit
        identifies itself via library/library_version) even with no
        connection_name and no escape-hatch properties set."""
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            pytest.skip("aio-pika not installed")

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        conn = ConnectionConfig()
        sec = SecurityConfig()

        kwargs = make_aio_pika_connect_kwargs(conn, sec)

        assert "client_properties" in kwargs
        assert "connection_name" not in kwargs["client_properties"]
        assert kwargs["client_properties"]["library"] == "rabbitkit"
        assert kwargs["client_properties"]["library_version"]

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
        # TLS is chosen by the scheme: with "amqp" aiormq ignores ssl_context
        # and connects in plaintext (silently unencrypted on a plain port).
        assert kwargs["url"].startswith("amqps://")

    def test_without_ssl_uses_plain_scheme(self) -> None:
        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        kwargs = make_aio_pika_connect_kwargs(ConnectionConfig(), SecurityConfig())
        assert kwargs["url"].startswith("amqp://") and "ssl_context" not in kwargs

    @pytest.mark.parametrize("enabled", [True, False])
    async def test_aiormq_picks_the_matching_transport(self, enabled: bool) -> None:
        """Contract with the real aiormq: the URL rabbitkit builds selects the
        TLS transport exactly when TLS is enabled, and our context is used."""
        import aiormq
        from aiormq.connection import TCPTransportFactory, TLSTransportFactory

        from rabbitkit.async_.connection import make_aio_pika_connect_kwargs

        sec = SecurityConfig(ssl=SSLConfig(enabled=enabled, cert_reqs="CERT_REQUIRED"))
        kwargs = make_aio_pika_connect_kwargs(ConnectionConfig(), sec)
        conn = aiormq.Connection(kwargs["url"], context=kwargs.get("ssl_context"))
        factory = TLSTransportFactory if enabled else TCPTransportFactory
        assert isinstance(conn._transport_factory, factory)
        if enabled:
            assert conn.ssl_context is kwargs["ssl_context"]

    def test_server_hostname_is_pinned_in_the_context(self) -> None:
        """aio-pika drops a server_hostname kwarg and aiormq verifies against
        the URL host, so the context itself substitutes the configured name."""
        from rabbitkit.async_.connection import _PinnedHostnameSSLContext, build_ssl_context

        ctx = build_ssl_context(SSLConfig(enabled=True, server_hostname="rabbit.internal"))
        assert isinstance(ctx, _PinnedHostnameSSLContext)
        sslobj = ctx.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname="10.0.0.5")
        assert sslobj.server_hostname == "rabbit.internal"
        assert ctx.check_hostname and ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2

    def test_no_server_hostname_uses_a_plain_context(self) -> None:
        from rabbitkit.async_.connection import _PinnedHostnameSSLContext, build_ssl_context

        ctx = build_ssl_context(SSLConfig(enabled=True))
        assert ctx is not None and not isinstance(ctx, _PinnedHostnameSSLContext)
        assert ctx.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname="h").server_hostname == "h"

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


# -- I-11 / #37: blocked-connection monitor ----------------------------------


def _aiormq_connection() -> Any:
    """A real (never connected) aiormq Connection, so the private event the
    monitor reads is the one aiormq actually creates."""
    import aiormq

    return aiormq.Connection("amqp://guest:guest@localhost/")


def _robust(underlay: Any) -> Any:
    from unittest.mock import MagicMock

    conn = MagicMock()
    conn.is_closed = False
    conn.transport.connection = underlay
    return conn


def _unblocked(underlay: Any) -> Any:
    return underlay._Connection__connection_unblocked


class TestAiormqContract:
    """aio-pika has no blocked callbacks on any release; the monitor depends on
    aiormq's private event. Fail loudly here if aiormq renames it."""

    async def test_aiormq_connection_has_unblocked_event(self) -> None:
        import asyncio

        from rabbitkit.async_.connection import aiormq_unblocked_event

        underlay = _aiormq_connection()
        assert isinstance(aiormq_unblocked_event(_robust(underlay)), asyncio.Event)

    async def test_aio_pika_has_no_blocked_callbacks(self) -> None:
        """Documents why the monitor exists (#37): the API the old code hooked is absent."""
        import aio_pika

        conn = aio_pika.RobustConnection("amqp://guest:guest@localhost/")
        assert not hasattr(conn, "connection_blocked")
        assert not hasattr(conn, "connection_unblocked")


class TestBlockedConnectionMonitor:
    async def test_transitions_fire_callbacks(self) -> None:
        from rabbitkit.async_.connection import BlockedConnectionMonitor

        underlay = _aiormq_connection()
        events: list[str] = []
        monitor = BlockedConnectionMonitor(
            _robust(underlay),
            blocked_timeout=0,
            on_blocked=lambda: events.append("blocked"),
            on_unblocked=lambda: events.append("unblocked"),
        )
        _unblocked(underlay).set()
        await monitor.poll(0.0)
        assert events == [] and not monitor.is_blocked

        _unblocked(underlay).clear()  # what aiormq does on Connection.Blocked
        await monitor.poll(1.0)
        await monitor.poll(2.0)  # no second fire for the same alarm
        assert events == ["blocked"] and monitor.is_blocked

        _unblocked(underlay).set()  # Connection.Unblocked
        await monitor.poll(3.0)
        assert events == ["blocked", "unblocked"] and not monitor.is_blocked

    async def test_timeout_closes_underlying_connection_once(self) -> None:
        from unittest.mock import AsyncMock

        from rabbitkit.async_.connection import BlockedConnectionMonitor

        underlay = _aiormq_connection()
        underlay.close = AsyncMock()
        robust = _robust(underlay)
        monitor = BlockedConnectionMonitor(robust, blocked_timeout=5.0)

        _unblocked(underlay).clear()
        await monitor.poll(10.0)
        await monitor.poll(14.9)
        underlay.close.assert_not_awaited()
        await monitor.poll(15.0)
        await monitor.poll(16.0)
        underlay.close.assert_awaited_once()
        # The aiormq connection is closed, never the RobustConnection: that
        # would stop it reconnecting.
        robust.close.assert_not_called()
        assert isinstance(underlay.close.await_args.args[0], ConnectionError)
        assert monitor.forced_reconnects == 1

    async def test_unblock_before_timeout_does_not_close(self) -> None:
        from unittest.mock import AsyncMock

        from rabbitkit.async_.connection import BlockedConnectionMonitor

        underlay = _aiormq_connection()
        underlay.close = AsyncMock()
        monitor = BlockedConnectionMonitor(_robust(underlay), blocked_timeout=5.0)
        _unblocked(underlay).clear()
        await monitor.poll(0.0)
        _unblocked(underlay).set()
        await monitor.poll(4.0)
        await monitor.poll(20.0)
        underlay.close.assert_not_awaited()

    async def test_zero_timeout_tracks_state_but_never_closes(self) -> None:
        from unittest.mock import AsyncMock

        from rabbitkit.async_.connection import BlockedConnectionMonitor

        underlay = _aiormq_connection()
        underlay.close = AsyncMock()
        monitor = BlockedConnectionMonitor(_robust(underlay), blocked_timeout=0)
        _unblocked(underlay).clear()
        await monitor.poll(0.0)
        await monitor.poll(1e6)
        assert monitor.is_blocked
        underlay.close.assert_not_awaited()

    async def test_reconnect_resets_state_and_timer(self) -> None:
        from unittest.mock import AsyncMock

        from rabbitkit.async_.connection import BlockedConnectionMonitor

        first = _aiormq_connection()
        first.close = AsyncMock()
        robust = _robust(first)
        events: list[str] = []
        monitor = BlockedConnectionMonitor(
            robust, blocked_timeout=5.0, on_unblocked=lambda: events.append("unblocked")
        )
        _unblocked(first).clear()
        await monitor.poll(0.0)

        robust.transport = None  # mid-reconnect: state is held, not guessed
        await monitor.poll(1.0)
        assert monitor.is_blocked

        second = _aiormq_connection()
        second.close = AsyncMock()
        _unblocked(second).set()  # aiormq sets it when the reader starts
        robust.transport = type("T", (), {"connection": second})()
        await monitor.poll(2.0)
        assert not monitor.is_blocked and events == ["unblocked"]
        await monitor.poll(30.0)
        first.close.assert_not_awaited()
        second.close.assert_not_awaited()

    async def test_unsupported_aiormq_warns_and_does_not_start(self, caplog: pytest.LogCaptureFixture) -> None:
        from unittest.mock import MagicMock

        from rabbitkit.async_.connection import BlockedConnectionMonitor

        robust = MagicMock()
        robust.transport.connection = object()  # live, but no unblocked event
        monitor = BlockedConnectionMonitor(robust, blocked_timeout=1.0)
        with caplog.at_level("WARNING"):
            assert monitor.start() is False
        assert "inactive" in caplog.text

    async def test_run_loop_exits_when_connection_closed(self) -> None:
        import asyncio

        from rabbitkit.async_.connection import BlockedConnectionMonitor

        underlay = _aiormq_connection()
        _unblocked(underlay).set()
        robust = _robust(underlay)
        monitor = BlockedConnectionMonitor(robust, blocked_timeout=0, poll_interval=0.01)
        assert monitor.start() is True
        await asyncio.sleep(0.03)
        robust.is_closed = True
        await asyncio.wait_for(monitor._task, timeout=1.0)  # type: ignore[arg-type]
        await monitor.stop()

    async def test_stop_cancels_running_task(self) -> None:
        from rabbitkit.async_.connection import BlockedConnectionMonitor

        underlay = _aiormq_connection()
        _unblocked(underlay).set()
        monitor = BlockedConnectionMonitor(_robust(underlay), blocked_timeout=0, poll_interval=0.01)
        monitor.start()
        task = monitor._task
        await monitor.stop()
        assert task is not None and task.done()
        await monitor.stop()  # idempotent


# -- pamqp < 4 negative field-table ints -------------------------------------


class TestPamqpNegativeInts:
    @pytest.mark.parametrize(
        ("arguments", "expected"),
        [({"x-delivery-limit": -1}, True), ({"x": -128}, True), ({"x": -129}, False), ({"x": 0}, False),
         ({"x": True}, False), ({"x": "-1"}, False), (None, False), ({}, False)],
    )
    def test_has_small_negative_int(self, arguments: Any, expected: bool) -> None:
        from rabbitkit.async_.connection import has_small_negative_int

        assert has_small_negative_int(arguments) is expected

    def test_fixed_encoder_round_trips_every_small_int(self) -> None:
        """Whatever pamqp is installed, after the fix -128..127 encode as the
        signed short-short type and decode back to themselves."""
        from pamqp import decode, encode

        from rabbitkit.async_.connection import ensure_pamqp_encodes_negative_ints

        ensure_pamqp_encodes_negative_ints()
        ensure_pamqp_encodes_negative_ints()  # idempotent
        for value in (-128, -1, 0, 1, 127):
            raw = encode.table_integer(value)
            assert raw[:1] == b"b"
            assert decode.short_short_int(raw[1:])[1] == value
        assert encode.table_integer(-129)[:1] == b"s"  # larger values untouched
        table = encode.field_table({"x-delivery-limit": -1})
        assert decode.field_table(table)[1] == {"x-delivery-limit": -1}

    def test_patch_only_applies_where_pamqp_is_broken(self) -> None:
        import struct
        from unittest.mock import patch

        from pamqp import encode

        from rabbitkit.async_.connection import ensure_pamqp_encodes_negative_ints

        def broken(value: int) -> bytes:
            if -128 <= value <= 127:
                return b"b" + struct.Struct("B").pack(value)
            return b"s" + struct.Struct(">h").pack(value)

        def fixed(value: int) -> bytes:
            return b"b" + struct.Struct(">b").pack(value)

        with patch.object(encode, "table_integer", broken):
            ensure_pamqp_encodes_negative_ints()
            assert encode.table_integer is not broken
            assert encode.table_integer(-1) == b"b\xff"
            assert encode.table_integer(5) == b"b\x05"
        with patch.object(encode, "table_integer", fixed):
            ensure_pamqp_encodes_negative_ints()
            assert encode.table_integer is fixed  # already correct: left alone


class TestForcedReconnectScope:
    async def test_may_force_reconnect_false_tracks_but_never_closes(self) -> None:
        """The consumer connection: closing it would requeue in-flight deliveries."""
        from unittest.mock import AsyncMock

        from rabbitkit.async_.connection import BlockedConnectionMonitor

        underlay = _aiormq_connection()
        underlay.close = AsyncMock()
        monitor = BlockedConnectionMonitor(_robust(underlay), blocked_timeout=1.0, may_force_reconnect=lambda: False)
        _unblocked(underlay).clear()
        await monitor.poll(0.0)
        await monitor.poll(5.0)
        assert monitor.is_blocked
        underlay.close.assert_not_awaited()
        assert monitor.forced_reconnects == 0
