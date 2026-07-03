"""Tests for the top-level rabbitkit package's public API surface (L8/L9)."""

from __future__ import annotations

import contextlib
import importlib
import sys
import warnings as _warnings
from typing import ClassVar

import pytest


@contextlib.contextmanager
def _warnings_should_not_include_aio_deprecation():
    with _warnings.catch_warnings(record=True) as caught:
        _warnings.simplefilter("always")
        yield
    aio_warnings = [w for w in caught if "rabbitkit.aio" in str(w.message)]
    assert aio_warnings == [], f"importing rabbitkit itself must not warn about rabbitkit.aio: {aio_warnings}"


class TestTopLevelBrokerExports:
    """L8: AsyncBroker/SyncBroker must be importable from the top-level package."""

    def test_async_broker_importable_from_top_level(self) -> None:
        import rabbitkit

        assert rabbitkit.AsyncBroker is not None
        from rabbitkit import AsyncBroker

        assert AsyncBroker is not None

    def test_sync_broker_importable_from_top_level(self) -> None:
        import rabbitkit

        assert rabbitkit.SyncBroker is not None
        from rabbitkit import SyncBroker

        assert SyncBroker is not None

    def test_both_brokers_in_dunder_all(self) -> None:
        import rabbitkit

        assert "AsyncBroker" in rabbitkit.__all__
        assert "SyncBroker" in rabbitkit.__all__

    def test_top_level_and_canonical_submodule_are_the_same_class(self) -> None:
        from rabbitkit import AsyncBroker as TopLevelAsyncBroker
        from rabbitkit.async_.broker import AsyncBroker as CanonicalAsyncBroker

        assert TopLevelAsyncBroker is CanonicalAsyncBroker


class TestExperimentalSymbolsNotAtTopLevel:
    """L9: experimental-tier symbols must NOT be re-exported at the top
    level -- only via `from rabbitkit.experimental import ...`."""

    EXPERIMENTAL_SYMBOLS: ClassVar[list[str]] = [
        "SigningMiddleware",
        "SigningConfig",
        "InvalidSignatureError",
        "create_dashboard_app",
        "RPCClient",
        "AsyncRPCClient",
        "RPCTimeoutError",
        "DistributedLock",
        "LockMiddleware",
        "RedisLock",
        "ResultMiddleware",
        "ResultBackend",
        "RedisResultBackend",
        "StreamConsumerConfig",
        "StreamOffset",
        "StreamOffsetType",
    ]

    @pytest.mark.parametrize("name", EXPERIMENTAL_SYMBOLS)
    def test_not_in_dunder_all(self, name: str) -> None:
        import rabbitkit

        assert name not in rabbitkit.__all__

    @pytest.mark.parametrize("name", EXPERIMENTAL_SYMBOLS)
    def test_not_an_attribute_of_top_level_package(self, name: str) -> None:
        import rabbitkit

        assert not hasattr(rabbitkit, name), (
            f"{name} should only be importable via rabbitkit.experimental, not the top level"
        )

    @pytest.mark.parametrize("name", EXPERIMENTAL_SYMBOLS)
    def test_still_importable_from_experimental(self, name: str) -> None:
        import rabbitkit.experimental

        assert hasattr(rabbitkit.experimental, name)
        assert name in rabbitkit.experimental.__all__


class TestAioDeprecated:
    """L8: rabbitkit.aio is a deprecated alias for rabbitkit.async_."""

    def test_importing_aio_emits_deprecation_warning(self) -> None:
        sys.modules.pop("rabbitkit.aio", None)  # force re-import to re-trigger the warning

        with pytest.warns(DeprecationWarning, match="rabbitkit.async_"):
            importlib.import_module("rabbitkit.aio")

    def test_aio_still_works_despite_deprecation(self) -> None:
        sys.modules.pop("rabbitkit.aio", None)

        with pytest.warns(DeprecationWarning):
            aio = importlib.import_module("rabbitkit.aio")

        from rabbitkit.async_.broker import AsyncBroker as CanonicalAsyncBroker

        assert aio.AsyncBroker is CanonicalAsyncBroker

    def test_aio_not_eagerly_imported_by_top_level_package(self) -> None:
        """Importing rabbitkit itself must NOT trigger rabbitkit.aio's
        deprecation warning as a side effect -- only importing rabbitkit.aio
        directly should warn."""
        sys.modules.pop("rabbitkit.aio", None)
        sys.modules.pop("rabbitkit", None)

        with _warnings_should_not_include_aio_deprecation():
            importlib.import_module("rabbitkit")


class TestSyncAsyncBrokerAPIParity:
    """Loop Engineering Review, Architecture: sync/async twins must expose
    the same public method names and semantics -- enforced here, not just
    by convention, so the two brokers can't silently drift apart.

    Two documented, deliberate asymmetries are allowed (see
    docs/guide/full-guide.md's "Sync vs. async: two different connection
    models" section): SyncBroker.pump_idle() has no async equivalent because
    AsyncBroker's two aio-pika connections self-heartbeat via
    connect_robust() with nothing to pump; AsyncBroker.request_shutdown()
    has no sync equivalent because it triggers an async drain that only
    makes sense against the async signal-handling model. Any OTHER
    asymmetry is a real API drift and must fail this test.
    """

    ALLOWED_SYNC_ONLY: ClassVar[frozenset[str]] = frozenset({"pump_idle"})
    ALLOWED_ASYNC_ONLY: ClassVar[frozenset[str]] = frozenset({"request_shutdown"})

    @staticmethod
    def _public_members(cls: type) -> frozenset[str]:
        return frozenset(name for name in dir(cls) if not name.startswith("_"))

    def test_no_undocumented_sync_only_members(self) -> None:
        from rabbitkit.async_.broker import AsyncBroker
        from rabbitkit.sync.broker import SyncBroker

        sync_only = self._public_members(SyncBroker) - self._public_members(AsyncBroker)
        undocumented = sync_only - self.ALLOWED_SYNC_ONLY
        assert not undocumented, (
            f"SyncBroker has public members AsyncBroker lacks, not in the documented "
            f"allowlist: {sorted(undocumented)}. Either add the equivalent to "
            f"AsyncBroker, or add it to ALLOWED_SYNC_ONLY with a comment explaining "
            f"why the asymmetry is deliberate (see docs/guide/full-guide.md)."
        )

    def test_no_undocumented_async_only_members(self) -> None:
        from rabbitkit.async_.broker import AsyncBroker
        from rabbitkit.sync.broker import SyncBroker

        async_only = self._public_members(AsyncBroker) - self._public_members(SyncBroker)
        undocumented = async_only - self.ALLOWED_ASYNC_ONLY
        assert not undocumented, (
            f"AsyncBroker has public members SyncBroker lacks, not in the documented "
            f"allowlist: {sorted(undocumented)}. Either add the equivalent to "
            f"SyncBroker, or add it to ALLOWED_ASYNC_ONLY with a comment explaining "
            f"why the asymmetry is deliberate (see docs/guide/full-guide.md)."
        )

    def test_documented_asymmetries_are_still_real(self) -> None:
        """If pump_idle()/request_shutdown() ever get a same-named twin
        added to the other broker, the allowlists above should shrink --
        this catches the allowlist going stale in the other direction."""
        from rabbitkit.async_.broker import AsyncBroker
        from rabbitkit.sync.broker import SyncBroker

        assert not hasattr(AsyncBroker, "pump_idle")
        assert not hasattr(SyncBroker, "request_shutdown")

    def test_common_public_surface_is_substantial(self) -> None:
        """Sanity check that the two brokers actually share a large,
        meaningful public API, not just a coincidental couple of names."""
        from rabbitkit.async_.broker import AsyncBroker
        from rabbitkit.sync.broker import SyncBroker

        common = self._public_members(SyncBroker) & self._public_members(AsyncBroker)
        for expected in ("start", "stop", "run", "publish", "subscriber", "publisher", "routes"):
            assert expected in common, f"{expected!r} should be common to both brokers"
        assert len(common) >= 10
