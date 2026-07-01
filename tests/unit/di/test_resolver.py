"""Tests for di/resolver.py — DIResolver parameter resolution."""

from typing import Annotated, Any

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.di.context import Context, ContextRepo, Header, Path
from rabbitkit.di.depends import Depends
from rabbitkit.di.resolver import ConfigurationError, DependencyScope, DIResolver

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b'{"id": 1}',
        "routing_key": "orders.created",
        "headers": {"x-tenant": "acme"},
        "path": {"level": "info"},
    }
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


def _get_db() -> str:
    return "db-session"


def _get_cache() -> str:
    return "cache"


# ── validate_handler ─────────────────────────────────────────────────────


class TestValidateHandler:
    def test_valid_body_only(self) -> None:
        def handler(body: bytes) -> None:
            pass

        DIResolver().validate_handler(handler)  # no exception

    def test_valid_no_params(self) -> None:
        def handler() -> None:
            pass

        DIResolver().validate_handler(handler)

    def test_valid_message_only(self) -> None:
        def handler(msg: RabbitMessage) -> None:
            pass

        DIResolver().validate_handler(handler)

    def test_valid_body_and_message(self) -> None:
        def handler(body: bytes, msg: RabbitMessage) -> None:
            pass

        DIResolver().validate_handler(handler)

    def test_valid_body_and_di(self) -> None:
        def handler(
            body: bytes,
            db: Annotated[str, Depends(_get_db)],
        ) -> None:
            pass

        DIResolver().validate_handler(handler)

    def test_rejects_args(self) -> None:
        def handler(*args: object) -> None:
            pass

        with pytest.raises(ConfigurationError, match="args"):
            DIResolver().validate_handler(handler)

    def test_rejects_kwargs(self) -> None:
        def handler(**kwargs: object) -> None:
            pass

        with pytest.raises(ConfigurationError, match="kwargs"):
            DIResolver().validate_handler(handler)

    def test_rejects_multiple_body_params(self) -> None:
        def handler(body1: bytes, body2: str) -> None:
            pass

        with pytest.raises(ConfigurationError, match="multiple body"):
            DIResolver().validate_handler(handler)

    def test_default_params_not_body(self) -> None:
        """Parameters with defaults are not counted as body params."""

        def handler(body: bytes, extra: str = "default") -> None:
            pass

        DIResolver().validate_handler(handler)


# ── resolve — body injection ─────────────────────────────────────────────


class TestResolveBody:
    def test_inject_body(self) -> None:
        def handler(body: bytes) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, msg, None, b"payload")
        assert kwargs["body"] == b"payload"

    def test_inject_body_and_message(self) -> None:
        def handler(body: bytes, msg: RabbitMessage) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, msg, None, b"payload")
        assert kwargs["body"] == b"payload"
        assert kwargs["msg"] is msg


# ── resolve — Depends ────────────────────────────────────────────────────


class TestResolveDepends:
    def test_depends_resolved(self) -> None:
        def handler(
            body: bytes,
            db: Annotated[str, Depends(_get_db)],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, msg, None, b"payload")
        assert kwargs["db"] == "db-session"

    def test_depends_cached(self) -> None:
        call_count = 0

        def expensive() -> str:
            nonlocal call_count
            call_count += 1
            return "result"

        def handler(
            a: Annotated[str, Depends(expensive)],
            b: Annotated[str, Depends(expensive)],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, msg, None, b"payload")
        assert kwargs["a"] == "result"
        assert kwargs["b"] == "result"
        assert call_count == 1  # cached

    def test_depends_no_cache(self) -> None:
        call_count = 0

        def factory() -> str:
            nonlocal call_count
            call_count += 1
            return f"result-{call_count}"

        def handler(
            a: Annotated[str, Depends(factory, use_cache=False)],
            b: Annotated[str, Depends(factory, use_cache=False)],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        resolver.resolve(handler, msg, None, b"payload")
        assert call_count == 2  # not cached


# ── resolve — Header ────────────────────────────────────────────────────


class TestResolveHeader:
    def test_header_resolved(self) -> None:
        def handler(
            body: bytes,
            tenant: Annotated[str, Header("x-tenant")],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, msg, None, b"payload")
        assert kwargs["tenant"] == "acme"

    def test_header_missing_raises(self) -> None:
        def handler(
            body: bytes,
            missing: Annotated[str, Header("x-missing")],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        with pytest.raises(KeyError, match="x-missing"):
            resolver.resolve(handler, msg, None, b"payload")


# ── resolve — Path ──────────────────────────────────────────────────────


class TestResolvePath:
    def test_path_resolved(self) -> None:
        def handler(
            body: bytes,
            level: Annotated[str, Path("level")],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, msg, None, b"payload")
        assert kwargs["level"] == "info"

    def test_path_missing_raises(self) -> None:
        def handler(
            body: bytes,
            missing: Annotated[str, Path("missing")],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        with pytest.raises(KeyError, match="missing"):
            resolver.resolve(handler, msg, None, b"payload")


# ── resolve — Context ───────────────────────────────────────────────────


class TestResolveContext:
    def test_context_resolved(self) -> None:
        def handler(
            body: bytes,
            app: Annotated[str, Context("app")],
        ) -> None:
            pass

        msg = _make_message()
        repo = ContextRepo()
        repo.set_global("app", "my-app")
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, msg, repo, b"payload")
        assert kwargs["app"] == "my-app"

    def test_context_missing_raises(self) -> None:
        def handler(
            body: bytes,
            missing: Annotated[str, Context("missing")],
        ) -> None:
            pass

        msg = _make_message()
        repo = ContextRepo()
        resolver = DIResolver()
        with pytest.raises(KeyError, match="missing"):
            resolver.resolve(handler, msg, repo, b"payload")

    def test_context_no_repo_raises(self) -> None:
        def handler(
            body: bytes,
            app: Annotated[str, Context("app")],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        with pytest.raises(ConfigurationError, match="ContextRepo"):
            resolver.resolve(handler, msg, None, b"payload")


# ── resolve — combined ──────────────────────────────────────────────────


class TestResolveCombined:
    def test_full_resolution(self) -> None:
        def handler(
            body: bytes,
            msg: RabbitMessage,
            tenant: Annotated[str, Header("x-tenant")],
            level: Annotated[str, Path("level")],
            db: Annotated[str, Depends(_get_db)],
            app: Annotated[str, Context("app")],
        ) -> None:
            pass

        message = _make_message()
        repo = ContextRepo()
        repo.set_global("app", "my-app")
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, message, repo, b"payload")

        assert kwargs["body"] == b"payload"
        assert kwargs["msg"] is message
        assert kwargs["tenant"] == "acme"
        assert kwargs["level"] == "info"
        assert kwargs["db"] == "db-session"
        assert kwargs["app"] == "my-app"

    def test_no_body_param(self) -> None:
        def handler(msg: RabbitMessage) -> None:
            pass

        message = _make_message()
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, message, None, b"payload")
        assert kwargs["msg"] is message
        assert "body" not in kwargs


# ── DependencyScope ──────────────────────────────────────────────────────


class TestDependencyScope:
    def test_cleanup_with_sync_generator_normal(self) -> None:
        """Sync generator that yields once; cleanup drains the remaining value."""
        log: list[str] = []

        def gen_factory() -> Any:
            log.append("setup")
            yield "value"
            log.append("teardown")

        scope = DependencyScope()
        gen = gen_factory()
        next(gen)  # advance to yield
        scope.add_sync_generator(gen)
        scope.cleanup()
        assert "teardown" in log
        assert scope._sync_generators == []

    def test_cleanup_stopiteration_branch(self) -> None:
        """Sync generator that is already exhausted; StopIteration is swallowed."""

        def already_done() -> Any:
            yield "value"

        scope = DependencyScope()
        gen = already_done()
        next(gen)  # exhaust it — generator is now past the only yield
        # Manually exhaust so next() raises StopIteration
        scope.add_sync_generator(gen)
        scope.cleanup()  # must not raise
        assert scope._sync_generators == []

    def test_cleanup_multiple_generators_reversed(self) -> None:
        """Generators are cleaned up in reverse registration order."""
        order: list[int] = []

        def make_gen(n: int) -> Any:
            yield n
            order.append(n)

        scope = DependencyScope()
        for i in range(3):
            gen = make_gen(i)
            next(gen)
            scope.add_sync_generator(gen)
        scope.cleanup()
        assert order == [2, 1, 0]

    async def test_cleanup_async_with_async_generator(self) -> None:
        """Async generator that yields once; cleanup_async drains it."""
        log: list[str] = []

        async def agen_factory() -> Any:
            log.append("async-setup")
            yield "value"
            log.append("async-teardown")

        scope = DependencyScope()
        gen = agen_factory()
        await gen.__anext__()  # advance to yield
        scope.add_async_generator(gen)
        await scope.cleanup_async()
        assert "async-teardown" in log
        assert scope._async_generators == []
        assert scope._sync_generators == []

    async def test_cleanup_async_stopasynciteration_swallowed(self) -> None:
        """Exhausted async generator; StopAsyncIteration is swallowed."""

        async def agen_empty() -> Any:
            yield "value"

        scope = DependencyScope()
        gen = agen_empty()
        await gen.__anext__()
        scope.add_async_generator(gen)
        await scope.cleanup_async()  # must not raise

    async def test_cleanup_async_with_sync_generator_stopiteration(self) -> None:
        """cleanup_async also cleans sync generators; StopIteration on exhausted gen is swallowed."""

        def already_done() -> Any:
            yield "value"

        scope = DependencyScope()
        gen = already_done()
        next(gen)  # exhaust it
        scope.add_sync_generator(gen)
        await scope.cleanup_async()  # must not raise — covers lines 60-61
        assert scope._sync_generators == []

    async def test_cleanup_async_with_sync_generator_normal(self) -> None:
        """cleanup_async drains sync generators registered alongside async ones."""
        teardown_ran = False

        def sync_gen() -> Any:
            nonlocal teardown_ran
            yield "val"
            teardown_ran = True

        scope = DependencyScope()
        gen = sync_gen()
        next(gen)
        scope.add_sync_generator(gen)
        await scope.cleanup_async()
        assert teardown_ran


# ── _get_type_hints fallback paths ───────────────────────────────────────

# Module-level type alias used for the closure test so it lives in module scope.
_MyStr = str


def _make_handler_with_local_type_closure() -> Any:
    """Return a handler whose annotation refers to a locally-scoped type captured
    via closure. typing.get_type_hints() fails (name not in globals), but the
    attempt-2 fallback succeeds by using the closure variable as localns."""
    local_type = str
    # The annotation uses the string form to avoid eager evaluation.
    # The name 'local_type' is only visible in this enclosing scope (closure).

    def handler(body: "local_type") -> None:  # type: ignore[name-defined]
        _ = local_type  # capture in closure

    return handler


class TestGetTypeHintsFallbacks:
    def test_fallback_for_broken_annotations(self) -> None:
        """When get_type_hints fails (e.g. forward ref to unknown name), the
        resolver falls through to the inspect-based fallbacks and still
        produces a usable hints dict."""

        # Build a function whose __annotations__ reference a name that cannot
        # be resolved by typing.get_type_hints() because it isn't in any
        # accessible namespace.
        def handler(body: "NonExistentType123") -> None:  # type: ignore[name-defined]  # noqa: F821
            pass

        # The resolver must not raise — it should fall back gracefully.
        resolver = DIResolver()
        hints = resolver._get_type_hints(handler)
        # The fallback returns either a string or the raw annotation; either
        # way, body must appear in the hints dict.
        assert "body" in hints

    def test_closure_variable_extraction_fallback(self) -> None:
        """Covers lines 103-105: attempt-2 fallback uses closure vars as localns.

        The handler is built by _make_handler_with_local_type_closure() so that
        typing.get_type_hints() fails (the type name is not in module globals)
        but attempt 2 succeeds by extracting 'LocalType' from the closure.
        """
        handler = _make_handler_with_local_type_closure()
        resolver = DIResolver()
        hints = resolver._get_type_hints(handler)
        assert "body" in hints
        assert hints["body"] is str

    def test_fallback_returns_raw_when_all_else_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Force all typing paths and inspect.get_annotations(eval_str=True) to fail
        so the final raw-annotation fallback (lines 119-125) is exercised.

        inspect.signature() calls get_annotations with eval_str=False; we only
        fail calls that pass eval_str=True so that the final fallback path itself
        still works.
        """
        import inspect as _inspect_mod

        import rabbitkit.di.resolver as _resolver_mod

        _real_get_annotations = _inspect_mod.get_annotations

        def _failing_get_type_hints(*args: Any, **kwargs: Any) -> Any:
            raise NameError("forced failure")

        def _selective_get_annotations(*args: Any, **kwargs: Any) -> Any:
            # Only fail when resolver.py calls it (eval_str=True).
            if kwargs.get("eval_str", False):
                raise AttributeError("forced failure")
            return _real_get_annotations(*args, **kwargs)

        monkeypatch.setattr(_resolver_mod.typing, "get_type_hints", _failing_get_type_hints)
        monkeypatch.setattr(_resolver_mod.inspect, "get_annotations", _selective_get_annotations)

        def handler(body: bytes) -> None:
            pass

        resolver = DIResolver()
        hints = resolver._get_type_hints(handler)
        assert "body" in hints


# ── resolve_async ────────────────────────────────────────────────────────

# Module-level dependency factories (required: from __future__ import annotations
# makes annotations lazy; local functions in test methods cannot be resolved).


def _async_get_db() -> str:
    return "db-async"


async def _async_coro_dep() -> str:
    return "coro-result"


def _sync_gen_dep_async() -> Any:
    yield "sync-gen-async"


async def _async_gen_dep() -> Any:
    yield "async-gen-value"


class TestResolveAsync:
    async def test_inject_body_async(self) -> None:
        def handler(body: bytes) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, b"async-payload")
        assert kwargs["body"] == b"async-payload"

    async def test_inject_message_async(self) -> None:
        """Covers lines 241-242: RabbitMessage injection in resolve_async."""

        def handler(msg: RabbitMessage) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, b"payload")
        assert kwargs["msg"] is msg

    async def test_inject_body_and_message_async(self) -> None:
        def handler(body: bytes, msg: RabbitMessage) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, b"data")
        assert kwargs["body"] == b"data"
        assert kwargs["msg"] is msg

    async def test_resolve_depends_async_plain(self) -> None:
        """Covers _resolve_depends_async for a plain (non-generator) dependency."""

        def handler(db: Annotated[str, Depends(_async_get_db)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, None)
        assert kwargs["db"] == "db-async"

    async def test_resolve_depends_async_coroutine(self) -> None:
        """Covers _resolve_depends_async awaitable branch (lines 335-337)."""

        def handler(result: Annotated[str, Depends(_async_coro_dep)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, None)
        assert kwargs["result"] == "coro-result"

    async def test_resolve_depends_async_sync_generator(self) -> None:
        """Covers _resolve_depends_async sync-generator branch (lines 329-333)."""

        def handler(dep: Annotated[str, Depends(_sync_gen_dep_async)]) -> None:
            pass

        msg = _make_message()
        scope = DependencyScope()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, None, scope=scope)
        assert kwargs["dep"] == "sync-gen-async"
        assert len(scope._sync_generators) == 1

    async def test_resolve_depends_async_async_generator(self) -> None:
        """Covers _resolve_depends_async async-generator branch (lines 324-328)."""

        def handler(dep: Annotated[str, Depends(_async_gen_dep)]) -> None:
            pass

        msg = _make_message()
        scope = DependencyScope()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, None, scope=scope)
        assert kwargs["dep"] == "async-gen-value"
        assert len(scope._async_generators) == 1

    async def test_resolve_async_header_marker(self) -> None:
        """Covers line 236: _resolve_marker called for Header in resolve_async."""

        def handler(tenant: Annotated[str, Header("x-tenant")]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, None)
        assert kwargs["tenant"] == "acme"

    async def test_resolve_async_path_marker(self) -> None:
        """Covers line 236: _resolve_marker called for Path in resolve_async."""

        def handler(level: Annotated[str, Path("level")]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, None)
        assert kwargs["level"] == "info"

    async def test_resolve_async_context_marker(self) -> None:
        """Covers line 236: _resolve_marker called for Context in resolve_async."""

        def handler(app: Annotated[str, Context("app")]) -> None:
            pass

        msg = _make_message()
        repo = ContextRepo()
        repo.set_global("app", "my-app")
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, repo, None)
        assert kwargs["app"] == "my-app"

    async def test_depends_cached_async(self) -> None:
        """Covers _resolve_depends_async cache hit branch."""
        call_count = 0

        def counter() -> str:
            nonlocal call_count
            call_count += 1
            return "once"

        def handler(
            a: Annotated[str, Depends(counter)],
            b: Annotated[str, Depends(counter)],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, None)
        assert kwargs["a"] == "once"
        assert kwargs["b"] == "once"
        assert call_count == 1


# ── _extract_di_marker — inspect.Parameter.empty branch (line 263) ──────


class TestExtractDiMarkerEmpty:
    def test_unannotated_param_triggers_empty_check(self) -> None:
        """Covers line 263: _extract_di_marker returns None for inspect.Parameter.empty.

        An unannotated parameter causes hints.get(name, inspect.Parameter.empty)
        to return inspect.Parameter.empty, exercising the early-return guard.
        """
        import inspect as _inspect

        resolver = DIResolver()
        result = resolver._extract_di_marker(_inspect.Parameter.empty)
        assert result is None

    def test_resolve_with_unannotated_body_param(self) -> None:
        """Calling resolve() with an unannotated body parameter exercises line 263."""

        def handler(body) -> None:  # type: ignore[no-untyped-def]
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, msg, None, b"raw")
        assert kwargs["body"] == b"raw"


# ── _resolve_marker — unknown marker branch ──────────────────────────────


class TestResolveMarkerUnknown:
    def test_unknown_marker_raises_configuration_error(self) -> None:
        """Covers line 289: raise ConfigurationError for unknown DI marker."""

        class _UnknownMarker:
            pass

        resolver = DIResolver()
        msg = _make_message()
        with pytest.raises(ConfigurationError, match="Unknown DI marker"):
            resolver._resolve_marker(  # type: ignore[arg-type]
                _UnknownMarker(),  # type: ignore[arg-type]
                msg,
                None,
                {},
            )


# ── _resolve_marker — Depends branch (line 282) ──────────────────────────


class TestResolveMarkerDepends:
    def test_resolve_marker_calls_depends(self) -> None:
        """Covers line 282: _resolve_marker dispatches to _resolve_depends."""
        marker = Depends(_get_db)
        resolver = DIResolver()
        msg = _make_message()
        result = resolver._resolve_marker(marker, msg, None, {})
        assert result == "db-session"


# ── _resolve_depends — sync generator + async gen error ──────────────────

# Module-level sync generator factory (must be at module level).


def _sync_gen_teardown_factory() -> Any:
    yield "resource"


async def _async_gen_factory() -> Any:
    yield "async-resource"


class TestResolveDependsGenerators:
    def test_sync_generator_dependency_with_scope(self) -> None:
        """Covers lines 300-303: sync generator in _resolve_depends when scope provided."""
        marker = Depends(_sync_gen_teardown_factory)
        scope = DependencyScope()
        resolver = DIResolver()
        result = resolver._resolve_depends(marker, {}, scope)
        assert result == "resource"
        assert len(scope._sync_generators) == 1

    def test_sync_generator_dependency_no_scope(self) -> None:
        """Covers lines 300-302: sync generator without scope (gen not stored)."""
        marker = Depends(_sync_gen_teardown_factory)
        resolver = DIResolver()
        result = resolver._resolve_depends(marker, {})
        assert result == "resource"

    def test_async_gen_in_sync_pipeline_raises(self) -> None:
        """Covers line 305: async generator dep in sync _resolve_depends raises ConfigurationError."""
        marker = Depends(_async_gen_factory)
        resolver = DIResolver()
        with pytest.raises(ConfigurationError, match="Async generator dependency"):
            resolver._resolve_depends(marker, {})


class TestSignatureHintsCache:
    def test_sig_and_hints_is_cached_per_handler(self) -> None:
        """Reflection (signature + get_type_hints) is computed once and reused."""

        def handler(body: bytes) -> None: ...

        resolver = DIResolver()
        first = resolver._sig_and_hints(handler)
        second = resolver._sig_and_hints(handler)

        assert first is second  # same cached tuple, not recomputed
        assert handler in resolver._sig_hints_cache


# ── DependencyScope — error-path coverage ────────────────────────────────


class TestDependencyScopeErrorPaths:
    def test_cleanup_sync_gen_close_raises_is_logged(self, caplog: Any) -> None:
        """Lines 64-65: gen.close() raising Exception is caught and logged.

        A generator that intercepts GeneratorExit in its finally block and
        raises a different exception causes gen.close() to propagate that
        exception. The DependencyScope must catch it and log a warning instead
        of crashing.
        """
        import logging

        def raising_close_gen() -> Any:
            try:
                yield "first"   # test advances here
                yield "second"  # cleanup() next() call advances here; close() then raises
            finally:
                raise RuntimeError("close exploded")

        scope = DependencyScope()
        gen = raising_close_gen()
        next(gen)  # advance past the yield
        scope.add_sync_generator(gen)

        with caplog.at_level(logging.WARNING, logger="rabbitkit.di.resolver"):
            scope.cleanup()  # must NOT raise

        assert any("close() raised" in r.message for r in caplog.records)
        assert scope._sync_generators == []

    async def test_cleanup_async_gen_aclose_raises_is_logged(self, caplog: Any) -> None:
        """Lines 85-86: gen.aclose() raising Exception is caught and logged."""
        import logging

        async def raising_aclose_gen() -> Any:
            try:
                yield "first"   # test advances here
                yield "second"  # cleanup_async() __anext__() advances here; aclose() then raises
            finally:
                raise RuntimeError("aclose exploded")

        scope = DependencyScope()
        gen = raising_aclose_gen()
        await gen.__anext__()  # advance past the yield
        scope.add_async_generator(gen)

        with caplog.at_level(logging.WARNING, logger="rabbitkit.di.resolver"):
            await scope.cleanup_async()  # must NOT raise

        assert any("aclose() raised" in r.message for r in caplog.records)
        assert scope._async_generators == []

    async def test_cleanup_async_sync_gen_teardown_raises_is_logged(self, caplog: Any) -> None:
        """Lines 94-95: sync generator teardown (next()) raising in cleanup_async is logged."""
        import logging

        def raising_teardown_gen() -> Any:
            yield "value"
            raise RuntimeError("teardown boom")

        scope = DependencyScope()
        gen = raising_teardown_gen()
        next(gen)  # advance past the yield; teardown will raise on next()
        scope.add_sync_generator(gen)

        with caplog.at_level(logging.WARNING, logger="rabbitkit.di.resolver"):
            await scope.cleanup_async()  # must NOT raise

        # The teardown-raised warning must be present.
        assert any("teardown raised" in r.message for r in caplog.records)
        assert scope._sync_generators == []

    async def test_cleanup_async_sync_gen_close_raises_is_logged(self, caplog: Any) -> None:
        """Lines 99-100: sync gen close() raising in cleanup_async is logged."""
        import logging

        def raising_close_sync_gen() -> Any:
            try:
                yield "first"   # test advances here
                yield "second"  # cleanup_async() next() call advances here; close() then raises
            finally:
                raise RuntimeError("sync close boom in async path")

        scope = DependencyScope()
        gen = raising_close_sync_gen()
        next(gen)  # advance past the yield
        scope.add_sync_generator(gen)

        with caplog.at_level(logging.WARNING, logger="rabbitkit.di.resolver"):
            await scope.cleanup_async()  # must NOT raise

        assert any("close() raised" in r.message for r in caplog.records)
        assert scope._sync_generators == []


# ── validate_handler — unannotated param among annotated ones (line 216) ─


class TestValidateHandlerUnannotatedParam:
    def test_unannotated_param_among_annotated_not_counted_as_body(self) -> None:
        """Line 216: unannotated param triggers the 'if ann is empty: continue'
        branch in validate_handler, so it is NOT counted as a body-like param.

        A handler (annotated_body, unannotated) should pass validation even
        though there are two non-DI, non-message params, because the second one
        has no annotation and is skipped by the early-continue guard.
        """

        def handler(body: bytes, extra) -> None:  # type: ignore[no-untyped-def]
            pass

        DIResolver().validate_handler(handler)  # must not raise
