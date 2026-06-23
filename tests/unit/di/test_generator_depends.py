"""Tests for generator (yield-based) dependency injection."""

from typing import Annotated

import pytest

from rabbitkit.core.message import RabbitMessage
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


# ── DependencyScope ──────────────────────────────────────────────────────


class TestDependencyScope:
    def test_cleanup_closes_sync_generators(self) -> None:
        closed: list[bool] = []

        def gen_factory():  # type: ignore[no-untyped-def]
            yield "value"
            closed.append(True)

        scope = DependencyScope()
        gen = gen_factory()
        value = next(gen)
        scope.add_sync_generator(gen)
        assert value == "value"
        scope.cleanup()
        assert closed == [True]

    def test_cleanup_reverse_order(self) -> None:
        order: list[str] = []

        def gen_a():  # type: ignore[no-untyped-def]
            yield "a"
            order.append("a")

        def gen_b():  # type: ignore[no-untyped-def]
            yield "b"
            order.append("b")

        scope = DependencyScope()
        ga = gen_a()
        next(ga)
        scope.add_sync_generator(ga)
        gb = gen_b()
        next(gb)
        scope.add_sync_generator(gb)
        scope.cleanup()
        assert order == ["b", "a"]  # reversed

    def test_cleanup_empty_is_noop(self) -> None:
        scope = DependencyScope()
        scope.cleanup()  # no error

    @pytest.mark.asyncio
    async def test_async_cleanup_closes_async_generators(self) -> None:
        closed: list[bool] = []

        async def gen_factory():  # type: ignore[no-untyped-def]
            yield "async-value"
            closed.append(True)

        scope = DependencyScope()
        gen = gen_factory()
        value = await gen.__anext__()
        scope.add_async_generator(gen)
        assert value == "async-value"
        await scope.cleanup_async()
        assert closed == [True]

    @pytest.mark.asyncio
    async def test_async_cleanup_handles_both_sync_and_async(self) -> None:
        order: list[str] = []

        def sync_gen():  # type: ignore[no-untyped-def]
            yield "sync"
            order.append("sync")

        async def async_gen():  # type: ignore[no-untyped-def]
            yield "async"
            order.append("async")

        scope = DependencyScope()
        sg = sync_gen()
        next(sg)
        scope.add_sync_generator(sg)
        ag = async_gen()
        await ag.__anext__()
        scope.add_async_generator(ag)
        await scope.cleanup_async()
        # async cleaned first, then sync
        assert "async" in order
        assert "sync" in order

    @pytest.mark.asyncio
    async def test_async_cleanup_reverse_order(self) -> None:
        order: list[str] = []

        async def gen_a():  # type: ignore[no-untyped-def]
            yield "a"
            order.append("a")

        async def gen_b():  # type: ignore[no-untyped-def]
            yield "b"
            order.append("b")

        scope = DependencyScope()
        ga = gen_a()
        await ga.__anext__()
        scope.add_async_generator(ga)
        gb = gen_b()
        await gb.__anext__()
        scope.add_async_generator(gb)
        await scope.cleanup_async()
        assert order == ["b", "a"]  # reversed

    @pytest.mark.asyncio
    async def test_async_cleanup_empty_is_noop(self) -> None:
        scope = DependencyScope()
        await scope.cleanup_async()  # no error


# ── Sync generator resolution ───────────────────────────────────────────


class TestResolveGeneratorDepends:
    def test_sync_generator_dependency_resolved(self) -> None:
        def get_resource():  # type: ignore[no-untyped-def]
            yield "resource-value"

        def handler(body: bytes, res: Annotated[str, Depends(get_resource)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        kwargs = resolver.resolve(handler, msg, None, b"payload", scope=scope)
        assert kwargs["res"] == "resource-value"
        scope.cleanup()

    def test_sync_generator_cleanup_runs(self) -> None:
        cleaned: list[bool] = []

        def get_db():  # type: ignore[no-untyped-def]
            yield "db-session"
            cleaned.append(True)

        def handler(body: bytes, db: Annotated[str, Depends(get_db)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        resolver.resolve(handler, msg, None, b"payload", scope=scope)
        assert cleaned == []
        scope.cleanup()
        assert cleaned == [True]

    def test_generator_cached(self) -> None:
        call_count = 0

        def expensive():  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            yield f"result-{call_count}"

        def handler(
            a: Annotated[str, Depends(expensive)],
            b: Annotated[str, Depends(expensive)],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        kwargs = resolver.resolve(handler, msg, None, b"payload", scope=scope)
        assert kwargs["a"] == "result-1"
        assert kwargs["b"] == "result-1"
        assert call_count == 1
        scope.cleanup()

    def test_generator_no_scope_still_works(self) -> None:
        """Generator dependency works without scope (no cleanup)."""

        def get_resource():  # type: ignore[no-untyped-def]
            yield "value"

        def handler(body: bytes, res: Annotated[str, Depends(get_resource)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = resolver.resolve(handler, msg, None, b"payload")  # no scope
        assert kwargs["res"] == "value"

    def test_mixed_regular_and_generator(self) -> None:
        def regular_dep() -> str:
            return "regular"

        def generator_dep():  # type: ignore[no-untyped-def]
            yield "generator"

        def handler(
            body: bytes,
            a: Annotated[str, Depends(regular_dep)],
            b: Annotated[str, Depends(generator_dep)],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        kwargs = resolver.resolve(handler, msg, None, b"payload", scope=scope)
        assert kwargs["a"] == "regular"
        assert kwargs["b"] == "generator"
        scope.cleanup()


# ── Async generator resolution ──────────────────────────────────────────


class TestResolveAsyncGeneratorDepends:
    @pytest.mark.asyncio
    async def test_async_generator_dependency_resolved(self) -> None:
        async def get_resource():  # type: ignore[no-untyped-def]
            yield "async-resource"

        def handler(body: bytes, res: Annotated[str, Depends(get_resource)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        kwargs = await resolver.resolve_async(handler, msg, None, b"payload", scope=scope)
        assert kwargs["res"] == "async-resource"
        await scope.cleanup_async()

    @pytest.mark.asyncio
    async def test_async_generator_cleanup_runs(self) -> None:
        cleaned: list[bool] = []

        async def get_db():  # type: ignore[no-untyped-def]
            yield "async-db"
            cleaned.append(True)

        def handler(body: bytes, db: Annotated[str, Depends(get_db)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        await resolver.resolve_async(handler, msg, None, b"payload", scope=scope)
        assert cleaned == []
        await scope.cleanup_async()
        assert cleaned == [True]

    @pytest.mark.asyncio
    async def test_async_regular_dependency_awaited(self) -> None:
        """Non-generator async callable is awaited."""

        async def get_value() -> str:
            return "awaited"

        def handler(body: bytes, val: Annotated[str, Depends(get_value)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        kwargs = await resolver.resolve_async(handler, msg, None, b"payload")
        assert kwargs["val"] == "awaited"

    @pytest.mark.asyncio
    async def test_mixed_sync_async_generators(self) -> None:
        sync_cleaned: list[bool] = []
        async_cleaned: list[bool] = []

        def sync_dep():  # type: ignore[no-untyped-def]
            yield "sync-val"
            sync_cleaned.append(True)

        async def async_dep():  # type: ignore[no-untyped-def]
            yield "async-val"
            async_cleaned.append(True)

        def handler(
            body: bytes,
            s: Annotated[str, Depends(sync_dep)],
            a: Annotated[str, Depends(async_dep)],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        kwargs = await resolver.resolve_async(handler, msg, None, b"payload", scope=scope)
        assert kwargs["s"] == "sync-val"
        assert kwargs["a"] == "async-val"
        await scope.cleanup_async()
        assert sync_cleaned == [True]
        assert async_cleaned == [True]

    def test_async_gen_in_sync_resolve_raises(self) -> None:
        """Async generator dependency in sync resolve raises ConfigurationError."""

        async def async_dep():  # type: ignore[no-untyped-def]
            yield "value"

        def handler(body: bytes, val: Annotated[str, Depends(async_dep)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        with pytest.raises(ConfigurationError, match="Async generator"):
            resolver.resolve(handler, msg, None, b"payload")

    @pytest.mark.asyncio
    async def test_async_generator_cached(self) -> None:
        call_count = 0

        async def expensive():  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            yield f"async-result-{call_count}"

        def handler(
            a: Annotated[str, Depends(expensive)],
            b: Annotated[str, Depends(expensive)],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        kwargs = await resolver.resolve_async(handler, msg, None, b"payload", scope=scope)
        assert kwargs["a"] == "async-result-1"
        assert kwargs["b"] == "async-result-1"
        assert call_count == 1
        await scope.cleanup_async()


# ── Generator edge cases ────────────────────────────────────────────────


class TestGeneratorEdgeCases:
    """Edge cases for generator dependency lifecycle."""

    def test_cleanup_exception_propagates(self) -> None:
        """If generator cleanup raises, the exception propagates from scope.cleanup().

        The current implementation does not suppress cleanup exceptions, so callers
        (e.g. the pipeline) must handle them. This verifies the raw scope behavior.
        """

        def gen_raises_on_cleanup():  # type: ignore[no-untyped-def]
            yield "value"
            raise RuntimeError("cleanup failed!")

        scope = DependencyScope()
        gen = gen_raises_on_cleanup()
        val = next(gen)
        scope.add_sync_generator(gen)
        assert val == "value"

        with pytest.raises(RuntimeError, match="cleanup failed!"):
            scope.cleanup()

    def test_generator_exception_before_yield(self) -> None:
        """If generator raises BEFORE yield, the original exception propagates
        from next() — no value is produced."""

        def gen_raises_before():  # type: ignore[no-untyped-def]
            raise ValueError("setup failure")
            yield "never reached"

        def handler(
            body: bytes, val: Annotated[str, Depends(gen_raises_before)]
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()

        with pytest.raises(ValueError, match="setup failure"):
            resolver.resolve(handler, msg, None, b"payload", scope=scope)

    def test_multiple_generators_partial_cleanup_failure(self) -> None:
        """If gen_b cleanup fails (reversed order, cleaned first), gen_a cleanup
        does NOT run because the exception short-circuits the loop."""
        order: list[str] = []

        def gen_a():  # type: ignore[no-untyped-def]
            yield "a"
            order.append("a-cleanup")

        def gen_b():  # type: ignore[no-untyped-def]
            yield "b"
            order.append("b-cleanup")
            raise RuntimeError("b cleanup failed")

        scope = DependencyScope()
        ga = gen_a()
        next(ga)
        scope.add_sync_generator(ga)
        gb = gen_b()
        next(gb)
        scope.add_sync_generator(gb)

        with pytest.raises(RuntimeError, match="b cleanup failed"):
            scope.cleanup()

        # gen_b ran cleanup (reversed order), but gen_a did NOT because
        # the exception from gen_b propagated before gen_a could be cleaned.
        assert order == ["b-cleanup"]

    def test_use_cache_false_calls_generator_each_time(self) -> None:
        """Depends(gen_fn, use_cache=False) creates fresh generator per resolution."""
        call_count = 0

        def gen_factory():  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            yield f"result-{call_count}"

        def handler(
            a: Annotated[str, Depends(gen_factory, use_cache=False)],
            b: Annotated[str, Depends(gen_factory, use_cache=False)],
        ) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        kwargs = resolver.resolve(handler, msg, None, b"payload", scope=scope)

        # Each parameter gets its own generator invocation
        assert kwargs["a"] == "result-1"
        assert kwargs["b"] == "result-2"
        assert call_count == 2
        scope.cleanup()

    def test_generator_yielding_none(self) -> None:
        """Generator that yields None is valid — None is the resolved value."""

        def gen_none():  # type: ignore[no-untyped-def]
            yield None

        def handler(body: bytes, val: Annotated[None, Depends(gen_none)]) -> None:
            pass

        msg = _make_message()
        resolver = DIResolver()
        scope = DependencyScope()
        kwargs = resolver.resolve(handler, msg, None, b"payload", scope=scope)

        assert "val" in kwargs
        assert kwargs["val"] is None
        scope.cleanup()

    @pytest.mark.asyncio
    async def test_sync_generator_in_async_context(self) -> None:
        """Sync generators work fine in async context (cleanup via scope.cleanup_async).

        cleanup_async() handles both async and sync generators — sync generators
        are cleaned up in the second pass after async generators.
        """
        cleaned: list[bool] = []

        def sync_gen():  # type: ignore[no-untyped-def]
            yield "sync-in-async"
            cleaned.append(True)

        scope = DependencyScope()
        gen = sync_gen()
        val = next(gen)
        scope.add_sync_generator(gen)
        assert val == "sync-in-async"

        # cleanup_async handles sync generators too (via its sync gen loop)
        await scope.cleanup_async()
        assert cleaned == [True]

    @pytest.mark.asyncio
    async def test_async_cleanup_exception_propagates(self) -> None:
        """If async generator cleanup raises, exception propagates from cleanup_async()."""

        async def gen_raises_on_cleanup():  # type: ignore[no-untyped-def]
            yield "async-value"
            raise RuntimeError("async cleanup failed!")

        scope = DependencyScope()
        gen = gen_raises_on_cleanup()
        val = await gen.__anext__()
        scope.add_async_generator(gen)
        assert val == "async-value"

        with pytest.raises(RuntimeError, match="async cleanup failed!"):
            await scope.cleanup_async()

    @pytest.mark.asyncio
    async def test_mixed_cleanup_one_fails(self) -> None:
        """Mixed sync+async generators: if the async generator cleanup fails,
        the sync generator cleanup does NOT run because the exception propagates."""
        order: list[str] = []

        def sync_gen():  # type: ignore[no-untyped-def]
            yield "sync"
            order.append("sync-cleanup")

        async def async_gen():  # type: ignore[no-untyped-def]
            yield "async"
            order.append("async-cleanup")
            raise RuntimeError("async cleanup failed")

        scope = DependencyScope()
        sg = sync_gen()
        next(sg)
        scope.add_sync_generator(sg)
        ag = async_gen()
        await ag.__anext__()
        scope.add_async_generator(ag)

        with pytest.raises(RuntimeError, match="async cleanup failed"):
            await scope.cleanup_async()

        # Async generators are cleaned first. The failure prevents sync cleanup.
        assert order == ["async-cleanup"]
