"""Tests for di/context.py — Context, Header, Path markers + ContextRepo."""

from __future__ import annotations

import threading

from rabbitkit.di.context import _MISSING, Context, ContextRepo, Header, Path

# ── Context marker ──────────────────────────────────────────────────────


class TestContextMarker:
    def test_create(self) -> None:
        ctx = Context("app")
        assert ctx.key == "app"

    def test_repr(self) -> None:
        ctx = Context("app")
        assert repr(ctx) == "Context('app')"

    def test_equality(self) -> None:
        assert Context("app") == Context("app")
        assert Context("app") != Context("other")
        assert Context("app") != "not a context"

    def test_hashable(self) -> None:
        s = {Context("app"), Context("app")}
        assert len(s) == 1

    def test_no_default_by_default(self) -> None:
        """H10: without default=, has_default is False."""
        ctx = Context("app")
        assert ctx.has_default is False
        assert ctx.default is _MISSING

    def test_explicit_default(self) -> None:
        """H10: default= (including an explicit None) makes has_default True."""
        ctx = Context("app", default="fallback")
        assert ctx.has_default is True
        assert ctx.default == "fallback"

        ctx_none = Context("app", default=None)
        assert ctx_none.has_default is True
        assert ctx_none.default is None

    def test_equality_considers_default(self) -> None:
        assert Context("app", default="a") == Context("app", default="a")
        assert Context("app", default="a") != Context("app", default="b")
        assert Context("app") != Context("app", default="a")


# ── Header marker ──────────────────────────────────────────────────────


class TestHeaderMarker:
    def test_create(self) -> None:
        h = Header("x-tenant")
        assert h.name == "x-tenant"

    def test_repr(self) -> None:
        h = Header("x-tenant")
        assert repr(h) == "Header('x-tenant')"

    def test_equality(self) -> None:
        assert Header("x-tenant") == Header("x-tenant")
        assert Header("x-tenant") != Header("x-other")
        assert Header("x-tenant") != "not a header"

    def test_hashable(self) -> None:
        s = {Header("x-tenant"), Header("x-tenant")}
        assert len(s) == 1

    def test_no_default_by_default(self) -> None:
        h = Header("x-tenant")
        assert h.has_default is False
        assert h.default is _MISSING

    def test_explicit_default(self) -> None:
        h = Header("x-tenant", default="anonymous")
        assert h.has_default is True
        assert h.default == "anonymous"

        h_none = Header("x-tenant", default=None)
        assert h_none.has_default is True
        assert h_none.default is None

    def test_equality_considers_default(self) -> None:
        assert Header("x-tenant", default="a") == Header("x-tenant", default="a")
        assert Header("x-tenant", default="a") != Header("x-tenant", default="b")
        assert Header("x-tenant") != Header("x-tenant", default="a")


# ── Path marker ────────────────────────────────────────────────────────


class TestPathMarker:
    def test_create(self) -> None:
        p = Path("level")
        assert p.segment == "level"

    def test_repr(self) -> None:
        p = Path("level")
        assert repr(p) == "Path('level')"

    def test_equality(self) -> None:
        assert Path("level") == Path("level")
        assert Path("level") != Path("other")
        assert Path("level") != "not a path"

    def test_hashable(self) -> None:
        s = {Path("level"), Path("level")}
        assert len(s) == 1

    def test_no_default_by_default(self) -> None:
        p = Path("level")
        assert p.has_default is False
        assert p.default is _MISSING

    def test_explicit_default(self) -> None:
        p = Path("level", default="unknown")
        assert p.has_default is True
        assert p.default == "unknown"

        p_none = Path("level", default=None)
        assert p_none.has_default is True
        assert p_none.default is None

    def test_equality_considers_default(self) -> None:
        assert Path("level", default="a") == Path("level", default="a")
        assert Path("level", default="a") != Path("level", default="b")
        assert Path("level") != Path("level", default="a")


class TestMissingSentinel:
    def test_repr(self) -> None:
        assert repr(_MISSING) == "<no default>"


# ── ContextRepo ─────────────────────────────────────────────────────────


class TestContextRepo:
    def test_set_get_global(self) -> None:
        repo = ContextRepo()
        repo.set_global("app", "my-app")
        assert repo.get("app") == "my-app"

    def test_get_missing_default(self) -> None:
        repo = ContextRepo()
        assert repo.get("missing") is None
        assert repo.get("missing", "default") == "default"

    def test_set_get_local(self) -> None:
        repo = ContextRepo()
        repo.set_local("request_id", "req-123")
        assert repo.get("request_id") == "req-123"

    def test_local_overrides_global(self) -> None:
        repo = ContextRepo()
        repo.set_global("key", "global-value")
        repo.set_local("key", "local-value")
        assert repo.get("key") == "local-value"

    def test_clear_local(self) -> None:
        repo = ContextRepo()
        repo.set_global("key", "global")
        repo.set_local("key", "local")
        repo.clear_local()
        assert repo.get("key") == "global"

    def test_has_global(self) -> None:
        repo = ContextRepo()
        repo.set_global("key", "value")
        assert repo.has("key") is True
        assert repo.has("missing") is False

    def test_has_local(self) -> None:
        repo = ContextRepo()
        repo.set_local("key", "value")
        assert repo.has("key") is True

    def test_thread_isolation(self) -> None:
        repo = ContextRepo()
        repo.set_global("shared", "global")
        results: dict[str, str | None] = {}

        def thread_work(name: str) -> None:
            repo.set_local("thread_key", name)
            results[name] = repo.get("thread_key")

        t1 = threading.Thread(target=thread_work, args=("t1",))
        t2 = threading.Thread(target=thread_work, args=("t2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert results["t1"] == "t1"
        assert results["t2"] == "t2"
