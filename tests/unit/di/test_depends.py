"""Tests for di/depends.py — Depends marker."""

from __future__ import annotations

from rabbitkit.di.depends import Depends


def _get_db() -> str:
    return "db-session"


def _get_cache() -> str:
    return "cache-instance"


class TestDepends:
    def test_create(self) -> None:
        dep = Depends(_get_db)
        assert dep.dependency is _get_db
        assert dep.use_cache is True

    def test_no_cache(self) -> None:
        dep = Depends(_get_db, use_cache=False)
        assert dep.use_cache is False

    def test_repr(self) -> None:
        dep = Depends(_get_db)
        assert "_get_db" in repr(dep)
        assert "use_cache=True" in repr(dep)

    def test_equality(self) -> None:
        d1 = Depends(_get_db)
        d2 = Depends(_get_db)
        assert d1 == d2

    def test_inequality_different_func(self) -> None:
        d1 = Depends(_get_db)
        d2 = Depends(_get_cache)
        assert d1 != d2

    def test_inequality_different_cache(self) -> None:
        d1 = Depends(_get_db, use_cache=True)
        d2 = Depends(_get_db, use_cache=False)
        assert d1 != d2

    def test_inequality_other_type(self) -> None:
        d = Depends(_get_db)
        assert d != "not a depends"

    def test_hashable(self) -> None:
        d = Depends(_get_db)
        hash(d)  # should not raise

    def test_same_hash_for_equal(self) -> None:
        d1 = Depends(_get_db)
        d2 = Depends(_get_db)
        assert hash(d1) == hash(d2)
