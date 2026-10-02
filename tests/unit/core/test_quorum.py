"""Tests for core/quorum.py — delivery limits and when browsing is destructive.

The semantics table is the one measured against RabbitMQ 3.13.7 and 4.1.8
(issue #32); tests/integration/test_dlq_safety.py re-measures it live.
"""

from __future__ import annotations

from typing import Any

import pytest

from rabbitkit.core.errors import UnsafeToBrowseError
from rabbitkit.core.quorum import (
    DEFAULT_DELIVERY_LIMIT_4X,
    assert_browsable,
    broker_major_version,
    dlq_delivery_limit,
    effective_delivery_limit,
    is_quorum,
    unlimited_fix,
)


def _queue(*, qtype: str = "quorum", arg: Any = None, policy: Any = None, stats: bool = True) -> dict[str, Any]:
    arguments: dict[str, Any] = {"x-queue-type": qtype}
    if arg is not None:
        arguments["x-delivery-limit"] = arg
    info: dict[str, Any] = {
        "type": qtype,
        "arguments": arguments,
        "effective_policy_definition": {} if policy is None else {"delivery-limit": policy},
    }
    if stats:
        info["messages"] = 3
    return info


class TestBrokerMajorVersion:
    @pytest.mark.parametrize(
        ("version", "major"),
        [("4.1.8", 4), ("3.13.7", 3), ("10.0", 10), (" 4.0.0-rc.1", 4), ("", None), (None, None), ("x.y", None)],
    )
    def test_parse(self, version: str | None, major: int | None) -> None:
        assert broker_major_version(version) == major


class TestDlqDeliveryLimit:
    def test_unlimited_on_4x(self) -> None:
        assert dlq_delivery_limit("4.1.8") == -1

    def test_no_argument_on_3x_where_minus_one_drops(self) -> None:
        assert dlq_delivery_limit("3.13.7") is None

    def test_no_argument_when_version_unknown(self) -> None:
        assert dlq_delivery_limit(None) is None


class TestIsQuorum:
    def test_type_field(self) -> None:
        assert is_quorum({"type": "quorum"})
        assert not is_quorum({"type": "classic"})

    def test_falls_back_to_argument(self) -> None:
        assert is_quorum({"arguments": {"x-queue-type": "quorum"}})
        assert not is_quorum({})


class TestEffectiveDeliveryLimit:
    """The measured semantics (issue #32)."""

    @pytest.mark.parametrize(
        ("arg", "policy", "limit"),
        [
            (None, None, DEFAULT_DELIVERY_LIMIT_4X),  # 4.x default
            (-1, None, None),  # -1 = unlimited
            (None, -1, None),  # the policy fix for an existing queue
            (-1, -1, None),
            (5, None, 5),
            (None, 7, 7),
            (5, 7, 5),  # lowest non-negative wins
            (-1, 7, 7),
            (9, -1, 9),
            (0, None, 0),
        ],
    )
    def test_4x(self, arg: Any, policy: Any, limit: int | None) -> None:
        assert effective_delivery_limit(_queue(arg=arg, policy=policy), "4.1.8") == limit

    @pytest.mark.parametrize(
        ("arg", "policy", "limit"),
        [
            (None, None, None),  # 3.x: no default
            (-1, None, 0),  # 3.x: -1 drops on the first return
            (None, -1, 0),
            (5, None, 5),
            (5, 7, 5),
        ],
    )
    def test_3x(self, arg: Any, policy: Any, limit: int | None) -> None:
        assert effective_delivery_limit(_queue(arg=arg, policy=policy), "3.13.7") == limit

    def test_classic_has_none(self) -> None:
        assert effective_delivery_limit(_queue(qtype="classic", arg=None), "4.1.8") is None

    def test_policy_not_a_dict_is_ignored(self) -> None:
        info = _queue()
        info["effective_policy_definition"] = []
        assert effective_delivery_limit(info, "4.1.8") == DEFAULT_DELIVERY_LIMIT_4X

    def test_string_values_from_the_api(self) -> None:
        assert effective_delivery_limit(_queue(arg="-1"), "4.1.8") is None


class TestAssertBrowsable:
    def test_classic_is_always_browsable(self) -> None:
        assert_browsable("q", _queue(qtype="classic", stats=False), None)

    def test_unlimited_quorum_on_4x(self) -> None:
        assert_browsable("q", _queue(arg=-1), "4.1.8")

    def test_unlimited_quorum_on_3x(self) -> None:
        assert_browsable("q", _queue(), "3.13.7")

    def test_default_limit_on_4x_refuses(self) -> None:
        with pytest.raises(UnsafeToBrowseError, match="delivery limit of 20") as err:
            assert_browsable("orders.dlq", _queue(), "4.1.8")
        assert "set_policy" in str(err.value)

    def test_minus_one_on_3x_refuses_with_the_3x_fix(self) -> None:
        with pytest.raises(UnsafeToBrowseError, match="delivery limit of 0") as err:
            assert_browsable("orders.dlq", _queue(arg=-1), "3.13.7")
        assert "remove x-delivery-limit" in str(err.value)

    def test_unknown_version_fails_closed(self) -> None:
        with pytest.raises(UnsafeToBrowseError, match="version is unknown"):
            assert_browsable("q", _queue(arg=-1), None)

    def test_missing_statistics_fails_closed(self) -> None:
        """Until the first stats emission a policy-defined limit is invisible."""
        with pytest.raises(UnsafeToBrowseError, match="statistics"):
            assert_browsable("q", _queue(arg=-1, stats=False), "4.1.8")


class TestUnlimitedFix:
    def test_versions(self) -> None:
        assert "delivery-limit" in unlimited_fix("4.0.0")
        assert "-1 is not unlimited" in unlimited_fix("3.12.0")
        assert "set_policy" in unlimited_fix(None)
