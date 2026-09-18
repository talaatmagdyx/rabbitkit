"""Public API surface for the 0.12 reliability/bulk symbols (top-level re-exports)."""

from __future__ import annotations

import pytest

import rabbitkit

NEW_SYMBOLS = [
    # bulk publishing
    "BulkPublishError",
    "BulkPublishItem",
    "BulkPublishOptions",
    "BulkPublishResult",
    "BulkPublishStatus",
    "PublishPreparer",
    # settlement
    "SettlementAction",
    "SettlementCommand",
    "SettlementCoordinator",
    "SettlementItem",
    "SettlementItemStatus",
    "SettlementReport",
    "SettlementReportError",
    "DeliveryState",
    # highload
    "BatchClosedError",
    "BatchFlushError",
    "CoalescingAcker",
    "CoalescingFlushReport",
    "FlushItem",
    "FlushReason",
    "FlushReport",
    # profiles / preflight
    "ReliabilityProfile",
    "PreflightStatus",
    "PreflightCheck",
    "PreflightReport",
    "ProfileViolation",
    "PolicyTemplate",
    "apply_profile",
    "critical_config",
    "standard_config",
    "validate_profile",
    "preflight",
    "policy_templates",
    # retry hardening
    "RetryHandoffConfig",
    "RetryHandoffTracker",
    "HandoffState",
    "ErrorSanitizer",
    "SanitizedError",
]


@pytest.mark.parametrize("name", NEW_SYMBOLS)
def test_symbol_exported_and_in_all(name: str) -> None:
    assert hasattr(rabbitkit, name), name
    assert name in rabbitkit.__all__, name


def test_all_is_unique() -> None:
    assert len(rabbitkit.__all__) == len(set(rabbitkit.__all__))


def test_every_all_entry_resolves() -> None:
    for name in rabbitkit.__all__:
        assert getattr(rabbitkit, name, None) is not None, name


def test_top_level_and_canonical_are_same_objects() -> None:
    from rabbitkit.core.bulk import BulkPublishOptions
    from rabbitkit.core.settlement import SettlementCoordinator
    from rabbitkit.core.types import BulkPublishStatus, SettlementItemStatus
    from rabbitkit.highload.batch import CoalescingAcker

    assert rabbitkit.BulkPublishOptions is BulkPublishOptions
    assert rabbitkit.SettlementCoordinator is SettlementCoordinator
    assert rabbitkit.BulkPublishStatus is BulkPublishStatus
    assert rabbitkit.SettlementItemStatus is SettlementItemStatus
    assert rabbitkit.CoalescingAcker is CoalescingAcker


def test_highload_package_exports() -> None:
    from rabbitkit import highload

    for name in (
        "BatchAcker",
        "BatchPublisher",
        "CoalescingAcker",
        "FlushReport",
        "BatchFlushError",
        "BatchClosedError",
    ):
        assert name in highload.__all__ and hasattr(highload, name)


def test_enum_values_are_metric_label_safe() -> None:
    """Every new enum value is a short lowercase identifier — safe as a
    Prometheus label value and stable for dashboards."""
    import re

    for enum_cls in (
        rabbitkit.BulkPublishStatus,
        rabbitkit.SettlementItemStatus,
        rabbitkit.SettlementAction,
        rabbitkit.DeliveryState,
        rabbitkit.FlushReason,
        rabbitkit.ReliabilityProfile,
        rabbitkit.PreflightStatus,
        rabbitkit.HandoffState,
    ):
        for member in enum_cls:
            assert re.fullmatch(r"[a-z_]{1,32}", member.value), (enum_cls, member)
            assert isinstance(member, str)
