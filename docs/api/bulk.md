# Bulk Operations & Reliability

See the [Bulk Operations guide](../bulk-operations.md) for the contract and
safety invariants.

## Bulk publishing

::: rabbitkit.core.bulk.BulkPublishOptions
::: rabbitkit.core.bulk.BulkPublishItem
::: rabbitkit.core.bulk.BulkPublishResult
::: rabbitkit.core.bulk.BulkPublishError
::: rabbitkit.core.types.BulkPublishStatus
::: rabbitkit.core.bulk.PublishPreparer
::: rabbitkit.core.bulk.classify_publish_outcome

## Selected settlement

::: rabbitkit.core.settlement.SettlementReport
::: rabbitkit.core.settlement.SettlementItem
::: rabbitkit.core.settlement.SettlementReportError
::: rabbitkit.core.types.SettlementItemStatus
::: rabbitkit.core.types.SettlementAction
::: rabbitkit.core.settlement.plan_selected_settlement

## Safe coalescing

::: rabbitkit.core.settlement.SettlementCoordinator
::: rabbitkit.core.settlement.SettlementCommand
::: rabbitkit.core.types.DeliveryState
::: rabbitkit.highload.batch.CoalescingAcker
::: rabbitkit.highload.batch.CoalescingFlushReport

## Reliability profiles & preflight

::: rabbitkit.core.types.ReliabilityProfile
::: rabbitkit.core.profiles.apply_profile
::: rabbitkit.core.profiles.validate_profile
::: rabbitkit.core.profiles.critical_config
::: rabbitkit.core.profiles.standard_config
::: rabbitkit.core.profiles.ProfileViolation
::: rabbitkit.core.profiles.preflight
::: rabbitkit.core.profiles.PreflightReport
::: rabbitkit.core.profiles.PreflightCheck
::: rabbitkit.core.types.PreflightStatus
::: rabbitkit.core.profiles.policy_templates
::: rabbitkit.core.profiles.PolicyTemplate

## Retry hardening

::: rabbitkit.core.config.RetryHandoffConfig
::: rabbitkit.core.retry_handoff.RetryHandoffTracker
::: rabbitkit.core.types.HandoffState
::: rabbitkit.core.sanitizer.ErrorSanitizer
::: rabbitkit.core.sanitizer.SanitizedError
