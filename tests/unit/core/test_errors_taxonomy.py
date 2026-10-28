"""The failure taxonomy: ErrorCategory, ClassifiedError.category and
how classify_error assigns it."""

from __future__ import annotations

import asyncio
import json

import pytest

from rabbitkit.core.errors import (
    HandlerTimeoutError,
    InfrastructureError,
    InvalidMessageError,
    MissingDependencyError,
    classify_error,
    infrastructure_errors,
    mark_invalid_message,
    register_infrastructure_errors,
)
from rabbitkit.core.types import ClassifiedError, ErrorCategory, ErrorSeverity


class _BrokerDown(Exception):
    """Stand-in for a transport's connection error (core never imports one)."""


class TestErrorCategory:
    @pytest.mark.parametrize(
        ("category", "severity"),
        [
            (ErrorCategory.RETRYABLE, ErrorSeverity.TRANSIENT),
            (ErrorCategory.PERMANENT, ErrorSeverity.PERMANENT),
            (ErrorCategory.INVALID_MESSAGE, ErrorSeverity.PERMANENT),
            (ErrorCategory.UNKNOWN_OUTCOME, ErrorSeverity.TRANSIENT),
            (ErrorCategory.INFRASTRUCTURE, ErrorSeverity.TRANSIENT),
            (ErrorCategory.SHUTDOWN, ErrorSeverity.TRANSIENT),
        ],
    )
    def test_each_category_implies_a_severity(self, category: ErrorCategory, severity: ErrorSeverity) -> None:
        assert category.severity is severity

    def test_values_are_bounded_snake_case(self) -> None:
        assert {c.value for c in ErrorCategory} == {
            "retryable",
            "permanent",
            "invalid_message",
            "unknown_outcome",
            "infrastructure",
            "shutdown",
        }


class TestClassifiedErrorCategory:
    def test_derived_from_severity_when_omitted(self) -> None:
        exc = RuntimeError()
        assert ClassifiedError(ErrorSeverity.TRANSIENT, exc, "r").category is ErrorCategory.RETRYABLE
        assert ClassifiedError(ErrorSeverity.PERMANENT, exc, "r").category is ErrorCategory.PERMANENT

    def test_a_contradicting_category_yields_to_severity(self) -> None:
        c = ClassifiedError(ErrorSeverity.TRANSIENT, RuntimeError(), "r", category=ErrorCategory.INVALID_MESSAGE)
        assert c.category is ErrorCategory.RETRYABLE

    def test_outcome_unknown_refines_retryable(self) -> None:
        c = ClassifiedError(ErrorSeverity.TRANSIENT, RuntimeError(), "r", outcome_unknown=True)
        assert c.category is ErrorCategory.UNKNOWN_OUTCOME

    def test_explicit_consistent_category_is_kept(self) -> None:
        c = ClassifiedError(ErrorSeverity.TRANSIENT, RuntimeError(), "r", category=ErrorCategory.INFRASTRUCTURE)
        assert c.category is ErrorCategory.INFRASTRUCTURE


class TestClassifyCategory:
    @pytest.mark.parametrize("exc", [asyncio.CancelledError(), KeyboardInterrupt(), SystemExit(0)])
    def test_shutdown(self, exc: BaseException) -> None:
        c = classify_error(exc, predicates=[lambda e: False])
        assert c.category is ErrorCategory.SHUTDOWN
        assert c.severity is ErrorSeverity.TRANSIENT

    def test_stdlib_transient_is_retryable(self) -> None:
        assert classify_error(ConnectionResetError()).category is ErrorCategory.RETRYABLE

    def test_unclassified_default_stays_permanent(self) -> None:
        c = classify_error(RuntimeError("?"))
        assert (c.severity, c.category) == (ErrorSeverity.PERMANENT, ErrorCategory.PERMANENT)

    def test_unknown_policy_transient_is_retryable(self) -> None:
        c = classify_error(RuntimeError("?"), unknown_policy=ErrorSeverity.TRANSIENT)
        assert c.category is ErrorCategory.RETRYABLE

    def test_handler_timeout_is_unknown_outcome(self) -> None:
        c = classify_error(HandlerTimeoutError(2.0))
        assert c.category is ErrorCategory.UNKNOWN_OUTCOME and c.outcome_unknown

    def test_permanent_timeout_keeps_permanent_category(self) -> None:
        c = classify_error(HandlerTimeoutError(2.0), predicates=[lambda e: False])
        assert c.category is ErrorCategory.PERMANENT and c.outcome_unknown

    def test_typed_exceptions_declare_their_category(self) -> None:
        assert classify_error(InvalidMessageError("x")).category is ErrorCategory.INVALID_MESSAGE
        assert classify_error(InfrastructureError("x")).category is ErrorCategory.INFRASTRUCTURE
        assert classify_error(InfrastructureError("x")).severity is ErrorSeverity.TRANSIENT
        assert classify_error(MissingDependencyError("Header('x')", "x")).category is ErrorCategory.INVALID_MESSAGE

    def test_a_declared_category_beats_predicates(self) -> None:
        c = classify_error(InvalidMessageError("x"), predicates=[lambda e: True])
        assert c.severity is ErrorSeverity.PERMANENT

    def test_predicate_may_return_a_category(self) -> None:
        c = classify_error(RuntimeError("db"), predicates=[lambda e: ErrorCategory.INFRASTRUCTURE])
        assert (c.severity, c.category) == (ErrorSeverity.TRANSIENT, ErrorCategory.INFRASTRUCTURE)
        assert "predicate classified as infrastructure" in c.reason

    def test_bool_predicates_keep_their_meaning(self) -> None:
        assert classify_error(ValueError(), predicates=[lambda e: True]).category is ErrorCategory.RETRYABLE
        assert classify_error(OSError(), predicates=[lambda e: False]).category is ErrorCategory.PERMANENT

    def test_explicit_infrastructure_tuple(self) -> None:
        c = classify_error(_BrokerDown(), infrastructure=(_BrokerDown,))
        assert (c.severity, c.category) == (ErrorSeverity.TRANSIENT, ErrorCategory.INFRASTRUCTURE)

    def test_explicit_empty_tuple_ignores_the_registry(self) -> None:
        assert classify_error(_BrokerDown(), infrastructure=()).severity is ErrorSeverity.PERMANENT


class TestRegistry:
    def test_registered_types_classify_as_infrastructure(self) -> None:
        class _Registered(Exception):
            pass

        register_infrastructure_errors(_Registered)
        register_infrastructure_errors(_Registered)  # idempotent
        assert infrastructure_errors().count(_Registered) == 1
        c = classify_error(_Registered())
        assert (c.severity, c.category) == (ErrorSeverity.TRANSIENT, ErrorCategory.INFRASTRUCTURE)

    def test_registering_nothing_new_is_a_no_op(self) -> None:
        before = infrastructure_errors()
        register_infrastructure_errors(*before)
        assert infrastructure_errors() == before

    def test_brokers_register_their_client_errors(self) -> None:
        aiormq_exceptions = pytest.importorskip("aiormq.exceptions")
        pika_exceptions = pytest.importorskip("pika.exceptions")
        from rabbitkit.async_.broker import AsyncBroker
        from rabbitkit.sync.broker import SyncBroker

        AsyncBroker()
        SyncBroker()
        assert classify_error(aiormq_exceptions.ChannelClosed()).category is ErrorCategory.INFRASTRUCTURE
        assert classify_error(aiormq_exceptions.ChannelInvalidStateError()).severity is ErrorSeverity.TRANSIENT
        # pika's errors are not OSError subclasses: they used to classify PERMANENT
        assert classify_error(pika_exceptions.StreamLostError()).category is ErrorCategory.INFRASTRUCTURE


class TestMarkInvalidMessage:
    def test_tags_an_untagged_exception(self) -> None:
        exc = json.JSONDecodeError("bad", "{", 0)
        mark_invalid_message(exc)
        assert classify_error(exc, predicates=[lambda e: True]).category is ErrorCategory.INVALID_MESSAGE

    def test_keeps_a_declared_category(self) -> None:
        exc = InfrastructureError("x")
        mark_invalid_message(exc)
        assert classify_error(exc).category is ErrorCategory.INFRASTRUCTURE

    def test_tolerates_exceptions_that_refuse_attributes(self) -> None:
        class _Frozen(Exception):
            def __setattr__(self, name: str, value: object) -> None:
                raise AttributeError(name)

        exc = _Frozen()
        mark_invalid_message(exc)  # no raise
        assert classify_error(exc).category is ErrorCategory.PERMANENT
