"""Tests for middleware/error_classifier.py — ErrorClassifierMiddleware."""

from __future__ import annotations

from rabbitkit.core.types import ErrorSeverity
from rabbitkit.middleware.error_classifier import ErrorClassifierMiddleware


class TestErrorClassifierMiddleware:
    def test_transient_error(self) -> None:
        classifier = ErrorClassifierMiddleware()
        result = classifier.classify(ConnectionResetError("lost"))
        assert result.severity == ErrorSeverity.TRANSIENT

    def test_permanent_error(self) -> None:
        classifier = ErrorClassifierMiddleware()
        result = classifier.classify(ValueError("bad"))
        assert result.severity == ErrorSeverity.PERMANENT

    def test_unknown_defaults_permanent(self) -> None:
        classifier = ErrorClassifierMiddleware()
        result = classifier.classify(RuntimeError("unknown"))
        assert result.severity == ErrorSeverity.PERMANENT

    def test_unknown_policy_override(self) -> None:
        classifier = ErrorClassifierMiddleware(unknown_policy=ErrorSeverity.TRANSIENT)
        result = classifier.classify(RuntimeError("unknown"))
        assert result.severity == ErrorSeverity.TRANSIENT

    def test_predicate_true_is_transient(self) -> None:
        classifier = ErrorClassifierMiddleware(
            predicates=[lambda e: isinstance(e, RuntimeError)],
        )
        result = classifier.classify(RuntimeError("retryable"))
        assert result.severity == ErrorSeverity.TRANSIENT

    def test_predicate_false_is_permanent(self) -> None:
        classifier = ErrorClassifierMiddleware(
            predicates=[lambda e: False],
        )
        result = classifier.classify(RuntimeError("fatal"))
        assert result.severity == ErrorSeverity.PERMANENT

    def test_predicate_none_falls_through(self) -> None:
        classifier = ErrorClassifierMiddleware(
            predicates=[lambda e: None],
        )
        result = classifier.classify(ConnectionResetError("reset"))
        assert result.severity == ErrorSeverity.TRANSIENT  # falls through to tuple

    def test_unknown_policy_property(self) -> None:
        classifier = ErrorClassifierMiddleware(unknown_policy=ErrorSeverity.TRANSIENT)
        assert classifier.unknown_policy == ErrorSeverity.TRANSIENT
