"""Tests for core/errors.py — error classification."""

from __future__ import annotations

import json

import pytest

from rabbitkit.core.errors import (
    PERMANENT_ERRORS,
    TRANSIENT_ERRORS,
    ErrorPredicate,
    classify_error,
)
from rabbitkit.core.types import ErrorSeverity

# ── transient errors ──────────────────────────────────────────────────────


class TestTransientErrors:
    @pytest.mark.parametrize(
        "error_cls",
        [
            ConnectionResetError,
            BrokenPipeError,
            ConnectionAbortedError,
            TimeoutError,
            EOFError,
        ],
    )
    def test_stdlib_transient_errors(self, error_cls: type[BaseException]) -> None:
        exc = error_cls("test")
        result = classify_error(exc)
        assert result.severity == ErrorSeverity.TRANSIENT
        assert result.original is exc
        assert "transient" in result.reason

    def test_os_error_is_transient(self) -> None:
        result = classify_error(OSError("network"))
        assert result.severity == ErrorSeverity.TRANSIENT


# ── permanent errors ──────────────────────────────────────────────────────


class TestPermanentErrors:
    @pytest.mark.parametrize(
        "error_cls",
        [
            KeyError,
            ValueError,
            TypeError,
            UnicodeDecodeError,
            AttributeError,
        ],
    )
    def test_stdlib_permanent_errors(self, error_cls: type[BaseException]) -> None:
        if error_cls is UnicodeDecodeError:
            exc = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid")
        else:
            exc = error_cls("test")
        result = classify_error(exc)
        assert result.severity == ErrorSeverity.PERMANENT
        assert result.original is exc
        assert "permanent" in result.reason

    def test_json_decode_error_is_permanent(self) -> None:
        exc = json.JSONDecodeError("bad json", "{", 0)
        result = classify_error(exc)
        assert result.severity == ErrorSeverity.PERMANENT


# ── unknown errors ────────────────────────────────────────────────────────


class TestUnknownErrors:
    def test_unknown_defaults_to_permanent(self) -> None:
        """Default unknown_policy is PERMANENT — safe default."""
        exc = RuntimeError("something unexpected")
        result = classify_error(exc)
        assert result.severity == ErrorSeverity.PERMANENT
        assert "unknown" in result.reason

    def test_unknown_policy_override(self) -> None:
        exc = RuntimeError("hmm")
        result = classify_error(exc, unknown_policy=ErrorSeverity.TRANSIENT)
        assert result.severity == ErrorSeverity.TRANSIENT
        assert "unknown" in result.reason

    def test_custom_exception_is_unknown(self) -> None:
        class MyError(Exception):
            pass

        result = classify_error(MyError("custom"))
        assert result.severity == ErrorSeverity.PERMANENT  # default unknown_policy


# ── predicates ────────────────────────────────────────────────────────────


class TestPredicates:
    def test_predicate_true_is_transient(self) -> None:
        exc = RuntimeError("retryable")
        predicate: ErrorPredicate = lambda e: isinstance(e, RuntimeError)
        result = classify_error(exc, predicates=[predicate])
        assert result.severity == ErrorSeverity.TRANSIENT
        assert "predicate" in result.reason

    def test_predicate_false_is_permanent(self) -> None:
        exc = RuntimeError("fatal")
        predicate: ErrorPredicate = lambda e: False
        result = classify_error(exc, predicates=[predicate])
        assert result.severity == ErrorSeverity.PERMANENT
        assert "predicate" in result.reason

    def test_predicate_none_falls_through(self) -> None:
        exc = RuntimeError("dunno")
        predicate: ErrorPredicate = lambda e: None
        result = classify_error(exc, predicates=[predicate])
        # Falls through to unknown_policy (PERMANENT)
        assert result.severity == ErrorSeverity.PERMANENT
        assert "unknown" in result.reason

    def test_first_predicate_wins(self) -> None:
        exc = RuntimeError("test")
        p1: ErrorPredicate = lambda e: True  # transient
        p2: ErrorPredicate = lambda e: False  # permanent (never reached)
        result = classify_error(exc, predicates=[p1, p2])
        assert result.severity == ErrorSeverity.TRANSIENT

    def test_predicate_takes_precedence_over_tuple(self) -> None:
        """Predicates run before isinstance checks."""
        exc = ValueError("actually retryable")
        predicate: ErrorPredicate = lambda e: True  # override to transient
        result = classify_error(exc, predicates=[predicate])
        assert result.severity == ErrorSeverity.TRANSIENT  # even though ValueError is permanent

    def test_multiple_predicates_skip_none(self) -> None:
        exc = RuntimeError("test")
        p1: ErrorPredicate = lambda e: None  # no opinion
        p2: ErrorPredicate = lambda e: True  # transient
        result = classify_error(exc, predicates=[p1, p2])
        assert result.severity == ErrorSeverity.TRANSIENT

    def test_empty_predicates(self) -> None:
        exc = ConnectionResetError("reset")
        result = classify_error(exc, predicates=[])
        assert result.severity == ErrorSeverity.TRANSIENT


# ── custom tuples ─────────────────────────────────────────────────────────


class TestCustomTuples:
    def test_custom_transient_tuple(self) -> None:
        class RetryableError(Exception):
            pass

        exc = RetryableError("retry me")
        result = classify_error(exc, transient=(RetryableError,))
        assert result.severity == ErrorSeverity.TRANSIENT

    def test_custom_permanent_tuple(self) -> None:
        class FatalError(Exception):
            pass

        exc = FatalError("done")
        result = classify_error(exc, permanent=(FatalError,))
        assert result.severity == ErrorSeverity.PERMANENT

    def test_transient_checked_before_permanent(self) -> None:
        """If exception matches both tuples, transient wins."""
        exc = OSError("could be both")
        result = classify_error(
            exc,
            transient=(OSError,),
            permanent=(OSError,),  # also listed here
        )
        assert result.severity == ErrorSeverity.TRANSIENT


# ── tuple contents ────────────────────────────────────────────────────────


class TestTupleContents:
    def test_transient_errors_tuple(self) -> None:
        # ConnectionResetError is covered by OSError (isinstance), not listed explicitly
        assert issubclass(ConnectionResetError, OSError)
        assert issubclass(BrokenPipeError, OSError)
        assert TimeoutError in TRANSIENT_ERRORS
        assert EOFError in TRANSIENT_ERRORS
        assert OSError in TRANSIENT_ERRORS

    def test_permanent_errors_tuple(self) -> None:
        assert json.JSONDecodeError in PERMANENT_ERRORS
        assert KeyError in PERMANENT_ERRORS
        assert ValueError in PERMANENT_ERRORS
        assert TypeError in PERMANENT_ERRORS


# ── custom validation / runtime error taxonomy ───────────────────────────


class TestCustomErrorTaxonomy:
    """Each custom error must (a) be raised at its site and (b) keep the
    builtin base class it replaced, so pre-existing ``except ValueError`` /
    ``except RuntimeError`` handlers (and tests) keep working unchanged.
    """

    def test_inheritance_contract(self) -> None:
        from rabbitkit.core.errors import (
            BrokerNotStartedError,
            ConfigurationError,
            ConfigValidationError,
            MessageTooLargeError,
            SettlementError,
            TopologyValidationError,
        )

        assert issubclass(ConfigValidationError, ValueError)
        assert issubclass(ConfigValidationError, ConfigurationError)
        assert issubclass(TopologyValidationError, ValueError)
        assert issubclass(TopologyValidationError, ConfigurationError)
        assert issubclass(MessageTooLargeError, ValueError)
        assert issubclass(BrokerNotStartedError, RuntimeError)
        assert issubclass(SettlementError, RuntimeError)

    def test_all_exported_at_top_level(self) -> None:
        import rabbitkit

        for name in (
            "ConfigValidationError",
            "TopologyValidationError",
            "MessageTooLargeError",
            "BrokerNotStartedError",
            "SettlementError",
        ):
            assert hasattr(rabbitkit, name), name
            assert name in rabbitkit.__all__, name

    def test_config_validation_error_raised(self) -> None:
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.core.errors import ConfigValidationError

        with pytest.raises(ConfigValidationError):
            RetryConfig(max_retries=-1)

    def test_shortstr_validation_raises_config_validation_error(self) -> None:
        from rabbitkit.core.errors import ConfigValidationError
        from rabbitkit.core.types import validate_amqp_shortstr

        with pytest.raises(ConfigValidationError, match="shortstr"):
            validate_amqp_shortstr("Queue name", "q" * 256)

    def test_topology_validation_error_raised(self) -> None:
        from rabbitkit.core.errors import TopologyValidationError
        from rabbitkit.core.topology import RabbitQueue
        from rabbitkit.core.types import QueueType

        with pytest.raises(TopologyValidationError):
            RabbitQueue(name="q", queue_type=QueueType.QUORUM, durable=False)
        with pytest.raises(TopologyValidationError):
            RabbitQueue(name="q", consumer_timeout=0)

    def test_settlement_error_raised_on_sync_ack_of_async_message(self) -> None:
        from rabbitkit.core.errors import SettlementError
        from rabbitkit.core.message import RabbitMessage

        msg = RabbitMessage(body=b"x", routing_key="rk")  # no settlement fns wired
        with pytest.raises(SettlementError):
            msg.ack()

    def test_old_catches_still_work(self) -> None:
        """The exact backward-compat promise: bare builtin catches see them."""
        from rabbitkit.core.config import RetryConfig
        from rabbitkit.core.message import RabbitMessage
        from rabbitkit.core.topology import RabbitQueue

        with pytest.raises(ValueError):
            RetryConfig(max_retries=-1)
        with pytest.raises(ValueError):
            RabbitQueue(name="q", consumer_timeout=-5)
        with pytest.raises(RuntimeError):
            RabbitMessage(body=b"x", routing_key="rk").ack()
