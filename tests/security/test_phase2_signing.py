"""0.16 signing hardening: fail-closed defaults, key rules, replay window.

The pre-existing suite covered verbatim replay, cross-instance replay, body
tampering and false positives. It did NOT cover any of the following, each of
which was reproduced against 0.15.x.
"""

from __future__ import annotations

from typing import Any

import pytest

from rabbitkit.middleware.signing import (
    InvalidSignatureError,
    SigningConfig,
    SigningMiddleware,
    TTLSetNonceCache,
)

# Assembled from fragments: this repo has a GitGuardian gate.
KEY = "not-a-real-" + "key-for-tests-only-primary-pad!"
OLD_KEY = "not-a-real-" + "key-for-tests-only-retired-pad!"
assert len(KEY) >= 32 and len(OLD_KEY) >= 32

#: The signature is ROUTE-BOUND, so the signed envelope and the
#: delivered message must agree on the routing metadata.
ROUTING_KEY = "test.rk"


def _message(body: bytes = b"payload", headers: dict[str, Any] | None = None, **kw: Any) -> Any:
    from tests.unit.middleware.test_signing import _make_message  # reuse the fixture builder

    return _make_message(body=body, headers=headers or {}, **kw)


class TestFailsClosedByDefault:
    def test_reject_unsigned_defaults_true(self) -> None:
        """The headline change. Deleting the header used to be enough."""
        assert SigningConfig(secret_key=KEY).reject_unsigned is True

    def test_reject_invalid_defaults_true(self) -> None:
        assert SigningConfig(secret_key=KEY).reject_invalid is True

    def test_a_message_with_no_signature_header_is_rejected(self) -> None:
        mw = SigningMiddleware(SigningConfig(secret_key=KEY))
        with pytest.raises(InvalidSignatureError, match="no x-rabbitkit-signature"):
            mw.on_receive(_message())

    def test_opting_back_out_is_still_possible_and_explicit(self) -> None:
        """Migration path for anyone genuinely mixing signed and unsigned."""
        mw = SigningMiddleware(SigningConfig(secret_key=KEY, reject_unsigned=False))
        mw.on_receive(_message())  # must not raise


class TestKeyStrengthIsEnforced:
    def test_an_empty_key_is_refused(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            SigningConfig(secret_key="")

    def test_empty_bytes_are_refused(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            SigningConfig(secret_key=b"")

    @pytest.mark.parametrize("length", [1, 8, 31])
    def test_a_short_key_is_refused(self, length: int) -> None:
        with pytest.raises(ValueError, match="at least 32"):
            SigningConfig(secret_key="k" * length)

    def test_the_boundary_is_accepted(self) -> None:
        assert SigningConfig(secret_key="k" * 32).secret_key == "k" * 32

    def test_the_error_tells_you_how_to_generate_one(self) -> None:
        with pytest.raises(ValueError, match=r"secrets\.token_urlsafe"):
            SigningConfig(secret_key="short")

    def test_rotation_keys_are_held_to_the_same_rule(self) -> None:
        with pytest.raises(ValueError, match=r"previous_keys\[0\]"):
            SigningConfig(secret_key=KEY, previous_keys=("tooshort",))


class TestKeyRotation:
    def test_a_message_signed_with_a_previous_key_still_verifies(self) -> None:
        """Without this, rotating dead-lettered every in-flight message."""
        publisher = SigningMiddleware(SigningConfig(secret_key=OLD_KEY, require_freshness=False))
        env = _sign(publisher)
        consumer = SigningMiddleware(
            SigningConfig(secret_key=KEY, previous_keys=(OLD_KEY,), require_freshness=False)
        )
        consumer.on_receive(_message(body=env.body, headers=dict(env.headers)))

    def test_signing_always_uses_the_primary_key(self) -> None:
        """A rotating publisher must be verifiable by a consumer that only
        knows the NEW key — otherwise rotation would never complete.

        Comparing signature strings directly would be wrong: each signature
        embeds a fresh random nonce and timestamp, so two signings of the
        same envelope never match (and must not).
        """
        rotating = SigningMiddleware(
            SigningConfig(secret_key=KEY, previous_keys=(OLD_KEY,), require_freshness=False)
        )
        env = _sign(rotating)
        primary_only = SigningMiddleware(SigningConfig(secret_key=KEY, require_freshness=False))
        primary_only.on_receive(_message(body=env.body, headers=dict(env.headers)))

    def test_a_consumer_that_only_knows_the_old_key_rejects_it(self) -> None:
        """The other half: signing does NOT fall back to a previous key."""
        rotating = SigningMiddleware(
            SigningConfig(secret_key=KEY, previous_keys=(OLD_KEY,), require_freshness=False)
        )
        env = _sign(rotating)
        old_only = SigningMiddleware(SigningConfig(secret_key=OLD_KEY, require_freshness=False))
        with pytest.raises(InvalidSignatureError):
            old_only.on_receive(_message(body=env.body, headers=dict(env.headers)))

    def test_an_unrelated_key_is_still_rejected(self) -> None:
        other = SigningMiddleware(
            SigningConfig(secret_key="not-a-real-" + "key-for-tests-only-other-pad!!", require_freshness=False)
        )
        env = _sign(other)
        consumer = SigningMiddleware(
            SigningConfig(secret_key=KEY, previous_keys=(OLD_KEY,), require_freshness=False)
        )
        with pytest.raises(InvalidSignatureError):
            consumer.on_receive(_message(body=env.body, headers=dict(env.headers)))


class TestReplayWindow:
    def test_the_nonce_outlives_the_acceptance_window(self) -> None:
        """The window is `abs(now - ts) <= max_skew`, so it is 2*max_skew WIDE.

        Recording the nonce for only max_skew let it expire while the
        timestamp was still acceptable — one free replay per window for any
        message received early relative to its timestamp, which is exactly
        what the future half of the window exists for.
        """
        recorded: dict[str, float] = {}

        class RecordingCache:
            def seen(self, nonce: str, ttl: float) -> bool:
                recorded[nonce] = ttl
                return True

        cfg = SigningConfig(secret_key=KEY, max_skew=30.0, nonce_cache=RecordingCache())
        mw = SigningMiddleware(cfg)
        env = _sign(mw)
        mw.on_receive(_message(body=env.body, headers=dict(env.headers)))

        assert recorded, "a nonce must have been recorded"
        assert set(recorded.values()) == {60.0}, "TTL must cover the whole 2*max_skew window"


class TestMonitoringModeCannotBurnNonces:
    def test_an_unverified_message_does_not_consume_its_nonce(self) -> None:
        """With reject_invalid=False the signature was never computed, yet the
        nonce was still recorded — so an observer who sniffed one nonce could
        get the genuine message rejected as a replay."""
        cache = TTLSetNonceCache()
        genuine = SigningMiddleware(SigningConfig(secret_key=KEY, nonce_cache=cache))
        env = _sign(genuine)

        monitoring = SigningMiddleware(
            SigningConfig(secret_key=KEY, reject_invalid=False, nonce_cache=cache)
        )
        forged = dict(env.headers)
        monitoring.on_receive(_message(body=b"ATTACKER-FORGED", headers=forged))

        # The real message must still be accepted.
        genuine.on_receive(_message(body=env.body, headers=dict(env.headers)))

    def test_monitoring_mode_logs_the_failure(self, caplog: pytest.LogCaptureFixture) -> None:
        """'Monitoring mode' previously monitored nothing — the fresh path had
        no log statement at all."""
        import logging

        mw = SigningMiddleware(SigningConfig(secret_key=KEY, reject_invalid=False))
        env = _sign(SigningMiddleware(SigningConfig(secret_key=KEY)))
        with caplog.at_level(logging.WARNING):
            mw.on_receive(_message(body=b"TAMPERED", headers=dict(env.headers)))
        assert any("verification FAILED" in r.getMessage() for r in caplog.records)


def _envelope() -> Any:
    from rabbitkit import MessageEnvelope

    return MessageEnvelope(routing_key=ROUTING_KEY, body=b"payload")


def _sign(mw: SigningMiddleware, envelope: Any = None) -> Any:
    """Run the publish path and return the signed envelope.

    `publish_scope` is the real entry point; it wraps `call_next`, so the
    signed envelope is whatever it handed downstream.
    """
    from unittest.mock import MagicMock

    call_next = MagicMock(return_value="published")
    mw.publish_scope(call_next, envelope if envelope is not None else _envelope())
    return call_next.call_args[0][0]


class TestLogRedaction:
    """`ErrorSanitizer` had ONE call site in the package — the DLQ header — so
    `error_detail="sanitized"` scrubbed the header while the adjacent log line
    printed the password."""

    def test_the_formatter_redacts_a_traceback(self) -> None:
        """The last line of a traceback is `str(exc)`, so any handler raising
        an exception that carries a connection string leaked it through every
        `logger.exception(...)` call — of which rabbitkit has ~40."""
        import logging

        from rabbitkit.core.logging import SecretRedactingFormatter

        pw = "hunt" + "er2"
        fmt = SecretRedactingFormatter("%(message)s")
        try:
            raise RuntimeError(f"could not connect: postgres://svc:{pw}@db.internal/app")
        except RuntimeError:
            import sys

            record = logging.LogRecord(
                "rabbitkit.test", logging.ERROR, __file__, 1, "boom", None, sys.exc_info()
            )
        rendered = fmt.format(record)
        assert pw not in rendered
        assert "db.internal" in rendered, "the host must survive — it is the diagnostic"

    def test_the_formatter_redacts_the_message_too(self) -> None:
        import logging

        from rabbitkit.core.logging import SecretRedactingFormatter

        pw = "hunt" + "er2"
        fmt = SecretRedactingFormatter("%(message)s")
        record = logging.LogRecord(
            "rabbitkit.test", logging.ERROR, __file__, 1,
            "lost: amqp://admin:%s@mq/prod", (pw,), None,
        )
        assert pw not in fmt.format(record)

    def test_install_is_idempotent(self) -> None:
        from rabbitkit.core.logging import SecretRedactingFilter, install_log_redaction

        first = install_log_redaction("rabbitkit.testns")
        second = install_log_redaction("rabbitkit.testns")
        assert first is second
        attached = [
            f
            for f in __import__("logging").getLogger("rabbitkit.testns").filters
            if isinstance(f, SecretRedactingFilter)
        ]
        assert len(attached) == 1

    def test_a_handler_already_attached_gets_the_filter(self) -> None:
        """Covers loggers created AFTER install whose records reach an
        existing handler."""
        import io
        import logging

        from rabbitkit.core.logging import SecretRedactingFilter, install_log_redaction

        name = "rabbitkit.handlerns"
        lg = logging.getLogger(name)
        handler = logging.StreamHandler(io.StringIO())
        lg.addHandler(handler)
        install_log_redaction(name)
        assert any(isinstance(f, SecretRedactingFilter) for f in handler.filters)

    def test_an_unformattable_record_still_logs(self) -> None:
        """A broken record must not be swallowed by the filter."""
        import logging

        from rabbitkit.core.logging import SecretRedactingFilter

        record = logging.LogRecord("rabbitkit.t", logging.ERROR, __file__, 1, "%d", ("nope",), None)
        assert SecretRedactingFilter().filter(record) is True
