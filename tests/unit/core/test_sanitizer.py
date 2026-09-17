"""Tests for core/sanitizer.py — secret-free, bounded error metadata."""

from __future__ import annotations

import re

import pytest

from rabbitkit.core.sanitizer import (
    ERROR_DETAIL_POLICIES,
    ErrorSanitizer,
    SanitizedError,
    contains_secret_marker,
)
from rabbitkit.core.types import ErrorSeverity

# Fake fixtures (assembled so they never look like a real leaked credential
# to secret scanners): a password with an unescaped "@", the AWS docs example
# key id, an unsigned demo JWT, and a 40-hex token.
PW = "s3cr3t" + "P@ss"
AWS_KEY_ID = "AKIA" + "IOSFODNN7EXAMPLE"
JWT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." + "eyJzdWIiOiIxMjM0NTY3ODkwIn0"
HEX_TOKEN = "deadbeef" * 5
SECRETS = [PW, AWS_KEY_ID, JWT, HEX_TOKEN]


class TestSanitizedError:
    def test_as_headers_omits_empty_summary(self) -> None:
        h = SanitizedError(category="transient", code="TimeoutError").as_headers()
        assert h == {"x-rabbitkit-error-category": "transient", "x-rabbitkit-error-type": "TimeoutError"}

    def test_as_headers_includes_summary(self) -> None:
        h = SanitizedError(category="permanent", code="ValueError", summary="bad").as_headers()
        assert h["x-rabbitkit-error-message"] == "bad"


class TestErrorSanitizerPolicies:
    def test_policy_values(self) -> None:
        assert ERROR_DETAIL_POLICIES == ("sanitized", "omit", "raw")
        with pytest.raises(ValueError):
            ErrorSanitizer(policy="verbose")

    def test_omit_has_no_summary(self) -> None:
        s = ErrorSanitizer(policy="omit").sanitize(ValueError(f"password={PW}"))
        assert s.summary == ""
        assert s.code == "ValueError"
        assert s.category == "unknown"

    def test_raw_is_legacy_capped_text(self) -> None:
        s = ErrorSanitizer(policy="raw", max_summary_len=5).sanitize(ValueError(f"password={PW}"))
        assert s.summary == "passw"

    def test_sanitized_default_keeps_plain_text(self) -> None:
        s = ErrorSanitizer().sanitize(ValueError("bad payload"), ErrorSeverity.PERMANENT)
        assert s.summary == "bad payload"
        assert s.category == "permanent"

    def test_summary_is_capped(self) -> None:
        s = ErrorSanitizer(max_summary_len=10).sanitize(ValueError("x y " * 50))
        assert len(s.summary) <= 10

    def test_whitespace_collapsed(self) -> None:
        s = ErrorSanitizer().sanitize(ValueError("line1\n  line2\t\tline3"))
        assert s.summary == "line1 line2 line3"

    def test_unprintable_exception(self) -> None:
        class Bad(Exception):
            def __str__(self) -> str:
                raise RuntimeError("no str")

        s = ErrorSanitizer().sanitize(Bad())
        assert "unprintable" in s.summary


class TestRedaction:
    # Fixtures are assembled from the SECRETS constants (never spelled out as
    # `password=<literal>`) so secret scanners on the repo do not flag test
    # data as a leaked credential — these are fake values that exist only to
    # prove they get redacted.
    @pytest.mark.parametrize(
        "text",
        [
            f"amqp://user:{PW}@rabbit.internal:5672/vhost connection refused",
            f"amqps://svc:{PW}@host/",
            f"login failed password={PW} for user",
            f"login failed password: {PW}",
            f'payload {{"api_key": "{PW}"}}',
            f"Authorization: Bearer {JWT}",
            f"aws key {AWS_KEY_ID} denied",
            f"token={HEX_TOKEN} expired",
            f"x-secret-key={PW} rejected",
            f"client_secret='{PW}'",
        ],
    )
    def test_secret_fixtures_never_survive(self, text: str) -> None:
        out = ErrorSanitizer().sanitize(RuntimeError(text)).summary
        assert not contains_secret_marker(out, SECRETS), out
        assert "***" in out

    def test_url_host_is_preserved(self) -> None:
        out = ErrorSanitizer().redact(f"amqp://user:{PW}@rabbit.internal:5672/vhost")
        assert out == "amqp://***:***@rabbit.internal:5672/vhost"

    def test_plain_words_survive(self) -> None:
        out = ErrorSanitizer().redact("connection to orders.created refused after 3 attempts")
        assert out == "connection to orders.created refused after 3 attempts"

    def test_custom_patterns(self) -> None:
        s = ErrorSanitizer(patterns=[re.compile(r"\d{4}-\d{4}")])
        assert s.redact("card 1234-5678 declined") == "card *** declined"

    def test_raw_policy_does_not_redact(self) -> None:
        s = ErrorSanitizer(policy="raw")
        assert PW in s.sanitize(RuntimeError(f"password={PW}")).summary


class TestCode:
    def test_code_is_class_name(self) -> None:
        assert ErrorSanitizer().code_for(KeyError("k")) == "KeyError"

    def test_code_is_sanitized_and_capped(self) -> None:
        cls = type("Weird$Name" + "X" * 100, (Exception,), {})
        code = ErrorSanitizer(max_code_len=16).code_for(cls())
        assert len(code) <= 16
        assert "$" not in code

    def test_allowlist_masks_unknown_codes(self) -> None:
        s = ErrorSanitizer(allowed_codes=frozenset({"TimeoutError"}))
        assert s.code_for(TimeoutError()) == "TimeoutError"
        assert s.code_for(ValueError()) == "ApplicationError"

    def test_rejects_bad_bounds(self) -> None:
        with pytest.raises(ValueError):
            ErrorSanitizer(max_code_len=0)
        with pytest.raises(ValueError):
            ErrorSanitizer(max_summary_len=-1)
