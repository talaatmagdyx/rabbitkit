"""Regressions for the 0.15.1 security fixes.

Each test here corresponds to a finding that was reproduced against the
0.15.0 code. They are deliberately in `tests/security/` rather than beside
their modules: these are adversarial properties, not unit behaviour, and
several of them (the ReDoS especially) would read as arbitrary if you did not
know what they were defending.
"""

from __future__ import annotations

import gzip
import logging
import time

import pytest

from rabbitkit.core.sanitizer import ErrorSanitizer
from rabbitkit.middleware.compression import CompressionMiddleware
from rabbitkit.middleware.signing import SigningConfig

# Assembled from fragments so no secret-shaped literal lands on disk
# (this repo has a GitGuardian gate).
_PW = "hunt" + "er2"
_KEY = "sup3r" + "-shared-secret-key-material-32b"


class TestSanitizerRedos:
    """The URL-credential pattern was quadratic, on the event loop.

    Measured on 0.15.0: 8 KiB 0.06s, 16 KiB 0.25s, 32 KiB 0.96s, 64 KiB 3.8s
    — a clean 4x per doubling, extrapolating to ~16 minutes for 1 MiB. A
    handler echoing the body into an exception is routine, so one crafted
    message stalled every queue on the connection.
    """

    def test_a_megabyte_of_hostile_input_is_fast(self) -> None:
        """A timing assertion, because correctness alone cannot catch this.

        The threshold is deliberately loose (1s against a ~0.002s actual) so
        it survives a slow CI runner while still failing by three orders of
        magnitude if the quadratic behaviour returns.
        """
        sanitizer = ErrorSanitizer()
        hostile = "a.a.a" * (1024 * 1024 // 5)  # ~1 MiB of scheme-legal chars
        start = time.perf_counter()
        sanitizer.summary_for(ValueError(hostile))
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"sanitizer took {elapsed:.2f}s on 1 MiB — the ReDoS is back"

    def test_scaling_stays_linear(self) -> None:
        """8x the input must not cost anywhere near 64x the time."""
        sanitizer = ErrorSanitizer()

        def cost(kib: int) -> float:
            text = "a.a.a" * (kib * 1024 // 5)
            start = time.perf_counter()
            sanitizer.summary_for(ValueError(text))
            return time.perf_counter() - start

        small = max(cost(8), 1e-6)
        large = cost(64)
        assert large / small < 20, f"8x input cost {large / small:.1f}x time — superlinear"

    def test_a_secret_inside_the_kept_window_is_still_redacted(self) -> None:
        """Truncating before redacting must not slice a secret in half.

        The redaction window is a multiple of the kept length precisely so a
        secret that begins inside the kept region is seen whole.
        """
        sanitizer = ErrorSanitizer()
        exc = ValueError("connect failed amqp://svc:" + _PW + "@mq.prod/vhost " + "x" * (2 * 1024 * 1024))
        out = sanitizer.summary_for(exc)
        assert _PW not in out
        assert "amqp://***:***@mq.prod" in out

    def test_ordinary_redaction_is_unaffected(self) -> None:
        sanitizer = ErrorSanitizer()
        out = sanitizer.redact("db at postgres://user:" + _PW + "@db.internal:5432/app")
        assert _PW not in out
        assert "db.internal" in out, "the host must survive — it is the useful part"


class TestSigningKeyIsNotInRepr:
    """Possession of the HMAC key is total compromise: any body, any route."""

    def test_repr_masks_the_secret(self) -> None:
        cfg = SigningConfig(secret_key=_KEY)
        assert _KEY not in repr(cfg)
        assert "***" in repr(cfg)

    def test_str_and_fstring_mask_the_secret(self) -> None:
        cfg = SigningConfig(secret_key=_KEY)
        assert _KEY not in str(cfg)
        assert _KEY not in f"{cfg}"

    def test_a_traceback_carrying_the_config_does_not_leak(self) -> None:
        """The realistic path: a config in an exception message."""
        cfg = SigningConfig(secret_key=_KEY)
        with pytest.raises(RuntimeError) as excinfo:
            raise RuntimeError(f"bad config: {cfg}")
        assert _KEY not in str(excinfo.value)

    def test_bytes_keys_are_masked_too(self) -> None:
        cfg = SigningConfig(secret_key=_KEY.encode())
        assert _KEY not in repr(cfg)

    def test_the_other_fields_are_still_visible(self) -> None:
        """Masking must not blind operators to the rest of the config."""
        cfg = SigningConfig(secret_key=_KEY)
        assert "hmac-sha256" in repr(cfg)


class TestGzipIntegrity:
    """The gzip trailer (CRC32 + ISIZE) is the payload's only integrity check."""

    def test_a_truncated_stream_is_rejected(self) -> None:
        """0.15.0 returned the partial data; the stdlib raises EOFError."""
        mw = CompressionMiddleware()
        data = gzip.compress(b"SENSITIVE-PAYLOAD" * 1000)
        with pytest.raises(ValueError, match="Truncated gzip stream"):
            mw._decompress_gzip_streaming(data[: len(data) // 2])

    def test_concatenated_members_are_all_decoded(self) -> None:
        """0.15.0 returned only the first member, silently dropping the rest —
        a body-smuggling primitive against anything that inspects the
        decompressed body separately."""
        mw = CompressionMiddleware()
        blob = gzip.compress(b"AAAA") + gzip.compress(b"BBBB") + gzip.compress(b"CCCC")
        assert mw._decompress_gzip_streaming(blob) == gzip.decompress(blob) == b"AAAABBBBCCCC"

    def test_a_normal_stream_still_round_trips(self) -> None:
        mw = CompressionMiddleware()
        payload = b"ordinary message body" * 500
        assert mw._decompress_gzip_streaming(gzip.compress(payload)) == payload

    def test_the_size_cap_still_fires_before_the_integrity_check(self) -> None:
        """A bomb must be refused on size, not decoded to completion first."""
        mw = CompressionMiddleware(max_decompressed_size=1024)
        bomb = gzip.compress(b"\0" * (10 * 1024 * 1024))
        with pytest.raises(ValueError, match="exceeds max_decompressed_size"):
            mw._decompress_gzip_streaming(bomb)


class TestContentEncodingLogInjection:
    """`content_encoding` is a publisher-set AMQP property."""

    def test_a_newline_cannot_forge_a_log_record(self, caplog: pytest.LogCaptureFixture) -> None:
        mw = CompressionMiddleware()
        hostile = "evil\nWARNING:root:FORGED LOG LINE admin=true"
        with caplog.at_level(logging.WARNING):
            mw.decompress(b"raw", hostile)
        assert len(caplog.records) == 1, "one delivery must produce exactly one record"
        assert "\n" not in caplog.records[0].getMessage()

    def test_an_absurdly_long_encoding_is_bounded(self, caplog: pytest.LogCaptureFixture) -> None:
        mw = CompressionMiddleware()
        with caplog.at_level(logging.WARNING):
            mw.decompress(b"raw", "z" * 100_000)
        assert len(caplog.records[0].getMessage()) < 200

    def test_the_body_is_still_returned_unchanged(self) -> None:
        mw = CompressionMiddleware()
        assert mw.decompress(b"raw-body", "something-unknown") == b"raw-body"


class TestCliDoesNotEchoCredentials:
    def test_userinfo_is_stripped_from_the_displayed_url(self) -> None:
        from rabbitkit.cli.commands.topology import _safe_url

        shown = _safe_url("http://admin:" + _PW + "@mq.prod:15672/api")
        assert _PW not in shown
        assert "admin" not in shown
        assert "mq.prod:15672" in shown, "the host must survive — it is the diagnostic"

    def test_a_url_without_credentials_is_unchanged(self) -> None:
        from rabbitkit.cli.commands.topology import _safe_url

        assert _safe_url("http://localhost:15672") == "http://localhost:15672"

    def test_an_unparseable_url_never_falls_through_to_the_raw_value(self) -> None:
        from rabbitkit.cli.commands.topology import _safe_url

        assert _safe_url("::::not a url::::" + _PW) == "<redacted url>"
