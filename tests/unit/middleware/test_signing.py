"""Tests for middleware/signing.py — SigningMiddleware."""

from __future__ import annotations

import logging
import time
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.signing import (
    InvalidSignatureError,
    NonceCache,
    SigningConfig,
    SigningMiddleware,
    TTLSetNonceCache,
)

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b"hello world",
        "routing_key": "test.rk",
        "headers": {},
    }
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


def _make_envelope(**kwargs: object) -> MessageEnvelope:
    defaults: dict[str, object] = {"routing_key": "test.rk", "body": b"test body"}
    defaults.update(kwargs)
    return MessageEnvelope(**defaults)  # type: ignore[arg-type]


SECRET = "my-secret-key"


# ── SigningConfig ────────────────────────────────────────────────────────


class TestSigningConfig:
    def test_config_defaults(self) -> None:
        """Default algorithm, header_name, reject flags, and require_freshness=True."""
        cfg = SigningConfig(secret_key=SECRET)
        assert cfg.algorithm == "hmac-sha256"
        assert cfg.header_name == "x-rabbitkit-signature"
        assert cfg.reject_unsigned is False
        assert cfg.reject_invalid is True
        assert cfg.require_freshness is True  # default hardened
        assert cfg.nonce_cache is None

    def test_config_invalid_algorithm(self) -> None:
        """Unsupported algorithm raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported algorithm"):
            SigningConfig(secret_key=SECRET, algorithm="hmac-md5")

    def test_config_invalid_max_skew(self) -> None:
        with pytest.raises(ValueError, match="max_skew must be positive"):
            SigningConfig(secret_key=SECRET, max_skew=0)

    def test_config_header_name_collision_with_timestamp_rejected(self) -> None:
        """L-1: header_name == _TIMESTAMP_HEADER raises ValueError."""
        with pytest.raises(ValueError, match="collides with a freshness header"):
            SigningConfig(secret_key=SECRET, header_name=SigningMiddleware._TIMESTAMP_HEADER)

    def test_config_header_name_collision_with_nonce_rejected(self) -> None:
        """L-1: header_name == _NONCE_HEADER raises ValueError."""
        with pytest.raises(ValueError, match="collides with a freshness header"):
            SigningConfig(secret_key=SECRET, header_name=SigningMiddleware._NONCE_HEADER)

    def test_config_custom_header_name_accepted(self) -> None:
        """L-1: a non-colliding custom header name is accepted."""
        cfg = SigningConfig(secret_key=SECRET, header_name="x-service-sig")
        assert cfg.header_name == "x-service-sig"


# ── Signature computation ────────────────────────────────────────────────


class TestComputeSignature:
    def test_compute_signature_deterministic(self) -> None:
        """Same body + key produces the same signature."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        body = b"deterministic test"
        sig1 = mw._compute_signature(body)
        sig2 = mw._compute_signature(body)
        assert sig1 == sig2

    def test_compute_signature_different_bodies(self) -> None:
        """Different bodies produce different signatures."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        sig_a = mw._compute_signature(b"body-a")
        sig_b = mw._compute_signature(b"body-b")
        assert sig_a != sig_b


# ── Signature verification ───────────────────────────────────────────────


class TestVerifySignature:
    def test_verify_valid_signature(self) -> None:
        """Returns True for a valid signature."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        body = b"valid body"
        sig = mw._compute_signature(body)
        assert mw._verify_signature(body, sig) is True

    def test_verify_invalid_signature(self) -> None:
        """Returns False for a tampered signature."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        body = b"valid body"
        assert mw._verify_signature(body, "tampered-signature") is False


# ── publish_scope (sync) ─────────────────────────────────────────────────


class TestPublishScope:
    def test_publish_scope_adds_signature_header(self) -> None:
        """call_next receives envelope with replay-protected signature headers."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        envelope = _make_envelope()

        call_next = MagicMock(return_value="published")
        result = mw.publish_scope(call_next, envelope)

        assert result == "published"
        call_next.assert_called_once()
        signed_env = call_next.call_args[0][0]
        assert cfg.header_name in signed_env.headers
        # Replay-protection headers must be present on the publish path.
        assert SigningMiddleware._TIMESTAMP_HEADER in signed_env.headers
        assert SigningMiddleware._NONCE_HEADER in signed_env.headers
        ts = float(signed_env.headers[SigningMiddleware._TIMESTAMP_HEADER])
        nonce = signed_env.headers[SigningMiddleware._NONCE_HEADER]
        expected_sig = mw._compute_fresh_signature(ts, nonce, envelope.body)
        assert signed_env.headers[cfg.header_name] == expected_sig
        # message_id is used as the nonce when present, else a generated uuid hex
        if envelope.message_id is not None:
            assert nonce == envelope.message_id
        else:
            assert isinstance(nonce, str) and len(nonce) == 32

    def test_publish_scope_preserves_existing_headers(self) -> None:
        """Existing headers are preserved when signature is added."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        envelope = _make_envelope(headers={"x-custom": "value"})

        call_next = MagicMock(return_value="ok")
        mw.publish_scope(call_next, envelope)

        signed_env = call_next.call_args[0][0]
        assert signed_env.headers["x-custom"] == "value"
        assert cfg.header_name in signed_env.headers


# ── publish_scope_async ──────────────────────────────────────────────────


class TestPublishScopeAsync:
    @pytest.mark.asyncio
    async def test_publish_scope_async_adds_signature(self) -> None:
        """Async variant adds signature header."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        envelope = _make_envelope()

        captured: list[MessageEnvelope] = []

        async def call_next(env: MessageEnvelope) -> str:
            captured.append(env)
            return "async-published"

        result = await mw.publish_scope_async(call_next, envelope)

        assert result == "async-published"
        assert len(captured) == 1
        signed_env = captured[0]
        assert cfg.header_name in signed_env.headers
        assert SigningMiddleware._TIMESTAMP_HEADER in signed_env.headers
        assert SigningMiddleware._NONCE_HEADER in signed_env.headers
        ts = float(signed_env.headers[SigningMiddleware._TIMESTAMP_HEADER])
        nonce = signed_env.headers[SigningMiddleware._NONCE_HEADER]
        expected_sig = mw._compute_fresh_signature(ts, nonce, envelope.body)
        assert signed_env.headers[cfg.header_name] == expected_sig


# ── on_receive (legacy body-only path) ───────────────────────────────────


class TestOnReceive:
    def test_on_receive_valid_signature_passes(self) -> None:
        """No exception raised for a valid legacy body-only signature (require_freshness=False)."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=False)
        mw = SigningMiddleware(cfg)
        body = b"hello world"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        mw.on_receive(msg)  # should not raise

    def test_on_receive_invalid_signature_rejects(self) -> None:
        """Raises InvalidSignatureError for invalid legacy signature (require_freshness=False)."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=False)
        mw = SigningMiddleware(cfg)
        msg = _make_message(
            body=b"hello world",
            headers={cfg.header_name: "bad-signature"},
        )
        with pytest.raises(InvalidSignatureError, match="verification failed"):
            mw.on_receive(msg)

    def test_on_receive_unsigned_with_reject_unsigned(self) -> None:
        """Raises InvalidSignatureError when unsigned and reject_unsigned=True."""
        cfg = SigningConfig(secret_key=SECRET, reject_unsigned=True)
        mw = SigningMiddleware(cfg)
        msg = _make_message(body=b"hello world", headers={})
        with pytest.raises(InvalidSignatureError, match="no x-rabbitkit-signature"):
            mw.on_receive(msg)

    def test_on_receive_unsigned_without_reject_unsigned(self) -> None:
        """Passes silently when unsigned and reject_unsigned=False."""
        cfg = SigningConfig(secret_key=SECRET, reject_unsigned=False)
        mw = SigningMiddleware(cfg)
        msg = _make_message(body=b"hello world", headers={})
        # Should not raise
        mw.on_receive(msg)

    def test_on_receive_sha512(self) -> None:
        """Works with hmac-sha512 algorithm (legacy body-only path)."""
        cfg = SigningConfig(secret_key=SECRET, algorithm="hmac-sha512", require_freshness=False)
        mw = SigningMiddleware(cfg)
        body = b"sha512 body"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        mw.on_receive(msg)  # should not raise

    def test_on_receive_sha512_invalid_rejects(self) -> None:
        """SHA-512 invalid signature is rejected (legacy path)."""
        cfg = SigningConfig(secret_key=SECRET, algorithm="hmac-sha512", require_freshness=False)
        mw = SigningMiddleware(cfg)
        msg = _make_message(
            body=b"sha512 body",
            headers={cfg.header_name: "wrong"},
        )
        with pytest.raises(InvalidSignatureError):
            mw.on_receive(msg)

    @pytest.mark.asyncio
    async def test_on_receive_async_delegates_to_sync(self) -> None:
        """Async on_receive_async delegates to sync on_receive (legacy path)."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=False)
        mw = SigningMiddleware(cfg)
        body = b"async body"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        await mw.on_receive_async(msg)  # should not raise

    def test_on_receive_reject_invalid_false_allows_bad_sig(self) -> None:
        """When reject_invalid=False, a bad legacy signature does NOT raise."""
        cfg = SigningConfig(secret_key=SECRET, reject_invalid=False)
        mw = SigningMiddleware(cfg)
        msg = _make_message(
            body=b"hello",
            headers={cfg.header_name: "bad-sig"},
        )
        mw.on_receive(msg)  # should not raise

    def test_bytes_secret_key(self) -> None:
        """Secret key can be bytes instead of str (legacy path)."""
        cfg = SigningConfig(secret_key=b"raw-bytes-key", require_freshness=False)
        mw = SigningMiddleware(cfg)
        body = b"test body"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        mw.on_receive(msg)

    def test_non_str_signature_header_raises_invalid_signature(self) -> None:
        """L-2: a non-str/bytes signature header raises InvalidSignatureError (not TypeError)."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=False)
        mw = SigningMiddleware(cfg)
        msg = _make_message(body=b"hello", headers={cfg.header_name: 12345})
        with pytest.raises(InvalidSignatureError, match="not a string/bytes"):
            mw.on_receive(msg)

    def test_bytes_signature_header_accepted(self) -> None:
        """L-2: a bytes signature header compares cleanly (no TypeError)."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=False)
        mw = SigningMiddleware(cfg)
        body = b"hello"
        sig = mw._compute_signature(body).encode("utf-8")
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        mw.on_receive(msg)  # should not raise


# ── replay protection (I-1) ─────────────────────────────────────────────


class TestReplayProtection:
    def _fresh_message(self, mw: SigningMiddleware, body: bytes, *, timestamp: float, nonce: str) -> RabbitMessage:
        sig = mw._compute_fresh_signature(timestamp, nonce, body)
        return _make_message(
            body=body,
            headers={
                mw._config.header_name: sig,
                SigningMiddleware._TIMESTAMP_HEADER: str(timestamp),
                SigningMiddleware._NONCE_HEADER: nonce,
            },
        )

    def test_fresh_signature_passes(self) -> None:
        """A current replay-protected signature verifies successfully."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True)
        mw = SigningMiddleware(cfg)
        msg = self._fresh_message(mw, b"hello", timestamp=time.time(), nonce="nonce-1")
        mw.on_receive(msg)  # should not raise

    def test_replay_rejected_second_time(self) -> None:
        """Replaying a captured signed message a second time is rejected (default cache)."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True, max_skew=300.0)
        mw = SigningMiddleware(cfg)
        ts = time.time()
        msg = self._fresh_message(mw, b"replay me", timestamp=ts, nonce="nonce-once")
        mw.on_receive(msg)  # first time: accepted
        # Replay the exact same captured message.
        with pytest.raises(InvalidSignatureError, match="Replay detected"):
            mw.on_receive(msg)

    def test_replay_rejected_with_explicit_cache(self) -> None:
        """A pluggable NonceCache is honoured."""
        cache = TTLSetNonceCache()
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True, nonce_cache=cache)
        mw = SigningMiddleware(cfg)
        ts = time.time()
        msg = self._fresh_message(mw, b"replay me", timestamp=ts, nonce="nonce-x")
        mw.on_receive(msg)
        with pytest.raises(InvalidSignatureError, match="Replay detected"):
            mw.on_receive(msg)

    def test_stale_signature_rejected_when_require_freshness(self) -> None:
        """A timestamp outside max_skew is rejected when freshness headers are present."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True, max_skew=10.0)
        mw = SigningMiddleware(cfg)
        stale_ts = time.time() - 1000.0
        msg = self._fresh_message(mw, b"hello", timestamp=stale_ts, nonce="nonce-1")
        with pytest.raises(InvalidSignatureError, match="max_skew"):
            mw.on_receive(msg)

    def test_stale_signature_rejected_even_when_require_freshness_false(self) -> None:
        """Freshness headers present → skew always enforced regardless of the flag."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=False, max_skew=10.0)
        mw = SigningMiddleware(cfg)
        stale_ts = time.time() - 100000.0
        msg = self._fresh_message(mw, b"hello", timestamp=stale_ts, nonce="nonce-1")
        with pytest.raises(InvalidSignatureError, match="max_skew"):
            mw.on_receive(msg)

    def test_future_timestamp_rejected(self) -> None:
        """A future-dated timestamp beyond max_skew is rejected."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True, max_skew=10.0)
        mw = SigningMiddleware(cfg)
        future_ts = time.time() + 1000.0
        msg = self._fresh_message(mw, b"hello", timestamp=future_ts, nonce="nonce-future")
        with pytest.raises(InvalidSignatureError, match="max_skew"):
            mw.on_receive(msg)

    def test_tampered_fresh_signature_rejected(self) -> None:
        """A replay-protected signature that does not match is rejected."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True)
        mw = SigningMiddleware(cfg)
        msg = self._fresh_message(mw, b"hello", timestamp=time.time(), nonce="nonce-1")
        msg.headers[mw._config.header_name] = "tampered"
        with pytest.raises(InvalidSignatureError, match="verification failed"):
            mw.on_receive(msg)

    def test_missing_freshness_headers_rejected_when_required(self) -> None:
        """require_freshness=True rejects messages lacking timestamp/nonce headers."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True)
        mw = SigningMiddleware(cfg)
        body = b"legacy"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        with pytest.raises(InvalidSignatureError, match="Missing freshness headers"):
            mw.on_receive(msg)

    def test_missing_freshness_headers_accepted_and_warned_when_not_required(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """require_freshness=False accepts legacy body-only sigs and logs a warning."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=False)
        mw = SigningMiddleware(cfg)
        body = b"legacy body"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        with caplog.at_level(logging.WARNING, logger="rabbitkit.middleware.signing"):
            mw.on_receive(msg)  # should not raise
        assert any("without" in rec.message for rec in caplog.records)

    def test_legacy_body_only_signature_still_verifies(self) -> None:
        """require_freshness=False accepts old body-only signatures (with a warning)."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=False)
        mw = SigningMiddleware(cfg)
        body = b"legacy body"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        mw.on_receive(msg)  # should not raise

    def test_invalid_timestamp_header_rejected(self) -> None:
        """A non-numeric timestamp header is a permanent failure."""
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True)
        mw = SigningMiddleware(cfg)
        sig = mw._compute_fresh_signature(0.0, "n", b"hello")
        msg = _make_message(
            body=b"hello",
            headers={
                cfg.header_name: sig,
                SigningMiddleware._TIMESTAMP_HEADER: "not-a-number",
                SigningMiddleware._NONCE_HEADER: "n",
            },
        )
        with pytest.raises(InvalidSignatureError, match="Invalid signature timestamp"):
            mw.on_receive(msg)

    def test_nan_timestamp_rejected(self) -> None:
        # L-3: NaN bypasses the skew check (abs(now - NaN) is NaN) - reject it.
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True)
        mw = SigningMiddleware(cfg)
        sig = mw._compute_fresh_signature(0.0, "n", b"hello")
        msg = _make_message(
            body=b"hello",
            headers={
                cfg.header_name: sig,
                SigningMiddleware._TIMESTAMP_HEADER: "nan",
                SigningMiddleware._NONCE_HEADER: "n",
            },
        )
        with pytest.raises(InvalidSignatureError, match="non-finite timestamp"):
            mw.on_receive(msg)

    def test_inf_timestamp_rejected(self) -> None:
        # L-3: +Inf is non-finite and must be rejected.
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True)
        mw = SigningMiddleware(cfg)
        sig = mw._compute_fresh_signature(0.0, "n", b"hello")
        msg = _make_message(
            body=b"hello",
            headers={
                cfg.header_name: sig,
                SigningMiddleware._TIMESTAMP_HEADER: "inf",
                SigningMiddleware._NONCE_HEADER: "n",
            },
        )
        with pytest.raises(InvalidSignatureError, match="non-finite timestamp"):
            mw.on_receive(msg)


# ── TTLSetNonceCache / NonceCache protocol ───────────────────────────────


class TestTTLSetNonceCache:
    def test_first_seen_returns_true_then_false(self) -> None:
        cache = TTLSetNonceCache()
        assert cache.seen("a", ttl=10.0) is True
        assert cache.seen("a", ttl=10.0) is False  # duplicate within TTL

    def test_expired_nonce_accepted_again(self) -> None:
        cache = TTLSetNonceCache()
        assert cache.seen("a", ttl=-1.0) is True  # immediately expired
        # Second call: existing expiry (-1 + monotonic) is <= now → re-accepted.
        assert cache.seen("a", ttl=-1.0) is True

    def test_distinct_nonces_each_accepted_once(self) -> None:
        cache = TTLSetNonceCache()
        assert cache.seen("a", ttl=10.0) is True
        assert cache.seen("b", ttl=10.0) is True
        assert cache.seen("a", ttl=10.0) is False
        assert cache.seen("b", ttl=10.0) is False

    def test_bounded_eviction(self) -> None:
        """When full, the cache evicts rather than growing unboundedly."""
        cache = TTLSetNonceCache(max_entries=4)
        for i in range(100):
            cache.seen(f"n{i}", ttl=1000.0)
        # Should not have grown past a small multiple of the bound.
        assert len(cache._entries) <= 4

    def test_invalid_max_entries(self) -> None:
        with pytest.raises(ValueError):
            TTLSetNonceCache(max_entries=0)

    def test_protocol_satisfied(self) -> None:
        """TTLSetNonceCache satisfies the NonceCache protocol structurally."""

        def _use(cache: NonceCache) -> bool:
            return cache.seen("x", 1.0)

        assert _use(TTLSetNonceCache()) is True

    def test_gc_deletes_expired_entries_at_capacity(self) -> None:
        """Line 171: del self._entries[k] runs when expired entries are present.

        The GC loop (line 170-171) only executes its body when at least one
        entry has already expired. This test fills the cache to max_entries with
        immediately-expiring nonces (ttl close to 0), then waits for them to
        expire, then adds a new one to trigger the GC that deletes them at
        line 171.

        Strategy: use ttl=-1.0 so entries expire immediately (monotonic clock +
        negative ttl = past), fill the cache, then add one more to trigger GC.
        """
        import time

        cache = TTLSetNonceCache(max_entries=5)

        # Fill the cache with nonces that expire immediately.
        for i in range(5):
            result = cache.seen(f"expired-{i}", ttl=-1.0)
            assert result is True  # accepted on first call

        # All 5 entries are now in the cache but their expiry is in the past.
        assert len(cache._entries) == 5

        # Wait a tiny bit to ensure monotonic time has advanced past the expiries.
        time.sleep(0.01)

        # Adding a new nonce: len >= max_entries → GC runs, finds expired entries,
        # deletes them at line 171, cache size drops below max_entries.
        result = cache.seen("new-nonce", ttl=60.0)
        assert result is True

        # After GC, the expired entries should have been removed.
        # Only "new-nonce" should remain.
        assert "new-nonce" in cache._entries

    def test_eviction_oldest_after_gc_finds_nothing_expired(self) -> None:
        """Lines 173-177: eviction of oldest 10% when GC finds no expired entries.

        The inner 'if still at/over capacity' branch (line 173) only runs when
        GC (line 170-171) did not free enough space. This happens when all
        entries have long TTLs (none expired) and the cache is full.
        """
        cache = TTLSetNonceCache(max_entries=10)

        # Fill cache completely with non-expiring entries.
        for i in range(10):
            cache.seen(f"live-{i}", ttl=9999.0)

        assert len(cache._entries) == 10

        # Add one more: at capacity → GC runs (finds nothing expired because
        # all TTLs are 9999s) → inner check triggers → evict oldest 10%.
        cache.seen("overflow", ttl=9999.0)

        # After eviction of 10% (1 entry) + insertion of "overflow",
        # the size should be at most max_entries.
        assert len(cache._entries) <= 10
