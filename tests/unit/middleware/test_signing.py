"""Tests for middleware/signing.py — SigningMiddleware."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.signing import (
    InvalidSignatureError,
    SigningConfig,
    SigningMiddleware,
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
        """Default algorithm, header_name, and reject flags."""
        cfg = SigningConfig(secret_key=SECRET)
        assert cfg.algorithm == "hmac-sha256"
        assert cfg.header_name == "x-rabbitkit-signature"
        assert cfg.reject_unsigned is False
        assert cfg.reject_invalid is True

    def test_config_invalid_algorithm(self) -> None:
        """Unsupported algorithm raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported algorithm"):
            SigningConfig(secret_key=SECRET, algorithm="hmac-md5")


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
        """call_next receives envelope with signature header."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        envelope = _make_envelope()

        call_next = MagicMock(return_value="published")
        result = mw.publish_scope(call_next, envelope)

        assert result == "published"
        call_next.assert_called_once()
        signed_env = call_next.call_args[0][0]
        assert cfg.header_name in signed_env.headers
        # Verify the signature is correct
        expected_sig = mw._compute_signature(envelope.body)
        assert signed_env.headers[cfg.header_name] == expected_sig

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
        expected_sig = mw._compute_signature(envelope.body)
        assert signed_env.headers[cfg.header_name] == expected_sig


# ── on_receive ───────────────────────────────────────────────────────────


class TestOnReceive:
    def test_on_receive_valid_signature_passes(self) -> None:
        """No exception raised for valid signature."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        body = b"hello world"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        # Should not raise
        mw.on_receive(msg)

    def test_on_receive_invalid_signature_rejects(self) -> None:
        """Raises InvalidSignatureError for invalid signature."""
        cfg = SigningConfig(secret_key=SECRET)
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
        """Works with hmac-sha512 algorithm."""
        cfg = SigningConfig(secret_key=SECRET, algorithm="hmac-sha512")
        mw = SigningMiddleware(cfg)
        body = b"sha512 body"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        # Should not raise
        mw.on_receive(msg)

    def test_on_receive_sha512_invalid_rejects(self) -> None:
        """SHA-512 invalid signature is rejected."""
        cfg = SigningConfig(secret_key=SECRET, algorithm="hmac-sha512")
        mw = SigningMiddleware(cfg)
        msg = _make_message(
            body=b"sha512 body",
            headers={cfg.header_name: "wrong"},
        )
        with pytest.raises(InvalidSignatureError):
            mw.on_receive(msg)

    @pytest.mark.asyncio
    async def test_on_receive_async_delegates_to_sync(self) -> None:
        """Async on_receive_async delegates to sync on_receive."""
        cfg = SigningConfig(secret_key=SECRET)
        mw = SigningMiddleware(cfg)
        body = b"async body"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        # Should not raise
        await mw.on_receive_async(msg)

    def test_on_receive_reject_invalid_false_allows_bad_sig(self) -> None:
        """When reject_invalid=False, bad signature does NOT raise."""
        cfg = SigningConfig(
            secret_key=SECRET, reject_invalid=False
        )
        mw = SigningMiddleware(cfg)
        msg = _make_message(
            body=b"hello",
            headers={cfg.header_name: "bad-sig"},
        )
        # Should not raise
        mw.on_receive(msg)

    def test_bytes_secret_key(self) -> None:
        """Secret key can be bytes instead of str."""
        cfg = SigningConfig(secret_key=b"raw-bytes-key")
        mw = SigningMiddleware(cfg)
        body = b"test body"
        sig = mw._compute_signature(body)
        msg = _make_message(body=body, headers={cfg.header_name: sig})
        mw.on_receive(msg)
