"""Cryptographic message signing middleware.

Signs outgoing messages with **HMAC** (SHA-256 or SHA-512) and verifies
incoming signatures.  Uses stdlib ``hmac`` + ``hashlib`` — no extra deps.

How it works
------------
*Publish path*: Before a message is sent, ``SigningMiddleware`` computes
``HMAC(secret_key, body)`` and stores the hex digest in an AMQP header
(default: ``x-rabbitkit-signature``).

*Consume path*: On receipt the middleware reads the signature header and
calls ``hmac.compare_digest`` (constant-time) to verify it.  Behaviour when
verification fails is configurable:

* ``reject_invalid=True`` (default) — raise ``InvalidSignatureError``
* ``reject_unsigned=True`` — raise ``InvalidSignatureError`` if the header is absent
* Both ``False`` — log / pass unsigned/invalid messages through (monitoring mode)

Quick start — symmetric signing between two services
-----------------------------------------------------
Sender side::

    from rabbitkit.middleware.signing import SigningMiddleware, SigningConfig

    signing_mw = SigningMiddleware(
        SigningConfig(secret_key="shared-secret-do-not-commit")
    )

    # Attach to broker so ALL outgoing publishes are signed
    @broker.publisher(exchange="events", routing_key="order.created")
    @broker.subscriber(queue="orders-input", middlewares=[signing_mw])
    async def process_order(body: bytes) -> bytes:
        return b'{"status": "ok"}'

Receiver side (different service, same shared secret)::

    signing_mw = SigningMiddleware(
        SigningConfig(
            secret_key="shared-secret-do-not-commit",
            reject_unsigned=True,   # reject messages without a signature
            reject_invalid=True,    # reject messages with wrong signature
        )
    )

    @broker.subscriber(queue="order-results", middlewares=[signing_mw])
    async def handle_result(body: bytes) -> None:
        ...

Stronger algorithm (SHA-512)::

    signing_mw = SigningMiddleware(
        SigningConfig(
            secret_key=b"\\x00very\\xff long\\xde random\\xad key",
            algorithm="hmac-sha512",
        )
    )

Custom header name::

    SigningConfig(secret_key="s3cr3t", header_name="x-service-sig")

Exceptions
----------
``InvalidSignatureError`` is raised by ``on_receive`` / ``on_receive_async``
when verification fails.  It is classified as a ``PERMANENT`` error by the
default error classifier, so retry will not be attempted — the message goes
straight to the DLQ.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware


class InvalidSignatureError(Exception):
    """Raised when a message signature is invalid."""


@dataclass(frozen=True, slots=True)
class SigningConfig:
    """Configuration for message signing.

    Attributes:
        secret_key: Shared secret for HMAC computation.
        algorithm: Hash algorithm ("hmac-sha256" or "hmac-sha512").
        header_name: AMQP header name for the signature.
        reject_unsigned: If True, reject messages without a signature.
        reject_invalid: If True, reject messages with invalid signatures.
    """

    secret_key: str | bytes
    algorithm: str = "hmac-sha256"
    header_name: str = "x-rabbitkit-signature"
    reject_unsigned: bool = False
    reject_invalid: bool = True

    def __post_init__(self) -> None:
        if self.algorithm not in ("hmac-sha256", "hmac-sha512"):
            raise ValueError(
                f"Unsupported algorithm: {self.algorithm}. "
                "Use 'hmac-sha256' or 'hmac-sha512'."
            )


class SigningMiddleware(BaseMiddleware):
    """Signs outgoing messages and verifies incoming signatures.

    On publish: computes HMAC of body and adds signature to headers.
    On receive: verifies signature against body, rejects if invalid.
    """

    def __init__(self, config: SigningConfig) -> None:
        self._config = config
        self._key = (
            config.secret_key.encode("utf-8")
            if isinstance(config.secret_key, str)
            else config.secret_key
        )
        self._hash_name = (
            "sha256" if config.algorithm == "hmac-sha256" else "sha512"
        )

    def _compute_signature(self, body: bytes) -> str:
        """Compute HMAC signature for message body."""
        return hmac.new(
            self._key, body, getattr(hashlib, self._hash_name)
        ).hexdigest()

    def _verify_signature(self, body: bytes, signature: str) -> bool:
        """Verify HMAC signature using constant-time comparison."""
        expected = self._compute_signature(body)
        return hmac.compare_digest(expected, signature)

    # ── Helpers ──────────────────────────────────────────────────────────

    def _sign_envelope(self, envelope: MessageEnvelope) -> MessageEnvelope:
        """Return a new envelope with signature header added."""
        sig = self._compute_signature(envelope.body)
        headers = dict(envelope.headers) if envelope.headers else {}
        headers[self._config.header_name] = sig
        return MessageEnvelope(
            routing_key=envelope.routing_key,
            body=envelope.body,
            exchange=envelope.exchange,
            correlation_id=envelope.correlation_id,
            headers=headers,
            message_id=envelope.message_id,
            reply_to=envelope.reply_to,
            timestamp=envelope.timestamp,
            content_type=envelope.content_type,
            content_encoding=envelope.content_encoding,
            expiration=envelope.expiration,
            priority=envelope.priority,
            mandatory=envelope.mandatory,
            delivery_mode=envelope.delivery_mode,
            type=envelope.type,
            user_id=envelope.user_id,
            app_id=envelope.app_id,
        )

    # ── Publish: sign outgoing messages ──────────────────────────────────

    def publish_scope(self, call_next: Any, envelope: MessageEnvelope) -> Any:
        """Add HMAC signature to outgoing message headers."""
        signed = self._sign_envelope(envelope)
        return call_next(signed)

    async def publish_scope_async(
        self, call_next: Any, envelope: MessageEnvelope
    ) -> Any:
        """Async variant — add HMAC signature."""
        signed = self._sign_envelope(envelope)
        return await call_next(signed)

    # ── Receive: verify incoming signatures ──────────────────────────────

    def on_receive(self, message: RabbitMessage) -> None:
        """Verify signature on incoming message."""
        sig = message.headers.get(self._config.header_name)
        if sig is None:
            if self._config.reject_unsigned:
                raise InvalidSignatureError(
                    f"Message has no {self._config.header_name} header"
                )
            return
        if self._config.reject_invalid and not self._verify_signature(
            message.body, sig
        ):
            raise InvalidSignatureError(
                "Message signature verification failed"
            )

    async def on_receive_async(self, message: RabbitMessage) -> None:
        """Async variant — verify signature."""
        self.on_receive(message)
