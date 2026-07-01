"""Cryptographic message signing middleware.

Signs outgoing messages with **HMAC** (SHA-256 or SHA-512) and verifies
incoming signatures.  Uses stdlib ``hmac`` + ``hashlib`` — no extra deps.

How it works
------------
*Publish path*: Before a message is sent, ``SigningMiddleware`` computes a
replay-protected, route-bound HMAC and stores the hex digest in an AMQP
header (default: ``x-rabbitkit-signature``), alongside
``x-rabbitkit-sign-timestamp`` and ``x-rabbitkit-sign-nonce`` headers.

*Consume path*: On receipt the middleware reads the signature header and
calls ``hmac.compare_digest`` (constant-time) to verify it.  When freshness
headers are present the timestamp skew AND a server-side nonce seen-set are
enforced to defeat replay attacks.  Behaviour when verification fails is
configurable:

* ``reject_invalid=True`` (default) — raise ``InvalidSignatureError``
* ``reject_unsigned=True`` — raise ``InvalidSignatureError`` if the header is absent
* Both ``False`` — log / pass unsigned/invalid messages through (monitoring mode)

What the signature covers (H3)
-------------------------------
The **fresh** (replay-protected, ``require_freshness=True`` default) signature
is an HMAC over::

    timestamp:nonce: + exchange \\x00 routing_key \\x00 content_encoding \\x00 reply_to \\x00 + body

i.e. ``timestamp``, ``nonce``, ``exchange``, ``routing_key``,
``content_encoding``, ``reply_to``, and ``body`` — computed at publish time
from the outgoing ``MessageEnvelope`` and re-derived at consume time from the
*delivered* ``RabbitMessage``'s broker-reported routing metadata (not from
attacker-controlled headers). Changing any of those fields on a captured
message — re-publishing it under a different routing key, redirecting an RPC
reply via ``reply_to``, or flipping ``content_encoding`` to hit a different
decompression path — invalidates the signature even though the body,
timestamp, and nonce are all unchanged.

**Not covered**: any header other than the signature/timestamp/nonce triplet
itself. Do not use freeform headers for security-critical routing or
dispatch decisions under this middleware — they are not authenticated.

The **legacy** body-only signature (only reachable with
``require_freshness=False``, for interop with producers that predate the
freshness headers) covers ``body`` ONLY — no routing metadata, no replay
protection. It exists solely for backward compatibility with signers this
library does not control; prefer the default ``require_freshness=True`` for
any security-sensitive deployment.

Combining with CompressionMiddleware (H7)
------------------------------------------
Use ``middlewares=[CompressionMiddleware(...), SigningMiddleware(...)]`` —
compression OUTER, signing INNER. This order is required, not a suggestion:
the signature covers ``content_encoding`` (see above), a field
``CompressionMiddleware``'s ``publish_scope`` is what actually sets. With
signing outer (the reverse order), signing would sign
``content_encoding=None`` (unset at that point) while compression sets it to
e.g. ``"gzip"`` afterward — the delivered message's ``content_encoding``
then never matches what was signed, and verification fails
unconditionally. With the correct order, compression sets
``content_encoding`` first, signing signs the final value and the compressed
body, and ``HandlerPipeline`` runs ``on_receive`` hooks in the REVERSE of
registration order on consume (verify before decompress, mirroring
compress-then-sign on publish) so the two compose correctly end-to-end. A
signature/decompression failure in ``on_receive`` is NOT retry-eligible (see
``HandlerPipeline._run_consume_sync``'s docstring) — it settles per the
route's ``AckPolicy`` directly, bypassing any ``RetryMiddleware`` on the
route.

Replay protection
-----------------
``require_freshness`` defaults to ``True``. The consume-time rules are:

* **Freshness headers present** (new producer): skew (``abs(now - ts) <= max_skew``,
  both past and future) and the nonce seen-set are **always** enforced regardless
  of ``require_freshness``. A stale timestamp or a duplicate nonce raises
  ``InvalidSignatureError`` (permanent — no retry).
* **Freshness headers absent + ``require_freshness=True``**: rejected (strict).
* **Freshness headers absent + ``require_freshness=False``**: the legacy
  body-only signature is verified and a warning is logged (backward compat
  with old producers).

The nonce seen-set is pluggable via the ``NonceCache`` protocol; a default
in-memory ``TTLSetNonceCache`` is used when none is supplied so replay
protection works out of the box **for a single process**.

Shared nonce store for multi-process/multi-pod deployments (H4)
-----------------------------------------------------------------
``TTLSetNonceCache`` is a plain in-process dict. In any multi-process or
multi-pod deployment — the normal case for a consumer with more than one
replica — a nonce recorded by one process is invisible to every other
process, and it is lost entirely on restart. A replay that happens to land on
a *different* worker than the original message passes the nonce check even
though the exact same signed payload was already processed elsewhere.
``SigningMiddleware`` warns at construction time (``RuntimeWarning``) when
``require_freshness=True`` and no explicit ``nonce_cache`` was supplied, for
exactly this reason.

Use :class:`RedisNonceCache` (or your own ``NonceCache`` implementation) to
share the seen-set across every process/pod that verifies signatures for the
same producer::

    import redis
    from rabbitkit.middleware.signing import RedisNonceCache, SigningConfig

    cache = RedisNonceCache(redis.Redis(host="redis", port=6379))
    config = SigningConfig(secret_key="shared-secret", nonce_cache=cache)

``RedisNonceCache`` records each nonce with an atomic ``SET NX EX`` — two
processes racing on the same nonce can never both "win" the check, which is
exactly the guarantee replay protection needs across multiple workers.

For payments or other high-value traffic, also consider a tighter
``max_skew`` than the default (60s) to shrink the replay window, and always
use a shared ``nonce_cache`` — the in-memory default is not sufficient once
there is more than one consumer process.

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
import logging
import math
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)


class InvalidSignatureError(Exception):
    """Raised when a message signature is invalid."""


def _const_time_eq(expected: str, signature: str | bytes) -> bool:
    """Constant-time compare of a hex-digest ``expected`` against ``signature``.

    ``hmac.compare_digest`` requires both args to share a type; normalise the
    signature to bytes so a ``bytes`` signature header compares cleanly against
    the str hex digest without raising ``TypeError``.
    """
    expected_b = expected.encode("utf-8")
    sig_b = signature.encode("utf-8") if isinstance(signature, str) else signature
    return hmac.compare_digest(expected_b, sig_b)


# ── Replay protection: pluggable nonce seen-set ───────────────────────────


class NonceCache(Protocol):
    """A server-side seen-set for replay-protection nonces.

    ``seen(nonce, ttl)`` returns ``True`` when the nonce is first observed
    (and records it for ``ttl`` seconds), and ``False`` when the nonce has
    already been seen and is still within its TTL (i.e. a replay).
    """

    def seen(self, nonce: str, ttl: float) -> bool:
        """Record/lookup a nonce. True = first-seen/accepted, False = duplicate."""


class TTLSetNonceCache:
    """Default in-memory nonce seen-set with TTL eviction.

    Thread-safe (a single lock guards the dict). Bounded to ``max_entries``
    nonces; when full, expired entries are reclaimed first and, if still too
    large, the oldest 10% are evicted (LRU-ish, relying on dict insertion order).
    Expiry is lazy — checked on each lookup and opportunistically during GC.
    """

    def __init__(self, max_entries: int = 100_000) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._entries: dict[str, float] = {}
        self._lock = threading.Lock()
        self._max_entries = max_entries

    def seen(self, nonce: str, ttl: float) -> bool:
        # monotonic clock — immune to wall-clock adjustments.
        now = time.monotonic()
        expiry = now + ttl
        with self._lock:
            existing = self._entries.get(nonce)
            if existing is not None and existing > now:
                # Already seen and still valid → replay.
                return False

            # Lazy GC: bound the set size.
            if len(self._entries) >= self._max_entries:
                # Drop expired entries first (cheap-ish pass over the dict).
                for k in [k for k, exp in self._entries.items() if exp <= now]:
                    del self._entries[k]
                # If still at/over capacity, evict the oldest 10% (insertion order).
                if len(self._entries) >= self._max_entries:
                    drop = max(1, len(self._entries) // 10)
                    for _ in range(drop):
                        k = next(iter(self._entries))
                        del self._entries[k]

            self._entries[nonce] = expiry
            return True


class RedisNonceCache:
    """Redis-backed nonce seen-set for replay protection shared across
    multiple processes/pods (H4).

    ``TTLSetNonceCache`` (the default) is per-process, in-memory — see the
    module docstring's "Shared nonce store" section for why that is
    insufficient once there is more than one consumer process. This class
    shares the seen-set across every process pointed at the same Redis
    instance/key prefix using an atomic ``SET NX EX``: two processes racing
    to record the same nonce can never both receive ``True``, which is
    exactly the guarantee needed to catch a replay that lands on a different
    worker than the original message.

    Requires a **synchronous** redis client (``redis-py``'s ``redis.Redis``,
    or any duck-typed equivalent exposing
    ``.set(key, value, nx=True, ex=ttl)``) — signature verification runs
    synchronously even under ``AsyncBroker`` (``on_receive_async`` delegates
    to the sync ``on_receive``), so there is no async code path to plug an
    async redis client into.

    Usage::

        import redis
        from rabbitkit.middleware.signing import RedisNonceCache, SigningConfig

        cache = RedisNonceCache(redis.Redis(host="redis", port=6379))
        config = SigningConfig(secret_key="shared-secret", nonce_cache=cache)
    """

    def __init__(self, redis_client: Any, key_prefix: str = "rabbitkit:nonce:") -> None:
        self._redis = redis_client
        self._prefix = key_prefix

    def _key(self, nonce: str) -> str:
        return f"{self._prefix}{nonce}"

    def seen(self, nonce: str, ttl: float) -> bool:
        """Atomically record *nonce*. True = first-seen, False = replay.

        ``SET NX EX`` only sets the key (and returns truthy) when it does not
        already exist, so this is the same first-seen/duplicate semantics as
        :class:`TTLSetNonceCache`, but enforced by Redis across every process
        sharing this client/prefix rather than by an in-process lock.
        """
        return bool(self._redis.set(self._key(nonce), "1", nx=True, ex=max(1, int(ttl))))


@dataclass(frozen=True, slots=True)
class SigningConfig:
    """Configuration for message signing.

    Attributes:
        secret_key: Shared secret for HMAC computation.
        algorithm: Hash algorithm ("hmac-sha256" or "hmac-sha512").
        header_name: AMQP header name for the signature.
        reject_unsigned: If True, reject messages without a signature.
        reject_invalid: If True, reject messages with invalid signatures.
        max_skew: Max allowed |now - timestamp| skew in seconds (both past/future).
            Also the nonce's replay-window TTL (H4: tightened default of 60s —
            shrink further for high-value/financial traffic; a captured
            signature is replayable within this window on any process that
            has not already seen the nonce).
        require_freshness: If True (default), reject messages lacking freshness
            headers. If False, accept legacy body-only signatures with a warning.
        nonce_cache: Pluggable nonce seen-set. ``None`` means a default
            in-memory ``TTLSetNonceCache`` is created lazily by the middleware
            (H4: per-process only — use :class:`RedisNonceCache` or your own
            shared implementation for any multi-process/multi-pod deployment;
            a ``RuntimeWarning`` is emitted when this default is left unset
            with ``require_freshness=True``).
    """

    secret_key: str | bytes
    algorithm: str = "hmac-sha256"
    header_name: str = "x-rabbitkit-signature"
    reject_unsigned: bool = False
    reject_invalid: bool = True
    # Replay protection. H4: default tightened from 300s to 60s — shrink
    # further for payments/high-value traffic.
    max_skew: float = 60.0
    require_freshness: bool = True
    nonce_cache: NonceCache | None = None

    def __post_init__(self) -> None:
        if self.algorithm not in ("hmac-sha256", "hmac-sha512"):
            raise ValueError(f"Unsupported algorithm: {self.algorithm}. Use 'hmac-sha256' or 'hmac-sha512'.")
        if self.max_skew <= 0:
            raise ValueError("max_skew must be positive")
        # L-1: the signature header must not collide with the freshness headers,
        # otherwise the timestamp/nonce would be overwritten by the signature.
        if self.header_name in (SigningMiddleware._TIMESTAMP_HEADER, SigningMiddleware._NONCE_HEADER):
            raise ValueError(
                f"header_name {self.header_name!r} collides with a freshness header; "
                "choose a different signature header name."
            )


class SigningMiddleware(BaseMiddleware):
    """Signs outgoing messages and verifies incoming signatures.

    On publish: computes HMAC of body and adds signature to headers.
    On receive: verifies signature against body, rejects if invalid.
    """

    # ── Timestamp / nonce header names (replay protection) ──────────────
    _TIMESTAMP_HEADER = "x-rabbitkit-sign-timestamp"
    _NONCE_HEADER = "x-rabbitkit-sign-nonce"

    def __init__(self, config: SigningConfig) -> None:
        self._config = config
        self._key = config.secret_key.encode("utf-8") if isinstance(config.secret_key, str) else config.secret_key
        self._hash_name = "sha256" if config.algorithm == "hmac-sha256" else "sha512"
        # Default to the in-memory cache so replay protection is functional
        # out of the box for a single process; callers may inject a shared
        # (e.g. RedisNonceCache) cache — see the H4 warning below.
        self._nonce_cache: NonceCache = config.nonce_cache if config.nonce_cache is not None else TTLSetNonceCache()
        # H4: the default in-memory cache is per-process — a replay that
        # lands on a different worker/pod, or after a restart, is invisible
        # to it. This can't detect an actual multi-process deployment, so it
        # fires whenever the default is left in place with require_freshness
        # (the risky combination), rather than silently claiming replay
        # protection "works out of the box" for the common multi-replica case.
        if config.nonce_cache is None and config.require_freshness:
            import warnings

            warnings.warn(
                "SigningMiddleware is using the default in-memory TTLSetNonceCache "
                "with require_freshness=True. This nonce cache is per-process: in "
                "any multi-process or multi-pod deployment (or after a restart), a "
                "replayed message that lands on a different worker will NOT be "
                "detected. Pass nonce_cache=RedisNonceCache(...) (or your own "
                "NonceCache) to share the seen-set across processes before relying "
                "on this for security-sensitive traffic.",
                RuntimeWarning,
                stacklevel=2,
            )

    def _compute_signature(self, body: bytes) -> str:
        """Compute the legacy body-only HMAC signature (backward-compat).

        Deliberately NOT extended to cover routing metadata (H3): this path
        only runs when ``require_freshness=False`` and verifies signatures
        from producers that predate the freshness headers — external/legacy
        signers whose signing scheme this library cannot retroactively
        change. It carries no replay protection either; see the module
        docstring. Prefer the default ``require_freshness=True`` (the fresh
        path below) for any security-sensitive deployment.
        """
        return hmac.new(self._key, body, getattr(hashlib, self._hash_name)).hexdigest()

    @staticmethod
    def _canonical_route(
        exchange: str,
        routing_key: str,
        content_encoding: str | None,
        reply_to: str | None,
    ) -> bytes:
        """Canonical, NUL-delimited encoding of the routing metadata bound
        into the fresh signature (H3).

        Covers exactly ``exchange``, ``routing_key``, ``content_encoding``,
        and ``reply_to`` — the fields an attacker could otherwise flip on a
        captured, validly-signed message to re-route it, redirect an RPC
        reply, or hit a different decompression path, all without touching
        (or being able to forge) the body. Headers other than the
        signature/timestamp/nonce triplet itself are NOT covered — do not
        rely on freeform headers for security-critical routing decisions
        under this middleware. NUL (``\\x00``) is used as the field
        delimiter (including a trailing one) so concatenation cannot make
        two different field combinations hash identically.
        """
        return f"{exchange or ''}\x00{routing_key or ''}\x00{content_encoding or ''}\x00{reply_to or ''}\x00".encode()

    def _compute_fresh_signature(
        self,
        timestamp: float,
        nonce: str,
        body: bytes,
        *,
        exchange: str = "",
        routing_key: str = "",
        content_encoding: str | None = None,
        reply_to: str | None = None,
    ) -> str:
        """Compute the replay-protected, route-bound signature (H3).

        Signs ``timestamp:nonce:`` + the canonical routing metadata
        (:meth:`_canonical_route`) + ``body``. Binding the routing metadata
        means changing ``exchange``/``routing_key``/``content_encoding``/
        ``reply_to`` on a delivered message invalidates the signature even
        when the body, timestamp, and nonce are all byte-for-byte unchanged.
        """
        route = self._canonical_route(exchange, routing_key, content_encoding, reply_to)
        signed = f"{timestamp}:{nonce}:".encode() + route + body
        return hmac.new(self._key, signed, getattr(hashlib, self._hash_name)).hexdigest()

    def _verify_signature(self, body: bytes, signature: str | bytes) -> bool:
        """Verify HMAC signature using constant-time comparison."""
        expected = self._compute_signature(body)
        return _const_time_eq(expected, signature)

    def _verify_fresh_signature(
        self,
        timestamp: float,
        nonce: str,
        body: bytes,
        signature: str | bytes,
        *,
        exchange: str = "",
        routing_key: str = "",
        content_encoding: str | None = None,
        reply_to: str | None = None,
    ) -> bool:
        """Verify replay-protected, route-bound signature (H3) using
        constant-time comparison."""
        expected = self._compute_fresh_signature(
            timestamp,
            nonce,
            body,
            exchange=exchange,
            routing_key=routing_key,
            content_encoding=content_encoding,
            reply_to=reply_to,
        )
        return _const_time_eq(expected, signature)

    # ── Helpers ──────────────────────────────────────────────────────────

    def _sign_envelope(self, envelope: MessageEnvelope) -> MessageEnvelope:
        """Return a new envelope with signature (and freshness) headers added."""
        headers = dict(envelope.headers) if envelope.headers else {}
        # H4: always a fresh random nonce, independent of message_id. A
        # message_id is often caller-supplied and may be reused across
        # publishes (e.g. an at-least-once retry re-sending the "same"
        # message) — using it as the nonce would make the nonce predictable
        # and/or reused, weakening the seen-set's replay guarantee, which
        # depends on the nonce being unique per signing operation.
        nonce = uuid.uuid4().hex
        timestamp = time.time()
        sig = self._compute_fresh_signature(
            timestamp,
            nonce,
            envelope.body,
            exchange=envelope.exchange,
            routing_key=envelope.routing_key,
            content_encoding=envelope.content_encoding,
            reply_to=envelope.reply_to,
        )
        headers[self._config.header_name] = sig
        headers[self._TIMESTAMP_HEADER] = str(timestamp)
        headers[self._NONCE_HEADER] = nonce
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

    async def publish_scope_async(self, call_next: Any, envelope: MessageEnvelope) -> Any:
        """Async variant — add HMAC signature."""
        signed = self._sign_envelope(envelope)
        return await call_next(signed)

    # ── Receive: verify incoming signatures ──────────────────────────────

    def on_receive(self, message: RabbitMessage) -> None:
        """Verify signature on incoming message.

        Replay protection rules (see module docstring):
        * Freshness headers present → skew + nonce always enforced.
        * Headers absent + require_freshness=True → rejected (strict).
        * Headers absent + require_freshness=False → legacy body-only verify + warn.
        """
        sig = message.headers.get(self._config.header_name)
        if sig is None:
            if self._config.reject_unsigned:
                raise InvalidSignatureError(f"Message has no {self._config.header_name} header")
            return
        # L-2: a non-str/bytes signature header would make hmac.compare_digest
        # raise TypeError; surface it as a permanent InvalidSignatureError instead.
        if not isinstance(sig, (str, bytes)):
            raise InvalidSignatureError("signature header is not a string/bytes")

        ts_raw = message.headers.get(self._TIMESTAMP_HEADER)
        nonce = message.headers.get(self._NONCE_HEADER)

        if ts_raw is not None and nonce is not None:
            # Fresh producer path — enforce skew + nonce unconditionally.
            try:
                timestamp = float(ts_raw)
            except (TypeError, ValueError) as exc:
                raise InvalidSignatureError("Invalid signature timestamp header") from exc
            # L-3: NaN/Inf would bypass the skew check (abs(now - NaN) is NaN,
            # which compares False to any threshold); reject non-finite values.
            if not math.isfinite(timestamp):
                raise InvalidSignatureError("non-finite timestamp")

            skew = abs(time.time() - timestamp)
            if skew > self._config.max_skew:
                raise InvalidSignatureError(
                    f"Signature timestamp outside max_skew ({skew:.1f}s > {self._config.max_skew}s)"
                )

            if self._config.reject_invalid and not self._verify_fresh_signature(
                timestamp,
                str(nonce),
                message.body,
                sig,
                exchange=message.exchange,
                routing_key=message.routing_key,
                content_encoding=message.content_encoding,
                reply_to=message.reply_to,
            ):
                raise InvalidSignatureError("Message signature verification failed")

            # Nonce replay check — mark after signature verifies so bogus
            # messages can't burn nonces. TTL covers the replay window.
            if not self._nonce_cache.seen(str(nonce), self._config.max_skew):
                raise InvalidSignatureError("Replay detected: duplicate nonce")
            return

        # No freshness headers — legacy producer.
        if self._config.require_freshness:
            if self._config.reject_invalid:
                raise InvalidSignatureError("Missing freshness headers (require_freshness=True)")
            return

        logger.warning(
            "Message from %s without %s/%s headers — verifying body-only signature (no replay protection).",
            self._config.header_name,
            self._TIMESTAMP_HEADER,
            self._NONCE_HEADER,
        )
        if self._config.reject_invalid and not self._verify_signature(message.body, sig):
            raise InvalidSignatureError("Message signature verification failed")

    async def on_receive_async(self, message: RabbitMessage) -> None:
        """Async variant — verify signature."""
        self.on_receive(message)
