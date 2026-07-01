"""Security regression: SigningMiddleware replay attack (L17).

Black-box scenario using only the public middleware API: sign a message the
way a real publisher would (``publish_scope``), simulate the wire carrying
it to a consumer, then simulate an attacker capturing and re-sending that
exact message. The second delivery must be rejected.

See ``tests/unit/middleware/test_signing.py::TestReplayProtection`` for
implementation-level coverage of the same nonce/timestamp mechanics.
"""

from __future__ import annotations

import time

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.signing import InvalidSignatureError, SigningConfig, SigningMiddleware

SECRET = "attacker-does-not-know-this"


def _envelope_after_wire(envelope: MessageEnvelope) -> RabbitMessage:
    """Simulate the envelope crossing the wire and arriving as a delivery.

    Only carries the fields a real transport would deliver back --
    routing_key, body, headers, exchange, content_encoding, reply_to (the
    fields SigningMiddleware's signature covers; see its module docstring).
    """
    return RabbitMessage(
        body=envelope.body,
        routing_key=envelope.routing_key,
        headers=dict(envelope.headers),
        exchange=envelope.exchange,
        content_encoding=envelope.content_encoding,
        reply_to=envelope.reply_to,
    )


class TestSigningReplayAttack:
    def test_replayed_message_is_rejected(self) -> None:
        """An attacker who captures one signed delivery and resends it
        verbatim is rejected on the second delivery."""
        mw = SigningMiddleware(SigningConfig(secret_key=SECRET, require_freshness=True))

        original = MessageEnvelope(routing_key="payments.charge", body=b'{"amount": 999999}')
        signed = mw.publish_scope(lambda e: e, original)

        captured = _envelope_after_wire(signed)

        mw.on_receive(captured)  # legitimate first delivery: accepted

        replayed = _envelope_after_wire(signed)  # attacker resends the exact same bytes
        with pytest.raises(InvalidSignatureError, match="Replay detected"):
            mw.on_receive(replayed)

    def test_replay_rejected_even_across_separate_middleware_instances(self) -> None:
        """A shared nonce_cache (e.g. Redis in production) rejects a replay
        even when the attacker's resend is processed by a different
        consumer process/worker than the original delivery."""
        from rabbitkit.middleware.signing import TTLSetNonceCache

        shared_cache = TTLSetNonceCache()
        cfg = SigningConfig(secret_key=SECRET, require_freshness=True, nonce_cache=shared_cache)

        publisher_side = SigningMiddleware(cfg)
        worker_a = SigningMiddleware(cfg)
        worker_b = SigningMiddleware(cfg)

        original = MessageEnvelope(routing_key="orders.created", body=b'{"order_id": 1}')
        signed = publisher_side.publish_scope(lambda e: e, original)

        worker_a.on_receive(_envelope_after_wire(signed))  # worker A gets it first

        with pytest.raises(InvalidSignatureError, match="Replay detected"):
            worker_b.on_receive(_envelope_after_wire(signed))  # attacker replays to worker B

    def test_tampered_body_after_capture_is_rejected(self) -> None:
        """An attacker who captures a signed message and modifies the body
        before resending (not a pure replay) is still rejected -- the
        signature covers the body, so a mismatch is detected regardless of
        the nonce/timestamp being fresh-looking."""
        mw = SigningMiddleware(SigningConfig(secret_key=SECRET, require_freshness=True))

        original = MessageEnvelope(routing_key="payments.charge", body=b'{"amount": 10}')
        signed = mw.publish_scope(lambda e: e, original)

        tampered = _envelope_after_wire(signed)
        tampered.body = b'{"amount": 999999}'  # attacker inflates the amount

        with pytest.raises(InvalidSignatureError, match="verification failed"):
            mw.on_receive(tampered)

    def test_legitimate_traffic_at_different_times_is_never_falsely_flagged(self) -> None:
        """Sanity check that the replay guard doesn't cry wolf: distinct,
        legitimately-published messages are all accepted."""
        mw = SigningMiddleware(SigningConfig(secret_key=SECRET, require_freshness=True))

        for i in range(5):
            envelope = MessageEnvelope(routing_key="orders.created", body=f'{{"order_id": {i}}}'.encode())
            signed = mw.publish_scope(lambda e: e, envelope)
            mw.on_receive(_envelope_after_wire(signed))  # must not raise
            time.sleep(0.001)  # ensure distinct timestamps, not required for correctness
