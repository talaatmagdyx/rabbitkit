"""Retry hardening: sanitized DLQ triage headers and bounded handoff failures.

Runs WITHOUT a broker: it drives ``RetryMiddleware`` directly with a fake
delay-queue publish so you can see two behaviours in isolation:

1. The retry envelope's triage headers never carry raw exception text. URL
   passwords, ``token=`` pairs and long opaque tokens are redacted, and an
   ``x-rabbitkit-error-category`` header (transient/permanent) is added.
2. When the delay-queue publish itself fails (returned / nacked / timed out /
   connection drop) the source message is NACK-REQUEUED — never acked — and
   consecutive failures get capped exponential backoff instead of a hot loop,
   until the tracker reports EXHAUSTED and the ``on_handoff_exhausted`` hook
   lets an owner stop the consumer.

Run:
    python examples/bulk_operations/06_retry_handoff_and_sanitizer.py
"""

from rabbitkit import (
    HandoffState,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    RabbitMessage,
    RetryConfig,
    RetryHandoffConfig,
    RetryHandoffTracker,
)
from rabbitkit.middleware.retry import RetryMiddleware

SECRET = "sup3rS3cret"


def make_message() -> RabbitMessage:
    msg = RabbitMessage(body=b'{"order": 1}', headers={"x-rabbitkit-original-queue": "orders"}, routing_key="orders")
    msg._ack_fn = lambda: print("    source ACKED")
    msg._nack_fn = lambda requeue: print(f"    source NACKED (requeue={requeue})")
    return msg


def show_sanitized_headers() -> None:
    print("1. sanitized triage headers")
    mw = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120)))
    exc = ConnectionError(f"amqp://svc:{SECRET}@rabbit.internal:5672/prod refused; token={SECRET}")
    envelope = mw._build_retry_envelope(make_message(), retry_count=0, exc=exc)
    for key in ("x-rabbitkit-error-category", "x-rabbitkit-error-type", "x-rabbitkit-error-message"):
        print(f"    {key}: {envelope.headers[key]}")
    assert SECRET not in repr(envelope.headers), "secret leaked!"

    mw_omit = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120), error_detail="omit"))
    envelope = mw_omit._build_retry_envelope(make_message(), retry_count=0, exc=exc)
    print(f"    error_detail='omit' → message header present: {'x-rabbitkit-error-message' in envelope.headers}\n")


def show_handoff_backoff() -> None:
    print("2. retry-publish (handoff) failures are braked, never acked")

    def broken_delay_queue_publish(envelope: MessageEnvelope) -> PublishOutcome:
        # The broker bounced the retry envelope (e.g. someone deleted orders.retry.1).
        return PublishOutcome(status=PublishStatus.RETURNED, routing_key=envelope.routing_key)

    exhausted: list[str] = []
    tracker = RetryHandoffTracker(
        RetryHandoffConfig(backoff_initial=0.5, backoff_max=4.0, jitter=0.0, max_consecutive_failures=4)
    )
    mw = RetryMiddleware(
        RetryConfig(max_retries=3, delays=(5, 30, 120)),
        publish_fn=broken_delay_queue_publish,
        handoff_tracker=tracker,
        on_handoff_failure=lambda backoff, t: print(f"    hook: back off {backoff:.1f}s (state={t.state.value})"),
        on_handoff_exhausted=lambda t: exhausted.append(t.state.value),
    )
    for attempt in range(1, 5):
        print(f"  redelivery #{attempt}")
        mw._route_to_delay_queue_sync(make_message(), retry_count=0, exc=ConnectionError("db down"))
    print(f"\n  tracker: consecutive={tracker.consecutive_failures} state={tracker.state.value}")
    print(f"  exhausted hook fired with: {exhausted}")
    assert tracker.state is HandoffState.EXHAUSTED

    # A successful handoff resets the streak.
    mw_ok = RetryMiddleware(
        RetryConfig(max_retries=3, delays=(5, 30, 120)),
        publish_fn=lambda e: PublishOutcome(status=PublishStatus.CONFIRMED),
        handoff_tracker=tracker,
    )
    print("  delay queue restored:")
    mw_ok._route_to_delay_queue_sync(make_message(), retry_count=0, exc=ConnectionError("db down"))
    print(f"  tracker: state={tracker.state.value} total_failures={tracker.total_failures}")


if __name__ == "__main__":
    show_sanitized_headers()
    show_handoff_backoff()
