# Event Metadata

Optional, generic identity for event messages. The body stays the payload
(any codec); the identity travels in AMQP properties and a few headers, so the
broker, the DLQ tooling, deduplication and tracing can read it without decoding
the body. Nothing in rabbitkit requires or reads these fields unless you use
the API on this page. Whether your services require them is an application
contract choice: the CRITICAL [reliability profile](reliable-messaging.md#enabling-the-critical-profile)
does not turn it on.

## Field mapping

| Field | AMQP location | Required when read | Notes |
|---|---|---|---|
| `message_id` | `message_id` property | yes | Reuse it when republishing the same logical message (after an `UNKNOWN` publish outcome). A time-ordered id (UUIDv7) is a good choice; `EventMetadata.create()` defaults to a UUIDv4. |
| `idempotency_key` | header `idempotency-key` | yes | Business deduplication identity, e.g. `order:1234`. Stable across producers and republishes. |
| `event_type` | `type` property | yes | e.g. `order.created` |
| `event_version` | header `event-version` (int) | yes | Schema version of the body, `>= 1`. |
| `source` | `app_id` property | yes | Producing service. |
| `occurred_at` | header `occurred-at` (RFC 3339, UTC) | yes | Business event time. The AMQP `timestamp` property is the publish time. |
| `correlation_id` | `correlation_id` property | yes | `EventMetadata.create()` defaults it to `message_id`. |
| `tenant_id` | header `tenant-id` | no (`require_tenant=True` makes it required) | Keep tenants out of routing keys and metric labels. |
| trace context | `traceparent` / `tracestate` | — | Not part of `EventMetadata`: `OTelTracingMiddleware` owns these headers. |

Every string is non-empty and at most 255 UTF-8 bytes (the AMQP short-string
limit for the property fields; the same bound for the header fields). Hash a
longer business key before using it as `idempotency_key`.

## Publishing

```python
from rabbitkit import AsyncBroker, EventMetadata

meta = EventMetadata.create(
    event_type="order.created",
    event_version=1,
    source="checkout-service",
    idempotency_key="order:1234",
)
outcome = await broker.publish(meta.to_envelope(routing_key="orders", body=b'{"id": 1234}'))
```

- `EventMetadata(...)` / `EventMetadata.create(...)` validate every field and
  raise `EventMetadataError` (a `ValueError`) naming the bad fields, before
  anything is published. The error message never contains the values.
- `to_envelope(...)` builds a `MessageEnvelope`; `apply_to(envelope)` adds the
  metadata to an existing one. Both set the AMQP `timestamp` to now when the
  envelope has none, and refuse (raise) to overwrite a *different* `type`,
  `app_id`, `correlation_id` or metadata header already on the envelope.
  `message_id` always comes from the metadata.

## Validating

Add `EventMetadataMiddleware` where you want the contract enforced:

```python
from rabbitkit import EventMetadataMiddleware

# Every broker.publish() must carry valid metadata of a supported version.
broker = AsyncBroker(config, middlewares=[EventMetadataMiddleware(supported_versions={"order.created": {1, 2}})])


# Every delivery on this route must carry valid metadata before the handler runs.
@broker.subscriber(
    "orders",
    middlewares=[EventMetadataMiddleware(supported_versions={"order.created": {1, 2}}, validate_publish=False)],
)
async def handle(body: dict, message: RabbitMessage) -> None:
    meta = EventMetadata.from_message(message)
```

- `supported_versions`: `None` (any version), a collection of versions for
  every event type, or a mapping `event_type -> versions` (an event type that
  is not in the mapping is rejected).
- On consume, a missing or invalid field or an unsupported version raises
  `EventMetadataError`, an `InvalidMessageError`: category
  `INVALID_MESSAGE`, never retried, dead-lettered. With a `RetryConfig` on
  the route (`terminal_action="dead_letter"`, the default) the DLQ copy carries
  `x-rabbitkit-error-category: invalid_message` and
  `x-rabbitkit-error-type: EventMetadataError`, because the auto-wired
  `RetryMiddleware` sits outside route middlewares. Without retry, the
  pipeline rejects the delivery into the source queue's dead-letter exchange.
- On publish (`publish_scope`), the envelope is validated before it is sent,
  and the AMQP `timestamp` is set when missing. On a route, `publish_scope`
  wraps the handler's result / RPC reply publishes: pass
  `validate_publish=False` when those are not events, otherwise a result
  without metadata raises after the handler ran and the source delivery is
  dead-lettered.
- `require_tenant=True` also requires `tenant-id`.

You can read and validate without the middleware:
`EventMetadata.from_message(message)` / `EventMetadata.from_envelope(envelope)`
and `check_supported_version(meta, policy)`.

## Retries, DLQ and replay

Retry delay-queue copies, terminal DLQ copies and `DLQInspector` replays copy
every AMQP property (`message_id`, `correlation_id`, `type`, `app_id`,
`timestamp`, `delivery_mode`, ...) and every header, so the metadata arrives
unchanged at every hop (unit tests in `tests/unit/middleware/test_event_metadata.py`,
real-broker test in `tests/integration/test_profile_foundation.py`).

## Deduplication

`idempotency_key` is the key to deduplicate on. See
[Reliable Messaging → Idempotency](reliable-messaging.md#idempotency) for the
durable-inbox pattern; rabbitkit does not deduplicate on its own.
