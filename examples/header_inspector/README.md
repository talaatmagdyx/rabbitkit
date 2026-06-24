# header_inspector

A runnable example that answers **"what headers/fields does a consumer actually see?"**

It spins up RabbitMQ in Docker, publishes a batch of **dynamically-generated** messages
(random tenant, trace-id, event type, priority, per-message TTL, etc.), and a consumer
**dumps every field** on each incoming `RabbitMessage` — headers dict, all AMQP
properties, delivery metadata, and the async-only `raw_message` escape hatch.

## Run

```bash
cd examples/header_inspector
docker compose up -d                 # RabbitMQ on 127.0.0.1:5672, UI on http://127.0.0.1:15672 (guest/guest)
pip install -e ../..[async]          # or: pip install rabbitkit[async]
python inspect_headers.py
docker compose down                  # stop the broker
```

## What it shows

For each consumed message the script prints, grouped:

- **`msg.headers`** — your custom headers (`x-tenant`, `trace-id`, …) plus any protocol
  headers (`x-rabbitkit-*`, and RabbitMQ's `x-death` if the message was dead-lettered).
- **AMQP properties** surfaced as attributes: `message_id`, `correlation_id`, `reply_to`,
  `content_type`, `content_encoding`, `type`, `app_id`, `timestamp`.
- **Delivery metadata**: `routing_key`, `exchange`, `delivery_tag`, `redelivered`, `consumer_tag`.
- **Other**: `body`, `path`, `is_settled`, whether `raw_message` is set.
- **`raw_message`** (async only): `priority`, `expiration`, `timestamp`, `delivery_mode`, `user_id`.

## Gotchas it makes visible

- **`msg.timestamp` is `None` on consume** even though the producer set it — rabbitkit
  surfaces it on publish but doesn't map it back. The real value is on
  `msg.raw_message.timestamp` (async). The output prints both so you can see the gap.
- **`priority` / `expiration` / `delivery_mode` / `user_id` are not attributes** on
  `RabbitMessage` — only reachable via `msg.raw_message`, which is **async-only**
  (the sync transport leaves `raw_message=None`).

Each run produces different data (random producer), so re-run it to see varied headers.
