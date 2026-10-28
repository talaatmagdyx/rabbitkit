# Flow control and resource alarms

**Alerts:**

- `RabbitMQConnectionBlocked` — `rabbitkit_connection_blocked == 1` (client side).
- `RabbitMQDiskAlarm`, `RabbitMQMemoryAlarm` — broker side, need the
  `rabbitmq_prometheus` plugin (`rabbitmq_alarms_free_disk_space_watermark`,
  `rabbitmq_alarms_memory_used_watermark`).

## What it means

When a node crosses its memory high watermark or its free-disk limit,
RabbitMQ raises an alarm and blocks every publishing connection in the
cluster (`connection.blocked`). rabbitkit sets `connection_blocked` to 1
until `connection.unblocked`. The client cannot tell which alarm it is: the
broker-side rules (or `rabbitmq-diagnostics alarms`) say which.

While blocked: publishes wait and then time out (`TIMEOUT` / `NOT_SENT` with
an opt-in `FlowController`), consumers keep consuming (which is what clears a
memory alarm). With `ConnectionConfig.blocked_connection_timeout` set, a
connection blocked longer than that is closed and reconnected.

## Triage

1. `rabbitmq-diagnostics alarms`, `rabbitmq-diagnostics -q memory_breakdown`,
   `df` on the node's data directory.
2. Which queues hold the memory or disk: large backlogs
   ([consumer backlog](consumer-backlog.md)), DLQs nobody drains
   ([DLQ growth](dlq-growth.md)), classic queues with large message bodies.

## Mitigation

- Let consumers drain; stop or slow the producers that filled the queues.
- Free disk / add capacity; raise the watermark only with a capacity plan.
- Never restart consumers to "unblock": they are what drains the backlog.
