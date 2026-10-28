# Flow control and resource alarms

**Alerts** (all client side, from rabbitkit's own metrics, `source: rabbitkit`):

- `RabbitMQDiskAlarm` — `rabbitkit_connection_blocked_reason{reason="disk"}`:
  the broker blocked the service's connections with "low on disk".
- `RabbitMQMemoryAlarm` — `rabbitkit_connection_blocked_reason{reason="memory"}`:
  "low on memory".
- `RabbitMQConnectionBlocked` — `rabbitkit_connection_blocked`, for any other
  reason (or a client that cannot read it). It does not fire alongside the
  disk or memory alert.

Each fires when the connection was blocked for most of the last 2 minutes,
for 1 minute. The `rabbitmq-broker-plugin` group has broker-side
`RabbitMQDiskAlarm` / `RabbitMQMemoryAlarm` (`source: rabbitmq_prometheus`,
from `rabbitmq_alarms_free_disk_space_watermark` /
`rabbitmq_alarms_memory_used_watermark`) for clusters that run the
`rabbitmq_prometheus` plugin: they see an alarm before any client publishes.

## What it means

When a node crosses its memory high watermark or its free-disk limit,
RabbitMQ raises an alarm and blocks every publishing connection in the
cluster (`connection.blocked`, with the reason "low on disk" or "low on
memory"). rabbitkit sets `connection_blocked` to 1 and
`connection_blocked_reason` to 1 for the named alarm until
`connection.unblocked`; `transport.blocked_reason` has the broker's text.
RabbitMQ blocks a connection when it next publishes, so a service that is
not publishing does not see the alarm.

While blocked: publishes wait and then time out (`TIMEOUT` / `NOT_SENT` with
an opt-in `FlowController`), consumers keep consuming (which is what clears a
memory alarm). With `ConnectionConfig.blocked_connection_timeout` set (60 s
by default), a publisher connection blocked longer than that is closed and
reconnected; the gauges briefly read 0 after each such reconnect, which is
why the alerts average over 2 minutes.

## Triage

1. Which alarm: the alert name (or `rabbitkit_connection_blocked_reason`),
   then `rabbitmq-diagnostics alarms`, `rabbitmq-diagnostics -q memory_breakdown`,
   `df` on the node's data directory.
2. Which queues hold the memory or disk: large backlogs
   ([consumer backlog](consumer-backlog.md)), DLQs nobody drains
   ([DLQ growth](dlq-growth.md)), classic queues with large message bodies.

## Mitigation

- Let consumers drain; stop or slow the producers that filled the queues.
- Free disk / add capacity; raise the watermark only with a capacity plan.
- Never restart consumers to "unblock": they are what drains the backlog.
