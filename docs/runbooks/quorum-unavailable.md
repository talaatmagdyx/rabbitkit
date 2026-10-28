# Quorum unavailable

**Alerts** (client side, from the `QueueMetricsPoller` gauges
`rabbitkit_queue_quorum_members` / `rabbitkit_queue_quorum_online`, which
read the `members` and `online` lists the management API reports for every
quorum queue; no broker plugin needed):

- `RabbitMQQuorumMajorityLost` (critical) — `online <= members / 2` for 1
  minute: the queue has lost its majority and is down.
- `RabbitMQQuorumUnhealthy` (warning) — `online < members` (but still a
  majority) for 5 minutes: the queue works but has lost redundancy.

The `rabbitmq-broker-plugin` group has a cluster-level
`RabbitMQQuorumUnhealthy` (`source: rabbitmq_prometheus`): fewer nodes report
`rabbitmq_identity_info` than in the last day.

## What it means

A quorum queue needs a majority of its members online to accept publishes
and deliver. With 3 members, one offline is a warning (one more failure
stops the queue) and two offline is an outage. Measured on a 3-node cluster:
with 2 of 3 members down, publishes resolve `TIMEOUT` (never `CONFIRMED`)
and confirm again 10 to 18 s after the nodes return
([reliable messaging](../reliable-messaging.md#verified-failure-scenarios)).

Client symptoms: publishes to the queue time out or are nacked
([publisher runbook](publisher-confirm-timeout.md)), consumers are cancelled
by the broker (`rabbitkit_consumer_cancelled_total`) and re-subscribe once a
leader is elected.

## Triage

1. `rabbitmq-diagnostics -q cluster_status` and `rabbitmq-queues quorum_status <queue>`.
2. Which nodes are down or partitioned; recent restarts or network events.
3. Members below majority for a queue: `rabbitmq-queues check_if_node_is_quorum_critical`.

## Mitigation

- Bring the missing nodes back; quorum queues recover on their own once a
  majority is online.
- During rolling restarts restart one node at a time and wait for
  `check_if_node_is_quorum_critical` to pass.
- Do not delete and re-create a quorum queue to "fix" it: that loses its
  messages.
