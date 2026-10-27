# Quorum unavailable

**Alert:** `RabbitMQQuorumUnhealthy` — needs the RabbitMQ `rabbitmq_prometheus`
plugin (`rabbitmq_identity_info`, `rabbitmq_raft_term_total`); rabbitkit cannot
see Raft state from the client.

## What it means

A quorum queue needs a majority of its members online to accept publishes
and deliver. The rule fires when fewer nodes report than the expected cluster
size (edit the `3` in the rule to your size), or when Raft leader elections
churn (term increasing quickly).

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
