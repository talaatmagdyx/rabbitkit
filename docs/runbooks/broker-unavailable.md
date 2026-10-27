# Broker unavailable

**Alert:** `RabbitMQBrokerUnavailable` — `rabbitkit_broker_connected == 0` for 2 minutes.

## What it means

At least one of the service's RabbitMQ connections is down. `broker_connected`
is 1 only while every connection is up (async: publisher and consumer; sync:
the one shared connection) and follows drops and reconnects.
`rabbitkit_connection_up{role}` says which connection.

While it lasts: publishes return `NOT_SENT` (`not_connected`) or `UNKNOWN`
(`connection_lost`), nothing is consumed, readiness is false. Messages already
on the broker are safe; unacked deliveries are redelivered after the reconnect.

## Triage

1. Is it one pod or all of them? `count by (job) (rabbitkit_broker_connected == 0)`.
   All pods: the broker or the network. One pod: that pod's network, DNS or
   credentials.
2. Why: `sum by (role, reason) (increase(rabbitkit_connection_failures_total[15m]))`.
   `connection_refused` / `dns_failure` / `timeout`: the broker endpoint is
   unreachable. `broker_closed`: the broker closed the connection (restart,
   `rabbitmqctl close_connection`, node shutdown). A terminal reason
   (`authentication_failed`, ...) is the [terminal failure runbook](connection-storm.md).
3. Broker side: `rabbitmq-diagnostics -q check_running`,
   `rabbitmq-diagnostics -q check_port_connectivity`, and the node logs.
4. Logs: WARNING `... connection to host:port lost ...; reconnecting` and
   `reconnect attempt N ... failed` lines carry the endpoint and the error.

## Mitigation

- Broker down: restore it; the clients reconnect on their own (exponential
  backoff with jitter, unlimited unless `reconnect_budget_*` is set).
- Cluster: `ConnectionConfig.nodes` lets a reconnect move to another node.
- Do not restart healthy pods in a loop: that only adds connection churn.
