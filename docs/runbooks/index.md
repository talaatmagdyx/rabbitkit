# Runbooks

One page per alert family in
[`deploy/prometheus/rabbitkit-alerts.yaml`](https://github.com/talaatmagdyx/rabbitkit/blob/main/deploy/prometheus/rabbitkit-alerts.yaml).
Every alert's `runbook_url` annotation points here. Thresholds in the rules
are placeholders: tune them from your measured workloads before paging on
them. The metrics are listed in the [observability reference](../observability.md).

| Alert | Runbook |
|---|---|
| `RabbitMQBrokerUnavailable` | [Broker unavailable](broker-unavailable.md) |
| `PublisherConfirmFailures`, `PublisherUnknownOutcomes` | [Publisher confirm failures and unknown outcomes](publisher-confirm-timeout.md) |
| `PublisherUnroutableMessages` | [Unroutable publishes](unroutable.md) |
| `RabbitMQConnectionFailures`, `RabbitMQConnectionTerminalFailure` | [Connection storm / terminal connection failure](connection-storm.md) |
| `ConsumerBacklogGrowing`, `ConsumerUnackedHigh` | [Consumer backlog](consumer-backlog.md) |
| `ConsumerProcessingFailures`, `ConsumerNotConsuming` | [Consumer failures](consumer-failures.md) |
| `RetryStormDetected` | [Retry storm](retry-storm.md) |
| `DeadLetterQueueGrowing` | [DLQ growth](dlq-growth.md) |
| `RabbitMQQuorumUnhealthy` | [Quorum unavailable](quorum-unavailable.md) |
| `RabbitMQConnectionBlocked`, `RabbitMQDiskAlarm`, `RabbitMQMemoryAlarm` | [Flow control and resource alarms](flow-control.md) |
