### Added

- **Resource alarms by cause, from the client.** RabbitMQ's
  `connection.blocked` carries a reason (`"low on disk"` / `"low on
  memory"`). New gauge `rabbitkit_connection_blocked_reason{reason}` with the
  bounded labels `disk`, `memory`, `other` is 1 for each alarm the active
  reason names (all 0 while unblocked), and `transport.blocked_reason` (sync
  and async) has the broker's text. On async, aiormq only logged the reason:
  `ensure_aiormq_records_blocked_reason()` (called at connect) wraps aiormq's
  private blocked handler at class level to keep it; on an aiormq without
  that handler the reason is reported as `other`.
  `BrokerMetrics.blocked(is_blocked, reason=None)` and
  `BrokerMetrics.sample(..., connection_blocked_reason=None)` take it
  (both new parameters optional).
- **Quorum-queue health from the management API.** `QueueMetricsPoller` now
  exports `rabbitkit_queue_quorum_members{queue}` and
  `rabbitkit_queue_quorum_online{queue}` for every quorum queue, counted from
  the `members` / `online` lists the management API reports (verified on
  RabbitMQ 3.13 and 4.1, and on a 3-node cluster where `online` drops when a
  member's node stops). `QueueInfo` gains `members`, `online`, `leader`.
- **Alert rules that need no broker plugin.** `deploy/prometheus/rabbitkit-alerts.yaml`:
  `RabbitMQDiskAlarm` / `RabbitMQMemoryAlarm` now come from
  `connection_blocked_reason` (default groups, `source: rabbitkit`); new
  `RabbitMQQuorumMajorityLost` (critical, `online <= members / 2`) and a
  per-queue `RabbitMQQuorumUnhealthy` (warning, `online < members`) from the
  poller; new `PublisherConfirmLatencyHigh` (p99 confirm latency); a
  `rabbitkit-recording` group with the ratios the alerts compare. The
  plugin-based rules moved to an alternative group,
  `rabbitmq-broker-plugin` (`source: rabbitmq_prometheus`).
- Nightly real-broker run on the aio-pika 9.1.0 floor:
  `.github/workflows/dependency-matrix.yml`'s `integration-aio-pika-9` job is
  now a matrix over 9.1.0 and 9.6.2.

### Changed (behaviour)

- **Alert thresholds are reasoned defaults, no longer placeholders.** Every
  alert has a `rationale` annotation and a `source` label. Changed
  expressions (alert names unchanged): `PublisherUnknownOutcomes` is a ratio
  (> 1% for 10m, was any for 5m); `RabbitMQConnectionFailures` is > 10 failed
  attempts in 5m for 5m (was > 5 in 10m for 10m, which a single broker
  restart could trip); `RabbitMQConnectionBlocked` averages over 2m so the
  blocked-connection watchdog's reconnects do not hide an alarm, and no
  longer fires for disk/memory alarms (their own alerts do);
  `ConsumerBacklogGrowing` uses the drain time (backlog / delivery rate >
  10 min and growing, was > 1000 ready); `ConsumerUnackedHigh` fires on
  unacked deliveries with no handler completion for 10m (was > 5000 unacked);
  `ConsumerProcessingFailures` and `RetryStormDetected` need at least one
  handler run a minute; the plugin `RabbitMQQuorumUnhealthy` compares the
  node count with its own 1-day maximum (no hardcoded `3`) and no longer
  uses `rabbitmq_raft_term_total`, whose rate scales with the number of
  queues.

### Fixed

- **aio-pika 9.x: `connection_name` and `client_properties` never reached
  the broker.** aio-pika 9.x `connect_robust()` folds `client_properties`
  into `make_url()`, which returns a caller-supplied URL untouched, and never
  passes them to the connection class (only aio-pika 10 does). On the whole
  9.x line (the 9.1.0 floor through 9.6.2) connections were nameless in the
  management UI. `RabbitkitRobustConnection` now sets them where aiormq reads
  them, on every connect and reconnect.
- **aio-pika 9.1: `RabbitMessage.timestamp` was a naive datetime.** pamqp
  3.2 (resolved by aio-pika 9.1) decodes the AMQP timestamp with
  `utcfromtimestamp()`; `.timestamp()` on it read it as local time, shifting
  it by the host's UTC offset. The async transport now returns an aware UTC
  datetime, like the sync transport.
- Tests: the aiormq transport-factory unit test and the integration helper
  that holds confirms open now work on aiormq 6.7 (aio-pika 9.1), so the
  nightly dependency-matrix unit run on 9.1.0 passes.

### Documentation

- `docs/observability.md`: the new metrics, the alert table, the plugin
  alternative, and a "Tuning the alerts" section (which knob, how to derive
  it from an SLO, examples). Runbooks updated (flow control, quorum,
  consumer backlog, connection storm, publisher). `docs/troubleshooting.md`:
  the two aio-pika 9.x notes.
