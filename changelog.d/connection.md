<!-- Connection stream (G-08, G-10). Merge into CHANGELOG.md [Unreleased]. No README change needed. -->

### Changed (behaviour)

- **Permanent connection failures fail fast instead of being retried
  (async).** A refused login (`ACCESS_REFUSED`), a refused vhost
  (`NOT_ALLOWED` at `connection.open`: missing vhost or no permissions) and a
  TLS failure that retrying cannot fix (certificate verification, hostname
  mismatch, a broker-rejected client certificate, a non-TLS port) used to be
  retried like a refused socket: 30 attempts at startup (6 to 12 minutes;
  reproduced: still retrying after 15 s) and forever after any later drop.
  Now `AsyncBroker.start()` raises `AuthenticationFailedError`,
  `VhostAccessRefusedError` or `TLSVerificationError` at once (measured on a
  real broker: under 5 s, typically tens of milliseconds). After startup, a
  reconnect attempt that hits one stops that connection for good (see
  "terminal connection failure" below). These are `ConnectionError`s, like
  the aiormq/ssl errors they replace, so `except ConnectionError` /
  `except OSError` around `start()` still catches them. A `NOT_ALLOWED` that
  says the vhost "is down" stays retryable. Credential rotation must make the
  new secret available to `credentials_provider` before the old one is
  revoked.
- **Reconnects after the first connect back off exponentially with full
  jitter (async).** aio-pika retried at one fixed interval per process
  (in `[base, 2*base]`) forever. Each failed reconnect attempt now waits
  `uniform(0, min(reconnect_backoff_max, reconnect_backoff_base * 2**(n-1)))`,
  and the counter resets when a reconnect succeeds. Still unlimited by
  default (see the new budgets below).
- **`ConnectionConfig` rejects timing values that broke reconnects:**
  `reconnect_backoff_base <= 0` (every retry was immediate),
  `reconnect_backoff_max < reconnect_backoff_base`, `socket_timeout <= 0`
  and `heartbeat < 0` raise `ConfigValidationError`; `heartbeat=0` (which
  disables dead-peer detection) warns.
- **`AsyncBroker.run()` raises a terminal connection failure** after
  stopping the broker, so the process exits and its supervisor restarts it.
  Under bare `start()` nothing stops by itself; health turns false and
  `on_connection_failed` hooks fire.
- **`broker_liveness()` is False once a connection has failed for good**
  (it ignored connectivity before): only a restart recovers. A transient
  disconnect still counts as alive.
- **The async broker emits `rabbitkit_reconnects_total` again** (with a
  `MetricsMiddleware` on a route), like the sync broker. It counts
  connections: one outage counts two (publisher and consumer). The startup
  log line saying it was not emitted is gone.

### Added

- `ConnectionConfig.reconnect_budget_attempts` / `reconnect_budget_seconds`
  (default `None`, unlimited: the old availability-first behaviour): bound
  one outage of the async broker by consecutive failed reconnect attempts
  and/or seconds since the connection was lost. When spent, the connection
  stops for good with `ReconnectBudgetExhaustedError`. The sync broker
  ignores both; its `reconnect_max_attempts` is unchanged.
- Exceptions (in `rabbitkit.core.errors`, exported from `rabbitkit`):
  `TerminalConnectionError` (a `ConnectionError`) with
  `PermanentConnectionError` (`AuthenticationFailedError`,
  `VhostAccessRefusedError`, `TLSVerificationError`) and
  `ReconnectBudgetExhaustedError`. Each has a bounded snake_case `reason`
  and the failing `endpoint`; messages never contain credentials.
- **Terminal connection failure (async).** When a connection stops
  reconnecting for good (permanent failure or spent budget) it is closed,
  `AsyncTransportImpl.connection_failure` holds the error, `is_connected()`
  is False and stays False (no connection is attempted again, so publishes
  fail before anything is sent), `broker_health_check` reports
  `connection failed for good (<reason>); restart required`, and a CRITICAL
  line is logged.
- Transport hooks for the reconnect lifecycle:
  `AsyncTransportImpl.on_reconnect_attempt(cb)` (after every failed
  reconnect attempt; receives a `ReconnectAttempt` with `role`
  ("publisher"/"consumer"), `attempt`, `delay`, `elapsed`, `endpoint`,
  `error`) and `on_connection_failed(cb)` (once, with the
  `TerminalConnectionError`), next to the existing `on_reconnect`.

### Fixed

- **Credential rotation and failover did not apply to aio-pika's in-place
  reconnects.** `credentials_provider` and `ConnectionConfig.nodes` were only
  used when rabbitkit built a connection itself; after a drop aio-pika
  reconnected with the first URL, so a rotated password failed forever
  (reproduced) and a connection stayed pinned to a dead node. Both are now
  re-resolved before every reconnect attempt: after a drop the connection
  retries its node with fresh credentials, then moves to the next endpoint
  after each failed attempt. aio-pika still restores channels, QoS,
  topology and consumers. `docs/security.md` described this behaviour
  already; it is now true.
- **A failed `AsyncBroker.start()` leaked everything it had started.**
  If start() failed part-way (a 403/404/406 during topology, a refused
  consume, a cancellation), both connections, their blocked-connection
  monitors, the batch publisher's workers and any consumer already started
  kept running, and `stop()` was a no-op. Reproduced: 2 connections left
  behind per failed start. start() now undoes what it created (consumers,
  worker pool, in-flight handlers, batch publisher, heartbeat, signal
  handlers, connections) before re-raising, so it can simply be retried.
  `AsyncTransportImpl.connect()` and `AsyncConnectionPool.connect()` are
  all-or-nothing as well.
- A cancelled initial connect left aio-pika's background reconnect task
  running forever; the abandoned connection object is now closed.
- `AsyncBroker.stop()` hung after a terminal connection failure: cancelling
  a consumer on a connection closed for good waited forever for a
  reconnect. The AMQP cancel is skipped there.

### Documentation

- `docs/security.md` (rotation order, permanent failures),
  `docs/observability.md` (new "Async reconnects" section replacing "Async
  does not emit reconnects_total"; reconnect hooks),
  `docs/kubernetes.md` (async failover on reconnect, liveness after a
  terminal failure), `docs/concurrency-model.md`,
  `docs/production/scale.md`, `docs/production/checklist.md`.
