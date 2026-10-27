# Connection storm / terminal connection failure

**Alerts:**

- `RabbitMQConnectionFailures` — `rabbitkit_reconnect_attempts_total` keeps rising.
- `RabbitMQConnectionTerminalFailure` — a `connection_failures_total` with a terminal reason.

## What it means

`reconnect_attempts_total{role}` counts FAILED reconnect attempts of a
connection that had been up; successful ones count in `reconnects_total`.
`connection_failures_total{role,reason}` counts lost connections, failed
attempts and terminal give-ups, by bounded reason.

Terminal reasons stop reconnecting for good:

| reason | cause |
|---|---|
| `authentication_failed` | the broker refused the credentials |
| `vhost_access_refused` | the vhost does not exist or the user has no permissions on it |
| `tls_verification_failed`, `tls_handshake_rejected` | certificate verification / client certificate / non-TLS port |
| `reconnect_budget_exhausted` | `reconnect_budget_attempts` / `reconnect_budget_seconds` ran out |

After a terminal failure the connection is closed, readiness and liveness are
false, and `AsyncBroker.run()` stops the broker and raises so the supervisor
restarts the process.

## Triage

1. `sum by (role, reason) (increase(rabbitkit_connection_failures_total[15m]))`.
2. Many pods at once: broker restarts, load balancer idle timeouts, or a
   heartbeat too short for the network (`broker_closed`, `connection_lost`).
   One pod: its network or DNS.
3. Churn without failures: `rate(rabbitkit_reconnects_total[5m])` high with a
   low failure rate means connections drop and come back; check broker logs
   for `client unexpectedly closed TCP connection` / missed heartbeats.
4. Terminal: the CRITICAL log line `RabbitMQ connection failed for good (<reason>)`
   names the endpoint; never the password.

## Mitigation

- `authentication_failed` during credential rotation: the new secret must be
  available to `credentials_provider` before the old one is revoked. Fix,
  then restart the pods.
- `vhost_access_refused`: create the vhost or grant permissions.
- TLS: fix the CA bundle / certificate / port; retrying does not help.
- `reconnect_budget_exhausted`: the broker was unreachable for longer than the
  budget; restore it, then restart (or rely on the supervisor).
- A storm caused by many clients reconnecting at once settles by itself (full
  jitter); do not restart pods to "help".
