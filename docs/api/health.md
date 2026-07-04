# Health Checks

## broker_health_check

::: rabbitkit.health.broker_health_check

## broker_liveness / broker_readiness

::: rabbitkit.health.broker_liveness
::: rabbitkit.health.broker_readiness
::: rabbitkit.health.broker_liveness_async
::: rabbitkit.health.broker_readiness_async

## HealthStatus / BrokerHealthResult

::: rabbitkit.health.HealthStatus
::: rabbitkit.health.BrokerHealthResult

## HealthProvider Protocol

::: rabbitkit.core.protocols.HealthProvider

## HealthWatcher

Opt-in push-style health notifications with debounced transitions. On
Kubernetes keep probes primary; this is for bare metal/VMs and direct
pager/webhook wiring.

::: rabbitkit.health.HealthWatcher

::: rabbitkit.health.AsyncHealthWatcher
