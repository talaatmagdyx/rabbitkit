# Kubernetes Deployment Guide

## Health Probes

**Probes must be HTTP endpoints served by the worker process itself — an
`exec` probe cannot work here.** An exec probe spawns a *fresh* Python
process; importing your module there produces a brand-new, never-started
broker object with no relationship to the consumer actually running in the
container, so any CLI-based check would report "not started" forever and
CrashLoop the pod. The health functions themselves are safe to call from
any thread or event loop (see `docs/concurrency-model.md`), so a tiny HTTP
server inside the worker is the correct pattern:

```python
# Inside your worker process, next to the running broker.
from aiohttp import web  # or starlette/uvicorn — anything in-process

from rabbitkit import broker_liveness, broker_readiness

async def liveness(_request: web.Request) -> web.Response:
    alive: bool = broker_liveness(broker)
    return web.Response(status=200 if alive else 503, text="ok" if alive else "wedged")

async def readiness(_request: web.Request) -> web.Response:
    ready: bool = broker_readiness(broker)
    return web.Response(status=200 if ready else 503, text="ok" if ready else "not ready")

app = web.Application()
app.router.add_get("/healthz", liveness)
app.router.add_get("/readyz", readiness)
# serve on :8080 alongside the broker — full wiring in
# examples/kubernetes_worker/worker.py
```

### Liveness

Liveness should NOT fail on a temporary RabbitMQ disconnect. A broker that
is reconnecting is still alive; restarting the pod would only make things
worse. `broker_liveness()` is built exactly this way: it checks the process
is making forward progress (the heartbeat ticks during message handling,
idle I/O loops, *and* each reconnect-backoff iteration), and deliberately
ignores broker connectivity and `connection.blocked` alarms.

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30
  failureThreshold: 3
```

### Readiness

Readiness SHOULD fail when the broker is disconnected, the consumer channel
died, or the connection is blocked by a broker alarm. Kubernetes will stop
routing to the pod until it recovers — without restarting it.

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
  failureThreshold: 2
```

## Graceful Shutdown

### terminationGracePeriodSeconds

Set `terminationGracePeriodSeconds` to be greater than
`ConsumerConfig.graceful_timeout` **plus** your `preStop` sleep. The drain
budget on `broker.stop()` is `graceful_timeout` — that is the knob to size
to your slowest handler (`WorkerConfig.stop_timeout` is only a fallback for
directly-driven worker pools and has no effect on the standard shutdown
path).

```yaml
spec:
  terminationGracePeriodSeconds: 60
```

### preStop Hook

Use a `preStop` sleep to let Kubernetes remove the pod from service endpoints before SIGTERM arrives. This prevents new messages from being routed to a pod that is about to shut down.

```yaml
lifecycle:
  preStop:
    exec:
      command: ["sleep", "10"]
```

## Full Manifest Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: orders-worker
  template:
    metadata:
      labels:
        app: orders-worker
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: worker
          image: myapp:latest
          command: ["python", "-m", "myapp.worker"]  # serves /healthz + /readyz on :8080
          env:
            - name: RABBITMQ_URL
              valueFrom:
                secretKeyRef:
                  name: rabbitmq-credentials
                  key: url
          ports:
            - name: health
              containerPort: 8080
          livenessProbe:
            httpGet:
              path: /healthz
              port: health
            initialDelaySeconds: 10
            periodSeconds: 30
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /readyz
              port: health
            initialDelaySeconds: 5
            periodSeconds: 10
            failureThreshold: 2
          lifecycle:
            preStop:
              exec:
                command: ["sleep", "10"]
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RABBITMQ_URL` | AMQP connection URL, e.g. `amqp://user:pass@host/vhost` |

## FastAPI with Lifespan

When running rabbitkit inside FastAPI, use `rabbitkit_lifespan` to tie the broker lifecycle to the ASGI lifespan:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from rabbitkit import rabbitkit_lifespan
from rabbitkit.async_ import AsyncBroker

broker = AsyncBroker(config)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with rabbitkit_lifespan(broker):
        yield

app = FastAPI(lifespan=lifespan)
```

When Kubernetes sends SIGTERM, FastAPI triggers the lifespan teardown, which calls `broker.stop()`. The broker drains in-flight messages and closes the connection cleanly before the process exits.

## HPA Scaling

When scaling horizontally, ensure that:

- Each pod consumes from the same queue (RabbitMQ distributes messages across consumers).
- `prefetch_count` is set appropriately for your workload — too high can cause uneven distribution during scale-down. Note the limit is **per queue** (each queue gets its own consumer channel), and `WorkerConfig.prefetch_per_worker` multiplies by `worker_count` per queue — a 4-worker consumer on 5 queues with `prefetch_per_worker=8` can hold up to `4 x 8 x 5 = 160` unacked messages in memory at once.
- The DLQ capacity is monitored so that message loss is detected before scaling events hide it.

**CPU-based HPA is the wrong signal for a queue consumer.** A consumer with
a slow downstream call (a DB, an external API) can sit near-idle on CPU
while its queue backlog grows unbounded — a plain `HorizontalPodAutoscaler`
targeting CPU utilization never reacts to that. Scale on queue depth
instead, via [KEDA](https://keda.sh)'s built-in `rabbitmq` scaler, which
polls the management API directly (no extra exporter needed):

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: worker-scaler
spec:
  scaleTargetRef:
    name: worker-deployment
  minReplicaCount: 1
  maxReplicaCount: 20
  triggers:
    - type: rabbitmq
      metadata:
        queueName: orders
        mode: QueueLength
        value: "50" # target: ~50 ready messages per replica
      authenticationRef:
        name: rabbitmq-trigger-auth
---
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: rabbitmq-trigger-auth
spec:
  secretTargetRef:
    - parameter: host
      name: rabbitmq-secret
      key: management-url # http://user:pass@rabbitmq:15672/vhost
```

If you're already polling queue depth in-process for metrics/dashboards
(`rabbitkit.queue_metrics.QueueMetricsPoller`, wrapping
`RabbitManagementClient`), the same management API credentials work for
KEDA's `host` parameter — no separate scaling-specific integration needed.

## Cluster Failover (`ConnectionConfig.nodes`)

Against a multi-node RabbitMQ cluster, list the other nodes so a dead primary
doesn't take the client down at startup:

```python
from rabbitkit import RabbitConfig, ConnectionConfig

config = RabbitConfig(
    connection=ConnectionConfig(
        host="rabbit-0", port=5672,
        nodes=("rabbit-1", "rabbit-2:5672"),  # "host" or "host:port"
    )
)
```

- **Sync (pika):** all nodes are tried natively via a `ConnectionParameters`
  list — full failover, including on reconnect.
- **Async (aio-pika):** endpoints are cycled on the *initial* connect; once
  `connect_robust` succeeds it pins to that node for reconnects (aio-pika has
  no multi-host reconnect). For per-reconnect failover on async, front the
  cluster with a load balancer or DNS round-robin and point `host` at the VIP.

For a queue-level HA guarantee independent of connection failover, use
**quorum queues** (`RabbitQueue(queue_type=QueueType.QUORUM)`) so messages are
replicated across nodes — see the retry docs for the `x-delivery-limit`
crash-loop backstop.

## Active/Standby Consumers (`single_active_consumer`)

For strictly-ordered processing where only ONE pod may consume at a time,
declare the queue with `RabbitQueue(single_active_consumer=True)` (classic
RabbitMQ 3.8+ and quorum queues alike) and run multiple replicas: every pod
subscribes, the broker delivers to exactly one, and fails over to a standby
automatically when the active consumer's channel dies — no leader-election
sidecar needed. Readiness will report ready on the standbys too (they ARE
connected and subscribed); that is correct — they're warm spares, not
broken pods.
