# Kubernetes Deployment Guide

## Health Probes

### Liveness

Liveness should NOT fail on a temporary RabbitMQ disconnect. A broker that is reconnecting is still alive; restarting the pod would only make things worse.

```yaml
livenessProbe:
  exec:
    command: ["rabbitkit", "health", "liveness", "myapp.main:broker"]
  initialDelaySeconds: 10
  periodSeconds: 30
  failureThreshold: 3
```

`liveness` returns exit code 0 even when the broker is reconnecting, as long as the process is still healthy.

### Readiness

Readiness SHOULD fail when the broker is disconnected or consumers are not active. Kubernetes will stop sending traffic to the pod until it reconnects.

```yaml
readinessProbe:
  exec:
    command: ["rabbitkit", "health", "readiness", "myapp.main:broker"]
  initialDelaySeconds: 5
  periodSeconds: 10
  failureThreshold: 2
```

## Graceful Shutdown

### terminationGracePeriodSeconds

Set `terminationGracePeriodSeconds` to be greater than your broker's `graceful_timeout`. This gives the broker time to drain in-flight messages before the pod is killed.

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
          command: ["rabbitkit", "run", "myapp.main:broker"]
          env:
            - name: RABBITMQ_URL
              valueFrom:
                secretKeyRef:
                  name: rabbitmq-credentials
                  key: url
          livenessProbe:
            exec:
              command: ["rabbitkit", "health", "liveness", "myapp.main:broker"]
            initialDelaySeconds: 10
            periodSeconds: 30
            failureThreshold: 3
          readinessProbe:
            exec:
              command: ["rabbitkit", "health", "readiness", "myapp.main:broker"]
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
- `prefetch_count` is set appropriately for your workload — too high can cause uneven distribution during scale-down.
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
