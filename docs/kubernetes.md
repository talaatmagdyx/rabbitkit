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
from rabbitkit.aio import AsyncBroker

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
