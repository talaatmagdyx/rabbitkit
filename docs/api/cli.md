# CLI

The `rabbitkit` CLI provides commands for running consumers, health checks,
topology inspection, DLQ management, and interactive debugging.

Install the CLI extra:

```bash
pip install rabbitkit[cli]
```

---

## run

Start a broker and block until SIGINT/SIGTERM.

```bash
rabbitkit run myapp.main:broker
rabbitkit run myapp.main:broker --worker-count 4
rabbitkit run myapp.main:broker --reload        # hot-reload on file changes
```

---

## health

Kubernetes-friendly health probes. Exit code 0 = healthy, 1 = unhealthy.

```bash
# Liveness: returns 0 even when reconnecting (process is still alive)
rabbitkit health liveness myapp.main:broker

# Readiness: returns 1 when disconnected or consumers not active
rabbitkit health readiness myapp.main:broker
```

---

## topology

Inspect and manage RabbitMQ topology declared by the broker.

### topology list

Print registered routes, queues, and exchanges.

```bash
rabbitkit topology list myapp.main:broker
rabbitkit topology list myapp.main:broker --format json
```

### topology validate

Compare declared topology against the live broker. Exit code 1 if mismatches are found.

```bash
rabbitkit topology validate myapp.main:broker
rabbitkit topology validate myapp.main:broker --url http://guest:guest@localhost:15672 --vhost /
```

### topology diff

Show what is declared in code but missing from RabbitMQ, and vice versa.

```bash
rabbitkit topology diff myapp.main:broker
rabbitkit topology diff myapp.main:broker --format json
```

Output symbols:
- `+` — declared in code, missing from RabbitMQ
- `~` — in RabbitMQ, not declared in code
- `!` — property mismatch (e.g. `durable` differs)

### topology apply

Declare all registered queues and exchanges via AMQP. Safe to run repeatedly.

```bash
rabbitkit topology apply myapp.main:broker
rabbitkit topology apply myapp.main:broker --url amqp://guest:guest@localhost/
rabbitkit topology apply myapp.main:broker --dry-run   # preview without connecting
```

---

## dlq

Inspect and replay dead-letter queues.

### dlq inspect

View messages in a DLQ without consuming them.

```bash
rabbitkit dlq inspect orders.created.dlq
rabbitkit dlq inspect orders.created.dlq --full        # include full body
rabbitkit dlq inspect orders.created.dlq --limit 20
```

### dlq replay

Re-publish messages from a DLQ back to the original exchange.

```bash
rabbitkit dlq replay orders.created.dlq orders
rabbitkit dlq replay orders.created.dlq orders --limit 10
```

Messages are republished with the original routing key and headers, with `x-retry-count` reset to `0`.
Replay uses publisher confirms — if a message fails to publish, replay stops and reports the error.

---

## routes

Inspect registered handler routes.

```bash
# List all routes
rabbitkit routes list myapp.main:broker

# Describe a specific route
rabbitkit routes describe myapp.main:broker orders.created
```

---

## shell

Open an interactive Python shell with the broker pre-loaded (requires IPython).

```bash
rabbitkit shell myapp.main:broker
```

---

::: rabbitkit.cli
