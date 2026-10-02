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
rabbitkit dlq inspect orders.created.dlq --limit 20
rabbitkit dlq inspect orders.created.dlq --management-url http://ops:secret@rabbit:15672
```

Each message is shown once: all fetched messages are held unacked until the
fetch ends, then requeued. On a quorum queue every requeue counts as a
delivery, so with `--management-url` (or `RABBITMQ_MANAGEMENT_URL`) the
command first checks the queue's delivery limit and refuses (exit 2) when
browsing could eventually drop messages. Without it, the command stops at
the first message carrying `x-delivery-count`. `--no-delivery-limit-check`
skips both. See [Quorum DLQs and delivery limits](../retry-and-dlq.md#quorum-dlqs-and-delivery-limits).

### dlq replay

Re-publish messages from a DLQ back to the original exchange.

```bash
rabbitkit dlq replay orders.created.dlq orders
rabbitkit dlq replay orders.created.dlq orders --limit 10
```

Messages are republished with their original routing key (`--routing-key`
overrides it) and headers verbatim; `--reset-retry-count` strips the retry
counter. Replay uses publisher confirms and `mandatory=True`: a message whose
republish fails stays on the DLQ, replay continues with the rest, and the
command exits 1. A message that would be published back into the DLQ itself
is skipped. `--dry-run` requeues what it shows, so it gets the same
delivery-limit checks as `inspect`.

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

## `rabbitkit topology migrate`

Classic→quorum queue migration: plan (default, never mutates), `--execute
--strategy drain-cutover|bridge`, `--dry-run`, `--resume`. See the
[quorum migration guide](../quorum-migration.md).
