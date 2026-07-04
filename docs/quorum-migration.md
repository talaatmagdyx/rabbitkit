# Migrating classic queues to quorum

The [production checklist](production/checklist.md) mandates quorum queues
(with `delivery_limit=`) for money/order flows — but RabbitMQ cannot change
a queue's `x-queue-type` in place, and re-declaring an existing classic
queue with quorum arguments fails startup with a 406. `rabbitkit topology
migrate` is the supported path across that gap.

Every strategy is *create new → move messages → cut over*, driven through
the management API with the `rabbitmq_shovel` plugin (a missing plugin is
detected up front with a clear error naming
`rabbitmq-plugins enable rabbitmq_shovel`).

## Plan first (never mutates)

```bash
rabbitkit topology migrate myapp.main:broker \
  --url http://user:pass@rabbit:15672 --amqp-url amqp://user:pass@rabbit/
```

The default mode compares each route's **declared** queue type against the
**live** one and, for every classic→quorum mismatch, prints an ordered
runbook and writes a JSON snapshot of the queue's bindings and arguments —
the rollback artifact every destructive step can be reversed from.

## Execute: `drain-cutover` (bounded downtime, keeps the queue name)

```bash
rabbitkit topology migrate myapp.main:broker --execute --strategy drain-cutover ...
```

Per queue, eleven checkpointed steps: verify **zero consumers** (refused
otherwise; `--force` exists and is dangerous), snapshot bindings, create a
temporary quorum queue, shovel old→tmp, wait empty, delete the old queue,
re-declare it with quorum arguments, recreate bindings from the snapshot,
shovel tmp→back, wait empty, delete tmp. Message counts are re-verified
before every destructive step; progress is checkpointed to
`.rabbitkit-migrate.json` after each step so a crashed run continues with
`--resume`. `--dry-run` prints every management call without issuing any.

## Execute: `bridge` (near-zero downtime, new name)

Creates `{queue}.q2` as quorum with duplicated bindings and **deletes
nothing** — move consumers to the new name via a config change, let the
shovel drain stragglers, retire the old queue yourself.

| | drain-cutover | bridge |
|---|---|---|
| Downtime | bounded (consumers stopped for the drain) | near zero |
| Queue name | preserved | changes (`.q2`) |
| Deletes the old queue | yes (after verified-empty) | no |

Useful flags: `--queue` (one queue), `--vhost`, `--timeout` (drain-wait
bound, default 300s), `--state-file`, `--snapshot-file`.
