# Ordering Guarantees

## What RabbitKit Guarantees

RabbitKit follows RabbitMQ's native ordering semantics. Within a single queue with a single consumer and no concurrent processing, messages are delivered in the order they were published to that queue. RabbitKit does not add any additional ordering layer beyond what RabbitMQ provides.

**There is no strict global ordering guarantee when concurrency is involved.** This is a fundamental property of distributed message queues, not a limitation of RabbitKit specifically.

---

## What Breaks Ordering

The following configuration choices relax ordering, intentionally or otherwise:

### worker_count > 1

When `broker.start(worker_config=WorkerConfig(worker_count=4))` is used, multiple workers consume from the same queue concurrently. Messages that arrive in order may be processed out of order if one worker is slower than another.

### prefetch_count > 1

With `prefetch_count=10`, the broker delivers up to 10 unacknowledged messages to the consumer at once. The handler can process these in any order depending on async task scheduling.

### Multiple consumers on the same queue

Each consumer receives a different subset of messages via round-robin. There is no coordination between consumers, so message N+1 may be processed before message N if it lands on a faster consumer.

### Retry delays

When a message fails and enters a retry queue with a TTL delay, newer messages continue to be processed by the consumer. After the delay expires, the retried message is redelivered and may be processed after messages that were published after it.

### Batch acks

When using `AckPolicy.MANUAL` and batching acks (acknowledging multiple messages at once), the handler may complete processing of message N+1 before N, and both are acked together. From the broker's perspective ordering is preserved, but side effects may have occurred out of order.

---

## How to Achieve Strict Ordering

If your use case requires that messages are processed in the exact order they were published, apply all of the following constraints:

| Setting | Value | Reason |
|---|---|---|
| `worker_count` | `1` | Single worker; no concurrent processing |
| `prefetch_count` | `1` | Broker delivers one message at a time |
| Consumers per queue | `1` | No distribution across multiple processes |
| `RetryConfig` | Not used | Retry delays interleave old and new messages |
| `AckPolicy` | `AUTO` or `MANUAL` with immediate ack | No deferred batch acks |

Example:

```python
from rabbitkit import AsyncBroker, RabbitConfig
from rabbitkit.core.config import WorkerConfig

config = RabbitConfig(url="amqp://guest:guest@localhost/")
broker = AsyncBroker(config)

@broker.subscriber(
    queue="orders.created",
    prefetch_count=1,
)
async def handle_order(order_id: str) -> None:
    await process_order(order_id)


async def main() -> None:
    async with broker:
        await broker.start(
            worker_config=WorkerConfig(worker_count=1),
        )
```

This configuration processes exactly one message at a time, in delivery order.

---

## Ordering by Partition Key

For high-throughput workloads where strict per-entity ordering matters but global ordering is not required, the standard pattern is to partition by a key (for example, `order_id` or `customer_id`) and route each partition to a dedicated queue. Each partition queue uses a single consumer, giving strict ordering within the partition and full parallelism across partitions.

RabbitKit supports this through topic exchange routing keys:

```
orders.exchange (topic)
  orders.customer.1  →  queue.shard.1  (worker_count=1, prefetch=1)
  orders.customer.2  →  queue.shard.2  (worker_count=1, prefetch=1)
  orders.customer.*  →  queue.shard.*
```

Partitioning strategy and shard assignment are application responsibilities. RabbitKit handles the routing and consumption.

---

## Summary

| Scenario | Ordering |
|---|---|
| Single consumer, prefetch=1, no retry | Strict FIFO per queue |
| Single consumer, prefetch>1 | Delivery order only; processing may differ |
| Multiple workers | Not guaranteed |
| Multiple consumers | Not guaranteed |
| Retry enabled | Not guaranteed (delayed messages interleave) |
| Partition-per-key, one consumer per partition | Strict FIFO within each partition |
