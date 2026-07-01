# RabbitKit vs Other Libraries

This page describes how RabbitKit relates to other Python messaging libraries. The goal is to help you choose the right tool for your situation, not to argue for a particular choice.

---

## When to Use What

| Use Case | Recommended |
|---|---|
| Multi-broker framework (RabbitMQ, Kafka, NATS, Redis) | FastStream |
| Low-level async RabbitMQ client | aio-pika |
| Low-level sync RabbitMQ client | pika |
| Distributed task queue (beat scheduler, workers, results) | Celery |
| RabbitMQ production toolkit with safety guarantees | RabbitKit |

---

## How RabbitKit Compares

### FastStream

FastStream is a well-designed multi-broker framework with a clean API and strong async-first design. It supports RabbitMQ, Kafka, NATS, Redis, and others behind a unified interface.

RabbitKit is narrower: it only targets RabbitMQ. In exchange for that constraint it goes deeper into RabbitMQ-specific production concerns — retry topology, publisher confirms, DLQ management, and operational tooling — that a multi-broker abstraction cannot expose without becoming broker-specific anyway.

If you need to switch brokers or work across multiple brokers in the same codebase, FastStream is the better fit. If you are committed to RabbitMQ and need the full production feature set, RabbitKit is designed for that.

### aio-pika

aio-pika is the standard async Python client for RabbitMQ. It is a thin, well-maintained wrapper over the AMQP protocol. RabbitKit uses aio-pika as its async transport layer.

aio-pika gives you full control over every AMQP primitive: exchanges, queues, bindings, channels, consumers, publisher confirms. That control comes with corresponding complexity — retry topology, dead-letter routing, connection recovery, and ack orchestration are all your responsibility.

RabbitKit builds the production-safety layer on top of aio-pika so you do not have to reimplement it per project.

### pika

pika is the standard sync Python client for RabbitMQ. RabbitKit uses pika as its sync transport layer, providing the same retry, DLQ, publisher confirm, and ack policy guarantees for sync codebases that it provides for async ones.

### Celery

Celery is a mature distributed task queue. It supports RabbitMQ as a broker and provides task scheduling, workers, result backends, and a monitoring UI (Flower).

Celery is a good fit for background job processing where the unit of work is a function call. It is a larger operational dependency and less suitable for event-driven architectures where the message schema, routing, and exchange topology are central to the design.

RabbitKit does not provide a task scheduler or a result backend in the same sense as Celery. It is oriented toward service-to-service messaging where handlers process domain events with full AMQP semantics.

---

## What RabbitKit Provides That Others Do Not

The following features are available in RabbitKit out of the box and are not provided — or require significant custom implementation — in the alternatives listed above:

- **Safe retry and DLQ**: Retry topology is declared automatically. Publisher confirms are used to ensure the retry message is durably accepted before the original is acked. Max-retries enforcement routes exhausted messages to a DLQ without data loss.
- **Publisher confirms on all paths**: Every publish that matters (retry, DLQ, RPC response) uses confirms. There is no fire-and-forget on safety-critical paths.
- **Explicit ack policies**: `AckPolicy.AUTO`, `AckPolicy.MANUAL`, `AckPolicy.NACK_ON_ERROR`, and `AckPolicy.REJECT_ON_ERROR` are first-class configuration choices, not something you wire up manually.
- **Topology validation at startup**: Exchange and queue declarations are validated before the broker starts consuming. Misconfiguration fails fast rather than at the first message.
- **TestBroker**: An in-memory broker implementation that routes messages through the same handler pipeline without a RabbitMQ instance. Usable in unit tests without Docker or mocking.
- **Kubernetes lifecycle**: Built-in liveness and readiness probes, graceful shutdown with drain period, and a CLI for health checking from probe commands.
- **CLI operations**: `rabbitkit dlq inspect`, `rabbitkit dlq replay`, `rabbitkit health`, `rabbitkit topology` — operational tooling that works against a live broker.
