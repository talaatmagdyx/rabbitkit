# rabbitkit Examples

Self-contained examples covering every feature of rabbitkit.

## Prerequisites

```bash
# Start RabbitMQ (Docker)
docker run -d --name rabbit \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3.13-management

# Install rabbitkit with all extras
pip install "rabbitkit[all]"
```

Management UI: http://localhost:15672 (guest / guest)

## Running an example

```bash
# From the project root
python examples/quickstart/01_sync_broker.py
```

## Index

### Quickstart
| File | Description |
|------|-------------|
| `quickstart/01_sync_broker.py` | SyncBroker — blocking consumer loop |
| `quickstart/02_async_broker.py` | AsyncBroker — asyncio consumer loop |

### Routing
| File | Description |
|------|-------------|
| `routing/01_basic_routing.py` | Queue, exchange, routing key basics |
| `routing/02_modular_router.py` | RabbitRouter with prefix and shared exchange |
| `routing/03_exchange_types.py` | Direct, fanout, topic, headers exchanges |
| `routing/04_topic_wildcards.py` | `*` and `#` wildcard routing |
| `routing/05_subscriber_filtering.py` | `filter_fn=` — reject before deserialization |

### Message Handling
| File | Description |
|------|-------------|
| `message_handling/01_ack_policies.py` | AUTO, MANUAL, ACK_FIRST, NACK_ON_ERROR |
| `message_handling/02_rabbit_message.py` | Accessing headers, properties, body |
| `message_handling/03_pydantic_validation.py` | Auto-validate body with Pydantic |
| `message_handling/04_exception_settlement.py` | AckMessage / NackMessage / RejectMessage |

### Middleware
| File | Description |
|------|-------------|
| `middleware/01_retry.py` | Retry with delay queues and DLQ |
| `middleware/02_compression.py` | gzip / zstd compression |
| `middleware/03_deduplication.py` | Redis-based idempotent processing |
| `middleware/04_circuit_breaker.py` | Fail-fast on cascading failures |
| `middleware/05_rate_limit.py` | Token-bucket rate limiting |
| `middleware/06_signing.py` | HMAC message signing and verification |
| `middleware/07_timeout.py` | Hard timeout per handler |
| `middleware/08_tracing.py` | OpenTelemetry tracing |
| `middleware/09_custom_middleware.py` | Write your own middleware |

### Dependency Injection
| File | Description |
|------|-------------|
| `dependency_injection/01_depends.py` | `Depends()` factory injection |
| `dependency_injection/02_generator_deps.py` | `yield`-based deps with teardown |
| `dependency_injection/03_header_path_context.py` | `Header()`, `Path()`, `Context()` |

### Serialization
| File | Description |
|------|-------------|
| `serialization/01_json.py` | Built-in JSON serializer |
| `serialization/02_pydantic.py` | Pydantic model serialization |
| `serialization/03_msgspec.py` | msgspec high-performance serialization |
| `serialization/04_pipeline.py` | Two-stage SerializationPipeline |

### RPC
| File | Description |
|------|-------------|
| `rpc/01_sync_rpc.py` | RPCClient — sync request/response |
| `rpc/02_async_rpc.py` | AsyncRPCClient — async request/response |
| `rpc/03_broker_request.py` | `broker.request()` shorthand |

### High-Load
| File | Description |
|------|-------------|
| `highload/01_worker_pools.py` | SyncWorkerPool + AsyncWorkerPool |
| `highload/02_batch_publisher.py` | Buffered batch publishing |
| `highload/03_batch_acker.py` | Batched multi-ack |
| `highload/04_backpressure.py` | FlowController — publish-side pressure |
| `highload/05_ten_queues_high_volume.py` | One broker, 10 queues, 3,000 messages — fan-out at volume |

### Configuration
| File | Description |
|------|-------------|
| `config/01_rabbit_config.py` | Full RabbitConfig composition |
| `config/02_env_config.py` | RABBITMQ_* env vars via pydantic-settings |
| `config/03_structured_logging.py` | structlog — dev console and JSON modes |

### Advanced
| File | Description |
|------|-------------|
| `advanced/01_dlq_inspector.py` | Peek, replay, purge dead-letter queues |
| `advanced/02_stream_queues.py` | RabbitMQ stream queues |
| `advanced/03_distributed_locking.py` | RedisLock + LockMiddleware |
| `advanced/04_result_backends.py` | Fire-and-retrieve with RedisResultBackend |
| `advanced/05_asyncapi_docs.py` | Generate AsyncAPI 2.6.0 spec |
| `advanced/06_management_api.py` | RabbitManagementClient HTTP API |
| `advanced/07_monitoring_dashboard.py` | Starlette monitoring dashboard |

### Integrations
| File | Description |
|------|-------------|
| `integrations/01_fastapi.py` | rabbitkit_lifespan + FastAPI |
| `integrations/02_app_lifecycle.py` | RabbitApp startup/shutdown hooks |
| `integrations/03_testing.py` | TestBroker + TestApp patterns |
