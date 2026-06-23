"""AsyncAPI 2.6.0 document generator from rabbitkit routes.

Generates an **AsyncAPI** specification from a broker's route definitions.
The spec describes your messaging API contract — queues, exchanges, message
schemas, and operations — in a machine-readable format compatible with the
AsyncAPI toolchain (Studio, code generators, documentation renderers).

What is generated
-----------------
For every ``RouteDefinition`` in ``broker.routes``:

* A **channel** named after the queue.
* A **subscribe operation** with the handler's body type as the message
  payload schema (JSON Schema).  Pydantic models use ``model_json_schema()``;
  stdlib dataclasses use field introspection; primitives map to JSON primitives.
* **AMQP bindings** — exchange type/name, queue durable/exclusive flags,
  routing key.
* **Tags** from ``route.tags`` and **description** from ``route.description``.
* A **publish operation** when ``route.result_publisher`` is configured
  (documents the reply channel).

Quick start
-----------
    from rabbitkit.asyncapi.generator import (
        generate_asyncapi_doc,
        generate_asyncapi_json,
        AsyncAPIGeneratorConfig,
    )

    # broker.routes populated after registering @broker.subscriber decorators
    doc = generate_asyncapi_doc(
        broker.routes,
        config=AsyncAPIGeneratorConfig(
            title="Order Service",
            version="2.1.0",
            description="Processes incoming orders and emits confirmations.",
            server_url="rabbitmq.prod.internal:5672",
        ),
    )

    # Pretty-print JSON spec
    json_str = generate_asyncapi_json(broker.routes, indent=2)
    print(json_str)

CLI integration
---------------
    rabbitkit docs generate myapp.main:broker > asyncapi.json
    rabbitkit docs serve   myapp.main:broker   # opens browser preview

Saving to file::

    import json
    with open("asyncapi.json", "w") as f:
        json.dump(generate_asyncapi_doc(broker.routes), f, indent=2)

Example output (condensed)::

    {
      "asyncapi": "2.6.0",
      "info": { "title": "Order Service", "version": "2.1.0" },
      "servers": {
        "rabbitmq": { "url": "localhost:5672", "protocol": "amqp" }
      },
      "channels": {
        "orders": {
          "subscribe": {
            "operationId": "handle_order",
            "message": {
              "name": "handle_order",
              "payload": {
                "type": "object",
                "properties": {
                  "id":   { "type": "integer" },
                  "item": { "type": "string"  }
                }
              }
            }
          },
          "bindings": {
            "amqp": {
              "is": "queue",
              "queue": { "name": "orders", "durable": true }
            }
          }
        }
      }
    }
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from rabbitkit.asyncapi.schema import extract_json_schema, get_handler_body_type
from rabbitkit.core.route import RouteDefinition


@dataclass
class AsyncAPIGeneratorConfig:
    """Configuration for AsyncAPI document generation."""

    title: str = "rabbitkit Service"
    version: str = "1.0.0"
    description: str = ""
    server_url: str = "localhost:5672"
    server_description: str = "RabbitMQ"


def generate_asyncapi_doc(
    routes: list[RouteDefinition],
    config: AsyncAPIGeneratorConfig | None = None,
) -> dict[str, Any]:
    """Generate an AsyncAPI 2.6.0 document from route definitions.

    Args:
        routes: List of RouteDefinition from broker.routes.
        config: Generator configuration.

    Returns:
        AsyncAPI 2.6.0 spec as a JSON-serializable dict.
    """
    if config is None:
        config = AsyncAPIGeneratorConfig()

    doc: dict[str, Any] = {
        "asyncapi": "2.6.0",
        "info": {
            "title": config.title,
            "version": config.version,
        },
        "servers": {
            "rabbitmq": {
                "url": config.server_url,
                "protocol": "amqp",
                "description": config.server_description,
            },
        },
        "channels": {},
    }

    if config.description:
        doc["info"]["description"] = config.description

    for route in routes:
        channel_name = route.queue.name
        channel: dict[str, Any] = {}

        if route.description:
            channel["description"] = route.description

        # Subscribe operation (consumer)
        operation: dict[str, Any] = {
            "operationId": route.name,
        }

        # Message schema from handler type hints
        body_type = get_handler_body_type(route.handler)
        payload = extract_json_schema(body_type)
        message: dict[str, Any] = {
            "name": route.name,
        }
        if payload:
            message["payload"] = payload
        operation["message"] = message

        # Tags
        if route.tags:
            operation["tags"] = [{"name": t} for t in sorted(route.tags)]

        channel["subscribe"] = operation

        # AMQP bindings
        bindings: dict[str, Any] = {"amqp": {"is": "queue"}}
        queue_binding: dict[str, Any] = {
            "name": route.queue.name,
            "durable": route.queue.durable,
        }
        if hasattr(route.queue, "exclusive"):
            queue_binding["exclusive"] = route.queue.exclusive
        bindings["amqp"]["queue"] = queue_binding

        if route.exchange is not None:
            exchange_binding: dict[str, Any] = {
                "name": route.exchange.name,
                "type": (
                    route.exchange.type.value
                    if hasattr(route.exchange.type, "value")
                    else str(route.exchange.type)
                ),
            }
            if hasattr(route.exchange, "durable"):
                exchange_binding["durable"] = route.exchange.durable
            bindings["amqp"]["exchange"] = exchange_binding

        channel["bindings"] = bindings

        # Publish operation (if result_publisher is set)
        if route.result_publisher is not None:
            publish_op: dict[str, Any] = {
                "operationId": f"{route.name}.reply",
                "message": {"name": f"{route.name}.response"},
            }
            channel["publish"] = publish_op

        doc["channels"][channel_name] = channel

    return doc


def generate_asyncapi_json(
    routes: list[RouteDefinition],
    config: AsyncAPIGeneratorConfig | None = None,
    indent: int | None = 2,
) -> str:
    """Generate AsyncAPI spec as a JSON string."""
    doc = generate_asyncapi_doc(routes, config)
    return json.dumps(doc, indent=indent)
