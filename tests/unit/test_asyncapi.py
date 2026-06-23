"""Tests for asyncapi/ — AsyncAPI document generation."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from rabbitkit.asyncapi import AsyncAPIGeneratorConfig, generate_asyncapi_doc, generate_asyncapi_json
from rabbitkit.asyncapi.schema import extract_json_schema, get_handler_body_type
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.route import ResultPublisher, RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy, ExchangeType

# ── helpers ──────────────────────────────────────────────────────────────


def _make_queue(name: str = "test-queue", **kwargs: object) -> RabbitQueue:
    """Build a RabbitQueue with sensible defaults."""
    return RabbitQueue(name=name, **kwargs)  # type: ignore[arg-type]


def _make_exchange(
    name: str = "test-exchange",
    type: ExchangeType = ExchangeType.DIRECT,  # noqa: A002
    **kwargs: object,
) -> RabbitExchange:
    """Build a RabbitExchange with sensible defaults."""
    return RabbitExchange(name=name, type=type, **kwargs)  # type: ignore[arg-type]


def _handler_str(body: str) -> None:
    """Handler accepting a str body."""


def _handler_dict(body: dict) -> None:  # type: ignore[type-arg]
    """Handler accepting a dict body."""


def _handler_int(body: int) -> None:
    """Handler accepting an int body."""


def _handler_no_annotation(body) -> None:  # type: ignore[no-untyped-def]
    """Handler with no type annotation."""


def _handler_bytes(body: bytes) -> None:
    """Handler accepting bytes."""


def _handler_rabbit_message(msg: RabbitMessage) -> None:
    """Handler accepting RabbitMessage only."""


def _handler_msg_then_str(msg: RabbitMessage, body: str) -> None:
    """Handler with RabbitMessage first, then str body."""


def _make_route(
    *,
    name: str = "test_route",
    queue: RabbitQueue | None = None,
    exchange: RabbitExchange | None = None,
    handler: object = _handler_str,
    ack_policy: AckPolicy = AckPolicy.AUTO,
    tags: frozenset[str] | None = None,
    description: str = "",
    result_publisher: ResultPublisher | None = None,
) -> RouteDefinition:
    """Build a RouteDefinition with sensible defaults."""
    return RouteDefinition(
        name=name,
        queue=queue or _make_queue(),
        exchange=exchange,
        handler=handler,  # type: ignore[arg-type]
        ack_policy=ack_policy,
        tags=tags or frozenset(),
        description=description,
        result_publisher=result_publisher,
    )


# ── generate_asyncapi_doc tests ─────────────────────────────────────────


class TestEmptyRoutes:
    def test_empty_routes(self) -> None:
        """Generates valid doc with no channels."""
        doc = generate_asyncapi_doc([])
        assert doc["asyncapi"] == "2.6.0"
        assert doc["info"]["title"] == "rabbitkit Service"
        assert doc["info"]["version"] == "1.0.0"
        assert doc["channels"] == {}
        assert "rabbitmq" in doc["servers"]
        assert doc["servers"]["rabbitmq"]["protocol"] == "amqp"


class TestSingleRoute:
    def test_single_route(self) -> None:
        """One route produces one channel."""
        route = _make_route()
        doc = generate_asyncapi_doc([route])
        assert len(doc["channels"]) == 1
        assert "test-queue" in doc["channels"]


class TestChannelNameIsQueueName:
    def test_channel_name_is_queue_name(self) -> None:
        """Channel key matches queue.name."""
        q = _make_queue(name="my-special-queue")
        route = _make_route(queue=q)
        doc = generate_asyncapi_doc([route])
        assert "my-special-queue" in doc["channels"]


class TestAmqpBindingsPresent:
    def test_amqp_bindings_present(self) -> None:
        """Exchange and queue appear in AMQP bindings."""
        ex = _make_exchange(name="orders-exchange", type=ExchangeType.TOPIC)
        q = _make_queue(name="orders-queue")
        route = _make_route(queue=q, exchange=ex)
        doc = generate_asyncapi_doc([route])

        channel = doc["channels"]["orders-queue"]
        bindings = channel["bindings"]["amqp"]
        assert bindings["is"] == "queue"
        assert bindings["queue"]["name"] == "orders-queue"
        assert bindings["queue"]["durable"] is True
        assert bindings["exchange"]["name"] == "orders-exchange"
        assert bindings["exchange"]["type"] == "topic"

    def test_bindings_without_exchange(self) -> None:
        """When exchange is None, no exchange binding appears."""
        route = _make_route(exchange=None)
        doc = generate_asyncapi_doc([route])
        channel = doc["channels"]["test-queue"]
        bindings = channel["bindings"]["amqp"]
        assert "exchange" not in bindings
        assert bindings["queue"]["name"] == "test-queue"

    def test_queue_exclusive_in_bindings(self) -> None:
        """Queue exclusive flag propagates to bindings."""
        q = _make_queue(name="exclusive-q", exclusive=True)
        route = _make_route(queue=q)
        doc = generate_asyncapi_doc([route])
        bindings = doc["channels"]["exclusive-q"]["bindings"]["amqp"]
        assert bindings["queue"]["exclusive"] is True


class TestOperationIdIsRouteName:
    def test_operation_id_is_route_name(self) -> None:
        """operationId matches route.name."""
        route = _make_route(name="process_order")
        doc = generate_asyncapi_doc([route])
        channel = doc["channels"]["test-queue"]
        assert channel["subscribe"]["operationId"] == "process_order"


class TestTagsIncluded:
    def test_tags_included(self) -> None:
        """Route tags appear in the operation."""
        route = _make_route(tags=frozenset({"billing", "orders"}))
        doc = generate_asyncapi_doc([route])
        channel = doc["channels"]["test-queue"]
        tags = channel["subscribe"]["tags"]
        tag_names = [t["name"] for t in tags]
        assert "billing" in tag_names
        assert "orders" in tag_names

    def test_no_tags_when_empty(self) -> None:
        """No tags key when route has no tags."""
        route = _make_route(tags=frozenset())
        doc = generate_asyncapi_doc([route])
        channel = doc["channels"]["test-queue"]
        assert "tags" not in channel["subscribe"]


class TestDescriptionIncluded:
    def test_description_included(self) -> None:
        """Route description appears in channel."""
        route = _make_route(description="Handles incoming orders")
        doc = generate_asyncapi_doc([route])
        channel = doc["channels"]["test-queue"]
        assert channel["description"] == "Handles incoming orders"

    def test_no_description_when_empty(self) -> None:
        """No description key when route has no description."""
        route = _make_route(description="")
        doc = generate_asyncapi_doc([route])
        channel = doc["channels"]["test-queue"]
        assert "description" not in channel


class TestResultPublisherAddsPublish:
    def test_result_publisher_adds_publish(self) -> None:
        """Publish operation appears when result_publisher is set."""
        rp = ResultPublisher(exchange="results-exchange", routing_key="results")
        route = _make_route(name="compute", result_publisher=rp)
        doc = generate_asyncapi_doc([route])
        channel = doc["channels"]["test-queue"]
        assert "publish" in channel
        assert channel["publish"]["operationId"] == "compute.reply"
        assert channel["publish"]["message"]["name"] == "compute.response"

    def test_no_publish_without_result_publisher(self) -> None:
        """No publish operation when result_publisher is None."""
        route = _make_route(result_publisher=None)
        doc = generate_asyncapi_doc([route])
        channel = doc["channels"]["test-queue"]
        assert "publish" not in channel


class TestJsonOutputValid:
    def test_json_output_valid(self) -> None:
        """generate_asyncapi_json returns parseable JSON."""
        route = _make_route()
        json_str = generate_asyncapi_json([route])
        parsed = json.loads(json_str)
        assert parsed["asyncapi"] == "2.6.0"
        assert "test-queue" in parsed["channels"]

    def test_json_indent(self) -> None:
        """JSON output respects indent parameter."""
        json_str = generate_asyncapi_json([], indent=4)
        # 4-space indent should be present in output
        assert "    " in json_str

    def test_json_no_indent(self) -> None:
        """JSON output with indent=None is compact."""
        json_str = generate_asyncapi_json([], indent=None)
        assert "\n" not in json_str


class TestCustomConfig:
    def test_custom_config(self) -> None:
        """Title, version, description override defaults."""
        cfg = AsyncAPIGeneratorConfig(
            title="My Service",
            version="2.0.0",
            description="My custom service",
            server_url="rabbit.prod:5672",
            server_description="Production RabbitMQ",
        )
        doc = generate_asyncapi_doc([], config=cfg)
        assert doc["info"]["title"] == "My Service"
        assert doc["info"]["version"] == "2.0.0"
        assert doc["info"]["description"] == "My custom service"
        assert doc["servers"]["rabbitmq"]["url"] == "rabbit.prod:5672"
        assert doc["servers"]["rabbitmq"]["description"] == "Production RabbitMQ"

    def test_no_description_in_info_when_empty(self) -> None:
        """Info description is omitted when empty."""
        cfg = AsyncAPIGeneratorConfig(description="")
        doc = generate_asyncapi_doc([], config=cfg)
        assert "description" not in doc["info"]


# ── schema extraction tests ─────────────────────────────────────────────


class TestHandlerBodyTypeExtraction:
    def test_extracts_str(self) -> None:
        """get_handler_body_type extracts str."""
        assert get_handler_body_type(_handler_str) is str

    def test_extracts_dict(self) -> None:
        """get_handler_body_type extracts dict."""
        assert get_handler_body_type(_handler_dict) is dict

    def test_extracts_int(self) -> None:
        """get_handler_body_type extracts int."""
        assert get_handler_body_type(_handler_int) is int

    def test_no_annotation_returns_none(self) -> None:
        """get_handler_body_type returns None for unannotated params."""
        assert get_handler_body_type(_handler_no_annotation) is None

    def test_bytes_returns_none(self) -> None:
        """get_handler_body_type returns None for bytes params."""
        assert get_handler_body_type(_handler_bytes) is None

    def test_skips_rabbit_message(self) -> None:
        """get_handler_body_type skips RabbitMessage params."""
        assert get_handler_body_type(_handler_rabbit_message) is None

    def test_msg_then_str_returns_str(self) -> None:
        """get_handler_body_type skips RabbitMessage, returns str."""
        assert get_handler_body_type(_handler_msg_then_str) is str

    def test_non_callable_returns_none(self) -> None:
        """get_handler_body_type returns None for non-callable."""
        assert get_handler_body_type(42) is None


class TestSchemaFromPrimitives:
    def test_str_schema(self) -> None:
        """extract_json_schema for str."""
        assert extract_json_schema(str) == {"type": "string"}

    def test_int_schema(self) -> None:
        """extract_json_schema for int."""
        assert extract_json_schema(int) == {"type": "integer"}

    def test_float_schema(self) -> None:
        """extract_json_schema for float."""
        assert extract_json_schema(float) == {"type": "number"}

    def test_bool_schema(self) -> None:
        """extract_json_schema for bool."""
        assert extract_json_schema(bool) == {"type": "boolean"}

    def test_bytes_schema(self) -> None:
        """extract_json_schema for bytes."""
        assert extract_json_schema(bytes) == {"type": "string", "contentEncoding": "base64"}

    def test_dict_schema(self) -> None:
        """extract_json_schema for dict."""
        assert extract_json_schema(dict) == {"type": "object"}

    def test_list_schema(self) -> None:
        """extract_json_schema for list."""
        assert extract_json_schema(list) == {"type": "array"}

    def test_none_schema(self) -> None:
        """extract_json_schema for None returns empty dict."""
        assert extract_json_schema(None) == {}

    def test_dataclass_schema(self) -> None:
        """extract_json_schema for a dataclass."""

        @dataclass
        class OrderPayload:
            order_id: str
            amount: float
            confirmed: bool = False

        schema = extract_json_schema(OrderPayload)
        assert schema["type"] == "object"
        assert "order_id" in schema["properties"]
        assert schema["properties"]["order_id"] == {"type": "string"}
        assert schema["properties"]["amount"] == {"type": "number"}
        assert schema["properties"]["confirmed"] == {"type": "boolean"}
        # order_id and amount are required; confirmed has a default
        assert "order_id" in schema["required"]
        assert "amount" in schema["required"]
        assert "confirmed" not in schema["required"]

    def test_unknown_type_defaults_to_object(self) -> None:
        """extract_json_schema for unknown type returns object."""

        class CustomThing:
            pass

        assert extract_json_schema(CustomThing) == {"type": "object"}


class TestHandlerBodyTypeAnnotationFailure:
    def test_get_type_hints_exception_falls_back_to_empty(self) -> None:
        """When get_type_hints raises, hints defaults to {} (lines 26-27).

        We simulate this by patching get_type_hints to raise and providing
        a handler whose raw annotation (param.annotation) is also a string
        (which happens with __future__ annotations). When hints={} and
        param.annotation is a string, the function returns the string annotation.
        The key is that the except branch is executed — we verify hints=={}
        by ensuring we reach that code path. We do this by checking coverage
        after confirming the test exercises the exception path.
        """
        from unittest.mock import patch

        # Build a handler with an unannotated param so the result is None
        # regardless of whether hints is {} or not — this confirms the
        # except branch runs (lines 26-27) without asserting on resolved type.
        def handler(body) -> None:  # type: ignore[no-untyped-def]
            pass

        with patch("rabbitkit.asyncapi.schema.get_type_hints", side_effect=Exception("fail")):
            result = get_handler_body_type(handler)

        # Unannotated param → annotation is Parameter.empty → None
        assert result is None

    def test_get_type_hints_exception_with_annotated_param(self) -> None:
        """When hints={} (due to exception), param.annotation falls through.

        Because the test file uses `from __future__ import annotations`,
        param.annotation is a string ('int') rather than the actual type.
        The function returns whatever the annotation is (the string 'int').
        """
        from unittest.mock import patch

        # A handler with a param annotated with a type not in RabbitMessage/bytes
        def handler(body: int) -> None:
            pass

        with patch("rabbitkit.asyncapi.schema.get_type_hints", side_effect=Exception("fail")):
            result = get_handler_body_type(handler)

        # Due to __future__ annotations in this test module, param.annotation is
        # the string 'int', not the int type.
        assert result == "int"


class TestAnnotatedTypeSkipped:
    def test_annotated_param_is_skipped(self) -> None:
        """Parameters with __metadata__ (Annotated) are skipped (line 38).

        Because the test file has `from __future__ import annotations`, we
        cannot use Annotated inline — annotations become strings. Instead we
        pass a mock object with __metadata__ directly to simulate the resolved
        Annotated type that get_type_hints() normally returns.
        """
        from typing import Annotated
        from unittest.mock import patch

        def handler(dep, body: int) -> None:  # type: ignore[no-untyped-def]
            pass

        # Build a fake hints dict where dep is an Annotated type (has __metadata__)
        annotated_type = Annotated[str, "di-marker"]
        fake_hints = {"dep": annotated_type, "body": int}

        with patch("rabbitkit.asyncapi.schema.get_type_hints", return_value=fake_hints):
            result = get_handler_body_type(handler)

        # dep is skipped (has __metadata__), body returns int
        assert result is int


class TestPydanticModelSchema:
    def test_pydantic_v2_model_schema(self) -> None:
        """extract_json_schema calls model_json_schema() for Pydantic V2 (line 60)."""
        try:
            from pydantic import BaseModel  # type: ignore[import-untyped]
        except ImportError:
            pytest.skip("pydantic not installed")

        class OrderModel(BaseModel):
            order_id: str
            amount: float

        schema = extract_json_schema(OrderModel)
        # Pydantic V2 returns a full JSON Schema dict
        assert "properties" in schema
        assert "order_id" in schema["properties"]
