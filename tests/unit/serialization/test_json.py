"""Tests for serialization/json.py — JSONSerializer."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from rabbitkit.serialization.base import Serializer
from rabbitkit.serialization.json import JSONSerializer

# ── protocol compliance ─────────────────────────────────────────────────


class TestProtocolCompliance:
    def test_satisfies_serializer_protocol(self) -> None:
        s = JSONSerializer()
        assert isinstance(s, Serializer)

    def test_content_type(self) -> None:
        s = JSONSerializer()
        assert s.content_type == "application/json"

    def test_protocol_is_generic(self) -> None:
        """R-Generic-Serializer: Serializer is now generic (Parameterized)."""
        # A generic Protocol exposes its type parameters via __parameters__.
        assert hasattr(Serializer, "__parameters__")
        params = Serializer.__parameters__
        assert len(params) == 1

    def test_isinstance_works_with_generic_protocol(self) -> None:
        """runtime_checkable isinstance still works after making the protocol generic."""
        s = JSONSerializer()
        assert isinstance(s, Serializer)
        # Negative: a plain object is not a Serializer.
        assert not isinstance(object(), Serializer)

    def test_decode_returns_target_type(self) -> None:
        """Generic decode(data, target_type) -> T returns an instance of target_type."""
        s = JSONSerializer()
        result = s.decode(b'{"id": 1}', dict)
        assert isinstance(result, dict)
        assert result == {"id": 1}


# ── encode ───────────────────────────────────────────────────────────────


class TestEncode:
    def test_encode_dict(self) -> None:
        s = JSONSerializer()
        result = s.encode({"key": "value"})
        assert json.loads(result) == {"key": "value"}

    def test_encode_list(self) -> None:
        s = JSONSerializer()
        result = s.encode([1, 2, 3])
        assert json.loads(result) == [1, 2, 3]

    def test_encode_string(self) -> None:
        s = JSONSerializer()
        result = s.encode("hello")
        assert result == b"hello"

    def test_encode_bytes_passthrough(self) -> None:
        s = JSONSerializer()
        data = b"raw bytes"
        result = s.encode(data)
        assert result is data

    def test_encode_dataclass(self) -> None:
        @dataclass
        class Order:
            id: int
            name: str

        s = JSONSerializer()
        result = s.encode(Order(id=1, name="test"))
        parsed = json.loads(result)
        assert parsed == {"id": 1, "name": "test"}

    def test_encode_nested_dict(self) -> None:
        s = JSONSerializer()
        data = {"order": {"id": 1, "items": [{"sku": "A"}]}}
        result = s.encode(data)
        assert json.loads(result) == data

    def test_encode_int(self) -> None:
        s = JSONSerializer()
        result = s.encode(42)
        assert json.loads(result) == 42

    def test_encode_none(self) -> None:
        s = JSONSerializer()
        result = s.encode(None)
        assert json.loads(result) is None


# ── decode ───────────────────────────────────────────────────────────────


class TestDecode:
    def test_decode_to_dict(self) -> None:
        s = JSONSerializer()
        data = json.dumps({"key": "value"}).encode()
        result = s.decode(data, dict)
        assert result == {"key": "value"}

    def test_decode_to_list(self) -> None:
        s = JSONSerializer()
        data = json.dumps([1, 2, 3]).encode()
        result = s.decode(data, list)
        assert result == [1, 2, 3]

    def test_decode_to_bytes(self) -> None:
        s = JSONSerializer()
        data = b"raw bytes"
        result = s.decode(data, bytes)
        assert result is data

    def test_decode_to_str(self) -> None:
        s = JSONSerializer()
        data = b"hello world"
        result = s.decode(data, str)
        assert result == "hello world"

    def test_decode_to_dataclass(self) -> None:
        @dataclass
        class Order:
            id: int
            name: str

        s = JSONSerializer()
        data = json.dumps({"id": 1, "name": "test"}).encode()
        result = s.decode(data, Order)
        assert isinstance(result, Order)
        assert result.id == 1
        assert result.name == "test"

    def test_decode_fallback(self) -> None:
        """Unknown types fall back to json.loads."""
        s = JSONSerializer()
        data = b"42"
        result = s.decode(data, int)
        assert result == 42


# ── round-trip ───────────────────────────────────────────────────────────


class TestRoundTrip:
    def test_dict_round_trip(self) -> None:
        s = JSONSerializer()
        original = {"order_id": 1, "items": ["a", "b"]}
        encoded = s.encode(original)
        decoded = s.decode(encoded, dict)
        assert decoded == original

    def test_dataclass_round_trip(self) -> None:
        @dataclass
        class Event:
            type: str
            data: int

        s = JSONSerializer()
        original = Event(type="click", data=42)
        encoded = s.encode(original)
        decoded = s.decode(encoded, Event)
        assert decoded == original


# ── Pydantic V2 encode/decode paths (lines 33-35, 57, 61-62) ────────────


class TestPydanticSupport:
    def test_encode_pydantic_model(self) -> None:
        """Lines 33-35: model_dump_json() path — str result encoded to UTF-8."""
        try:
            from pydantic import BaseModel
        except ImportError:
            pytest.skip("pydantic not installed")

        class MyModel(BaseModel):
            id: int
            name: str

        s = JSONSerializer()
        model = MyModel(id=1, name="test")
        encoded = s.encode(model)
        assert isinstance(encoded, bytes)
        parsed = json.loads(encoded)
        assert parsed["id"] == 1
        assert parsed["name"] == "test"

    def test_decode_with_model_validate_json(self) -> None:
        """Line 57: model_validate_json() path (Pydantic model decode)."""
        try:
            from pydantic import BaseModel
        except ImportError:
            pytest.skip("pydantic not installed")

        class MyModel(BaseModel):
            id: int

        s = JSONSerializer()
        decoded = s.decode(b'{"id": 42}', MyModel)
        assert isinstance(decoded, MyModel)
        assert decoded.id == 42

    def test_decode_with_model_validate_only(self) -> None:
        """Lines 61-62: model_validate() path — type has model_validate but NOT model_validate_json."""

        class FakeModel:
            @classmethod
            def model_validate(cls, data: dict) -> FakeModel:  # type: ignore[override]
                obj = cls()
                obj.data = data  # type: ignore[attr-defined]
                return obj

        s = JSONSerializer()
        decoded = s.decode(b'{"key": "val"}', FakeModel)
        assert decoded.data == {"key": "val"}  # type: ignore[attr-defined]

    def test_decode_dataclass(self) -> None:
        """Line 69: dataclass decode path — json.loads then constructor."""

        @dataclass
        class Point:
            x: int
            y: int

        s = JSONSerializer()
        decoded = s.decode(b'{"x": 1, "y": 2}', Point)
        assert isinstance(decoded, Point)
        assert decoded.x == 1
        assert decoded.y == 2


class TestDecodeDataclassNonDict:
    def test_decode_dataclass_with_non_dict_json(self) -> None:
        """Line 69: dataclass decode returns parsed value when not a dict."""
        from dataclasses import dataclass

        @dataclass
        class Item:
            value: int

        s = JSONSerializer()
        # json.loads("[1,2,3]") returns a list, not a dict → returns parsed as-is
        result = s.decode(b"[1, 2, 3]", Item)
        assert result == [1, 2, 3]


# ── default=str coercion (L-P1) ──────────────────────────────────────────


class TestNoSilentCoercion:
    def test_default_raises_on_unserializable(self) -> None:
        """By default, encoding an un-serializable object raises TypeError."""
        from datetime import datetime

        s = JSONSerializer()
        with pytest.raises(TypeError, match="not JSON serializable"):
            s.encode({"ts": datetime(2024, 1, 1)})

    def test_default_raises_on_decimal(self) -> None:
        from decimal import Decimal

        s = JSONSerializer()
        with pytest.raises(TypeError, match="not JSON serializable"):
            s.encode({"amount": Decimal("1.5")})

    def test_coerce_unknown_to_str_opt_in(self) -> None:
        """coerce_unknown_to_str=True restores the legacy default=str behaviour."""
        from datetime import datetime

        s = JSONSerializer(coerce_unknown_to_str=True)
        result = s.encode({"ts": datetime(2024, 1, 1)})
        import json as _json

        parsed = _json.loads(result)
        assert parsed["ts"] == "2024-01-01 00:00:00"

    def test_coerce_decimal_to_str_opt_in(self) -> None:
        from decimal import Decimal

        s = JSONSerializer(coerce_unknown_to_str=True)
        result = s.encode({"amount": Decimal("1.5")})
        import json as _json

        assert _json.loads(result)["amount"] == "1.5"

    def test_normal_data_still_serializes_without_coerce(self) -> None:
        s = JSONSerializer()  # default coerce=False
        result = s.encode({"a": 1, "b": [1, 2], "c": "x"})
        import json as _json

        assert _json.loads(result) == {"a": 1, "b": [1, 2], "c": "x"}


# ── L-9: json max_parse_bytes cap ─────────────────────────────────────────


class TestJsonParseSizeCap:
    def test_oversized_input_raises(self) -> None:
        from rabbitkit.serialization.json import JSONSerializer

        ser = JSONSerializer(max_parse_bytes=16)
        with pytest.raises(ValueError, match="max_parse_bytes"):
            ser.decode(b'{"x": "this is way longer than sixteen bytes"}', dict)

    def test_within_cap_decodes(self) -> None:
        from rabbitkit.serialization.json import JSONSerializer

        ser = JSONSerializer(max_parse_bytes=100)
        assert ser.decode(b'{"a": 1}', dict) == {"a": 1}

    def test_default_none_no_cap(self) -> None:
        from rabbitkit.serialization.json import JSONSerializer

        ser = JSONSerializer()
        big = b'{"x": "' + b'a' * 100_000 + b'"}'
        assert ser.decode(big, dict)["x"] == "a" * 100_000
