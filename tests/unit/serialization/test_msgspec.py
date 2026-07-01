"""Tests for serialization/msgspec.py — MsgspecSerializer."""

from __future__ import annotations

import pytest

# msgspec is optional — skip all tests if not installed
msgspec = pytest.importorskip("msgspec")

from rabbitkit.serialization.base import Serializer  # noqa: E402
from rabbitkit.serialization.msgspec import MsgspecSerializer  # noqa: E402


class SampleStruct(msgspec.Struct):
    id: int
    name: str


# ── protocol compliance ─────────────────────────────────────────────────


class TestProtocolCompliance:
    def test_satisfies_serializer_protocol(self) -> None:
        s = MsgspecSerializer()
        assert isinstance(s, Serializer)

    def test_content_type(self) -> None:
        s = MsgspecSerializer()
        assert s.content_type == "application/json"


# ── encode ───────────────────────────────────────────────────────────────


class TestEncode:
    def test_encode_dict(self) -> None:
        s = MsgspecSerializer()
        result = s.encode({"key": "value"})
        decoded = msgspec.json.decode(result)
        assert decoded == {"key": "value"}

    def test_encode_struct(self) -> None:
        s = MsgspecSerializer()
        result = s.encode(SampleStruct(id=1, name="test"))
        decoded = msgspec.json.decode(result, type=SampleStruct)
        assert decoded.id == 1
        assert decoded.name == "test"

    def test_encode_bytes_passthrough(self) -> None:
        s = MsgspecSerializer()
        data = b"raw"
        assert s.encode(data) is data

    def test_encode_string(self) -> None:
        s = MsgspecSerializer()
        result = s.encode("hello")
        assert result == b"hello"


# ── decode ───────────────────────────────────────────────────────────────


class TestDecode:
    def test_decode_struct(self) -> None:
        s = MsgspecSerializer()
        data = msgspec.json.encode(SampleStruct(id=1, name="test"))
        result = s.decode(data, SampleStruct)
        assert isinstance(result, SampleStruct)
        assert result.id == 1
        assert result.name == "test"

    def test_decode_to_bytes(self) -> None:
        s = MsgspecSerializer()
        data = b"raw"
        assert s.decode(data, bytes) is data

    def test_decode_to_str(self) -> None:
        s = MsgspecSerializer()
        assert s.decode(b"hello", str) == "hello"

    def test_decode_dict(self) -> None:
        s = MsgspecSerializer()
        data = msgspec.json.encode({"key": "value"})
        result = s.decode(data, dict)
        assert result == {"key": "value"}


# ── round-trip ───────────────────────────────────────────────────────────


class TestRoundTrip:
    def test_struct_round_trip(self) -> None:
        s = MsgspecSerializer()
        original = SampleStruct(id=42, name="round-trip")
        encoded = s.encode(original)
        decoded = s.decode(encoded, SampleStruct)
        assert decoded == original


# ── decode: generic alias paths ─────────────────────────────────────────


class TestDecodeGenericAlias:
    def test_decode_dict_generic_alias(self) -> None:
        """dict[str, Any] generic alias uses a cached Decoder(type=...)."""
        from typing import Any

        s = MsgspecSerializer()
        data = msgspec.json.encode({"hello": 42})
        result = s.decode(data, dict[str, Any])
        assert result == {"hello": 42}

    def test_decode_list_generic_alias(self) -> None:
        """list[int] generic alias uses a cached Decoder(type=...)."""
        s = MsgspecSerializer()
        data = msgspec.json.encode([1, 2, 3])
        result = s.decode(data, list[int])
        assert result == [1, 2, 3]

    def test_decode_generic_alias_cached(self) -> None:
        """Second call reuses the cached Decoder (no new entry added)."""
        from typing import Any

        s = MsgspecSerializer()
        data = msgspec.json.encode({"a": 1})
        t = dict[str, Any]
        # First call — creates and caches the decoder
        s.decode(data, t)
        decoder_after_first = s._decoders.get(t)
        # Second call — must reuse the cached decoder
        s.decode(data, t)
        assert s._decoders.get(t) is decoder_after_first

    def test_decode_generic_alias_decoder_creation_fails_fallback(self) -> None:
        """If Decoder(type=...) raises for a generic alias, falls back to json.decode."""
        from unittest.mock import MagicMock, patch

        s = MsgspecSerializer()

        # Patch msgspec.json.Decoder to raise, forcing the fallback branch
        bad_decoder_cls = MagicMock(side_effect=Exception("unsupported type"))
        with patch.object(s._msgspec.json, "Decoder", bad_decoder_cls):
            # Use a real generic alias type that has origin=dict
            from typing import Any

            t = dict[str, Any]
            data = msgspec.json.encode({"x": 1})
            # Should fall back to msgspec.json.decode (untyped)
            result = s.decode(data, t)
        assert result == {"x": 1}


# ── decode: Pydantic V2 path ─────────────────────────────────────────────


class TestDecodePydantic:
    def test_decode_pydantic_v2_model(self) -> None:
        """Pydantic V2 model uses model_validate_json."""
        pytest.importorskip("pydantic")
        from pydantic import BaseModel

        class Order(BaseModel):
            order_id: str
            amount: float

        s = MsgspecSerializer()
        data = msgspec.json.encode({"order_id": "ord-1", "amount": 99.5})
        result = s.decode(data, Order)
        assert isinstance(result, Order)
        assert result.order_id == "ord-1"
        assert result.amount == 99.5


# ── decode: issubclass TypeError guard ──────────────────────────────────


class TestDecodeIsSubclassTypeError:
    def test_issubclass_typeerror_falls_through_to_general_decoder(self) -> None:
        """When issubclass(target_type, Struct) raises TypeError, is_struct=False."""
        s = MsgspecSerializer()

        # A type that makes issubclass raise TypeError (e.g. a generic alias without __origin__)
        # We can trigger it by patching issubclass behaviour inside the method by making Struct
        # comparison raise. We use a mock type with no __origin__ so it passes the origin check,
        # no model_validate_json so it passes Pydantic check, then hits issubclass.
        class WeirdMeta(type):
            def __subclasscheck__(cls, sub: type) -> bool:
                raise TypeError("not a class")

        # Create a fake Struct class whose subclass check always raises
        fake_struct = WeirdMeta("FakeStruct", (), {})

        data = msgspec.json.encode(42)

        original_struct = s._msgspec.Struct
        try:
            # Temporarily replace Struct with our raising fake
            s._msgspec.Struct = fake_struct  # type: ignore[assignment]
            # int has no __origin__ and no model_validate_json, so it reaches issubclass
            result = s.decode(data, int)
        finally:
            s._msgspec.Struct = original_struct  # type: ignore[assignment]

        assert result == 42


# ── decode: general typed decoder (cached) ───────────────────────────────


class TestDecodeGeneralTypedDecoder:
    def test_decode_general_typed_decoder_int(self) -> None:
        """Non-Struct, non-Pydantic type uses the general cached Decoder."""
        s = MsgspecSerializer()
        data = msgspec.json.encode(123)
        result = s.decode(data, int)
        assert result == 123

    def test_decode_general_typed_decoder_cached(self) -> None:
        """The general typed decoder is cached on second call."""
        s = MsgspecSerializer()
        data = msgspec.json.encode(7)
        s.decode(data, int)
        decoder_first = s._decoders.get(int)
        s.decode(data, int)
        assert s._decoders.get(int) is decoder_first

    def test_decode_general_typed_decoder_creation_fails_fallback(self) -> None:
        """If Decoder(type=T) raises for a general type, falls back to json.decode."""
        from unittest.mock import MagicMock, patch

        s = MsgspecSerializer()

        bad_decoder_cls = MagicMock(side_effect=Exception("cannot create decoder"))
        data = msgspec.json.encode({"key": "val"})

        with patch.object(s._msgspec.json, "Decoder", bad_decoder_cls):
            # Use int here — no __origin__, no model_validate_json, issubclass(int, Struct)=False
            result = s.decode(data, int)
        # Falls back to untyped json.decode
        assert result == {"key": "val"}
