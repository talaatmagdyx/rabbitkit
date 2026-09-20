"""RabbitMessage.channel_alive — the transport-wired staleness probe used by ack_many."""

from __future__ import annotations

from rabbitkit.core.message import RabbitMessage


def test_unknown_when_no_probe() -> None:
    assert RabbitMessage(body=b"x").channel_alive is None


def test_true_false_from_probe() -> None:
    m = RabbitMessage(body=b"x")
    state = {"open": True}
    m._channel_alive = lambda: state["open"]
    assert m.channel_alive is True
    state["open"] = False
    assert m.channel_alive is False


def test_raising_probe_reports_false() -> None:
    m = RabbitMessage(body=b"x")

    def boom() -> bool:
        raise RuntimeError("channel object gone")

    m._channel_alive = boom
    assert m.channel_alive is False


def test_probe_is_a_slot_not_dict_attr() -> None:
    m = RabbitMessage(body=b"x")
    assert "_channel_alive" in RabbitMessage.__slots__
    assert not hasattr(m, "__dict__")
