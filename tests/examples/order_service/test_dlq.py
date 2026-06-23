"""DLQ predicates + throttled replay logic (docs §10/§31). Uses a fake inspector
so the control flow (batching, abort-on-spike, hard cap) is tested without a broker."""

from __future__ import annotations

from examples.order_service.dlq_tools import error_type_is, safe_replay, tenant_is
from rabbitkit.core.message import RabbitMessage


def _msg(**headers: str) -> RabbitMessage:
    return RabbitMessage(body=b"{}", headers=dict(headers), routing_key="orders.created")


def test_predicates() -> None:
    assert error_type_is("HandlerTimeoutError")(_msg(**{"x-error-type": "HandlerTimeoutError"}))
    assert not error_type_is("HandlerTimeoutError")(_msg(**{"x-error-type": "InvalidTenant"}))
    assert tenant_is("t-42")(_msg(**{"x-tenant-id": "t-42"}))
    assert not tenant_is("t-42")(_msg(**{"x-tenant-id": "t-1"}))


class _FakeInspector:
    """Returns preset peek batches and replay counts; records calls."""

    def __init__(self, peeks: list[list[RabbitMessage]], replays: list[int]) -> None:
        self._peeks = list(peeks)
        self._replays = list(replays)
        self.replay_calls = 0

    async def peek_async(self, dlq: str, limit: int = 10) -> list[RabbitMessage]:
        return self._peeks.pop(0) if self._peeks else []

    async def replay_async(self, dlq: str, predicate: object = None) -> int:
        self.replay_calls += 1
        return self._replays.pop(0) if self._replays else 0


async def test_safe_replay_batches_until_empty() -> None:
    insp = _FakeInspector(peeks=[[_msg()], [_msg()], []], replays=[2, 1])
    total = await safe_replay(insp, "orders.queue.dlq", lambda m: True, pause=0.0)
    assert total == 3
    assert insp.replay_calls == 2


async def test_safe_replay_aborts_on_spike() -> None:
    insp = _FakeInspector(peeks=[[_msg()], [_msg()]], replays=[2, 2])
    total = await safe_replay(
        insp, "orders.queue.dlq", lambda m: True, pause=0.0, abort_check=lambda: True
    )
    assert total == 2
    assert insp.replay_calls == 1  # stopped after the first batch


async def test_safe_replay_respects_max_total() -> None:
    insp = _FakeInspector(peeks=[[_msg()], [_msg()]], replays=[5, 5])
    total = await safe_replay(insp, "orders.queue.dlq", lambda m: True, pause=0.0, max_total=2)
    assert total == 5  # one batch ran, then the cap stopped the loop
    assert insp.replay_calls == 1
