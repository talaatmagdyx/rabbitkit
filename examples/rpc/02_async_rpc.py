"""RPC: AsyncRPCClient — async request/response.

Run:
    python examples/rpc/02_async_rpc.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.rpc import AsyncRPCClient, RPCTimeoutError

broker = AsyncBroker(RabbitConfig())


# ── RPC server handlers ───────────────────────────────────────────────────────

@broker.subscriber(queue="rpc.users")
async def handle_get_user(body: bytes) -> bytes:
    """User lookup RPC handler."""
    request = json.loads(body)
    user_id = request.get("id")

    # Simulate DB lookup
    await asyncio.sleep(0.05)

    if user_id == 1:
        return json.dumps({"id": 1, "name": "Alice", "email": "alice@example.com"}).encode()
    elif user_id == 2:
        return json.dumps({"id": 2, "name": "Bob", "email": "bob@example.com"}).encode()
    else:
        return json.dumps({"error": "user_not_found", "id": user_id}).encode()


@broker.subscriber(queue="rpc.inventory")
async def handle_check_stock(body: bytes) -> bytes:
    """Inventory check RPC handler."""
    request = json.loads(body)
    sku = request.get("sku")
    await asyncio.sleep(0.01)
    stock = {"WGT-A": 45, "GAD-B": 3, "SVC-C": 999}.get(sku, 0)
    return json.dumps({"sku": sku, "in_stock": stock > 0, "qty": stock}).encode()


# ── Client ────────────────────────────────────────────────────────────────────

async def run_client() -> None:
    rpc = AsyncRPCClient(broker._transport, max_pending=50)

    try:
        # Single request
        response = await rpc.call(
            routing_key="rpc.users",
            body=json.dumps({"id": 1}).encode(),
            timeout=5.0,
        )
        user = json.loads(response.body)
        print(f"[rpc] User: {user['name']} <{user['email']}>")

        # Concurrent requests
        results = await asyncio.gather(
            rpc.call("rpc.users",     json.dumps({"id": 2}).encode(),        timeout=5.0),
            rpc.call("rpc.inventory", json.dumps({"sku": "WGT-A"}).encode(), timeout=5.0),
            rpc.call("rpc.inventory", json.dumps({"sku": "GAD-B"}).encode(), timeout=5.0),
        )
        bob     = json.loads(results[0].body)
        stock_a = json.loads(results[1].body)
        stock_b = json.loads(results[2].body)
        print(f"[rpc] {bob['name']}: WGT-A stock={stock_a['qty']}, GAD-B stock={stock_b['qty']}")

        # Not-found case
        response = await rpc.call(
            routing_key="rpc.users",
            body=json.dumps({"id": 999}).encode(),
            timeout=5.0,
        )
        error = json.loads(response.body)
        print(f"[rpc] Not found: {error}")

        # Timeout
        try:
            await rpc.call("rpc.nonexistent", b"{}", timeout=1.0)
        except RPCTimeoutError:
            print("[rpc] Timeout (expected for nonexistent queue)")

    finally:
        await rpc.close()


async def main() -> None:
    await broker.start()
    print("RPC server ready. Running client requests...\n")
    await asyncio.sleep(0.1)  # let consumers start
    await run_client()
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
