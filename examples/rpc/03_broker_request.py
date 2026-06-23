"""RPC: broker.request() shorthand.

broker.request() is a convenience method that lazily creates an RPCClient
and reuses it across calls. No need to manage the client lifecycle.

Run:
    python examples/rpc/03_broker_request.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import RabbitConfig
from rabbitkit.async_ import AsyncBroker

broker = AsyncBroker(RabbitConfig())


# ── RPC server handlers ───────────────────────────────────────────────────────

@broker.subscriber(queue="rpc.prices")
async def get_price(body: bytes) -> bytes:
    """Returns price for the given SKU."""
    req = json.loads(body)
    prices = {"SKU-001": 9.99, "SKU-002": 24.99, "SKU-003": 4.49}
    price = prices.get(req["sku"], 0.0)
    return json.dumps({"sku": req["sku"], "price": price}).encode()


@broker.subscriber(queue="rpc.validate")
async def validate_coupon(body: bytes) -> bytes:
    """Validates a discount coupon."""
    req = json.loads(body)
    valid_coupons = {"SAVE10": 0.10, "SAVE20": 0.20}
    discount = valid_coupons.get(req.get("code", ""))
    if discount:
        return json.dumps({"valid": True, "discount": discount}).encode()
    return json.dumps({"valid": False, "discount": 0.0}).encode()


# ── Client using broker.request() ────────────────────────────────────────────

async def checkout(sku: str, coupon: str) -> None:
    """Orchestrates multiple RPC calls to complete a checkout."""
    print(f"\n--- Checkout: sku={sku!r}, coupon={coupon!r} ---")

    # Get price
    price_resp = await broker.request(
        routing_key="rpc.prices",
        body=json.dumps({"sku": sku}).encode(),
        timeout=5.0,
    )
    price_data = json.loads(price_resp.body)

    # Validate coupon concurrently (in a real app)
    coupon_resp = await broker.request(
        routing_key="rpc.validate",
        body=json.dumps({"code": coupon}).encode(),
        timeout=5.0,
    )
    coupon_data = json.loads(coupon_resp.body)

    # Calculate final price
    base_price = price_data["price"]
    discount = coupon_data["discount"] if coupon_data["valid"] else 0.0
    final = base_price * (1 - discount)

    print(f"  Price: ${base_price:.2f}")
    print(f"  Coupon: {'valid (' + str(int(discount*100)) + '% off)' if coupon_data['valid'] else 'invalid'}")
    print(f"  Total: ${final:.2f}")


async def main() -> None:
    await broker.start()
    await asyncio.sleep(0.1)  # let consumers register

    await checkout("SKU-001", "SAVE20")   # valid coupon
    await checkout("SKU-002", "INVALID")  # invalid coupon
    await checkout("SKU-003", "SAVE10")   # valid coupon

    # broker.request() with extra options
    resp = await broker.request(
        routing_key="rpc.prices",
        body=json.dumps({"sku": "SKU-002"}).encode(),
        timeout=3.0,
        exchange="",                          # default exchange
        headers={"x-caller": "checkout-service"},
    )
    print(f"\nDirect request result: {json.loads(resp.body)}")

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
