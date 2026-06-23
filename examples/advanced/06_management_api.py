"""Advanced: RabbitMQ Management HTTP API client.

Inspect queues, exchanges, connections, and node health via the
RabbitMQ Management HTTP API (port 15672).

Sync: uses stdlib urllib (no extra deps).
Async: uses aiohttp (requires rabbitkit[management]).

Run:
    python examples/advanced/06_management_api.py

Requirements:
    pip install "rabbitkit[management]"    # for async; sync works without it
    RabbitMQ running with Management UI: docker run -p 15672:15672 -p 5672:5672 rabbitmq:3.13-management
"""

import asyncio
import json

from rabbitkit.management import RabbitManagementClient, ManagementConfig


def main_sync() -> None:
    """Synchronous management API examples."""
    client = RabbitManagementClient(
        ManagementConfig(
            url="http://localhost:15672",
            username="guest",
            password="guest",
            timeout=10.0,
        )
    )

    print("=== Sync Management API ===\n")

    # ── Health check ──────────────────────────────────────────────────────────
    healthy = client.health_check()
    print(f"Node healthy: {healthy}")

    if not healthy:
        print("RabbitMQ is not healthy — check the Management UI")
        return

    # ── Overview ──────────────────────────────────────────────────────────────
    overview = client.overview()
    print(f"RabbitMQ version: {overview.get('rabbitmq_version')}")
    print(f"Erlang version:   {overview.get('erlang_version')}")
    mq_stats = overview.get("message_stats", {})
    print(f"Publish rate:     {mq_stats.get('publish_details', {}).get('rate', 0):.1f}/s")

    # ── List queues ───────────────────────────────────────────────────────────
    print("\n--- Queues ---")
    queues = client.list_queues()
    for q in queues:
        if q.get("name", "").startswith("amq."):
            continue  # skip internal queues
        print(f"  {q['name']:<40} messages={q.get('messages', 0):>6} "
              f"consumers={q.get('consumers', 0):>3} "
              f"state={q.get('state', '?')!r}")

    # ── Get specific queue ────────────────────────────────────────────────────
    # Uncomment if you have an 'orders' queue:
    # queue = client.get_queue("orders")
    # print(f"\nOrders queue: {json.dumps(queue, indent=2)}")

    # ── List exchanges ────────────────────────────────────────────────────────
    print("\n--- Exchanges (non-default) ---")
    exchanges = client.list_exchanges()
    for ex in exchanges:
        name = ex.get("name", "")
        if name and not name.startswith("amq."):
            print(f"  {name:<40} type={ex.get('type')!r} durable={ex.get('durable')}")

    # ── List connections ──────────────────────────────────────────────────────
    connections = client.list_connections()
    print(f"\nActive connections: {len(connections)}")
    for conn in connections[:3]:  # show first 3
        print(f"  {conn.get('name', '?')[:60]} state={conn.get('state')!r}")

    # ── List channels ─────────────────────────────────────────────────────────
    channels = client.list_channels()
    print(f"Active channels: {len(channels)}")

    # ── Queue operations ─────────────────────────────────────────────────────
    # Purge a queue (removes all messages — use with caution!)
    # client.purge_queue("test-queue")
    # print("Purged test-queue")

    # Delete a queue
    # client.delete_queue("temp-queue")
    # print("Deleted temp-queue")


async def main_async() -> None:
    """Asynchronous management API examples."""
    client = RabbitManagementClient(ManagementConfig(
        url="http://localhost:15672",
        username="guest",
        password="guest",
    ))

    print("\n=== Async Management API ===\n")

    try:
        healthy = await client.health_check_async()
        print(f"Node healthy (async): {healthy}")

        queues = await client.list_queues_async()
        user_queues = [q for q in queues if not q.get("name", "").startswith("amq.")]
        print(f"User queues (async): {len(user_queues)}")

        overview = await client.overview_async()
        print(f"Version (async): {overview.get('rabbitmq_version')}")

    except ImportError:
        print("aiohttp not installed — run: pip install 'rabbitkit[management]'")


if __name__ == "__main__":
    main_sync()
    asyncio.run(main_async())
