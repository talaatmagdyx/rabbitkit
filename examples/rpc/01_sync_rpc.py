"""RPC: Sync RPCClient — request/response over RabbitMQ.

Uses RabbitMQ direct reply-to (amq.rabbitmq.reply-to) for zero-latency
RPC without declaring extra queues.

Run:
    python examples/rpc/01_sync_rpc.py

Requirements:
    pip install "rabbitkit[sync]"
    RabbitMQ running on localhost:5672
"""

import json
import threading

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.rpc import RPCClient, RPCTimeoutError
from rabbitkit.sync import SyncBroker

# ── Server side — a handler that returns a response ──────────────────────────
server_broker = SyncBroker(RabbitConfig())


@server_broker.subscriber(queue="rpc.calculator")
def handle_calculate(body: bytes) -> bytes:
    """RPC server handler — returns result as bytes."""
    request = json.loads(body)
    op = request.get("op", "add")
    a, b = request.get("a", 0), request.get("b", 0)

    if op == "add":
        result = a + b
    elif op == "multiply":
        result = a * b
    elif op == "divide":
        result = a / b if b != 0 else None
    else:
        result = None

    print(f"[rpc-server] {a} {op} {b} = {result}")
    return json.dumps({"result": result, "op": op}).encode()


# ── Client side ───────────────────────────────────────────────────────────────

def run_server() -> None:
    server_broker.run()  # blocks until Ctrl+C


def run_client() -> None:
    import time
    time.sleep(1)  # wait for server to start

    client_broker = SyncBroker(RabbitConfig())
    client_broker.start()

    # Create RPC client. Sync RPC needs a dedicated reply connection so the
    # reply I/O loop can be pumped while call() blocks waiting.
    rpc = RPCClient(
        client_broker._transport,
        reply_connection=client_broker._transport._connection,
        max_pending=10,
    )

    try:
        # Addition
        response = rpc.call(
            routing_key="rpc.calculator",
            body=json.dumps({"op": "add", "a": 15, "b": 27}).encode(),
            timeout=5.0,
        )
        result = json.loads(response.body)
        print(f"[rpc-client] 15 + 27 = {result['result']}")

        # Multiplication
        response = rpc.call(
            routing_key="rpc.calculator",
            body=json.dumps({"op": "multiply", "a": 6, "b": 7}).encode(),
            timeout=5.0,
        )
        result = json.loads(response.body)
        print(f"[rpc-client] 6 x 7 = {result['result']}")

        # Timeout example
        try:
            response = rpc.call(
                routing_key="rpc.nonexistent",
                body=b"{}",
                timeout=1.0,  # short timeout
            )
        except RPCTimeoutError:
            print("[rpc-client] timeout! (expected — no handler for this queue)")

    finally:
        rpc.close()
        client_broker.stop()


if __name__ == "__main__":
    # Run server in a background thread, client in main thread
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    run_client()
