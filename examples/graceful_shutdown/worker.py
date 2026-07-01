"""Worker with graceful SIGTERM shutdown.

broker.stop() drains in-flight messages before closing the connection.
Send SIGTERM (or press Ctrl+C) and the worker will finish any message
currently being processed before exiting.

Run:
    docker run -d --rm -p 5672:5672 rabbitmq:4-management
    python examples/graceful_shutdown/worker.py

In another terminal:
    kill -TERM <pid>

Requirements:
    pip install "rabbitkit[async]"
"""

import asyncio
import signal

from rabbitkit import ConnectionConfig, RabbitConfig
from rabbitkit.aio import AsyncBroker

broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig(host="localhost")))

_shutdown = asyncio.Event()


@broker.subscriber(queue="jobs", routing_key="jobs.run")
async def handle_job(body: bytes) -> None:
    print(f"[handler] started: {body.decode()!r}")
    await asyncio.sleep(3)  # simulates slow processing
    print(f"[handler] done:    {body.decode()!r}")


def _on_signal() -> None:
    print("\n[worker] shutdown signal received — draining in-flight messages...")
    _shutdown.set()


async def main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    await broker.start()
    print("[worker] started — waiting for messages (send SIGTERM to stop gracefully)")

    await _shutdown.wait()

    print("[worker] stopping broker...")
    await broker.stop()
    print("[worker] clean exit")


if __name__ == "__main__":
    asyncio.run(main())
