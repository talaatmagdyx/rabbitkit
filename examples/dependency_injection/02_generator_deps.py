"""Dependency Injection: Generator dependencies with teardown.

yield-based factories open resources before the handler runs and
clean them up in a finally block after the handler returns (or raises).
Cleanup happens in reverse registration order.

Run:
    python examples/dependency_injection/02_generator_deps.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
from typing import Annotated, AsyncGenerator, Generator

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.di.depends import Depends

broker = AsyncBroker(RabbitConfig())


# ── 1. Sync generator (works in both sync and async handlers) ─────────────────

class DbSession:
    def __init__(self, name: str) -> None:
        self.name = name
        print(f"[db] OPEN: {self.name}")

    def execute(self, query: str) -> str:
        return f"result of: {query}"

    def close(self) -> None:
        print(f"[db] CLOSE: {self.name}")


def get_db_session() -> Generator[DbSession, None, None]:
    """Open session before handler, close it after (even on error)."""
    session = DbSession("primary")
    try:
        yield session
    except Exception as exc:
        print(f"[db] rolling back due to: {exc}")
        raise
    finally:
        session.close()   # always runs


@broker.subscriber(queue="gen-deps-demo")
async def handle_with_session(
    body: bytes,
    db: Annotated[DbSession, Depends(get_db_session)],
) -> None:
    result = db.execute(f"SELECT * FROM events WHERE id = '{body.decode()}'")
    print(f"[handler] {result}")


# ── 2. Async generator ────────────────────────────────────────────────────────

class AsyncHttpClient:
    def __init__(self) -> None:
        print("[http] client created")

    async def get(self, url: str) -> str:
        await asyncio.sleep(0.01)  # simulated HTTP call
        return f"response from {url}"

    async def close(self) -> None:
        await asyncio.sleep(0)
        print("[http] client closed")


async def get_http_client() -> AsyncGenerator[AsyncHttpClient, None]:
    """Async generator — creates and destroys an HTTP client per message."""
    client = AsyncHttpClient()
    try:
        yield client
    finally:
        await client.close()


@broker.subscriber(queue="async-gen-demo")
async def handle_with_client(
    body: bytes,
    client: Annotated[AsyncHttpClient, Depends(get_http_client)],
) -> None:
    response = await client.get(f"https://api.example.com/{body.decode()}")
    print(f"[handler] {response}")


# ── 3. Transaction pattern ────────────────────────────────────────────────────

class Transaction:
    def __init__(self) -> None:
        self._committed = False
        print("[tx] BEGIN")

    def commit(self) -> None:
        self._committed = True
        print("[tx] COMMIT")

    def rollback(self) -> None:
        print("[tx] ROLLBACK")


def get_transaction() -> Generator[Transaction, None, None]:
    tx = Transaction()
    try:
        yield tx
        tx.commit()      # auto-commit on success
    except Exception:
        tx.rollback()    # auto-rollback on error
        raise


@broker.subscriber(queue="tx-demo")
async def handle_with_transaction(
    body: bytes,
    tx: Annotated[Transaction, Depends(get_transaction)],
    db: Annotated[DbSession, Depends(get_db_session)],  # both deps created and cleaned up
) -> None:
    print(f"[handler] tx committed={tx._committed}, db={db.name}")


async def main() -> None:
    await broker.start()

    for queue in ["gen-deps-demo", "async-gen-demo", "tx-demo"]:
        await broker.publish(MessageEnvelope(
            routing_key=queue,
            body=b"test-id-42",
        ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
