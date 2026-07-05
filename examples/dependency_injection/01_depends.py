"""Dependency Injection: Depends() — factory injection.

Inject services, database connections, and other dependencies into
handler parameters using Depends() with typing.Annotated.

Run:
    python examples/dependency_injection/01_depends.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
from typing import Annotated

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.di.depends import Depends

broker = AsyncBroker(RabbitConfig())


# ── 1. Simple factory ─────────────────────────────────────────────────────────

class DatabaseSession:
    def __init__(self, name: str) -> None:
        self.name = name
        self._ops: list[str] = []

    def save(self, data: str) -> None:
        self._ops.append(data)
        print(f"[db:{self.name}] saved: {data!r}")

    def close(self) -> None:
        print(f"[db:{self.name}] connection closed ({len(self._ops)} ops)")


_db_call_count = 0

def get_db() -> DatabaseSession:
    """Factory called once per message (cached by default)."""
    global _db_call_count
    _db_call_count += 1
    print(f"[factory] creating db session #{_db_call_count}")
    return DatabaseSession(f"session-{_db_call_count}")


@broker.subscriber(queue="di-demo")
async def handle_order(
    body: bytes,
    db: Annotated[DatabaseSession, Depends(get_db)],
) -> None:
    db.save(body.decode())


# ── 2. Dependency caching ─────────────────────────────────────────────────────
# By default, Depends() caches the result per message.
# Multiple parameters with the same factory get the same instance.

class Logger:
    def __init__(self) -> None:
        self.id = id(self)
        print(f"[factory] creating logger id={self.id}")

    def log(self, msg: str) -> None:
        print(f"[logger:{self.id}] {msg}")

def get_logger() -> Logger:
    return Logger()


@broker.subscriber(queue="di-cache-demo")
async def handle_with_two_deps(
    body: bytes,
    db: Annotated[DatabaseSession, Depends(get_db)],
    logger1: Annotated[Logger, Depends(get_logger)],
    logger2: Annotated[Logger, Depends(get_logger)],  # same factory → same instance
) -> None:
    print(f"logger1 is logger2: {logger1 is logger2}")  # True — same cached instance
    logger1.log(f"processing: {body.decode()}")
    db.save(body.decode())


# ── 3. Disable caching ────────────────────────────────────────────────────────

def get_request_id() -> str:
    import uuid
    return str(uuid.uuid4())[:8]

@broker.subscriber(queue="di-no-cache")
async def handle_no_cache(
    body: bytes,
    req_id1: Annotated[str, Depends(get_request_id, use_cache=False)],
    req_id2: Annotated[str, Depends(get_request_id, use_cache=False)],  # different!
) -> None:
    print(f"req_id1={req_id1!r}, req_id2={req_id2!r}")  # different UUIDs
    print(f"same? {req_id1 == req_id2}")  # False


# ── 4. Nested dependencies ────────────────────────────────────────────────────

def get_config() -> dict[str, str]:
    return {"db_url": "postgresql://localhost/mydb", "env": "dev"}

def get_db_from_config(config: Annotated[dict, Depends(get_config)]) -> DatabaseSession:
    """Depends on another dependency (config)."""
    print(f"[factory] creating db from config: {config['db_url']}")
    return DatabaseSession(config["env"])

@broker.subscriber(queue="di-nested")
async def handle_nested(
    body: bytes,
    db: Annotated[DatabaseSession, Depends(get_db_from_config)],
) -> None:
    db.save(body.decode())


async def main() -> None:
    await broker.start()

    for queue in ["di-demo", "di-cache-demo", "di-no-cache", "di-nested"]:
        await broker.publish(MessageEnvelope(
            routing_key=queue,
            body=f"hello from {queue}".encode(),
        ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
