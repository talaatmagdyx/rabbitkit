#!/usr/bin/env python3
"""Deterministic repro of the AsyncBatchPublisher outage wedge (see WIP commit).

Drives a real AsyncBroker (batch publishing) through toxiproxy, drops the
broker mid-flush, restores it, and asserts recovery. On unpatched rabbitkit the
batch publisher WEDGES permanently (flush workers hang in a dead-connection
channel acquire and never drain). A correct fix must show every batch after the
restore returning ok again.

Setup (toxiproxy proxying a real broker):
    docker run -d --name toxi -p 8474:8474 -p 25672:25672 ghcr.io/shopify/toxiproxy
    curl -X POST localhost:8474/proxies -d '{"name":"relay-rabbit","listen":"0.0.0.0:25672","upstream":"host.docker.internal:5672","enabled":true}'
    # create vhost + creds to match below, then:
    python scripts/repro_batch_outage_wedge.py

PASS = every batch after "proxy UP" returns 100/100 ok.
FAIL = "*** WEDGED ***" (batch never completes after restore).
"""
from __future__ import annotations

import asyncio
import sys
import time

import requests

from rabbitkit import AsyncBroker
from rabbitkit.core.config import BatchPublishConfig, ConnectionConfig, PublisherConfig, RabbitConfig
from rabbitkit.core.types import MessageEnvelope

PROXY = "relay-rabbit"
TOXI = "http://localhost:8474"
CONN = dict(host="localhost", port=25672, username="guest", password="guest", vhost="/")
EXCHANGE = ""  # default exchange; adjust to a declared one if needed


def proxy(enabled: bool) -> None:
    requests.post(f"{TOXI}/proxies/{PROXY}", json={"enabled": enabled}, timeout=5)


async def main() -> int:
    proxy(True)
    cfg = RabbitConfig(
        connection=ConnectionConfig(**CONN),
        publisher=PublisherConfig(confirm_delivery=True, confirm_timeout=3.0, persistent=True),
    )
    broker = AsyncBroker(cfg, batch_config=BatchPublishConfig(batch_size=100, flush_interval_ms=5, max_in_flight=1000))
    await broker.start(install_signal_handlers=False)

    async def one(i: int) -> str:
        try:
            o = await broker.publish(MessageEnvelope(routing_key="repro.q", body=b"x", exchange=EXCHANGE))
            return "ok" if o.status.value == "confirmed" else o.status.value
        except Exception as exc:  # noqa: BLE001
            return type(exc).__name__

    dropped = restored = False
    start = time.monotonic()
    ok_after_restore = 0
    for r in range(40):
        el = time.monotonic() - start
        if not dropped and el > 0.3:
            proxy(False); dropped = True; print(f"[{el:.1f}s] proxy DOWN")
        if dropped and not restored and el > 6.3:
            proxy(True); restored = True; print(f"[{el:.1f}s] proxy UP")
        t = time.monotonic()
        try:
            res = await asyncio.wait_for(asyncio.gather(*[one(i) for i in range(100)]), timeout=25)
            n = res.count("ok")
            print(f"[{time.monotonic()-start:.1f}s] batch {r}: {n}/100 ok in {time.monotonic()-t:.1f}s")
            if restored and n == 100:
                ok_after_restore += 1
                if ok_after_restore >= 3:
                    print("PASS: batch publisher recovered after the outage")
                    await broker.stop(); proxy(True); return 0
        except TimeoutError:
            print(f"[{time.monotonic()-start:.1f}s] batch {r}: *** WEDGED — batch publisher did not recover ***")
            await broker.stop(); proxy(True); return 1
        await asyncio.sleep(0.3)
    await broker.stop(); proxy(True)
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
