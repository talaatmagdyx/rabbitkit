"""rabbitkit topology — inspect, validate, diff, and apply broker topology."""

from __future__ import annotations

import json
from typing import Any, cast

import typer

from rabbitkit.cli._utils import load_broker

topology_app = typer.Typer(help="Topology inspection and management commands.")


@topology_app.command("list")
def topology_list(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
    output_format: str = typer.Option("table", "--format", "-f", help="Output format: table or json"),
) -> None:
    """List all registered routes, queues, and exchanges."""
    broker = load_broker(app_path)
    routes = broker.routes

    rows = []
    for r in routes:
        rows.append({
            "name": r.name,
            "queue": r.queue.name,
            "exchange": r.exchange.name if r.exchange else "",
            "routing_key": r.queue.routing_key,
            "ack_policy": r.ack_policy.value,
            "tags": ",".join(sorted(r.tags)) if r.tags else "",
            "description": r.description,
        })

    if output_format == "json":
        typer.echo(json.dumps(rows, indent=2))
    else:
        if not rows:
            typer.echo("No routes registered.")
            return
        headers = ["name", "queue", "exchange", "routing_key", "ack_policy"]
        widths = {h: max(len(h), max((len(str(r.get(h, ""))) for r in rows), default=0)) for h in headers}
        header_line = "  ".join(h.ljust(widths[h]) for h in headers)
        typer.echo(header_line)
        typer.echo("-" * len(header_line))
        for row in rows:
            typer.echo("  ".join(str(row.get(h, "")).ljust(widths[h]) for h in headers))


def _declared_resources(broker: Any) -> dict[str, Any]:
    """Extract queues and exchanges declared by the broker's routes."""
    queues: dict[str, Any] = {}
    exchanges: dict[str, Any] = {}
    for route in broker.routes:
        q = route.queue
        queues[q.name] = {
            "durable": q.durable,
            "exclusive": q.exclusive,
            "auto_delete": q.auto_delete,
        }
        if route.exchange and route.exchange.name:
            ex = route.exchange
            exchanges[ex.name] = {
                "type": ex.type.value if hasattr(ex.type, "value") else str(ex.type),
                "durable": ex.durable,
                "auto_delete": ex.auto_delete,
            }
    return {"queues": queues, "exchanges": exchanges}


def _live_resources(management_url: str, vhost: str) -> dict[str, Any]:
    """Fetch live queues and exchanges from the management API."""
    import urllib.parse
    import urllib.request

    scheme = urllib.parse.urlparse(management_url).scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError(
            f"Unsupported management URL scheme {scheme!r}; only 'http' and 'https' are "
            "allowed (--url is passed straight to urlopen, which will happily fetch "
            "file:// or other schemes if not restricted)."
        )

    def get(path: str) -> list[dict[str, Any]]:
        req = urllib.request.Request(f"{management_url.rstrip('/')}{path}")  # noqa: S310
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
            return cast("list[dict[str, Any]]", json.loads(resp.read()))

    encoded_vhost = urllib.parse.quote(vhost, safe="")
    try:
        queues_raw = get(f"/api/queues/{encoded_vhost}")
    except Exception:
        queues_raw = []
    try:
        exchanges_raw = get(f"/api/exchanges/{encoded_vhost}")
    except Exception:
        exchanges_raw = []

    queues = {
        q["name"]: {
            "durable": q.get("durable", False),
            "exclusive": q.get("exclusive", False),
            "auto_delete": q.get("auto_delete", False),
        }
        for q in queues_raw
    }
    exchanges = {
        ex["name"]: {
            "type": ex.get("type", "direct"),
            "durable": ex.get("durable", False),
            "auto_delete": ex.get("auto_delete", False),
        }
        for ex in exchanges_raw
        if ex.get("name")  # skip default "" exchange
    }
    return {"queues": queues, "exchanges": exchanges}


@topology_app.command("validate")
def topology_validate(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
    management_url: str = typer.Option(
        "http://guest:guest@localhost:15672",
        "--url", "-u",
        help="RabbitMQ management URL",
    ),
    vhost: str = typer.Option("/", "--vhost", "-v", help="Virtual host"),
) -> None:
    """Validate declared topology against the live RabbitMQ broker.

    Compares queues and exchanges declared by the broker's routes against
    what actually exists in RabbitMQ and reports mismatches.

    Exit code 0 = valid, 1 = mismatches found or connection error.
    """
    broker = load_broker(app_path)
    declared = _declared_resources(broker)

    try:
        live = _live_resources(management_url, vhost)
    except Exception as exc:
        typer.echo(f"ERROR: could not reach management API at {management_url}: {exc}", err=True)
        raise typer.Exit(1) from None

    issues: list[str] = []

    for qname, qprops in declared["queues"].items():
        if qname not in live["queues"]:
            issues.append(f"MISSING queue '{qname}' (not declared in RabbitMQ)")
        else:
            live_q = live["queues"][qname]
            for prop, expected in qprops.items():
                actual = live_q.get(prop)
                if actual != expected:
                    issues.append(f"MISMATCH queue '{qname}' {prop}: declared={expected!r} live={actual!r}")

    for exname, exprops in declared["exchanges"].items():
        if exname not in live["exchanges"]:
            issues.append(f"MISSING exchange '{exname}' (not declared in RabbitMQ)")
        else:
            live_ex = live["exchanges"][exname]
            for prop, expected in exprops.items():
                actual = live_ex.get(prop)
                if actual != expected:
                    issues.append(f"MISMATCH exchange '{exname}' {prop}: declared={expected!r} live={actual!r}")

    if issues:
        typer.echo("Topology validation FAILED:")
        for issue in issues:
            typer.echo(f"  - {issue}")
        raise typer.Exit(1) from None

    n_queues = len(declared["queues"])
    n_exchanges = len(declared["exchanges"])
    typer.echo(f"OK — {n_queues} queue(s) and {n_exchanges} exchange(s) match live broker.")


@topology_app.command("diff")
def topology_diff(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
    management_url: str = typer.Option(
        "http://guest:guest@localhost:15672",
        "--url", "-u",
        help="RabbitMQ management URL",
    ),
    vhost: str = typer.Option("/", "--vhost", "-v", help="Virtual host"),
    output_format: str = typer.Option("text", "--format", "-f", help="Output format: text or json"),
) -> None:
    """Show differences between declared topology and the live broker.

    Lists resources that are declared in code but missing from RabbitMQ,
    and resources that exist in RabbitMQ but are not declared in code.

    Exit code 0 = no diff, 1 = diff found or connection error.
    """
    broker = load_broker(app_path)
    declared = _declared_resources(broker)

    try:
        live = _live_resources(management_url, vhost)
    except Exception as exc:
        typer.echo(f"ERROR: could not reach management API at {management_url}: {exc}", err=True)
        raise typer.Exit(1) from None

    diff: dict[str, Any] = {
        "queues": {
            "declared_not_live": [q for q in declared["queues"] if q not in live["queues"]],
            "live_not_declared": [q for q in live["queues"] if q not in declared["queues"]],
            "property_mismatch": [],
        },
        "exchanges": {
            "declared_not_live": [ex for ex in declared["exchanges"] if ex not in live["exchanges"]],
            "live_not_declared": [ex for ex in live["exchanges"] if ex not in declared["exchanges"]],
            "property_mismatch": [],
        },
    }

    for qname, qprops in declared["queues"].items():
        if qname in live["queues"]:
            for prop, expected in qprops.items():
                actual = live["queues"][qname].get(prop)
                if actual != expected:
                    diff["queues"]["property_mismatch"].append(
                        {"queue": qname, "property": prop, "declared": expected, "live": actual}
                    )

    for exname, exprops in declared["exchanges"].items():
        if exname in live["exchanges"]:
            for prop, expected in exprops.items():
                actual = live["exchanges"][exname].get(prop)
                if actual != expected:
                    diff["exchanges"]["property_mismatch"].append(
                        {"exchange": exname, "property": prop, "declared": expected, "live": actual}
                    )

    has_diff = any(
        diff[r][k]
        for r in ("queues", "exchanges")
        for k in ("declared_not_live", "live_not_declared", "property_mismatch")
    )

    if output_format == "json":
        typer.echo(json.dumps(diff, indent=2))
    else:
        if not has_diff:
            typer.echo("No diff — declared topology matches live broker.")
        else:
            for resource in ("queues", "exchanges"):
                d = diff[resource]
                for name in d["declared_not_live"]:
                    typer.echo(f"+ {resource[:-1]} '{name}' (declared, not in RabbitMQ)")
                for name in d["live_not_declared"]:
                    typer.echo(f"~ {resource[:-1]} '{name}' (in RabbitMQ, not declared in code)")
                for m in d["property_mismatch"]:
                    r_key = "queue" if resource == "queues" else "exchange"
                    typer.echo(
                        f"! {r_key} '{m[r_key]}' {m['property']}: "
                        f"declared={m['declared']!r} live={m['live']!r}"
                    )

    if has_diff:
        raise typer.Exit(1) from None


@topology_app.command("apply")
def topology_apply(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
    amqp_url: str = typer.Option(
        "amqp://guest:guest@localhost/",
        "--url", "-u",
        help="AMQP URL for the broker connection",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print what would be declared without applying"),
) -> None:
    """Declare all queues and exchanges from the broker's registered routes.

    Connects to RabbitMQ and calls queue_declare / exchange_declare for every
    resource registered with the broker. Safe to run repeatedly — uses
    passive=False so existing resources are verified but not recreated.

    Exit code 0 = success, 1 = error.
    """
    import asyncio

    broker = load_broker(app_path)
    declared = _declared_resources(broker)

    n_queues = len(declared["queues"])
    n_exchanges = len(declared["exchanges"])

    if dry_run:
        typer.echo(f"[dry-run] would declare {n_queues} queue(s) and {n_exchanges} exchange(s):")
        for qname, qprops in declared["queues"].items():
            typer.echo(f"  queue    {qname!r}  durable={qprops['durable']}")
        for exname, exprops in declared["exchanges"].items():
            typer.echo(f"  exchange {exname!r}  type={exprops['type']}  durable={exprops['durable']}")
        return

    async def _apply() -> None:
        from rabbitkit.async_.broker import AsyncBroker
        from rabbitkit.core.config import ConnectionConfig, RabbitConfig

        apply_broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(amqp_url)))
        await apply_broker.start()

        transport = apply_broker._transport
        assert transport is not None, "broker transport not initialised after start()"

        from rabbitkit.core.topology import RabbitExchange, RabbitQueue
        from rabbitkit.core.types import ExchangeType

        for qname, qprops in declared["queues"].items():
            q = RabbitQueue(
                name=qname,
                durable=qprops["durable"],
                exclusive=qprops["exclusive"],
                auto_delete=qprops["auto_delete"],
            )
            await transport.declare_queue(q)
            typer.echo(f"  declared queue '{qname}'")

        for exname, exprops in declared["exchanges"].items():
            ex_type_str = exprops["type"]
            valid_types = {e.value for e in ExchangeType}
            ex_type = ExchangeType(ex_type_str) if ex_type_str in valid_types else ExchangeType.DIRECT
            ex = RabbitExchange(
                name=exname,
                type=ex_type,
                durable=exprops["durable"],
                auto_delete=exprops["auto_delete"],
            )
            await transport.declare_exchange(ex)
            typer.echo(f"  declared exchange '{exname}'")

        await apply_broker.stop()

    try:
        asyncio.run(_apply())
        typer.echo(f"Applied: {n_queues} queue(s), {n_exchanges} exchange(s).")
    except Exception as exc:
        typer.echo(f"ERROR: {exc}", err=True)
        raise typer.Exit(1) from None
