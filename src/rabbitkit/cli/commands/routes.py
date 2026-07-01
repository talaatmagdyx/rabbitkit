"""rabbitkit routes — inspect and describe registered consumer routes."""

from __future__ import annotations

import json

import typer

from rabbitkit.cli._utils import load_broker

routes_app = typer.Typer(help="Route inspection commands.")


@routes_app.command("list")
def routes_list(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
    output_format: str = typer.Option("table", "--format", "-f", help="Output format: table or json"),
) -> None:
    """List all registered consumer routes."""
    broker = load_broker(app_path)
    routes = broker.routes

    rows = []
    for r in routes:
        retry = r.retry_config
        rows.append({
            "name": r.name,
            "queue": r.queue.name,
            "exchange": r.exchange.name if r.exchange else "",
            "routing_key": r.queue.routing_key or r.queue.name,
            "ack_policy": r.ack_policy.value,
            "retry": f"{retry.max_retries}x" if retry and hasattr(retry, "max_retries") else "disabled",
            "tags": ",".join(sorted(r.tags)) if r.tags else "",
        })

    if output_format == "json":
        typer.echo(json.dumps(rows, indent=2))
        return

    if not rows:
        typer.echo("No routes registered.")
        return

    headers = ["name", "queue", "exchange", "routing_key", "ack_policy", "retry"]
    widths = {h: max(len(h), max((len(str(r.get(h, ""))) for r in rows), default=0)) for h in headers}
    header_line = "  ".join(h.ljust(widths[h]) for h in headers)
    typer.echo(header_line)
    typer.echo("-" * len(header_line))
    for row in rows:
        typer.echo("  ".join(str(row.get(h, "")).ljust(widths[h]) for h in headers))


@routes_app.command("describe")
def routes_describe(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
    route_name: str = typer.Argument(..., help="Route name to describe"),
) -> None:
    """Show full details of a single consumer route."""
    broker = load_broker(app_path)
    routes = broker.routes

    match = next((r for r in routes if r.name == route_name), None)
    if match is None:
        typer.echo(f"Route '{route_name}' not found.", err=True)
        raise typer.Exit(1)

    retry = match.retry_config
    info: dict[str, object] = {
        "name": match.name,
        "queue": {
            "name": match.queue.name,
            "routing_key": match.queue.routing_key or match.queue.name,
            "durable": match.queue.durable,
            "auto_delete": match.queue.auto_delete,
        },
        "exchange": {
            "name": match.exchange.name,
            "type": match.exchange.type.value if match.exchange else None,
        } if match.exchange else None,
        "ack_policy": match.ack_policy.value,
        "retry": {
            "max_retries": retry.max_retries,
            "delays": list(retry.delays),
        } if retry and hasattr(retry, "max_retries") else None,
        "description": match.description,
        "tags": sorted(match.tags) if match.tags else [],
    }
    typer.echo(json.dumps(info, indent=2))
