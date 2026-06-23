"""rabbitkit topology — inspect registered routes."""

from __future__ import annotations

import json

import typer

from rabbitkit.cli._utils import load_broker

topology_app = typer.Typer(help="Topology inspection commands.")


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
        # Simple table output
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
