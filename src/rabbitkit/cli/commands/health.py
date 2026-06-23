"""rabbitkit health — broker health check."""

from __future__ import annotations

import json

import typer

from rabbitkit.cli._utils import load_broker

health_app = typer.Typer(help="Health check commands.")


@health_app.command("check")
def health_check(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
) -> None:
    """Check broker health and exit with code 1 if unhealthy."""
    from rabbitkit.health import HealthStatus, broker_health_check

    broker = load_broker(app_path)
    result = broker_health_check(broker)

    output = {
        "status": result.status.value,
        "started": result.started,
        "connected": result.connected,
        "consumer_count": result.consumer_count,
        "route_count": result.route_count,
    }
    typer.echo(json.dumps(output, indent=2))

    if result.status != HealthStatus.HEALTHY:
        raise typer.Exit(code=1)
