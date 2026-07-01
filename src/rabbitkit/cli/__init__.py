"""rabbitkit CLI — production-grade RabbitMQ toolkit.

Provides the ``rabbitkit`` command-line interface for running, inspecting,
and interacting with rabbitkit brokers.

Requires: ``pip install rabbitkit[cli]``

Available commands
------------------
rabbitkit run <app_path>            Start a broker
rabbitkit run <app_path> --reload   Start with hot-reload (requires rabbitkit[reload])
rabbitkit run <app_path> -w 4       Start 4 worker processes

rabbitkit health check <app_path>   Print broker health as JSON

rabbitkit topology list <app_path>  List all registered routes

rabbitkit routes list <app_path>    List all consumer routes with retry info
rabbitkit routes describe <app_path> <name>  Show full route details

rabbitkit dlq inspect <queue>       Peek at messages in a dead-letter queue
rabbitkit dlq replay <queue> <target>  Republish DLQ messages to a target exchange

rabbitkit shell <app_path>          Open interactive Python shell with broker pre-loaded

App path format
---------------
``<module.path>:<attribute_name>``

Examples::

    rabbitkit run myapp.main:broker
    rabbitkit health check myapp.main:broker
    rabbitkit topology list myapp.main:broker --format json
    rabbitkit routes list myapp.main:broker
    rabbitkit routes describe myapp.main:broker handle_order
    rabbitkit dlq inspect orders.created.dlq --limit 50
    rabbitkit dlq replay orders.created.dlq orders --dry-run
    rabbitkit shell myapp.main:broker

The module must be importable from the current working directory (add it to
``PYTHONPATH`` if needed, or run from the project root).

Installation check::

    pip install "rabbitkit[cli]"
    rabbitkit --help
"""

from __future__ import annotations

try:
    import typer
except ImportError as _err:  # pragma: no cover
    raise ImportError(
        "rabbitkit CLI requires typer. Install with: pip install rabbitkit[cli]"
    ) from _err

from rabbitkit.cli.commands.dlq import dlq_app
from rabbitkit.cli.commands.health import health_app
from rabbitkit.cli.commands.routes import routes_app
from rabbitkit.cli.commands.run import run_command
from rabbitkit.cli.commands.shell import shell_command
from rabbitkit.cli.commands.topology import topology_app

app = typer.Typer(
    name="rabbitkit",
    help="Production-grade RabbitMQ toolkit CLI.",
    no_args_is_help=True,
)

app.command("run")(run_command)
app.command("shell")(shell_command)
app.add_typer(health_app, name="health")
app.add_typer(topology_app, name="topology")
app.add_typer(routes_app, name="routes")
app.add_typer(dlq_app, name="dlq")
