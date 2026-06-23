"""rabbitkit shell — interactive Python shell with broker pre-loaded.

Opens a Python REPL with the broker instance and its key attributes already
in scope so you can inspect routes, publish test messages, and debug without
writing a throwaway script.

Usage::

    rabbitkit shell myapp.main:broker

Pre-loaded variables
--------------------
    broker   — the broker instance (AsyncBroker or SyncBroker)
    routes   — broker.routes  (list[RouteDefinition])
    config   — broker.config  (RabbitConfig)
    publish  — broker.publish (callable)

Shell preference
----------------
Uses **IPython** if installed (``pip install ipython``) for auto-complete,
syntax highlighting, and ``%history``.  Falls back to ``code.interact``
(stdlib) if IPython is not available.

Example session::

    rabbitkit shell myapp.main:broker

    rabbitkit shell -- 3 routes loaded from myapp.main:broker
    In [1]: routes
    Out[1]: [RouteDefinition(queue=RabbitQueue(name='orders', ...), ...)]

    In [2]: broker.config.connection.host
    Out[2]: 'localhost'

    In [3]: import json; publish(MessageEnvelope(routing_key='orders', body=json.dumps({'test': 1}).encode()))
    Out[3]: PublishOutcome(ok=True, ...)

    In [4]: exit
"""

from __future__ import annotations

import typer

from rabbitkit.cli._utils import load_broker


def shell_command(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
) -> None:
    """Launch interactive Python shell with broker pre-loaded.

    Available variables: broker, routes, config, publish.
    Uses IPython if available, falls back to stdlib code.interact.
    """
    broker = load_broker(app_path)
    local_vars = {
        "broker": broker,
        "routes": broker.routes,
        "config": broker.config,
        "publish": broker.publish,
    }
    banner = f"rabbitkit shell -- {len(broker.routes)} routes loaded from {app_path}"

    try:
        from IPython import embed

        embed(user_ns=local_vars, banner1=banner)
    except ImportError:
        import code

        code.interact(banner=banner, local=local_vars)
