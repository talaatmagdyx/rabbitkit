"""rabbitkit dlq — dead-letter queue inspection and replay commands."""

from __future__ import annotations

import json

import typer

dlq_app = typer.Typer(help="Dead-letter queue commands.")


@dlq_app.command("inspect")
def dlq_inspect(
    queue: str = typer.Argument(..., help="DLQ name to inspect, e.g. 'orders.created.dlq'"),
    amqp_url: str = typer.Option(
        "amqp://guest:guest@localhost/",
        "--url",
        "-u",
        envvar="RABBITMQ_URL",
        help="AMQP connection URL",
    ),
    limit: int = typer.Option(20, "--limit", "-n", help="Maximum messages to fetch"),
    output_format: str = typer.Option("table", "--format", "-f", help="Output format: table or json"),
) -> None:
    """Inspect messages in a dead-letter queue without removing them.

    Connects directly to RabbitMQ and peeks at the DLQ contents using
    basic_get in passive mode.

    Example::

        rabbitkit dlq inspect orders.created.dlq
        rabbitkit dlq inspect orders.created.dlq --limit 100 --format json
    """
    try:
        import pika
    except ImportError:
        typer.echo("pika is required: pip install pika", err=True)
        raise typer.Exit(1) from None

    params = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    messages = []
    for _ in range(limit):
        method, properties, body = channel.basic_get(queue=queue, auto_ack=False)
        if method is None:
            break
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        entry = {
            "routing_key": method.routing_key,
            "exchange": method.exchange,
            "redelivered": method.redelivered,
            "message_id": properties.message_id,
            "correlation_id": properties.correlation_id,
            "headers": dict(properties.headers or {}),
            "body_preview": body[:200].decode(errors="replace"),
        }
        messages.append(entry)

    connection.close()

    if output_format == "json":
        typer.echo(json.dumps(messages, indent=2, default=str))
        return

    if not messages:
        typer.echo(f"No messages in {queue!r}.")
        return

    typer.echo(f"Messages in {queue!r} ({len(messages)} shown):")
    typer.echo("-" * 60)
    for i, msg in enumerate(messages, 1):
        typer.echo(f"[{i}] routing_key={msg['routing_key']}  message_id={msg['message_id']}")
        if msg["headers"]:
            typer.echo(f"     headers={msg['headers']}")
        typer.echo(f"     body: {msg['body_preview']}")
        typer.echo()


@dlq_app.command("replay")
def dlq_replay(
    queue: str = typer.Argument(..., help="DLQ name to replay from, e.g. 'orders.created.dlq'"),
    target: str = typer.Argument(..., help="Target exchange or queue to republish to"),
    amqp_url: str = typer.Option(
        "amqp://guest:guest@localhost/",
        "--url",
        "-u",
        envvar="RABBITMQ_URL",
        help="AMQP connection URL",
    ),
    limit: int = typer.Option(10, "--limit", "-n", help="Maximum messages to replay"),
    routing_key: str | None = typer.Option(None, "--routing-key", "-k", help="Override routing key"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be replayed without publishing"),
) -> None:
    """Replay messages from a dead-letter queue to a target exchange/queue.

    Messages are consumed from the DLQ and published to the target.
    On successful publish, the DLQ message is acked (removed).

    Example::

        # Replay up to 10 messages to the original exchange
        rabbitkit dlq replay orders.created.dlq orders

        # Dry-run to preview without publishing
        rabbitkit dlq replay orders.created.dlq orders --dry-run

        # Replay with a specific routing key
        rabbitkit dlq replay orders.created.dlq orders -k orders.created
    """
    try:
        import pika
    except ImportError:
        typer.echo("pika is required: pip install pika", err=True)
        raise typer.Exit(1) from None

    params = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    replayed = 0
    for _ in range(limit):
        method, properties, body = channel.basic_get(queue=queue, auto_ack=False)
        if method is None:
            break

        rk = routing_key or method.routing_key
        if dry_run:
            typer.echo(f"[dry-run] Would publish to exchange={target!r} routing_key={rk!r}  body={body[:100]!r}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            channel.basic_publish(
                exchange=target,
                routing_key=rk,
                body=body,
                properties=properties,
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            typer.echo(f"Replayed: routing_key={rk!r}  message_id={properties.message_id}")
            replayed += 1

    connection.close()

    if not dry_run:
        typer.echo(f"\nReplayed {replayed} message(s) from {queue!r} → {target!r}.")
