"""rabbitkit dlq — dead-letter queue inspection and replay commands."""

from __future__ import annotations

import json
import urllib.parse
from typing import Any

import typer

dlq_app = typer.Typer(help="Dead-letter queue commands.")

_MANAGEMENT_URL_HELP = (
    "RabbitMQ management API URL, e.g. http://user:pass@host:15672. With it, the command first "
    "checks the queue's type and delivery limit, and refuses a quorum queue where requeueing would "
    "eventually drop messages. Credentials default to the AMQP URL's."
)
_NO_LIMIT_CHECK_HELP = "Skip the quorum delivery-limit checks (see --management-url)."


class _Unsafe(Exception):
    """Internal: the queue is not safe to browse; the message says why."""


def _management_client(management_url: str, amqp_url: str) -> Any:
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    parsed = urllib.parse.urlparse(management_url)
    amqp = urllib.parse.urlparse(amqp_url)
    username = urllib.parse.unquote(parsed.username or amqp.username or "guest")
    password = urllib.parse.unquote(parsed.password or amqp.password or "guest")
    netloc = parsed.hostname or ""
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    url = urllib.parse.urlunparse((parsed.scheme, netloc, parsed.path.rstrip("/"), "", "", ""))
    return RabbitManagementClient(ManagementConfig(url=url, username=username, password=password))


def _vhost(amqp_url: str) -> str:
    path = urllib.parse.urlparse(amqp_url).path
    return urllib.parse.unquote(path[1:]) if len(path) > 1 else "/"


def _check_browsable(queue: str, management_url: str | None, amqp_url: str) -> bool:
    """Refuse up front when the management API shows requeueing is destructive.

    Returns True when the queue was VETTED. False (no management URL, or the
    API doesn't know the queue) means the x-delivery-count tripwire stays armed.
    """
    return _browse_plan(queue, management_url, amqp_url)[0]


def _browse_plan(queue: str, management_url: str | None, amqp_url: str) -> tuple[bool, bool]:
    """(vetted, read whole). A vetted quorum queue is read only whole: it puts returned
    messages at the back, so a partial read would reorder it."""
    if management_url is None:
        return False, False
    from rabbitkit.core.errors import UnsafeToBrowseError
    from rabbitkit.core.quorum import DEFAULT_MAX_QUORUM_SCAN, assert_browsable, assert_whole_scan, is_quorum

    client = _management_client(management_url, amqp_url)
    try:
        info = client.get_queue(queue, vhost=_vhost(amqp_url))
    except Exception as exc:
        if getattr(exc, "code", None) == 404:
            return False, False  # basic_get reports a missing queue; a wrong vhost trips the wire
        raise _Unsafe(f"could not read {queue!r} from the management API: {exc}") from exc
    try:
        version = client.overview().get("rabbitmq_version")
    except Exception as exc:
        raise _Unsafe(f"could not read the RabbitMQ version from the management API: {exc}") from exc
    try:
        assert_browsable(queue, dict(info), str(version) if version else None)
        if is_quorum(dict(info)):
            assert_whole_scan(queue, dict(info), DEFAULT_MAX_QUORUM_SCAN)
            return True, True
    except UnsafeToBrowseError as exc:
        raise _Unsafe(str(exc)) from exc
    return True, False


def _tripwire_message(queue: str) -> str:
    from rabbitkit.core.quorum import DELIVERY_COUNT_HEADER, unlimited_fix

    return (
        f"{queue!r} is a quorum queue (its messages carry {DELIVERY_COUNT_HEADER!r}): every requeue "
        "counts as a delivery, and past its delivery limit RabbitMQ drops the message. Stopped "
        "before going further. Pass --management-url to verify the limit, or make the queue "
        f"unlimited first: {unlimited_fix(None)}."
    )


def _release(channel: Any, delivery_tags: list[int]) -> None:
    """Requeue held deliveries; best effort, the channel close requeues the rest."""
    for tag in delivery_tags:
        try:
            channel.basic_nack(delivery_tag=tag, requeue=True)
        except Exception:
            break


def _vet_requeue(
    queue: str,
    management_url: str | None,
    amqp_url: str,
    no_limit_check: bool,
    vetted: bool | None,
    properties: Any,
) -> tuple[str | None, bool | None]:
    """A real replay is about to requeue a message: (refusal or None, vetted)."""
    from rabbitkit.core.quorum import DELIVERY_COUNT_HEADER

    if no_limit_check:
        return None, vetted
    if vetted is None:
        try:
            vetted = _check_browsable(queue, management_url, amqp_url)
        except _Unsafe as exc:
            return str(exc), vetted
    if not vetted and DELIVERY_COUNT_HEADER in (properties.headers or {}):
        return _tripwire_message(queue), vetted
    return None, vetted


def _fail(message: str) -> typer.Exit:
    typer.echo(f"REFUSED: {message}", err=True)
    return typer.Exit(2)


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
    management_url: str | None = typer.Option(
        None, "--management-url", "-m", envvar="RABBITMQ_MANAGEMENT_URL", help=_MANAGEMENT_URL_HELP
    ),
    no_limit_check: bool = typer.Option(False, "--no-delivery-limit-check", help=_NO_LIMIT_CHECK_HELP),
) -> None:
    """Inspect messages in a dead-letter queue without removing them.

    Fetches up to ``--limit`` messages with ``basic_get`` and holds them all
    unacked until the fetch ends, so each message is shown once, then
    requeues them in the order they were read. (Requeueing each one straight
    away put it back at the head, so the same message came back ``--limit``
    times.) With ``--management-url``, a quorum queue is read whole and the
    first ``--limit`` are shown: it puts returned messages at the back, so a
    partial read would reorder it.

    Example::

        rabbitkit dlq inspect orders.created.dlq
        rabbitkit dlq inspect orders.created.dlq --limit 100 --format json
    """
    try:
        import pika
    except ImportError:
        typer.echo("pika is required: pip install pika", err=True)
        raise typer.Exit(1) from None
    from rabbitkit.core.quorum import DEFAULT_MAX_QUORUM_SCAN, DELIVERY_COUNT_HEADER, too_deep

    armed = False  # the x-delivery-count tripwire
    whole = False
    if not no_limit_check:
        try:
            vetted, whole = _browse_plan(queue, management_url, amqp_url)
        except _Unsafe as exc:
            raise _fail(str(exc)) from None
        armed = not vetted

    params = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    messages = []
    held: list[int] = []
    tripped = False
    # one past the scan limit: a whole read must see the queue's end
    fetch = DEFAULT_MAX_QUORUM_SCAN + 1 if whole else limit
    try:
        for _ in range(fetch):
            method, properties, body = channel.basic_get(queue=queue, auto_ack=False)
            if method is None:
                break
            held.append(method.delivery_tag)
            headers = dict(properties.headers or {})
            if armed and DELIVERY_COUNT_HEADER in headers:
                tripped = True
                break
            messages.append(
                {
                    "routing_key": method.routing_key,
                    "exchange": method.exchange,
                    "redelivered": method.redelivered,
                    "message_id": properties.message_id,
                    "correlation_id": properties.correlation_id,
                    "headers": headers,
                    "body_preview": body[:200].decode(errors="replace"),
                }
            )
    finally:
        _release(channel, held)
        connection.close()

    if tripped:
        raise _fail(_tripwire_message(queue))
    if whole and len(held) > DEFAULT_MAX_QUORUM_SCAN:  # it grew while it was read
        raise _fail(too_deep(queue, len(held), DEFAULT_MAX_QUORUM_SCAN))
    messages = messages[:limit]

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
    reset_retry_count: bool = typer.Option(
        False,
        "--reset-retry-count",
        help=(
            "Strip the retry-count header before replaying, so the message gets a fresh "
            "retry ladder instead of resuming at its old count (default: preserve headers "
            "verbatim -- a previously max-retried message is terminal after one more failed "
            "attempt and returns straight to the DLQ)."
        ),
    ),
    retry_count_header: str = typer.Option(
        "x-rabbitkit-retry-count",
        "--retry-count-header",
        help="Header name --reset-retry-count strips. Match RetryConfig.retry_header if customized.",
    ),
    management_url: str | None = typer.Option(
        None, "--management-url", "-m", envvar="RABBITMQ_MANAGEMENT_URL", help=_MANAGEMENT_URL_HELP
    ),
    no_limit_check: bool = typer.Option(False, "--no-delivery-limit-check", help=_NO_LIMIT_CHECK_HELP),
) -> None:
    """Replay messages from a dead-letter queue to a target exchange/queue.

    Messages are consumed from the DLQ and published to the target with
    publisher confirms and ``mandatory=True``. The DLQ message is acked
    (removed) only after the broker confirms the republish; a failed or
    unroutable publish is requeued so the message stays on the DLQ.

    Without ``--routing-key``, a message is published with the routing key
    it had before it was dead-lettered (``x-rabbitkit-original-routing-key``,
    then the broker's ``x-death``). A message that would be published
    straight back into the DLQ is skipped.

    ``--dry-run`` requeues every message it shows, so it gets the same
    quorum delivery-limit checks as ``inspect``.

    Example::

        # Replay up to 10 messages to the original exchange
        rabbitkit dlq replay orders.created.dlq orders

        # Dry-run to preview without publishing
        rabbitkit dlq replay orders.created.dlq orders --dry-run

        # Replay with a specific routing key
        rabbitkit dlq replay orders.created.dlq orders -k orders.created

        # Give a previously max-retried message a fresh retry ladder
        rabbitkit dlq replay orders.created.dlq orders --reset-retry-count
    """
    try:
        import pika
        from pika import exceptions as pika_exceptions
    except ImportError:
        typer.echo("pika is required: pip install pika", err=True)
        raise typer.Exit(1) from None
    from rabbitkit.core.quorum import DELIVERY_COUNT_HEADER
    from rabbitkit.dlq import original_routing_key

    # --dry-run requeues everything it shows: vet up front. A real replay
    # acks what it republishes and vets at its first requeue (a failed or
    # skipped message), since each one costs a delivery per run.
    vetted: bool | None = None
    if dry_run and not no_limit_check:
        try:
            vetted = _check_browsable(queue, management_url, amqp_url)
        except _Unsafe as exc:
            raise _fail(str(exc)) from None
    refusal: str | None = None

    params = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    # Confirms make basic_publish raise on nack/unroutable instead of
    # fire-and-forget — without this, ack-after-publish can lose the message.
    channel.confirm_delivery()

    replayed = 0
    failed = 0
    skipped = 0
    tripped = False
    # Requeued only after the loop: a message requeued at once goes back to
    # the head, and the next basic_get would fetch it again.
    held: list[int] = []
    try:
        for _ in range(limit):
            method, properties, body = channel.basic_get(queue=queue, auto_ack=False)
            if method is None:
                break

            if vetted is False and DELIVERY_COUNT_HEADER in (properties.headers or {}):
                held.append(method.delivery_tag)
                tripped = True
                break

            rk = routing_key or original_routing_key(dict(properties.headers or {})) or method.routing_key
            if reset_retry_count and properties.headers and retry_count_header in properties.headers:
                properties.headers.pop(retry_count_header, None)

            if dry_run:
                typer.echo(f"[dry-run] Would publish to exchange={target!r} routing_key={rk!r}  body={body[:100]!r}")
                held.append(method.delivery_tag)
                continue

            if target == "" and rk == queue:
                typer.echo(
                    f"SKIPPED (stays on DLQ): message_id={properties.message_id} would be published back "
                    f"into {queue!r}; pass --routing-key",
                    err=True,
                )
                held.append(method.delivery_tag)
                skipped += 1
                refusal, vetted = _vet_requeue(queue, management_url, amqp_url, no_limit_check, vetted, properties)
                if refusal:
                    break
                continue

            try:
                channel.basic_publish(
                    exchange=target,
                    routing_key=rk,
                    body=body,
                    properties=properties,
                    mandatory=True,
                )
            except (pika_exceptions.UnroutableError, pika_exceptions.NackError) as exc:
                held.append(method.delivery_tag)
                typer.echo(
                    f"FAILED (stays on DLQ): routing_key={rk!r}  message_id={properties.message_id}  ({exc})",
                    err=True,
                )
                failed += 1
                refusal, vetted = _vet_requeue(queue, management_url, amqp_url, no_limit_check, vetted, properties)
                if refusal:
                    break
                continue

            channel.basic_ack(delivery_tag=method.delivery_tag)
            typer.echo(f"Replayed: routing_key={rk!r}  message_id={properties.message_id}")
            replayed += 1
    finally:
        _release(channel, held)
        connection.close()

    if tripped:
        raise _fail(_tripwire_message(queue))
    if refusal:
        typer.echo(f"Replayed {replayed} message(s) before stopping.", err=True)
        raise _fail(refusal)

    if not dry_run:
        typer.echo(f"\nReplayed {replayed} message(s) from {queue!r} → {target!r}.")
        if failed or skipped:
            typer.echo(f"{failed + skipped} message(s) were not replayed and remain on {queue!r}.", err=True)
            raise typer.Exit(1)
