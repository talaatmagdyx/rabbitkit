"""rabbitkit topology migrate — classic → quorum queue migration tool.

RabbitMQ cannot change ``x-queue-type`` in place: re-declaring an existing
classic queue as quorum fails with a 406 PRECONDITION_FAILED. This command
provides a supported migration path using dynamic shovels.

Modes
-----
* Default (``--plan``): read-only. Compares queues whose *declared* type is
  quorum against the *live* broker; for each queue that is still classic,
  prints an ordered runbook and writes a JSON snapshot (bindings + queue
  arguments) as the rollback artifact. Never mutates.
* ``--execute --strategy drain-cutover``: performs the runbook via the
  management API. Rails: refuses queues with consumers (unless ``--force``),
  verifies message counts before every destructive step, and persists
  progress to a state file after each completed step so a crashed run can
  ``--resume``.
* ``--execute --strategy bridge``: creates ``{queue}.q2`` quorum queues and
  duplicates all bindings; prints instructions for moving consumers. Deletes
  nothing.
* ``--execute --dry-run``: prints every management call it would make and
  issues none of them (only read-only discovery calls are performed).

Requires the ``rabbitmq_shovel`` plugin for drain-cutover
(``rabbitmq-plugins enable rabbitmq_shovel``).
"""

from __future__ import annotations

import json
import time
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import typer

from rabbitkit.cli._utils import load_broker
from rabbitkit.management import ManagementConfig, RabbitManagementClient

if TYPE_CHECKING:
    from collections.abc import Callable

DEFAULT_MANAGEMENT_URL = "http://guest:guest@localhost:15672"
DEFAULT_AMQP_URL = "amqp://guest:guest@localhost/"
DEFAULT_STATE_FILE = ".rabbitkit-migrate.json"
DEFAULT_SNAPSHOT_FILE = "rabbitkit-migrate-snapshot.json"

_TMP_SUFFIX = ".migrate-tmp"
_POLL_INTERVAL = 1.0
_STRATEGIES = ("drain-cutover", "bridge")


def _management_client(management_url: str) -> RabbitManagementClient:
    """Build a management client from a URL that may embed credentials.

    Validates the scheme (http/https only — the URL feeds urllib directly)
    and strips userinfo out of the base URL.
    """
    parsed = urllib.parse.urlparse(management_url)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError(
            f"Unsupported management URL scheme {scheme!r}; only 'http' and 'https' are allowed."
        )
    host = parsed.hostname or "localhost"
    netloc = host if parsed.port is None else f"{host}:{parsed.port}"
    base = f"{scheme}://{netloc}{parsed.path.rstrip('/')}"
    return RabbitManagementClient(
        ManagementConfig(
            url=base,
            username=parsed.username or "guest",
            password=parsed.password or "guest",
        )
    )


def _declared_quorum_queues(broker: Any) -> set[str]:
    """Names of queues whose declared queue_type is quorum."""
    names: set[str] = set()
    for route in broker.routes:
        q = route.queue
        queue_type = getattr(q, "queue_type", None)
        if getattr(queue_type, "value", queue_type) == "quorum":
            names.add(q.name)
    return names


def _live_classic_queues(client: RabbitManagementClient, vhost: str) -> dict[str, dict[str, Any]]:
    """Live queues whose actual x-queue-type is (implicitly or explicitly) classic."""
    live: dict[str, dict[str, Any]] = {}
    for queue_info in client.list_queues(vhost):
        info = cast("dict[str, Any]", queue_info)
        arguments = info.get("arguments") or {}
        live_type = info.get("type") or arguments.get("x-queue-type") or "classic"
        if live_type == "classic":
            live[info["name"]] = info
    return live


def _shovel_value(amqp_url: str, src: str, dest: str) -> dict[str, Any]:
    """Dynamic-shovel parameter body moving all current messages from src to dest."""
    return {
        "value": {
            "src-protocol": "amqp091",
            "src-uri": amqp_url,
            "src-queue": src,
            "dest-protocol": "amqp091",
            "dest-uri": amqp_url,
            "dest-queue": dest,
            "src-delete-after": "queue-length",
        }
    }


def _require_shovel_plugin(client: RabbitManagementClient) -> None:
    """Fail fast when GET /api/shovels errors (plugin not enabled)."""
    try:
        client.list_shovel_statuses()
    except Exception as exc:
        typer.echo(
            f"ERROR: the RabbitMQ shovel plugin is not available (GET /api/shovels failed: {exc}). "
            "Enable it with: rabbitmq-plugins enable rabbitmq_shovel",
            err=True,
        )
        raise typer.Exit(1) from None


def _load_state(state_file: str) -> dict[str, Any]:
    path = Path(state_file)
    if path.exists():
        return cast("dict[str, Any]", json.loads(path.read_text()))
    return {"queues": {}}


def _save_state(state_file: str, state: dict[str, Any]) -> None:
    Path(state_file).write_text(json.dumps(state, indent=2))


def _call(dry_run: bool, description: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """Issue a mutating management call, or just print it under --dry-run."""
    if dry_run:
        typer.echo(f"  [dry-run] {description}")
        return
    typer.echo(f"  {description}")
    fn(*args, **kwargs)


def _wait_empty(
    client: RabbitManagementClient, name: str, vhost: str, timeout: float, dry_run: bool
) -> None:
    """Poll queue message count until zero, bounded by ``timeout`` seconds."""
    if dry_run:
        typer.echo(f"  [dry-run] poll GET /api/queues/.../{name} until messages == 0 (timeout {timeout}s)")
        return
    deadline = time.monotonic() + timeout
    while True:
        info = cast("dict[str, Any]", client.get_queue(name, vhost))
        messages = int(info.get("messages") or 0)
        if messages == 0:
            return
        if time.monotonic() >= deadline:
            typer.echo(
                f"ERROR: timed out after {timeout}s waiting for queue '{name}' to drain "
                f"({messages} message(s) left). Re-run with --resume once it is empty.",
                err=True,
            )
            raise typer.Exit(1)
        time.sleep(_POLL_INTERVAL)


def _verify_empty(client: RabbitManagementClient, name: str, vhost: str, dry_run: bool) -> None:
    """Rail: re-verify the message count immediately before a destructive step."""
    if dry_run:
        return
    info = cast("dict[str, Any]", client.get_queue(name, vhost))
    messages = int(info.get("messages") or 0)
    if messages:
        typer.echo(
            f"ERROR: refusing to delete '{name}': {messages} message(s) still queued.",
            err=True,
        )
        raise typer.Exit(1)


def _plan(
    client: RabbitManagementClient,
    candidates: list[str],
    live: dict[str, dict[str, Any]],
    vhost: str,
    snapshot_file: str,
) -> None:
    """Print the ordered runbook and write the rollback snapshot. Never mutates."""
    typer.echo(f"Migration plan — {len(candidates)} queue(s) need classic -> quorum migration.")
    typer.echo("")
    snapshot: dict[str, Any] = {"vhost": vhost, "queues": {}}
    for name in candidates:
        info = live[name]
        bindings = client.get_queue_bindings(name, vhost)
        snapshot["queues"][name] = {
            "queue": {
                "durable": info.get("durable", True),
                "arguments": dict(info.get("arguments") or {}),
            },
            "bindings": bindings,
        }
        tmp = name + _TMP_SUFFIX
        consumers = int(info.get("consumers") or 0)
        n_bindings = len([b for b in bindings if b.get("source")])
        steps = [
            f"Verify zero consumers on '{name}' (currently {consumers})",
            f"Snapshot bindings and queue arguments (rollback artifact: {snapshot_file})",
            f"Create temporary quorum queue '{tmp}'",
            f"Create shovel 'rabbitkit-migrate-{name}-out': '{name}' -> '{tmp}' (src-delete-after=queue-length)",
            f"Wait until '{name}' is empty",
            f"Delete classic queue '{name}'",
            f"Redeclare '{name}' with x-queue-type=quorum (original arguments preserved)",
            f"Recreate {n_bindings} binding(s) on '{name}'",
            f"Create shovel 'rabbitkit-migrate-{name}-back': '{tmp}' -> '{name}'",
            f"Wait until '{tmp}' is empty",
            f"Delete temporary queue '{tmp}'",
        ]
        typer.echo(f"Queue '{name}' (vhost {vhost!r}):")
        for i, step in enumerate(steps, 1):
            typer.echo(f"  {i:2d}. {step}")
        typer.echo("")
    Path(snapshot_file).write_text(json.dumps(snapshot, indent=2))
    typer.echo(f"Snapshot written to {snapshot_file} (rollback artifact).")
    typer.echo("Re-run with --execute --strategy drain-cutover to perform the migration.")


def _bridge(
    client: RabbitManagementClient, candidates: list[str], vhost: str, dry_run: bool
) -> None:
    """Create '{q}.q2' quorum queues with duplicated bindings. Deletes nothing."""
    vhost_encoded = urllib.parse.quote(vhost, safe="")
    for name in candidates:
        bridge_queue = f"{name}.q2"
        bindings = client.get_queue_bindings(name, vhost)
        suffix = " [dry-run]" if dry_run else ""
        typer.echo(f"Bridging '{name}' -> '{bridge_queue}' (quorum){suffix}:")
        _call(
            dry_run,
            f"PUT /api/queues/{vhost_encoded}/{bridge_queue} (x-queue-type=quorum)",
            client.declare_queue,
            bridge_queue,
            vhost=vhost,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )
        duplicated = 0
        for binding in bindings:
            source = binding.get("source") or ""
            if not source:  # default-exchange binding is implicit
                continue
            routing_key = binding.get("routing_key", "")
            _call(
                dry_run,
                f"POST /api/bindings/{vhost_encoded}/e/{source}/q/{bridge_queue} (routing_key={routing_key!r})",
                client.bind_queue,
                bridge_queue,
                source,
                routing_key,
                vhost,
                binding.get("arguments") or None,
            )
            duplicated += 1
        typer.echo(f"  Duplicated {duplicated} binding(s) onto '{bridge_queue}'.")
    typer.echo("")
    typer.echo("Bridge queues created. Next steps (manual):")
    typer.echo("  1. Point consumers at the new '.q2' quorum queues and deploy them.")
    typer.echo("  2. Let the old classic queues drain (both queues receive new messages).")
    typer.echo("  3. Once drained, delete the old classic queues and (optionally) rename consumers.")
    typer.echo("Nothing was deleted by this command.")


def _drain_cutover_queue(
    client: RabbitManagementClient,
    name: str,
    vhost: str,
    amqp_url: str,
    state: dict[str, Any],
    state_file: str,
    force: bool,
    timeout: float,
    dry_run: bool,
) -> None:
    """Run the shovel-based drain-cutover for a single queue, checkpointing each step."""
    tmp = name + _TMP_SUFFIX
    vhost_encoded = urllib.parse.quote(vhost, safe="")
    qstate = state["queues"].setdefault(name, {})
    completed: list[str] = qstate.setdefault("completed", [])
    qstate.setdefault("snapshot", None)

    def done(step: str) -> bool:
        return step in completed

    def mark(step: str) -> None:
        completed.append(step)
        if not dry_run:
            _save_state(state_file, state)

    suffix = " [dry-run]" if dry_run else ""
    typer.echo(f"Migrating '{name}' (drain-cutover){suffix}:")

    # 1. Rail: refuse to move a queue that still has consumers.
    if not done("check-consumers"):
        info = cast("dict[str, Any]", client.get_queue(name, vhost))
        consumers = int(info.get("consumers") or 0)
        if consumers > 0 and not force:
            typer.echo(
                f"ERROR: queue '{name}' has {consumers} consumer(s). "
                "Stop them first, or pass --force to migrate anyway.",
                err=True,
            )
            raise typer.Exit(1)
        mark("check-consumers")

    # 2. Snapshot bindings + arguments (rollback artifact, persisted in the state file).
    if not done("snapshot") or qstate["snapshot"] is None:
        bindings = client.get_queue_bindings(name, vhost)
        info = cast("dict[str, Any]", client.get_queue(name, vhost))
        qstate["snapshot"] = {
            "queue": {
                "durable": info.get("durable", True),
                "arguments": dict(info.get("arguments") or {}),
            },
            "bindings": bindings,
        }
        if not done("snapshot"):
            mark("snapshot")
        elif not dry_run:  # pragma: no cover — re-snapshot only when state was hand-edited
            _save_state(state_file, state)
    snapshot = qstate["snapshot"]

    quorum_args = {k: v for k, v in snapshot["queue"]["arguments"].items() if k != "x-queue-type"}
    quorum_args["x-queue-type"] = "quorum"

    # 3. Temporary quorum queue that will hold messages during the cutover.
    if not done("create-tmp"):
        _call(
            dry_run,
            f"PUT /api/queues/{vhost_encoded}/{tmp} (x-queue-type=quorum)",
            client.declare_queue,
            tmp,
            vhost=vhost,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )
        mark("create-tmp")

    # 4. Shovel old -> tmp (auto-deletes itself after moving the initial queue length).
    shovel_out = f"rabbitkit-migrate-{name}-out"
    if not done("shovel-to-tmp"):
        _call(
            dry_run,
            f"PUT /api/parameters/shovel/{vhost_encoded}/{shovel_out} ('{name}' -> '{tmp}')",
            client.put_parameter,
            "shovel",
            vhost,
            shovel_out,
            _shovel_value(amqp_url, name, tmp),
        )
        mark("shovel-to-tmp")

    # 5. Wait for the source to drain.
    if not done("wait-source-empty"):
        _wait_empty(client, name, vhost, timeout, dry_run)
        mark("wait-source-empty")

    # 6. Rail: re-verify emptiness, then delete the classic queue.
    if not done("delete-source"):
        _verify_empty(client, name, vhost, dry_run)
        _call(
            dry_run,
            f"DELETE /api/queues/{vhost_encoded}/{name}",
            client.delete_queue,
            name,
            vhost,
        )
        mark("delete-source")

    # 7. Redeclare under the same name as quorum, preserving original arguments.
    if not done("redeclare-quorum"):
        _call(
            dry_run,
            f"PUT /api/queues/{vhost_encoded}/{name} (x-queue-type=quorum, original arguments)",
            client.declare_queue,
            name,
            vhost=vhost,
            durable=True,
            arguments=quorum_args,
        )
        mark("redeclare-quorum")

    # 8. Recreate the snapshotted bindings.
    if not done("recreate-bindings"):
        for binding in snapshot["bindings"]:
            source = binding.get("source") or ""
            if not source:  # default-exchange binding is implicit
                continue
            routing_key = binding.get("routing_key", "")
            _call(
                dry_run,
                f"POST /api/bindings/{vhost_encoded}/e/{source}/q/{name} (routing_key={routing_key!r})",
                client.bind_queue,
                name,
                source,
                routing_key,
                vhost,
                binding.get("arguments") or None,
            )
        mark("recreate-bindings")

    # 9. Shovel tmp -> new quorum queue.
    shovel_back = f"rabbitkit-migrate-{name}-back"
    if not done("shovel-back"):
        _call(
            dry_run,
            f"PUT /api/parameters/shovel/{vhost_encoded}/{shovel_back} ('{tmp}' -> '{name}')",
            client.put_parameter,
            "shovel",
            vhost,
            shovel_back,
            _shovel_value(amqp_url, tmp, name),
        )
        mark("shovel-back")

    # 10. Wait for tmp to drain.
    if not done("wait-tmp-empty"):
        _wait_empty(client, tmp, vhost, timeout, dry_run)
        mark("wait-tmp-empty")

    # 11. Rail: re-verify emptiness, then delete the temporary queue.
    if not done("delete-tmp"):
        _verify_empty(client, tmp, vhost, dry_run)
        _call(
            dry_run,
            f"DELETE /api/queues/{vhost_encoded}/{tmp}",
            client.delete_queue,
            tmp,
            vhost,
        )
        mark("delete-tmp")

    typer.echo(f"  Queue '{name}' migrated to quorum.")


def migrate_command(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
    management_url: str = typer.Option(
        DEFAULT_MANAGEMENT_URL,
        "--url",
        "-u",
        help="RabbitMQ management URL (may embed credentials)",
    ),
    amqp_url: str = typer.Option(
        DEFAULT_AMQP_URL,
        "--amqp-url",
        help="AMQP URI used as shovel src/dest URI",
    ),
    vhost: str = typer.Option("/", "--vhost", "-v", help="Virtual host"),
    strategy: str = typer.Option(
        "drain-cutover",
        "--strategy",
        help="Migration strategy: drain-cutover or bridge",
    ),
    plan: bool = typer.Option(False, "--plan", help="Print the runbook only (default mode)"),
    execute: bool = typer.Option(False, "--execute", help="Perform the migration"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="With --execute: print every management call, issue none"
    ),
    timeout: float = typer.Option(
        300.0, "--timeout", help="Max seconds to wait for a queue to drain"
    ),
    queue: str | None = typer.Option(None, "--queue", "-q", help="Limit migration to one queue"),
    force: bool = typer.Option(
        False, "--force", help="Proceed even if the queue has consumers"
    ),
    resume: bool = typer.Option(
        False, "--resume", help="Resume a crashed run from the state file"
    ),
    state_file: str = typer.Option(
        DEFAULT_STATE_FILE, "--state-file", help="Progress checkpoint file for --resume"
    ),
    snapshot_file: str = typer.Option(
        DEFAULT_SNAPSHOT_FILE, "--snapshot-file", help="Rollback snapshot file (plan mode)"
    ),
) -> None:
    """Migrate classic queues that are declared as quorum to actual quorum queues.

    RabbitMQ cannot change x-queue-type in place — re-declaring 406s. Default
    mode prints an ordered runbook and writes a rollback snapshot; --execute
    performs it via the management API (requires the rabbitmq_shovel plugin
    for the drain-cutover strategy).

    Exit code 0 = success / nothing to do, 1 = refused or failed.
    """
    if plan and execute:
        typer.echo("ERROR: --plan and --execute are mutually exclusive.", err=True)
        raise typer.Exit(1)
    if dry_run and not execute:
        typer.echo("ERROR: --dry-run only makes sense with --execute.", err=True)
        raise typer.Exit(1)
    if execute and strategy not in _STRATEGIES:
        typer.echo(
            f"ERROR: unknown strategy {strategy!r}; expected one of: {', '.join(_STRATEGIES)}.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        client = _management_client(management_url)
    except ValueError as exc:
        typer.echo(f"ERROR: {exc}", err=True)
        raise typer.Exit(1) from None

    broker = load_broker(app_path)
    declared = _declared_quorum_queues(broker)

    try:
        live = _live_classic_queues(client, vhost)
    except Exception as exc:
        typer.echo(
            f"ERROR: could not reach management API at {management_url}: {exc}", err=True
        )
        raise typer.Exit(1) from None

    candidates = [name for name in sorted(declared) if name in live]
    if queue is not None:
        candidates = [name for name in candidates if name == queue]
    if not candidates:
        typer.echo("No queues need classic -> quorum migration.")
        return

    if not execute:
        _plan(client, candidates, live, vhost, snapshot_file)
        return

    if strategy == "bridge":
        _bridge(client, candidates, vhost, dry_run)
        return

    # drain-cutover
    if dry_run:
        typer.echo("[dry-run] GET /api/shovels (verify the rabbitmq_shovel plugin is enabled)")
    else:
        _require_shovel_plugin(client)

    state = _load_state(state_file) if resume else {"queues": {}}
    for name in candidates:
        _drain_cutover_queue(
            client,
            name,
            vhost=vhost,
            amqp_url=amqp_url,
            state=state,
            state_file=state_file,
            force=force,
            timeout=timeout,
            dry_run=dry_run,
        )

    if dry_run:
        typer.echo(f"[dry-run] no changes were made; {len(candidates)} queue(s) would be migrated.")
    else:
        typer.echo(
            f"Done — {len(candidates)} queue(s) migrated to quorum. "
            f"State file: {state_file} (safe to delete)."
        )
