"""rabbitkit run — start a broker instance.

Supports three execution modes:

Single process (default)::

    rabbitkit run myapp.main:broker

The broker type (sync/async) is detected automatically:
* ``SyncBroker`` — calls ``broker.run()`` which blocks until Ctrl+C.
* ``AsyncBroker`` — calls ``await broker.start()``, then blocks in an async
  loop until Ctrl+C, then calls ``await broker.stop()``.

Hot reload (``--reload``)::

    rabbitkit run myapp.main:broker --reload

    # Watch extra file types (e.g., YAML config)
    rabbitkit run myapp.main:broker --reload --reload-ext .yml,.toml

Requires ``pip install rabbitkit[reload]`` (watchfiles).  Any ``.py`` file
change in the current directory tree triggers a graceful restart.

Multiple processes (``--workers N``)::

    rabbitkit run myapp.main:broker --workers 4

Spawns *N* independent processes using ``multiprocessing.Process``.  Each
process runs a full broker instance.  Ctrl+C sends ``SIGTERM`` to all workers
and waits up to 10 s for clean shutdown.

Notes
-----
* ``--reload`` and ``--workers`` are mutually exclusive — hot-reload only
  supports single-process mode.
* In Docker/Kubernetes, prefer ``--workers 1`` and scale via replicas.
* The app path must be importable (run from project root or set PYTHONPATH).
"""

from __future__ import annotations

import asyncio
import multiprocessing

import typer

from rabbitkit.cli._utils import load_broker


def run_command(
    app_path: str = typer.Argument(..., help="Broker path, e.g. 'myapp.main:broker'"),
    reload: bool = typer.Option(False, "--reload", help="Watch files and restart on changes"),
    reload_ext: str = typer.Option("", "--reload-ext", help="Extra file extensions to watch (comma-separated)"),
    workers: int = typer.Option(1, "--workers", "-w", help="Number of worker processes"),
) -> None:
    """Start a rabbitkit broker.

    Example: rabbitkit run myapp.main:broker --reload --workers 4
    """
    if reload:
        _run_with_reload(app_path, reload_ext)
        return

    if workers > 1:
        _run_multiprocess(app_path, workers)
        return

    _run_single(app_path)


def _run_single(app_path: str) -> None:
    """Run broker in single-process mode."""
    broker = load_broker(app_path)

    # Detect sync vs async broker
    if hasattr(broker, "run") and callable(broker.run):
        # SyncBroker has a blocking run() method
        typer.echo(f"Starting sync broker from {app_path}...")
        broker.run()
    else:
        # AsyncBroker — start + block
        typer.echo(f"Starting async broker from {app_path}...")

        async def _run_async() -> None:
            await broker.start()
            try:
                # Block forever (until Ctrl+C)
                while True:
                    await asyncio.sleep(3600)
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass
            finally:
                await broker.stop()

        asyncio.run(_run_async())


def _run_with_reload(app_path: str, reload_ext: str) -> None:
    """Run broker with file watching and auto-restart."""
    try:
        import watchfiles
    except ImportError:
        typer.echo(
            "Hot reload requires watchfiles. Install with: pip install rabbitkit[reload]",
            err=True,
        )
        raise typer.Exit(code=1)  # noqa: B904

    extensions = {".py"}
    if reload_ext:
        for ext in reload_ext.split(","):
            ext = ext.strip()
            if not ext.startswith("."):
                ext = f".{ext}"
            extensions.add(ext)

    typer.echo(f"Watching for changes (extensions: {extensions})...")
    watchfiles.run_process(
        ".",
        target=_run_single,
        args=(app_path,),
        watch_filter=watchfiles.PythonFilter(extra_extensions=tuple(sorted(extensions - {".py"}))),
    )


def _run_multiprocess(app_path: str, workers: int) -> None:
    """Run multiple broker processes."""
    typer.echo(f"Starting {workers} worker processes...")
    processes: list[multiprocessing.Process] = []

    for i in range(workers):
        p = multiprocessing.Process(target=_run_single, args=(app_path,), name=f"rabbitkit-worker-{i}")
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        typer.echo("Shutting down workers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join(timeout=10)
