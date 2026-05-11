from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path

import typer

import sluice.dag.run_pipeline as dag_run_pipeline_module
from sluice.dag.rotation.lock import RotationLock, check_rotation_fast_path
from sluice.dag.writer import WriterThread

app = typer.Typer(no_args_is_help=True)
items_app = typer.Typer(no_args_is_help=True, help="Inspect / manage items")
app.add_typer(items_app, name="items")


async def ensure_dynamic_tables(task):
    return await dag_run_pipeline_module.ensure_dynamic_tables(task)


async def dag_run_pipeline(*args, **kwargs):
    return await dag_run_pipeline_module.run_pipeline(*args, **kwargs)


@app.command()
def validate(config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c")):
    """Validate all v2 task config directories."""
    from sluice.dag.loader import load_all_tasks
    from sluice.dag.validator import validate_task

    tasks = load_all_tasks(config_dir)
    for task in tasks:
        validate_task(task)
    typer.echo(f"OK: {len(tasks)} task(s) loaded")


@app.command("list")
def list_tasks(config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c")):
    """List configured v2 tasks and pipelines."""
    from sluice.dag.loader import load_all_tasks

    for task in load_all_tasks(config_dir):
        pipelines = ",".join(p.name for p in task.pipelines)
        typer.echo(f"{task.name}\tenabled={task.enabled}\tpipelines={pipelines}")


@app.command()
def deploy(
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
    sluice_config_path: Path | None = typer.Option(None, "--sluice-config"),
):
    """Register all v2 task pipelines and rotations as Prefect deployments."""
    from sluice.dag.deploy import _prefect_api_settings, push_all_deployments
    from sluice.dag.loader import load_all_tasks
    from sluice.dag.sluice_config import load_sluice_config

    config_dir = config_dir.resolve()
    sluice_config_path = (
        sluice_config_path.resolve()
        if sluice_config_path
        else (config_dir / "sluice.toml").resolve()
    )
    if not sluice_config_path.is_relative_to(config_dir):
        typer.echo("--sluice-config must be inside --config-dir", err=True)
        raise typer.Exit(code=2)
    tasks = load_all_tasks(config_dir)
    sluice_cfg = load_sluice_config(sluice_config_path)

    with _prefect_api_settings(sluice_cfg.runtime):
        n_pip, n_rot = asyncio.run(
            push_all_deployments(tasks, sluice_cfg, str(config_dir), str(sluice_config_path))
        )

    if n_pip == 0 and n_rot == 0:
        typer.echo("Warning: No v2 task deployments registered.")
    typer.echo(f"Registered {n_pip} pipeline deployment(s) + {n_rot} rotation deployment(s).")


@app.command()
def migrate(
    task_name: str = typer.Argument(..., metavar="TASK"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
):
    """Apply schema migrations and create per-stage/per-pipeline tables for TASK."""
    from sluice.dag.admin import migrate_task
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    asyncio.run(migrate_task(task))
    typer.echo(f"Migrated task {task_name!r}.")


@app.command()
def retry(
    task_name: str = typer.Option(..., "--task", "-t"),
    pipeline: str = typer.Option(..., "--pipeline", "-p"),
    item: str | None = typer.Option(None, "--item"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
    remote: bool = typer.Option(False, "--remote", help="Also trigger a Prefect run now."),
):
    """Clear terminal pipeline/stage rows so the next cron retries."""
    from sluice.dag.admin import retry_pipeline
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    cleared = asyncio.run(retry_pipeline(task, pipeline=pipeline, item_id=item))
    typer.echo(f"Cleared {cleared} terminal row(s) for pipeline {pipeline!r}.")
    if remote:
        from prefect.deployments import run_deployment

        asyncio.run(run_deployment(name=f"sluice-run-pipeline/{task.name}-{pipeline}"))
        typer.echo("Triggered Prefect run.")


@items_app.command("list")
def items_list(
    task_name: str = typer.Argument(..., metavar="TASK"),
    limit: int = typer.Option(50, "--limit", "-n"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
):
    """List recent items and per-pipeline status."""
    from sluice.dag.admin import list_items
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    rows = asyncio.run(list_items(task, limit=limit))
    for row in rows:
        typer.echo(json.dumps(row, ensure_ascii=False))


@items_app.command("show")
def items_show(
    task_name: str = typer.Argument(..., metavar="TASK"),
    item_id: str = typer.Argument(..., metavar="ITEM_ID"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
):
    from sluice.dag.admin import show_item
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    try:
        prov = asyncio.run(show_item(task, item_id=item_id))
    except KeyError:
        typer.echo(f"item {item_id!r} not found", err=True)
        raise typer.Exit(code=1)
    typer.echo(json.dumps(prov, ensure_ascii=False, indent=2, default=str))


@items_app.command("kill")
def items_kill(
    task_name: str = typer.Argument(..., metavar="TASK"),
    item_id: str = typer.Argument(..., metavar="ITEM_ID"),
    reason: str = typer.Option("killed_by_operator", "--reason"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
):
    from sluice.dag.admin import kill_item
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    try:
        asyncio.run(kill_item(task, item_id=item_id, reason=reason))
    except KeyError:
        typer.echo(f"item {item_id!r} not found", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"Killed item {item_id!r}.")


@app.command("scaffold-task")
def scaffold_task(
    from_old: Path = typer.Option(..., "--from-old"),
    out_dir: Path = typer.Option(..., "--out"),
):
    """Best-effort scaffold a v2 task directory from a v1 pipeline TOML file."""
    from sluice.dag.admin import scaffold_from_v1

    scaffold_from_v1(old_path=from_old, out_dir=out_dir)
    typer.echo(f"Scaffolded {out_dir}. Hand-tuning required.")


@app.command("run")
def dag_run(
    task: str = typer.Option(..., "-t", "--task", help="task name under <config-dir>/tasks/"),
    pipeline: str = typer.Option(..., "-p", "--pipeline", help="pipeline name within the task"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
    cycle: str | None = typer.Option(
        None,
        "--cycle",
        help="ISO-8601 timestamp to override current time.",
    ),
):
    """Run a single pipeline once."""
    task_dir = config_dir / "tasks" / task
    if not task_dir.exists():
        typer.echo(f"task not found: {task_dir}", err=True)
        raise typer.Exit(code=2)

    from sluice.dag.loader import load_task

    cfg = load_task(task_dir)
    if not any(p.name == pipeline for p in cfg.pipelines):
        typer.echo(f"pipeline {pipeline!r} not found in task {task!r}", err=True)
        raise typer.Exit(code=2)

    async def _go():
        lock = RotationLock(cfg.db_path)
        try:
            lock.acquire()
        except RuntimeError:
            fast = check_rotation_fast_path(cfg, pipeline)
            if fast is not None:
                return fast
            raise

        try:
            await ensure_dynamic_tables(cfg)
            writer = WriterThread(
                active_path=cfg.db_path,
                seen_path=cfg.seen_db,
                hold_rotation_lock=False,
            )
            await writer.start()
            try:
                now = datetime.fromisoformat(cycle) if cycle else None
                return await dag_run_pipeline(cfg, pipeline, writer=writer, now=now)
            finally:
                await writer.stop()
        finally:
            lock.release()

    try:
        result = asyncio.run(_go())
    except Exception as exc:
        typer.echo(f"pipeline run failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(
        json.dumps(
            {
                "pipeline_name": result.pipeline_name,
                "run_key": result.run_key,
                "status": result.status,
                "items_in": result.items_in,
                "items_out": result.items_out,
                "error_msg": result.error_msg,
            },
            ensure_ascii=False,
        )
    )
    if result.status not in {"success", "skipped_gating", "skipped_rotation"}:
        raise typer.Exit(code=1)


@app.command("gc-rotate")
def gc_rotate(
    task: str = typer.Option(..., "-t", "--task"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
    sluice_config_path: Path | None = typer.Option(None, "--sluice-config"),
    dry_run: bool = typer.Option(False, "--dry-run"),
):
    """Rotate a task active DB: drain, partition, archive, and reopen fresh active."""
    task_dir = config_dir / "tasks" / task
    if not task_dir.exists():
        typer.echo(f"task not found: {task_dir}", err=True)
        raise typer.Exit(code=2)

    from sluice.dag.db import open_active_db
    from sluice.dag.loader import load_task
    from sluice.dag.rotation.orchestrator import rotate_task
    from sluice.dag.rotation.partition import partition_items
    from sluice.dag.sluice_config import load_sluice_config

    cfg = load_task(task_dir)
    sluice_cfg = load_sluice_config(sluice_config_path)

    if dry_run:

        async def _dry_run():
            lock = RotationLock(cfg.db_path)
            try:
                lock.acquire()
            except RuntimeError:
                return None
            try:
                async with open_active_db(cfg.db_path) as conn:
                    return await partition_items(conn, cfg)
            finally:
                lock.release()

        partition = asyncio.run(_dry_run())
        if partition is None:
            typer.echo(
                json.dumps(
                    {"status": "lock_busy", "carryover_count": 0, "archive_count": 0},
                    ensure_ascii=False,
                )
            )
            return
        typer.echo(
            json.dumps(
                {
                    "status": "dry_run",
                    "carryover_count": len(partition.carryover_ids),
                    "archive_count": len(partition.archive_ids),
                },
                ensure_ascii=False,
            )
        )
        return

    result = asyncio.run(rotate_task(cfg, config=sluice_cfg))
    typer.echo(
        json.dumps(
            {
                "task": result.task_name,
                "status": result.status,
                "carryover": result.carryover_count,
                "archive": result.archive_count,
                "archive_path": result.archive_path,
                "error": result.error_msg,
            },
            ensure_ascii=False,
        )
    )
    if result.status not in {"rotated", "no_op", "lock_busy", "drain_timeout"}:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
