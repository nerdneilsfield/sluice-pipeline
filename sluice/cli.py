from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Sequence

import typer
from rich.console import Console
from rich.table import Table
from tqdm import tqdm

from sluice.loader import load_all
from sluice.runner import run_pipeline

app = typer.Typer(no_args_is_help=True)
console = Console()


def _import_all():
    # v1 uses explicit imports for plugin registration. Entry-points or
    # package discovery can replace this once third-party plugins exist.
    from sluice.sources import rss  # noqa
    from sluice.fetchers import trafilatura_fetcher, firecrawl, jina_reader  # noqa
    from sluice.sinks import file_md, notion  # noqa


@app.command()
def validate(config_dir: Path = typer.Option("./configs", "--config-dir", "-c")):
    """Validate all TOML config files."""
    _import_all()
    bundle = load_all(config_dir)
    typer.echo(f"OK: {len(bundle.pipelines)} pipeline(s) loaded")


@app.command()
def list(config_dir: Path = typer.Option("./configs", "--config-dir", "-c")):
    """List configured pipelines."""
    _import_all()
    bundle = load_all(config_dir)
    for pid, pipe in bundle.pipelines.items():
        cron = pipe.cron or bundle.global_cfg.runtime.default_cron
        typer.echo(f"{pid}\t{cron}\tenabled={pipe.enabled}")


@app.command()
def run(
    pipeline_id: str,
    config_dir: Path = typer.Option("./configs", "--config-dir", "-c"),
    dry_run: bool = typer.Option(False, "--dry-run"),
):
    """Run a pipeline once."""
    _import_all()
    bundle = load_all(config_dir)
    if pipeline_id not in bundle.pipelines:
        typer.echo(f"unknown pipeline: {pipeline_id}", err=True)
        raise typer.Exit(2)
    progress_bar = None
    steps = []

    def progress(event: str, **data):
        nonlocal progress_bar
        if event == "plan":
            total = max(1, data["total"])
            progress_bar = tqdm(
                total=total,
                unit="step",
                desc="starting",
                dynamic_ncols=True,
                disable=not console.is_terminal,
            )
            return
        if event in {"source_started", "processor_started", "sink_started"} and progress_bar:
            progress_bar.set_description(_event_label(event, data))
            return
        if event in {"source_done", "processor_done", "sink_done"}:
            steps.append(_step_row(event, data))
            if progress_bar:
                progress_bar.update(data.get("advance", 1))

    try:
        res = asyncio.run(
            run_pipeline(bundle, pipeline_id=pipeline_id, dry_run=dry_run, progress=progress)
        )
    finally:
        if progress_bar:
            progress_bar.close()

    _print_step_table(steps)
    _print_run_summary(res, dry_run=dry_run)
    raise typer.Exit(0 if res.status == "success" else 1)


def _event_label(event: str, data: dict) -> str:
    if event == "source_started":
        return f"rss {data.get('index')}/{data.get('total')}: {data.get('name')}"
    if event == "processor_started":
        return f"stage: {data.get('name')} ({data.get('items_in')} in)"
    if event == "sink_started":
        return f"sink: {data.get('id')} ({data.get('items_in')} in)"
    return event


def _step_row(event: str, data: dict) -> tuple[str, str, str, str, str]:
    if event == "source_done":
        return (
            "source",
            str(data.get("name")),
            "-",
            str(data.get("items_out", 0)),
            f"total={data.get('total_items', 0)}",
        )
    if event == "processor_done":
        return (
            "stage",
            str(data.get("name")),
            str(data.get("items_in", 0)),
            str(data.get("items_out", 0)),
            _format_details(data.get("details")),
        )
    return (
        "sink",
        f"{data.get('id')}:{data.get('type')}",
        str(data.get("items_in", 0)),
        "emitted" if data.get("emitted") else "skipped",
        "",
    )


def _format_details(details: Any) -> str:
    if not isinstance(details, dict):
        return ""
    parts = []
    for key in ("fetched", "used_existing", "empty", "failed"):
        value = details.get(key)
        if value:
            parts.append(f"{key}={value}")
    errors = details.get("errors")
    if isinstance(errors, dict):
        parts.extend(f"{k}={v}" for k, v in sorted(errors.items()) if v)
    return " ".join(parts)


def _print_step_table(steps: Sequence[tuple[str, str, str, str, str]]) -> None:
    if not steps:
        return
    table = Table(title="Step Summary")
    table.add_column("kind")
    table.add_column("name")
    table.add_column("in", justify="right")
    table.add_column("out", justify="right")
    table.add_column("details")
    for row in steps:
        table.add_row(*row)
    console.print(table)


def _print_run_summary(res, *, dry_run: bool) -> None:
    table = Table(title="Run Summary")
    table.add_column("metric")
    table.add_column("value")
    table.add_row("status", str(res.status))
    table.add_row("run_key", str(res.run_key))
    table.add_row("items_in", str(getattr(res, "items_in", "")))
    table.add_row("items_out", str(getattr(res, "items_out", "")))
    table.add_row("dry_run", str(dry_run))
    if getattr(res, "error", None):
        table.add_row("error", str(res.error))
    console.print(table)


@app.command()
def failures(
    pipeline_id: str,
    config_dir: Path = typer.Option("./configs", "--config-dir", "-c"),
    retry: str | None = typer.Option(
        None,
        "--retry",
        help="item_key of a dead-letter to re-queue; resets attempts to 0",
    ),
):
    """List failed items, optionally re-queue one."""
    _import_all()
    bundle = load_all(config_dir)

    async def _run():
        from sluice.state.db import open_db
        from sluice.state.failures import FailureStore

        db_path = bundle.pipelines[pipeline_id].state.db_path or bundle.global_cfg.state.db_path
        async with open_db(db_path) as db:
            f = FailureStore(db)
            if retry:
                await db.execute(
                    "UPDATE failed_items SET status='failed', attempts=0 "
                    "WHERE pipeline_id=? AND item_key=?",
                    (pipeline_id, retry),
                )
                await db.commit()
                typer.echo(f"requeued {retry}")
                return
            rows = await f.list(pipeline_id)
            for r in rows:
                typer.echo(
                    f"{r['item_key']}\t{r['status']}\t"
                    f"attempts={r['attempts']}\t"
                    f"{r['error_class']}: {r['error_msg'][:80]}"
                )

    asyncio.run(_run())


@app.command()
def deploy(config_dir: Path = typer.Option("./configs", "--config-dir", "-c")):
    """Register all enabled pipelines as Prefect deployments."""
    _import_all()
    bundle = load_all(config_dir)
    from prefect import serve as prefect_serve
    from prefect.client.schemas.schedules import CronSchedule

    from sluice.flow import build_flow

    deployments = []
    for pid, pipe in bundle.pipelines.items():
        if not pipe.enabled:
            continue
        cron = pipe.cron or bundle.global_cfg.runtime.default_cron
        tz = pipe.timezone or bundle.global_cfg.runtime.timezone
        flow_obj = build_flow(pid)
        dep = flow_obj.to_deployment(
            name=f"sluice-{pid}",
            schedule=CronSchedule(cron=cron, timezone=tz),
            parameters={"config_dir": str(config_dir.resolve())},
        )
        deployments.append(dep)

    if deployments:
        prefect_serve(*deployments)


if __name__ == "__main__":
    app()
