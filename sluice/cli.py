import asyncio
from pathlib import Path
import typer
from sluice.loader import load_all
from sluice.runner import run_pipeline

app = typer.Typer(no_args_is_help=True)

def _import_all():
    from sluice.sources import rss  # noqa
    from sluice.fetchers import trafilatura_fetcher, firecrawl, jina_reader  # noqa
    from sluice.processors import (
        dedupe, filter as _f, field_filter, fetcher_apply, llm_stage, render,
    )  # noqa
    from sluice.sinks import file_md, notion  # noqa

@app.command()
def validate(config_dir: Path = typer.Option("./configs",
                                              "--config-dir", "-c")):
    """Validate all TOML config files."""
    _import_all()
    bundle = load_all(config_dir)
    typer.echo(f"OK: {len(bundle.pipelines)} pipeline(s) loaded")

@app.command()
def list(config_dir: Path = typer.Option("./configs",
                                          "--config-dir", "-c")):
    """List configured pipelines."""
    _import_all()
    bundle = load_all(config_dir)
    for pid, pipe in bundle.pipelines.items():
        cron = pipe.cron or bundle.global_cfg.runtime.default_cron
        typer.echo(f"{pid}\t{cron}\tenabled={pipe.enabled}")

@app.command()
def run(pipeline_id: str,
        config_dir: Path = typer.Option("./configs", "--config-dir", "-c"),
        dry_run: bool = typer.Option(False, "--dry-run")):
    """Run a pipeline once."""
    _import_all()
    bundle = load_all(config_dir)
    if pipeline_id not in bundle.pipelines:
        typer.echo(f"unknown pipeline: {pipeline_id}", err=True)
        raise typer.Exit(2)
    res = asyncio.run(run_pipeline(bundle, pipeline_id=pipeline_id,
                                   dry_run=dry_run))
    typer.echo(f"{res.status}\t{res.run_key}\titems_out={res.items_out}"
               + (f"\terror={res.error}" if res.error else ""))
    raise typer.Exit(0 if res.status == "success" else 1)

@app.command()
def failures(pipeline_id: str,
             config_dir: Path = typer.Option("./configs", "--config-dir", "-c"),
             retry: str | None = typer.Option(None, "--retry",
                 help="item_key of a dead-letter to re-queue")):
    """List failed items, optionally re-queue one."""
    _import_all()
    bundle = load_all(config_dir)
    async def _run():
        from sluice.state.db import open_db
        from sluice.state.failures import FailureStore
        db_path = bundle.pipelines[pipeline_id].state.db_path \
            or bundle.global_cfg.state.db_path
        async with open_db(db_path) as db:
            f = FailureStore(db)
            if retry:
                await db.execute(
                    "UPDATE failed_items SET status='failed', attempts=0 "
                    "WHERE pipeline_id=? AND item_key=?",
                    (pipeline_id, retry))
                await db.commit()
                typer.echo(f"requeued {retry}")
                return
            rows = await f.list(pipeline_id)
            for r in rows:
                typer.echo(f"{r['item_key']}\t{r['status']}\t"
                           f"attempts={r['attempts']}\t"
                           f"{r['error_class']}: {r['error_msg'][:80]}")
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
