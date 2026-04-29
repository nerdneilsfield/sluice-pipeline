from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import typer
from prefect import serve as prefect_serve
from prefect.client.schemas.schedules import CronSchedule
from rich.console import Console
from rich.table import Table
from tqdm import tqdm

from sluice.loader import load_all
from sluice.logging_setup import configure_cli_logging, create_logger
from sluice.runner import run_pipeline
from sluice.window import parse_duration

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
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show DEBUG logs."),
    log_file: Path | None = typer.Option(
        None,
        "--log-file",
        help="Write DEBUG JSONL diagnostics to this file.",
    ),
):
    """Run a pipeline once."""
    configure_cli_logging(verbose=verbose, log_file=log_file)
    cli_log = create_logger(verbose=verbose)
    _import_all()
    cli_log.bind(config_dir=str(config_dir), pipeline_id=pipeline_id).info("cli.run.load_config")
    bundle = load_all(config_dir)
    if pipeline_id not in bundle.pipelines:
        cli_log.bind(pipeline_id=pipeline_id).error("unknown pipeline")
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
        cli_log.bind(pipeline_id=pipeline_id, dry_run=dry_run).info("cli.run.start")
        res = asyncio.run(
            run_pipeline(bundle, pipeline_id=pipeline_id, dry_run=dry_run, progress=progress)
        )
    finally:
        if progress_bar:
            progress_bar.close()

    _print_step_table(steps)
    _print_run_summary(res, dry_run=dry_run)
    cli_log.bind(
        pipeline_id=pipeline_id,
        status=res.status,
        run_key=res.run_key,
        items_out=res.items_out,
    ).info("cli.run.done")
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

    if bundle.global_cfg.gc.schedule.enabled:
        from sluice.gc_flow import sluice_gc_flow

        gc_dep = sluice_gc_flow.to_deployment(
            name="sluice-gc",
            schedule=CronSchedule(
                cron=bundle.global_cfg.gc.schedule.cron,
                timezone=bundle.global_cfg.runtime.timezone,
            ),
            parameters={"config_dir": str(config_dir.resolve())},
        )
        deployments.append(gc_dep)

    if deployments:
        prefect_serve(*deployments)


@app.command()
def gc(
    config_dir: str = typer.Option("./configs", "--config-dir"),
    older_than: str = typer.Option("90d", help="threshold for failed_items"),
    pipeline: str | None = typer.Option(None),
    tables: str = typer.Option("failed_items,url_cache,attachment_mirror"),
    dry_run: bool = typer.Option(False, "--dry-run"),
):
    """Reclaim space from state tables."""
    from sluice.gc import gc_attachments, gc_failed_items, gc_url_cache
    from sluice.state.db import open_db

    bundle = load_all(Path(config_dir))
    cfg = bundle.global_cfg
    if pipeline:
        if pipeline not in bundle.pipelines:
            typer.echo(f"unknown pipeline: {pipeline}", err=True)
            raise typer.Exit(2)
        db_path = bundle.pipelines[pipeline].state.db_path or cfg.state.db_path
        att_dir = bundle.pipelines[pipeline].state.attachment_dir or cfg.state.attachment_dir
    else:
        db_path = cfg.state.db_path
        att_dir = cfg.state.attachment_dir
    cutoff_iso = (datetime.now(timezone.utc) - parse_duration(older_than)).isoformat(
        timespec="seconds"
    )
    selected = set(tables.split(","))

    async def _run():
        async with open_db(db_path) as db:
            if "failed_items" in selected:
                if dry_run:
                    sql = (
                        "SELECT COUNT(*) FROM failed_items "
                        "WHERE status IN ('resolved','dead_letter') "
                        "AND last_failed_at < ?"
                    )
                    args = [cutoff_iso]
                    if pipeline:
                        sql += " AND pipeline_id = ?"
                        args.append(pipeline)
                    cur = await db.execute(sql, args)
                    n = (await cur.fetchone())[0]
                else:
                    n = await gc_failed_items(db, older_than_iso=cutoff_iso, pipeline=pipeline)
                typer.echo(f"failed_items: {n} {'(would delete)' if dry_run else 'deleted'}")
            if "url_cache" in selected:
                keep_cutoff = (
                    datetime.now(timezone.utc) - parse_duration(cfg.gc.url_cache_keep_expired)
                ).isoformat(timespec="seconds")
                if dry_run:
                    cur = await db.execute(
                        "SELECT COUNT(*) FROM url_cache WHERE expires_at < ?", (keep_cutoff,)
                    )
                    n_expired = (await cur.fetchone())[0]
                    cur = await db.execute(
                        "SELECT COUNT(*) FROM url_cache WHERE expires_at >= ?", (keep_cutoff,)
                    )
                    remaining = (await cur.fetchone())[0]
                    n_size = max(0, remaining - cfg.gc.url_cache_max_rows)
                    n = n_expired + n_size
                else:
                    n = await gc_url_cache(
                        db, max_rows=cfg.gc.url_cache_max_rows, keep_expired_iso=keep_cutoff
                    )
                typer.echo(f"url_cache: {n} {'(would delete)' if dry_run else 'deleted'}")
            if "attachment_mirror" in selected:
                a_cutoff = (
                    datetime.now(timezone.utc)
                    - parse_duration(cfg.gc.attachment_unreferenced_after)
                ).isoformat(timespec="seconds")
                if dry_run:
                    sql = "SELECT COUNT(*) FROM attachment_mirror WHERE last_referenced_at < ?"
                    args = [a_cutoff]
                    if pipeline:
                        sql += " AND pipeline_id = ?"
                        args.append(pipeline)
                    cur = await db.execute(sql, args)
                    n = (await cur.fetchone())[0]
                else:
                    n = await gc_attachments(
                        db, base_dir=Path(att_dir), older_than_iso=a_cutoff, pipeline=pipeline
                    )
                typer.echo(f"attachment_mirror: {n} {'(would delete)' if dry_run else 'deleted'}")

    asyncio.run(_run())


@app.command()
def stats(
    pipeline: str | None = typer.Argument(None),
    config_dir: str = typer.Option("./configs", "--config-dir"),
    since: str = typer.Option("7d"),
    format: str = typer.Option("table"),
):
    """Print run statistics from run_log."""
    import json
    import sqlite3

    from sluice.runner import _resolve_db_path

    cutoff = (datetime.now(timezone.utc) - parse_duration(since)).isoformat()
    bundle = load_all(Path(config_dir))
    cfg = bundle.global_cfg
    if pipeline:
        if pipeline not in bundle.pipelines:
            typer.echo(f"unknown pipeline: {pipeline}", err=True)
            raise typer.Exit(2)
        db_path = _resolve_db_path(cfg, bundle.pipelines[pipeline])
    else:
        db_path = cfg.state.db_path
    sql = (
        "SELECT pipeline_id, COUNT(*) AS runs, "
        " 100.0 * SUM(CASE status WHEN 'success' THEN 1 ELSE 0 END)/COUNT(*) AS success_pct, "
        " AVG(items_in), AVG(items_out), SUM(llm_calls), SUM(est_cost_usd) "
        "FROM run_log WHERE started_at >= ?"
    )
    args = [cutoff]
    if pipeline:
        sql += " AND pipeline_id = ?"
        args.append(pipeline)
    sql += " GROUP BY pipeline_id"
    with sqlite3.connect(db_path) as c:
        rows = c.execute(sql, args).fetchall()
    if format == "json":
        keys = [
            "pipeline_id",
            "runs",
            "success_pct",
            "avg_items_in",
            "avg_items_out",
            "llm_calls",
            "cost_usd",
        ]
        typer.echo(json.dumps([dict(zip(keys, r)) for r in rows], indent=2))
    else:
        for r in rows:
            typer.echo(" | ".join(str(x) for x in r))


@app.command("metrics-server")
def metrics_server(
    host: str = "0.0.0.0",
    port: int = 9090,
    config_dir: str = typer.Option("./configs", "--config-dir"),
):
    """Start Prometheus exposition server."""
    import time

    from prometheus_client import REGISTRY, start_http_server

    from sluice.metrics import SluiceCollector

    bundle = load_all(Path(config_dir))
    cfg = bundle.global_cfg
    overrides = [
        pid
        for pid, p in bundle.pipelines.items()
        if p.state.db_path is not None and p.state.db_path != cfg.state.db_path
    ]
    if overrides:
        typer.echo(
            f"WARNING: pipelines {overrides} override state.db_path; "
            f"their metrics will NOT be exposed by this server "
            f"(global DB only).",
            err=True,
        )
    REGISTRY.register(SluiceCollector(cfg.state.db_path))
    start_http_server(port, addr=host)
    typer.echo(f"metrics: http://{host}:{port}/metrics")
    while True:
        time.sleep(3600)


@app.command()
def deliveries(
    pipeline_id: str,
    config_dir: str = typer.Option("./configs", "--config-dir"),
    run: str | None = typer.Option(None, "--run"),
    status: str | None = typer.Option(None, "--status"),
    since: str = typer.Option("7d"),
):
    """List delivery audit trail for a pipeline."""
    import sqlite3

    from sluice.runner import _resolve_db_path

    bundle = load_all(Path(config_dir))
    cfg = bundle.global_cfg
    if pipeline_id not in bundle.pipelines:
        typer.echo(f"unknown pipeline: {pipeline_id}", err=True)
        raise typer.Exit(2)
    db_path = _resolve_db_path(cfg, bundle.pipelines[pipeline_id])
    since_cutoff = (datetime.now(timezone.utc) - parse_duration(since)).isoformat(
        timespec="seconds"
    )
    sql = (
        "SELECT attempt_at, sink_id, sink_type, ordinal, message_kind, "
        " recipient, status, error_msg "
        "FROM sink_delivery_log "
        "WHERE pipeline_id = ? AND attempt_at >= ?"
    )
    args = [pipeline_id, since_cutoff]
    if run:
        sql += " AND run_key = ?"
        args.append(run)
    if status:
        sql += " AND status = ?"
        args.append(status)
    sql += " ORDER BY attempt_at DESC LIMIT 200"
    with sqlite3.connect(db_path) as c:
        for row in c.execute(sql, args):
            typer.echo(" | ".join(str(x) if x is not None else "-" for x in row))


if __name__ == "__main__":
    app()
