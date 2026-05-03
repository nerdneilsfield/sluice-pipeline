from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sluice.dag.config import TaskConfig
from sluice.dag.cycle import compute_cycle_anchor, make_run_key
from sluice.dag.db import open_active_db
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.validator import validate_task
from sluice.dag.writer import WriteJob, WriterThread


@dataclass(frozen=True)
class RunResult:
    pipeline_name: str
    run_key: str
    status: str
    items_in: int
    items_out: int


async def _ensure_dynamic_tables(task: TaskConfig) -> None:
    async with open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)


async def run_pipeline_stub(
    task: TaskConfig,
    *,
    pipeline_name: str,
    now: datetime | None = None,
    trigger: str = "manual",
) -> RunResult:
    validate_task(task)
    pipeline = next(
        (candidate for candidate in task.pipelines if candidate.name == pipeline_name),
        None,
    )
    if pipeline is None:
        raise ValueError(f"unknown pipeline: {pipeline_name!r}")

    now = now or datetime.now(timezone.utc)
    cycle_anchor = compute_cycle_anchor(
        task.window.anchor,
        task.timezone or "UTC",
        now,
    )
    run_key = make_run_key(task.name, pipeline.name, cycle_anchor)
    started_at = now.isoformat()

    await _ensure_dynamic_tables(task)

    writer = WriterThread(active_path=task.db_path, seen_path=task.seen_db)
    await writer.start()
    try:
        run_id_holder: dict[str, int] = {}

        async def tx1(active_conn, seen_conn) -> None:
            store = PipelinesStore(active_conn)
            run_id_holder["id"] = await store.upsert_running(
                run_key=run_key,
                task=task.name,
                pipeline_name=pipeline.name,
                cycle_anchor=cycle_anchor.isoformat(),
                trigger=trigger,
                started_at=started_at,
            )

        await (await writer.submit(WriteJob(run=tx1)))

        async def tx2(active_conn, seen_conn) -> None:
            store = PipelinesStore(active_conn)
            await store.finalize(
                run_id_holder["id"],
                finished_at=datetime.now(timezone.utc).isoformat(),
                status="success",
                items_in=0,
                items_out=0,
                error_msg=None,
            )

        await (await writer.submit(WriteJob(run=tx2)))
    finally:
        await writer.stop()

    return RunResult(
        pipeline_name=pipeline.name,
        run_key=run_key,
        status="success",
        items_in=0,
        items_out=0,
    )
