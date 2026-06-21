from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

import sluice.dag.stages  # noqa: F401
from sluice.dag.cleanup import apply_post_run_cleanup
from sluice.dag.config import StageDef, TaskConfig
from sluice.dag.cycle import compute_cycle_anchor, make_run_key
from sluice.dag.db import open_active_db, open_seen_db
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables
from sluice.dag.eligibility import (
    evaluate_run_gating,
    select_eligible_items,
    select_entry_carryover,
)
from sluice.dag.emission import SinkEmitter
from sluice.dag.item import Item
from sluice.dag.phases.resume import load_resume_done
from sluice.dag.phases.run_key_resolve import resolve_run_key
from sluice.dag.phases.seen_post import build_seen_post_job
from sluice.dag.phases.skip_compute import compute_skip_ids
from sluice.dag.phases.tx0 import build_tx0_job, make_lease_owner
from sluice.dag.phases.tx1 import build_tx1_job
from sluice.dag.phases.tx2 import build_tx2_job
from sluice.dag.rotation.admission import (
    RotationGateClosed,
    build_admission_job,
    build_deadmission_job,
)
from sluice.dag.sinks.base import get_sink_class
from sluice.dag.stage_driver import drive_stages
from sluice.dag.stores.item_seen import ItemSeenStore
from sluice.dag.stores.sink_pending import SinkPendingStore
from sluice.dag.validator import validate_task
from sluice.dag.writer import WriterThread

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    pipeline_name: str
    run_key: str
    status: str
    items_in: int
    items_out: int
    error_msg: str | None = None


async def ensure_dynamic_tables(task: TaskConfig) -> None:
    async with open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
    # Prime seen migrations before the writer starts; the writer may commit active
    # and seen independently, so startup DDL should not happen inside a run phase.
    async with open_seen_db(task.seen_db):
        pass


def _override_stage_params(
    stage_defs: dict[str, StageDef], *, overrides_by_type: dict[str, dict]
) -> dict[str, StageDef]:
    if not overrides_by_type:
        return stage_defs
    out = dict(stage_defs)
    for name, stage_def in stage_defs.items():
        extra = overrides_by_type.get(stage_def.type)
        if extra:
            out[name] = replace(stage_def, params={**stage_def.params, **extra})
    return out


def _window_delta(length: str) -> timedelta:
    text = length.strip().lower()
    if text.endswith("h"):
        return timedelta(hours=float(text[:-1]))
    if text.endswith("d"):
        return timedelta(days=float(text[:-1]))
    if text.endswith("m"):
        return timedelta(minutes=float(text[:-1]))
    return timedelta(hours=24)


async def run_pipeline(
    task: TaskConfig,
    pipeline_name: str,
    *,
    writer: WriterThread,
    now: datetime | None = None,
    trigger: str = "manual",
    runtime_core: str = "v2",
    lease_duration_seconds: float = 30 * 60.0,
) -> RunResult:
    validate_task(task)
    pipeline = next(
        (candidate for candidate in task.pipelines if candidate.name == pipeline_name),
        None,
    )
    if pipeline is None:
        raise ValueError(f"unknown pipeline: {pipeline_name!r}")

    admission_id = str(uuid.uuid4())
    try:
        await (
            await writer.submit(
                build_admission_job(
                    admission_id=admission_id,
                    task_name=task.name,
                    pipeline_name=pipeline.name,
                )
            )
        )
    except RotationGateClosed as exc:
        return RunResult(
            pipeline_name=pipeline.name,
            run_key="",
            status="skipped_rotation",
            items_in=0,
            items_out=0,
            error_msg=str(exc),
        )

    try:
        return await _run_pipeline_admitted(
            task,
            pipeline_name,
            writer=writer,
            now=now,
            trigger=trigger,
            runtime_core=runtime_core,
            lease_duration_seconds=lease_duration_seconds,
        )
    finally:
        try:
            await (await writer.submit(build_deadmission_job(admission_id)))
        except Exception as exc:  # noqa: BLE001
            logger.warning("deadmission failed for %s: %s", admission_id, exc)


async def _run_pipeline_admitted(
    task: TaskConfig,
    pipeline_name: str,
    *,
    writer: WriterThread,
    now: datetime | None = None,
    trigger: str = "manual",
    runtime_core: str = "v2",
    lease_duration_seconds: float = 30 * 60.0,
) -> RunResult:
    validate_task(task)
    pipeline = next(
        (candidate for candidate in task.pipelines if candidate.name == pipeline_name),
        None,
    )
    if pipeline is None:
        raise ValueError(f"unknown pipeline: {pipeline_name!r}")

    stage_defs: dict[str, StageDef] = {stage.name: stage for stage in task.stages}
    stage_types = {name: stage_def.type for name, stage_def in stage_defs.items()}

    now = now or datetime.now(timezone.utc)
    cycle_anchor = compute_cycle_anchor(task.window.anchor, task.timezone or "UTC", now)
    fresh_run_key = make_run_key(task.name, pipeline.name, cycle_anchor)
    started_at = now.isoformat()

    # v3: create the run row with lease BEFORE stages run so drain can see us.
    tx0_out: dict = {}
    lease_owner = ""
    lease_version = 0
    if runtime_core == "v3":
        lease_owner = make_lease_owner()
        await (
            await writer.submit(
                build_tx0_job(
                    task=task,
                    pipeline=pipeline,
                    run_key=fresh_run_key,
                    cycle_anchor=cycle_anchor.isoformat(),
                    trigger=trigger,
                    started_at=started_at,
                    lease_owner=lease_owner,
                    lease_duration_seconds=lease_duration_seconds,
                    out=tx0_out,
                )
            )
        )
        lease_version = tx0_out.get("lease_version", 0)

    skip_ids: set[str] = set()
    rows: list[dict] = []
    items_carryover: list[dict] = []
    deferred_ids: list[str] = []
    already_seen_ids: set[str] = set()
    pending_item_ids: set[str] = set()
    effective_run_key = fresh_run_key

    async with open_active_db(task.db_path) as conn_pre:
        gating = await evaluate_run_gating(conn_pre, pipeline)
        if not gating.proceed:
            return RunResult(
                pipeline_name=pipeline.name,
                run_key=fresh_run_key,
                status="skipped_gating",
                items_in=0,
                items_out=0,
                error_msg=gating.reason,
            )

        effective_run_key, deferred_ids = await resolve_run_key(
            conn_pre, pipeline.name, fresh_run_key
        )
        if pipeline.sinks:
            pending_item_ids = await SinkPendingStore(conn_pre, pipeline.name).items_for_run_key(
                effective_run_key
            )

        if pipeline.upstream_all or pipeline.upstream_any:
            rows = await select_eligible_items(conn_pre, pipeline)
        else:
            async with open_seen_db(task.seen_db) as conn_seen:
                skip_ids = await compute_skip_ids(conn_pre, conn_seen, pipeline)
            items_carryover = await select_entry_carryover(conn_pre, pipeline)

        deferred = set(deferred_ids)
        rows = [row for row in rows if row["item_id"] not in deferred]
        items_carryover = [row for row in items_carryover if row["item_id"] not in deferred]
        items = [Item.from_row(row) for row in rows + items_carryover]

        carryover_ids = {row["item_id"] for row in items_carryover}
        skip_ids |= carryover_ids

        if any(stage_types.get(stage_name) == "dedupe" for stage_name in pipeline.stages):
            ids_to_check = {item["item_id"] for item in items}
            async with open_seen_db(task.seen_db) as conn_seen:
                already_seen_ids = await ItemSeenStore(conn_seen).filter_seen(ids_to_check)
            already_seen_ids -= pending_item_ids

        resume_done = await load_resume_done(
            conn_pre,
            pipeline_name=pipeline.name,
            stage_names=pipeline.stages,
            stage_types=stage_types,
            item_ids=[item["item_id"] for item in items],
        )

    overrides: dict[str, dict] = {}
    if pipeline.sources:
        source_defs = {source.name: source for source in task.sources}
        feeds = []
        for source_name in pipeline.sources:
            source_def = source_defs[source_name]
            feeds.append({**source_def.params, "name": source_def.name})
        overrides["fetch_rss"] = {
            "feeds": feeds,
            "window_start": (cycle_anchor - _window_delta(task.window.length)).isoformat(),
            "window_end": cycle_anchor.isoformat(),
            "skip_item_ids": frozenset(skip_ids),
        }
    if already_seen_ids:
        overrides["dedupe"] = {"already_seen_ids": frozenset(already_seen_ids)}
    if task.task_dir:
        overrides["render"] = {"config_root": task.task_dir}
    stage_defs = _override_stage_params(stage_defs, overrides_by_type=overrides)

    driver_result = await drive_stages(
        task_name=task.name,
        pipeline=pipeline,
        stage_defs=stage_defs,
        run_key=effective_run_key,
        cycle_anchor=cycle_anchor.isoformat(),
        items=items,
        resume_done=resume_done,
    )
    all_items = driver_result.final_working_set

    tx1_out: dict = {}
    await (
        await writer.submit(
            build_tx1_job(
                task=task,
                pipeline=pipeline,
                stage_defs=stage_defs,
                run_key=effective_run_key,
                cycle_anchor=cycle_anchor.isoformat(),
                started_at=started_at,
                trigger=trigger,
                items=all_items,
                driver_result=driver_result,
                out=tx1_out,
                pipeline_run_id=tx0_out.get("pipeline_run_id"),
                cycle_id=tx0_out.get("cycle_id"),
                runtime_core=runtime_core,
            )
        )
    )
    pipeline_run_id = tx1_out["pipeline_run_id"]
    items_in = tx1_out["items_in"]

    async def finalize_tx1_error(exc: Exception) -> RunResult:
        tx2_out: dict = {}
        await (
            await writer.submit(
                build_tx2_job(
                    pipeline=pipeline,
                    stage_defs=stage_defs,
                    pipeline_run_id=pipeline_run_id,
                    run_key=effective_run_key,
                    items=all_items,
                    driver_result=driver_result,
                    emission_results=[],
                    items_in=items_in,
                    run_level_error=str(exc),
                    out=tx2_out,
                    cycle_id=tx0_out.get("cycle_id"),
                    lease_owner=lease_owner or None,
                    lease_version=lease_version or None,
                    runtime_core=runtime_core,
                )
            )
        )
        return RunResult(
            pipeline_name=pipeline.name,
            run_key=effective_run_key,
            status=tx2_out["status"],
            items_in=items_in,
            items_out=tx2_out["items_out"],
            error_msg=tx2_out["error_msg"],
        )

    emission_results = []
    try:
        if driver_result.new_items:
            await (await writer.submit(build_seen_post_job(driver_result.new_items)))

        if pipeline.sinks:
            emitter = SinkEmitter(
                writer=writer,
                run_key=effective_run_key,
                pipeline_name=pipeline.name,
            )
            for sink_id in pipeline.sinks:
                sink_def = next(sink for sink in task.sinks if sink.name == sink_id)
                sink = get_sink_class(sink_def.type)()
                sink_params = dict(sink_def.params)
                sink_params.update(
                    task=task.name,
                    pipeline=pipeline.name,
                    run_key=effective_run_key,
                    cycle_anchor=cycle_anchor.isoformat(),
                    sink_id=sink_id,
                    config_root=task.task_dir,
                )
                emission_results.extend(
                    await emitter.emit_all(
                        sink_id=sink_id,
                        sink_type=sink_def.type,
                        sink=sink,
                        rendered=driver_result.rendered_output,
                        params=sink_params,
                    )
                )
    except Exception as exc:  # noqa: BLE001
        logger.warning("post-tx1 pipeline phase failed for %s: %s", pipeline.name, exc)
        return await finalize_tx1_error(exc)

    tx2_out: dict = {}
    await (
        await writer.submit(
            build_tx2_job(
                pipeline=pipeline,
                stage_defs=stage_defs,
                pipeline_run_id=pipeline_run_id,
                run_key=effective_run_key,
                items=all_items,
                driver_result=driver_result,
                emission_results=emission_results,
                items_in=items_in,
                run_level_error=None,
                out=tx2_out,
                cycle_id=tx0_out.get("cycle_id"),
                lease_owner=lease_owner or None,
                lease_version=lease_version or None,
                runtime_core=runtime_core,
            )
        )
    )

    if pipeline.post_run_cleanup not in (None, "none"):
        sink_failed = bool(pipeline.sinks) and any(
            not result.success for result in emission_results
        )
        try:
            await apply_post_run_cleanup(
                writer=writer,
                task=task,
                pipeline=pipeline,
                sink_failed=sink_failed,
                working_set_ids={item["item_id"] for item in all_items},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("post-run cleanup failed for %s: %s", pipeline.name, exc)
            return await finalize_tx1_error(exc)

    return RunResult(
        pipeline_name=pipeline.name,
        run_key=effective_run_key,
        status=tx2_out["status"],
        items_in=items_in,
        items_out=tx2_out["items_out"],
        error_msg=tx2_out["error_msg"],
    )
