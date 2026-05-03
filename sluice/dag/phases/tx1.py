from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sluice.dag.config import PipelineDef, StageDef, TaskConfig
from sluice.dag.item import Item
from sluice.dag.stage_driver import VOLATILE_TYPES, DriverResult
from sluice.dag.stores.items import _UPDATABLE, ItemsStore
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.stores.stage_attempt_log import StageAttemptLogStore
from sluice.dag.stores.stage_state import StageStateStore
from sluice.dag.stores.stages import StagesStore
from sluice.dag.writer import WriteJob


def build_tx1_job(
    *,
    task: TaskConfig,
    pipeline: PipelineDef,
    stage_defs: dict[str, StageDef],
    run_key: str,
    cycle_anchor: str,
    started_at: str,
    trigger: str,
    items: list[Item],
    driver_result: DriverResult,
    out: dict[str, Any],
) -> WriteJob:
    async def job(active, seen) -> None:  # noqa: ARG001
        run_id = await PipelinesStore(active).upsert_running(
            run_key=run_key,
            task=task.name,
            pipeline_name=pipeline.name,
            cycle_anchor=cycle_anchor,
            trigger=trigger,
            started_at=started_at,
        )
        now = datetime.now(timezone.utc).isoformat()
        items_store = ItemsStore(active)

        for item in driver_result.new_items:
            await items_store.insert_new(
                item_id=item["item_id"],
                source_name=item["source_name"],
                guid=item["guid"],
                url=item["url"],
                title=item["title"],
                author=item["author"],
                published_at=item["published_at"],
                raw_json=item["raw_json"] or "{}",
                ingested_at=item["ingested_at"],
                ingested_by=item["ingested_by"],
                tag=item["tag"],
                raw_summary=item["raw_summary"],
                raw_content=item["raw_content"],
            )

        stages_store = StagesStore(active)
        log_store = StageAttemptLogStore(active)
        for stage_name in pipeline.stages:
            stage_def = stage_defs[stage_name]
            if stage_def.type in VOLATILE_TYPES:
                continue
            stage_run_id = await stages_store.start(
                pipeline_run_id=run_id,
                stage_name=stage_name,
                started_at=started_at,
            )
            drops = {
                drop.item_id: drop
                for drop in driver_result.drops_per_stage.get(stage_name, ())
            }
            errors = {
                error.item_id: error
                for error in driver_result.errors_per_stage.get(stage_name, ())
            }
            skipped = driver_result.skipped_per_stage.get(stage_name, set())
            done_ids = driver_result.done_ids_per_stage.get(stage_name, set())
            state_store = StageStateStore(active, stage_name)

            for item_id in done_ids | set(drops) | set(errors):
                if item_id in done_ids:
                    status, error_class, error_msg = "done", None, None
                elif item_id in drops:
                    status, error_class, error_msg = "dropped", None, drops[item_id].reason
                else:
                    error = errors[item_id]
                    status = "failed"
                    error_class, error_msg = error.error_class, error.error_msg

                attempt_no = await log_store.next_attempt_no(
                    pipeline_name=pipeline.name,
                    stage_name=stage_name,
                    item_id=item_id,
                )
                await log_store.append(
                    stage_run_id=stage_run_id,
                    pipeline_run_id=run_id,
                    pipeline_name=pipeline.name,
                    stage_name=stage_name,
                    item_id=item_id,
                    attempt_no=attempt_no,
                    status=status,
                    processed_at=now,
                    error_class=error_class,
                    error_msg=error_msg,
                )
                prior = await state_store.attempts(
                    pipeline_name=pipeline.name,
                    item_id=item_id,
                )
                attempts = prior + 1 if status == "failed" else prior
                if status in ("done", "dropped") and attempts == 0:
                    attempts = 1
                await state_store.upsert(
                    pipeline_name=pipeline.name,
                    item_id=item_id,
                    status=status,
                    attempts=attempts,
                    last_stage_run_id=stage_run_id,
                    last_pipeline_run_id=run_id,
                    last_processed_at=now,
                    last_error_class=error_class,
                    last_error_msg=error_msg,
                )

            for item_id in skipped:
                attempt_no = await log_store.next_attempt_no(
                    pipeline_name=pipeline.name,
                    stage_name=stage_name,
                    item_id=item_id,
                )
                await log_store.append(
                    stage_run_id=stage_run_id,
                    pipeline_run_id=run_id,
                    pipeline_name=pipeline.name,
                    stage_name=stage_name,
                    item_id=item_id,
                    attempt_no=attempt_no,
                    status="skipped",
                    processed_at=now,
                    error_class=None,
                    error_msg=None,
                )

            await stages_store.finalize(
                stage_run_id,
                finished_at=now,
                status="success",
                items_in=len(done_ids) + len(drops) + len(errors),
                items_out=len(done_ids),
                error_msg=None,
            )

        for item in items:
            updatable = {
                key: value
                for key, value in item.to_dirty_dict().items()
                if key in _UPDATABLE
            }
            if updatable:
                await items_store.update_columns(
                    item["item_id"], updatable, updated_at=now
                )

        out["pipeline_run_id"] = run_id
        out["items_in"] = len(items)

    return WriteJob(run=job)
