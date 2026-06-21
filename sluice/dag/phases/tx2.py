from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sluice.dag.config import PipelineDef, StageDef
from sluice.dag.item import Item
from sluice.dag.sinks.base import EmissionResult
from sluice.dag.stage_driver import VOLATILE_TYPES, DriverResult
from sluice.dag.stores.pipeline_state import PipelineStateStore
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.stores.sink_pending import SinkPendingStore
from sluice.dag.stores.stage_state import StageStateStore
from sluice.dag.writer import WriteJob


def build_tx2_job(
    *,
    pipeline: PipelineDef,
    stage_defs: dict[str, StageDef],
    pipeline_run_id: int,
    run_key: str,
    items: list[Item],
    driver_result: DriverResult,
    emission_results: list[EmissionResult],
    items_in: int,
    run_level_error: str | None,
    out: dict[str, Any],
    cycle_id: str | None = None,
    lease_owner: str | None = None,
    lease_version: int | None = None,
    runtime_core: str = "v2",
) -> WriteJob:
    async def job(active, seen) -> None:  # noqa: ARG001
        now = datetime.now(timezone.utc).isoformat()
        state = PipelineStateStore(active, pipeline.name)
        pending = SinkPendingStore(active, pipeline.name) if pipeline.sinks else None
        non_volatile_stages = [
            name for name in pipeline.stages if stage_defs[name].type not in VOLATILE_TYPES
        ]
        per_item_status: dict[str, tuple[str, str | None]] = {}

        for item in items:
            item_id = item["item_id"]
            dropped_at_stage = next(
                (
                    stage
                    for stage in non_volatile_stages
                    if item_id
                    in {drop.item_id for drop in driver_result.drops_per_stage.get(stage, ())}
                ),
                None,
            )
            errored_at_stage = next(
                (
                    stage
                    for stage in non_volatile_stages
                    if item_id
                    in {error.item_id for error in driver_result.errors_per_stage.get(stage, ())}
                ),
                None,
            )
            if dropped_at_stage:
                per_item_status[item_id] = ("dropped", None)
            elif errored_at_stage:
                if runtime_core == "v3":
                    attempts = await StageStateStore(active, errored_at_stage).attempts_v3(
                        run_id=pipeline_run_id,
                        pipeline_name=pipeline.name,
                        item_id=item_id,
                    )
                else:
                    attempts = await StageStateStore(active, errored_at_stage).attempts(
                        pipeline_name=pipeline.name,
                        item_id=item_id,
                    )
                if attempts >= pipeline.max_attempts:
                    per_item_status[item_id] = (
                        "failed",
                        f"stage_{errored_at_stage}_max_attempts",
                    )
                else:
                    per_item_status[item_id] = ("transient", None)
            else:
                per_item_status[item_id] = ("eligible_success", None)

        all_sinks_ok = all(result.success for result in emission_results)
        had_emit_attempts = bool(emission_results)
        items_out = 0
        for item_id, (kind, error_msg) in per_item_status.items():
            if kind == "transient":
                continue
            if kind == "dropped":
                if runtime_core == "v3":
                    await state.upsert_v3(
                        run_id=pipeline_run_id, item_id=item_id,
                        cycle_id=cycle_id or "", status="dropped",
                        processed_at=now, error_msg=None,
                    )
                else:
                    await state.upsert(
                        item_id=item_id, last_run_id=pipeline_run_id,
                        status="dropped", processed_at=now, error_msg=None,
                    )
                if pending is not None:
                    await pending.delete_for_items([item_id])
            elif kind == "failed":
                if runtime_core == "v3":
                    await state.upsert_v3(
                        run_id=pipeline_run_id, item_id=item_id,
                        cycle_id=cycle_id or "", status="failed",
                        processed_at=now, error_msg=error_msg,
                    )
                else:
                    await state.upsert(
                        item_id=item_id, last_run_id=pipeline_run_id,
                        status="failed", processed_at=now, error_msg=error_msg,
                    )
                if pending is not None:
                    await pending.delete_for_items([item_id])
            elif pipeline.sinks and not all_sinks_ok:
                assert pending is not None
                await pending.upsert(
                    item_id=item_id, run_key=run_key, pending_since=now,
                )
            else:
                if runtime_core == "v3":
                    await state.upsert_v3(
                        run_id=pipeline_run_id, item_id=item_id,
                        cycle_id=cycle_id or "", status="success",
                        processed_at=now, error_msg=None,
                    )
                else:
                    await state.upsert(
                        item_id=item_id, last_run_id=pipeline_run_id,
                        status="success", processed_at=now, error_msg=None,
                    )
                items_out += 1
                if pending is not None:
                    await pending.delete_for_items([item_id])

        if run_level_error:
            status = "failed"
            error_msg = run_level_error
        else:
            kinds = [kind for kind, _ in per_item_status.values()]
            if not kinds:
                base = "success"
            elif all(kind == "dropped" for kind in kinds):
                base = "success"
            elif all(kind == "eligible_success" for kind in kinds):
                base = "success"
            else:
                base = "partial"

            success_eligible = sum(1 for kind in kinds if kind == "eligible_success")
            error_msg = None
            if pipeline.sinks and had_emit_attempts and success_eligible:
                ok = sum(1 for result in emission_results if result.success)
                bad = sum(1 for result in emission_results if not result.success)
                if bad == 0:
                    status = base
                elif ok == 0:
                    status = "failed"
                    error_msg = (
                        "all_sinks_failed: "
                        + "; ".join(
                            f"{result.sink_id}#{result.ordinal}:{result.error_msg}"
                            for result in emission_results
                            if not result.success
                        )[:500]
                    )
                else:
                    status = "partial" if base == "success" else base
            else:
                status = base

        if runtime_core == "v3" and lease_owner is not None and lease_version is not None:
            # Fenced finalize: only updates if lease still matches. Zero rowcount
            # means drain fenced us; surface that to the caller via status.
            rowcount = await PipelinesStore(active).finalize_v3(
                pipeline_run_id,
                finished_at=now, status=status, items_in=items_in,
                items_out=items_out, error_msg=error_msg,
                lease_owner=lease_owner, lease_version=lease_version,
            )
            if rowcount == 0:
                status = "failed"
                fence_signal = "lease_fenced_during_finalize"
                error_msg = (
                    f"{error_msg}; {fence_signal}" if error_msg else fence_signal
                )
        else:
            await PipelinesStore(active).finalize(
                pipeline_run_id,
                finished_at=now, status=status, items_in=items_in,
                items_out=items_out, error_msg=error_msg,
            )
        out["status"] = status
        out["items_out"] = items_out
        out["error_msg"] = error_msg

    return WriteJob(run=job)
