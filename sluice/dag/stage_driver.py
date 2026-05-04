from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from sluice.dag.config import PipelineDef, StageDef
from sluice.dag.item import Item
from sluice.dag.stage_context import ItemDrop, ItemError, StageContext
from sluice.dag.stage_executor import get_executor_class

VOLATILE_TYPES: frozenset[str] = frozenset({"sort", "render"})


@dataclass
class DriverResult:
    drops_per_stage: dict[str, list[ItemDrop]] = field(default_factory=dict)
    errors_per_stage: dict[str, list[ItemError]] = field(default_factory=dict)
    skipped_per_stage: dict[str, set[str]] = field(default_factory=dict)
    input_ids_per_stage: dict[str, set[str]] = field(default_factory=dict)
    done_ids_per_stage: dict[str, set[str]] = field(default_factory=dict)
    new_items: list[Item] = field(default_factory=list)
    rendered_output: str | None = None
    final_working_set: list[Item] = field(default_factory=list)


async def drive_stages(
    *,
    task_name: str,
    pipeline: PipelineDef,
    stage_defs: Mapping[str, StageDef],
    run_key: str,
    cycle_anchor: str,
    items: list[Item],
    resume_done: Mapping[str, set[str]],
) -> DriverResult:
    out = DriverResult()
    working_set = list(items)
    excluded: set[str] = set()

    for stage_name in pipeline.stages:
        stage_def = stage_defs[stage_name]
        is_volatile = stage_def.type in VOLATILE_TYPES
        done_for_stage = set() if is_volatile else set(resume_done.get(stage_name, ()))

        stage_items: list[Item] = []
        skipped_ids: set[str] = set()
        for item in working_set:
            if item.is_dropped():
                continue
            item_id = item["item_id"]
            if item_id in excluded:
                continue
            if item_id in done_for_stage:
                skipped_ids.add(item_id)
                continue
            stage_items.append(item)

        input_ids = {item["item_id"] for item in stage_items}
        out.input_ids_per_stage[stage_name] = input_ids
        out.skipped_per_stage[stage_name] = skipped_ids

        original_stage_input_ids = {item["item_id"] for item in stage_items}
        ctx = StageContext(
            task_name=task_name,
            pipeline_name=pipeline.name,
            stage_name=stage_name,
            stage_type=stage_def.type,
            run_key=run_key,
            cycle_anchor=cycle_anchor,
            items=stage_items,
            params=dict(stage_def.params),
        )
        executor = get_executor_class(stage_def.type)()
        result = await executor.execute(ctx)

        out.drops_per_stage[stage_name] = list(result.drops)
        out.errors_per_stage[stage_name] = list(result.errors)
        out.new_items.extend(result.new_items)
        if result.rendered_output is not None:
            out.rendered_output = result.rendered_output

        dropped_here = result.dropped_ids()
        errored_here = result.errored_ids()
        excluded.update(errored_here)
        out.done_ids_per_stage[stage_name] = input_ids - dropped_here - errored_here

        if executor.reorders_items:
            untouched = [
                item
                for item in working_set
                if item["item_id"] not in original_stage_input_ids
            ]
            working_set = stage_items + untouched

        if result.new_items:
            working_set.extend(result.new_items)

    out.final_working_set = working_set
    return out
