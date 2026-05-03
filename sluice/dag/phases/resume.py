from __future__ import annotations

from sluice.dag.stage_driver import VOLATILE_TYPES
from sluice.dag.stores.stage_state import StageStateStore


async def load_resume_done(
    active,
    *,
    pipeline_name: str,
    stage_names: list[str],
    stage_types: dict[str, str],
    item_ids: list[str],
) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    if not item_ids:
        return out
    for stage_name in stage_names:
        if stage_types[stage_name] in VOLATILE_TYPES:
            continue
        out[stage_name] = await StageStateStore(active, stage_name).done_item_ids(
            pipeline_name=pipeline_name,
            item_ids=item_ids,
        )
    return out
