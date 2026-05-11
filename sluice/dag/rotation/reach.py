from __future__ import annotations

from collections.abc import Iterable

from sluice.dag.config import TaskConfig
from sluice.dag.validator import compute_reach


def compute_item_reach(
    task: TaskConfig, items_rows: Iterable[dict]
) -> dict[str, frozenset[str]]:
    pipeline_names = {pipeline.name for pipeline in task.pipelines}
    cache: dict[str, frozenset[str]] = {}
    out: dict[str, frozenset[str]] = {}

    for row in items_rows:
        item_id = row["item_id"]
        entry = row.get("ingested_by")
        if entry not in pipeline_names:
            out[item_id] = frozenset()
            continue
        if entry not in cache:
            cache[entry] = frozenset(compute_reach(task, entry))
        out[item_id] = cache[entry]

    return out
