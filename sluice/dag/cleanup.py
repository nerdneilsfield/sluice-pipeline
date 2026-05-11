from __future__ import annotations

from datetime import datetime, timezone

from sluice.dag._name import safe_name
from sluice.dag.config import PipelineDef, TaskConfig
from sluice.dag.writer import WriteJob, WriterThread


async def apply_post_run_cleanup(
    *,
    writer: WriterThread,
    task: TaskConfig,
    pipeline: PipelineDef,
    sink_failed: bool,
    working_set_ids: set[str],
) -> int:
    if pipeline.post_run_cleanup in (None, "none"):
        return 0
    if sink_failed:
        return 0

    mode = pipeline.post_run_cleanup
    safe_p = safe_name(pipeline.name)
    upstreams = list(dict.fromkeys((pipeline.upstream_all or []) + (pipeline.upstream_any or [])))
    if not upstreams:
        return 0

    upstream_clauses = " OR ".join(
        f"EXISTS (SELECT 1 FROM pipeline_{safe_name(upstream)} pu "
        "WHERE pu.item_id = items.item_id AND pu.status = 'success')"
        for upstream in upstreams
    )
    working_clause = ""
    params: tuple = ()
    if working_set_ids:
        placeholders = ",".join("?" for _ in working_set_ids)
        working_clause = f"AND items.item_id NOT IN ({placeholders}) "
        params = tuple(working_set_ids)

    sink_pending_clause = ""
    if pipeline.sinks:
        sink_pending_clause = (
            f"AND NOT EXISTS (SELECT 1 FROM pipeline_{safe_p}_sink_pending sp "
            "WHERE sp.item_id = items.item_id) "
        )

    sql_select = (
        "SELECT items.item_id FROM items "
        "WHERE items.dropped = 0 "
        f"AND ({upstream_clauses}) "
        f"AND NOT EXISTS (SELECT 1 FROM pipeline_{safe_p} pp "
        "WHERE pp.item_id = items.item_id) "
        f"{sink_pending_clause}"
        f"{working_clause}"
    )
    out = {"count": 0}
    now = datetime.now(timezone.utc).isoformat()

    async def cleanup_job(active, seen) -> None:  # noqa: ARG001
        cur = await active.execute(sql_select, params)
        ids = [row[0] for row in await cur.fetchall()]
        if not ids:
            return
        for item_id in ids:
            await active.execute(
                f"""
                INSERT INTO pipeline_{safe_p} (
                  item_id, last_run_id, status, processed_at, error_msg
                ) VALUES (
                  ?,
                  (SELECT MAX(id) FROM pipelines WHERE task = ? AND pipeline_name = ?),
                  'dropped',
                  ?,
                  ?
                )
                ON CONFLICT(item_id) DO NOTHING
                """,
                (item_id, task.name, pipeline.name, now, f"abandoned_by_{pipeline.name}"),
            )

        if mode == "drop_unprocessed_upstream_global":
            placeholders = ",".join("?" for _ in ids)
            await active.execute(
                f"UPDATE items SET dropped = 1, dropped_reason = ?, dropped_at = ?, "
                f"updated_at = ? WHERE item_id IN ({placeholders})",
                (f"abandoned_by_{pipeline.name}", now, now, *ids),
            )
        out["count"] = len(ids)

    await (await writer.submit(WriteJob(run=cleanup_job)))
    return out["count"]
