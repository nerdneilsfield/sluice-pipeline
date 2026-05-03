from __future__ import annotations

import re
from dataclasses import dataclass

import aiosqlite

from sluice.dag.config import PipelineDef

_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")

_ITEM_COLS = (
    "item_id",
    "source_name",
    "guid",
    "url",
    "title",
    "author",
    "published_at",
    "ingested_at",
    "ingested_by",
    "updated_at",
    "tag",
    "raw_summary",
    "raw_content",
    "raw_json",
    "fulltext",
    "attachments_json",
    "summary",
    "score",
    "tags_json",
    "extras_json",
    "dropped",
    "dropped_reason",
    "dropped_at",
)
_ITEM_COLS_SQL = ", ".join(f"items.{col}" for col in _ITEM_COLS)
_TERMINAL = "'success','dropped','failed'"


def _safe(name: str) -> str:
    if _NAME_RE.fullmatch(name) is None:
        raise ValueError(f"unsafe pipeline name: {name!r}")
    return name


@dataclass(frozen=True)
class RunGatingDecision:
    proceed: bool
    reason: str = ""


async def evaluate_run_gating(
    conn: aiosqlite.Connection, pipeline: PipelineDef
) -> RunGatingDecision:
    upstream_all = pipeline.upstream_all or []
    upstream_any = pipeline.upstream_any or []
    if not upstream_all and not upstream_any:
        return RunGatingDecision(proceed=True)

    cur = await conn.execute(
        "SELECT MAX(cycle_anchor) FROM pipelines "
        "WHERE pipeline_name = ? AND status = 'success'",
        (pipeline.name,),
    )
    row = await cur.fetchone()
    p_last = row[0] if row else None

    async def has_success(upstream: str) -> bool:
        if p_last is None:
            cur_inner = await conn.execute(
                "SELECT 1 FROM pipelines "
                "WHERE pipeline_name = ? AND status = 'success' LIMIT 1",
                (upstream,),
            )
        else:
            cur_inner = await conn.execute(
                "SELECT 1 FROM pipelines "
                "WHERE pipeline_name = ? AND status = 'success' "
                "AND cycle_anchor >= ? LIMIT 1",
                (upstream, p_last),
            )
        return await cur_inner.fetchone() is not None

    for upstream in upstream_all:
        if not await has_success(upstream):
            return RunGatingDecision(
                False, f"upstream_all: {upstream} has no qualifying success"
            )

    if upstream_any:
        for upstream in upstream_any:
            if await has_success(upstream):
                break
        else:
            return RunGatingDecision(False, "upstream_any: none have a qualifying success")

    return RunGatingDecision(proceed=True)


async def select_eligible_items(
    conn: aiosqlite.Connection, pipeline: PipelineDef
) -> list[dict]:
    upstreams = list(pipeline.upstream_all or []) + list(pipeline.upstream_any or [])
    if not upstreams:
        return []

    safe_p = _safe(pipeline.name)
    if pipeline.input_mode == "intersection":
        sources = pipeline.upstream_all or []
        joiner = " AND "
    else:
        sources = list(dict.fromkeys(upstreams))
        joiner = " OR "
    if not sources:
        return []

    upstream_clauses = [
        f"EXISTS (SELECT 1 FROM pipeline_{_safe(upstream)} pu "
        "WHERE pu.item_id = items.item_id AND pu.status = 'success')"
        for upstream in sources
    ]
    sql = (
        f"SELECT {_ITEM_COLS_SQL} FROM items "
        "WHERE items.dropped = 0 "
        f"AND ({joiner.join(upstream_clauses)}) "
        f"AND NOT EXISTS (SELECT 1 FROM pipeline_{safe_p} pp "
        f"WHERE pp.item_id = items.item_id AND pp.status IN ({_TERMINAL}))"
    )
    cur = await conn.execute(sql)
    rows = await cur.fetchall()
    return [dict(zip(_ITEM_COLS, row, strict=True)) for row in rows]


async def select_entry_carryover(
    conn: aiosqlite.Connection, pipeline: PipelineDef
) -> list[dict]:
    if not pipeline.sources:
        return []
    safe_p = _safe(pipeline.name)
    placeholders = ",".join("?" for _ in pipeline.sources)
    sql = (
        f"SELECT {_ITEM_COLS_SQL} FROM items "
        "WHERE items.dropped = 0 "
        f"AND items.source_name IN ({placeholders}) "
        f"AND NOT EXISTS (SELECT 1 FROM pipeline_{safe_p} pp "
        f"WHERE pp.item_id = items.item_id AND pp.status IN ({_TERMINAL}))"
    )
    cur = await conn.execute(sql, tuple(pipeline.sources))
    rows = await cur.fetchall()
    return [dict(zip(_ITEM_COLS, row, strict=True)) for row in rows]
