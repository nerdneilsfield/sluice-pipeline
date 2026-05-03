from __future__ import annotations

import re

from sluice.dag.config import PipelineDef

_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def _safe(name: str) -> str:
    if _NAME_RE.fullmatch(name) is None:
        raise ValueError(f"unsafe pipeline name: {name!r}")
    return name


async def compute_skip_ids(active, seen, pipeline: PipelineDef) -> set[str]:
    if not pipeline.sources:
        return set()
    safe_p = _safe(pipeline.name)
    skip: set[str] = set()
    cur = await active.execute(
        f"SELECT item_id FROM pipeline_{safe_p} "
        "WHERE status IN ('success','dropped','failed')"
    )
    skip |= {row[0] for row in await cur.fetchall()}
    cur = await active.execute("SELECT item_id FROM items WHERE dropped = 1")
    skip |= {row[0] for row in await cur.fetchall()}
    cur = await seen.execute("SELECT item_id FROM item_seen")
    skip |= {row[0] for row in await cur.fetchall()}
    return skip
