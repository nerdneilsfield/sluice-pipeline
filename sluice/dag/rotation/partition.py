from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import aiosqlite

from sluice.dag._name import safe_name
from sluice.dag.config import TaskConfig
from sluice.dag.rotation.reach import compute_item_reach


@dataclass(frozen=True)
class Partition:
    carryover_ids: set[str]
    archive_ids: set[str]
    item_reach: dict[str, frozenset[str]]


async def partition_items(conn: aiosqlite.Connection, task: TaskConfig) -> Partition:
    cur = await conn.execute("SELECT item_id, ingested_by, dropped FROM items")
    item_rows = [
        {"item_id": row[0], "ingested_by": row[1], "dropped": int(row[2])}
        for row in await cur.fetchall()
    ]
    item_reach = compute_item_reach(task, item_rows)

    terminal_by_pipeline: dict[str, set[str]] = {}
    for pipeline in task.pipelines:
        safe_pipeline = safe_name(pipeline.name)
        try:
            cur = await conn.execute(
                f"SELECT item_id FROM pipeline_{safe_pipeline} "
                "WHERE status IN ('success','dropped','failed')"
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise
            terminal_by_pipeline[pipeline.name] = set()
            continue
        terminal_by_pipeline[pipeline.name] = {row[0] for row in await cur.fetchall()}

    carryover: set[str] = set()
    archive: set[str] = set()
    for row in item_rows:
        item_id = row["item_id"]
        if row["dropped"] == 1:
            archive.add(item_id)
            continue
        reach = item_reach[item_id]
        if not reach:
            archive.add(item_id)
            continue
        if all(item_id in terminal_by_pipeline[pipeline] for pipeline in reach):
            archive.add(item_id)
        else:
            carryover.add(item_id)

    return Partition(carryover_ids=carryover, archive_ids=archive, item_reach=item_reach)
