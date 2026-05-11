from __future__ import annotations

import asyncio

from sluice.dag.db import open_active_db


async def count_in_flight(active_path: str, task_name: str) -> tuple[int, int]:
    """Return (running pipelines, admitted pre-tx1 pipelines) for a task."""
    async with open_active_db(active_path) as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM pipelines WHERE task = ? AND status = 'running'",
            (task_name,),
        )
        running = int((await cur.fetchone())[0])
        cur = await conn.execute(
            "SELECT COUNT(*) FROM pipeline_admissions WHERE task = ?",
            (task_name,),
        )
        admitted = int((await cur.fetchone())[0])
        return running, admitted


async def count_running(active_path: str, task_name: str) -> int:
    running, _ = await count_in_flight(active_path, task_name)
    return running


async def drain_running_pipelines(
    active_path: str,
    task_name: str,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 1.0,
) -> bool:
    """Wait until no running/admitted pipelines remain, or return False.

    Plan 3 intentionally has no zombie recovery. A crashed writer can leave a
    running pipeline row or admission row that blocks drain until an operator
    manually marks/deletes it; Plan 4 adds age-based recovery.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_seconds
    while True:
        running, admitted = await count_in_flight(active_path, task_name)
        if running == 0 and admitted == 0:
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(poll_interval_seconds)
