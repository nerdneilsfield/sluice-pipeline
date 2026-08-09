from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import aiosqlite

from sluice.dag.db import open_active_db

_LOCK_FENCE_VERSION_BUMP = 1


async def count_in_flight(active_path: str, task_name: str) -> tuple[int, int]:
    """Return (running pipelines, admitted pre-tx1 pipelines) for a task."""
    async with open_active_db(active_path) as conn:
        running, admitted = await _count_in_flight(conn, task_name)
        return running, admitted


async def _count_in_flight(
    conn: aiosqlite.Connection, task_name: str
) -> tuple[int, int]:
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


async def _has_v3_columns(conn: aiosqlite.Connection) -> bool:
    """Detect whether pipelines.lease_until exists (i.e. v3 schema applied)."""
    cur = await conn.execute("PRAGMA table_info(pipelines)")
    columns = {row[1] for row in await cur.fetchall()}
    return "lease_until" in columns


async def recover_expired_leases(
    active_path: str,
    task_name: str,
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    """Recover leases that expired before ``now``.

    Two classes of recovery:

    1. Unlinked admissions (run_id IS NULL) past their lease_until: delete.
    2. Linked running pipelines past their lease_until: increment
       lease_version, clear lease_owner, set status='failed' with
       error_msg='lease_expired'. Subsequent fenced writes (heartbeat or
       finalize_v3) from the original owner will return rowcount=0.

    Returns counts of each class recovered. On v2-only DBs (no lease
    columns), returns ``{"unlinked_deleted": 0, "linked_fenced": 0}`` as a
    no-op so callers can run unconditionally.
    """
    now = now or datetime.now(timezone.utc)
    now_iso = now.isoformat()
    async with open_active_db(active_path) as conn:
        if not await _has_v3_columns(conn):
            return {"unlinked_deleted": 0, "linked_fenced": 0}
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                "DELETE FROM pipeline_admissions "
                "WHERE task = ? AND run_id IS NULL "
                "AND (lease_until IS NOT NULL AND lease_until < ?)",
                (task_name, now_iso),
            )
            unlinked_deleted = cur.rowcount or 0

            cur = await conn.execute(
                "UPDATE pipelines "
                "SET status = 'failed', "
                "    error_msg = 'lease_expired', "
                "    finished_at = ?, "
                "    lease_version = lease_version + ?, "
                "    lease_owner = NULL "
                "WHERE task = ? AND status = 'running' "
                "AND lease_until IS NOT NULL AND lease_until < ?",
                (now_iso, _LOCK_FENCE_VERSION_BUMP, task_name, now_iso),
            )
            linked_fenced = cur.rowcount or 0

            if linked_fenced:
                await conn.execute(
                    "DELETE FROM pipeline_admissions "
                    "WHERE task = ? AND run_id IN ("
                    "    SELECT id FROM pipelines WHERE task = ? AND status = 'failed'"
                    ")",
                    (task_name, task_name),
                )
            await conn.commit()
        except BaseException:
            await conn.execute("ROLLBACK")
            raise
    return {"unlinked_deleted": unlinked_deleted, "linked_fenced": linked_fenced}


async def drain_running_pipelines(
    active_path: str,
    task_name: str,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 1.0,
    recover_first: bool = True,
) -> bool:
    """Wait until no running/admitted pipelines remain, or return False.

    With ``recover_first=True`` (default), expired leases are recovered
    once before polling starts. This prevents a single crashed writer
    from blocking drain forever.
    """
    if recover_first:
        await recover_expired_leases(active_path, task_name)

    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_seconds
    while True:
        running, admitted = await count_in_flight(active_path, task_name)
        if running == 0 and admitted == 0:
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(poll_interval_seconds)
