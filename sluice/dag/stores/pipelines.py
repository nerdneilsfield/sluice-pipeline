from __future__ import annotations

import aiosqlite


class PipelinesStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def get_run_id(self, run_key: str) -> int | None:
        cur = await self.conn.execute(
            "SELECT id FROM pipelines WHERE run_key = ?",
            (run_key,),
        )
        row = await cur.fetchone()
        return None if row is None else int(row[0])

    async def upsert_running(
        self,
        *,
        run_key: str,
        task: str,
        pipeline_name: str,
        cycle_anchor: str,
        trigger: str,
        started_at: str,
    ) -> int:
        existing = await self.get_run_id(run_key)
        if existing is None:
            cur = await self.conn.execute(
                """
                INSERT INTO pipelines (
                  run_key, task, pipeline_name, cycle_anchor,
                  trigger, started_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, 'running')
                """,
                (run_key, task, pipeline_name, cycle_anchor, trigger, started_at),
            )
            return int(cur.lastrowid)

        await self.conn.execute(
            """
            UPDATE pipelines
            SET status = 'running',
                started_at = ?,
                finished_at = NULL,
                items_in = NULL,
                items_out = NULL,
                error_msg = NULL
            WHERE id = ?
            """,
            (started_at, existing),
        )
        return existing

    async def finalize(
        self,
        run_id: int,
        *,
        finished_at: str,
        status: str,
        items_in: int | None,
        items_out: int | None,
        error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE pipelines
            SET finished_at = ?, status = ?, items_in = ?, items_out = ?, error_msg = ?
            WHERE id = ?
            """,
            (finished_at, status, items_in, items_out, error_msg, run_id),
        )
