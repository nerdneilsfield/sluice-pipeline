from __future__ import annotations

import aiosqlite


class StagesStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def start(
        self,
        *,
        pipeline_run_id: int,
        stage_name: str,
        started_at: str,
    ) -> int:
        cur = await self.conn.execute(
            """
            INSERT INTO stages (
              pipeline_run_id, stage_name, started_at, status
            ) VALUES (?, ?, ?, 'running')
            """,
            (pipeline_run_id, stage_name, started_at),
        )
        return int(cur.lastrowid)

    async def finalize(
        self,
        stage_run_id: int,
        *,
        finished_at: str,
        status: str,
        items_in: int | None,
        items_out: int | None,
        error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE stages
            SET finished_at = ?, status = ?, items_in = ?, items_out = ?, error_msg = ?
            WHERE id = ?
            """,
            (finished_at, status, items_in, items_out, error_msg, stage_run_id),
        )
